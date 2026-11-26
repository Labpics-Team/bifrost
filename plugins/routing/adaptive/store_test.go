package adaptive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	bifrost "github.com/maximhq/bifrost/core"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore"
	"github.com/maximhq/bifrost/framework/configstore/tables"
	"gorm.io/gorm"
)

type settingsStore struct {
	configstore.ConfigStore
	mu   sync.Mutex
	rows map[string]string
	fail bool
}

func newSettingsStore(t *testing.T) *settingsStore {
	t.Helper()
	store, err := configstore.NewConfigStore(context.Background(), &configstore.Config{Enabled: true, Type: configstore.ConfigStoreTypeSQLite, Config: &configstore.SQLiteConfig{Path: filepath.Join(t.TempDir(), "config.db")}}, bifrost.NewDefaultLogger(schemas.LogLevelError))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close(context.Background()) })
	return &settingsStore{ConfigStore: store, rows: map[string]string{}}
}
func attachStore(t *testing.T, c *Controller, s *settingsStore) {
	t.Helper()
	c.store = s
	var err error
	c.stateStore, err = configstore.NewAdaptiveRoutingStore(s)
	if err != nil {
		t.Fatal(err)
	}
}
func (s *settingsStore) GetConfig(_ context.Context, key string) (*tables.TableGovernanceConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.rows[key]
	if !ok {
		return nil, configstore.ErrNotFound
	}
	return &tables.TableGovernanceConfig{Key: key, Value: v}, nil
}
func (s *settingsStore) UpdateConfig(_ context.Context, row *tables.TableGovernanceConfig, _ ...*gorm.DB) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("database offline")
	}
	s.rows[row.Key] = row.Value
	return nil
}

func TestAdaptiveSettingsRequireExplicitConfiguration(t *testing.T) {
	s := newSettingsStore(t)
	c, err := NewFromStore(context.Background(), s, nil)
	if err != nil || c.Enabled() {
		t.Fatalf("existing deployment unexpectedly enabled: %v", err)
	}
	if c.BeginProviderAttempt(testContext(), schemas.ProviderAttemptInfo{Provider: "p", Model: "m", KeyID: "k"}) != nil {
		t.Fatal("unconfigured deployment retained attempt state")
	}
	cfg := DefaultConfig()
	cfg.AppendFallbacksToPinned = true
	s.fail = true
	if c.Save(context.Background(), cfg) == nil || c.Enabled() {
		t.Fatal("failed persistence changed runtime settings")
	}
	s.fail = false
	if err := c.Save(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	file := DefaultConfig()
	loaded, err := NewFromStore(context.Background(), s, &file)
	if err != nil || !loaded.Enabled() || !loaded.Config().AppendFallbacksToPinned {
		t.Fatalf("saved settings not restored: %v", err)
	}
}

func TestAdaptiveHoldsSurviveRestartButNotCredentialRotation(t *testing.T) {
	s := newSettingsStore(t)
	c, _ := providerController()
	attachStore(t, c, s)
	keys := []schemas.Key{{ID: "a", Name: "A", Value: *schemas.NewSecretVar("secret-value")}}
	setKeys := func(target *Controller) {
		target.SetCatalog(nil, func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) { return keys, nil }, nil)
	}
	setKeys(c)
	observe(c, "p1", "m", "a", responseError(401))
	if err := c.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, _ := c.stateStore.Load(context.Background())
	encoded, _ := json.Marshal(state)
	if strings.Contains(string(encoded), "secret-value") {
		t.Fatal("snapshot contains credential")
	}
	loaded, _ := testController()
	attachStore(t, loaded, s)
	setKeys(loaded)
	if err := loaded.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(loaded.conditions) != 1 || loaded.conditions[routeID{Provider: "p1", KeyID: "a"}] == nil {
		t.Fatalf("whole-key hold was lost: %+v", loaded.routes)
	}
	for _, r := range loaded.routes {
		if r.Inflight != 0 {
			t.Fatal("old in-flight attempts restored")
		}
	}
	keys[0].Value = *schemas.NewSecretVar("rotated-value")
	fresh, _ := testController()
	attachStore(t, fresh, s)
	setKeys(fresh)
	fresh.Restore(context.Background())
	if len(fresh.routes) != 0 {
		t.Fatal("old hold affected rotated key")
	}
}

func cooldownStoreFixture(t *testing.T) (*Controller, *time.Time, *settingsStore) {
	t.Helper()
	c, now := providerController()
	store := newSettingsStore(t)
	attachStore(t, c, store)
	for _, key := range testKeys {
		for n := 0; n < 3; n++ {
			ctx := testContext()
			ctx.SetValue(schemas.BifrostContextKeyRequestID, fmt.Sprintf("failure-%s-%d", key.ID, n))
			observeWithContext(c, ctx, "p1", "m", key.ID, responseError(503))
			*now = now.Add(time.Second)
		}
	}
	return c, now, store
}

func TestAdaptiveCooldownMaintenanceAndRestartPreserveEvidence(t *testing.T) {
	for _, restartBeforeExpiry := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%t", restartBeforeExpiry), func(t *testing.T) {
			ctx := context.Background()
			c, now, store := cooldownStoreFixture(t)
			beforeRoutes := map[routeID]route{}
			beforeIncidents := map[routeID]Incident{}
			for id, r := range c.routes {
				beforeRoutes[id] = *r
			}
			for id, incident := range c.incidents {
				if incident.State != "failed" {
					t.Fatalf("fixture did not establish Failed: %+v", incident)
				}
				beforeIncidents[id] = *incident
			}
			if len(beforeIncidents) != 3 {
				t.Fatalf("expected two key incidents and the provider incident, got %d", len(beforeIncidents))
			}
			if err := c.Checkpoint(ctx); err != nil {
				t.Fatal(err)
			}
			*now = now.Add(recoveryInterval + time.Second)
			if restartBeforeExpiry {
				loaded, loadedNow := providerController()
				*loadedNow = *now
				attachStore(t, loaded, store)
				if err := loaded.Restore(ctx); err != nil {
					t.Fatal(err)
				}
				c = loaded
			} else if err := c.maintain(ctx); err != nil {
				t.Fatal(err)
			}
			for id, before := range beforeIncidents {
				detail, err := c.GetIncident(ctx, before.ID, "", 100)
				if err != nil {
					t.Fatal(err)
				}
				after := detail.Incident
				if after.State != "recovering" || after.Status != "recovering" || after.ClosedAt != nil || after.ResolvedAt != nil {
					t.Fatalf("%+v did not enter unconfirmed recovery after cooldown: %+v", id, after)
				}
				if !after.LastSeen.Equal(before.LastSeen) || after.FirstRequestID != before.FirstRequestID || after.LastRequestID != before.LastRequestID ||
					after.Checks != before.Checks || after.FailedChecks != before.FailedChecks || after.RefusedAgain != before.RefusedAgain ||
					after.FailureClass != before.FailureClass || after.WorstState != before.WorstState {
					t.Fatalf("timer invented or replaced request evidence: before=%+v after=%+v", before, after)
				}
				cooldowns := 0
				for _, event := range detail.Events {
					if event.Kind == "state_changed" && strings.Contains(event.Message, "cooldown") {
						cooldowns++
						if event.RequestID != "" || event.State != "recovering" || !event.Timestamp.Equal(*now) || !strings.Contains(strings.ToLower(event.Message), "cooldown") {
							t.Fatalf("timer event pretends to be a real observation: %+v", event)
						}
					}
				}
				if cooldowns != 1 {
					t.Fatalf("cooldown events=%d, want exactly one", cooldowns)
				}
			}
			for id, before := range beforeRoutes {
				if after := c.routes[id]; after == nil || !reflect.DeepEqual(before, *after) {
					t.Fatalf("maintenance changed real route telemetry: before=%+v after=%+v", before, after)
				}
			}
			for _, status := range c.Status() {
				if status.State != "recovering" {
					t.Fatalf("route stayed Failed after cooldown: %+v", status)
				}
			}
			if err := c.Checkpoint(ctx); err != nil {
				t.Fatal(err)
			}
			revision := c.revision
			loaded, loadedNow := providerController()
			*loadedNow = *now
			attachStore(t, loaded, store)
			if err := loaded.Restore(ctx); err != nil {
				t.Fatal(err)
			}
			for n := 0; n < 2; n++ {
				if err := loaded.maintain(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if loaded.revision != revision {
				t.Fatalf("restart/repeated maintenance duplicated the lifecycle transition: %d -> %d", revision, loaded.revision)
			}
		})
	}
}

func TestAdaptiveCooldownMaintenanceKeepsHoldsAndDisabledHistory(t *testing.T) {
	for _, held := range []bool{false, true} {
		t.Run(fmt.Sprintf("held=%t", held), func(t *testing.T) {
			ctx := context.Background()
			c, now, _ := cooldownStoreFixture(t)
			if held {
				observe(c, "p1", "m", "a", responseError(429))
				observe(c, "p1", "m", "b", responseError(401))
			} else {
				config := c.Config()
				config.HistoryEnabled = false
				if err := c.Apply(config); err != nil {
					t.Fatal(err)
				}
			}
			ids := make([]string, 0, len(c.incidents))
			for _, incident := range c.incidents {
				ids = append(ids, incident.ID)
			}
			*now = now.Add(time.Hour)
			if err := c.maintain(ctx); err != nil {
				t.Fatal(err)
			}
			for _, id := range ids {
				detail, err := c.GetIncident(ctx, id, "", 100)
				if err != nil {
					t.Fatal(err)
				}
				for _, event := range detail.Events {
					if event.Kind == "state_changed" && strings.Contains(event.Message, "cooldown") {
						t.Fatalf("held route or disabled history gained cooldown evidence: %+v", event)
					}
				}
				if !held && (detail.Incident.ClosedAt == nil || detail.Incident.Status != "no_longer_observed") {
					t.Fatalf("disabled optional history remained open: %+v", detail.Incident)
				}
			}
			if held {
				for _, status := range c.Status() {
					if status.State != "held" {
						t.Fatalf("elapsed timer released a refusal: %+v", status)
					}
				}
				if len(c.conditions) != 2 {
					t.Fatal("maintenance removed an unresolved hold")
				}
			}
		})
	}
}

func TestAdaptiveCooldownMaintenanceRejectsChangedCatalog(t *testing.T) {
	ctx := context.Background()
	c, now, _ := cooldownStoreFixture(t)
	ids := make([]string, 0, len(c.incidents))
	for _, incident := range c.incidents {
		ids = append(ids, incident.ID)
	}
	*now = now.Add(recoveryInterval + time.Second)
	catalog := c.keys
	changed := false
	c.keys = func(ctx context.Context, provider schemas.ModelProvider) ([]schemas.Key, error) {
		keys, err := catalog(ctx, provider)
		if !changed {
			changed = true
			if resetErr := c.ResetProvider(provider); resetErr != nil {
				t.Fatal(resetErr)
			}
		}
		return keys, err
	}
	if err := c.maintain(ctx); err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("maintenance did not reconcile the provider against the live catalog")
	}
	for _, id := range ids {
		detail, err := c.GetIncident(ctx, id, "", 100)
		if err != nil || detail.Incident.ClosedAt == nil || detail.Incident.Status != "no_longer_observed" {
			t.Fatalf("stale catalog revived a retired incident: %+v %v", detail, err)
		}
		for _, event := range detail.Events {
			if event.Kind == "state_changed" && strings.Contains(event.Message, "cooldown") {
				t.Fatalf("retired revision received a stale timer event: %+v", event)
			}
		}
	}
}

func TestAdaptiveCooldownMaintenanceTracksStaggeredProviderRecovery(t *testing.T) {
	ctx := context.Background()
	c, now, _ := cooldownStoreFixture(t)
	provider := *c.incidents[routeID{Provider: "p1", Model: "m"}]
	for _, phase := range []struct {
		key, state string
	}{{"a", "degraded"}, {"b", "recovering"}} {
		*now = c.routes[routeID{"p1", "m", phase.key}].FailedUntil
		if err := c.maintain(ctx); err != nil {
			t.Fatal(err)
		}
		detail, err := c.GetIncident(ctx, provider.ID, "", 100)
		if err != nil || detail.Incident.State != phase.state || !detail.Incident.LastSeen.Equal(provider.LastSeen) || detail.Incident.LastRequestID != provider.LastRequestID {
			t.Fatalf("provider disagrees with elapsed key cooldowns: %+v %v", detail.Incident, err)
		}
	}
	detail, err := c.GetIncident(ctx, provider.ID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	cooldowns := 0
	for _, event := range detail.Events {
		if event.Kind == "state_changed" && strings.Contains(event.Message, "cooldown") {
			cooldowns++
			if event.RequestID != "" {
				t.Fatalf("provider cooldown invented request evidence: %+v", event)
			}
		}
	}
	if cooldowns != 2 {
		t.Fatalf("expected one provider transition per elapsed cooldown, got %d", cooldowns)
	}
	revision := c.revision
	if err := c.maintain(ctx); err != nil || c.revision != revision {
		t.Fatalf("unchanged provider cooldown duplicated its transition: %v", err)
	}
}

func TestAdaptiveCooldownMaintenanceUpdatesReleasedWholeKeyScope(t *testing.T) {
	ctx := context.Background()
	c, now := providerController()
	attachStore(t, c, newSettingsStore(t))
	observe(c, "p1", "m", "a", responseError(401))
	if err := c.Release(ctx, "p1", "a", ""); err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 3; n++ {
		request := testContext()
		request.SetValue(schemas.BifrostContextKeyRequestID, fmt.Sprintf("transient-after-release-%d", n))
		observeWithContext(c, request, "p1", "m", "a", responseError(503))
		*now = now.Add(time.Second)
	}
	whole := *c.incidents[routeID{Provider: "p1", KeyID: "a"}]
	routeBefore := *c.routes[routeID{"p1", "m", "a"}]
	if whole.State != "failed" || len(c.conditions) != 0 {
		t.Fatalf("released whole-key incident did not retain transient failure: %+v", whole)
	}
	*now = routeBefore.FailedUntil
	if err := c.maintain(ctx); err != nil {
		t.Fatal(err)
	}
	detail, err := c.GetIncident(ctx, whole.ID, "", 100)
	if err != nil || detail.Incident.State != "recovering" || detail.Incident.ClosedAt != nil ||
		!detail.Incident.LastSeen.Equal(whole.LastSeen) || detail.Incident.LastRequestID != whole.LastRequestID ||
		detail.Incident.Checks != whole.Checks || detail.Incident.WorstState != whole.WorstState {
		t.Fatalf("whole-key history disagrees with the route or invented recovery: %+v %v", detail.Incident, err)
	}
	if after := c.routes[routeBefore.routeID]; !reflect.DeepEqual(routeBefore, *after) {
		t.Fatalf("whole-key reconciliation changed real samples: before=%+v after=%+v", routeBefore, after)
	}
	cooldowns := 0
	for _, event := range detail.Events {
		if event.Kind == "state_changed" && strings.Contains(event.Message, "cooldown") {
			cooldowns++
			if event.RequestID != "" {
				t.Fatalf("whole-key cooldown borrowed request evidence: %+v", event)
			}
		}
	}
	if cooldowns != 1 {
		t.Fatalf("whole-key cooldown events=%d, want one", cooldowns)
	}
}

func TestAdaptiveCooldownMaintenanceDoesNotLoseSuccessBeforeTick(t *testing.T) {
	ctx := context.Background()
	c, now, _ := cooldownStoreFixture(t)
	id := routeID{"p1", "m", "a"}
	samples := c.routes[id].Samples
	keyIncident := c.incidents[id].ID
	providerIncident := c.incidents[routeID{Provider: "p1", Model: "m"}].ID
	*now = now.Add(recoveryInterval + time.Second)
	request := testContext()
	request.SetValue(schemas.BifrostContextKeyRequestID, "first-real-request-after-cooldown")
	observeWithContext(c, request, "p1", "m", "a", nil)
	if c.routes[id].Samples != samples+1 || c.stateLocked(c.routes[id], *now) != "recovering" {
		t.Fatalf("real success did not add exactly one recovery sample: %+v", c.routes[id])
	}
	// The request finishes before maintenance can see the old FailedUntil.
	for _, incidentID := range []string{keyIncident, providerIncident} {
		detail, err := c.GetIncident(ctx, incidentID, "", 100)
		if err != nil || detail.Incident.State != "recovering" || detail.Incident.Status != "recovering" ||
			detail.Incident.LastRequestID != "first-real-request-after-cooldown" || !detail.Incident.LastSeen.Equal(*now) {
			t.Fatalf("real recovery was hidden by the elapsed cooldown: %+v %v", detail.Incident, err)
		}
		for _, event := range detail.Events {
			if event.Kind == "state_changed" && strings.Contains(event.Message, "cooldown") {
				t.Fatalf("real completion was replaced with a synthetic timer event: %+v", event)
			}
		}
	}
}

func TestAdaptiveProviderDiagnosticRetryRetainsCurrentObservation(t *testing.T) {
	for _, cause := range []string{"catalog_failure", "revision_changed"} {
		t.Run(cause, func(t *testing.T) {
			ctx := context.Background()
			c, now, _ := cooldownStoreFixture(t)
			providerID := c.incidents[routeID{Provider: "p1", Model: "m"}].ID
			id := routeID{"p1", "m", "a"}
			*now = now.Add(recoveryInterval + time.Second)
			request := testContext()
			request.SetValue(schemas.BifrostContextKeyRequestID, "actual-recovery-for-diagnostic")
			attempt := c.BeginProviderAttempt(request, schemas.ProviderAttemptInfo{Provider: "p1", Model: "m", KeyID: "a"})
			if attempt == nil {
				t.Fatal("real recovery attempt was not admitted")
			}
			catalog := c.keys
			blocked := true
			c.keys = func(ctx context.Context, provider schemas.ModelProvider) ([]schemas.Key, error) {
				keys, err := catalog(ctx, provider)
				if blocked {
					if cause == "catalog_failure" {
						return nil, errors.New("temporary catalog failure")
					}
					blocked = false
					// An unrelated configuration transaction changes the writer
					// revision without invalidating this provider's observation.
					if resetErr := c.ResetProvider("p2"); resetErr != nil {
						t.Fatal(resetErr)
					}
				}
				return keys, err
			}
			attempt.Observe(&schemas.BifrostResponse{}, nil)
			attempt.End()
			afterObservation := *c.routes[id]
			if len(c.pendingDiagnostics) != 1 {
				t.Fatalf("current observation was dropped on %s: %+v", cause, c.pendingDiagnostics)
			}
			if cause == "catalog_failure" {
				for n := 0; n < 2; n++ {
					c.flushDiagnostics()
					if len(c.pendingDiagnostics) != 1 {
						t.Fatalf("flush lost or duplicated a temporarily blocked diagnostic: %+v", c.pendingDiagnostics)
					}
				}
			}
			blocked = false
			c.flushDiagnostics()
			detail, err := c.GetIncident(ctx, providerID, "", 100)
			if err != nil || detail.Incident.State != "recovering" || detail.Incident.LastRequestID != "actual-recovery-for-diagnostic" || len(c.pendingDiagnostics) != 0 {
				t.Fatalf("recovered catalog did not apply the retained observation: %+v pending=%d err=%v", detail.Incident, len(c.pendingDiagnostics), err)
			}
			if after := c.routes[id]; !reflect.DeepEqual(afterObservation, *after) {
				t.Fatalf("diagnostic retry added synthetic telemetry: before=%+v after=%+v", afterObservation, after)
			}
		})
	}
}

func TestAdaptiveWholeKeyRecoveryWaitsForOtherModels(t *testing.T) {
	ctx := context.Background()
	c, now := providerController()
	attachStore(t, c, newSettingsStore(t))
	observe(c, "p1", "model-a", "a", responseError(401))
	if err := c.Release(ctx, "p1", "a", ""); err != nil {
		t.Fatal(err)
	}
	for _, model := range []string{"model-a", "model-b"} {
		for n := 0; n < 3; n++ {
			observe(c, "p1", model, "a", responseError(503))
			*now = now.Add(time.Second)
		}
		*now = now.Add(10 * time.Second)
	}
	whole := c.incidents[routeID{Provider: "p1", KeyID: "a"}].ID
	a := c.routes[routeID{"p1", "model-a", "a"}]
	b := c.routes[routeID{"p1", "model-b", "a"}]
	*now = a.FailedUntil
	if !b.FailedUntil.After(*now) {
		t.Fatal("fixture must retain model B's failed cooldown")
	}
	request := testContext()
	request.SetValue(schemas.BifrostContextKeyRequestID, "model-a-real-recovery")
	observeWithContext(c, request, "p1", "model-a", "a", nil)
	detail, err := c.GetIncident(ctx, whole, "", 100)
	if err != nil || detail.Incident.State != "degraded" || detail.Incident.ResolvedAt != nil || detail.Incident.ClosedAt != nil ||
		detail.Incident.LastRequestID != "model-a-real-recovery" {
		t.Fatalf("one model's success hid another model's failure: %+v %v", detail.Incident, err)
	}
	// Even fully restoring A's score cannot resolve this key's other failed model.
	for n := 0; n < 20; n++ {
		observe(c, "p1", "model-a", "a", nil)
	}
	if c.stateLocked(a, *now) != "healthy" || c.stateLocked(b, *now) != "failed" {
		t.Fatalf("expected healthy A and failed B: A=%s B=%s", c.stateLocked(a, *now), c.stateLocked(b, *now))
	}
	detail, err = c.GetIncident(ctx, whole, "", 100)
	if err != nil || detail.Incident.State != "degraded" || detail.Incident.ResolvedAt != nil || detail.Incident.ClosedAt != nil {
		t.Fatalf("whole-key incident resolved before model B recovered: %+v %v", detail.Incident, err)
	}
	beforeB := *b
	*now = b.FailedUntil
	if err := c.maintain(ctx); err != nil {
		t.Fatal(err)
	}
	detail, err = c.GetIncident(ctx, whole, "", 100)
	if err != nil || detail.Incident.State != "recovering" || detail.Incident.ResolvedAt != nil || detail.Incident.ClosedAt != nil {
		t.Fatalf("maintenance disagrees with the same model observations: %+v %v", detail.Incident, err)
	}
	if !reflect.DeepEqual(beforeB, *b) {
		t.Fatalf("model B acquired synthetic recovery evidence: before=%+v after=%+v", beforeB, b)
	}
}
