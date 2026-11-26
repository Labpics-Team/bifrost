package adaptive

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore"
	"gorm.io/gorm"
)

func TestAdaptiveOldSuccessCannotReleaseNewHold(t *testing.T) {
	c, now := testController()
	a := c.BeginProviderAttempt(testContext(), schemas.ProviderAttemptInfo{Provider: "p", Model: "m", KeyID: "a"})
	*now = now.Add(time.Second)
	observe(c, "p", "m", "a", responseError(401))
	a.Observe(&schemas.BifrostResponse{}, nil)
	a.End()
	for _, r := range c.Status() {
		if r.KeyID == "a" && r.State == "held" {
			return
		}
	}
	t.Fatal("a success started before the refusal released its newer hold")
}

func TestAdaptiveOldCompletionCannotReleaseNewProbe(t *testing.T) {
	c, now := testController()
	observe(c, "p", "m", "a", responseError(401))
	*now = now.Add(time.Minute)
	ctx1 := testContext()
	k, _ := c.SelectKey(ctx1, testKeys, "p", "m")
	if k.ID != "a" {
		t.Fatal("first due key was not selected")
	}
	a := c.BeginProviderAttempt(ctx1, schemas.ProviderAttemptInfo{Provider: "p", Model: "m", KeyID: "a"})
	*now = now.Add(6 * time.Minute)
	k, _ = c.SelectKey(testContext(), testKeys, "p", "m")
	if k.ID != "a" {
		t.Fatal("new check was not reserved after lost-check timeout")
	}
	a.End()
	k, _ = c.SelectKey(testContext(), testKeys, "p", "m")
	if k.ID != "b" {
		t.Fatal("old completion cleared the newer check reservation")
	}
}

func TestAdaptiveStatusDoesNotForgetUnresolvedHold(t *testing.T) {
	c, now := testController()
	observe(c, "p", "m", "a", responseError(401))
	*now = now.Add(25 * time.Hour)
	if got := c.Status(); len(got) == 0 {
		t.Fatal("reading status deleted an unresolved hold")
	}
	c.SelectKey(testContext(), testKeys, "p", "m")
	k, _ := c.SelectKey(testContext(), testKeys, "p", "m")
	if k.ID != "b" {
		t.Fatal("expired recheck wait became ordinary serving traffic")
	}
}

func TestAdaptiveDueHoldSurvivesSnapshotRestart(t *testing.T) {
	c, now := providerController()
	s := newSettingsStore(t)
	attachStore(t, c, s)
	observe(c, "p1", "m", "a", responseError(401))
	*now = now.Add(time.Hour)
	if err := c.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	loaded, _ := providerController()
	attachStore(t, loaded, s)
	if err := loaded.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(loaded.Status()) == 0 {
		t.Fatal("restart lost a due but unresolved hold")
	}
}

func TestAdaptiveSnapshotDoesNotRebindRefusalToRotatedKey(t *testing.T) {
	c, _ := providerController()
	s := newSettingsStore(t)
	attachStore(t, c, s)
	old := schemas.Key{ID: "a", Value: *schemas.NewSecretVar("old")}
	rotated := schemas.Key{ID: "a", Value: *schemas.NewSecretVar("new")}
	c.SetCatalog(nil, func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) { return []schemas.Key{old}, nil }, nil)
	observe(c, "p1", "m", "a", responseError(401))
	// The rotation happens after the snapshot captured the old refusal but
	// before the old implementation asks the catalog for a fingerprint.
	c.SetCatalog(nil, func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) {
		c.ResetKey("p1", "a")
		return []schemas.Key{rotated}, nil
	}, nil)
	if err := c.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	loaded, _ := testController()
	attachStore(t, loaded, s)
	loaded.SetCatalog(nil, func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) {
		return []schemas.Key{rotated}, nil
	}, nil)
	if err := loaded.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(loaded.Status()) != 0 {
		t.Fatal("old refusal was restored onto the new credential")
	}
}

func TestAdaptiveProviderNeedsEligibleKey(t *testing.T) {
	c, _ := providerController()
	c.SetCatalog(func(string) []schemas.ModelProvider { return []schemas.ModelProvider{"p1", "p2"} }, func(_ context.Context, p schemas.ModelProvider) ([]schemas.Key, error) {
		if p == "p1" {
			return nil, nil
		}
		return testKeys, nil
	}, func(schemas.ModelProvider, schemas.Key, string) bool { return true })
	r := routeRequest("")
	c.Route(testContext(), r)
	p, _, f := r.GetRequestFields()
	if p != "p2" || len(f) != 0 {
		t.Fatalf("ineligible provider entered routing: %s %+v", p, f)
	}
}

func TestAdaptiveFallbackRedirectUsesGlobalLastResort(t *testing.T) {
	c, _ := providerController()
	cfg := c.Config()
	cfg.RerouteFailedDirections = true
	cfg.RerouteTarget = "fallbacks"
	c.Apply(cfg)
	failProvider(c, "p1")
	failProvider(c, "failed")
	r := routeRequest("p1", schemas.Fallback{Provider: "failed", Model: "m"})
	c.Route(testContext(), r)
	if p, _, _ := r.GetRequestFields(); p != "p2" {
		t.Fatalf("global healthy last resort was not used: %s", p)
	}
}

func TestAdaptiveNamedDueProbeKeepsHealthyFallback(t *testing.T) {
	c, now := providerController()
	cfg := c.Config()
	cfg.RerouteFailedDirections = true
	c.Apply(cfg)
	failProvider(c, "p1")
	*now = now.Add(time.Minute)
	r := routeRequest("p1")
	c.Route(testContext(), r)
	p, _, f := r.GetRequestFields()
	if p != "p1" || len(f) != 1 || f[0].Provider != "p2" {
		t.Fatalf("named due check lost healthy fallback: %s %+v", p, f)
	}
}

func TestAdaptiveEvictionKeepsUnresolvedConditions(t *testing.T) {
	c, _ := testController()
	observe(c, "p", "held", "a", responseError(401))
	for i := 0; i < maxRoutes+4; i++ {
		observe(c, "p", fmt.Sprintf("model-%d", i), "b", nil)
	}
	if len(c.routes) > maxRoutes {
		t.Fatal("telemetry is unbounded")
	}
	if c.holdLocked(routeID{"p", "held", "a"}, c.now()) == nil {
		t.Fatal("telemetry eviction deleted a condition")
	}
}

func TestAdaptiveManualReleaseFencesOldCompletion(t *testing.T) {
	c, _ := testController()
	a := c.BeginProviderAttempt(testContext(), schemas.ProviderAttemptInfo{Provider: "p", Model: "m", KeyID: "a"})
	observe(c, "p", "m", "a", responseError(401))
	if err := c.Release(context.Background(), "p", "a", ""); err != nil {
		t.Fatal(err)
	}
	a.Observe(nil, responseError(401))
	a.End()
	if len(c.conditions) != 0 {
		t.Fatal("a completion from before release recreated the condition")
	}
}

func TestAdaptiveDurableIncidentLifecycle(t *testing.T) {
	c, now := providerController()
	s := newSettingsStore(t)
	attachStore(t, c, s)
	observe(c, "p1", "m", "a", responseError(401))
	page, err := c.ListIncidents(context.Background(), IncidentQuery{Scope: "key"})
	if err != nil || len(page.Incidents) != 1 {
		t.Fatalf("incident was not recorded: %+v %v", page, err)
	}
	id := page.Incidents[0].ID
	*now = now.Add(time.Minute)
	ctx := testContext()
	key, _ := c.SelectKey(ctx, testKeys, "p1", "m")
	if key.ID != "a" {
		t.Fatal("due key was not checked")
	}
	observeWithContext(c, ctx, "p1", "m", "a", nil)
	detail, err := c.GetIncident(context.Background(), id, "", 100)
	if err != nil || detail.Incident.Status != "recovering" {
		t.Fatalf("recovery was not durable: %+v %v", detail, err)
	}
	*now = now.Add(2 * time.Hour)
	if err := c.maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	detail, err = c.GetIncident(context.Background(), id, "", 100)
	if err != nil || detail.Incident.Status != "waiting_for_traffic" {
		t.Fatalf("absence of traffic was reported as recovery: %+v %v", detail, err)
	}
	observe(c, "p1", "m", "a", nil)
	detail, err = c.GetIncident(context.Background(), id, "", 100)
	if err != nil || detail.Incident.Status != "recovering" {
		t.Fatalf("resumed traffic did not resume recovery: %+v %v", detail, err)
	}
	for i := 0; i < 20; i++ {
		observe(c, "p1", "m", "a", nil)
	}
	detail, err = c.GetIncident(context.Background(), id, "", 100)
	if err != nil || detail.Incident.Status != "resolved" || detail.Incident.ResolvedAt == nil || detail.Incident.ClosedAt == nil {
		t.Fatalf("confirmed recovery did not end incident: %+v %v", detail, err)
	}
	kinds := map[string]bool{}
	for _, event := range detail.Events {
		kinds[event.Kind] = true
	}
	if !kinds["held"] || !kinds["check_started"] || !kinds["hold_released"] {
		t.Fatalf("missing lifecycle events: %+v", kinds)
	}
}

func TestAdaptiveFailedReleaseKeepsCommittedHold(t *testing.T) {
	c, _ := providerController()
	s := newSettingsStore(t)
	attachStore(t, c, s)
	observe(c, "p1", "m", "a", responseError(401))
	if err := s.DB().Exec("CREATE TRIGGER fail_adaptive_commit BEFORE UPDATE ON adaptive_routing_state BEGIN SELECT RAISE(FAIL, 'test disk failure'); END").Error; err != nil {
		t.Fatal(err)
	}
	if err := c.Release(context.Background(), "p1", "a", ""); err == nil {
		t.Fatal("release acknowledged a failed commit")
	}
	if c.holdLocked(routeID{"p1", "m", "a"}, c.now()) == nil || c.PersistenceStatus().Healthy {
		t.Fatal("failed commit changed published state or hid the error")
	}
	if err := s.DB().Exec("DROP TRIGGER fail_adaptive_commit").Error; err != nil {
		t.Fatal(err)
	}
	if err := c.Release(context.Background(), "p1", "a", ""); err != nil {
		t.Fatal(err)
	}
}

func TestAdaptiveSQLiteWaitDoesNotBlockHealthyTraffic(t *testing.T) {
	adaptiveHealthyTrafficDuringSQLiteWait(t, false, false, 401)
}

func TestAdaptiveDueKeyCheckDoesNotBlockHealthyTraffic(t *testing.T) {
	adaptiveHealthyTrafficDuringSQLiteWait(t, true, false, 401)
}

func TestAdaptiveDueProviderCheckDoesNotBlockHealthyTraffic(t *testing.T) {
	adaptiveHealthyTrafficDuringSQLiteWait(t, true, true, 401)
}

func TestAdaptiveTransientKeyCheckYieldsDuringSQLiteWrite(t *testing.T) {
	adaptiveHealthyTrafficDuringSQLiteWait(t, true, false, 503)
}

func TestAdaptiveTransientProviderCheckYieldsDuringSQLiteWrite(t *testing.T) {
	adaptiveHealthyTrafficDuringSQLiteWait(t, true, true, 503)
}

func adaptiveHealthyTrafficDuringSQLiteWait(t *testing.T, due, routeProvider bool, status int) {
	t.Helper()
	c, now := providerController()
	s := newSettingsStore(t)
	attachStore(t, c, s)
	failKey := func(key string) {
		attempts := 1
		if status == 503 {
			attempts = 3
		}
		for range attempts {
			observe(c, "p1", "m", key, responseError(status))
		}
	}
	failKey("a")
	if routeProvider {
		failKey("b")
		observe(c, "p2", "m", "b", nil)
		cfg := c.Config()
		cfg.RerouteFailedDirections = true
		c.Apply(cfg)
	} else {
		observe(c, "p1", "m", "b", nil)
	}
	if due {
		*now = now.Add(time.Minute)
	}
	locked := make(chan struct{})
	unlock := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- s.DB().Transaction(func(tx *gorm.DB) error {
			if err := tx.Exec("UPDATE adaptive_routing_state SET revision = revision WHERE id = 1").Error; err != nil {
				return err
			}
			close(locked)
			<-unlock
			return nil
		})
	}()
	<-locked
	releaseDone := make(chan error, 1)
	go func() {
		if status == 503 {
			releaseDone <- c.ResetKey("p1", "a")
		} else {
			releaseDone <- c.Release(context.Background(), "p1", "a", "")
		}
	}()
	deadline := time.Now().Add(time.Second)
	for c.lifeMu.TryLock() {
		c.lifeMu.Unlock()
		if time.Now().After(deadline) {
			close(unlock)
			<-writerDone
			t.Fatal("release never entered durable transition")
		}
		time.Sleep(time.Millisecond)
	}
	served := make(chan error, 1)
	go func() {
		if routeProvider {
			for _, provider := range []schemas.ModelProvider{"", "p1"} {
				ctx, req := testContext(), routeRequest(provider)
				if err := c.Route(ctx, req); err != nil {
					served <- err
					return
				}
				if chosen, _, _ := req.GetRequestFields(); chosen != "p2" {
					served <- fmt.Errorf("expected healthy provider p2, got %s", chosen)
					return
				}
				observeWithContext(c, ctx, "p2", "m", "b", nil)
			}
			served <- nil
			return
		}
		for _, model := range []string{"m", "new-model"} {
			ctx := testContext()
			key, err := c.SelectKey(ctx, testKeys, "p1", model)
			// A transient failure only affects m. A new model can use either
			// key, while a credential refusal holds a across every model.
			mustUseB := status == 401 || model == "m"
			if err != nil || key.ID != "a" && key.ID != "b" || mustUseB && key.ID != "b" {
				served <- fmt.Errorf("expected healthy key for %s after %d, got %s: %v", model, status, key.ID, err)
				return
			}
			observeWithContext(c, ctx, "p1", model, key.ID, nil)
		}
		// First use of a different configured key also needs no durable write.
		observe(c, "p2", "fresh", "b", nil)
		served <- nil
	}()
	var servingErr error
	select {
	case servingErr = <-served:
	case <-time.After(200 * time.Millisecond):
		close(unlock)
		<-writerDone
		<-releaseDone
		t.Fatal("healthy traffic waited for SQLite while a release was pending")
	}
	close(unlock)
	if err := <-writerDone; err != nil {
		t.Fatal(err)
	}
	if err := <-releaseDone; err != nil {
		t.Fatal(err)
	}
	if servingErr != nil {
		t.Fatal(servingErr)
	}
}

func TestAdaptiveScopeChangeResolvesAllRecoveredIncidents(t *testing.T) {
	for _, codes := range [][2]int{{429, 401}, {401, 429}} {
		t.Run(fmt.Sprint(codes), func(t *testing.T) {
			c, now := providerController()
			s := newSettingsStore(t)
			attachStore(t, c, s)
			observe(c, "p1", "m", "a", responseError(codes[0]))
			*now = now.Add(time.Minute)
			observe(c, "p1", "m", "a", responseError(codes[1]))
			if err := c.Release(context.Background(), "p1", "a", ""); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 30; i++ {
				observe(c, "p1", "m", "a", nil)
			}
			page, err := c.ListIncidents(context.Background(), IncidentQuery{})
			if err != nil {
				t.Fatal(err)
			}
			seen := map[string]bool{}
			for _, incident := range page.Incidents {
				if incident.KeyID != "a" {
					continue
				}
				seen[incident.Scope] = true
				cause := string(schemas.FailureClassRateLimit)
				if incident.Scope == "key" {
					cause = string(schemas.FailureClassCredential)
				}
				if incident.FailureClass != cause {
					t.Fatalf("recovery replaced scope's historical cause: %+v", incident)
				}
				if incident.Status != "resolved" || incident.ClosedAt == nil || incident.ResolvedAt == nil {
					t.Fatalf("recovered scope remained open: %+v", incident)
				}
			}
			if !seen["key"] || !seen["key_model"] {
				t.Fatalf("lost a related incident: %+v", seen)
			}
		})
	}
}

func TestAdaptiveSameScopeRefusalKeepsCompletedCheck(t *testing.T) {
	c, now := providerController()
	s := newSettingsStore(t)
	attachStore(t, c, s)
	observe(c, "p1", "m", "a", responseError(401))
	*now = now.Add(time.Minute)
	ctx := testContext()
	key, err := c.SelectKey(ctx, testKeys, "p1", "m")
	if err != nil || key.ID != "a" {
		t.Fatalf("due check was not selected: %s %v", key.ID, err)
	}
	observeWithContext(c, ctx, "p1", "m", "a", responseError(402))
	loaded, _ := providerController()
	attachStore(t, loaded, s)
	if err := loaded.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	page, err := loaded.ListIncidents(context.Background(), IncidentQuery{Scope: "key"})
	if err != nil || len(page.Incidents) != 1 {
		t.Fatalf("scope changed its incident identity: %+v %v", page, err)
	}
	i := page.Incidents[0]
	if i.Checks != 1 || i.FailureClass != string(schemas.FailureClassQuota) || i.ClosedAt != nil {
		t.Fatalf("same-scope transition lost its completed check: %+v", i)
	}
	detail, err := loaded.GetIncident(context.Background(), i.ID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	checks := 0
	for _, event := range detail.Events {
		if event.Kind == "check_started" {
			checks++
		}
	}
	if checks != 1 {
		t.Fatalf("expected one recorded check, got %d", checks)
	}
}

func TestAdaptiveResolvedIncidentKeepsFailureCauseAfterRestart(t *testing.T) {
	c, _ := providerController()
	s := newSettingsStore(t)
	attachStore(t, c, s)
	observe(c, "p1", "m", "a", responseError(429))
	if err := c.Release(context.Background(), "p1", "a", "m"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		observe(c, "p1", "m", "a", nil)
	}
	if err := c.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	loaded, _ := providerController()
	attachStore(t, loaded, s)
	if err := loaded.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	page, err := loaded.ListIncidents(context.Background(), IncidentQuery{Scope: "key_model"})
	if err != nil || len(page.Incidents) != 1 {
		t.Fatalf("resolved history lost: %+v %v", page, err)
	}
	i := page.Incidents[0]
	if i.Status != "resolved" || i.ClosedAt == nil || i.FailureClass != string(schemas.FailureClassRateLimit) {
		t.Fatalf("resolved incident lost its historical cause: %+v", i)
	}
	detail, err := loaded.GetIncident(context.Background(), i.ID, "", 1)
	if err != nil || detail.Incident.FailureClass != string(schemas.FailureClassRateLimit) {
		t.Fatalf("detail cause depends on the event page: %+v %v", detail, err)
	}
}

func TestAdaptiveProbeAdmissionIsVolatileUntilCompletion(t *testing.T) {
	c, now := providerController()
	s := newSettingsStore(t)
	attachStore(t, c, s)
	observe(c, "p1", "m", "a", responseError(401))
	*now = now.Add(time.Minute)
	if err := s.DB().Exec("CREATE TRIGGER fail_adaptive_commit BEFORE UPDATE ON adaptive_routing_state BEGIN SELECT RAISE(ABORT, 'write unavailable'); END").Error; err != nil {
		t.Fatal(err)
	}
	ctx := testContext()
	key, err := c.SelectKey(ctx, testKeys, "p1", "m")
	if err != nil || key.ID != "a" {
		t.Fatalf("probe admission depended on SQLite write: %s %v", key.ID, err)
	}
	a := c.BeginProviderAttempt(ctx, schemas.ProviderAttemptInfo{Provider: "p1", Model: "m", KeyID: key.ID})
	a.End() // caller cancellation produces no completed check or failure sample
	ctx = testContext()
	key, err = c.SelectKey(ctx, testKeys, "p1", "m")
	if err != nil || key.ID != "a" {
		t.Fatal("cancelled attempt retained its lease")
	}
	state, err := c.stateStore.Load(context.Background())
	if err != nil || len(state.Conditions) != 1 {
		t.Fatalf("lease changed the durable hold: %+v %v", state, err)
	}
	for _, i := range state.OpenIncidents {
		if i.Checks != 0 {
			t.Fatal("unfinished attempt counted as a completed check")
		}
	}
	// A new runtime sees the same hold and can acquire a fresh lease immediately.
	loaded, loadedNow := providerController()
	*loadedNow = *now
	attachStore(t, loaded, s)
	if err := loaded.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx = testContext()
	key, err = loaded.SelectKey(ctx, testKeys, "p1", "m")
	if err != nil || key.ID != "a" {
		t.Fatal("restart restored a stale lease")
	}
	if err := s.DB().Exec("DROP TRIGGER fail_adaptive_commit").Error; err != nil {
		t.Fatal(err)
	}
	started := *loadedNow
	a = loaded.BeginProviderAttempt(ctx, schemas.ProviderAttemptInfo{Provider: "p1", Model: "m", KeyID: key.ID})
	*loadedNow = loadedNow.Add(time.Second)
	a.Observe(nil, responseError(401))
	a.End()
	page, err := loaded.ListIncidents(context.Background(), IncidentQuery{Scope: "key"})
	if err != nil || len(page.Incidents) != 1 || page.Incidents[0].Checks != 1 {
		t.Fatalf("completed check was not recorded once: %+v %v", page, err)
	}
	detail, err := loaded.GetIncident(context.Background(), page.Incidents[0].ID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range detail.Events {
		if event.Kind == "check_started" {
			found = true
			if !event.Timestamp.Equal(started) {
				t.Fatal("check start lost its actual timestamp")
			}
		}
	}
	if !found {
		t.Fatal("completed check lost its start event")
	}
}

func TestAdaptiveFrozenRevisionDetectsNestedSecretRotation(t *testing.T) {
	c, _ := testController()
	key := schemas.Key{ID: "a", AzureKeyConfig: &schemas.AzureKeyConfig{Endpoint: *schemas.NewSecretVar("https://resource.example"), ClientSecret: schemas.NewSecretVar("old-secret")}}
	c.SetCatalog(nil, func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) { return []schemas.Key{key}, nil }, nil)
	a := c.BeginProviderAttempt(testContext(), schemas.ProviderAttemptInfo{Provider: "p", Model: "m", KeyID: "a", Key: key})
	observe(c, "p", "m", "a", responseError(401))
	key.AzureKeyConfig.ClientSecret.Val = "rotated-secret"
	if !c.syncKey("p", key) {
		t.Fatal("new nested credential not adopted")
	}
	if len(c.conditions) != 0 {
		t.Fatal("old hold survived credential rotation")
	}
	a.Observe(nil, responseError(401))
	a.End()
	if len(c.conditions) != 0 {
		t.Fatal("old attempt restored the previous credential's hold")
	}
}

func TestAdaptiveHistoryOffKeepsDurableConditions(t *testing.T) {
	c, _ := providerController()
	s := newSettingsStore(t)
	attachStore(t, c, s)
	cfg := c.Config()
	cfg.HistoryEnabled = false
	c.Apply(cfg)
	observe(c, "p1", "m", "a", responseError(401))
	if err := c.maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	page, err := c.ListIncidents(context.Background(), IncidentQuery{Scope: "key"})
	if err != nil || len(page.Incidents) != 1 || page.Incidents[0].ClosedAt != nil {
		t.Fatalf("history switch destroyed unresolved lifecycle: %+v %v", page, err)
	}
	if len(c.conditions) != 1 {
		t.Fatal("history switch released a held key")
	}
}

func TestAdaptiveDeletedHealthyProviderDoesNotPoisonRestart(t *testing.T) {
	c, _ := providerController()
	s := newSettingsStore(t)
	attachStore(t, c, s)
	removed := false
	setCatalog := func(target *Controller) {
		target.SetCatalog(nil, func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) {
			if removed {
				return nil, fmt.Errorf("provider no longer exists")
			}
			return testKeys, nil
		}, nil)
		target.SetProviderExists(func(schemas.ModelProvider) bool { return !removed })
	}
	setCatalog(c)
	observe(c, "p1", "m", "a", nil)
	if err := c.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.ResetProvider("p1"); err != nil {
		t.Fatal(err)
	}
	removed = true
	loaded, _ := testController()
	attachStore(t, loaded, s)
	setCatalog(loaded)
	if err := loaded.Restore(context.Background()); err != nil {
		t.Fatalf("deleted provider broke restart: %v", err)
	}
	if len(loaded.Status()) != 0 {
		t.Fatal("deleted provider's health was restored")
	}
}

func TestAdaptiveProviderEditFailedResetReconcilesAfterReopen(t *testing.T) {
	for _, status := range []int{401, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "config.db")
			open := func() configstore.ConfigStore {
				s, err := configstore.NewConfigStore(ctx, &configstore.Config{Enabled: true, Type: configstore.ConfigStoreTypeSQLite, Config: &configstore.SQLiteConfig{Path: path}}, bifrost.NewDefaultLogger(schemas.LogLevelError))
				if err != nil {
					t.Fatal(err)
				}
				return s
			}
			s := open()
			native := configstore.ProviderConfig{Keys: []schemas.Key{testKeys[0]}, NetworkConfig: &schemas.NetworkConfig{BaseURL: "https://old.example"}}
			if err := s.AddProvider(ctx, "p1", native); err != nil {
				t.Fatal(err)
			}
			load := func(s configstore.ConfigStore) (*Controller, string) {
				cfg := DefaultConfig()
				c, err := NewFromStore(ctx, s, &cfg)
				if err != nil {
					t.Fatal(err)
				}
				native, err := s.GetProviderConfig(ctx, "p1")
				if err != nil {
					t.Fatal(err)
				}
				c.SetCatalog(nil, func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) { return native.Keys, nil }, nil)
				revision := c.ProviderConfigured("p1", &schemas.ProviderConfig{NetworkConfig: *native.NetworkConfig})
				if err := c.Restore(ctx); err != nil {
					t.Fatal(err)
				}
				return c, revision
			}
			c, revision := load(s)
			info := schemas.ProviderAttemptInfo{Provider: "p1", ProviderRevision: revision, Model: "m", KeyID: "a"}
			a := c.BeginProviderAttempt(testContext(), info)
			a.Observe(nil, responseError(status))
			a.End()
			before, err := c.ListIncidents(ctx, IncidentQuery{})
			if err != nil || len(before.Incidents) == 0 {
				t.Fatalf("no incident before edit: %+v %v", before, err)
			}
			if err := c.Checkpoint(ctx); err != nil {
				t.Fatal(err)
			}
			oldAttempt := c.BeginProviderAttempt(testContext(), info)
			// Native config commits independently of the following failed adaptive reset.
			native.NetworkConfig = &schemas.NetworkConfig{BaseURL: "https://new.example"}
			if err := s.UpdateProvider(ctx, "p1", native); err != nil {
				t.Fatal(err)
			}
			c.ProviderConfigured("p1", &schemas.ProviderConfig{NetworkConfig: *native.NetworkConfig})
			if err := s.DB().Exec("CREATE TRIGGER fail_adaptive_commit BEFORE UPDATE ON adaptive_routing_state BEGIN SELECT RAISE(ABORT, 'write unavailable'); END").Error; err != nil {
				t.Fatal(err)
			}
			if err := c.ResetProvider("p1"); err == nil {
				t.Fatal("expected reset failure")
			}
			oldAttempt.Observe(nil, responseError(401))
			oldAttempt.End()
			if c.BeginProviderAttempt(testContext(), info) != nil {
				t.Fatal("retired worker was attributed to the new endpoint")
			}
			if err := s.DB().Exec("DROP TRIGGER fail_adaptive_commit").Error; err != nil {
				t.Fatal(err)
			}
			// Simulate process loss: close SQLite without controller maintenance/checkpoint.
			if err := s.Close(ctx); err != nil {
				t.Fatal(err)
			}
			s = open()
			defer s.Close(ctx)
			loaded, _ := load(s)
			if len(loaded.Status()) != 0 || len(loaded.conditions) != 0 {
				t.Fatal("old endpoint observations survived reopen")
			}
			after, err := loaded.ListIncidents(ctx, IncidentQuery{})
			if err != nil || len(after.Incidents) != len(before.Incidents) {
				t.Fatalf("incident history lost: %+v %v", after, err)
			}
			for _, i := range after.Incidents {
				if i.Status != "no_longer_observed" || i.ClosedAt == nil || i.ResolvedAt != nil {
					t.Fatalf("configuration edit claimed recovery: %+v", i)
				}
			}
		})
	}
}

func TestAdaptiveFailedConfigurationResetRetriesDuringMaintenance(t *testing.T) {
	c, _ := providerController()
	s := newSettingsStore(t)
	attachStore(t, c, s)
	observe(c, "p1", "m", "a", responseError(401))
	if err := s.DB().Exec("CREATE TRIGGER fail_adaptive_commit BEFORE UPDATE ON adaptive_routing_state BEGIN SELECT RAISE(ABORT, 'write unavailable'); END").Error; err != nil {
		t.Fatal(err)
	}
	if err := c.ResetKey("p1", "a"); err == nil {
		t.Fatal("expected reset failure")
	}
	if c.PersistenceStatus().Healthy || len(c.conditions) == 0 {
		t.Fatal("failed reset hid persistence failure or erased the committed hold")
	}
	if err := s.DB().Exec("DROP TRIGGER fail_adaptive_commit").Error; err != nil {
		t.Fatal(err)
	}
	if err := c.maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.conditions) != 0 || len(c.pendingResets) != 0 || !c.PersistenceStatus().Healthy {
		t.Fatal("maintenance did not finish configuration reconciliation")
	}
}

func TestAdaptiveNativeKeyRoundTripPreservesHold(t *testing.T) {
	var key schemas.Key
	if err := json.Unmarshal([]byte(`{"id":"failure-key","name":"Failure key","value":"sk-fixture","models":["test-model","unavailable-model"],"weight":1}`), &key); err != nil {
		t.Fatal(err)
	}
	s := newSettingsStore(t)
	if err := s.AddProvider(context.Background(), "p1", configstore.ProviderConfig{Keys: []schemas.Key{key}}); err != nil {
		t.Fatal(err)
	}
	stored, err := s.GetProviderConfig(context.Background(), "p1")
	if err != nil {
		t.Fatal(err)
	}
	if keyHash(key) != keyHash(stored.Keys[0]) {
		left, right := reflect.ValueOf(normalizeKey(key)), reflect.ValueOf(normalizeKey(stored.Keys[0]))
		for i := 0; i < left.NumField(); i++ {
			if !reflect.DeepEqual(left.Field(i).Interface(), right.Field(i).Interface()) {
				t.Errorf("native roundtrip changed key identity field %s", left.Type().Field(i).Name)
			}
		}
	}
	c, _ := testController()
	attachStore(t, c, s)
	c.SetCatalog(nil, func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) { return []schemas.Key{key}, nil }, nil)
	observe(c, "p1", "test-model", key.ID, responseError(429))
	loaded, _ := testController()
	attachStore(t, loaded, s)
	loaded.SetCatalog(nil, func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) { return stored.Keys, nil }, nil)
	if err := loaded.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(loaded.conditions) != 1 {
		t.Fatal("native default materialization released the persisted hold")
	}
}

func TestAdaptivePartialHoldDegradesProviderAndDefersDiagnostic(t *testing.T) {
	c, _ := providerController()
	observe(c, "p1", "m", "a", responseError(401))
	// A whole-key refusal applies to another model even when no failing
	// attempt was observed on that model. Its healthy sibling still serves.
	c.lifeMu.Lock()
	served := make(chan struct{})
	go func() {
		observe(c, "p1", "new-model", "b", nil)
		close(served)
	}()
	select {
	case <-served:
		c.lifeMu.Unlock()
	case <-time.After(time.Second):
		c.lifeMu.Unlock()
		<-served
		t.Fatal("healthy attempt waited for diagnostic persistence")
	}
	c.mu.Lock()
	pending := len(c.pendingDiagnostics)
	c.mu.Unlock()
	if pending == 0 {
		t.Fatal("partial hold diagnostic was lost instead of deferred")
	}
	c.flushDiagnostics()
	i := c.incidentLocked(routeID{Provider: "p1", Model: "new-model"})
	if i == nil || i.State != "degraded" {
		t.Fatalf("partial provider failure not recorded: %+v", i)
	}
}

func TestAdaptiveReholdResumesAndResetsLadder(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		nextModel string
	}{
		{"whole-key", 401, "other"},
		{"model", 429, "m"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, now := testController()
			id := routeID{Provider: "p1", Model: "m", KeyID: "a"}
			refuse := func(model string) { observe(c, "p1", model, "a", responseError(tc.status)) }
			refuse("m")
			h := c.holdLocked(id, *now)
			*now = h.HoldUntil.Add(time.Second)
			refuse("m")
			h = c.holdLocked(id, *now)
			if h.Rung != 2 {
				t.Fatalf("first failed check rung=%d", h.Rung)
			}
			*now = h.HoldUntil.Add(time.Second)
			observe(c, "p1", "m", "a", nil)
			*now = now.Add(time.Minute)
			refuse(tc.nextModel)
			h = c.holdLocked(routeID{Provider: "p1", Model: tc.nextModel, KeyID: "a"}, *now)
			if h == nil || h.Rung != 2 {
				t.Fatalf("flapping hold reset its ladder: %+v", h)
			}
			if err := c.Release(context.Background(), "p1", "a", ""); err != nil {
				t.Fatal(err)
			}
			refuse("m")
			h = c.holdLocked(id, *now)
			if h == nil || h.Rung != 1 {
				t.Fatalf("operator release did not reset ladder: %+v", h)
			}
			*now = h.HoldUntil.Add(time.Second)
			refuse("m")
			h = c.holdLocked(id, *now)
			*now = h.HoldUntil.Add(time.Second)
			observe(c, "p1", "m", "a", nil)
			*now = now.Add(11 * time.Minute)
			refuse("m")
			h = c.holdLocked(id, *now)
			if h == nil || h.Rung != 1 {
				t.Fatalf("expired release window retained ladder: %+v", h)
			}
		})
	}
}

func TestAdaptiveRecentReleaseMemoryIsBounded(t *testing.T) {
	c, now := testController()
	observe(c, "p1", "m", "a", responseError(401))
	held := c.holdLocked(routeID{Provider: "p1", Model: "m", KeyID: "a"}, *now)
	for i := range maxRoutes + 1 {
		c.rememberReleasedLocked(&condition{
			routeID:   routeID{Provider: "p2", Model: fmt.Sprint(i), KeyID: "b"},
			HoldClass: schemas.FailureClassRateLimit, Rung: 2, KeyRevision: "revision",
		}, now.Add(time.Duration(i)*time.Millisecond))
	}
	if len(c.releasedHolds) != maxRoutes {
		t.Fatalf("unbounded recent releases: %d", len(c.releasedHolds))
	}
	if _, exists := c.releasedHolds[routeID{Provider: "p2", Model: "0", KeyID: "b"}]; exists {
		t.Fatal("oldest release was retained")
	}
	if got := c.reholdRungLocked(routeID{Provider: "p2", Model: fmt.Sprint(maxRoutes), KeyID: "b"}, schemas.FailureClassRateLimit, "revision", now.Add(time.Minute)); got != 2 {
		t.Fatalf("newest release lost its rung: %d", got)
	}
	if c.holdLocked(routeID{Provider: "p1", Model: "m", KeyID: "a"}, *now) != held {
		t.Fatal("disposable history evicted an active hold")
	}
}
