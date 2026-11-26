package adaptive

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
)

func TestAdaptiveAdmissionCannotAdoptReplacementCredential(t *testing.T) {
	c, now := testController()
	old := schemas.Key{ID: "a", Value: *schemas.NewSecretVar("old")}
	live := old
	c.SetCatalog(nil, func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) { return []schemas.Key{live}, nil }, nil)
	if !c.syncKey("p", old) {
		t.Fatal("initial credential was not accepted")
	}
	// Pause at the gap after syncKey's fast path, before attempt admission.
	c.now = func() time.Time {
		c.now = func() time.Time { return *now }
		live.Value = *schemas.NewSecretVar("replacement")
		if !c.syncKey("p", live) {
			t.Fatal("replacement credential was not accepted")
		}
		return *now
	}
	a := c.BeginProviderAttempt(testContext(), schemas.ProviderAttemptInfo{Provider: "p", Model: "m", KeyID: "a", Key: old})
	if a != nil {
		a.Observe(nil, responseError(401))
		a.End()
	}
	if len(c.conditions) != 0 {
		t.Fatal("old credential's admission acquired the replacement's state")
	}
	observe(c, "p", "m", "a", nil)
	if got := c.Status(); len(got) != 1 || got[0].State != "healthy" {
		t.Fatalf("replacement credential inherited stale health: %+v", got)
	}
}

func TestAdaptiveWholeKeyCheckReplacesExistingModelHold(t *testing.T) {
	c, now := providerController()
	attachStore(t, c, newSettingsStore(t))
	observe(c, "p1", "model-a", "a", responseError(429))
	modelHold := *c.conditions[routeID{"p1", "model-a", "a"}]
	observe(c, "p1", "model-b", "a", responseError(401))
	*now = now.Add(time.Minute)
	ctx := testContext()
	key, err := c.SelectKey(ctx, testKeys, "p1", "model-a")
	if err != nil || key.ID != "a" {
		t.Fatalf("whole-key check not selected: %s %v", key.ID, err)
	}
	observeWithContext(c, ctx, "p1", "model-a", "a", responseError(429))
	state, err := c.stateStore.Load(context.Background())
	if err != nil || len(state.Conditions) != 1 || !c.PersistenceStatus().Healthy {
		t.Fatalf("masked model hold was not atomically replaced: %+v %v %+v", state.Conditions, err, c.PersistenceStatus())
	}
	h := state.Conditions[0]
	if h.Model != "model-a" || h.IncidentID != modelHold.IncidentID || h.ID == modelHold.ID {
		t.Fatalf("replacement lost scope, incident or new condition identity: %+v", h)
	}
	page, err := c.ListIncidents(context.Background(), IncidentQuery{Scope: "key"})
	if err != nil || len(page.Incidents) != 1 || page.Incidents[0].State != "recovering" || page.Incidents[0].Checks != 1 {
		t.Fatalf("whole-key release and completed check not recorded: %+v %v", page, err)
	}
}

func TestAdaptiveDisabledRestartRestoresLifecycleRevision(t *testing.T) {
	ctx := context.Background()
	s := newSettingsStore(t)
	first, err := NewFromStore(ctx, s, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.ResetProvider("p"); err != nil {
		t.Fatal(err)
	}
	loaded, err := NewFromStore(ctx, s, nil)
	if err != nil || loaded.Enabled() {
		t.Fatalf("restart unexpectedly enabled selection: %v", err)
	}
	if err := loaded.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := loaded.Save(ctx, DefaultConfig()); err != nil {
		t.Fatal(err)
	}
	observe(loaded, "p", "m", "a", responseError(401))
	state, err := loaded.stateStore.Load(ctx)
	if err != nil || len(state.Conditions) != 1 || !loaded.PersistenceStatus().Healthy {
		t.Fatalf("first refusal after enabling lost its durable revision: %+v %v %+v", state.Conditions, err, loaded.PersistenceStatus())
	}
}

func TestAdaptiveTelemetryRemovalClosesOnlyOptionalObservations(t *testing.T) {
	for _, mode := range []string{"idle", "eviction"} {
		t.Run(mode, func(t *testing.T) {
			c, now := testController()
			attachStore(t, c, newSettingsStore(t))
			observe(c, "p", "old", "a", responseError(503))
			id := c.incidents[routeID{"p", "old", "a"}].ID
			*now = now.Add(time.Second)
			observe(c, "p", "held", "b", responseError(401))
			if mode == "idle" {
				*now = now.Add(25 * time.Hour)
			} else {
				for n := 0; n < maxRoutes; n++ {
					*now = now.Add(time.Millisecond)
					observe(c, "p", fmt.Sprintf("fresh-%d", n), "a", nil)
				}
			}
			if err := c.maintain(context.Background()); err != nil {
				t.Fatal(err)
			}
			detail, err := c.GetIncident(context.Background(), id, "", 10)
			if err != nil || detail.Incident.Status != "no_longer_observed" || detail.Incident.ClosedAt == nil || detail.Incident.ResolvedAt != nil {
				t.Fatalf("removed telemetry left an open observation or claimed recovery: %+v %v", detail, err)
			}
			if len(c.incidents) != 1 || len(c.conditions) != 1 {
				t.Fatalf("optional observations or required holds leaked: incidents=%d conditions=%d", len(c.incidents), len(c.conditions))
			}
		})
	}
}

func TestAdaptiveOptionalIncidentAdmissionIsBounded(t *testing.T) {
	c, _ := testController()
	if !c.syncKey("p", schemas.Key{ID: "a"}) {
		t.Fatal("initial credential was not accepted")
	}
	// Seed a full cache as a restore can supply it, then exercise actual
	// attempt admission. Replaying thousands of evictions is unnecessary.
	for n := 0; n < maxOptionalIncidents; n++ {
		id := routeID{"p", fmt.Sprintf("failed-%d", n), "a"}
		i := c.newIncidentLocked(id, "a", c.now(), "")
		setIncidentState(i, "degraded", c.now(), "")
		c.incidents[id] = i
	}
	observe(c, "p", "new-failure", "a", responseError(503))
	if len(c.incidents) > maxOptionalIncidents {
		t.Fatalf("optional incident cache exceeded its bound: %d", len(c.incidents))
	}
	observe(c, "p", "required", "b", responseError(401))
	if c.conditions[routeID{Provider: "p", KeyID: "b"}] == nil {
		t.Fatal("optional history pressure suppressed a required hold")
	}
	if err := c.maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.incidents) > len(c.routes)+1 {
		t.Fatalf("evicted observations remained open: %d routes, %d incidents", len(c.routes), len(c.incidents))
	}
}

func TestAdaptiveUnchangedMaintenancePreservesCheckpoint(t *testing.T) {
	c, now := providerController()
	attachStore(t, c, newSettingsStore(t))
	observe(c, "p1", "m", "a", responseError(401))
	for _, phase := range []string{"held", "waiting_for_traffic"} {
		if phase == "waiting_for_traffic" {
			if err := c.Release(context.Background(), "p1", "a", ""); err != nil {
				t.Fatal(err)
			}
			*now = now.Add(2 * time.Hour)
			if err := c.maintain(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if err := c.Checkpoint(context.Background()); err != nil {
			t.Fatal(err)
		}
		before, err := c.stateStore.Load(context.Background())
		if err != nil || len(before.Checkpoint) == 0 {
			t.Fatalf("checkpoint missing: %v", err)
		}
		if err := c.maintain(context.Background()); err != nil {
			t.Fatal(err)
		}
		after, err := c.stateStore.Load(context.Background())
		if err != nil || before.Revision != after.Revision || !bytes.Equal(before.Checkpoint, after.Checkpoint) {
			t.Fatalf("unchanged %s maintenance advanced lifecycle or discarded checkpoint: %d -> %d, %v", phase, before.Revision, after.Revision, err)
		}
	}
}

func TestAdaptiveRecoveryIdentitySurvivesWithoutCheckpoint(t *testing.T) {
	for _, status := range []int{401, 429} {
		for _, rotated := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/rotated=%t", status, rotated), func(t *testing.T) {
				c, _ := testController()
				s := newSettingsStore(t)
				attachStore(t, c, s)
				live := schemas.Key{ID: "a", Value: *schemas.NewSecretVar("old")}
				catalog := func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) { return []schemas.Key{live}, nil }
				c.SetCatalog(nil, catalog, nil)
				observe(c, "p", "m", "a", responseError(status))
				scope := routeID{Provider: "p", KeyID: "a"}
				if status == 429 {
					scope.Model = "m"
				}
				id := c.incidents[scope].ID
				if err := c.Release(context.Background(), "p", "a", ""); err != nil {
					t.Fatal(err)
				}
				if rotated {
					live.Value = *schemas.NewSecretVar("new")
				}
				loaded, _ := testController()
				attachStore(t, loaded, s)
				loaded.SetCatalog(nil, catalog, nil)
				if err := loaded.Restore(context.Background()); err != nil {
					t.Fatal(err)
				}
				observe(loaded, "p", "m", "a", nil)
				detail, err := loaded.GetIncident(context.Background(), id, "", 20)
				if err != nil {
					t.Fatal(err)
				}
				if rotated {
					if detail.Incident.Status != "no_longer_observed" || detail.Incident.ClosedAt == nil || detail.Incident.ResolvedAt != nil {
						t.Fatalf("replacement inherited old recovery: %+v", detail.Incident)
					}
				} else if detail.Incident.Status != "recovering" || detail.Incident.ClosedAt != nil {
					t.Fatalf("same key lost incomplete recovery: %+v", detail.Incident)
				}
			})
		}
	}
}

func TestAdaptiveProviderIncidentRejectsStaleCatalog(t *testing.T) {
	c, now := testController()
	attachStore(t, c, newSettingsStore(t))
	live := testKeys[:1]
	inject := false
	c.SetCatalog(nil, func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) {
		snapshot := live
		if inject {
			inject = false
			live = testKeys[1:]
			if err := c.ResetProvider("p"); err != nil {
				t.Fatal(err)
			}
			for range 3 {
				observe(c, "p", "m", "b", responseError(503))
			}
		}
		return snapshot, nil
	}, func(schemas.ModelProvider, schemas.Key, string) bool { return true })
	for range 3 {
		observe(c, "p", "m", "a", responseError(503))
	}
	a := c.BeginProviderAttempt(testContext(), schemas.ProviderAttemptInfo{Provider: "p", Model: "m", KeyID: "a"})
	defer a.End()
	inject = true
	c.updateProviderIncident(a.(*attempt), "old-request", *now)
	i := c.incidents[routeID{Provider: "p", Model: "m"}]
	if i == nil || i.State != "failed" || i.ClosedAt != nil {
		t.Fatalf("stale removed-key snapshot resolved the current provider incident: %+v", i)
	}
}

func TestAdaptiveFailedProbeCommitReleasesOnlyItsLease(t *testing.T) {
	c, now := providerController()
	s := newSettingsStore(t)
	attachStore(t, c, s)
	observe(c, "p1", "m", "a", responseError(401))
	*now = now.Add(time.Minute)
	ctx := testContext()
	key, err := c.SelectKey(ctx, testKeys, "p1", "m")
	if err != nil || key.ID != "a" {
		t.Fatalf("probe not selected: %s %v", key.ID, err)
	}
	if err := s.DB().Exec("CREATE TRIGGER fail_adaptive_commit BEFORE UPDATE ON adaptive_routing_state BEGIN SELECT RAISE(ABORT, 'write unavailable'); END").Error; err != nil {
		t.Fatal(err)
	}
	observeWithContext(c, ctx, "p1", "m", "a", responseError(401))
	key, err = c.SelectKey(testContext(), testKeys, "p1", "m")
	if err != nil || key.ID != "a" {
		t.Fatalf("completed failed commit retained its in-flight lease: %s %v", key.ID, err)
	}
	state, err := c.stateStore.Load(context.Background())
	if err != nil || len(state.Conditions) != 1 || c.PersistenceStatus().Healthy {
		t.Fatalf("failed probe altered committed hold or hid storage error: %+v %v", state.Conditions, err)
	}
	for _, i := range state.OpenIncidents {
		if i.Checks != 0 {
			t.Fatal("uncommitted probe counted as a completed check")
		}
	}
}

func TestAdaptiveKeylessCheckpointPreservesHealth(t *testing.T) {
	c, _ := testController()
	s := newSettingsStore(t)
	attachStore(t, c, s)
	configure := func(target *Controller) string {
		target.SetCatalog(nil, func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) { return nil, nil }, func(schemas.ModelProvider, schemas.Key, string) bool { return true }, func(schemas.ModelProvider) bool { return true })
		return target.ProviderConfigured("p", &schemas.ProviderConfig{})
	}
	revision := configure(c)
	for range 3 {
		a := c.BeginProviderAttempt(testContext(), schemas.ProviderAttemptInfo{Provider: "p", ProviderRevision: revision, Model: "m"})
		a.Observe(nil, responseError(503))
		a.End()
	}
	if err := c.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	loaded, _ := testController()
	attachStore(t, loaded, s)
	configure(loaded)
	if err := loaded.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := loaded.Status(); len(got) != 1 || got[0].State != "failed" || got[0].Samples != 3 {
		t.Fatalf("keyless checkpoint was treated as a managed credential: %+v", got)
	}
	changed, _ := testController()
	attachStore(t, changed, s)
	configure(changed)
	changed.ProviderConfigured("p", &schemas.ProviderConfig{NetworkConfig: schemas.NetworkConfig{BaseURL: "https://replacement.example"}})
	if err := changed.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := changed.Status(); len(got) != 0 {
		t.Fatalf("keyless replacement inherited the old endpoint's checkpoint: %+v", got)
	}
}

func TestAdaptiveConcurrentAttemptNames(t *testing.T) {
	c, _ := testController()
	var wg sync.WaitGroup
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				a := c.BeginProviderAttempt(testContext(), schemas.ProviderAttemptInfo{Provider: "p", Model: "m", KeyID: "a", KeyName: fmt.Sprintf("name-%d", i)})
				a.Observe(nil, responseError(503))
				a.End()
			}
		}()
	}
	wg.Wait()
}

func TestAdaptiveKeyTransientRecoveryReservesSingleCheck(t *testing.T) {
	c, now := testController()
	for range 3 {
		observe(c, "p", "m", "a", responseError(503))
	}
	*now = now.Add(time.Minute)
	ctx := testContext()
	key, err := c.SelectKey(ctx, testKeys, "p", "m")
	if err != nil || key.ID != "a" {
		t.Fatalf("due failed key was not checked: %s %v", key.ID, err)
	}
	ordinary := testContext()
	key, err = c.SelectKey(ordinary, testKeys, "p", "m")
	if err != nil || key.ID != "a" && key.ID != "b" || ordinary.Value(probeContextKey{}) != nil {
		t.Fatalf("ordinary recovery traffic duplicated a priority check: %s %v", key.ID, err)
	}
	if c.weightLocked(routeID{"p", "m", "a"}, *now) <= 0 {
		t.Fatal("expired transient cooldown still excluded ordinary recovery traffic")
	}
	observeWithContext(c, ctx, "p", "m", "a", nil)
	if c.weightLocked(routeID{"p", "m", "a"}, *now) == 0 {
		t.Fatal("successful key check did not restore ordinary traffic")
	}
}

func TestAdaptiveIncidentUpdateCannotCrossManualRelease(t *testing.T) {
	c, now := testController()
	c.SetCatalog(nil, func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) { return testKeys[:1], nil }, func(schemas.ModelProvider, schemas.Key, string) bool { return true })
	observe(c, "p", "m", "a", responseError(401))
	a := c.BeginProviderAttempt(testContext(), schemas.ProviderAttemptInfo{Provider: "p", Model: "m", KeyID: "a"}).(*attempt)
	defer a.End()
	// The request's observation was accepted before the release; both later
	// incident writers must honor the same fence after that operator action.
	if err := c.Release(context.Background(), "p", "a", ""); err != nil {
		t.Fatal(err)
	}
	before := c.revision
	c.updateHealthIncident(a, "old-request", *now)
	c.updateProviderIncident(a, "old-request", *now)
	if c.revision != before {
		t.Fatal("an old completion wrote incident transitions after manual release")
	}
	for _, i := range c.incidents {
		if i.LastRequestID == "old-request" {
			t.Fatal("an old completion was attached to a newer lifecycle")
		}
	}
}

func TestAdaptiveSupersededTransientProbeCannotChangeHealth(t *testing.T) {
	for _, oldStatus := range []int{200, 503} {
		for _, currentStatus := range []int{0, 200, 503} {
			t.Run(fmt.Sprintf("old=%d/current=%d", oldStatus, currentStatus), func(t *testing.T) {
				c, now := providerController()
				for range 3 {
					observe(c, "p1", "m", "a", responseError(503))
				}
				beginCheck := func(request string) schemas.ProviderAttempt {
					t.Helper()
					ctx := testContext()
					ctx.SetValue(schemas.BifrostContextKeyRequestID, request)
					key, err := c.SelectKey(ctx, testKeys, "p1", "m")
					if err != nil || key.ID != "a" {
						t.Fatalf("due transient check was not selected: %s %v", key.ID, err)
					}
					a := c.BeginProviderAttempt(ctx, schemas.ProviderAttemptInfo{Provider: "p1", Model: "m", KeyID: "a"})
					if a == nil {
						t.Fatal("selected check was not observed")
					}
					return a
				}
				*now = now.Add(time.Minute)
				old := beginCheck("old-check")
				defer old.End()
				*now = now.Add(6 * time.Minute)
				current := beginCheck("current-check")
				defer current.End()
				if currentStatus != 0 {
					if currentStatus == 200 {
						current.Observe(&schemas.BifrostResponse{}, nil)
					} else {
						current.Observe(nil, responseError(currentStatus))
					}
					current.End()
				}
				before := c.Status()[0]
				revision := c.revision
				id := routeID{"p1", "m", "a"}
				probeID := c.routes[id].ProbeID
				*now = now.Add(time.Second)
				if oldStatus == 200 {
					old.Observe(&schemas.BifrostResponse{}, nil)
				} else {
					old.Observe(nil, responseError(oldStatus))
				}
				old.End()
				after := c.Status()[0]
				if after.State != before.State || before.Score == 0 && after.Score != 0 || after.Samples != before.Samples || after.ErrorRate != before.ErrorRate || after.LatencyMS != before.LatencyMS || !after.LastSeen.Equal(before.LastSeen) || after.Inflight != before.Inflight-1 {
					t.Fatalf("superseded check changed current health: before=%+v after=%+v", before, after)
				}
				if c.revision != revision || c.routes[id].ProbeID != probeID {
					t.Fatal("superseded check changed lifecycle or the current lease")
				}
				for _, i := range c.incidents {
					if i.LastRequestID == "old-check" {
						t.Fatal("superseded check was attached to a current incident")
					}
				}
				if currentStatus == 0 {
					current.Observe(&schemas.BifrostResponse{}, nil)
					current.End()
					if got := c.Status()[0]; got.Score == 0 || got.State != "recovering" || got.Samples != before.Samples+1 || got.Inflight != 0 {
						t.Fatalf("current successful check did not restore health: %+v", got)
					}
				}
			})
		}
	}
}

func TestAdaptiveDelayedProbeAdmissionKeepsSelectedGeneration(t *testing.T) {
	for _, refusal := range []int{401, 503} {
		for _, currentStatus := range []int{0, 200, refusal} {
			t.Run(fmt.Sprintf("refusal=%d/current=%d", refusal, currentStatus), func(t *testing.T) {
				c, now := providerController()
				for range 3 {
					observe(c, "p1", "m", "a", responseError(refusal))
				}
				selectCheck := func() *schemas.BifrostContext {
					t.Helper()
					ctx := testContext()
					key, err := c.SelectKey(ctx, testKeys, "p1", "m")
					if err != nil || key.ID != "a" {
						t.Fatalf("due check was not selected: %s %v", key.ID, err)
					}
					return ctx
				}
				*now = now.Add(time.Minute)
				oldCtx := selectCheck()
				*now = now.Add(6 * time.Minute)
				currentCtx := selectCheck()
				info := schemas.ProviderAttemptInfo{Provider: "p1", Model: "m", KeyID: "a"}
				current := c.BeginProviderAttempt(currentCtx, info)
				if current == nil {
					t.Fatal("current check was not observed")
				}
				defer current.End()
				if currentStatus != 0 {
					if currentStatus == 200 {
						current.Observe(&schemas.BifrostResponse{}, nil)
					} else {
						current.Observe(nil, responseError(currentStatus))
					}
					current.End()
				}
				before, revision := c.Status()[0], c.revision
				*now = now.Add(time.Second)
				// Core's actual dispatch is unaffected. Only the obsolete observer
				// must be rejected instead of acquiring the current generation.
				if old := c.BeginProviderAttempt(oldCtx, info); old != nil {
					old.Observe(&schemas.BifrostResponse{}, nil)
					old.End()
				}
				after := c.Status()[0]
				if !reflect.DeepEqual(after, before) || c.revision != revision {
					t.Fatalf("delayed admission rebound obsolete selection to current state: before=%+v after=%+v", before, after)
				}
			})
		}
	}
}

func TestAdaptiveCompletedProbeContextObservesNextAttempt(t *testing.T) {
	c, now := providerController()
	for range 3 {
		observe(c, "p1", "m", "a", responseError(503))
	}
	*now = now.Add(time.Minute)
	ctx := testContext()
	key, err := c.SelectKey(ctx, testKeys, "p1", "m")
	if err != nil || key.ID != "a" {
		t.Fatalf("due check was not selected: %s %v", key.ID, err)
	}
	info := schemas.ProviderAttemptInfo{Provider: "p1", Model: "m", KeyID: "a"}
	check := c.BeginProviderAttempt(ctx, info)
	check.Observe(nil, responseError(503))
	check.End()
	// Native keyless/fixed-key retry paths reuse their context without another
	// adaptive key selection. A consumed reservation must not hide that attempt.
	next := c.BeginProviderAttempt(ctx, info)
	if next == nil {
		t.Fatal("completed probe reservation suppressed the next actual attempt")
	}
	next.Observe(&schemas.BifrostResponse{}, nil)
	next.End()
	if got := c.Status()[0]; got.Samples != 5 || got.Score <= 0 {
		t.Fatalf("next actual attempt was not observed: %+v", got)
	}
}

func TestAdaptiveTransientCheckCannotOverrideNewHold(t *testing.T) {
	c, now := providerController()
	for range 3 {
		observe(c, "p1", "m", "a", responseError(503))
	}
	*now = now.Add(time.Minute)
	// SelectKey and direction both capture this recovery pointer before the
	// reservation. A refusal can commit between those two local boundaries.
	r := c.routes[routeID{"p1", "m", "a"}]
	observe(c, "p1", "m", "a", responseError(401))
	ctx := testContext()
	if c.reserveProbe(ctx, direction{provider: "p1", model: "m", recover: r}) || ctx.Value(probeContextKey{}) != nil {
		t.Fatal("stale transient direction reserved a check over a new credential hold")
	}
	if h := c.holdLocked(r.routeID, *now); h == nil || h.ProbeID != 0 {
		t.Fatal("new credential hold acquired an unsolicited check")
	}
}

func TestAdaptiveRecoveryReplacementPreservesOrdinaryOutcomesAndProgress(t *testing.T) {
	c, now := providerController()
	keys := testKeys[:1]
	c.SetCatalog(c.providers, func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) { return keys, nil }, c.supports)
	for range 3 {
		observe(c, "p1", "m", "a", responseError(503))
	}
	beginCheck := func() schemas.ProviderAttempt {
		t.Helper()
		ctx := testContext()
		requireRecoveryProvider(t, c, ctx, "p1")
		key, err := c.SelectKey(ctx, keys, "p1", "m")
		if err != nil || key.ID != "a" {
			t.Fatalf("recovery key not selected: %s %v", key.ID, err)
		}
		a := c.BeginProviderAttempt(ctx, schemas.ProviderAttemptInfo{Provider: "p1", Model: "m", KeyID: key.ID})
		if a == nil {
			t.Fatal("real check was not observed")
		}
		return a
	}
	*now = now.Add(31 * time.Second)
	old := beginCheck()
	defer old.End()
	*now = now.Add(time.Second)
	ordinary := c.BeginProviderAttempt(testContext(), schemas.ProviderAttemptInfo{Provider: "p1", Model: "m", KeyID: "a"})
	defer ordinary.End()
	*now = now.Add(6 * time.Minute)
	current := beginCheck()
	defer current.End()
	current.Observe(&schemas.BifrostResponse{}, nil)
	current.End()
	ordinary.Observe(&schemas.BifrostResponse{}, nil)
	ordinary.End()
	id := routeID{"p1", "m", "a"}
	if got := c.routes[id]; got.Samples != 5 || got.Inflight != 1 {
		t.Fatalf("replacement discarded an ordinary real outcome: %+v", got)
	}
	// The obsolete physical request is still running. It cannot permanently
	// block the next admission after the current check has completed.
	*now = now.Add(31 * time.Second)
	next := beginCheck()
	defer next.End()
	before := *c.routes[id]
	old.Observe(nil, responseError(503))
	old.End()
	after := *c.routes[id]
	before.Inflight--
	if after != before {
		t.Fatalf("obsolete check affected current observations or admission: before=%+v after=%+v", before, after)
	}
	next.Observe(&schemas.BifrostResponse{}, nil)
	next.End()
	if got := c.routes[id]; got.Samples != 6 || got.Inflight != 0 {
		t.Fatalf("current check failed to advance recovery: %+v", got)
	}
}
