package adaptive

import (
	"context"
	"testing"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/kvstore"
)

func TestAdaptiveEEChoosesHealthiestProviderAndWeightsKeys(t *testing.T) {
	c, _ := providerController()
	for _, key := range testKeys {
		observe(c, "p1", "m", key.ID, responseError(503))
		observe(c, "p2", "m", key.ID, nil)
	}
	for range 100 {
		req := routeRequest("")
		if err := c.Route(testContext(), req); err != nil {
			t.Fatal(err)
		}
		p, _, f := req.GetRequestFields()
		if p != "p2" || len(f) != 1 || f[0].Provider != "p1" {
			t.Fatalf("healthiest provider not selected: %s %+v", p, f)
		}
	}
	// Unlike provider ranking, usable keys keep a share of traffic.
	chosen := map[string]int{}
	for range 500 {
		k, err := c.SelectKey(testContext(), testKeys, "p1", "m")
		if err != nil {
			t.Fatal(err)
		}
		chosen[k.ID]++
	}
	if chosen["a"] == 0 || chosen["b"] == 0 {
		t.Fatalf("usable key starved: %v", chosen)
	}
}

func TestAdaptiveEESelfLatencyDegradesEvenWithoutPeer(t *testing.T) {
	c, _ := testController()
	latency := func(ms int64) {
		a := c.BeginProviderAttempt(testContext(), schemas.ProviderAttemptInfo{Provider: "p1", Model: "m", KeyID: "a"})
		a.Observe(&schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{ExtraFields: schemas.BifrostResponseExtraFields{Latency: ms}}}, nil)
		a.End()
	}
	for range 5 {
		latency(100)
	}
	before := c.Status()[0]
	for range 3 {
		latency(1000)
	}
	after := c.Status()[0]
	if before.State != "healthy" || after.State != "degraded" || after.ErrorRate != 0 || after.Score >= before.Score {
		t.Fatalf("latency degradation not reflected in state and score: before=%+v after=%+v", before, after)
	}
	for range 40 {
		latency(100)
	}
	if got := c.Status()[0]; got.State != "healthy" || got.Score <= after.Score {
		t.Fatalf("latency did not recover: %+v", got)
	}
}

func TestAdaptiveEEPartialHoldDegradesProviderAndDefersDiagnostic(t *testing.T) {
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

func TestAdaptiveEEReholdResumesAndResetsLadder(t *testing.T) {
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

func TestAdaptiveEENativeAffinityCannotKeepFailedRouteOrKey(t *testing.T) {
	c, now := providerController()
	kv, err := kvstore.New(kvstore.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	native := bifrost.NewSessionAffinity(kv, bifrost.NewDefaultLogger(schemas.LogLevelError))
	affinity := c.SessionAffinity(native)
	ctx := func() *schemas.BifrostContext {
		v := testContext()
		v.SetValue(schemas.BifrostContextKeySessionID, "adaptive-session")
		return v
	}
	requested := schemas.Route{Model: "m"}
	p1 := schemas.Route{Provider: "p1", Model: "m"}
	p2 := schemas.Route{Provider: "p2", Model: "m"}
	chain := []schemas.Route{p2, p1}
	routeBinding := bifrost.SessionStateKey(ctx(), bifrost.SessionStateKindRoute, "", "m")
	keyBinding := bifrost.SessionStateKey(ctx(), bifrost.SessionStateKindKey, "p1", "m")
	if err := kv.SetWithTTL(routeBinding, bifrost.RouteStateValue(p1), time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := kv.SetWithTTL(keyBinding, "a", time.Hour); err != nil {
		t.Fatal(err)
	}
	if selected := affinity.ResolveRoute(ctx(), requested, chain); selected[0] != p1 {
		t.Fatal("healthy route affinity lost")
	}
	if key, ok := affinity.ResolveKey(ctx(), "p1", "m", testKeys); !ok || key.ID != "a" {
		t.Fatal("healthy key affinity lost")
	}
	observe(c, "p1", "m", "a", responseError(401))
	if key, ok := affinity.ResolveKey(ctx(), "p1", "m", testKeys); ok && key.ID == "a" {
		t.Fatal("held key retained by affinity")
	}
	if _, found := bifrost.SessionStateString(kv, keyBinding); found {
		t.Fatal("native binding not cleared")
	}
	observe(c, "p1", "m", "b", responseError(401))
	if selected := affinity.ResolveRoute(ctx(), requested, chain); selected[0] != p2 || len(selected) != 2 {
		t.Fatalf("failed route promoted or fallback pruned: %+v", selected)
	}
	if _, found := bifrost.SessionStateString(kv, routeBinding); found {
		t.Fatal("native failed route binding not cleared")
	}
	// A healthy session cannot steal a due real recovery check.
	if err := kv.SetWithTTL(routeBinding, bifrost.RouteStateValue(p2), time.Hour); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Minute)
	probeCtx := ctx()
	req := routeRequest("")
	if err := c.Route(probeCtx, req); err != nil {
		t.Fatal(err)
	}
	provider, _, _ := req.GetRequestFields()
	if provider != "p1" {
		t.Fatalf("due recovery not selected: %s", provider)
	}
	if selected := affinity.ResolveRoute(probeCtx, requested, []schemas.Route{p1, p2}); selected[0] != p1 {
		t.Fatal("affinity displaced reserved check")
	}
	if _, ok := affinity.ResolveKey(probeCtx, "p1", "m", testKeys); ok {
		t.Fatal("affinity bypassed reserved key check")
	}
}
