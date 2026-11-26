package adaptive

import (
	"context"
	"testing"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/kvstore"
)

func TestAdaptiveNativeAffinityCannotKeepFailedRouteOrKey(t *testing.T) {
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

func TestAdaptiveNativeAffinityKeepsHealthyBindingAfterFiltering(t *testing.T) {
	for _, adaptiveProviders := range []bool{true, false} {
		c, _ := providerController()
		cfg := c.Config()
		cfg.DirectionSelectionEnabled = adaptiveProviders
		if err := c.Apply(cfg); err != nil {
			t.Fatal(err)
		}
		kv, err := kvstore.New(kvstore.Config{})
		if err != nil {
			t.Fatal(err)
		}
		defer kv.Close()
		affinity := c.SessionAffinity(bifrost.NewSessionAffinity(kv, bifrost.NewDefaultLogger(schemas.LogLevelError)))
		ctx := testContext()
		ctx.SetValue(schemas.BifrostContextKeySessionID, "healthy-bound-fallback")
		requested := schemas.Route{Model: "m"}
		p1 := schemas.Route{Provider: "p1", Model: "m"}
		p2 := schemas.Route{Provider: "p2", Model: "m"}
		binding := bifrost.SessionStateKey(ctx, bifrost.SessionStateKindRoute, "", "m")
		if err := kv.SetWithTTL(binding, bifrost.RouteStateValue(p2), time.Hour); err != nil {
			t.Fatal(err)
		}
		failProvider(c, "p1")
		if got := affinity.ResolveRoute(ctx, requested, []schemas.Route{p1, p2}); len(got) != 2 || got[0] != p2 || got[1] != p1 {
			t.Fatalf("healthy session binding discarded (adaptive providers=%v): %+v", adaptiveProviders, got)
		}
	}
}

func TestAdaptiveNativeAffinityRespectsConfiguredKeyWeightsMode(t *testing.T) {
	c, _ := providerController()
	cfg := c.Config()
	cfg.RouteSelectionEnabled = false
	if err := c.Apply(cfg); err != nil {
		t.Fatal(err)
	}
	kv, err := kvstore.New(kvstore.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	affinity := c.SessionAffinity(bifrost.NewSessionAffinity(kv, bifrost.NewDefaultLogger(schemas.LogLevelError)))
	ctx := testContext()
	ctx.SetValue(schemas.BifrostContextKeySessionID, "static-key-selection")
	binding := bifrost.SessionStateKey(ctx, bifrost.SessionStateKindKey, "p1", "m")
	if err := kv.SetWithTTL(binding, "a", time.Hour); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		observe(c, "p1", "m", "a", responseError(503))
	}
	if key, ok := affinity.ResolveKey(ctx, "p1", "m", testKeys); !ok || key.ID != "a" {
		t.Fatalf("disabled adaptive key selection still overrides affinity: %s %v", key.ID, ok)
	}
	observe(c, "p1", "m", "a", responseError(401))
	if key, ok := affinity.ResolveKey(ctx, "p1", "m", testKeys); ok && key.ID == "a" {
		t.Fatal("configured weights mode followed a held credential")
	}
}

// Exercise the native hook order: provider routing, route affinity, key affinity,
// selection, then observation of the key that actually served the request.
func TestAdaptiveProviderRecoveryPreservesConfiguredKeySelection(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		sticky, zeroWeight, held bool
	}{
		{name: "session binding", sticky: true},
		{name: "configured weights", zeroWeight: true},
		{name: "held key check", sticky: true, zeroWeight: true, held: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, now := providerController()
			cfg := c.Config()
			cfg.RouteSelectionEnabled = false
			if err := c.Apply(cfg); err != nil {
				t.Fatal(err)
			}
			keys := append([]schemas.Key(nil), testKeys...)
			keys[0].Weight, keys[1].Weight = 1, 1
			if tc.zeroWeight {
				keys[0].Weight = 0
			}
			c.SetCatalog(c.providers,
				func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) { return keys, nil },
				c.supports)
			for range 3 {
				observe(c, "p1", "m", "a", responseError(503))
			}
			if tc.held {
				observe(c, "p1", "m", "a", responseError(401))
			}
			observe(c, "p1", "m", "b", nil)
			for _, key := range keys {
				observe(c, "p2", "m", key.ID, nil)
			}
			before := *c.routes[routeID{"p1", "m", "a"}]
			*now = now.Add(time.Minute)

			kv, err := kvstore.New(kvstore.Config{})
			if err != nil {
				t.Fatal(err)
			}
			defer kv.Close()
			affinity := c.SessionAffinity(bifrost.NewSessionAffinity(kv, bifrost.NewDefaultLogger(schemas.LogLevelError)))
			ctx := testContext()
			ctx.SetValue(schemas.BifrostContextKeySessionID, "provider-recovery-static-keys")
			p1 := schemas.Route{Provider: "p1", Model: "m"}
			if tc.sticky {
				if err := kv.SetWithTTL(bifrost.SessionStateKey(ctx, bifrost.SessionStateKindRoute, "", "m"), bifrost.RouteStateValue(p1), time.Hour); err != nil {
					t.Fatal(err)
				}
				if err := kv.SetWithTTL(bifrost.SessionStateKey(ctx, bifrost.SessionStateKindKey, "p1", "m"), "b", time.Hour); err != nil {
					t.Fatal(err)
				}
			}
			req := routeRequest("")
			if err := c.Route(ctx, req); err != nil {
				t.Fatal(err)
			}
			provider, model, fallbacks := req.GetRequestFields()
			chain := []schemas.Route{{Provider: provider, Model: model}}
			for _, fb := range fallbacks {
				chain = append(chain, schemas.Route{Provider: fb.Provider, Model: fb.Model, KeyID: fb.KeyID})
			}
			resolved := affinity.ResolveRoute(ctx, schemas.Route{Model: "m"}, chain)
			if resolved[0] != p1 {
				t.Fatalf("provider recovery was starved: %+v", resolved)
			}
			key, bound := affinity.ResolveKey(ctx, "p1", "m", keys)
			if !bound {
				key, err = c.SelectKey(ctx, keys, "p1", "m")
				if err != nil {
					t.Fatal(err)
				}
			}
			want := "b"
			if tc.held {
				want = "a"
			}
			if key.ID != want {
				t.Fatalf("recovery selected key %s, want %s", key.ID, want)
			}
			attempt := c.BeginProviderAttempt(ctx, schemas.ProviderAttemptInfo{Provider: "p1", Model: "m", KeyID: key.ID, KeyName: key.Name, Key: key})
			if attempt == nil {
				t.Fatal("actual provider attempt was not observed")
			}
			defer attempt.End()

			pick := func(want schemas.ModelProvider) {
				t.Helper()
				req := routeRequest("")
				if err := c.Route(testContext(), req); err != nil {
					t.Fatal(err)
				}
				if got, _, _ := req.GetRequestFields(); got != want {
					t.Fatalf("selected provider %s, want %s", got, want)
				}
			}
			for range 10 {
				pick("p2")
			}
			attempt.Observe(&schemas.BifrostResponse{}, nil)
			attempt.End()
			if tc.held {
				if c.holdLocked(routeID{"p1", "m", "a"}, *now) != nil {
					t.Fatal("successful held-key check did not release the hold")
				}
				return
			}
			after := c.routes[routeID{"p1", "m", "a"}]
			if after.Samples != before.Samples || after.LastSeen != before.LastSeen || after.ErrorRate != before.ErrorRate {
				t.Fatal("an outcome on b changed health observations for a")
			}
			pick("p2")
			*now = now.Add(29 * time.Second)
			pick("p2")
			*now = now.Add(2 * time.Second)
			pick("p1")
		})
	}
}

func BenchmarkAdaptiveSessionAffinity(b *testing.B) {
	for _, mode := range []string{"native", "disabled", "adaptive"} {
		b.Run(mode, func(b *testing.B) {
			c, _ := providerController()
			kv, err := kvstore.New(kvstore.Config{})
			if err != nil {
				b.Fatal(err)
			}
			defer kv.Close()
			native := bifrost.NewSessionAffinity(kv, bifrost.NewDefaultLogger(schemas.LogLevelError))
			affinity := native
			if mode != "native" {
				c.enabled.Store(mode == "adaptive")
				affinity = c.SessionAffinity(native)
			}
			ctx := testContext()
			ctx.SetValue(schemas.BifrostContextKeySessionID, "benchmark-session")
			requested := schemas.Route{Model: "m"}
			chain := []schemas.Route{{Provider: "p1", Model: "m"}, {Provider: "p2", Model: "m"}}
			if err := kv.SetWithTTL(bifrost.SessionStateKey(ctx, bifrost.SessionStateKindRoute, "", "m"), bifrost.RouteStateValue(chain[1]), time.Hour); err != nil {
				b.Fatal(err)
			}
			if err := kv.SetWithTTL(bifrost.SessionStateKey(ctx, bifrost.SessionStateKindKey, "p2", "m"), "a", time.Hour); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				ctx.ClearValue(schemas.BifrostContextKeyRoutingEngineLogs)
				ctx.ClearValue(schemas.BifrostContextKeyRoutingEnginesUsed)
				resolved := affinity.ResolveRoute(ctx, requested, chain)
				key, found := affinity.ResolveKey(ctx, resolved[0].Provider, resolved[0].Model, testKeys)
				if !found || key.ID != "a" || resolved[0] != chain[1] {
					b.Fatal("session binding changed")
				}
			}
		})
	}
}
