package adaptive

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
)

func providerController() (*Controller, *time.Time) {
	c, now := testController()
	c.SetCatalog(func(string) []schemas.ModelProvider { return []schemas.ModelProvider{"p1", "p2"} }, func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) { return testKeys, nil }, func(schemas.ModelProvider, schemas.Key, string) bool { return true })
	return c, now
}
func routeRequest(provider schemas.ModelProvider, fallbacks ...schemas.Fallback) *schemas.BifrostRequest {
	return &schemas.BifrostRequest{RequestType: schemas.ChatCompletionRequest, ChatRequest: &schemas.BifrostChatRequest{Provider: provider, Model: "m", Fallbacks: fallbacks}}
}
func failProvider(c *Controller, provider schemas.ModelProvider) {
	for _, k := range testKeys {
		observe(c, provider, "m", k.ID, responseError(401))
	}
}

func TestAdaptiveProviderChoosesHealthyAndPreservesNamedTraffic(t *testing.T) {
	c, _ := providerController()
	failProvider(c, "p1")
	r := routeRequest("")
	c.Route(testContext(), r)
	p, _, _ := r.GetRequestFields()
	if p != "p2" {
		t.Fatalf("chose %s", p)
	}
	fallbacks := []schemas.Fallback{{Provider: "p3", Model: "other", KeyID: "explicit"}}
	r = routeRequest("p1", fallbacks...)
	c.Route(testContext(), r)
	p, _, f := r.GetRequestFields()
	if p != "p1" || !reflect.DeepEqual(f, fallbacks) {
		t.Fatal("changed explicitly named route with default settings")
	}
	ctx := testContext()
	ctx.SetValue(schemas.BifrostContextKeyRoutingAllowedProviders, []schemas.ModelProvider{"p1"})
	r = routeRequest("")
	c.Route(ctx, r)
	p, _, f = r.GetRequestFields()
	if p != "p1" || len(f) != 0 {
		t.Fatal("adaptive routing escaped governance allowlist")
	}
}

func TestAdaptiveProviderRedirectAndFallbackPin(t *testing.T) {
	c, _ := providerController()
	cfg := c.Config()
	cfg.RerouteFailedDirections = true
	cfg.RerouteTarget = "fallbacks"
	c.Apply(cfg)
	failProvider(c, "p1")
	r := routeRequest("p1", schemas.Fallback{Provider: "p2", Model: "other", KeyID: "a"})
	ctx := testContext()
	c.Route(ctx, r)
	p, m, f := r.GetRequestFields()
	if p != "p2" || m != "other" || len(f) != 1 || f[0].Provider != "p1" || ctx.Value(schemas.BifrostContextKeyRoutingPinnedAPIKeyID) != "a" {
		t.Fatalf("incorrect redirect: %s %s %+v", p, m, f)
	}
	ctx = testContext()
	ctx.SetValue(schemas.BifrostContextKeyAPIKeyID, "a")
	r = routeRequest("p1")
	c.Route(ctx, r)
	p, _, _ = r.GetRequestFields()
	if p != "p1" {
		t.Fatal("explicit credential moved providers")
	}
}

func TestAdaptiveProviderDueCheckIsReservedAcrossRequests(t *testing.T) {
	c, now := providerController()
	failProvider(c, "p1")
	*now = now.Add(time.Minute)
	ctx := testContext()
	r := routeRequest("")
	c.Route(ctx, r)
	p, _, f := r.GetRequestFields()
	if p != "p1" || len(f) != 1 || f[0].Provider != "p2" {
		t.Fatalf("due route not checked with healthy fallback: %s %+v", p, f)
	}
	// p1 has two held keys, so it gets at most one request per due key.
	for i := 0; i < 2; i++ {
		r = routeRequest("")
		c.Route(testContext(), r)
	}
	p, _, _ = r.GetRequestFields()
	if p != "p2" {
		t.Fatal("held provider receives a burst of rechecks")
	}
	k, err := c.SelectKey(ctx, testKeys, "p1", "m")
	if err != nil || k.ID == "" {
		t.Fatalf("reserved key not selected: %v", err)
	}
}

func TestAdaptiveProviderRecoversFromTransientFailureOnRealTraffic(t *testing.T) {
	c, now := providerController()
	for _, key := range testKeys {
		for i := 0; i < 3; i++ {
			observe(c, "p1", "m", key.ID, responseError(503))
		}
		observe(c, "p2", "m", key.ID, nil)
	}
	r := routeRequest("")
	c.Route(testContext(), r)
	if p, _, _ := r.GetRequestFields(); p != "p2" {
		t.Fatalf("failed provider received ordinary traffic: %s", p)
	}
	*now = now.Add(31 * time.Second)
	// Every formerly failed key gets one real attempt even though the healthy
	// provider keeps succeeding. Its success updates the score used next time.
	for range testKeys {
		ctx := testContext()
		r = routeRequest("")
		c.Route(ctx, r)
		p, _, fallbacks := r.GetRequestFields()
		if p != "p1" {
			t.Fatalf("recovered provider starved behind p2: %s", p)
		}
		if len(fallbacks) == 0 || fallbacks[0].Provider != "p2" {
			t.Fatal("recheck lost healthy fallback")
		}
		key, err := c.SelectKey(ctx, testKeys, p, "m")
		if err != nil {
			t.Fatal(err)
		}
		observeWithContext(c, ctx, p, "m", key.ID, nil)
	}
	for _, r := range c.Status() {
		if r.Provider == "p1" && (r.Samples != 4 || r.ErrorRate >= 1 || r.State != "recovering") {
			t.Fatalf("successful recovery not observed: %+v", r)
		}
	}
}

func TestAdaptivePruningFollowsNativeNamedProviderSettings(t *testing.T) {
	for _, test := range []struct {
		name                                  string
		append, redirect, failPrimary, pruned bool
	}{
		{"only prune keeps named fallback", false, false, false, false},
		{"no redirect keeps named fallback", false, true, false, false},
		{"append allows prune", true, false, false, true},
		{"actual redirect allows prune", false, true, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, _ := providerController()
			cfg := c.Config()
			cfg.PruneFailedFallbacks = true
			cfg.AppendFallbacksToPinned = test.append
			cfg.RerouteFailedDirections = test.redirect
			c.Apply(cfg)
			failProvider(c, "failed")
			if test.failPrimary {
				failProvider(c, "p1")
			}
			r := routeRequest("p1", schemas.Fallback{Provider: "failed", Model: "m"})
			c.Route(testContext(), r)
			_, _, fallbacks := r.GetRequestFields()
			hasFailed := false
			for _, fb := range fallbacks {
				if fb.Provider == "failed" {
					hasFailed = true
				}
			}
			if hasFailed == test.pruned {
				t.Fatalf("fallback pruning contradicted named-provider policy: %+v", fallbacks)
			}
		})
	}
}

func BenchmarkAdaptiveProviderRouting(b *testing.B) {
	c, _ := providerController()
	ctx := testContext()
	b.Run("named_default", func(b *testing.B) {
		r := routeRequest("p1")
		b.ReportAllocs()
		for b.Loop() {
			c.Route(ctx, r)
		}
	})
	b.Run("unnamed_two_providers", func(b *testing.B) {
		for _, provider := range []schemas.ModelProvider{"p1", "p2"} {
			for _, key := range testKeys {
				observe(c, provider, "m", key.ID, nil)
			}
		}
		r := routeRequest("")
		b.ReportAllocs()
		for b.Loop() {
			r.SetProvider("")
			r.SetFallbacks(nil)
			ctx.ClearValue(schemas.BifrostContextKeyRoutingEngineLogs)
			ctx.ClearValue(schemas.BifrostContextKeyRoutingEnginesUsed)
			c.Route(ctx, r)
		}
	})
	b.Run("unconfigured", func(b *testing.B) {
		c.enabled.Store(false)
		r := routeRequest("")
		b.ReportAllocs()
		for b.Loop() {
			c.Route(ctx, r)
		}
	})
}

func TestAdaptiveKeylessProviderUsesObservedHealth(t *testing.T) {
	c, _ := providerController()
	c.SetCatalog(func(string) []schemas.ModelProvider { return []schemas.ModelProvider{"p1", "p2"} }, func(_ context.Context, p schemas.ModelProvider) ([]schemas.Key, error) {
		if p == "p1" {
			return nil, nil
		}
		return testKeys, nil
	}, func(schemas.ModelProvider, schemas.Key, string) bool { return true }, func(p schemas.ModelProvider) bool { return p == "p1" })
	for i := 0; i < 3; i++ {
		a := c.BeginProviderAttempt(testContext(), schemas.ProviderAttemptInfo{Provider: "p1", Model: "m"})
		if a == nil {
			t.Fatal("keyless provider was not observed")
		}
		a.Observe(nil, responseError(503))
		a.End()
	}
	r := routeRequest("")
	c.Route(testContext(), r)
	if p, _, _ := r.GetRequestFields(); p != "p2" {
		t.Fatalf("failing keyless provider ignored observed health: %s", p)
	}
}
