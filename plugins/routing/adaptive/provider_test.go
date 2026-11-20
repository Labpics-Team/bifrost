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
	r := routeRequest("p1", schemas.Fallback{Provider: "p2", Model: "other", KeyID: "specific"})
	ctx := testContext()
	c.Route(ctx, r)
	p, m, f := r.GetRequestFields()
	if p != "p2" || m != "other" || len(f) != 1 || f[0].Provider != "p1" || ctx.Value(schemas.BifrostContextKeyRoutingPinnedAPIKeyID) != "specific" {
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
	b.Run("unconfigured", func(b *testing.B) {
		c.enabled.Store(false)
		r := routeRequest("")
		b.ReportAllocs()
		for b.Loop() {
			c.Route(ctx, r)
		}
	})
}
