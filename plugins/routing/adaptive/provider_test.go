package adaptive

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
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

func TestAdaptiveProviderRecoveryProbeKeepsOtherRequestsOnHealthyAlternative(t *testing.T) {
	for _, provider := range []schemas.ModelProvider{"", "p1"} {
		name := "bare model"
		if provider != "" {
			name = "named provider"
		}
		t.Run(name, func(t *testing.T) {
			c, now := providerController()
			keys := testKeys[:1]
			c.SetCatalog(func(string) []schemas.ModelProvider { return []schemas.ModelProvider{"p1", "p2"} }, func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) { return keys, nil }, func(schemas.ModelProvider, schemas.Key, string) bool { return true })
			cfg := c.Config()
			cfg.RerouteFailedDirections = true
			c.Apply(cfg)
			for range 3 {
				observe(c, "p1", "m", keys[0].ID, responseError(503))
			}
			*now = now.Add(31 * time.Second)

			ctx := testContext()
			probe := routeRequest(provider)
			if err := c.Route(ctx, probe); err != nil {
				t.Fatal(err)
			}
			p, model, fallbacks := probe.GetRequestFields()
			if p != "p1" || len(fallbacks) != 1 || fallbacks[0].Provider != "p2" {
				t.Fatalf("due check lost its healthy fallback: %s %+v", p, fallbacks)
			}
			key, err := c.SelectKey(ctx, keys, p, model)
			if err != nil {
				t.Fatal(err)
			}
			attempt := c.BeginProviderAttempt(ctx, schemas.ProviderAttemptInfo{Provider: p, Model: model, KeyID: key.ID, KeyName: key.Name})
			if attempt == nil {
				t.Fatal("recovery attempt was not observed")
			}
			defer attempt.End()

			other := routeRequest(provider)
			if err := c.Route(testContext(), other); err != nil {
				t.Fatal(err)
			}
			p, _, fallbacks = other.GetRequestFields()
			if p != "p2" {
				t.Fatalf("provider received ordinary traffic while its recovery check was in flight: %s", p)
			}
			if provider == "" && len(fallbacks) != 0 {
				t.Fatalf("in-flight check was offered as a healthy fallback: %+v", fallbacks)
			}

			attempt.Observe(&schemas.BifrostResponse{}, nil)
			attempt.End()
			for _, status := range c.Status() {
				if status.Provider == "p1" && (status.State != "recovering" || status.Score <= 0) {
					t.Fatalf("successful check did not restore a normal traffic score: %+v", status)
				}
			}
			recoveryCtx := testContext()
			recovered := routeRequest(provider)
			if err := c.Route(recoveryCtx, recovered); err != nil {
				t.Fatal(err)
			}
			if recoveryCtx.Value(probeContextKey{}) != nil {
				t.Fatal("successful recovery was replaced with another checking request")
			}
			p, _, fallbacks = recovered.GetRequestFields()
			if p != "p1" && (len(fallbacks) != 1 || fallbacks[0].Provider != "p1") {
				t.Fatalf("successful check did not return the provider to ordinary traffic: %s %+v", p, fallbacks)
			}
		})
	}
}

func TestAdaptiveProviderUnavailablePoolStillSendsNativeAttempt(t *testing.T) {
	for _, status := range []int{401, 503} {
		for _, provider := range []schemas.ModelProvider{"", "p1"} {
			name := "bare model"
			if provider != "" {
				name = "named provider"
			}
			if status == 401 {
				name += "/held"
			} else {
				name += "/failed"
			}
			t.Run(name, func(t *testing.T) {
				c, now := providerController()
				keys := testKeys[:1]
				c.SetCatalog(func(string) []schemas.ModelProvider { return []schemas.ModelProvider{"p1"} }, func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) { return keys, nil }, func(schemas.ModelProvider, schemas.Key, string) bool { return true })
				cfg := c.Config()
				cfg.RerouteFailedDirections = true
				c.Apply(cfg)
				for range 3 {
					observe(c, "p1", "m", keys[0].ID, responseError(status))
				}
				*now = now.Add(time.Minute)
				probe := routeRequest(provider)
				if err := c.Route(testContext(), probe); err != nil {
					t.Fatal(err)
				}
				// Even while the only direction's check is reserved, the native
				// fail-open path must still send an attempt instead of refusing it.
				ctx := testContext()
				r := routeRequest(provider)
				if err := c.Route(ctx, r); err != nil {
					t.Fatal(err)
				}
				p, model, _ := r.GetRequestFields()
				if p != "p1" {
					t.Fatalf("all-unavailable pool lost its provider: %s", p)
				}
				key, err := c.SelectKey(ctx, keys, p, model)
				if err != nil || key.ID != keys[0].ID {
					t.Fatalf("all-unavailable pool refused native key selection: %s %v", key.ID, err)
				}
				attempt := c.BeginProviderAttempt(ctx, schemas.ProviderAttemptInfo{Provider: p, Model: model, KeyID: key.ID, KeyName: key.Name})
				if attempt == nil {
					t.Fatal("native fail-open attempt was not observed")
				}
				attempt.Observe(nil, responseError(status))
				attempt.End()
			})
		}
	}
}

func TestAdaptivePruningFollowsNativeNamedProviderSettings(t *testing.T) {
	for _, test := range []struct {
		name                                  string
		append, redirect, failPrimary, pruned bool
		due                                   bool
	}{
		{"only prune keeps named fallback", false, false, false, false, false},
		{"no redirect keeps named fallback", false, true, false, false, false},
		{"append allows prune", true, false, false, true, false},
		{"actual redirect allows prune", false, true, true, true, false},
		{"due check keeps named fallback", false, true, true, false, true},
		{"due check with append allows prune", true, true, true, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, now := providerController()
			cfg := c.Config()
			cfg.PruneFailedFallbacks = true
			cfg.AppendFallbacksToPinned = test.append
			cfg.RerouteFailedDirections = test.redirect
			c.Apply(cfg)
			failProvider(c, "failed")
			if test.failPrimary {
				failProvider(c, "p1")
			}
			if test.due {
				*now = now.Add(time.Minute)
			}
			r := routeRequest("p1", schemas.Fallback{Provider: "failed", Model: "m"})
			c.Route(testContext(), r)
			provider, _, fallbacks := r.GetRequestFields()
			hasFailed := false
			hasHealthy := false
			for _, fb := range fallbacks {
				if fb.Provider == "failed" {
					hasFailed = true
				}
				if fb.Provider == "p2" {
					hasHealthy = true
				}
			}
			if hasFailed == test.pruned {
				t.Fatalf("fallback pruning contradicted named-provider policy: %+v", fallbacks)
			}
			if test.due && (provider != "p1" || !hasHealthy) {
				t.Fatalf("named provider check lost its healthy alternative: %s %+v", provider, fallbacks)
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

func TestAdaptiveUnknownHealthPreservesExplicitDirections(t *testing.T) {
	for _, provider := range []schemas.ModelProvider{"", "p1"} {
		t.Run(string(provider), func(t *testing.T) {
			c, _ := providerController()
			cfg := c.Config()
			cfg.PruneFailedFallbacks = true
			cfg.AppendFallbacksToPinned = true
			cfg.RerouteFailedDirections = true
			if err := c.Apply(cfg); err != nil {
				t.Fatal(err)
			}
			failProvider(c, "failed")
			c.SetCatalog(func(string) []schemas.ModelProvider { return []schemas.ModelProvider{"p1", "p2"} }, func(_ context.Context, p schemas.ModelProvider) ([]schemas.Key, error) {
				switch p {
				case "p1", "unknown":
					return nil, errors.New("catalog unavailable")
				case "empty":
					return nil, nil
				default:
					return testKeys, nil
				}
			}, func(schemas.ModelProvider, schemas.Key, string) bool { return true })
			unknown := schemas.Fallback{Provider: "unknown", Model: "other", KeyID: "explicit"}
			req := routeRequest(provider, unknown, schemas.Fallback{Provider: "failed", Model: "m"}, schemas.Fallback{Provider: "empty", Model: "m"})
			if err := c.Route(testContext(), req); err != nil {
				t.Fatal(err)
			}
			selected, _, fallbacks := req.GetRequestFields()
			if provider == "p1" && selected != provider {
				t.Fatalf("catalog error redirected a named provider: %s", selected)
			}
			if !slices.Contains(fallbacks, unknown) {
				t.Fatalf("catalog error removed the explicit fallback or its key/model: %+v", fallbacks)
			}
			if slices.ContainsFunc(fallbacks, func(fb schemas.Fallback) bool { return fb.Provider == "failed" || fb.Provider == "empty" }) {
				t.Fatalf("known unavailable fallback was retained: %+v", fallbacks)
			}
		})
	}
}

func TestAdaptiveLeavesLargePayloadExtensionToNativeSelection(t *testing.T) {
	largeText := strings.Repeat("x", int(schemas.DefaultLargePayloadRequestThresholdBytes)+1)
	for _, test := range []struct {
		name                     string
		extension, reader, large bool
		stream, metadata         bool
	}{
		{name: "extension", extension: true, reader: true},
		{name: "extension without reader", extension: true},
		{name: "native large parsed request", large: true},
		{name: "native streaming response", stream: true},
		{name: "metadata without extension mode", metadata: true},
		{name: "reader without extension mode", reader: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, _ := providerController()
			cfg := c.Config()
			cfg.RerouteFailedDirections = true
			if err := c.Apply(cfg); err != nil {
				t.Fatal(err)
			}
			failProvider(c, "p1")
			observe(c, "key-pool", "m", "a", responseError(401))
			ctx, req := testContext(), routeRequest("p1")
			if test.extension {
				ctx.SetValue(schemas.BifrostContextKeyLargePayloadMode, true)
			}
			reader := strings.NewReader("unparsed body")
			if test.reader {
				ctx.SetValue(schemas.BifrostContextKeyLargePayloadReader, reader)
			}
			if test.metadata {
				ctx.SetValue(schemas.BifrostContextKeyLargePayloadMetadata, &schemas.LargePayloadMetadata{Model: "m"})
			}
			if test.large {
				req.ChatRequest.Input = []schemas.ChatMessage{{Role: schemas.ChatMessageRoleUser, Content: &schemas.ChatMessageContent{ContentStr: &largeText}}}
			}
			if test.stream {
				req.RequestType = schemas.ChatCompletionStreamRequest
			}
			before := c.Status()
			if err := c.Route(ctx, req); err != nil {
				t.Fatal(err)
			}
			provider, _, _ := req.GetRequestFields()
			wantProvider, wantKey := schemas.ModelProvider("p2"), "b"
			if test.extension {
				wantProvider, wantKey = "p1", "a"
			}
			if provider != wantProvider {
				t.Errorf("provider=%s, want %s", provider, wantProvider)
			}
			// Native configured weights deterministically choose a. Adaptive
			// selection must instead skip its hold and choose healthy b.
			keys := []schemas.Key{{ID: "a", Name: "A", Weight: 1}, {ID: "b", Name: "B", Weight: 0}}
			key, err := c.SelectKey(ctx, keys, "key-pool", "m")
			if err != nil || key.ID != wantKey {
				t.Errorf("key=%s, want %s: %v", key.ID, wantKey, err)
			}
			attempt := c.BeginProviderAttempt(ctx, schemas.ProviderAttemptInfo{Provider: "key-pool", Model: "m", KeyID: "b", KeyName: "B"})
			if test.extension && attempt != nil {
				t.Error("extension attempt entered adaptive admission/observation")
			} else if !test.extension && attempt == nil {
				t.Error("native attempt lost adaptive observation")
			}
			if attempt != nil {
				attempt.Observe(&schemas.BifrostResponse{}, nil)
				attempt.End()
			}
			if ctx.Value(probeContextKey{}) != nil || reader.Len() != len("unparsed body") {
				t.Error("request unexpectedly reserved a check or consumed the body")
			}
			if test.extension && !reflect.DeepEqual(before, c.Status()) {
				t.Error("extension request changed adaptive state")
			}
		})
	}
}

// Ranking must let a provider earn traffic back from live outcomes even when
// its healthy alternative never fails.
func TestAdaptiveProviderRecoveryContinuesUntilHealthy(t *testing.T) {
	for _, tc := range []struct {
		name             string
		status, failures int
	}{
		{"hold", 401, 1}, {"transient", 503, 3}, {"degraded", 500, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, now := providerController()
			for _, key := range testKeys {
				for range tc.failures {
					observe(c, "p1", "m", key.ID, responseError(tc.status))
				}
				observe(c, "p2", "m", key.ID, nil)
			}
			for range 40 {
				*now = now.Add(time.Minute)
				ctx := testContext()
				req := routeRequest("")
				if err := c.Route(ctx, req); err != nil {
					t.Fatal(err)
				}
				provider, _, _ := req.GetRequestFields()
				key, err := c.SelectKey(ctx, testKeys, provider, "m")
				if err != nil {
					t.Fatal(err)
				}
				observeWithContext(c, ctx, provider, "m", key.ID, nil)
			}
			for _, row := range c.Status() {
				if row.Provider == "p1" && row.State != "healthy" {
					t.Fatalf("provider never earned traffic back: %+v", row)
				}
			}
		})
	}
}

func TestAdaptiveRecoveryObservationUsesOneLiveLease(t *testing.T) {
	c, now := providerController()
	keys := testKeys[:1]
	c.SetCatalog(func(string) []schemas.ModelProvider { return []schemas.ModelProvider{"p1", "p2"} },
		func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) { return keys, nil },
		func(schemas.ModelProvider, schemas.Key, string) bool { return true })
	observe(c, "p1", "m", "a", responseError(503))
	observe(c, "p2", "m", "a", nil)
	*now = now.Add(31 * time.Second)
	pick := func(ctx *schemas.BifrostContext, want schemas.ModelProvider) {
		t.Helper()
		req := routeRequest("")
		if err := c.Route(ctx, req); err != nil {
			t.Fatal(err)
		}
		if got, _, _ := req.GetRequestFields(); got != want {
			t.Fatalf("selected %s, want %s", got, want)
		}
	}
	check := testContext()
	pick(check, "p1")
	for range 10 {
		pick(testContext(), "p2")
	}
	key, err := c.SelectKey(check, keys, "p1", "m")
	if err != nil {
		t.Fatal(err)
	}
	observeWithContext(c, check, "p1", "m", key.ID, nil)
	pick(testContext(), "p2")
	*now = now.Add(29 * time.Second)
	pick(testContext(), "p2")
	*now = now.Add(2 * time.Second)
	pick(testContext(), "p1")
}

func TestAdaptiveChoosesHealthiestProviderAndWeightsKeys(t *testing.T) {
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

func staticProviderRecovery(t *testing.T) (*Controller, *time.Time, []schemas.Key) {
	t.Helper()
	c, now := providerController()
	cfg := c.Config()
	cfg.RouteSelectionEnabled = false
	if err := c.Apply(cfg); err != nil {
		t.Fatal(err)
	}
	keys := []schemas.Key{{ID: "a", Name: "A", Weight: 0}, {ID: "b", Name: "B", Weight: 1}}
	c.SetCatalog(func(string) []schemas.ModelProvider { return []schemas.ModelProvider{"p1", "p2"} },
		func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) { return keys, nil },
		func(schemas.ModelProvider, schemas.Key, string) bool { return true })
	for _, key := range keys {
		for range 3 {
			observe(c, "p1", "m", key.ID, responseError(503))
		}
		observe(c, "p2", "m", key.ID, nil)
	}
	*now = now.Add(31 * time.Second)
	return c, now, keys
}

func requireRecoveryProvider(t *testing.T, c *Controller, ctx *schemas.BifrostContext, want schemas.ModelProvider) {
	t.Helper()
	req := routeRequest("")
	if err := c.Route(ctx, req); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := req.GetRequestFields(); got != want {
		t.Fatalf("selected %s, want %s", got, want)
	}
}

func beginStaticRecoveryAttempt(t *testing.T, c *Controller, ctx *schemas.BifrostContext, keys []schemas.Key) schemas.ProviderAttempt {
	t.Helper()
	key, err := c.SelectKey(ctx, keys, "p1", "m")
	if err != nil || key.ID != "b" {
		t.Fatalf("provider recovery changed configured key selection: key=%s error=%v", key.ID, err)
	}
	a := c.BeginProviderAttempt(ctx, schemas.ProviderAttemptInfo{Provider: "p1", Model: "m", KeyID: key.ID, KeyName: key.Name})
	if a == nil {
		t.Fatal("selected key lost its real attempt observer")
	}
	return a
}

func TestAdaptiveProviderRecoveryPreservesStaticKeyAndAdmission(t *testing.T) {
	c, now, keys := staticProviderRecovery(t)
	ctx := testContext()
	requireRecoveryProvider(t, c, ctx, "p1")
	a := beginStaticRecoveryAttempt(t, c, ctx, keys)
	defer a.End()

	// Both keys failed, but concurrent requests share one provider check.
	results := make(chan error, 8)
	for range cap(results) {
		go func() {
			req := routeRequest("")
			err := c.Route(testContext(), req)
			if got, _, _ := req.GetRequestFields(); err == nil && got != "p2" {
				err = fmt.Errorf("active provider check admitted another request to %s", got)
			}
			results <- err
		}()
	}
	for range cap(results) {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}

	a.Observe(&schemas.BifrostResponse{}, nil)
	a.End()
	status := make(map[string]RouteStatus)
	for _, row := range c.Status() {
		if row.Provider == "p1" && row.Model == "m" {
			status[row.KeyID] = row
		}
	}
	if got := status["a"]; got.Samples != 3 || got.ErrorRate != 1 || got.State != "failed" {
		t.Fatalf("unattempted key received recovery: %+v", got)
	}
	if got := status["b"]; got.Samples != 4 || got.ErrorRate >= 1 || got.State != "recovering" || got.Inflight != 0 {
		t.Fatalf("actual static key did not receive its successful observation: %+v", got)
	}
	requireRecoveryProvider(t, c, testContext(), "p2")
	*now = now.Add(29 * time.Second)
	requireRecoveryProvider(t, c, testContext(), "p2")
	*now = now.Add(2 * time.Second)
	requireRecoveryProvider(t, c, testContext(), "p1")
}

func TestAdaptiveProviderRecoveryStaleCompletionKeepsNewAdmission(t *testing.T) {
	c, now, keys := staticProviderRecovery(t)
	oldCtx := testContext()
	requireRecoveryProvider(t, c, oldCtx, "p1")
	old := beginStaticRecoveryAttempt(t, c, oldCtx, keys)
	defer old.End()

	*now = now.Add(5*time.Minute + time.Second)
	currentCtx := testContext()
	requireRecoveryProvider(t, c, currentCtx, "p1")
	current := beginStaticRecoveryAttempt(t, c, currentCtx, keys)
	defer current.End()
	old.Observe(&schemas.BifrostResponse{}, nil)
	old.End()

	// A stale completion must not turn the new five-minute admission into
	// a thirty-second cooldown while the replacement call is still running.
	*now = now.Add(31 * time.Second)
	requireRecoveryProvider(t, c, testContext(), "p2")
	current.End()
	*now = now.Add(31 * time.Second)
	requireRecoveryProvider(t, c, testContext(), "p1")
}

func TestAdaptiveProviderRecoveryAbandonedAttemptReleasesAdmission(t *testing.T) {
	for _, name := range []string{"cancelled", "no response", "empty model", "fallback provider", "different model"} {
		t.Run(name, func(t *testing.T) {
			c, now, keys := staticProviderRecovery(t)
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := schemas.NewBifrostContext(parent, schemas.NoDeadline)
			requireRecoveryProvider(t, c, ctx, "p1")
			info := schemas.ProviderAttemptInfo{Provider: "p1", Model: "m", KeyID: "b", KeyName: "B"}
			switch name {
			case "empty model":
				info.Model = ""
			case "fallback provider":
				info.Provider = "p2"
			case "different model":
				info.Model = "other"
			}
			a := c.BeginProviderAttempt(ctx, info)
			if name == "empty model" {
				if a != nil {
					t.Fatal("invalid route unexpectedly began an observation")
				}
			} else {
				if a == nil {
					t.Fatal("actual route lost its attempt observer")
				}
				defer a.End()
				switch name {
				case "cancelled":
					a.Observe(&schemas.BifrostResponse{}, nil)
					cancel()
					a.End()
				case "no response":
					a.End()
				}
			}
			requireRecoveryProvider(t, c, testContext(), "p2")
			*now = now.Add(31 * time.Second)
			// A fallback on another provider/model may still be running.
			// It must release the original admission at dispatch, not at End.
			requireRecoveryProvider(t, c, testContext(), "p1")
			observed := 0
			for _, row := range c.Status() {
				if row.Provider != "p1" || row.Model != "m" {
					continue
				}
				observed++
				if row.Samples != 3 || row.ErrorRate != 1 || row.State != "failed" || row.Inflight != 0 {
					t.Fatalf("abandoned or different attempt changed original key health: %+v", row)
				}
			}
			if observed != len(keys) {
				t.Fatalf("lost original key observations: got %d, want %d", observed, len(keys))
			}
		})
	}
}

func TestAdaptiveProviderRecoveryCapacityPreservesLiveChecks(t *testing.T) {
	c, now := testController()
	reserve := func(model string) (probeReservation, bool) {
		ctx := testContext()
		c.mu.Lock()
		defer c.mu.Unlock()
		ok := c.reserveProviderCheckLocked(ctx, routeID{Provider: "p1", Model: model}, *now)
		lease, _ := ctx.Value(probeContextKey{}).(probeReservation)
		return lease, ok
	}
	leases := make([]probeReservation, maxRoutes)
	for i := range leases {
		var ok bool
		leases[i], ok = reserve(fmt.Sprint(i))
		if !ok {
			t.Fatalf("admission refused before its capacity at %d", i)
		}
	}
	if _, ok := reserve("overflow"); ok || len(c.providerChecks) > maxRoutes {
		t.Fatal("provider admission exceeded its memory bound")
	}
	if err := c.maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, lease := range leases {
		if !c.reservationCurrentLocked(lease, *now) {
			t.Fatalf("capacity pressure or maintenance evicted a live check for %s", lease.Model)
		}
	}
	*now = now.Add(5*time.Minute + time.Second)
	current, ok := reserve("overflow")
	if !ok {
		t.Fatal("expired admissions prevented a new real check")
	}
	if err := c.maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.providerChecks) != 1 || !c.reservationCurrentLocked(current, *now) {
		t.Fatal("maintenance did not discard expired checks while retaining the live one")
	}
	*now = now.Add(5*time.Minute + time.Second)
	if err := c.maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.providerChecks) != 0 {
		t.Fatal("expired provider admissions were retained without further traffic")
	}
}
