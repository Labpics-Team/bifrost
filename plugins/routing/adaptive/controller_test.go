package adaptive

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/keyselectors"
	providerUtils "github.com/maximhq/bifrost/core/providers/utils"
	"github.com/maximhq/bifrost/core/schemas"
)

func testContext() *schemas.BifrostContext {
	return schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
}
func responseError(status int) *schemas.BifrostError {
	return &schemas.BifrostError{StatusCode: &status, Error: &schemas.ErrorField{Message: "provider refused"}}
}
func testController() (*Controller, *time.Time) {
	c := New(DefaultConfig())
	now := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return now }
	return c, &now
}
func observe(c *Controller, provider schemas.ModelProvider, model, key string, err *schemas.BifrostError) {
	observeWithContext(c, testContext(), provider, model, key, err)
}
func observeWithContext(c *Controller, ctx *schemas.BifrostContext, provider schemas.ModelProvider, model, key string, err *schemas.BifrostError) {
	a := c.BeginProviderAttempt(ctx, schemas.ProviderAttemptInfo{Provider: provider, Model: model, KeyID: key, KeyName: key})
	if err == nil {
		a.Observe(&schemas.BifrostResponse{}, nil)
	} else {
		a.Observe(nil, err)
	}
	a.End()
}

var testKeys = []schemas.Key{{ID: "a", Name: "A", Weight: 1}, {ID: "b", Name: "B", Weight: 1}}

func TestAdaptiveKeyRefusalsAndRealRecovery(t *testing.T) {
	c, now := testController()
	observe(c, schemas.OpenAI, "model", "a", responseError(401))
	for _, model := range []string{"model", "other"} {
		for i := 0; i < 20; i++ {
			k, err := c.SelectKey(testContext(), testKeys, schemas.OpenAI, model)
			if err != nil || k.ID != "b" {
				t.Fatalf("held credential selected for %s: %s %v", model, k.ID, err)
			}
		}
	}
	*now = now.Add(time.Minute)
	probeCtx := testContext()
	k, _ := c.SelectKey(probeCtx, testKeys, schemas.OpenAI, "model")
	if k.ID != "a" {
		t.Fatal("due credential did not receive real recheck")
	}
	k, _ = c.SelectKey(testContext(), testKeys, schemas.OpenAI, "other")
	if k.ID != "b" {
		t.Fatal("parallel request duplicated whole-key recheck")
	}
	observeWithContext(c, probeCtx, schemas.OpenAI, "model", "a", nil)
	if c.holdLocked(routeID{schemas.OpenAI, "other", "a"}, *now) != nil {
		t.Fatal("successful recheck retained hold")
	}
	if status := c.Status()[0]; status.State != "recovering" {
		t.Fatalf("state=%s, want recovering", status.State)
	}
}

func TestAdaptiveNativeFailureScope(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		held   bool
		whole  bool
	}{{"credential", 401, true, true}, {"quota", 402, true, true}, {"rate", 429, true, false}, {"model", 404, false, false}, {"request", 400, false, false}, {"transient", 503, false, false}} {
		t.Run(test.name, func(t *testing.T) {
			c, now := testController()
			observe(c, schemas.OpenAI, "model", "a", responseError(test.status))
			held := c.holdLocked(routeID{schemas.OpenAI, "model", "a"}, *now) != nil
			if held != test.held {
				t.Fatalf("held=%v", held)
			}
			whole := c.holdLocked(routeID{schemas.OpenAI, "other", "a"}, *now) != nil
			if whole != test.whole {
				t.Fatalf("whole=%v", whole)
			}
		})
	}
}

func TestAdaptiveHoldDoesNotGrowFromAlreadyInflightCalls(t *testing.T) {
	c, now := testController()
	a := c.BeginProviderAttempt(testContext(), schemas.ProviderAttemptInfo{Provider: schemas.OpenAI, Model: "m", KeyID: "a"})
	observe(c, schemas.OpenAI, "m", "a", responseError(429))
	h := c.holdLocked(routeID{schemas.OpenAI, "m", "a"}, *now)
	until := h.HoldUntil
	*now = now.Add(time.Second)
	a.Observe(nil, responseError(429))
	a.End()
	if h.HoldUntil != until || h.Rung != 1 {
		t.Fatal("concurrent refusals lengthened hold")
	}
	*now = until.Add(time.Second)
	observe(c, schemas.OpenAI, "m", "a", responseError(429))
	h = c.holdLocked(routeID{schemas.OpenAI, "m", "a"}, *now)
	if h.Rung != 2 || h.HoldUntil.Sub(*now) < 54*time.Second {
		t.Fatalf("recheck rung=%d delay=%v", h.Rung, h.HoldUntil.Sub(*now))
	}
	if got := holdDuration(schemas.FailureClassRateLimit, 1, 1); got != 10*time.Second {
		t.Fatalf("Retry-After floor %v", got)
	}
}

func TestAdaptiveTransientRecheckKeepsRefusalScope(t *testing.T) {
	c, now := testController()
	observe(c, schemas.OpenAI, "m", "a", responseError(429))
	h := c.holdLocked(routeID{schemas.OpenAI, "m", "a"}, *now)
	*now = h.HoldUntil.Add(time.Second)
	observe(c, schemas.OpenAI, "m", "a", responseError(503))
	h = c.holdLocked(routeID{schemas.OpenAI, "m", "a"}, *now)
	if h.HoldClass != schemas.FailureClassRateLimit || h.Rung != 1 || !h.HoldUntil.After(*now) {
		t.Fatalf("network failure changed refusal: %+v", h)
	}
	if status := c.Status()[0]; status.FailureClass != schemas.FailureClassRateLimit {
		t.Fatalf("dashboard lost refusal reason: %+v", status)
	}
}

func TestAdaptiveUsesNativeLatencyForInjectedTools(t *testing.T) {
	c, now := testController()
	a := c.BeginProviderAttempt(testContext(), schemas.ProviderAttemptInfo{Provider: schemas.OpenAI, Model: "m", KeyID: "a"})
	*now = now.Add(time.Minute)
	a.Observe(&schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{ExtraFields: schemas.BifrostResponseExtraFields{Latency: 100}}}, nil)
	a.End()
	if got := c.Status()[0].LatencyMS; got != 100 {
		t.Fatalf("tool work included in provider latency: %v", got)
	}
}

func TestAdaptiveStreamsMeasureFirstResponseAndIgnoreCancellation(t *testing.T) {
	c, now := testController()
	ctx := testContext()
	a := c.BeginProviderAttempt(ctx, schemas.ProviderAttemptInfo{Provider: schemas.OpenAI, Model: "m", KeyID: "a"})
	*now = now.Add(10 * time.Millisecond)
	a.Observe(&schemas.BifrostResponse{}, nil)
	*now = now.Add(time.Hour)
	a.Observe(&schemas.BifrostResponse{}, nil)
	a.End()
	a.End()
	status := c.Status()[0]
	if status.LatencyMS != 10 || status.Inflight != 0 || status.Samples != 1 {
		t.Fatalf("stream sample: %+v", status)
	}
	parent, cancel := context.WithCancel(context.Background())
	ctx = schemas.NewBifrostContext(parent, schemas.NoDeadline)
	a = c.BeginProviderAttempt(ctx, schemas.ProviderAttemptInfo{Provider: schemas.OpenAI, Model: "m", KeyID: "a"})
	a.Observe(&schemas.BifrostResponse{}, nil)
	cancel()
	a.End()
	if status = c.Status()[0]; status.Samples != 1 || status.Inflight != 0 {
		t.Fatalf("cancellation changed health: %+v", status)
	}
	ctx = testContext()
	ctx.SetValue(schemas.BifrostContextKeyDirectKey, schemas.Key{ID: "caller"})
	if c.BeginProviderAttempt(ctx, schemas.ProviderAttemptInfo{KeyID: "caller", Model: "m"}) != nil {
		t.Fatal("direct credential was tracked")
	}
}

func TestAdaptiveTransportTimeoutDistinguishesCallerDeadline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer srv.Close()
	for _, callerDeadline := range []bool{true, false} {
		t.Run(map[bool]string{true: "caller deadline", false: "provider timeout"}[callerDeadline], func(t *testing.T) {
			parent := context.Background()
			client := &http.Client{Timeout: 30 * time.Millisecond}
			if callerDeadline {
				var cancel context.CancelFunc
				parent, cancel = context.WithTimeout(parent, 30*time.Millisecond)
				defer cancel()
				client.Timeout = time.Second
			}
			ctx := schemas.NewBifrostContext(parent, schemas.NoDeadline)
			c := New(DefaultConfig())
			a := c.BeginProviderAttempt(ctx, schemas.ProviderAttemptInfo{Provider: schemas.OpenAI, Model: "m", KeyID: "a"})
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, transportErr := providerUtils.DoHTTPRequest(client, req)
			if transportErr == nil {
				t.Fatal("stalled transport did not time out")
			}
			a.Observe(nil, providerUtils.NewBifrostTimeoutError(schemas.ErrProviderRequestTimedOut, transportErr))
			a.End()
			status := c.Status()[0]
			if callerDeadline {
				if ctx.Err() == nil || status.Samples != 0 || status.ErrorRate != 0 || status.Inflight != 0 {
					t.Fatalf("caller deadline reduced key health: %+v", status)
				}
			} else if ctx.Err() != nil || status.Samples != 1 || status.FailureClass != schemas.FailureClassTimeout {
				t.Fatalf("provider timeout was ignored: %+v", status)
			}
		})
	}
}

func TestAdaptiveKeyEditDiscardsOldInflightFailure(t *testing.T) {
	c, _ := testController()
	a := c.BeginProviderAttempt(testContext(), schemas.ProviderAttemptInfo{Provider: schemas.OpenAI, Model: "m", KeyID: "a"})
	c.ResetKey(schemas.OpenAI, "a")
	a.Observe(nil, responseError(401))
	a.End()
	if len(c.routes) != 0 {
		t.Fatal("old credential held its replacement")
	}
}

func TestAdaptiveWeightedModeAndFailOpen(t *testing.T) {
	c, _ := testController()
	config := c.Config()
	config.RouteSelectionEnabled = false
	c.Apply(config)
	keys := []schemas.Key{{ID: "a", Weight: 10}, {ID: "b", Weight: 0}}
	for i := 0; i < 20; i++ {
		k, _ := c.SelectKey(testContext(), keys, schemas.OpenAI, "m")
		if k.ID != "a" {
			t.Fatal("static weight was ignored")
		}
	}
	observe(c, schemas.OpenAI, "m", "a", responseError(401))
	k, _ := c.SelectKey(testContext(), keys, schemas.OpenAI, "m")
	if k.ID != "b" {
		t.Fatal("static mode used held key")
	}
	observe(c, schemas.OpenAI, "m", "b", responseError(401))
	if _, err := c.SelectKey(testContext(), keys, schemas.OpenAI, "m"); err != nil {
		t.Fatalf("all held failed closed: %v", err)
	}
	if _, err := c.SelectKey(testContext(), nil, schemas.OpenAI, "m"); err == nil {
		t.Fatal("empty pool was accepted")
	}
}

func TestAdaptiveStateBoundAndConcurrentStreams(t *testing.T) {
	c := New(DefaultConfig())
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				observe(c, schemas.OpenAI, "m", "a", nil)
				c.SelectKey(testContext(), testKeys, schemas.OpenAI, "m")
				c.Status()
			}
		}()
	}
	wg.Wait()
	if status := c.Status()[0]; status.Inflight != 0 || status.Samples != 3200 {
		t.Fatalf("lost updates: %+v", status)
	}
	for i := 0; i < maxRoutes+2; i++ {
		observe(c, schemas.OpenAI, fmt.Sprintf("m%d", i), "a", nil)
	}
	if len(c.routes) > maxRoutes {
		t.Fatalf("unbounded telemetry routes=%d", len(c.routes))
	}
}

func BenchmarkAdaptiveKeySelection(b *testing.B) {
	ctx := testContext()
	c := New(DefaultConfig())
	for _, key := range testKeys {
		observe(c, schemas.OpenAI, "m", key.ID, nil)
	}
	b.Run("native", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			keyselectors.WeightedRandom(ctx, testKeys, schemas.OpenAI, "m")
		}
	})
	b.Run("unconfigured", func(b *testing.B) {
		c.enabled.Store(false)
		b.ReportAllocs()
		for b.Loop() {
			ctx.ClearValue(schemas.BifrostContextKeyRoutingEngineLogs)
			c.SelectKey(ctx, testKeys, schemas.OpenAI, "m")
		}
	})
	b.Run("adaptive", func(b *testing.B) {
		c.enabled.Store(true)
		b.ReportAllocs()
		for b.Loop() {
			ctx.ClearValue(schemas.BifrostContextKeyRoutingEngineLogs)
			c.SelectKey(ctx, testKeys, schemas.OpenAI, "m")
		}
	})
	b.Run("attempt", func(b *testing.B) {
		response := &schemas.BifrostResponse{}
		b.ReportAllocs()
		for b.Loop() {
			a := c.BeginProviderAttempt(ctx, schemas.ProviderAttemptInfo{Provider: schemas.OpenAI, Model: "m", KeyID: "a"})
			a.Observe(response, nil)
			a.End()
		}
	})
}

func TestAdaptiveSelfLatencyDegradesEvenWithoutPeer(t *testing.T) {
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

func TestAdaptiveTransientCooldownReturnsReducedTrafficWithoutObservation(t *testing.T) {
	c, now := testController()
	for range 3 {
		observe(c, "p", "m", "a", responseError(503))
	}
	id := routeID{"p", "m", "a"}
	before := *c.routes[id]
	for _, tc := range []struct {
		at     time.Time
		state  string
		usable bool
	}{
		{before.FailedUntil.Add(-time.Nanosecond), "failed", false},
		{before.FailedUntil, "recovering", true},
		{before.FailedUntil.Add(time.Hour), "recovering", true},
	} {
		*now = tc.at
		for range 2 {
			status := c.Status()[0]
			if status.State != tc.state || (status.Score > 0) != tc.usable || status.Score >= 100 {
				t.Fatalf("cooldown state at %s: %+v", tc.at, status)
			}
			if after := *c.routes[id]; after != before {
				t.Fatalf("reading timed recovery changed observations: before=%+v after=%+v", before, after)
			}
		}
	}
	// The same timer never releases a provider refusal.
	observe(c, "p", "m", "a", responseError(401))
	*now = now.Add(time.Hour)
	if status := c.Status()[0]; status.State != "held" || status.Score != 0 {
		t.Fatalf("cooldown released a held key: %+v", status)
	}
}
