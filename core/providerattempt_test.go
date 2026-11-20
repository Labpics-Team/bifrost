package bifrost

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
)

type recordedProviderAttempt struct {
	info    schemas.ProviderAttemptInfo
	failure schemas.FailureClass
	success bool
	ended   bool
	mu      sync.Mutex
	done    chan *recordedProviderAttempt
}

func (a *recordedProviderAttempt) Observe(resp *schemas.BifrostResponse, err *schemas.BifrostError) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err != nil {
		a.failure = ClassifyFailure(err)
	}
	if resp != nil {
		a.success = true
	}
}
func (a *recordedProviderAttempt) End() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.ended {
		a.ended = true
		a.done <- a
	}
}

type recordingProviderObserver struct{ done chan *recordedProviderAttempt }

func (o *recordingProviderObserver) BeginProviderAttempt(_ *schemas.BifrostContext, info schemas.ProviderAttemptInfo) schemas.ProviderAttempt {
	return &recordedProviderAttempt{info: info, done: o.done}
}

func TestProviderAttemptObserverSeesEachRetryAndStreamingTeardown(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(map[bool]string{false: "unary", true: "stream"}[streaming], func(t *testing.T) {
			u := newScenarioUpstream(t)
			u.answer("sk-a", 401)
			account := scenarioAccount(u, map[schemas.ModelProvider][]schemas.Key{provA: {scenarioKey("a", 1), scenarioKey("b", 1)}})
			observer := &recordingProviderObserver{done: make(chan *recordedProviderAttempt, 8)}
			client, err := Init(context.Background(), schemas.BifrostConfig{Account: account, Logger: NewDefaultLogger(schemas.LogLevelError), ProviderAttemptObserver: observer, KeySelector: func(_ *schemas.BifrostContext, keys []schemas.Key, _ schemas.ModelProvider, _ string) (schemas.Key, error) {
				return keys[0], nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Shutdown()
			ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
			if streaming {
				ch, e := client.ChatCompletionStreamRequest(ctx, scenarioRequest(provA, "m", nil))
				if e != nil {
					t.Fatal(e.GetErrorString())
				}
				for chunk := range ch {
					if chunk.BifrostError != nil {
						t.Fatal(chunk.BifrostError.GetErrorString())
					}
				}
			} else {
				_, e := client.ChatCompletionRequest(ctx, scenarioRequest(provA, "m", nil))
				if e != nil {
					t.Fatal(e.GetErrorString())
				}
			}
			seen := map[string]*recordedProviderAttempt{}
			for i := 0; i < 2; i++ {
				select {
				case a := <-observer.done:
					seen[a.info.KeyID] = a
				case <-time.After(2 * time.Second):
					t.Fatal("provider attempt never ended")
				}
			}
			if seen["a"] == nil || seen["a"].failure != schemas.FailureClassCredential || seen["b"] == nil || !seen["b"].success || seen["b"].failure != "" {
				t.Fatalf("incorrect attempt observations: %+v", seen)
			}
			requireHits(t, u, map[string]int{"sk-a": 1, "sk-b": 1})
		})
	}
}

func BenchmarkProviderAttemptObserverDisabled(b *testing.B) {
	client := &Bifrost{}
	ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	key := schemas.Key{ID: "key"}
	b.ReportAllocs()
	for b.Loop() {
		client.beginProviderAttempt(ctx, schemas.OpenAI, "model", key, nil)
	}
}
