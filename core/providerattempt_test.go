package bifrost

import (
	"context"
	"net/http"
	"strings"
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

func (o *recordingProviderObserver) ProviderConfigured(provider schemas.ModelProvider, config *schemas.ProviderConfig) string {
	return string(provider) + ":" + config.NetworkConfig.BaseURL
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
			for _, a := range seen {
				if a.info.ProviderRevision == "" || a.info.Key.ID != a.info.KeyID {
					t.Fatalf("attempt lost actual provider/key configuration: %+v", a.info)
				}
			}
			requireHits(t, u, map[string]int{"sk-a": 1, "sk-b": 1})
		})
	}
}

func TestProviderAttemptObserverSeesTTFTFailureBeforeHealthyFallback(t *testing.T) {
	upstream := newTTFTUpstream()
	fallback := newTTFTUpstream()
	client := newTTFTTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { upstream.stall(r, time.Second) }), fallback.slowAnthropicHandler(0))
	observer := &recordingProviderObserver{done: make(chan *recordedProviderAttempt, 8)}
	client.providerAttemptObserver = observer
	ctx := ttftContext(30 * time.Millisecond)
	stream, err := client.ChatCompletionStreamRequest(ctx, ttftChatRequest(anthropicFallback))
	if err != nil {
		t.Fatal(err.GetErrorString())
	}
	for chunk := range stream {
		if chunk.BifrostError != nil {
			t.Fatal(chunk.BifrostError.GetErrorString())
		}
	}
	seen := map[schemas.ModelProvider]*recordedProviderAttempt{}
	for i := 0; i < 2; i++ {
		select {
		case a := <-observer.done:
			seen[a.info.Provider] = a
		case <-time.After(2 * time.Second):
			t.Fatal("stream attempt did not finish")
		}
	}
	if ctx.Err() != nil {
		t.Fatalf("TTFT cancelled caller context: %v", ctx.Err())
	}
	if a := seen[schemas.OpenAI]; a == nil || a.failure != schemas.FailureClassTimeout {
		t.Fatalf("TTFT header refusal not observed as timeout: %+v", a)
	}
	if a := seen[schemas.Anthropic]; a == nil || !a.success || a.failure != "" {
		t.Fatalf("healthy fallback not observed: %+v", a)
	}
}

func BenchmarkProviderAttemptObserverDisabled(b *testing.B) {
	client := &Bifrost{}
	ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	key := schemas.Key{ID: "key"}
	b.ReportAllocs()
	for b.Loop() {
		client.beginProviderAttempt(ctx, schemas.OpenAI, "model", key, nil, "")
	}
}

func TestExplicitKeySelectionExplainsNativePin(t *testing.T) {
	keys := []schemas.Key{{ID: "first", Name: "First", Value: *schemas.NewSecretVar("sk-first"), Models: []string{"*"}}, {ID: "chosen", Name: "Chosen", Value: *schemas.NewSecretVar("sk-chosen"), Models: []string{"*"}}}
	account := NewMockAccount()
	account.AddProvider(schemas.OpenAI, 1, 10)
	account.SetKeysForProvider(schemas.OpenAI, keys)
	client := &Bifrost{account: account, logger: NewDefaultLogger(schemas.LogLevelError)}
	for _, byName := range []bool{false, true} {
		for _, multi := range []bool{false, true} {
			ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
			if byName {
				ctx.SetValue(schemas.BifrostContextKeyAPIKeyName, "Chosen")
			} else {
				ctx.SetValue(schemas.BifrostContextKeyAPIKeyID, "chosen")
			}
			var selected []schemas.Key
			var err error
			if multi {
				selected, err = pinKeys(ctx, keys, schemas.OpenAI)
			} else {
				var canRotate bool
				selected, canRotate, err = client.selectKeyFromProviderForModelWithPool(ctx, schemas.ChatCompletionRequest, schemas.OpenAI, "model", schemas.OpenAI)
				if canRotate {
					t.Fatal("explicit pin became a rotating pool")
				}
			}
			if err != nil || len(selected) != 1 || selected[0].ID != "chosen" {
				t.Fatalf("pin changed: %+v %v", selected, err)
			}
			logs := ctx.GetRoutingEngineLogs()
			if len(logs) != 1 || logs[0].Engine != schemas.RoutingEngineCore || !strings.Contains(strings.ToLower(logs[0].Message), "explicit") || !strings.Contains(logs[0].Message, "Chosen") {
				t.Fatalf("native pin lost its explanation: %+v", logs)
			}
		}
	}
}
