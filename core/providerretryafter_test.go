package bifrost

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
)

func retryAfterTestClient(t *testing.T, baseURL string, keyCount int) *Bifrost {
	t.Helper()
	account := NewMockAccount()
	account.AddProviderWithBaseURL(schemas.OpenAI, 1, 1, baseURL)
	config := account.configs[schemas.OpenAI]
	config.NetworkConfig.MaxRetries = 1
	config.NetworkConfig.RetryBackoffInitial = time.Millisecond
	config.NetworkConfig.RetryBackoffMax = time.Millisecond
	var keys []schemas.Key
	for i := 0; i < keyCount; i++ {
		id := fmt.Sprintf("key-%d", i)
		keys = append(keys, schemas.Key{ID: id, Value: *schemas.NewSecretVar(id), Models: schemas.WhiteList{"*"}, Weight: 100})
	}
	account.SetKeysForProvider(schemas.OpenAI, keys)
	return newStreamTestClient(t, account)
}

func writeRetryAfterTestSuccess(w http.ResponseWriter, stream bool) {
	if stream {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"id\":\"done\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"useful\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
		return
	}
	fmt.Fprint(w, "{\"id\":\"done\",\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":\"useful\"},\"finish_reason\":\"stop\"}]}")
}

// Use the real provider parser and retry loop, with a backoff cap far below
// the provider hint. A 429 must keep the hint even when it rotates the key.
func TestProviderRetryAfterWaitsForHint(t *testing.T) {
	for _, tc := range []struct {
		name   string
		stream bool
		keys   int
	}{
		{"seconds", false, 1},
		{"http-date", true, 1},
		{"milliseconds with key rotation", true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type attempt struct {
				at        time.Time
				notBefore time.Time
				key       string
			}
			events := make(chan attempt, 3)
			var sends atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				event := attempt{at: time.Now(), key: r.Header.Get("Authorization")}
				if sends.Add(1) == 1 {
					switch tc.name {
					case "seconds":
						w.Header().Set("Retry-After", "1")
						event.notBefore = time.Now().Add(time.Second)
					case "http-date":
						event.notBefore = time.Now().Add(2 * time.Second).Truncate(time.Second)
						w.Header().Set("Retry-After", event.notBefore.UTC().Format(http.TimeFormat))
					default:
						w.Header().Set("retry-after-ms", "1100")
						event.notBefore = time.Now().Add(1100 * time.Millisecond)
					}
					events <- event
					w.WriteHeader(http.StatusTooManyRequests)
					fmt.Fprint(w, "{\"error\":{\"message\":\"rate limited\",\"type\":\"rate_limit_error\"}}")
					return
				}
				events <- event
				writeRetryAfterTestSuccess(w, tc.stream)
			}))
			defer upstream.Close()
			client := retryAfterTestClient(t, upstream.URL, tc.keys)
			ctx := ttftContext(0)
			if tc.stream {
				stream, err := client.ChatCompletionStreamRequest(ctx, ttftChatRequest())
				if err != nil {
					t.Fatal(err)
				}
				if content, errs := drainChatStream(stream); content != "useful" || len(errs) != 0 {
					t.Fatalf("retry result: content=%q errors=%v", content, errs)
				}
			} else {
				response, err := client.ChatCompletionRequest(ctx, ttftChatRequest())
				if err != nil || response == nil || *response.Choices[0].Message.Content.ContentStr != "useful" {
					t.Fatalf("retry result: response=%v error=%v", response, err)
				}
			}
			if sends.Load() != 2 {
				t.Fatalf("provider sends = %d, want 2", sends.Load())
			}
			first, retry := <-events, <-events
			if retry.at.Before(first.notBefore) {
				t.Fatalf("provider retry was %v early", first.notBefore.Sub(retry.at))
			}
			if tc.keys > 1 && first.key == retry.key {
				t.Fatal("fixture did not exercise key rotation")
			}
		})
	}
}

// Completion of a healthy follower on the same single worker proves that the
// cancelled operation stopped; returning an error alone cannot prove this.
func TestProviderRetryAfterCancellationReleasesWorker(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "caller cancellation"
		if deadline {
			name = "caller deadline"
		}
		t.Run(name, func(t *testing.T) {
			first := make(chan struct{}, 1)
			var originalSends atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					Model string
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if request.Model == "healthy-follower" {
					writeRetryAfterTestSuccess(w, true)
					return
				}
				originalSends.Add(1)
				w.Header().Set("Retry-After", "3600")
				w.WriteHeader(http.StatusTooManyRequests)
				fmt.Fprint(w, "{\"error\":{\"message\":\"rate limited\",\"type\":\"rate_limit_error\"}}")
				select {
				case first <- struct{}{}:
				default:
				}
			}))
			defer upstream.Close()
			client := retryAfterTestClient(t, upstream.URL, 2)
			parent, cancel := context.WithCancel(context.Background())
			if deadline {
				cancel()
				parent, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
			}
			defer cancel()
			ctx := schemas.NewBifrostContext(parent, schemas.NoDeadline)
			done := make(chan *schemas.BifrostError, 1)
			go func() {
				stream, err := client.ChatCompletionStreamRequest(ctx, ttftChatRequest())
				if stream != nil {
					for range stream {
					}
				}
				done <- err
			}()
			select {
			case <-first:
			case <-time.After(2 * time.Second):
				t.Fatal("provider did not receive the first attempt")
			}
			if !deadline {
				time.Sleep(50 * time.Millisecond)
				cancel()
			}
			wantType := schemas.RequestCancelled
			if deadline {
				wantType = schemas.RequestTimedOut
			}
			select {
			case err := <-done:
				if err == nil || err.Error == nil || err.Error.Type == nil || *err.Error.Type != wantType {
					t.Fatalf("cancelled hint wait returned %v, want %s", err, wantType)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("cancelled hint wait did not return")
			}
			follower := ttftChatRequest()
			follower.Model = "healthy-follower"
			stream, err := client.ChatCompletionStreamRequest(
				schemas.NewBifrostContext(context.Background(), time.Now().Add(2*time.Second)), follower)
			if err != nil {
				t.Fatalf("healthy follower could not use the released worker: %v", err)
			}
			if content, errs := drainChatStream(stream); content != "useful" || len(errs) != 0 {
				t.Fatalf("healthy follower: content=%q errors=%v", content, errs)
			}
			if originalSends.Load() != 1 {
				t.Fatalf("cancelled operation sent %d provider requests, want 1", originalSends.Load())
			}
			trail, _ := ctx.Value(schemas.BifrostContextKeyAttemptTrail).([]schemas.KeyAttemptRecord)
			if len(trail) != 1 {
				t.Fatalf("cancelled operation recorded %d attempts, want 1", len(trail))
			}
		})
	}
}

// Fake time exercises the real loop without a multi-hour test. A later error
// without a hint must resume ordinary backoff, rather than reuse a stale hint.
func TestProviderRetryAfterBackoffBounds(t *testing.T) {
	for _, tc := range []struct {
		name     string
		hintMS   int64
		backoff  time.Duration
		firstMin time.Duration
		firstMax time.Duration
	}{
		{"absent", 0, time.Millisecond, 800 * time.Microsecond, time.Millisecond},
		{"negative", -1, time.Millisecond, 800 * time.Microsecond, time.Millisecond},
		{"one hour", time.Hour.Milliseconds(), time.Millisecond, time.Hour, time.Hour},
		{"backoff exceeds hint", 1000, 2 * time.Second, 1600 * time.Millisecond, 2 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
				ctx.SetValue(schemas.BifrostContextKeyTracer, &schemas.NoOpTracer{})
				config := createTestConfig(2, tc.backoff, tc.backoff)
				var calls []time.Time
				handler := func(_ schemas.Key) (string, *schemas.BifrostError) {
					calls = append(calls, time.Now())
					if len(calls) == 3 {
						return "useful", nil
					}
					err := createBifrostError("rate limited", schemas.Ptr(429), nil, false)
					if len(calls) == 1 {
						err.ExtraFields.RetryAfter = tc.hintMS
					}
					return "", err
				}
				result, err := executeRequestWithRetries(ctx, config, handler, nil,
					schemas.ChatCompletionRequest, schemas.OpenAI, "test", nil, NewDefaultLogger(schemas.LogLevelError))
				if err != nil || result != "useful" || len(calls) != 3 {
					t.Fatalf("retry result: result=%q error=%v calls=%d", result, err, len(calls))
				}
				if gap := calls[1].Sub(calls[0]); gap < tc.firstMin || gap > tc.firstMax {
					t.Fatalf("first delay = %v, want [%v, %v]", gap, tc.firstMin, tc.firstMax)
				}
				if gap := calls[2].Sub(calls[1]); gap < tc.backoff*8/10 || gap > tc.backoff {
					t.Fatalf("hint leaked into subsequent attempt: delay=%v backoff=%v", gap, tc.backoff)
				}
			})
		})
	}
	t.Run("overflowing milliseconds still wait for cancellation", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			parent, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			ctx := schemas.NewBifrostContext(parent, schemas.NoDeadline)
			ctx.SetValue(schemas.BifrostContextKeyTracer, &schemas.NoOpTracer{})
			calls := 0
			handler := func(_ schemas.Key) (string, *schemas.BifrostError) {
				calls++
				err := createBifrostError("rate limited", schemas.Ptr(429), nil, false)
				err.ExtraFields.RetryAfter = 1<<63 - 1
				return "", err
			}
			start := time.Now()
			_, err := executeRequestWithRetries(ctx, createTestConfig(1, time.Millisecond, time.Millisecond), handler, nil,
				schemas.ChatCompletionRequest, schemas.OpenAI, "test", nil, NewDefaultLogger(schemas.LogLevelError))
			if calls != 1 || time.Since(start) != time.Hour || err == nil || err.Error == nil ||
				err.Error.Type == nil || *err.Error.Type != schemas.RequestTimedOut {
				t.Fatalf("extreme hint: calls=%d elapsed=%v error=%v", calls, time.Since(start), err)
			}
		})
	})
}
