package semanticcache

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bytedance/sonic"
	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/grant"
	"github.com/maximhq/bifrost/framework/vectorstore"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestResolveCacheKey pins that the cache key is the caller's own value. Tenant isolation is
// carried by the identity scope folded into the request hash, never by rewriting the key, so the
// stored cache_key stays the raw value and DELETE /api/cache/clear-by-key/{key} reaches every
// identity's entries under it. Pure-logic test - no vector store needed.
func TestResolveCacheKey(t *testing.T) {
	plugin := &Plugin{config: &Config{DefaultCacheKey: "shared-default-bucket"}}

	newCtx := func() *schemas.BifrostContext {
		return schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	}

	t.Run("no header falls back to default_cache_key", func(t *testing.T) {
		key, ok := plugin.resolveCacheKey(newCtx())
		if !ok || key != "shared-default-bucket" {
			t.Fatalf("got (%q, %v), want (\"shared-default-bucket\", true)", key, ok)
		}
	})

	t.Run("explicit header wins over default_cache_key", func(t *testing.T) {
		ctx := newCtx()
		ctx.SetValue(CacheKey, "tenant-x")
		key, ok := plugin.resolveCacheKey(ctx)
		if !ok || key != "tenant-x" {
			t.Fatalf("got (%q, %v), want (\"tenant-x\", true)", key, ok)
		}
	})

	t.Run("no cache key configured or supplied, caching skipped", func(t *testing.T) {
		bare := &Plugin{config: &Config{}}
		if _, ok := bare.resolveCacheKey(newCtx()); ok {
			t.Fatal("expected caching to be skipped with no cache key available")
		}
	})

	t.Run("a virtual key on the request leaves the key unchanged", func(t *testing.T) {
		ctx := newCtx()
		ctx.SetValue(schemas.BifrostContextKeyGovernanceVirtualKeyID, "vk-A")
		ctx.SetValue(CacheKey, "session-1")
		key, ok := plugin.resolveCacheKey(ctx)
		if !ok || key != "session-1" {
			t.Fatalf("got (%q, %v), want (\"session-1\", true)", key, ok)
		}
	})

	t.Run("a key shaped like vk:<id>:<key> is an ordinary key", func(t *testing.T) {
		ctx := newCtx()
		ctx.SetValue(CacheKey, "vk:vk-A:shared-default-bucket")
		key, ok := plugin.resolveCacheKey(ctx)
		if !ok || key != "vk:vk-A:shared-default-bucket" {
			t.Fatalf("got (%q, %v), want the key unchanged", key, ok)
		}
	})
}

// TestResolveCacheThreshold covers the floor: a caller may raise the similarity bar but
// never lower it below the operator's configured value.
func TestResolveCacheThreshold(t *testing.T) {
	logger := bifrost.NewDefaultLogger(schemas.LogLevelError)
	plugin := &Plugin{config: &Config{Threshold: 0.8}, logger: logger}

	newCtx := func() *schemas.BifrostContext {
		return schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	}

	tests := []struct {
		name      string
		override  any
		wantValue float64
	}{
		{"no override, uses configured threshold", nil, 0.8},
		{"override of 0 (defeat attempt) is floored at configured threshold", 0.0, 0.8},
		{"override below configured is floored at configured threshold", 0.5, 0.8},
		{"override above configured raises the bar", 0.95, 0.95},
		{"override exactly at configured threshold is a no-op", 0.8, 0.8},
		{"non-float override falls back to configured threshold", "not-a-float", 0.8},
		{"override above 1 is capped at 1", 1.7, 1.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := newCtx()
			if tt.override != nil {
				ctx.SetValue(CacheThresholdKey, tt.override)
			}
			got := plugin.resolveCacheThreshold(ctx)
			if got != tt.wantValue {
				t.Errorf("resolveCacheThreshold() = %v, want %v", got, tt.wantValue)
			}
		})
	}
}

// TestResponseHasToolCalls_ShellAndApplyPatch pins that shell and apply_patch calls
// count as client-executed tool calls, so cache_tool_call_responses=false skips them.
func TestResponseHasToolCalls_ShellAndApplyPatch(t *testing.T) {
	for _, itemType := range []schemas.ResponsesMessageType{
		schemas.ResponsesMessageTypeShellCall,
		schemas.ResponsesMessageTypeApplyPatchCall,
	} {
		t.Run(string(itemType), func(t *testing.T) {
			item := schemas.ResponsesMessage{Type: schemas.Ptr(itemType)}
			if !responseHasToolCalls(&schemas.BifrostResponse{ResponsesResponse: &schemas.BifrostResponsesResponse{Output: []schemas.ResponsesMessage{item}}}) {
				t.Errorf("non-stream %s not treated as a tool call", itemType)
			}
			if !responseHasToolCalls(&schemas.BifrostResponse{ResponsesStreamResponse: &schemas.BifrostResponsesStreamResponse{Item: &item}}) {
				t.Errorf("stream %s not treated as a tool call", itemType)
			}
		})
	}
}

// identityScopeTestPlugin builds a direct-only plugin over store. configJSON is decoded over the
// defaults the way the plugins API decodes a saved config, so a test can toggle settings by their
// wire names.
func identityScopeTestPlugin(t *testing.T, store vectorstore.VectorStore, configJSON string) *Plugin {
	t.Helper()
	config := &Config{
		Dimension:                    1,
		TTL:                          5 * time.Minute,
		Threshold:                    DefaultCacheThreshold,
		ConversationHistoryThreshold: DefaultConversationHistoryThreshold,
	}
	if configJSON != "" {
		if err := json.Unmarshal([]byte(configJSON), config); err != nil {
			t.Fatalf("decode config %s: %v", configJSON, err)
		}
	}
	return &Plugin{
		store:  store,
		config: config,
		logger: bifrost.NewDefaultLogger(schemas.LogLevelError),
		stopCh: make(chan struct{}),
	}
}

// identityRef names who a test request is made by. Empty fields mean "not known".
type identityRef struct {
	vk   string
	user string
}

// identityContext builds a request context the way the HTTP transport and governance leave it, with
// the identity on the request's grant. A zero identityRef is a request with no identity at all.
func identityContext(t *testing.T, cacheKey string, who identityRef) *schemas.BifrostContext {
	t.Helper()
	ctx := withTestRequestID(schemas.NewBifrostContextWithValue(context.Background(), schemas.NoDeadline, CacheKey, cacheKey))
	var vk *schemas.EntityRef
	if who.vk != "" {
		vk = &schemas.EntityRef{ID: who.vk, Name: who.vk}
	}
	var user *schemas.UserRef
	if who.user != "" {
		user = &schemas.UserRef{ID: who.user}
	}
	g := grant.New()
	g.SetIdentity(grant.NewIdentity(schemas.Credential{}, user, vk, nil, nil, nil, nil))
	if !ctx.SetGrant(g) {
		t.Fatal("SetGrant refused the test grant")
	}
	return ctx
}

func identityTestChatRequest(prompt string, extraParams map[string]interface{}) *schemas.BifrostRequest {
	req := newCrossProviderChatRequest(schemas.OpenAI, "gpt-4o-mini", schemas.ChatCompletionRequest, prompt)
	if extraParams != nil {
		req.ChatRequest.Params = &schemas.ChatParameters{ExtraParams: extraParams}
	}
	return req
}

func identityTestChatResponse(content string) *schemas.BifrostResponse {
	return &schemas.BifrostResponse{
		ChatResponse: &schemas.BifrostChatResponse{
			ID: "identity-scope-test",
			Choices: []schemas.BifrostResponseChoice{{
				ChatNonStreamResponseChoice: &schemas.ChatNonStreamResponseChoice{
					Message: &schemas.ChatMessage{
						Role:    schemas.ChatMessageRoleAssistant,
						Content: &schemas.ChatMessageContent{ContentStr: bifrost.Ptr(content)},
					},
				},
			}},
			ExtraFields: schemas.BifrostResponseExtraFields{
				Provider:               schemas.OpenAI,
				OriginalModelRequested: "gpt-4o-mini",
				ResolvedModelUsed:      "gpt-4o-mini",
				RequestType:            schemas.ChatCompletionRequest,
			},
		},
	}
}

// writeThroughCache runs one request through PreLLMHook (which must miss) and PostLLMHook, then
// waits for the async store write, so the next lookup sees the entry.
func writeThroughCache(t *testing.T, plugin *Plugin, ctx *schemas.BifrostContext, req *schemas.BifrostRequest) {
	t.Helper()
	_, shortCircuit, err := plugin.PreLLMHook(ctx, req)
	if err != nil {
		t.Fatalf("write PreLLMHook: %v", err)
	}
	if shortCircuit != nil {
		t.Fatal("write request unexpectedly hit the cache")
	}
	if _, _, err := plugin.PostLLMHook(ctx, identityTestChatResponse("cached answer"), nil); err != nil {
		t.Fatalf("write PostLLMHook: %v", err)
	}
	plugin.WaitForPendingOperations()
}

// cacheHits reports whether a lookup for req under ctx is served from the cache.
func cacheHits(t *testing.T, plugin *Plugin, ctx *schemas.BifrostContext, req *schemas.BifrostRequest) bool {
	t.Helper()
	_, shortCircuit, err := plugin.PreLLMHook(ctx, req)
	if err != nil {
		t.Fatalf("read PreLLMHook: %v", err)
	}
	plugin.clearCacheState(ctx.Value(schemas.BifrostContextKeyRequestID).(string))
	return shortCircuit != nil && shortCircuit.Response != nil
}

// TestCacheWriteKeepsDefaultEncoding pins that cache writes keep the pre-fix
// sonic.Marshal bytes: only Chat/Text responses with an explicit zero go through the
// usage wrapper, and even that keeps sonic's default (no HTML escaping) encoding.
func TestCacheWriteKeepsDefaultEncoding(t *testing.T) {
	const model = "fixture<b>&</b>"
	usage := &schemas.BifrostLLMUsage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5}
	for _, tc := range []struct {
		name string
		res  *schemas.BifrostResponse
	}{
		{"nonzero chat", &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{ID: "c", Model: model, Usage: usage}}},
		{"nonzero text", &schemas.BifrostResponse{TextCompletionResponse: &schemas.BifrostTextCompletionResponse{ID: "t", Model: model, Usage: usage}}},
		{"responses", &schemas.BifrostResponse{ResponsesResponse: &schemas.BifrostResponsesResponse{Model: model}}},
		{"embedding", &schemas.BifrostResponse{EmbeddingResponse: &schemas.BifrostEmbeddingResponse{Model: model}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want, err := sonic.Marshal(tc.res)
			require.NoError(t, err)
			got, err := marshalCachedResponse(tc.res)
			require.NoError(t, err)
			require.Equal(t, string(want), string(got))
		})
	}
	t.Run("explicit zero chat", func(t *testing.T) {
		zero := &schemas.BifrostLLMUsage{PromptTokensExplicitZero: true, CompletionTokensExplicitZero: true}
		got, err := marshalCachedResponse(&schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{ID: "z", Model: model, Usage: zero}})
		require.NoError(t, err)
		require.Contains(t, string(got), `"model":"`+model+`"`, "wrapper keeps default encoding")
		require.Contains(t, string(got), `"prompt_tokens":0`)
		require.Contains(t, string(got), `"completion_tokens":0`)
	})
}

// Exercise actual writes and hits, including SSE replay, after a JSON round trip.
func TestCompletionUsageSurvivesCache(t *testing.T) {
	for _, text := range []bool{false, true} {
		for _, streaming := range []bool{false, true} {
			for _, tc := range []struct {
				name  string
				usage *schemas.BifrostLLMUsage
				want  string
			}{
				{"zero", &schemas.BifrostLLMUsage{PromptTokensExplicitZero: true, CompletionTokensExplicitZero: true}, `{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}`},
				{"partial", &schemas.BifrostLLMUsage{PromptTokensExplicitZero: true, TotalTokens: 12}, `{"prompt_tokens":0,"total_tokens":12}`},
				{"missing", &schemas.BifrostLLMUsage{TotalTokens: 12}, `{"total_tokens":12}`},
			} {
				name := fmt.Sprintf("text=%t/stream=%t/%s", text, streaming, tc.name)
				t.Run(name, func(t *testing.T) {
					plugin := identityScopeTestPlugin(t, newObservableStore(), "")
					key := keyForTest(t, "")
					req := identityTestChatRequest("cache explicit zero counts", nil)
					response := identityTestChatResponse("answer")
					if streaming {
						req.RequestType = schemas.ChatCompletionStreamRequest
						response = newChatStreamChunk(0, "answer", nil)
					}
					response.ChatResponse.Usage = tc.usage
					if text {
						req = &schemas.BifrostRequest{RequestType: schemas.TextCompletionRequest, TextCompletionRequest: &schemas.BifrostTextCompletionRequest{Provider: schemas.OpenAI, Model: "gpt-4o-mini", Input: &schemas.TextCompletionInput{PromptStr: schemas.Ptr("cache explicit zero counts")}}}
						if streaming {
							req.RequestType = schemas.TextCompletionStreamRequest
						}
						response = &schemas.BifrostResponse{TextCompletionResponse: response.ChatResponse.ToTextCompletionResponse()}
						response.TextCompletionResponse.ExtraFields.RequestType = req.RequestType
					}
					seed := identityContext(t, key, identityRef{})
					_, sc, err := plugin.PreLLMHook(seed, req)
					require.Nil(t, err)
					require.Nil(t, sc, "expected cache miss")
					if streaming {
						seed.SetValue(schemas.BifrostContextKeyStreamEndIndicator, true)
					}
					_, _, err = plugin.PostLLMHook(seed, response, nil)
					require.Nil(t, err)
					plugin.WaitForPendingOperations()
					_, sc, err = plugin.PreLLMHook(identityContext(t, key, identityRef{}), req)
					require.Nil(t, err)
					require.NotNil(t, sc, "expected cache hit")
					check := func(view any) {
						t.Helper()
						wire, err := schemas.MarshalSorted(view)
						require.NoError(t, err)
						var body struct{ Usage json.RawMessage }
						require.NoError(t, json.Unmarshal(wire, &body))
						require.JSONEq(t, tc.want, string(body.Usage))
					}
					if streaming {
						require.NotNil(t, sc.Stream)
						count := 0
						for chunk := range sc.Stream {
							count++
							check(chunk)
						}
						require.Equal(t, 1, count)
					} else {
						require.NotNil(t, sc.Response)
						if text {
							require.NotNil(t, sc.Response.TextCompletionResponse)
							check(sc.Response.TextCompletionResponse.WithPreservedUsage())
						} else {
							require.NotNil(t, sc.Response.ChatResponse)
							check(sc.Response.ChatResponse.WithPreservedUsage())
						}
					}
				})
			}
		}
	}
}

func BenchmarkCachedCompletionUsage(b *testing.B) {
	for _, tc := range []struct {
		name  string
		usage *schemas.BifrostLLMUsage
	}{
		{"nonzero", &schemas.BifrostLLMUsage{PromptTokens: 10, CompletionTokens: 2, TotalTokens: 12}},
		{"zero", &schemas.BifrostLLMUsage{PromptTokensExplicitZero: true, CompletionTokensExplicitZero: true}},
		{"missing", &schemas.BifrostLLMUsage{TotalTokens: 12}},
		{"responses", nil},
		{"embedding", nil},
	} {
		response := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{ID: "bench", Model: "fixture", Usage: tc.usage}}
		if tc.name == "responses" {
			response = &schemas.BifrostResponse{ResponsesResponse: &schemas.BifrostResponsesResponse{ID: schemas.Ptr("bench"), Model: "fixture", Usage: &schemas.ResponsesResponseUsage{InputTokens: 10, OutputTokens: 2, TotalTokens: 12}}}
		}
		if tc.name == "embedding" {
			response = &schemas.BifrostResponse{EmbeddingResponse: &schemas.BifrostEmbeddingResponse{Model: "fixture", Usage: &schemas.BifrostLLMUsage{PromptTokens: 10, TotalTokens: 10}, Data: []schemas.EmbeddingData{{Embedding: schemas.EmbeddingStruct{EmbeddingArray: []float64{0.25, 0.75}}}}}}
		}
		body, err := marshalCachedResponse(response)
		if err != nil {
			b.Fatal(err)
		}
		for _, after := range []bool{false, true} {
			version := "before"
			if after {
				version = "after"
			}
			b.Run(tc.name+"/write/"+version, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var err error
					if after {
						_, err = marshalCachedResponse(response)
					} else {
						_, err = sonic.Marshal(response)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run(tc.name+"/read/"+version, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var response schemas.BifrostResponse
					var err error
					if after {
						err = unmarshalCachedResponse(body, &response)
					} else {
						err = json.Unmarshal(body, &response)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// TestIdentityScopedDirectCache pins identity isolation on the direct path with scoping on (the
// default): an entry is only ever served to the identity the request's grant names, and the stored
// cache_key stays the caller's raw key.
func TestIdentityScopedDirectCache(t *testing.T) {
	const prompt = "Name the largest moon of Saturn."
	vkA := identityRef{vk: "vk-A"}
	vkB := identityRef{vk: "vk-B"}
	anonymous := identityRef{}

	t.Run("same virtual key hits, other identities miss", func(t *testing.T) {
		plugin := identityScopeTestPlugin(t, newObservableStore(), "")
		key := keyForTest(t, "")
		writeThroughCache(t, plugin, identityContext(t, key, vkA), identityTestChatRequest(prompt, nil))
		if !cacheHits(t, plugin, identityContext(t, key, vkA), identityTestChatRequest(prompt, nil)) {
			t.Fatal("same virtual key should hit its own entry")
		}
		if cacheHits(t, plugin, identityContext(t, key, vkB), identityTestChatRequest(prompt, nil)) {
			t.Fatal("another virtual key must not be served vk-A's entry")
		}
		if cacheHits(t, plugin, identityContext(t, key, anonymous), identityTestChatRequest(prompt, nil)) {
			t.Fatal("a request with no identity must not be served vk-A's entry")
		}
	})

	// The grant is the only source of identity. The deprecated per-field context keys are not
	// read, so a request that carries only those is a request with no identity.
	t.Run("legacy context keys alone carry no identity", func(t *testing.T) {
		plugin := identityScopeTestPlugin(t, newObservableStore(), "")
		key := keyForTest(t, "")
		legacyOnly := func(vk, user string) *schemas.BifrostContext {
			ctx := withTestRequestID(schemas.NewBifrostContextWithValue(context.Background(), schemas.NoDeadline, CacheKey, key))
			ctx.SetValue(schemas.BifrostContextKeyGovernanceVirtualKeyID, vk)
			ctx.SetValue(schemas.BifrostContextKeyUserID, user)
			return ctx
		}
		writeThroughCache(t, plugin, legacyOnly("vk-A", "user-1"), identityTestChatRequest(prompt, nil))
		if !cacheHits(t, plugin, legacyOnly("vk-B", "user-2"), identityTestChatRequest(prompt, nil)) {
			t.Fatal("legacy keys alone must not scope the entry: both requests carry no identity on their grant")
		}
		if cacheHits(t, plugin, identityContext(t, key, vkA), identityTestChatRequest(prompt, nil)) {
			t.Fatal("a request whose grant names vk-A must not be served an entry written with no identity")
		}
	})

	t.Run("user identity scopes the entry", func(t *testing.T) {
		plugin := identityScopeTestPlugin(t, newObservableStore(), "")
		key := keyForTest(t, "")
		writeThroughCache(t, plugin, identityContext(t, key, identityRef{user: "user-1"}), identityTestChatRequest(prompt, nil))
		if !cacheHits(t, plugin, identityContext(t, key, identityRef{user: "user-1"}), identityTestChatRequest(prompt, nil)) {
			t.Fatal("same user should hit their own entry")
		}
		if cacheHits(t, plugin, identityContext(t, key, identityRef{user: "user-2"}), identityTestChatRequest(prompt, nil)) {
			t.Fatal("another user must not be served user-1's entry")
		}
		if cacheHits(t, plugin, identityContext(t, key, anonymous), identityTestChatRequest(prompt, nil)) {
			t.Fatal("a request with no identity must not be served user-1's entry")
		}
	})

	t.Run("virtual key and user together form one identity", func(t *testing.T) {
		plugin := identityScopeTestPlugin(t, newObservableStore(), "")
		key := keyForTest(t, "")
		both := identityRef{vk: "vk-A", user: "user-1"}
		writeThroughCache(t, plugin, identityContext(t, key, both), identityTestChatRequest(prompt, nil))
		if !cacheHits(t, plugin, identityContext(t, key, both), identityTestChatRequest(prompt, nil)) {
			t.Fatal("same virtual key and user should hit their own entry")
		}
		if cacheHits(t, plugin, identityContext(t, key, vkA), identityTestChatRequest(prompt, nil)) {
			t.Fatal("the same virtual key under no user must not be served the user's entry")
		}
		if cacheHits(t, plugin, identityContext(t, key, identityRef{vk: "vk-A", user: "user-2"}), identityTestChatRequest(prompt, nil)) {
			t.Fatal("the same virtual key under another user must not be served the user's entry")
		}
	})

	t.Run("stored cache_key is the raw key for every identity", func(t *testing.T) {
		store := newObservableStore()
		plugin := identityScopeTestPlugin(t, store, "")
		key := keyForTest(t, "")
		for _, who := range []identityRef{vkA, vkB, anonymous} {
			writeThroughCache(t, plugin, identityContext(t, key, who), identityTestChatRequest(prompt, nil))
		}
		store.mu.Lock()
		defer store.mu.Unlock()
		if len(store.chunks) != 3 {
			t.Fatalf("expected one entry per identity, got %d", len(store.chunks))
		}
		for id, chunk := range store.chunks {
			if got := chunk.Properties["cache_key"]; got != key {
				t.Fatalf("entry %s stored cache_key %v, want the raw key %q so clear-by-key reaches it", id, got, key)
			}
		}
	})

	t.Run("a request body field named identity_scope cannot borrow another identity's entry", func(t *testing.T) {
		plugin := identityScopeTestPlugin(t, newObservableStore(), "")
		key := keyForTest(t, "")
		writeThroughCache(t, plugin, identityContext(t, key, vkA), identityTestChatRequest(prompt, nil))
		forged := identityTestChatRequest(prompt, map[string]interface{}{"identity_scope": map[string]interface{}{"vk": "vk-A"}})
		if cacheHits(t, plugin, identityContext(t, key, vkB), forged) {
			t.Fatal("vk-B claimed vk-A's scope through a body field and was served vk-A's entry")
		}
		if cacheHits(t, plugin, identityContext(t, key, anonymous), forged) {
			t.Fatal("an anonymous caller claimed vk-A's scope through a body field and was served vk-A's entry")
		}
	})
}

// TestIdentityScopingDisabledSharesEntries pins the opt-out: with disable_identity_scoping set,
// every identity shares entries by cache key again, and entries written in that mode are never
// served once scoping is back on.
func TestIdentityScopingDisabledSharesEntries(t *testing.T) {
	const prompt = "Name the largest moon of Jupiter."
	vkA := identityRef{vk: "vk-A"}
	vkB := identityRef{vk: "vk-B"}
	anonymous := identityRef{}

	t.Run("every identity shares an entry", func(t *testing.T) {
		plugin := identityScopeTestPlugin(t, newObservableStore(), `{"disable_identity_scoping": true}`)
		key := keyForTest(t, "")
		writeThroughCache(t, plugin, identityContext(t, key, vkA), identityTestChatRequest(prompt, nil))
		if !cacheHits(t, plugin, identityContext(t, key, vkB), identityTestChatRequest(prompt, nil)) {
			t.Fatal("with identity scoping disabled, vk-B should be served vk-A's entry")
		}
		if !cacheHits(t, plugin, identityContext(t, key, anonymous), identityTestChatRequest(prompt, nil)) {
			t.Fatal("with identity scoping disabled, a request with no identity should be served vk-A's entry")
		}
	})

	t.Run("shared entries are not served once scoping is back on", func(t *testing.T) {
		store := newObservableStore()
		shared := identityScopeTestPlugin(t, store, `{"disable_identity_scoping": true}`)
		scoped := identityScopeTestPlugin(t, store, "")
		key := keyForTest(t, "")
		writeThroughCache(t, shared, identityContext(t, key, anonymous), identityTestChatRequest(prompt, nil))
		if cacheHits(t, scoped, identityContext(t, key, anonymous), identityTestChatRequest(prompt, nil)) {
			t.Fatal("an entry written while sharing must not be served to an anonymous request once scoping is on")
		}
		if cacheHits(t, scoped, identityContext(t, key, vkB), identityTestChatRequest(prompt, nil)) {
			t.Fatal("an entry written while sharing must not be served to vk-B once scoping is on")
		}
	})
}

// semanticFilterStore records the filters of every similarity search and reports no match, so a
// test can see what a semantic lookup would be scoped by.
type semanticFilterStore struct {
	*observableStore
	searches [][]vectorstore.Query
}

func (s *semanticFilterStore) GetNearest(ctx context.Context, ns string, v []float32, q []vectorstore.Query, sf []string, th float64, lim int64) ([]vectorstore.SearchResult, error) {
	s.mu.Lock()
	s.searches = append(s.searches, append([]vectorstore.Query(nil), q...))
	s.mu.Unlock()
	return nil, nil
}

func filterValue(queries []vectorstore.Query, field string) (interface{}, bool) {
	for _, q := range queries {
		if q.Field == field {
			return q.Value, true
		}
	}
	return nil, false
}

// TestIdentityScopeSemanticSearchFilters pins identity isolation on the semantic path: a
// similarity search is filtered by the raw cache key and by a params hash that differs per
// identity, so a paraphrase from one identity can never match another identity's entry.
func TestIdentityScopeSemanticSearchFilters(t *testing.T) {
	store := &semanticFilterStore{observableStore: newObservableStore()}
	plugin := identityScopeTestPlugin(t, store, `{"provider": "openai", "embedding_model": "text-embedding-3-small", "dimension": 3}`)
	plugin.SetEmbeddingRequestExecutor(func(_ *schemas.BifrostContext, _ *schemas.BifrostEmbeddingRequest) (*schemas.BifrostEmbeddingResponse, *schemas.BifrostError) {
		return &schemas.BifrostEmbeddingResponse{
			Data: []schemas.EmbeddingData{{Embedding: schemas.EmbeddingStruct{EmbeddingArray: []float64{0.1, 0.2, 0.3}}}},
		}, nil
	})
	key := keyForTest(t, "")
	const prompt = "Name the tallest mountain in Africa."

	for _, who := range []identityRef{{vk: "vk-A"}, {vk: "vk-A"}, {vk: "vk-B"}} {
		ctx := identityContext(t, key, who)
		ctx.SetValue(CacheTypeKey, CacheTypeSemantic)
		if cacheHits(t, plugin, ctx, identityTestChatRequest(prompt, nil)) {
			t.Fatal("the recording store never matches, so no lookup should hit")
		}
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.searches) != 3 {
		t.Fatalf("expected 3 similarity searches, got %d", len(store.searches))
	}
	for i, search := range store.searches {
		if got, _ := filterValue(search, "cache_key"); got != key {
			t.Fatalf("search %d filtered cache_key on %v, want the raw key %q", i, got, key)
		}
	}
	hashA1, _ := filterValue(store.searches[0], "params_hash")
	hashA2, _ := filterValue(store.searches[1], "params_hash")
	hashB, _ := filterValue(store.searches[2], "params_hash")
	if hashA1 != hashA2 {
		t.Fatalf("the same identity must search the same params hash, got %v and %v", hashA1, hashA2)
	}
	if hashA1 == hashB {
		t.Fatalf("vk-A and vk-B searched the same params hash %v, so a paraphrase could cross identities", hashA1)
	}
}

// TestPreLLMHookRecordsCacheLookup pins the lookup outcome the plugin records for response
// headers. It is recorded during the lookup, before any response exists, because a stream's
// headers are sent before any chunk carries cache metadata; and it must agree with what
// cache_debug reports for the same request.
func TestPreLLMHookRecordsCacheLookup(t *testing.T) {
	const prompt = "Name the smallest planet in the solar system."
	anonymous := identityRef{}

	t.Run("a miss records the id the entry will be written under", func(t *testing.T) {
		plugin := identityScopeTestPlugin(t, newObservableStore(), "")
		ctx := identityContext(t, keyForTest(t, ""), anonymous)
		if _, sc, err := plugin.PreLLMHook(ctx, identityTestChatRequest(prompt, nil)); err != nil || sc != nil {
			t.Fatalf("expected a miss, got short-circuit %v err %v", sc, err)
		}
		lookup, ok := schemas.CacheLookupFromContext(ctx)
		if !ok {
			t.Fatal("expected the lookup outcome recorded during PreLLMHook")
		}
		if lookup.CacheHit || lookup.HitType != nil || lookup.CacheID == nil {
			t.Fatalf("expected a miss with a cache id and no hit type, got %+v", lookup)
		}

		res := identityTestChatResponse("answer")
		if _, _, err := plugin.PostLLMHook(ctx, res, nil); err != nil {
			t.Fatalf("PostLLMHook: %v", err)
		}
		plugin.WaitForPendingOperations()
		debug := res.ChatResponse.ExtraFields.CacheDebug
		if debug == nil || debug.CacheHit || debug.CacheID == nil || *debug.CacheID != *lookup.CacheID {
			t.Fatalf("cache_debug %+v does not name the entry the lookup recorded (%s)", debug, *lookup.CacheID)
		}
	})

	t.Run("a hit records what cache_debug reports", func(t *testing.T) {
		plugin := identityScopeTestPlugin(t, newObservableStore(), "")
		key := keyForTest(t, "")
		writeThroughCache(t, plugin, identityContext(t, key, anonymous), identityTestChatRequest(prompt, nil))

		ctx := identityContext(t, key, anonymous)
		_, sc, err := plugin.PreLLMHook(ctx, identityTestChatRequest(prompt, nil))
		if err != nil || sc == nil || sc.Response == nil {
			t.Fatalf("expected a hit, got short-circuit %v err %v", sc, err)
		}
		lookup, ok := schemas.CacheLookupFromContext(ctx)
		if !ok {
			t.Fatal("expected the lookup outcome recorded during PreLLMHook")
		}
		debug := sc.Response.ChatResponse.ExtraFields.CacheDebug
		if !lookup.CacheHit || lookup.HitType == nil || *lookup.HitType != string(CacheTypeDirect) {
			t.Fatalf("expected a direct hit, got %+v", lookup)
		}
		if *lookup.CacheID != *debug.CacheID || *lookup.RequestedProvider != *debug.RequestedProvider || *lookup.RequestedModel != *debug.RequestedModel {
			t.Fatalf("lookup %+v disagrees with cache_debug %+v", lookup, debug)
		}
		if lookup.CacheHitLatency == nil {
			t.Fatal("expected the hit latency recorded")
		}
	})

	t.Run("a stream hit is recorded before any chunk is read", func(t *testing.T) {
		store := newObservableStore()
		plugin := identityScopeTestPlugin(t, store, "")
		key := keyForTest(t, "")
		streamRequest := func() *schemas.BifrostRequest {
			return newCrossProviderChatRequest(schemas.OpenAI, "gpt-4o-mini", schemas.ChatCompletionStreamRequest, prompt)
		}

		seed := identityContext(t, key, anonymous)
		if _, sc, err := plugin.PreLLMHook(seed, streamRequest()); err != nil || sc != nil {
			t.Fatalf("expected a miss, got short-circuit %v err %v", sc, err)
		}
		entryID := plugin.getCacheState(seed.Value(schemas.BifrostContextKeyRequestID).(string)).DirectCacheID
		chunk, err := json.Marshal(identityTestChatResponse("streamed answer"))
		if err != nil {
			t.Fatalf("marshal chunk: %v", err)
		}
		store.mu.Lock()
		store.chunks[entryID] = vectorstore.SearchResult{ID: entryID, Properties: map[string]interface{}{
			"stream_chunks": []interface{}{string(chunk)},
			"expires_at":    time.Now().Add(time.Hour).Unix(),
		}}
		store.mu.Unlock()

		ctx := identityContext(t, key, anonymous)
		_, sc, err := plugin.PreLLMHook(ctx, streamRequest())
		if err != nil || sc == nil || sc.Stream == nil {
			t.Fatalf("expected a stream replay, got short-circuit %v err %v", sc, err)
		}
		lookup, ok := schemas.CacheLookupFromContext(ctx)
		for range sc.Stream {
		}
		if !ok || !lookup.CacheHit || lookup.HitType == nil || *lookup.HitType != string(CacheTypeDirect) || *lookup.CacheID != entryID {
			t.Fatalf("expected a direct hit on %s recorded before the replay was read, got %+v (recorded=%v)", entryID, lookup, ok)
		}
	})

	t.Run("a semantic search records its embedding usage", func(t *testing.T) {
		store := &semanticFilterStore{observableStore: newObservableStore()}
		plugin := identityScopeTestPlugin(t, store, `{"provider": "openai", "embedding_model": "text-embedding-3-small", "dimension": 3}`)
		plugin.SetEmbeddingRequestExecutor(func(_ *schemas.BifrostContext, _ *schemas.BifrostEmbeddingRequest) (*schemas.BifrostEmbeddingResponse, *schemas.BifrostError) {
			return &schemas.BifrostEmbeddingResponse{
				Data:  []schemas.EmbeddingData{{Embedding: schemas.EmbeddingStruct{EmbeddingArray: []float64{0.1, 0.2, 0.3}}}},
				Usage: &schemas.BifrostLLMUsage{TotalTokens: 7},
			}, nil
		})
		ctx := identityContext(t, keyForTest(t, ""), anonymous)
		if _, sc, err := plugin.PreLLMHook(ctx, identityTestChatRequest(prompt, nil)); err != nil || sc != nil {
			t.Fatalf("expected a miss, got short-circuit %v err %v", sc, err)
		}
		lookup, ok := schemas.CacheLookupFromContext(ctx)
		if !ok || lookup.ProviderUsed == nil || *lookup.ProviderUsed != "openai" || lookup.ModelUsed == nil || lookup.InputTokens == nil || *lookup.InputTokens != 7 {
			t.Fatalf("expected the embedding provider, model and tokens recorded, got %+v (recorded=%v)", lookup, ok)
		}
	})

	t.Run("no lookup, nothing recorded", func(t *testing.T) {
		plugin := identityScopeTestPlugin(t, newObservableStore(), "")
		ctx := withTestRequestID(schemas.NewBifrostContext(context.Background(), schemas.NoDeadline))
		if _, sc, err := plugin.PreLLMHook(ctx, identityTestChatRequest(prompt, nil)); err != nil || sc != nil {
			t.Fatalf("expected the cache to be skipped, got short-circuit %v err %v", sc, err)
		}
		if _, ok := schemas.CacheLookupFromContext(ctx); ok {
			t.Fatal("a request without a cache key ran no lookup, so none should be recorded")
		}
	})
}

// TestMain drops the shared test namespace BEFORE the run starts (in case a
// previous run was interrupted and left stale entries) AND once after — both
// matter: tests share one namespace + one cache_key prefix per t.Name(),
// so stale writes from a prior interrupted run would surface as spurious
// cache hits on the first request of the next run.
func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() || flag.Lookup("test.list").Value.String() != "" {
		os.Exit(m.Run())
	}
	dropSharedTestNamespace() // pre-run sweep
	code := m.Run()
	dropSharedTestNamespace() // post-run sweep
	os.Exit(code)
}

// dropSharedTestNamespace removes the shared test namespace from EVERY vector
// store backend the suite exercises - not just Weaviate. Redis, Qdrant, and
// Pinecone are persistent external services, so a deterministic per-t.Name()
// cache_key written by one run is still present on the next run (within TTL)
// and surfaces as a spurious cache hit on the first request. Sweeping all
// backends here is the suite's only cleanup, since clearTestKeysWithStore is a
// no-op. Stores that aren't configured/reachable in this environment are
// skipped silently.
func dropSharedTestNamespace() {
	logger := bifrost.NewDefaultLogger(schemas.LogLevelError)
	for _, tc := range getVectorStoreTestCases() {
		storeConfig, ok := storeConfigForType(tc.StoreType)
		if !ok {
			continue
		}
		func() {
			store, err := vectorstore.NewVectorStore(context.Background(), &vectorstore.Config{
				Type:    tc.StoreType,
				Config:  storeConfig,
				Enabled: true,
			}, logger)
			if err != nil {
				return // backend not configured/available in this environment
			}
			defer store.Close(context.Background(), SharedTestNamespace)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = store.DeleteNamespace(ctx, SharedTestNamespace)
		}()
	}
}

type cacheAvailabilityStore struct {
	*observableStore
	create  func(context.Context) error
	get     func(context.Context, string) (vectorstore.SearchResult, error)
	nearest func(context.Context) ([]vectorstore.SearchResult, error)
	add     func(context.Context) error
}

func (s *cacheAvailabilityStore) CreateNamespace(ctx context.Context, _ string, _ int, _ map[string]vectorstore.VectorStoreProperties) error {
	return s.create(ctx)
}
func (s *cacheAvailabilityStore) GetChunk(ctx context.Context, ns, id string) (vectorstore.SearchResult, error) {
	if s.get != nil {
		return s.get(ctx, id)
	}
	return s.observableStore.GetChunk(ctx, ns, id)
}
func (s *cacheAvailabilityStore) GetNearest(ctx context.Context, ns string, vector []float32, queries []vectorstore.Query, selectFields []string, threshold float64, limit int64) ([]vectorstore.SearchResult, error) {
	if s.nearest != nil {
		return s.nearest(ctx)
	}
	return s.observableStore.GetNearest(ctx, ns, vector, queries, selectFields, threshold, limit)
}
func (s *cacheAvailabilityStore) Add(ctx context.Context, ns, id string, embedding []float32, metadata map[string]interface{}) error {
	if s.add != nil {
		if err := s.add(ctx); err != nil {
			return err
		}
	}
	return s.observableStore.Add(ctx, ns, id, embedding, metadata)
}
func cacheAvailabilityRequest() *schemas.BifrostRequest {
	return newCrossProviderChatRequest(schemas.OpenAI, "synthetic-model", schemas.ChatCompletionRequest, "synthetic-input")
}
func cacheAvailabilityContext() *schemas.BifrostContext {
	return withTestRequestID(schemas.NewBifrostContextWithValue(context.Background(), schemas.NoDeadline, CacheKey, "synthetic-cache-key"))
}
func cacheAvailabilityInit(t *testing.T, store vectorstore.VectorStore, config *Config) *Plugin {
	t.Helper()
	initialized, err := Init(context.Background(), config, bifrost.NewDefaultLogger(schemas.LogLevelError), store)
	if err != nil {
		t.Fatal(err)
	}
	return initialized.(*Plugin)
}

func TestCacheAvailabilityStartupAndRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var available atomic.Bool
		var namespaceCalls atomic.Int32
		store := &cacheAvailabilityStore{observableStore: newObservableStore()}
		store.create = func(context.Context) error {
			namespaceCalls.Add(1)
			if !available.Load() {
				return context.DeadlineExceeded
			}
			return nil
		}
		plugin := cacheAvailabilityInit(t, store, &Config{Dimension: 1})
		defer plugin.Cleanup()
		ctx := cacheAvailabilityContext()
		defer ctx.Cancel()
		req := cacheAvailabilityRequest()
		got, hit, err := plugin.PreLLMHook(ctx, req)
		if err != nil || got != req || hit != nil || !plugin.namespaceUnavailable.Load() {
			t.Fatalf("unavailable namespace changed provider path: got=%p hit=%v error=%v", got, hit, err)
		}
		requestID := ctx.Value(schemas.BifrostContextKeyRequestID).(string)
		if plugin.getCacheState(requestID) != nil {
			t.Fatal("unavailable cache retained pending writes")
		}
		available.Store(true)
		time.Sleep(time.Second)
		synctest.Wait()
		if plugin.namespaceUnavailable.Load() || namespaceCalls.Load() != 2 {
			t.Fatal("namespace did not recover once without restart")
		}
		got, hit, err = plugin.PreLLMHook(ctx, req)
		if err != nil || got != req || hit != nil || plugin.getCacheState(requestID) == nil {
			t.Fatal("recovered namespace did not resume cache lookup")
		}
		response := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{
			ID: "synthetic-response", Model: "synthetic-model", Object: "chat.completion",
			ExtraFields: schemas.BifrostResponseExtraFields{RequestType: schemas.ChatCompletionRequest, Provider: schemas.OpenAI, OriginalModelRequested: "synthetic-model"},
		}}
		if _, _, err := plugin.PostLLMHook(ctx, response, nil); err != nil {
			t.Fatal(err)
		}
		plugin.WaitForPendingOperations()
		replayCtx := cacheAvailabilityContext()
		defer replayCtx.Cancel()
		_, replay, err := plugin.PreLLMHook(replayCtx, req)
		if err != nil || replay == nil {
			t.Fatalf("recovered cache did not replay saved provider response: %v", err)
		}
		metadata, ok := schemas.CacheLookupFromContext(replayCtx)
		if !ok || !metadata.CacheHit {
			t.Fatal("cache hit did not reach the original request context")
		}
	})
}

func TestCacheAvailabilityRejectsKnownSchemaConflict(t *testing.T) {
	store := &cacheAvailabilityStore{observableStore: newObservableStore(), create: func(context.Context) error { return errors.New("namespace has an incompatible dimension") }}
	plugin, err := Init(context.Background(), &Config{Dimension: 1}, bifrost.NewDefaultLogger(schemas.LogLevelError), store)
	if err == nil || plugin != nil {
		t.Fatal("known reachable schema conflict was accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	store.create = func(ctx context.Context) error { return ctx.Err() }
	plugin, err = Init(canceled, &Config{Dimension: 1}, bifrost.NewDefaultLogger(schemas.LogLevelError), store)
	if err == nil || plugin != nil {
		t.Fatal("canceled initialization started maintenance")
	}
}

func TestCacheAvailabilityHangingStartupAndCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attempts atomic.Int32
		var active atomic.Int32
		store := &cacheAvailabilityStore{observableStore: newObservableStore()}
		store.create = func(ctx context.Context) error {
			attempts.Add(1)
			if active.Add(1) != 1 {
				t.Error("namespace recovery overlapped")
			}
			defer active.Add(-1)
			<-ctx.Done()
			return ctx.Err()
		}
		start := time.Now()
		plugin := cacheAvailabilityInit(t, store, &Config{Dimension: 1})
		defer plugin.Cleanup()
		if elapsed := time.Since(start); elapsed != CreateNamespaceTimeout {
			t.Fatalf("namespace initialization deadline=%v", elapsed)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if attempts.Load() != 2 || active.Load() != 1 {
			t.Fatal("serial recovery did not start")
		}
		if err := plugin.Cleanup(); err != nil {
			t.Fatal(err)
		}
		if err := plugin.Cleanup(); err != nil {
			t.Fatal(err)
		}
		if active.Load() != 0 {
			t.Fatal("cleanup left an active namespace RPC")
		}
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if attempts.Load() != 2 {
			t.Fatal("namespace recovery survived Cleanup")
		}
	})
}

func TestCacheAvailabilityWriterAdmissionDuringRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attempts atomic.Int32
		var writes atomic.Int32
		recoveryStarted, release := make(chan struct{}), make(chan struct{})
		store := &cacheAvailabilityStore{observableStore: newObservableStore()}
		store.create = func(ctx context.Context) error {
			if attempts.Add(1) > 1 {
				close(recoveryStarted)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}
		plugin := cacheAvailabilityInit(t, store, &Config{Dimension: 1})
		defer plugin.Cleanup()
		if !plugin.cacheWriter("before", func(context.Context) { writes.Add(1) }) {
			t.Fatal("healthy namespace refused a write")
		}
		plugin.requestNamespaceRecovery()
		<-recoveryStarted
		if plugin.cacheWriter("during", func(context.Context) { writes.Add(100) }) {
			t.Fatal("unavailable namespace admitted a new write")
		}
		close(release)
		synctest.Wait()
		if !plugin.cacheWriter("after", func(context.Context) { writes.Add(1) }) {
			t.Fatal("recovered namespace refused a write")
		}
		plugin.WaitForPendingOperations()
		if got := writes.Load(); got != 2 {
			t.Fatalf("writes = %d, want pre-recovery and recovered writes", got)
		}
	})
}

func TestCacheAvailabilityCallerCancellationPreservesNamespace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var embeddings atomic.Int32
		var namespaceCalls atomic.Int32
		store := &cacheAvailabilityStore{observableStore: newObservableStore(), create: func(context.Context) error { namespaceCalls.Add(1); return nil }}
		store.get = func(ctx context.Context, _ string) (vectorstore.SearchResult, error) {
			<-ctx.Done()
			return vectorstore.SearchResult{}, ctx.Err()
		}
		plugin := cacheAvailabilityInit(t, store, &Config{Provider: schemas.OpenAI, EmbeddingModel: "synthetic-embedding", Dimension: 4})
		defer plugin.Cleanup()
		plugin.SetEmbeddingRequestExecutor(func(*schemas.BifrostContext, *schemas.BifrostEmbeddingRequest) (*schemas.BifrostEmbeddingResponse, *schemas.BifrostError) {
			embeddings.Add(1)
			return nil, nil
		})
		parent, cancelParent := context.WithCancel(context.Background())
		defer cancelParent()
		ctx := withTestRequestID(schemas.NewBifrostContextWithValue(parent, schemas.NoDeadline, CacheKey, "synthetic-cache-key"))
		defer ctx.Cancel()
		go func() { time.Sleep(10 * time.Millisecond); cancelParent() }()
		req := cacheAvailabilityRequest()
		start := time.Now()
		got, hit, err := plugin.PreLLMHook(ctx, req)
		if elapsed := time.Since(start); elapsed != 10*time.Millisecond {
			t.Fatalf("caller cancellation took %v", elapsed)
		}
		if err != nil || got != req || hit != nil || ctx.Err() != context.Canceled {
			t.Fatal("cache lookup did not preserve caller cancellation")
		}
		if embeddings.Load() != 0 {
			t.Fatal("canceled direct lookup still dispatched embedding")
		}
		if plugin.getCacheState(ctx.Value(schemas.BifrostContextKeyRequestID).(string)) != nil {
			t.Fatal("canceled lookup left a cache writer")
		}
		synctest.Wait()
		if namespaceCalls.Load() != 1 || plugin.namespaceUnavailable.Load() {
			t.Fatal("caller cancellation invalidated the ready namespace")
		}
	})
}

func TestCacheAvailabilityEmbeddingKeepsCallerDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deadline := time.Now().Add(time.Second)
		store := &cacheAvailabilityStore{observableStore: newObservableStore(), create: func(context.Context) error { return nil }}
		nearestCalled := false
		store.nearest = func(ctx context.Context) ([]vectorstore.SearchResult, error) {
			nearestCalled = true
			if got, ok := ctx.Deadline(); !ok || !got.Equal(time.Now().Add(cacheStoreLookupTimeout)) || ctx.Err() != nil {
				t.Error("semantic store lookup did not receive a fresh availability budget")
			}
			time.Sleep(100 * time.Millisecond)
			if ctx.Err() != nil {
				t.Error("embedding time exhausted the following store lookup")
			}
			return nil, nil
		}
		plugin := cacheAvailabilityInit(t, store, &Config{Provider: schemas.OpenAI, EmbeddingModel: "synthetic-embedding", Dimension: 4})
		defer plugin.Cleanup()
		plugin.SetEmbeddingRequestExecutor(func(ctx *schemas.BifrostContext, _ *schemas.BifrostEmbeddingRequest) (*schemas.BifrostEmbeddingResponse, *schemas.BifrostError) {
			if got, ok := ctx.Deadline(); !ok || !got.Equal(deadline) {
				t.Error("embedding received a shorter deadline than its caller")
			}
			timer := time.NewTimer(350 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return nil, &schemas.BifrostError{Error: &schemas.ErrorField{Message: ctx.Err().Error()}}
			case <-timer.C:
				return &schemas.BifrostEmbeddingResponse{Data: []schemas.EmbeddingData{{Embedding: schemas.EmbeddingStruct{EmbeddingArray: []float64{0.1, 0.2, 0.3, 0.4}}}}}, nil
			}
		})
		ctx := withTestRequestID(schemas.NewBifrostContextWithValue(context.Background(), deadline, CacheKey, "synthetic-cache-key"))
		defer ctx.Cancel()
		req := cacheAvailabilityRequest()
		start := time.Now()
		got, hit, err := plugin.PreLLMHook(ctx, req)
		if err != nil || got != req || hit != nil || ctx.Err() != nil || !nearestCalled {
			t.Fatal("valid embedding did not reach semantic search within the caller deadline")
		}
		if elapsed := time.Since(start); elapsed != 450*time.Millisecond {
			t.Fatalf("semantic search ended after %v", elapsed)
		}
		state := plugin.getCacheState(ctx.Value(schemas.BifrostContextKeyRequestID).(string))
		if state == nil || state.PlaceholderEmbeddings || len(state.Embeddings) != 4 {
			t.Fatal("completed embedding was not retained for cache storage")
		}
	})
}

func TestCacheAvailabilityStoreReadDeadlines(t *testing.T) {
	for _, tc := range []struct {
		name           string
		semantic       bool
		callerDeadline bool
	}{
		{"direct store timeout", false, false},
		{"semantic store timeout after embedding", true, false},
		{"direct earlier caller deadline", false, true},
		{"semantic earlier caller deadline", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var namespaceCalls, embeddings atomic.Int32
				store := &cacheAvailabilityStore{observableStore: newObservableStore()}
				store.create = func(ctx context.Context) error {
					if namespaceCalls.Add(1) > 1 {
						<-ctx.Done()
						return ctx.Err()
					}
					return nil
				}
				store.get = func(ctx context.Context, _ string) (vectorstore.SearchResult, error) {
					<-ctx.Done()
					return vectorstore.SearchResult{}, ctx.Err()
				}
				store.nearest = func(ctx context.Context) ([]vectorstore.SearchResult, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				plugin := cacheAvailabilityInit(t, store, &Config{Provider: schemas.OpenAI, EmbeddingModel: "synthetic-embedding", Dimension: 4})
				defer plugin.Cleanup()
				plugin.SetEmbeddingRequestExecutor(func(*schemas.BifrostContext, *schemas.BifrostEmbeddingRequest) (*schemas.BifrostEmbeddingResponse, *schemas.BifrostError) {
					embeddings.Add(1)
					time.Sleep(350 * time.Millisecond)
					return &schemas.BifrostEmbeddingResponse{Data: []schemas.EmbeddingData{{Embedding: schemas.EmbeddingStruct{EmbeddingArray: []float64{1, 0, 0, 0}}}}}, nil
				})
				elapsed := cacheStoreLookupTimeout
				if tc.callerDeadline {
					elapsed = 50 * time.Millisecond
				}
				if tc.semantic {
					elapsed += 350 * time.Millisecond
				}
				deadline := time.Now().Add(2 * time.Second)
				if tc.callerDeadline {
					deadline = time.Now().Add(elapsed)
				}
				ctx := withTestRequestID(schemas.NewBifrostContextWithValue(context.Background(), deadline, CacheKey, "synthetic-cache-key"))
				defer ctx.Cancel()
				if tc.semantic {
					ctx.SetValue(CacheTypeKey, CacheTypeSemantic)
				}
				req := cacheAvailabilityRequest()
				start := time.Now()
				got, hit, err := plugin.PreLLMHook(ctx, req)
				if got != req || hit != nil || err != nil || time.Since(start) != elapsed {
					t.Fatalf("blocked store did not return the provider request after %v: %v", elapsed, err)
				}
				if tc.callerDeadline {
					if ctx.Err() != context.DeadlineExceeded {
						t.Fatal("store lookup lost the earlier caller deadline")
					}
				} else if ctx.Err() != nil {
					t.Fatal("store timeout canceled the provider request")
				}
				if (embeddings.Load() == 1) != tc.semantic {
					t.Fatal("failed direct lookup dispatched an embedding, or semantic lookup skipped it")
				}
				if plugin.getCacheState(ctx.Value(schemas.BifrostContextKeyRequestID).(string)) != nil {
					t.Fatal("failed store read left a cache writer")
				}
				synctest.Wait()
				if tc.callerDeadline {
					if namespaceCalls.Load() != 1 || plugin.namespaceUnavailable.Load() {
						t.Fatal("caller deadline invalidated a ready namespace")
					}
				} else if namespaceCalls.Load() != 2 || !plugin.namespaceUnavailable.Load() {
					t.Fatal("unresponsive store did not start namespace recovery")
				}
			})
		})
	}
}

func TestCacheAvailabilityNamespaceLossAfterHitAndRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var lost atomic.Bool
		var namespaceCalls atomic.Int32
		store := &cacheAvailabilityStore{observableStore: newObservableStore()}
		store.create = func(context.Context) error {
			namespaceCalls.Add(1)
			lost.Store(false)
			return nil
		}
		store.get = func(ctx context.Context, id string) (vectorstore.SearchResult, error) {
			if lost.Load() {
				return vectorstore.SearchResult{}, vectorstore.ErrNamespaceNotFound
			}
			return store.observableStore.GetChunk(ctx, "", id)
		}
		plugin := cacheAvailabilityInit(t, store, &Config{Dimension: 1})
		defer plugin.Cleanup()
		req := cacheAvailabilityRequest()
		writeCtx := cacheAvailabilityContext()
		defer writeCtx.Cancel()
		writeThroughCache(t, plugin, writeCtx, req)
		hitCtx := cacheAvailabilityContext()
		defer hitCtx.Cancel()
		if !cacheHits(t, plugin, hitCtx, req) {
			t.Fatal("initial saved response did not hit")
		}
		store.mu.Lock()
		clear(store.chunks)
		store.mu.Unlock()
		lost.Store(true)
		failedCtx := cacheAvailabilityContext()
		defer failedCtx.Cancel()
		got, hit, err := plugin.PreLLMHook(failedCtx, req)
		if err != nil || got != req || hit != nil || failedCtx.Err() != nil {
			t.Fatal("namespace loss changed the provider request")
		}
		if plugin.getCacheState(failedCtx.Value(schemas.BifrostContextKeyRequestID).(string)) != nil {
			t.Fatal("namespace loss was treated as a normal point miss")
		}
		synctest.Wait()
		if plugin.namespaceUnavailable.Load() || namespaceCalls.Load() != 2 {
			t.Fatal("lost namespace was not recreated without restart")
		}
		recoveredCtx := cacheAvailabilityContext()
		defer recoveredCtx.Cancel()
		writeThroughCache(t, plugin, recoveredCtx, req)
		replayCtx := cacheAvailabilityContext()
		defer replayCtx.Cancel()
		if !cacheHits(t, plugin, replayCtx, req) {
			t.Fatal("fresh namespace did not resume writes and hits")
		}
	})
}

func TestCacheAvailabilitySemanticOnlyNamespaceRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var lost atomic.Bool
		var namespaceCalls atomic.Int32
		store := &cacheAvailabilityStore{observableStore: newObservableStore()}
		store.create = func(context.Context) error {
			namespaceCalls.Add(1)
			lost.Store(false)
			return nil
		}
		store.nearest = func(context.Context) ([]vectorstore.SearchResult, error) {
			if lost.Load() {
				return nil, vectorstore.ErrNamespaceNotFound
			}
			return nil, nil
		}
		plugin := cacheAvailabilityInit(t, store, &Config{Provider: schemas.OpenAI, EmbeddingModel: "synthetic-embedding", Dimension: 4})
		defer plugin.Cleanup()
		plugin.SetEmbeddingRequestExecutor(func(*schemas.BifrostContext, *schemas.BifrostEmbeddingRequest) (*schemas.BifrostEmbeddingResponse, *schemas.BifrostError) {
			return &schemas.BifrostEmbeddingResponse{Data: []schemas.EmbeddingData{{Embedding: schemas.EmbeddingStruct{EmbeddingArray: []float64{1, 0, 0, 0}}}}}, nil
		})
		lost.Store(true)
		ctx := cacheAvailabilityContext()
		defer ctx.Cancel()
		ctx.SetValue(CacheTypeKey, CacheTypeSemantic)
		req := cacheAvailabilityRequest()
		got, hit, err := plugin.PreLLMHook(ctx, req)
		if err != nil || got != req || hit != nil || ctx.Err() != nil {
			t.Fatal("semantic store failure damaged provider continuation")
		}
		if plugin.getCacheState(ctx.Value(schemas.BifrostContextKeyRequestID).(string)) != nil {
			t.Fatal("failed semantic lookup retained an async write")
		}
		synctest.Wait()
		if namespaceCalls.Load() != 2 || plugin.namespaceUnavailable.Load() {
			t.Fatal("semantic-only lookup did not repair its namespace")
		}
		got, hit, err = plugin.PreLLMHook(ctx, req)
		if err != nil || got != req || hit != nil || plugin.getCacheState(ctx.Value(schemas.BifrostContextKeyRequestID).(string)) == nil {
			t.Fatal("semantic-only cache did not resume after recovery")
		}
	})
}

func TestCacheAvailabilityWriteNamespaceLossRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var lost atomic.Bool
		var namespaceCalls atomic.Int32
		store := &cacheAvailabilityStore{observableStore: newObservableStore()}
		store.create = func(context.Context) error {
			namespaceCalls.Add(1)
			lost.Store(false)
			return nil
		}
		store.add = func(context.Context) error {
			if lost.Load() {
				return vectorstore.ErrNamespaceNotFound
			}
			return nil
		}
		plugin := cacheAvailabilityInit(t, store, &Config{Dimension: 1})
		defer plugin.Cleanup()
		ctx := cacheAvailabilityContext()
		defer ctx.Cancel()
		req := cacheAvailabilityRequest()
		if got, hit, err := plugin.PreLLMHook(ctx, req); err != nil || got != req || hit != nil {
			t.Fatal("initial lookup failed")
		}
		lost.Store(true)
		response := identityTestChatResponse("provider response")
		if got, providerErr, err := plugin.PostLLMHook(ctx, response, nil); got != response || providerErr != nil || err != nil {
			t.Fatal("failed cache write changed the provider response")
		}
		plugin.WaitForPendingOperations()
		synctest.Wait()
		if namespaceCalls.Load() != 2 || plugin.namespaceUnavailable.Load() {
			t.Fatal("namespace loss between lookup and write did not recover")
		}
		recoveredCtx := cacheAvailabilityContext()
		defer recoveredCtx.Cancel()
		writeThroughCache(t, plugin, recoveredCtx, req)
		replayCtx := cacheAvailabilityContext()
		defer replayCtx.Cancel()
		if !cacheHits(t, plugin, replayCtx, req) {
			t.Fatal("cache writes did not resume after async failure")
		}
	})
}

func TestCacheAvailabilityAccessFailureIsNotQuietPointMiss(t *testing.T) {
	for _, code := range []codes.Code{codes.PermissionDenied, codes.Unauthenticated, codes.InvalidArgument} {
		t.Run(code.String(), func(t *testing.T) {
			store := &cacheAvailabilityStore{
				observableStore: newObservableStore(),
				create:          func(context.Context) error { return nil },
				get: func(context.Context, string) (vectorstore.SearchResult, error) {
					return vectorstore.SearchResult{}, status.Error(code, "subject not found")
				},
			}
			plugin := cacheAvailabilityInit(t, store, &Config{Dimension: 1})
			defer plugin.Cleanup()
			ctx := scopedTestContext(t, "")
			defer ctx.Cancel()
			ctx.SetValue(CacheNoStoreKey, true)
			req := cacheAvailabilityRequest()
			got, hit, err := plugin.PreLLMHook(ctx, req)
			if err != nil || got != req || hit != nil || ctx.Err() != nil {
				t.Fatal("cache access failure damaged the provider request")
			}
			if !pluginLogContains(ctx.GetPluginLogs(), schemas.LogLevelWarn, "subject not found") {
				t.Fatal("known service failure disappeared as a quiet point miss")
			}
			if plugin.namespaceUnavailable.Load() {
				t.Fatal("known access/schema error started transport recovery")
			}
			response := identityTestChatResponse("provider response")
			if got, providerErr, err := plugin.PostLLMHook(ctx, response, nil); got != response || providerErr != nil || err != nil {
				t.Fatal("cache access failure changed the provider response")
			}
			plugin.WaitForPendingOperations()
			store.mu.Lock()
			writes := len(store.addIDs)
			store.mu.Unlock()
			if writes != 0 {
				t.Fatal("no-store request wrote a cache entry")
			}
		})
	}
}

func TestCacheAvailabilityLookupFailureKeepsUsableWrite(t *testing.T) {
	for _, tc := range []struct {
		name      string
		cacheType CacheType
		direct    bool
		corrupt   bool
	}{
		{"direct unsupported", CacheTypeDirect, true, false},
		{"direct corrupt response", CacheTypeDirect, true, true},
		{"semantic unsupported", "", false, false},
		{"semantic-only unsupported", CacheTypeSemantic, false, false},
		{"semantic-only corrupt response", CacheTypeSemantic, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var failLookup atomic.Bool
			failLookup.Store(true)
			var namespaceCalls atomic.Int32
			store := &cacheAvailabilityStore{observableStore: newObservableStore()}
			store.create = func(context.Context) error {
				namespaceCalls.Add(1)
				return nil
			}
			failedResult := func(id string) (vectorstore.SearchResult, error) {
				if tc.corrupt {
					return vectorstore.SearchResult{ID: id, Properties: map[string]interface{}{"response": "{"}}, nil
				}
				return vectorstore.SearchResult{}, vectorstore.ErrNotSupported
			}
			store.get = func(ctx context.Context, id string) (vectorstore.SearchResult, error) {
				if tc.direct && failLookup.Load() {
					return failedResult(id)
				}
				return store.observableStore.GetChunk(ctx, "", id)
			}
			store.nearest = func(context.Context) ([]vectorstore.SearchResult, error) {
				if !tc.direct && failLookup.Load() {
					result, err := failedResult("synthetic-corrupt-entry")
					return []vectorstore.SearchResult{result}, err
				}
				return nil, nil
			}
			plugin := cacheAvailabilityInit(t, store, &Config{Provider: schemas.OpenAI, EmbeddingModel: "synthetic-embedding", Dimension: 4})
			defer plugin.Cleanup()
			plugin.SetEmbeddingRequestExecutor(func(*schemas.BifrostContext, *schemas.BifrostEmbeddingRequest) (*schemas.BifrostEmbeddingResponse, *schemas.BifrostError) {
				return &schemas.BifrostEmbeddingResponse{Data: []schemas.EmbeddingData{{Embedding: schemas.EmbeddingStruct{EmbeddingArray: []float64{1, 0, 0, 0}}}}}, nil
			})
			ctx := scopedTestContext(t, "")
			defer ctx.Cancel()
			if tc.cacheType != "" {
				ctx.SetValue(CacheTypeKey, tc.cacheType)
			}
			req := cacheAvailabilityRequest()
			got, hit, err := plugin.PreLLMHook(ctx, req)
			if err != nil || got != req || hit != nil || ctx.Err() != nil {
				t.Fatal("failed lookup changed the provider request")
			}
			if !pluginLogContains(ctx.GetPluginLogs(), schemas.LogLevelWarn, "failed") && !pluginLogContains(ctx.GetPluginLogs(), schemas.LogLevelWarn, "semantic search skipped") {
				t.Fatal("lookup failure was not visible as a warning")
			}
			response := identityTestChatResponse("synthetic-provider-answer")
			if got, providerErr, err := plugin.PostLLMHook(ctx, response, nil); got != response || providerErr != nil || err != nil {
				t.Fatal("cache write changed the provider response")
			}
			plugin.WaitForPendingOperations()
			store.mu.Lock()
			writes := len(store.addIDs)
			var written vectorstore.SearchResult
			var embedding []float32
			if writes == 1 {
				written = store.chunks[store.addIDs[0]]
				embedding = store.addEmbeddings[0]
			}
			store.mu.Unlock()
			if writes != 1 {
				t.Fatalf("usable response was not cached after lookup failure: %d writes", writes)
			}
			if !tc.direct && !slices.Equal(embedding, []float32{1, 0, 0, 0}) {
				t.Fatalf("successful embedding was lost before write: %v", embedding)
			}
			payload, ok := written.Properties["response"].(string)
			if !ok || !strings.Contains(payload, "synthetic-provider-answer") {
				t.Fatal("write did not preserve the provider response")
			}
			if namespaceCalls.Load() != 1 || plugin.namespaceUnavailable.Load() {
				t.Fatal("individual lookup failure started namespace recovery")
			}
			// Direct entries, including an unreadable former value, are replaced
			// under the same deterministic ID and can serve the next request.
			if tc.cacheType != CacheTypeSemantic {
				failLookup.Store(false)
				replayCtx := scopedTestContext(t, "")
				defer replayCtx.Cancel()
				if tc.cacheType != "" {
					replayCtx.SetValue(CacheTypeKey, tc.cacheType)
				}
				if !cacheHits(t, plugin, replayCtx, req) {
					t.Fatal("provider response did not repair the direct cache entry")
				}
			}
		})
	}
}

type cacheRetentionStore struct {
	*observableStore
	deleteFilter func(context.Context, string, []vectorstore.Query) error
	nearest      func(context.Context) ([]vectorstore.SearchResult, error)
}

func (s *cacheRetentionStore) DeleteByFilter(ctx context.Context, namespace string, queries []vectorstore.Query) error {
	return s.deleteFilter(ctx, namespace, queries)
}

func (s *cacheRetentionStore) GetNearest(ctx context.Context, namespace string, vector []float32, queries []vectorstore.Query, fields []string, threshold float64, limit int64) ([]vectorstore.SearchResult, error) {
	if s.nearest != nil {
		return s.nearest(ctx)
	}
	return s.observableStore.GetNearest(ctx, namespace, vector, queries, fields, threshold, limit)
}

type cacheRetentionLogger struct {
	schemas.Logger
	mu       sync.Mutex
	warnings []string
}

func (l *cacheRetentionLogger) Warn(message string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warnings = append(l.warnings, fmt.Sprintf(message, args...))
}

func newCacheRetentionPlugin(t *testing.T, ctx context.Context, store vectorstore.VectorStore, logger schemas.Logger) *Plugin {
	t.Helper()
	llmPlugin, err := Init(ctx, &Config{Dimension: 1, VectorStoreNamespace: "retention-test"}, logger, store)
	if err != nil {
		t.Fatal(err)
	}
	return llmPlugin.(*Plugin)
}

func TestCacheRetentionStartupAndPeriodicSweep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type call struct {
			namespace string
			queries   []vectorstore.Query
			deadline  time.Time
		}
		calls := make(chan call, 4)
		store := &cacheRetentionStore{observableStore: newObservableStore()}
		store.deleteFilter = func(ctx context.Context, namespace string, queries []vectorstore.Query) error {
			deadline, _ := ctx.Deadline()
			calls <- call{namespace, append([]vectorstore.Query(nil), queries...), deadline}
			return nil
		}
		start := time.Now()
		initCtx, cancelInit := context.WithCancel(context.Background())
		defer cancelInit()
		config := &Config{
			Provider: "cloudflare", EmbeddingModel: "@cf/baai/bge-m3", Dimension: 1024,
			TTL: 5 * time.Minute, VectorStoreNamespace: "retention-test",
			CacheByModel: new(false), CacheByProvider: new(false),
		}
		llmPlugin, err := Init(initCtx, config, bifrost.NewDefaultLogger(schemas.LogLevelError), store)
		if err != nil {
			t.Fatal(err)
		}
		plugin := llmPlugin.(*Plugin)
		defer plugin.Cleanup()
		cancelInit() // The completed Init request must not own the worker lifetime.
		synctest.Wait()

		check := func(cutoff time.Time) {
			t.Helper()
			select {
			case got := <-calls:
				want := []vectorstore.Query{
					{Field: "from_bifrost_semantic_cache_plugin", Operator: vectorstore.QueryOperatorEqual, Value: true},
					{Field: "expires_at", Operator: vectorstore.QueryOperatorGreaterThan, Value: int64(0)},
					{Field: "expires_at", Operator: vectorstore.QueryOperatorLessThan, Value: cutoff.Unix()},
				}
				if got.namespace != "retention-test" || !reflect.DeepEqual(got.queries, want) {
					t.Fatalf("unsafe retention scope: namespace=%q queries=%+v", got.namespace, got.queries)
				}
				if !got.deadline.Equal(cutoff.Add(30 * time.Second)) {
					t.Fatalf("deadline=%v, want %v", got.deadline, cutoff.Add(30*time.Second))
				}
			default:
				t.Fatal("expected a retention pass")
			}
		}
		check(start)
		if len(calls) != 0 {
			t.Fatal("more than one startup pass")
		}
		time.Sleep(59 * time.Second)
		synctest.Wait()
		if len(calls) != 0 {
			t.Fatal("retention ran before its interval")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		check(start.Add(time.Minute))
		if !VectorStoreProperties["expires_at"].Filterable || VectorStoreProperties["response"].Filterable || VectorStoreProperties["stream_chunks"].Filterable {
			t.Fatal("retention must index expiration without indexing response bodies")
		}
		if config.Provider != "cloudflare" || config.EmbeddingModel != "@cf/baai/bge-m3" || config.Dimension != 1024 || config.TTL != 5*time.Minute || *config.CacheByModel || *config.CacheByProvider {
			t.Fatal("retention changed existing cache policy or embedding configuration")
		}
		if err := plugin.Cleanup(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if len(calls) != 0 {
			t.Fatal("retention continued after Cleanup returned")
		}
	})
}

func TestCacheRetentionDeadlineAndShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{}, 4)
		finished := make(chan error, 4)
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		store := &cacheRetentionStore{observableStore: newObservableStore()}
		callNumber := 0 // Accessed only by the serial worker.
		store.deleteFilter = func(ctx context.Context, _ string, _ []vectorstore.Query) error {
			callNumber++
			started <- struct{}{}
			<-ctx.Done()
			finished <- ctx.Err()
			if callNumber == 2 {
				<-release // Cleanup must join the canceled operation before returning.
			}
			return ctx.Err()
		}
		logger := &cacheRetentionLogger{Logger: bifrost.NewDefaultLogger(schemas.LogLevelError)}
		plugin := newCacheRetentionPlugin(t, context.Background(), store, logger)
		defer func() { unblock(); _ = plugin.Cleanup() }()
		synctest.Wait()
		if len(started) != 1 {
			t.Fatal("startup cleanup did not start")
		}
		<-started
		time.Sleep(30 * time.Second)
		synctest.Wait()
		select {
		case err := <-finished:
			if err != context.DeadlineExceeded {
				t.Fatalf("pass ended with %v, want deadline exceeded", err)
			}
		default:
			t.Fatal("blocked store was not canceled after 30 seconds")
		}
		time.Sleep(30 * time.Second)
		synctest.Wait()
		if len(started) != 1 {
			t.Fatal("a failed pass prevented the next periodic pass")
		}
		<-started
		cleanupDone := make(chan error, 1)
		go func() { cleanupDone <- plugin.Cleanup() }()
		synctest.Wait()
		select {
		case err := <-finished:
			if err != context.Canceled {
				t.Fatalf("shutdown ended the store call with %v, want canceled", err)
			}
		default:
			t.Fatal("Cleanup did not cancel the current store call")
		}
		select {
		case <-cleanupDone:
			t.Fatal("Cleanup returned before the store call completed")
		default:
		}
		unblock()
		synctest.Wait()
		select {
		case err := <-cleanupDone:
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("Cleanup did not join the completed worker")
		}
		if err := plugin.Cleanup(); err != nil {
			t.Fatal(err)
		}
		logger.mu.Lock()
		defer logger.mu.Unlock()
		if len(logger.warnings) != 1 || !strings.Contains(logger.warnings[0], "deadline exceeded") {
			t.Fatalf("want one timeout warning and no shutdown warning, got %v", logger.warnings)
		}
	})
}

func TestCacheRetentionNoOverlap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{}, 4)
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		store := &cacheRetentionStore{observableStore: newObservableStore()}
		store.deleteFilter = func(ctx context.Context, _ string, _ []vectorstore.Query) error {
			started <- struct{}{}
			<-release // Deliberately ignore the deadline to expose spawned-per-tick work.
			return ctx.Err()
		}
		plugin := newCacheRetentionPlugin(t, context.Background(), store, bifrost.NewDefaultLogger(schemas.LogLevelError))
		defer func() { unblock(); _ = plugin.Cleanup() }()
		synctest.Wait()
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if len(started) != 1 {
			t.Fatalf("overlapping cleanup calls: got %d, want 1", len(started))
		}
		done := make(chan struct{})
		go func() { _ = plugin.Cleanup(); close(done) }()
		synctest.Wait()
		unblock()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("worker did not finish after its store call returned")
		}
		if len(started) != 1 {
			t.Fatal("a queued tick started another operation after cancellation")
		}
	})
}

func TestCacheRetentionLegacyStore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newObservableStore()
		logger := &cacheRetentionLogger{Logger: bifrost.NewDefaultLogger(schemas.LogLevelError)}
		plugin := newCacheRetentionPlugin(t, context.Background(), store, logger)
		defer plugin.Cleanup()
		time.Sleep(3 * time.Minute)
		synctest.Wait()
		if err := plugin.Cleanup(); err != nil {
			t.Fatal(err)
		}
		logger.mu.Lock()
		defer logger.mu.Unlock()
		if len(logger.warnings) != 1 || !strings.Contains(logger.warnings[0], "does not support conditional deletion") {
			t.Fatalf("unsupported cleanup must be visible once at Init, got %v", logger.warnings)
		}
		store.mu.Lock()
		defer store.mu.Unlock()
		if plugin.retentionCancel != nil || len(store.deleteIDs) != 0 || len(store.deleteAllQueries) != 0 || store.namespaceDeletes != 0 {
			t.Fatal("a store without conditional deletion received a destructive fallback")
		}
	})
}

func TestCacheRetentionLookupDoesNotDeleteRefreshedID(t *testing.T) {
	store := &cacheRetentionStore{observableStore: newObservableStore()}
	plugin := newTestPlugin(t, store)
	defer plugin.Cleanup()
	id := "same-deterministic-cache-id"
	fresh := vectorstore.SearchResult{ID: id, Properties: map[string]interface{}{"expires_at": time.Now().Add(time.Hour).Unix()}}
	store.chunks[id] = fresh
	staleSnapshot := vectorstore.SearchResult{ID: id, Properties: map[string]interface{}{"expires_at": time.Now().Add(-time.Hour).Unix()}}
	ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	result, err := plugin.buildResponseFromResult(ctx, nil, nil, staleSnapshot, CacheTypeDirect, nil, nil)
	if err != nil || result != nil {
		t.Fatalf("expired observation must be a miss, got result=%v err=%v", result, err)
	}
	plugin.WaitForPendingOperations()
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.deleteIDs) != 0 || !reflect.DeepEqual(store.chunks[id], fresh) {
		t.Fatal("stale observation deleted the refreshed cache ID")
	}
}

func TestCacheRetentionExpiredHitWakesSerialWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var sweeps atomic.Int32
		var expired atomic.Bool
		started, release := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		store := &cacheRetentionStore{observableStore: newObservableStore()}
		store.deleteFilter = func(ctx context.Context, _ string, _ []vectorstore.Query) error {
			n := sweeps.Add(1)
			if n == 2 {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			expired.Store(false)
			return nil
		}
		response, err := marshalCachedResponse(identityTestChatResponse("fresh semantic answer"))
		if err != nil {
			t.Fatal(err)
		}
		store.nearest = func(context.Context) ([]vectorstore.SearchResult, error) {
			if expired.Load() {
				return []vectorstore.SearchResult{{ID: "expired-first", Properties: map[string]interface{}{"expires_at": start.Add(-time.Hour).Unix()}}}, nil
			}
			return []vectorstore.SearchResult{{ID: "fresh-second", Properties: map[string]interface{}{"response": string(response), "expires_at": start.Add(time.Hour).Unix()}}}, nil
		}
		plugin := cacheAvailabilityInit(t, store, &Config{Provider: schemas.OpenAI, EmbeddingModel: "test-embedding", Dimension: 2})
		defer func() { unblock(); _ = plugin.Cleanup() }()
		plugin.SetEmbeddingRequestExecutor(func(*schemas.BifrostContext, *schemas.BifrostEmbeddingRequest) (*schemas.BifrostEmbeddingResponse, *schemas.BifrostError) {
			return &schemas.BifrostEmbeddingResponse{Data: []schemas.EmbeddingData{{Embedding: schemas.EmbeddingStruct{EmbeddingArray: []float64{1, 0}}}}}, nil
		})
		synctest.Wait()
		if sweeps.Load() != 1 {
			t.Fatal("startup sweep did not finish")
		}
		expired.Store(true)
		lookup := func() bool {
			ctx := cacheAvailabilityContext()
			defer ctx.Cancel()
			ctx.SetValue(CacheTypeKey, CacheTypeSemantic)
			return cacheHits(t, plugin, ctx, cacheAvailabilityRequest())
		}
		if lookup() {
			t.Fatal("expired nearest result was replayed")
		}
		synctest.Wait()
		select {
		case <-started:
		default:
			t.Fatal("expired hit waited for the periodic sweep")
		}
		for i := 0; i < 8; i++ {
			if lookup() {
				t.Fatal("blocked maintenance replayed the expired result")
			}
		}
		synctest.Wait()
		if sweeps.Load() != 2 {
			t.Fatal("expired hits started overlapping sweeps")
		}
		unblock()
		synctest.Wait()
		if sweeps.Load() != 2 {
			t.Fatalf("repeated expired hits started extra sweeps: %d", sweeps.Load())
		}
		if !lookup() || !time.Now().Equal(start) {
			t.Fatal("fresh second result was not available before the periodic tick")
		}
		store.mu.Lock()
		defer store.mu.Unlock()
		if len(store.deleteIDs) != 0 {
			t.Fatal("expired observation used an unsafe ID-only deletion")
		}
	})
}

func TestCacheRetentionWakeBackoff(t *testing.T) {
	for _, failDelete := range []bool{false, true} {
		t.Run(fmt.Sprintf("delete_error=%v", failDelete), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var sweeps atomic.Int32
				store := &cacheRetentionStore{observableStore: newObservableStore()}
				store.deleteFilter = func(context.Context, string, []vectorstore.Query) error {
					sweeps.Add(1)
					if failDelete {
						return fmt.Errorf("retention unavailable")
					}
					return nil
				}
				plugin := newCacheRetentionPlugin(t, context.Background(), store, bifrost.NewDefaultLogger(schemas.LogLevelError))
				defer plugin.Cleanup()
				synctest.Wait()
				expiry := int64(0) // A legacy sentinel rejected by lookup but excluded from the sweep.
				if failDelete {
					expiry = time.Now().Add(-time.Minute).Unix()
				}
				ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
				defer ctx.Cancel()
				lookup := func() {
					t.Helper()
					result, err := plugin.buildResponseFromResult(ctx, nil, nil, vectorstore.SearchResult{ID: "expired", Properties: map[string]interface{}{"expires_at": expiry}}, CacheTypeDirect, nil, nil)
					if result != nil || err != nil {
						t.Fatalf("expired entry did not remain a miss: %v, %v", result, err)
					}
					synctest.Wait() // Let even an immediately failing sweep finish between misses.
				}
				lookup()
				if sweeps.Load() != 2 {
					t.Fatal("first expired lookup did not accelerate retention")
				}
				for i := 0; i < 8; i++ {
					lookup()
				}
				if sweeps.Load() != 2 {
					t.Fatalf("persistent misses started an unbounded cleanup loop: %d sweeps", sweeps.Load())
				}
				time.Sleep(time.Minute)
				synctest.Wait()
				if sweeps.Load() != 3 {
					t.Fatal("accelerated cleanup suppressed the periodic sweep")
				}
				lookup()
				if sweeps.Load() != 4 {
					t.Fatal("periodic sweep did not re-enable one accelerated pass")
				}
			})
		})
	}
}

func TestCacheRetentionWriterShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		plugin := newTestPlugin(t, newObservableStore())
		started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		releaseWriter := sync.OnceFunc(func() { close(release) })
		defer func() { releaseWriter(); _ = plugin.Cleanup() }()
		var writes atomic.Int32
		plugin.cacheWriter("admitted", func(context.Context) { close(started); <-release; writes.Add(1) })
		<-started
		go func() { _ = plugin.Cleanup(); close(done) }()
		<-plugin.stopCh
		plugin.cacheWriter("during-shutdown", func(context.Context) { writes.Add(100) })
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("Cleanup returned before its admitted write completed")
		default:
		}
		releaseWriter()
		<-done
		plugin.cacheWriter("after-shutdown", func(context.Context) { writes.Add(100) })
		plugin.WaitForPendingOperations()
		if got := writes.Load(); got != 1 {
			t.Fatalf("retired instance accepted extra writes: got %d want 1", got)
		}
	})
}

func TestCacheRetentionLateDelete(t *testing.T) {
	store := newObservableStore()
	plugin := newTestPlugin(t, store)
	if err := plugin.Cleanup(); err != nil {
		t.Fatal(err)
	}
	ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(time.Second))
	defer ctx.Cancel()
	_, err := plugin.buildResponseFromResult(ctx, nil, nil, vectorstore.SearchResult{ID: "expired", Properties: map[string]interface{}{"expires_at": time.Now().Add(-time.Hour).Unix()}}, CacheTypeDirect, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	plugin.WaitForPendingOperations()
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.deleteIDs) != 0 {
		t.Fatal("retired instance accepted a stale-entry delete")
	}
}

func TestCacheRetentionLatePostHook(t *testing.T) {
	for _, test := range []struct{ stream, failed bool }{{false, false}, {true, false}, {true, true}} {
		t.Run(fmt.Sprintf("stream=%v/error=%v", test.stream, test.failed), func(t *testing.T) {
			store := newObservableStore()
			plugin := newTestPlugin(t, store)
			ctx := CreateContextWithCacheKeyAndType(t, "retired-cache", CacheTypeDirect)
			request := identityTestChatRequest("late response", nil)
			if test.stream {
				request.RequestType = schemas.ChatCompletionStreamRequest
			}
			mustPreLLMHookMiss(t, plugin, ctx, request)
			if test.stream {
				if _, _, err := plugin.PostLLMHook(ctx, newChatStreamChunk(0, "prefix", nil), nil); err != nil {
					t.Fatal(err)
				}
			}
			if err := plugin.Cleanup(); err != nil {
				t.Fatal(err)
			}
			response := identityTestChatResponse("complete response")
			if test.stream {
				ctx.SetValue(schemas.BifrostContextKeyStreamEndIndicator, true)
				response = newChatStreamChunk(1, "suffix", nil)
			}
			var upstreamErr *schemas.BifrostError
			if test.failed {
				upstreamErr = &schemas.BifrostError{Error: &schemas.ErrorField{Message: "stream ended with provider error"}}
			}
			got, bifrostErr, err := plugin.PostLLMHook(ctx, response, upstreamErr)
			if got != response || bifrostErr != upstreamErr || err != nil {
				t.Fatal("retired cache changed the delivered response")
			}
			plugin.WaitForPendingOperations()
			requestID, _ := ctx.Value(schemas.BifrostContextKeyRequestID).(string)
			if _, ok := plugin.streamAccumulators.Load(requestID); ok {
				t.Fatal("retired cache retained a late stream accumulator")
			}
			if plugin.getCacheState(requestID) != nil {
				t.Fatal("retired cache retained late request state")
			}
			store.mu.Lock()
			defer store.mu.Unlock()
			if len(store.addIDs) != 0 {
				t.Fatal("retired cache accepted a late response write")
			}
		})
	}
}

func BenchmarkCacheRetentionWriter(b *testing.B) {
	plugin := &Plugin{stopCh: make(chan struct{}), config: &Config{}, logger: bifrost.NewDefaultLogger(schemas.LogLevelError)}
	write := func(context.Context) {}
	b.ReportAllocs()
	for b.Loop() {
		plugin.cacheWriter("benchmark", write)
		plugin.WaitForPendingOperations()
	}
}
