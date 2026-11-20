package semanticcache

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
)

type nativeReplayAccount struct{ endpoint string }

func (a nativeReplayAccount) GetConfiguredProviders() ([]schemas.ModelProvider, error) {
	return []schemas.ModelProvider{schemas.OpenAI}, nil
}
func (a nativeReplayAccount) GetConfigForProvider(schemas.ModelProvider) (*schemas.ProviderConfig, error) {
	return &schemas.ProviderConfig{
		NetworkConfig:            schemas.NetworkConfig{BaseURL: a.endpoint, DefaultRequestTimeoutInSeconds: 5},
		ConcurrencyAndBufferSize: schemas.ConcurrencyAndBufferSize{Concurrency: 1, BufferSize: 10},
	}, nil
}
func (a nativeReplayAccount) GetKeysForProvider(context.Context, schemas.ModelProvider) ([]schemas.Key, error) {
	return []schemas.Key{{ID: "fixture", Value: *schemas.NewSecretVar("fixture"), Weight: 1, Models: []string{"*"}}}, nil
}

func TestCacheReplayPreservesNativeContent(t *testing.T) {
	for _, content := range []string{`{"answer":`, "opaque model content", `{"answer":true}`} {
		t.Run(content, func(t *testing.T) {
			var sends atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				sends.Add(1)
				var request struct {
					Stop           []string              `json:"stop"`
					ResponseFormat struct{ Type string } `json:"response_format"`
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.Stop) != 1 || request.Stop[0] != "END" || request.ResponseFormat.Type != "json_object" {
					t.Errorf("native parameters changed: %+v, %v", request, err)
				}
				response := identityTestChatResponse(content).ChatResponse
				response.Model = "gpt-4o-mini"
				response.Choices[0].FinishReason = schemas.Ptr("stop")
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(response)
			}))
			defer upstream.Close()
			plugin := identityScopeTestPlugin(t, newObservableStore(), "")
			client, err := bifrost.Init(context.Background(), schemas.BifrostConfig{
				Account: nativeReplayAccount{upstream.URL}, Logger: bifrost.NewDefaultLogger(schemas.LogLevelError),
				LLMPlugins: []schemas.LLMPlugin{plugin},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Shutdown()
			request := identityTestChatRequest("native content", nil).ChatRequest
			var format any = map[string]any{"type": "json_object"}
			request.Params = &schemas.ChatParameters{ResponseFormat: &format, Stop: []string{"END"}}
			request.Fallbacks = []schemas.Fallback{{Provider: schemas.OpenAI, Model: "fallback"}}
			for _, cached := range []bool{false, true} {
				ctx := identityContext(t, "native-content", identityRef{vk: "tenant"})
				ctx.SetValue(CacheTypeKey, CacheTypeDirect)
				response, err := client.ChatCompletionRequest(ctx, request)
				ctx.Cancel()
				plugin.WaitForPendingOperations()
				if err != nil || response == nil {
					t.Fatalf("native response became an error: cached=%v sends=%d error=%v", cached, sends.Load(), err)
				}
				if got := *response.Choices[0].Message.Content.ContentStr; got != content || *response.Choices[0].FinishReason != "stop" {
					t.Fatalf("native completion changed: %q", got)
				}
				if hit := response.ExtraFields.CacheDebug; hit == nil || hit.CacheHit != cached || sends.Load() != 1 {
					t.Fatalf("expected cached=%v with one upstream call, got cache=%+v sends=%d", cached, hit, sends.Load())
				}
			}
		})
	}
}

// Lowercasing changes the Python string literal and therefore the correct answer.
// Exercise cache replay with a deterministic provider, including identical replays,
// so a difference in hashes alone cannot make this regression pass.
func TestDirectCacheKeepsCaseSensitiveCodeDistinct(t *testing.T) {
	const upper = "What does Python print for print('US'.isupper())? Reply with the output only."
	const lower = "What does Python print for print('us'.isupper())? Reply with the output only."
	answers := map[string]string{upper: "True", lower: "False"}
	plugin := identityScopeTestPlugin(t, newObservableStore(), "")
	defer plugin.Cleanup()
	providerCalls := 0
	for _, tc := range []struct {
		prompt string
		cached bool
		calls  int
	}{
		{upper, false, 1},
		{upper, true, 1},
		{lower, false, 2},
		{lower, true, 2},
	} {
		ctx := identityContext(t, "case-sensitive-code", identityRef{vk: "tenant"})
		ctx.SetValue(CacheTypeKey, CacheTypeDirect)
		defer ctx.Cancel()
		_, cached, err := plugin.PreLLMHook(ctx, identityTestChatRequest(tc.prompt, nil))
		if err != nil {
			t.Fatal(err)
		}
		var response *schemas.BifrostResponse
		if cached != nil {
			response = cached.Response
		} else {
			providerCalls++
			response = identityTestChatResponse(answers[tc.prompt])
			if _, _, err := plugin.PostLLMHook(ctx, response, nil); err != nil {
				t.Fatal(err)
			}
			plugin.WaitForPendingOperations()
		}
		if response == nil || response.ChatResponse == nil || len(response.ChatResponse.Choices) != 1 {
			t.Fatalf("missing response for %q: %+v", tc.prompt, response)
		}
		got := *response.ChatResponse.Choices[0].Message.Content.ContentStr
		if got != answers[tc.prompt] {
			t.Fatalf("case-sensitive code received another prompt's answer: %q => %q, want %q (cached=%v, provider calls=%d)", tc.prompt, got, answers[tc.prompt], cached != nil, providerCalls)
		}
		if (cached != nil) != tc.cached || providerCalls != tc.calls {
			t.Fatalf("%q: cached=%v, provider calls=%d; want cached=%v, calls=%d", tc.prompt, cached != nil, providerCalls, tc.cached, tc.calls)
		}
	}
}

func TestCacheIdentityExactInput(t *testing.T) {
	p := identityScopeTestPlugin(t, newObservableStore(), "")
	for _, operation := range []string{"chat", "chat content blocks", "speech", "embedding"} {
		t.Run(operation, func(t *testing.T) {
			seen := map[string]string{}
			for _, text := range []string{"US", "us", " US", "US "} {
				req := identityTestChatRequest(text, nil)
				switch operation {
				case "chat content blocks":
					req.ChatRequest.Input[0].Content = &schemas.ChatMessageContent{
						ContentBlocks: []schemas.ChatContentBlock{{Type: schemas.ChatContentBlockTypeText, Text: schemas.Ptr(text)}},
					}
				case "speech":
					req = &schemas.BifrostRequest{RequestType: schemas.SpeechRequest, SpeechRequest: CreateSpeechRequest(text, "alloy")}
				case "embedding":
					req = &schemas.BifrostRequest{RequestType: schemas.EmbeddingRequest, EmbeddingRequest: &schemas.BifrostEmbeddingRequest{
						Input: []schemas.EmbeddingInputItem{{Content: schemas.EmbeddingContent{{Type: schemas.EmbeddingContentPartTypeText, Text: schemas.Ptr(text)}}}},
					}}
				}
				metadata, err := p.buildRequestMetadataForCaching(nil, req)
				if err != nil {
					t.Fatal(err)
				}
				hash, err := p.generateRequestHash(req, metadata)
				if err != nil {
					t.Fatal(err)
				}
				if other, exists := seen[hash]; exists {
					t.Errorf("%q and %q share direct identity", text, other)
				}
				seen[hash] = text
				again, err := p.generateRequestHash(req, metadata)
				if err != nil || again != hash {
					t.Fatal("same input changed identity")
				}
			}
		})
	}
	writeThroughCache(t, p, identityContext(t, "test", identityRef{vk: "a"}), identityTestChatRequest("US", nil))
	if !cacheHits(t, p, identityContext(t, "test", identityRef{vk: "a"}), identityTestChatRequest("US", nil)) {
		t.Fatal("identical request did not replay")
	}
	if cacheHits(t, p, identityContext(t, "test", identityRef{vk: "a"}), identityTestChatRequest("us", nil)) {
		t.Fatal("case change replayed another answer")
	}
}

func TestCacheIdentityFullParameters(t *testing.T) {
	p := identityScopeTestPlugin(t, newObservableStore(), "")
	hashes := map[string]string{}
	for _, raw := range []string{
		`{}`, `{"reasoning":{"max_tokens":100}}`, `{"reasoning":{"max_tokens":101}}`,
		`{"top_k":2}`, `{"top_k":3}`, `{"context_management":{"value":9007199254740992}}`,
		`{"context_management":{"value":9007199254740993}}`,
	} {
		req := identityTestChatRequest("same input", nil)
		req.ChatRequest.Params = &schemas.ChatParameters{}
		if err := json.Unmarshal([]byte(raw), req.ChatRequest.Params); err != nil {
			t.Fatal(err)
		}
		metadata, err := p.buildRequestMetadataForCaching(nil, req)
		if err != nil {
			t.Fatal(err)
		}
		hash, err := hashMap(metadata)
		if err != nil {
			t.Fatal(err)
		}
		if prior, exists := hashes[hash]; exists {
			t.Errorf("parameters %s and %s share identity", prior, raw)
		}
		hashes[hash] = raw
	}
	for _, value := range []uint64{9007199254740992, 9007199254740993} {
		req := identityTestChatRequest("same input", map[string]interface{}{"future_native_parameter": value})
		metadata, err := p.buildRequestMetadataForCaching(nil, req)
		if err != nil {
			t.Fatal(err)
		}
		hash, err := hashMap(metadata)
		if err != nil {
			t.Fatal(err)
		}
		if _, exists := hashes[hash]; exists {
			t.Error("extra parameter precision lost")
		}
		hashes[hash] = "extra"
	}
	var previous string
	for _, raw := range []string{`{"text":{"format":{"type":"json_schema","name":"first","schema":{"type":"integer"},"strict":true}}}`, `{"text":{"format":{"type":"json_schema","name":"second","schema":{"type":"string"},"strict":true}}}`} {
		params := &schemas.ResponsesParameters{}
		if err := json.Unmarshal([]byte(raw), params); err != nil {
			t.Fatal(err)
		}
		metadata, err := p.buildRequestMetadataForCaching(nil, &schemas.BifrostRequest{RequestType: schemas.ResponsesRequest, ResponsesRequest: &schemas.BifrostResponsesRequest{Params: params}})
		if err != nil {
			t.Fatal(err)
		}
		hash, err := hashMap(metadata)
		if err != nil {
			t.Fatal(err)
		}
		if previous == hash {
			t.Error("different output schemas share identity")
		}
		previous = hash
	}
}

func TestCacheIdentityHTTPContext(t *testing.T) {
	p := identityScopeTestPlugin(t, newObservableStore(), "")
	context := func(version, beta, query, trace string) *schemas.BifrostContext {
		ctx := identityContext(t, "test", identityRef{vk: "a"})
		req := &schemas.HTTPRequest{Method: "POST", Path: "/anthropic/v1/messages", Headers: map[string]string{"anthropic-version": version, "anthropic-beta": beta, "x-request-id": trace, "authorization": trace}, Query: map[string]string{"api-version": query}}
		_, err := p.HTTPTransportPreHook(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		// The inference context receives these native snapshots after HTTP hooks.
		ctx.SetValue(schemas.BifrostContextKeyRequestHeaders, req.Headers)
		ctx.SetValue(schemas.BifrostContextKeyRequestQuery, req.Query)
		return ctx
	}
	req := identityTestChatRequest("same input", nil)
	writeThroughCache(t, p, context("2023", "beta-a", "v1", "trace-a"), req)
	if !cacheHits(t, p, context("2023", "beta-a", "v1", "trace-b"), req) {
		t.Error("trace/auth changes fragmented cache")
	}
	for _, ctx := range []*schemas.BifrostContext{context("2024", "beta-a", "v1", "trace-a"), context("2023", "beta-b", "v1", "trace-a"), context("2023", "beta-a", "v2", "trace-a")} {
		if cacheHits(t, p, ctx, req) {
			t.Error("native protocol change replayed incompatible answer")
		}
	}
}

func TestCacheIdentityRawReplay(t *testing.T) {
	p := identityScopeTestPlugin(t, newObservableStore(), "")
	context := func() *schemas.BifrostContext {
		ctx := identityContext(t, "test", identityRef{vk: "a"})
		ctx.SetValue(schemas.BifrostContextKeyUseRawRequestBody, true)
		return ctx
	}
	request := func(body string) *schemas.BifrostRequest {
		req := identityTestChatRequest("same typed input", nil)
		req.ChatRequest.RawRequestBody = []byte(body)
		return req
	}
	const first = `{"messages":[{"role":"user","content":"hi"}],"future_native_knob":1}`
	writeThroughCache(t, p, context(), request(first))
	if !cacheHits(t, p, context(), request(first)) {
		t.Error("same raw request did not replay")
	}
	if cacheHits(t, p, context(), request(`{"messages":[{"role":"user","content":"hi"}],"future_native_knob":2}`)) {
		t.Error("raw native parameter was lost from identity")
	}
}

func TestCacheIdentityRawResponsePrecision(t *testing.T) {
	p := identityScopeTestPlugin(t, newObservableStore(), "")
	ctx := identityContext(t, "test", identityRef{vk: "a"})
	req := identityTestChatRequest("native response", nil)
	if _, hit, err := p.PreLLMHook(ctx, req); err != nil || hit != nil {
		t.Fatal("expected miss", err)
	}
	response := identityTestChatResponse("ordinary text")
	const raw = `{"number":9007199254740993,"ordinary":0.25}`
	response.ChatResponse.ExtraFields.RawResponse = json.RawMessage(raw)
	if _, _, err := p.PostLLMHook(ctx, response, nil); err != nil {
		t.Fatal(err)
	}
	p.WaitForPendingOperations()
	_, hit, err := p.PreLLMHook(identityContext(t, "test", identityRef{vk: "a"}), req)
	if err != nil || hit == nil || hit.Response == nil {
		t.Fatal("expected replay", err)
	}
	got, err := json.Marshal(hit.Response.ChatResponse.ExtraFields.RawResponse)
	if err != nil || string(got) != raw {
		t.Fatalf("native response changed: %s, err=%v", got, err)
	}
	if _, ok := hit.Response.ChatResponse.ExtraFields.RawResponse.(json.RawMessage); !ok {
		t.Fatal("native JSON lost its opaque representation")
	}
	if got := hit.Response.ChatResponse.Choices[0].Message.Content.ContentStr; got == nil || *got != "ordinary text" {
		t.Fatal("typed response changed")
	}
	// SSE raw responses are strings, not JSON objects. Preserve their type too.
	response.ChatResponse.ExtraFields.RawResponse = "event: message\ndata: {\"n\":9007199254740993}\n\n"
	encoded, err := marshalCachedResponse(response)
	if err != nil {
		t.Fatal(err)
	}
	var decoded schemas.BifrostResponse
	if err := unmarshalCachedResponse(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ChatResponse.ExtraFields.RawResponse != response.ChatResponse.ExtraFields.RawResponse {
		t.Fatal("native SSE string changed")
	}
	if err := unmarshalCachedResponse(append(encoded, []byte(` {}`)...), &decoded); err == nil {
		t.Fatal("trailing cached JSON accepted")
	}
}

func TestCacheIdentityResponsesRawResponsePrecision(t *testing.T) {
	p := identityScopeTestPlugin(t, newObservableStore(), "")
	req := &schemas.BifrostRequest{RequestType: schemas.ResponsesRequest, ResponsesRequest: &schemas.BifrostResponsesRequest{Provider: schemas.OpenAI, Model: "native"}}
	ctx := identityContext(t, "test", identityRef{vk: "a"})
	if _, hit, err := p.PreLLMHook(ctx, req); err != nil || hit != nil {
		t.Fatal("expected miss", err)
	}
	const raw = `{"number":9007199254740993,"ordinary":0.25}`
	response := &schemas.BifrostResponse{ResponsesResponse: &schemas.BifrostResponsesResponse{ID: schemas.Ptr("native"), Model: "native", ExtraFields: schemas.BifrostResponseExtraFields{Provider: schemas.OpenAI, RequestType: schemas.ResponsesRequest, OriginalModelRequested: "native", ResolvedModelUsed: "native", RawResponse: json.RawMessage(raw)}}}
	if _, _, err := p.PostLLMHook(ctx, response, nil); err != nil {
		t.Fatal(err)
	}
	p.WaitForPendingOperations()
	_, hit, err := p.PreLLMHook(identityContext(t, "test", identityRef{vk: "a"}), req)
	if err != nil || hit == nil || hit.Response == nil {
		t.Fatal("expected replay", err)
	}
	got, err := json.Marshal(hit.Response.ResponsesResponse.ExtraFields.RawResponse)
	if err != nil || string(got) != raw {
		t.Fatalf("native Responses response changed: %s, err=%v", got, err)
	}
	if _, ok := hit.Response.ResponsesResponse.ExtraFields.RawResponse.(json.RawMessage); !ok {
		t.Fatal("native Responses JSON lost its opaque representation")
	}
}

func TestCacheIdentitySemanticGrouping(t *testing.T) {
	store := &semanticFilterStore{observableStore: newObservableStore()}
	p := identityScopeTestPlugin(t, store, `{"provider":"openai","embedding_model":"embedding","dimension":3}`)
	embeddings := 0
	p.SetEmbeddingRequestExecutor(func(_ *schemas.BifrostContext, _ *schemas.BifrostEmbeddingRequest) (*schemas.BifrostEmbeddingResponse, *schemas.BifrostError) {
		embeddings++
		return &schemas.BifrostEmbeddingResponse{Data: []schemas.EmbeddingData{{Embedding: schemas.EmbeddingStruct{EmbeddingArray: []float64{.1, .2, .3}}}}}, nil
	})
	for _, prompt := range []string{"first phrasing", "different phrasing"} {
		ctx := identityContext(t, "test", identityRef{vk: "a"})
		ctx.SetValue(CacheTypeKey, CacheTypeSemantic)
		cacheHits(t, p, ctx, identityTestChatRequest(prompt, nil))
	}
	if len(store.searches) != 2 {
		t.Fatalf("expected two semantic searches, got %d", len(store.searches))
	}
	paramsHash := func(index int) string {
		t.Helper()
		value, exists := filterValue(store.searches[index], "params_hash")
		hash, ok := value.(string)
		if !exists || !ok || hash == "" {
			t.Fatal("semantic searches must carry nonempty parameter hashes")
		}
		return hash
	}
	a, b := paramsHash(0), paramsHash(1)
	if a != b {
		t.Error("prompt polluted semantic parameter grouping")
	}
	changed := identityContext(t, "test", identityRef{vk: "a"})
	changed.SetValue(CacheTypeKey, CacheTypeSemantic)
	cacheHits(t, p, changed, identityTestChatRequest("first phrasing", map[string]interface{}{"temperature": 0.1}))
	if len(store.searches) != 3 {
		t.Fatalf("changed parameters did not reach semantic search: %d searches", len(store.searches))
	}
	if paramsHash(2) == a {
		t.Fatal("changed inference parameters must change the semantic group")
	}
	ctx := identityContext(t, "test", identityRef{vk: "a"})
	ctx.SetValue(CacheTypeKey, CacheTypeSemantic)
	ctx.SetValue(schemas.BifrostContextKeyUseRawRequestBody, true)
	req := identityTestChatRequest("native prompt", nil)
	req.ChatRequest.RawRequestBody = []byte(`{"messages":[],"unknown_native_content":{"text":"sensitive"}}`)
	cacheHits(t, p, ctx, req)
	if embeddings != 3 {
		t.Error("raw passthrough spent embeddings on an unsafe semantic lookup")
	}
}

func BenchmarkCacheIdentity(b *testing.B) {
	p := &Plugin{config: &Config{}}
	req := identityTestChatRequest("Exact input including CASE and whitespace ", map[string]interface{}{"future": uint64(9007199254740993)})
	metadata, err := p.buildRequestMetadataForCaching(nil, req)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := p.generateRequestHash(req, metadata); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCacheIdentityHTTPContext(b *testing.B) {
	llm, err := Init(b.Context(), &Config{Dimension: 1, DefaultCacheKey: "benchmark"}, bifrost.NewDefaultLogger(schemas.LogLevelError), newObservableStore())
	if err != nil {
		b.Fatal(err)
	}
	p := llm.(*Plugin)
	b.Cleanup(func() { _ = p.Cleanup() })
	ctx := schemas.NewBifrostContext(b.Context(), schemas.NoDeadline)
	defer ctx.Cancel()
	ctx.SetValue(schemas.BifrostContextKeyRequestID, "cache-http-benchmark")
	httpReq := &schemas.HTTPRequest{
		Method: "POST", Path: "/anthropic/v1/messages",
		Headers: map[string]string{"anthropic-version": "2023-06-01", "anthropic-beta": "feature-a"},
		Query:   map[string]string{"api-version": "v1"},
	}
	ctx.SetValue(schemas.BifrostContextKeyRequestHeaders, httpReq.Headers)
	ctx.SetValue(schemas.BifrostContextKeyRequestQuery, httpReq.Query)
	req := identityTestChatRequest("same input", nil)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := p.HTTPTransportPreHook(ctx, httpReq); err != nil {
			b.Fatal(err)
		}
		if _, _, err := p.PreLLMHook(ctx, req); err != nil {
			b.Fatal(err)
		}
	}
}
