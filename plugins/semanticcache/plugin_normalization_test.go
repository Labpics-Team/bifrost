package semanticcache

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
)

// Exercise native matching through the client and provider boundary without an
// external vector store: normalized FAQ text and reordered tool sets replay the
// first answer instead of making another provider request.
func TestDirectCacheKeepsNativeMatching(t *testing.T) {
	const question = "How Do I Reset My Password?"
	const answer = "Use the password reset link on the sign-in page."
	tools := []schemas.ChatTool{
		{Type: schemas.ChatToolTypeFunction, Function: &schemas.ChatToolFunction{Name: "lookup_faq"}},
		{Type: schemas.ChatToolTypeFunction, Function: &schemas.ChatToolFunction{Name: "contact_support"}},
	}
	for _, tc := range []struct {
		name       string
		prompts    []string
		toolOrders [][]schemas.ChatTool
	}{
		{
			name: "TextNormalization",
			prompts: []string{
				question,
				strings.ToLower(question),
				" \t" + question + "\n ",
				" \t" + strings.ToLower(question) + "\n ",
			},
		},
		{
			name:       "ToolSetOrder",
			prompts:    []string{question, question},
			toolOrders: [][]schemas.ChatTool{tools, {tools[1], tools[0]}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sends atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				sends.Add(1)
				response := identityTestChatResponse(answer).ChatResponse
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
			var cacheID string
			for i, prompt := range tc.prompts {
				request := identityTestChatRequest(prompt, nil).ChatRequest
				if tc.toolOrders != nil {
					request.Params = &schemas.ChatParameters{
						Tools:      tc.toolOrders[i],
						ToolChoice: &schemas.ChatToolChoice{ChatToolChoiceStr: schemas.Ptr("none")},
					}
				}
				ctx := CreateContextWithCacheKeyAndType(t, "faq", CacheTypeDirect)
				response, err := client.ChatCompletionRequest(ctx, request)
				plugin.WaitForPendingOperations()
				ctx.Cancel()
				if err != nil || response == nil || len(response.Choices) != 1 || response.Choices[0].Message == nil || response.Choices[0].Message.Content == nil || response.Choices[0].Message.Content.ContentStr == nil {
					t.Fatalf("request %d did not return an answer: response=%+v error=%v", i, response, err)
				}
				if got := *response.Choices[0].Message.Content.ContentStr; got != answer {
					t.Fatalf("request %d changed the cached answer: %q", i, got)
				}
				debug := response.ExtraFields.CacheDebug
				if debug == nil || debug.CacheHit != (i > 0) || sends.Load() != 1 {
					t.Fatalf("request %d: want cached=%v with one provider send, got cache=%+v sends=%d", i, i > 0, debug, sends.Load())
				}
				if debug.CacheID == nil || *debug.CacheID == "" {
					t.Fatalf("request %d has no cache ID", i)
				}
				if i == 0 {
					cacheID = *debug.CacheID
				} else if debug.HitType == nil || *debug.HitType != "direct" || *debug.CacheID != cacheID {
					t.Fatalf("request %d did not directly replay cache ID %q: %+v", i, cacheID, debug)
				}
			}
		})
	}
}

// TestTextNormalizationDirectCache tests that text normalization works correctly
// for direct cache (hash-based) matching across all input types
func TestTextNormalizationDirectCache(t *testing.T) {
	t.Parallel()
	setup := NewTestSetup(t)
	defer setup.Cleanup()

	t.Run("ChatCompletion", func(t *testing.T) {
		testChatCompletionNormalization(t, setup)
	})

	t.Run("Speech", func(t *testing.T) {
		testSpeechNormalization(t, setup)
	})
}

func testChatCompletionNormalization(t *testing.T, setup *TestSetup) {
	ctx := CreateContextWithCacheKeyAndType(t, "test-chat-normalization", CacheTypeDirect)
	defer ctx.Cancel()

	// Test cases with different case and whitespace variations
	testCases := []struct {
		name      string
		userMsg   string
		systemMsg string
	}{
		{
			name:      "Original",
			userMsg:   "Explain quantum physics",
			systemMsg: "You are a helpful science teacher",
		},
		{
			name:      "Lowercase",
			userMsg:   "explain quantum physics",
			systemMsg: "you are a helpful science teacher",
		},
		{
			name:      "Uppercase",
			userMsg:   "EXPLAIN QUANTUM PHYSICS",
			systemMsg: "YOU ARE A HELPFUL SCIENCE TEACHER",
		},
		{
			name:      "Mixed Case",
			userMsg:   "ExPlAiN QuAnTuM PhYsIcS",
			systemMsg: "YoU aRe A hElPfUl ScIeNcE tEaChEr",
		},
		{
			name:      "With Whitespace",
			userMsg:   "  Explain quantum physics  ",
			systemMsg: "  You are a helpful science teacher  ",
		},
		{
			name:      "Extra Whitespace",
			userMsg:   "    Explain quantum physics    ",
			systemMsg: "    You are a helpful science teacher    ",
		},
	}

	// Create chat completion requests for all test cases
	requests := make([]*schemas.BifrostChatRequest, len(testCases))
	for i, tc := range testCases {
		requests[i] = &schemas.BifrostChatRequest{
			Provider: schemas.OpenAI,
			Model:    "gpt-4o-mini",
			Input: []schemas.ChatMessage{
				{
					Role: schemas.ChatMessageRoleSystem,
					Content: &schemas.ChatMessageContent{
						ContentStr: &tc.systemMsg,
					},
				},
				{
					Role: schemas.ChatMessageRoleUser,
					Content: &schemas.ChatMessageContent{
						ContentStr: &tc.userMsg,
					},
				},
			},
			Params: &schemas.ChatParameters{
				Temperature:         PtrFloat64(0.5),
				MaxCompletionTokens: PtrInt(50),
			},
		}
	}

	// Make first request (should miss cache and be stored)
	t.Logf("Making first request with user: '%s', system: '%s'", testCases[0].userMsg, testCases[0].systemMsg)
	response1, err1 := setup.Client.ChatCompletionRequest(ctx, requests[0])
	if err1 != nil {
		if isTransientUpstreamError(err1) {
			t.Skipf("transient upstream error, skipping test: %v", err1)
		}
		t.Fatalf("upstream request failed: %v", err1)
	}

	if response1 == nil || len(response1.Choices) == 0 {
		t.Fatal("First response is invalid")
	}

	AssertNoCacheHit(t, &schemas.BifrostResponse{ChatResponse: response1})
	WaitForCache(setup.Plugin)

	// Test all other variations should hit cache due to normalization
	for i := 1; i < len(testCases); i++ {
		tc := testCases[i]
		t.Logf("Testing variation '%s' with user: '%s', system: '%s'", tc.name, tc.userMsg, tc.systemMsg)

		response, err := setup.Client.ChatCompletionRequest(ctx, requests[i])
		if err != nil {
			t.Fatalf("Request for case '%s' failed: %v", tc.name, err)
		}

		if response == nil || len(response.Choices) == 0 {
			t.Fatalf("Response for case '%s' is invalid", tc.name)
		}

		// Should be cache hit due to normalization
		AssertCacheHit(t, &schemas.BifrostResponse{ChatResponse: response}, "direct")
		t.Logf("✓ Cache hit for '%s' variation", tc.name)
	}
}

func testSpeechNormalization(t *testing.T, setup *TestSetup) {
	ctx := CreateContextWithCacheKeyAndType(t, "test-speech-normalization", CacheTypeDirect)
	defer ctx.Cancel()

	// Test cases with different case and whitespace variations for speech input
	testCases := []struct {
		name  string
		input string
	}{
		{"Original", "Hello, this is a test speech synthesis"},
		{"Lowercase", "hello, this is a test speech synthesis"},
		{"Uppercase", "HELLO, THIS IS A TEST SPEECH SYNTHESIS"},
		{"Mixed Case", "HeLLo, ThIs Is A tEsT sPeEcH sYnThEsIs"},
		{"Leading Whitespace", "  Hello, this is a test speech synthesis"},
		{"Trailing Whitespace", "Hello, this is a test speech synthesis  "},
		{"Both Whitespace", "  Hello, this is a test speech synthesis  "},
		{"Extra Spaces", "   Hello, this is a test speech synthesis   "},
	}

	// Create speech requests for all test cases
	requests := make([]*schemas.BifrostSpeechRequest, len(testCases))
	for i, tc := range testCases {
		requests[i] = CreateSpeechRequest(tc.input, "alloy")
	}

	// Make first request (should miss cache and be stored)
	t.Logf("Making first speech request with: '%s'", testCases[0].input)
	response1, err1 := setup.Client.SpeechRequest(ctx, requests[0])
	if err1 != nil {
		if isTransientUpstreamError(err1) {
			t.Skipf("transient upstream error, skipping test: %v", err1)
		}
		t.Fatalf("upstream request failed: %v", err1)
	}

	if response1 == nil {
		t.Fatal("First response is invalid")
	}

	AssertNoCacheHit(t, &schemas.BifrostResponse{SpeechResponse: response1})
	WaitForCache(setup.Plugin)

	// Test all other variations should hit cache due to normalization
	for i := 1; i < len(testCases); i++ {
		tc := testCases[i]
		t.Logf("Testing variation '%s' with input: '%s'", tc.name, tc.input)

		response, err := setup.Client.SpeechRequest(ctx, requests[i])
		if err != nil {
			t.Fatalf("Request for case '%s' failed: %v", tc.name, err)
		}

		if response == nil {
			t.Fatalf("Response for case '%s' is invalid", tc.name)
		}

		// Should be cache hit due to normalization
		AssertCacheHit(t, &schemas.BifrostResponse{SpeechResponse: response}, "direct")
		t.Logf("✓ Cache hit for '%s' variation", tc.name)
	}
}

// TestChatCompletionContentBlocksNormalization tests normalization for content blocks
func TestChatCompletionContentBlocksNormalization(t *testing.T) {
	t.Parallel()
	setup := NewTestSetup(t)
	defer setup.Cleanup()

	ctx := CreateContextWithCacheKeyAndType(t, "test-content-blocks-normalization", CacheTypeDirect)
	defer ctx.Cancel()

	// Test cases with content blocks having different text normalization
	testCases := []struct {
		name       string
		textBlocks []string
	}{
		{
			name:       "Original",
			textBlocks: []string{"Hello World", "How are you today?"},
		},
		{
			name:       "Lowercase",
			textBlocks: []string{"hello world", "how are you today?"},
		},
		{
			name:       "With Whitespace",
			textBlocks: []string{"  Hello World  ", "  How are you today?  "},
		},
		{
			name:       "Mixed Case",
			textBlocks: []string{"HeLLo WoRLd", "HoW aRe YoU tOdAy?"},
		},
	}

	// Create chat completion requests with content blocks
	requests := make([]*schemas.BifrostChatRequest, len(testCases))
	for i, tc := range testCases {
		// Create content blocks
		contentBlocks := make([]schemas.ChatContentBlock, len(tc.textBlocks))
		for j, text := range tc.textBlocks {
			contentBlocks[j] = schemas.ChatContentBlock{
				Type: schemas.ChatContentBlockTypeText,
				Text: &text,
			}
		}

		requests[i] = &schemas.BifrostChatRequest{
			Provider: schemas.OpenAI,
			Model:    "gpt-4o-mini",
			Input: []schemas.ChatMessage{
				{
					Role: schemas.ChatMessageRoleUser,
					Content: &schemas.ChatMessageContent{
						ContentBlocks: contentBlocks,
					},
				},
			},
			Params: &schemas.ChatParameters{
				Temperature:         PtrFloat64(0.5),
				MaxCompletionTokens: PtrInt(50),
			},
		}
	}

	// Make first request (should miss cache and be stored)
	t.Logf("Making first request with content blocks: %v", testCases[0].textBlocks)
	response1, err1 := setup.Client.ChatCompletionRequest(ctx, requests[0])
	if err1 != nil {
		if isTransientUpstreamError(err1) {
			t.Skipf("transient upstream error, skipping test: %v", err1)
		}
		t.Fatalf("upstream request failed: %v", err1)
	}

	if response1 == nil || len(response1.Choices) == 0 {
		t.Fatal("First response is invalid")
	}

	AssertNoCacheHit(t, &schemas.BifrostResponse{ChatResponse: response1})
	WaitForCache(setup.Plugin)

	// Test all other variations should hit cache due to normalization
	for i := 1; i < len(testCases); i++ {
		tc := testCases[i]
		t.Logf("Testing variation '%s' with content blocks: %v", tc.name, tc.textBlocks)

		response, err := setup.Client.ChatCompletionRequest(ctx, requests[i])
		if err != nil {
			t.Fatalf("Request for case '%s' failed: %v", tc.name, err)
		}

		if response == nil || len(response.Choices) == 0 {
			t.Fatalf("Response for case '%s' is invalid", tc.name)
		}

		// Should be cache hit due to normalization
		AssertCacheHit(t, &schemas.BifrostResponse{ChatResponse: response}, "direct")
		t.Logf("✓ Cache hit for '%s' variation", tc.name)
	}
}

// TestNormalizationWithSemanticCache tests that normalization works with semantic cache as well
func TestNormalizationWithSemanticCache(t *testing.T) {
	t.Parallel()
	setup := NewTestSetup(t)
	defer setup.Cleanup()

	ctx := CreateContextWithCacheKey(t, "test-normalization-semantic")
	defer ctx.Cancel()

	// Make first request with original text
	originalRequest := CreateBasicChatRequest("What is Machine Learning?", 0.5, 50)
	t.Log("Making first request with original text...")
	response1, err1 := setup.Client.ChatCompletionRequest(ctx, originalRequest)
	if err1 != nil {
		if isTransientUpstreamError(err1) {
			t.Skipf("transient upstream error, skipping test: %v", err1)
		}
		t.Fatalf("upstream request failed: %v", err1)
	}

	AssertNoCacheHit(t, &schemas.BifrostResponse{ChatResponse: response1})
	WaitForCache(setup.Plugin)

	// Test semantic match with different case (should hit semantic cache after normalization)
	normalizedRequest := CreateBasicChatRequest("what is machine learning?", 0.5, 50)
	t.Log("Making semantic request with normalized case...")
	response2, err2 := setup.Client.ChatCompletionRequest(ctx, normalizedRequest)
	if err2 != nil {
		if err2.Error != nil {
			t.Fatalf("Second request failed: %v", err2.Error.Message)
		} else {
			t.Fatalf("Second request failed: %v", err2)
		}
	}

	// This should be a direct cache hit since the normalized text is identical
	AssertCacheHit(t, &schemas.BifrostResponse{ChatResponse: response2}, "direct")
	t.Log("✓ Direct cache hit with normalized text")

	// Test with semantically similar but different text
	semanticRequest := CreateBasicChatRequest("can you explain machine learning concepts?", 0.5, 50)
	t.Log("Making semantically similar request...")
	response3, err3 := setup.Client.ChatCompletionRequest(ctx, semanticRequest)
	if err3 != nil {
		t.Fatalf("Third request failed: %v", err3)
	}

	// This should be a semantic cache hit
	AssertCacheHit(t, &schemas.BifrostResponse{ChatResponse: response3}, "semantic")
	t.Log("✓ Semantic cache hit with similar content")
}

// Helper functions for pointer creation
func PtrFloat64(f float64) *float64 {
	return &f
}

func PtrInt(i int) *int {
	return &i
}
