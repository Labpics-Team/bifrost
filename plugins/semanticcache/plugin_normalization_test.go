package semanticcache

import (
	"testing"

	"github.com/maximhq/bifrost/core/schemas"
)

// TestDirectCachePreservesExactText keeps case and whitespace significant for
// direct replay, while identical requests still hit the cache.
func TestDirectCachePreservesExactText(t *testing.T) {
	t.Parallel()
	setup := NewTestSetup(t)
	defer setup.Cleanup()

	t.Run("ChatCompletion", func(t *testing.T) {
		testChatCompletionExactInput(t, setup)
	})

	t.Run("Speech", func(t *testing.T) {
		testSpeechExactInput(t, setup)
	})
}

func testChatCompletionExactInput(t *testing.T, setup *TestSetup) {
	ctx := CreateContextWithCacheKeyAndType(t, "test-chat-exact-input", CacheTypeDirect)
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

	// Each changed input must miss the direct cache, then replay under its own ID.
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

		AssertNoCacheHit(t, &schemas.BifrostResponse{ChatResponse: response})
		WaitForCache(setup.Plugin)
		replay, replayErr := setup.Client.ChatCompletionRequest(ctx, requests[i])
		if replayErr != nil || replay == nil || len(replay.Choices) == 0 {
			t.Fatalf("Exact replay for case %q failed: %v", tc.name, replayErr)
		}
		AssertCacheHit(t, &schemas.BifrostResponse{ChatResponse: replay}, "direct")
	}
}

func testSpeechExactInput(t *testing.T, setup *TestSetup) {
	ctx := CreateContextWithCacheKeyAndType(t, "test-speech-exact-input", CacheTypeDirect)
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

	// Each changed input must miss the direct cache, then replay under its own ID.
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

		AssertNoCacheHit(t, &schemas.BifrostResponse{SpeechResponse: response})
		WaitForCache(setup.Plugin)
		replay, replayErr := setup.Client.SpeechRequest(ctx, requests[i])
		if replayErr != nil || replay == nil {
			t.Fatalf("Exact replay for case %q failed: %v", tc.name, replayErr)
		}
		AssertCacheHit(t, &schemas.BifrostResponse{SpeechResponse: replay}, "direct")
	}
}

// TestDirectCachePreservesExactContentBlocks applies the same exact-input
// contract to text carried in content blocks.
func TestDirectCachePreservesExactContentBlocks(t *testing.T) {
	t.Parallel()
	setup := NewTestSetup(t)
	defer setup.Cleanup()

	ctx := CreateContextWithCacheKeyAndType(t, "test-content-blocks-exact-input", CacheTypeDirect)
	defer ctx.Cancel()

	// Test cases with distinct case and whitespace in content blocks.
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

	// Each changed input must miss the direct cache, then replay under its own ID.
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

		AssertNoCacheHit(t, &schemas.BifrostResponse{ChatResponse: response})
		WaitForCache(setup.Plugin)
		replay, replayErr := setup.Client.ChatCompletionRequest(ctx, requests[i])
		if replayErr != nil || replay == nil || len(replay.Choices) == 0 {
			t.Fatalf("Exact replay for case %q failed: %v", tc.name, replayErr)
		}
		AssertCacheHit(t, &schemas.BifrostResponse{ChatResponse: replay}, "direct")
	}
}

// TestNormalizationWithSemanticCache tests that normalization works with semantic cache as well
func TestNormalizationWithSemanticCache(t *testing.T) {
	t.Parallel()
	setup := NewTestSetup(t)
	defer setup.Cleanup()

	ctx := CreateContextWithCacheKeyAndType(t, "test-normalization-semantic", CacheTypeSemantic)
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

	// Normalization belongs to the semantic path; it must not be a direct hit.
	AssertCacheHit(t, &schemas.BifrostResponse{ChatResponse: response2}, "semantic")
	t.Log("✓ Semantic cache hit with normalized text")

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
