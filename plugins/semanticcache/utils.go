package semanticcache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/vectorstore"
	"github.com/tidwall/gjson"
)

// Cache the same completion usage fields sent to clients, so explicit zeros
// survive without a separate metadata format. Encoding stays sonic.Marshal as
// before the fix; only Chat/Text responses with an explicit zero are wrapped.
func marshalCachedResponse(res *schemas.BifrostResponse) ([]byte, error) {
	if res.ChatResponse != nil {
		if view := res.ChatResponse.WithPreservedUsage(); view != res.ChatResponse {
			return sonic.Marshal(&struct {
				*schemas.BifrostResponse
				ChatResponse any
			}{res, view})
		}
	}
	if res.TextCompletionResponse != nil {
		if view := res.TextCompletionResponse.WithPreservedUsage(); view != res.TextCompletionResponse {
			return sonic.Marshal(&struct {
				*schemas.BifrostResponse
				TextCompletionResponse any
			}{res, view})
		}
	}
	return sonic.Marshal(res)
}

func unmarshalCachedResponse(data []byte, res *schemas.BifrostResponse) error {
	// Native raw JSON is held in interface{} fields. Keep its exact numbers;
	// converting through float64 would corrupt integers beyond 2^53 on replay.
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(res); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("cached response contains trailing data")
	}
	// Some response variants have custom JSON decoders, which do not inherit
	// UseNumber. Restore opaque native JSON from the validated cache envelope,
	// without interpreting it through those typed decoders. SSE strings keep
	// their string type. The union's active variant is the only non-null payload.
	if extra := res.GetExtraFields(); extra != nil && extra.RawResponse != nil {
		gjson.ParseBytes(data).ForEach(func(_, response gjson.Result) bool {
			raw := response.Get("extra_fields.raw_response")
			if raw.IsObject() || raw.IsArray() {
				extra.RawResponse = json.RawMessage(raw.Raw)
				return false
			}
			return true
		})
	}
	if res.ChatResponse != nil {
		res.ChatResponse.Usage.PreserveTokenCountZeros(data, "ChatResponse.usage.prompt_tokens", "ChatResponse.usage.completion_tokens")
	}
	if res.TextCompletionResponse != nil {
		res.TextCompletionResponse.Usage.PreserveTokenCountZeros(data, "TextCompletionResponse.usage.prompt_tokens", "TextCompletionResponse.usage.completion_tokens")
	}
	return nil
}

// directCacheNamespace is a fixed namespace UUID for generating deterministic
// UUID v5 cache IDs via uuid.NewSHA1, used by generateDirectCacheID. The
// bytes are arbitrary — they only need to be stable across restarts so the
// same (cache_key, request_hash, params_hash) tuple maps to the same ID.
var directCacheNamespace = uuid.MustParse("b1f3c2d4-e5a6-7890-abcd-ef1234567890")

// isSemanticCacheSupportedRequestType reports whether semantic cache supports
// this request type for cache lookup and storage. Unsupported types are skipped.
//
// IMPORTANT: this list must stay in sync with the switch in buildRequestMetadataForCaching.
// When adding a new case there, add it here too.
func isSemanticCacheSupportedRequestType(requestType schemas.RequestType) bool {
	switch requestType {
	case schemas.TextCompletionRequest,
		schemas.TextCompletionStreamRequest,
		schemas.ChatCompletionRequest,
		schemas.ChatCompletionStreamRequest,
		schemas.ResponsesRequest,
		schemas.ResponsesStreamRequest,
		schemas.WebSocketResponsesRequest,
		schemas.SpeechRequest,
		schemas.SpeechStreamRequest,
		schemas.EmbeddingRequest,
		schemas.TranscriptionRequest,
		schemas.TranscriptionStreamRequest,
		schemas.ImageGenerationRequest,
		schemas.ImageGenerationStreamRequest:
		return true
	default:
		return false
	}
}

// hashMap preserves integer precision and array ordering. encoding/json sorts
// map keys without converting typed values through float64.
func hashMap(m map[string]interface{}) (string, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("failed to marshal cache metadata: %w", err)
	}
	return fmt.Sprintf("%x", xxhash.Sum64(data)), nil
}

// normalizeText applies consistent normalization to text inputs for better cache hit rates.
// It converts text to lowercase and trims whitespace to reduce cache misses due to minor variations.
func normalizeText(text string) string {
	return strings.ToLower(strings.TrimSpace(text))
}

// float64ToFloat32Embedding converts a []float64 to a []float32. The semantic cache
// keeps vector payloads as float32 even though the embedding APIs now
// preserve full float64 precision — the cosine similarity used at query
// time is well within float32 range.
func float64ToFloat32Embedding(values []float64) []float32 {
	if len(values) == 0 {
		return nil
	}

	embedding := make([]float32, len(values))
	for i, value := range values {
		embedding[i] = float32(value)
	}

	return embedding
}

// int8ToFloat32Embedding promotes a quantized int8 embedding (used for
// binary/quantized formats by some providers) to float32 so the cache can
// store and compare it uniformly against float32 entries.
func int8ToFloat32Embedding(values []int8) []float32 {
	if len(values) == 0 {
		return nil
	}
	embedding := make([]float32, len(values))
	for i, value := range values {
		embedding[i] = float32(value)
	}
	return embedding
}

// int32ToFloat32Embedding promotes a uint8/ubinary-style int32 embedding to
// float32 for the same reason as int8ToFloat32Embedding.
func int32ToFloat32Embedding(values []int32) []float32 {
	if len(values) == 0 {
		return nil
	}
	embedding := make([]float32, len(values))
	for i, value := range values {
		embedding[i] = float32(value)
	}
	return embedding
}

// decodeBase64Embedding decodes a base64-encoded embedding of raw IEEE 754 float32 bytes (little-endian).
func decodeBase64Embedding(s string) ([]float32, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		b, err = base64.URLEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("base64 decode failed: %w", err)
		}
	}
	if len(b)%4 != 0 {
		return nil, fmt.Errorf("base64 embedding byte length %d is not a multiple of 4", len(b))
	}
	vals := make([]float32, len(b)/4)
	for i := range vals {
		bits := binary.LittleEndian.Uint32(b[i*4 : i*4+4])
		vals[i] = math.Float32frombits(bits)
	}
	return vals, nil
}

// uint8ToFloat32Embedding promotes a uint8/ubinary embedding to float32 for
// cosine-similarity search in the semantic cache.
func uint8ToFloat32Embedding(values []uint8) []float32 {
	if len(values) == 0 {
		return nil
	}
	embedding := make([]float32, len(values))
	for i, value := range values {
		embedding[i] = float32(value)
	}
	return embedding
}

// flattenToFloat32Embedding concatenates a 2D embedding (one inner slice per
// input chunk) into a single flat []float32. Used when the provider returns
// per-chunk embeddings that we want to store as a single vector.
func flattenToFloat32Embedding(values [][]float64) []float32 {
	total := 0
	for _, arr := range values {
		total += len(arr)
	}
	if total == 0 {
		return nil
	}

	embedding := make([]float32, 0, total)
	for _, arr := range values {
		embedding = append(embedding, float64ToFloat32Embedding(arr)...)
	}

	return embedding
}

// buildRequestMetadataForCaching extracts the canonical, hashable parameter
// set for the request: anything that should change the cache key when it
// changes. The returned map is fed to hashMap to derive params_hash, which
// then anchors both direct and semantic lookups.
func (plugin *Plugin) buildRequestMetadataForCaching(state *cacheState, req *schemas.BifrostRequest) (map[string]interface{}, error) {
	metadata := map[string]interface{}{
		"stream":                 bifrost.IsStreamRequestType(req.RequestType),
		"cache_identity_version": 2,
	}

	if attachments := plugin.extractAttachmentsForCaching(state, req); len(attachments) > 0 {
		metadata["attachments"] = attachments
	}

	var rawBody []byte
	// request_family separates endpoints whose input and params hash the same,
	// such as chat and responses (issue #7560). It is set after the params so
	// extra params cannot override it. Stream variants share a family because
	// "stream" is already in the hash.
	switch req.RequestType {
	case schemas.TextCompletionRequest, schemas.TextCompletionStreamRequest:
		if req.TextCompletionRequest == nil {
			return nil, fmt.Errorf("text completion payload is nil")
		}
		rawBody = req.TextCompletionRequest.RawRequestBody
		if req.TextCompletionRequest != nil && req.TextCompletionRequest.Params != nil {
			metadata["parameters"] = req.TextCompletionRequest.Params
			metadata["extra_parameters"] = req.TextCompletionRequest.Params.ExtraParams
		}
		metadata["request_family"] = schemas.TextCompletionRequest
	case schemas.ChatCompletionRequest, schemas.ChatCompletionStreamRequest:
		if req.ChatRequest == nil {
			return nil, fmt.Errorf("chat payload is nil")
		}
		rawBody = req.ChatRequest.RawRequestBody
		if req.ChatRequest != nil && req.ChatRequest.Params != nil {
			metadata["parameters"] = req.ChatRequest.Params
			metadata["extra_parameters"] = req.ChatRequest.Params.ExtraParams
		}
		metadata["request_family"] = schemas.ChatCompletionRequest
	case schemas.ResponsesRequest, schemas.ResponsesStreamRequest, schemas.WebSocketResponsesRequest:
		if req.ResponsesRequest == nil {
			return nil, fmt.Errorf("responses payload is nil")
		}
		rawBody = req.ResponsesRequest.RawRequestBody
		if req.ResponsesRequest != nil && req.ResponsesRequest.Params != nil {
			metadata["parameters"] = req.ResponsesRequest.Params
			metadata["extra_parameters"] = req.ResponsesRequest.Params.ExtraParams
		}
		metadata["request_family"] = schemas.ResponsesRequest
	case schemas.SpeechRequest, schemas.SpeechStreamRequest:
		if req.SpeechRequest == nil {
			return nil, fmt.Errorf("speech payload is nil")
		}
		rawBody = req.SpeechRequest.RawRequestBody
		if req.SpeechRequest != nil && req.SpeechRequest.Params != nil {
			metadata["parameters"] = req.SpeechRequest.Params
			metadata["extra_parameters"] = req.SpeechRequest.Params.ExtraParams
		}
		metadata["request_family"] = schemas.SpeechRequest
	case schemas.EmbeddingRequest:
		if req.EmbeddingRequest == nil {
			return nil, fmt.Errorf("embedding payload is nil")
		}
		rawBody = req.EmbeddingRequest.RawRequestBody
		if req.EmbeddingRequest != nil && req.EmbeddingRequest.Params != nil {
			metadata["parameters"] = req.EmbeddingRequest.Params
			metadata["extra_parameters"] = req.EmbeddingRequest.Params.ExtraParams
		}
		metadata["request_family"] = schemas.EmbeddingRequest
	case schemas.TranscriptionRequest, schemas.TranscriptionStreamRequest:
		if req.TranscriptionRequest == nil {
			return nil, fmt.Errorf("transcription payload is nil")
		}
		rawBody = req.TranscriptionRequest.RawRequestBody
		if req.TranscriptionRequest != nil && req.TranscriptionRequest.Params != nil {
			metadata["parameters"] = req.TranscriptionRequest.Params
			metadata["extra_parameters"] = req.TranscriptionRequest.Params.ExtraParams
		}
		metadata["request_family"] = schemas.TranscriptionRequest
	case schemas.ImageGenerationRequest, schemas.ImageGenerationStreamRequest:
		if req.ImageGenerationRequest == nil {
			return nil, fmt.Errorf("image generation payload is nil")
		}
		rawBody = req.ImageGenerationRequest.RawRequestBody
		if req.ImageGenerationRequest != nil && req.ImageGenerationRequest.Params != nil {
			metadata["parameters"] = req.ImageGenerationRequest.Params
			metadata["extra_parameters"] = req.ImageGenerationRequest.Params.ExtraParams
		}
		metadata["request_family"] = schemas.ImageGenerationRequest
	default:
		return nil, fmt.Errorf("unsupported request type for semantic caching")
	}

	if state != nil && state.UseRawRequestBody {
		if len(rawBody) == 0 {
			return nil, fmt.Errorf("raw request body unavailable for cache identity")
		}
		// A native passthrough may carry fields absent from the typed request.
		// It can only use exact replay; never expose its body in stored metadata.
		metadata["raw_body_sha256"] = fmt.Sprintf("%x", sha256.Sum256(rawBody))
	}

	// identity_scope keeps entries with the identity that wrote them (see
	// resolveIdentityScope). Like request_family it is set after the params, so a
	// request body field of the same name can never claim another identity's
	// scope; with scoping disabled such a field is dropped rather than hashed, so
	// the key stays the plugin's own in both modes.
	if state != nil && state.IdentityScope != nil {
		metadata["identity_scope"] = state.IdentityScope
	} else {
		delete(metadata, "identity_scope")
	}

	return metadata, nil
}

// responseHasToolCalls reports whether a response (or stream chunk) carries a
// tool call the client would execute: chat tool_calls on a message or delta,
// or a Responses API output item of a tool-call type. Server-side tool items
// (web search, file search, code interpreter, image generation) are not
// client-executed and do not count.
func responseHasToolCalls(res *schemas.BifrostResponse) bool {
	if res == nil {
		return false
	}
	switch {
	case res.ChatResponse != nil:
		for _, choice := range res.ChatResponse.Choices {
			if choice.ChatNonStreamResponseChoice != nil && choice.Message != nil &&
				choice.Message.ChatAssistantMessage != nil && len(choice.Message.ToolCalls) > 0 {
				return true
			}
			if choice.ChatStreamResponseChoice != nil && choice.Delta != nil && len(choice.Delta.ToolCalls) > 0 {
				return true
			}
		}
	case res.ResponsesResponse != nil:
		return responsesOutputHasToolCalls(res.ResponsesResponse.Output)
	case res.ResponsesStreamResponse != nil:
		if item := res.ResponsesStreamResponse.Item; item != nil && item.Type != nil && isResponsesToolCallItem(*item.Type) {
			return true
		}
		if full := res.ResponsesStreamResponse.Response; full != nil {
			return responsesOutputHasToolCalls(full.Output)
		}
	}
	return false
}

func responsesOutputHasToolCalls(output []schemas.ResponsesMessage) bool {
	for _, item := range output {
		if item.Type != nil && isResponsesToolCallItem(*item.Type) {
			return true
		}
	}
	return false
}

func isResponsesToolCallItem(t schemas.ResponsesMessageType) bool {
	switch t {
	case schemas.ResponsesMessageTypeFunctionCall,
		schemas.ResponsesMessageTypeCustomToolCall,
		schemas.ResponsesMessageTypeComputerCall,
		schemas.ResponsesMessageTypeLocalShellCall,
		schemas.ResponsesMessageTypeShellCall,
		schemas.ResponsesMessageTypeApplyPatchCall,
		schemas.ResponsesMessageTypeMCPCall,
		schemas.ResponsesMessageTypeMCPApprovalRequest:
		return true
	}
	return false
}

// extractAttachmentsForCaching collects image/file URLs referenced by the
// request input in document order. Attachments are part of the cache key —
// two messages with identical text but different images must not collide.
// Honors ExcludeSystemPrompt via getInputForCaching. Returns nil for
// request types without attachment-bearing content blocks.
func (plugin *Plugin) extractAttachmentsForCaching(state *cacheState, req *schemas.BifrostRequest) []string {
	switch req.RequestType {
	case schemas.ChatCompletionRequest, schemas.ChatCompletionStreamRequest:
		messages, ok := plugin.getInputForCaching(state, req).([]schemas.ChatMessage)
		if !ok {
			return nil
		}
		var attachments []string
		for _, msg := range messages {
			if msg.Content == nil || msg.Content.ContentBlocks == nil {
				continue
			}
			for _, block := range msg.Content.ContentBlocks {
				if block.ImageURLStruct != nil && block.ImageURLStruct.URL != "" {
					attachments = append(attachments, block.ImageURLStruct.URL)
				}
			}
		}
		return attachments
	case schemas.ResponsesRequest, schemas.ResponsesStreamRequest, schemas.WebSocketResponsesRequest:
		messages, ok := plugin.getInputForCaching(state, req).([]schemas.ResponsesMessage)
		if !ok {
			return nil
		}
		var attachments []string
		for _, msg := range messages {
			if msg.Content == nil || msg.Content.ContentBlocks == nil {
				continue
			}
			for _, block := range msg.Content.ContentBlocks {
				if block.ResponsesInputMessageContentBlockImage != nil && block.ResponsesInputMessageContentBlockImage.ImageURL != nil {
					attachments = append(attachments, *block.ResponsesInputMessageContentBlockImage.ImageURL)
				}
				if block.ResponsesInputMessageContentBlockFile != nil && block.ResponsesInputMessageContentBlockFile.FileURL != nil {
					attachments = append(attachments, *block.ResponsesInputMessageContentBlockFile.FileURL)
				}
			}
		}
		return attachments
	}
	return nil
}

// extractChatMessageContent flattens a ChatMessage's content (string or
// blocks) into a single space-joined string. Returns "" when the message
// carries no text (e.g. assistant tool-call messages with nil content).
func extractChatMessageContent(msg schemas.ChatMessage) string {
	if msg.Content == nil {
		return ""
	}
	if msg.Content.ContentStr != nil {
		return *msg.Content.ContentStr
	}
	if msg.Content.ContentBlocks != nil {
		var parts []string
		for _, block := range msg.Content.ContentBlocks {
			if block.Text != nil {
				parts = append(parts, *block.Text)
			}
		}
		return strings.Join(parts, " ")
	}
	return ""
}

// extractResponsesMessageContent flattens a ResponsesMessage's content into a
// single string, mirroring extractChatMessageContent but for the Responses API.
func extractResponsesMessageContent(msg schemas.ResponsesMessage) string {
	if msg.Content == nil {
		return ""
	}
	if msg.Content.ContentStr != nil {
		return *msg.Content.ContentStr
	}
	if msg.Content.ContentBlocks != nil {
		var parts []string
		for _, block := range msg.Content.ContentBlocks {
			if block.Text != nil {
				parts = append(parts, *block.Text)
			}
		}
		return strings.Join(parts, " ")
	}
	return ""
}

// extractTextForEmbedding flattens the request input into a single string
// suitable for embedding generation. PreLLMHook short-circuits embedding and
// transcription requests before this is called (their inputs aren't
// themselves embeddable), so this function only handles request types that
// reach performSemanticSearch.
//
// Text serialization format (for cache consistency):
//   - Chat API: "role: content"
//   - Responses API: "role: msgType: content" (when msgType is present), "role: content" (when msgType is empty)
func (plugin *Plugin) extractTextForEmbedding(state *cacheState, req *schemas.BifrostRequest) (string, error) {
	switch {
	case req.TextCompletionRequest != nil:
		if req.TextCompletionRequest.Input.PromptStr != nil {
			return normalizeText(*req.TextCompletionRequest.Input.PromptStr), nil
		}
		if len(req.TextCompletionRequest.Input.PromptArray) > 0 {
			return normalizeText(strings.Join(req.TextCompletionRequest.Input.PromptArray, " ")), nil
		}
		return "", fmt.Errorf("no prompt found in text completion request")

	case req.ChatRequest != nil:
		reqInput, ok := plugin.getInputForCaching(state, req).([]schemas.ChatMessage)
		if !ok {
			return "", fmt.Errorf("failed to cast request input to chat messages")
		}
		var textParts []string
		for _, msg := range reqInput {
			content := extractChatMessageContent(msg)
			if content == "" {
				continue
			}
			textParts = append(textParts, fmt.Sprintf("%s: %s", msg.Role, normalizeText(content)))
		}
		if len(textParts) == 0 {
			return "", fmt.Errorf("no text content found in chat messages")
		}
		return strings.Join(textParts, "\n"), nil

	case req.ResponsesRequest != nil:
		reqInput, ok := plugin.getInputForCaching(state, req).([]schemas.ResponsesMessage)
		if !ok {
			return "", fmt.Errorf("failed to cast request input to responses messages")
		}
		var textParts []string
		for _, msg := range reqInput {
			content := extractResponsesMessageContent(msg)
			if content == "" {
				continue
			}
			content = normalizeText(content)
			role := ""
			msgType := ""
			if msg.Role != nil {
				role = string(*msg.Role)
			}
			if msg.Type != nil {
				msgType = string(*msg.Type)
			}
			if msgType != "" {
				textParts = append(textParts, fmt.Sprintf("%s: %s: %s", role, msgType, content))
			} else {
				textParts = append(textParts, fmt.Sprintf("%s: %s", role, content))
			}
		}
		if len(textParts) == 0 {
			return "", fmt.Errorf("no text content found in responses messages")
		}
		return strings.Join(textParts, "\n"), nil

	case req.SpeechRequest != nil:
		if req.SpeechRequest.Input.Input == "" {
			return "", fmt.Errorf("no input text found in speech request")
		}
		return normalizeText(req.SpeechRequest.Input.Input), nil

	case req.ImageGenerationRequest != nil:
		if req.ImageGenerationRequest.Input == nil || req.ImageGenerationRequest.Input.Prompt == "" {
			return "", fmt.Errorf("no prompt found in image generation request")
		}
		return normalizeText(req.ImageGenerationRequest.Input.Prompt), nil

	default:
		return "", fmt.Errorf("unsupported input type for semantic caching")
	}
}

// buildUnifiedMetadata builds the property map written alongside the cache
// entry: the columns the vector store indexes for filtering (cache_key,
// provider, model, params_hash, expires_at) plus the from_bifrost marker
// used by Cleanup and ClearCacheForKey to scope deletes. Caller still adds
// the response payload (response or stream_chunks) before Add.
func (plugin *Plugin) buildUnifiedMetadata(provider schemas.ModelProvider, model string, paramsHash string, cacheKey string, ttl time.Duration) map[string]interface{} {
	unifiedMetadata := make(map[string]interface{})
	unifiedMetadata["provider"] = string(provider)
	unifiedMetadata["model"] = model
	unifiedMetadata["cache_key"] = cacheKey
	unifiedMetadata["from_bifrost_semantic_cache_plugin"] = true
	unifiedMetadata["expires_at"] = time.Now().Add(ttl).Unix()
	if paramsHash != "" {
		unifiedMetadata["params_hash"] = paramsHash
	}
	return unifiedMetadata
}

// addNonStreamingResponse writes an already-serialized response as a single
// cache entry. The caller (PostLLMHook) marshals the response synchronously
// before the hook returns — this function must only ever receive owned bytes,
// never the caller's *BifrostResponse, because core mutates that object right
// after the post-hook chain returns (issue #7233). The metadata map is
// mutated (response + stream_chunks added) — safe because the calling
// goroutine owns it. The ttl parameter is retained for symmetry with
// addStreamingResponse; the actual expiry is already encoded in
// metadata["expires_at"] by buildUnifiedMetadata.
func (plugin *Plugin) addNonStreamingResponse(ctx context.Context, responseID string, responseData []byte, embedding []float32, metadata map[string]interface{}, ttl time.Duration) error {
	metadata["response"] = string(responseData)
	metadata["stream_chunks"] = []string{}

	if err := plugin.store.Add(ctx, plugin.config.VectorStoreNamespace, responseID, embedding, metadata); err != nil {
		if vectorstore.IsUnavailable(err) {
			plugin.requestNamespaceRecovery()
		}
		return fmt.Errorf("failed to store unified cache entry: %w", err)
	}

	plugin.logger.Debug("Successfully cached single response with ID: %s", responseID)
	return nil
}

// addStreamingResponse appends one pre-serialized chunk to the per-request
// accumulator and reports whether the caller should flush the accumulated
// stream (final chunk observed, and this call won the completion gate). It
// runs synchronously inside PostLLMHook — only the flush's store write is
// async — so the accumulator never retains the caller's response pointer
// (issue #7233).
//
// Errors never reach this function: PostLLMHook returns early on bifrostErr
// (errors are always delivered as the final chunk), so an errored stream
// simply leaves its accumulator behind for the periodic reaper. A stream
// with a chunk that failed to serialize (see failStreamAccumulator) is
// different: it is never flushed, and its accumulator is dropped as soon as
// the final chunk arrives.
func (plugin *Plugin) addStreamingResponse(requestID string, storageID string, chunk *StreamChunk, embedding []float32, metadata map[string]interface{}, ttl time.Duration, isFinalChunk bool) (shouldFlush bool, err error) {
	accumulator := plugin.getOrCreateStreamAccumulator(requestID, storageID, embedding, metadata, ttl)

	accumulator.mu.Lock()
	failed := accumulator.Failed
	if failed {
		// Discarded chunks still count as arrivals: keep LastSeenAt moving so
		// the reaper doesn't evict the Failed marker mid-stream, which would
		// let the next chunk start a clean accumulator and flush a partial
		// entry on the final chunk.
		accumulator.LastSeenAt = chunk.Timestamp
	}
	accumulator.mu.Unlock()
	if failed {
		if isFinalChunk {
			plugin.cleanupStreamAccumulator(requestID)
		}
		return false, nil
	}

	if err := plugin.addStreamChunk(requestID, chunk); err != nil {
		return false, fmt.Errorf("failed to add stream chunk: %w", err)
	}

	if !isFinalChunk {
		return false, nil
	}

	// Gate final processing so it runs exactly once even if multiple chunks
	// race here (shouldn't happen in practice but cheap insurance).
	accumulator.mu.Lock()
	alreadyComplete := accumulator.IsComplete
	if !alreadyComplete {
		accumulator.IsComplete = true
	}
	accumulator.mu.Unlock()

	return !alreadyComplete, nil
}

// parseStreamChunks parses stream_chunks data from the various shapes
// different vector store drivers hand back (Weaviate's JSON-decoded
// []interface{}, typed []string, or Redis's JSON-encoded string) into a
// flat []string of per-chunk JSON payloads.
//
// Non-string elements in the []interface{} case are dropped with a warning
// rather than failing the whole replay — partial cache hits are better than
// no hit at all.
func (plugin *Plugin) parseStreamChunks(streamData interface{}) ([]string, error) {
	if streamData == nil {
		return nil, fmt.Errorf("stream data is nil")
	}

	switch v := streamData.(type) {
	case []string:
		return v, nil
	case []interface{}:
		result := make([]string, 0, len(v))
		for i, item := range v {
			s, ok := item.(string)
			if !ok {
				plugin.logger.Warn("Stream chunk %d is not a string (got %T), skipping", i, item)
				continue
			}
			result = append(result, s)
		}
		return result, nil
	case string:
		// Redis: stream_chunks stored as a JSON-encoded array of strings.
		var stringArray []string
		if err := json.Unmarshal([]byte(v), &stringArray); err != nil {
			return nil, fmt.Errorf("failed to parse JSON string: %w", err)
		}
		return stringArray, nil
	default:
		return nil, fmt.Errorf("unsupported stream data type: %T", streamData)
	}
}

// getInputForCaching extracts request input for hashing/embedding without
// normalization. For Chat/Responses requests, system messages are filtered
// out when ExcludeSystemPrompt is enabled — that path returns a fresh slice;
// otherwise the original slice is returned by reference (no allocation).
// Other request types always return the underlying input directly.
//
// The slice for Chat/Responses is memoized on state so attachment extraction,
// embedding text extraction, and the history-threshold check reuse the same
// slice instead of re-walking on each call. State may be nil (tests /
// pre-state callers), in which case nothing is cached.
func (plugin *Plugin) getInputForCaching(state *cacheState, req *schemas.BifrostRequest) interface{} {
	if state != nil && state.FilteredInput != nil {
		return state.FilteredInput
	}
	excludeSystem := plugin.config.ExcludeSystemPrompt != nil && *plugin.config.ExcludeSystemPrompt
	var out interface{}
	switch req.RequestType {
	case schemas.TextCompletionRequest, schemas.TextCompletionStreamRequest:
		out = req.TextCompletionRequest.Input
	case schemas.ChatCompletionRequest, schemas.ChatCompletionStreamRequest:
		out = filterChatMessages(req.ChatRequest.Input, excludeSystem)
	case schemas.ResponsesRequest, schemas.ResponsesStreamRequest, schemas.WebSocketResponsesRequest:
		out = filterResponsesMessages(req.ResponsesRequest.Input, excludeSystem)
	case schemas.SpeechRequest, schemas.SpeechStreamRequest:
		out = req.SpeechRequest.Input.Input
	case schemas.EmbeddingRequest:
		out = req.EmbeddingRequest.Input
	case schemas.TranscriptionRequest, schemas.TranscriptionStreamRequest:
		out = req.TranscriptionRequest.Input
	case schemas.ImageGenerationRequest, schemas.ImageGenerationStreamRequest:
		out = req.ImageGenerationRequest.Input
	default:
		return nil
	}
	if state != nil {
		state.FilteredInput = out
	}
	return out
}

// filterChatMessages returns msgs unchanged when excludeSystem is false.
// Otherwise, returns a copy with system messages dropped.
func filterChatMessages(msgs []schemas.ChatMessage, excludeSystem bool) []schemas.ChatMessage {
	if !excludeSystem {
		return msgs
	}
	out := make([]schemas.ChatMessage, 0, len(msgs))
	for _, m := range msgs {
		if m.Role == schemas.ChatMessageRoleSystem {
			continue
		}
		out = append(out, m)
	}
	return out
}

// filterResponsesMessages returns msgs unchanged when excludeSystem is false.
// Otherwise, returns a copy with system messages dropped.
func filterResponsesMessages(msgs []schemas.ResponsesMessage, excludeSystem bool) []schemas.ResponsesMessage {
	if !excludeSystem {
		return msgs
	}
	out := make([]schemas.ResponsesMessage, 0, len(msgs))
	for _, m := range msgs {
		if m.Role != nil && *m.Role == schemas.ResponsesInputMessageRoleSystem {
			continue
		}
		out = append(out, m)
	}
	return out
}

// isConversationHistoryThresholdExceeded returns true when the request's
// conversation history is longer than ConversationHistoryThreshold. Long
// histories are unlikely to repeat and unlikely to be semantically similar
// to other requests, so caching them mostly bloats the store; PreLLMHook
// uses this to skip caching such requests entirely.
func (plugin *Plugin) isConversationHistoryThresholdExceeded(state *cacheState, req *schemas.BifrostRequest) bool {
	switch {
	case req.ChatRequest != nil:
		input, ok := plugin.getInputForCaching(state, req).([]schemas.ChatMessage)
		if !ok {
			return false
		}
		return len(input) > plugin.config.ConversationHistoryThreshold
	case req.ResponsesRequest != nil:
		input, ok := plugin.getInputForCaching(state, req).([]schemas.ResponsesMessage)
		if !ok {
			return false
		}
		return len(input) > plugin.config.ConversationHistoryThreshold
	default:
		return false
	}
}
