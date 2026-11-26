package bifrost

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A real provider SSE parser and a real local MCP transport, without external
// services or credentials. The second provider turn must contain the result,
// not a tool error returned to the application.
func TestAdmittedMCPStreamsExecuteInsideGateway(t *testing.T) {
	for _, responses := range []bool{false, true} {
		name := "chat"
		if responses {
			name = "responses"
		}
		t.Run(name, func(t *testing.T) {
			alpha := newAlphaServer(t)
			var callTurn, answerTurn http.HandlerFunc
			if responses {
				callTurn = sseHandler(
					`{"type":"response.created","sequence_number":0,"response":{"id":"r1","object":"response","created_at":1,"status":"in_progress","model":"gpt-4o","output":[]}}`,
					`{"type":"response.output_item.added","sequence_number":1,"output_index":0,"item":{"id":"fc1","type":"function_call","status":"in_progress","call_id":"c1","name":"alpha-echo_tool","arguments":""}}`,
					`{"type":"response.function_call_arguments.delta","sequence_number":2,"output_index":0,"item_id":"fc1","delta":"{\"text\":\"gateway-test-result\"}"}`,
					`{"type":"response.output_item.done","sequence_number":3,"output_index":0,"item":{"id":"fc1","type":"function_call","status":"completed","call_id":"c1","name":"alpha-echo_tool","arguments":"{\"text\":\"gateway-test-result\"}"}}`,
					`{"type":"response.completed","sequence_number":4,"response":{"id":"r1","object":"response","created_at":1,"status":"completed","model":"gpt-4o","output":[{"id":"fc1","type":"function_call","status":"completed","call_id":"c1","name":"alpha-echo_tool","arguments":"{\"text\":\"gateway-test-result\"}"}],"usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}}}`,
				)
				answerTurn = sseHandler(
					`{"type":"response.created","sequence_number":0,"response":{"id":"r2","object":"response","created_at":2,"status":"in_progress","model":"gpt-4o","output":[]}}`,
					`{"type":"response.output_item.added","sequence_number":1,"output_index":0,"item":{"id":"m2","type":"message","status":"in_progress","role":"assistant","content":[]}}`,
					`{"type":"response.output_text.delta","sequence_number":2,"output_index":0,"content_index":0,"item_id":"m2","delta":"Answered."}`,
					`{"type":"response.output_item.done","sequence_number":3,"output_index":0,"item":{"id":"m2","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"Answered.","annotations":[]}]}}`,
					`{"type":"response.completed","sequence_number":4,"response":{"id":"r2","object":"response","created_at":2,"status":"completed","model":"gpt-4o","output":[{"id":"m2","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"Answered.","annotations":[]}]}],"usage":{"input_tokens":20,"output_tokens":3,"total_tokens":23}}}`,
				)
			} else {
				callTurn = sseHandler(
					`{"id":"r1","object":"chat.completion.chunk","created":1,"model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"c1","type":"function","function":{"name":"alpha-echo_tool","arguments":""}}]},"finish_reason":null}]}`,
					`{"id":"r1","object":"chat.completion.chunk","created":1,"model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"text\":\"gateway-test-result\"}"}}]},"finish_reason":null}]}`,
					`{"id":"r1","object":"chat.completion.chunk","created":1,"model":"gpt-4o","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}`,
				)
				answerTurn = sseHandler(
					`{"id":"r2","object":"chat.completion.chunk","created":2,"model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant","content":"Answered."},"finish_reason":null}]}`,
					`{"id":"r2","object":"chat.completion.chunk","created":2,"model":"gpt-4o","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":20,"completion_tokens":3,"total_tokens":23}}`,
				)
			}
			recorder := &recordingServer{}
			recorder.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if recorder.record(string(body)) == 1 {
					callTurn(w, r)
					return
				}
				answerTurn(w, r)
			}))
			defer recorder.Close()
			account := NewMockAccount()
			configureUpstream(account, schemas.OpenAI, "openai-key", recorder.URL, 0)
			mcp := alphaClient(alpha.url)
			mcp.ToolsToAutoExecute = schemas.WhiteList{"echo_tool"}
			client := newSDKClient(t, schemas.BifrostConfig{Account: account, MCPConfig: &schemas.MCPConfig{ClientConfigs: []*schemas.MCPClientConfig{mcp}}})
			client.ConnectConfiguredMCPClients(context.Background(), nil)

			ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(10*time.Second))
			defer ctx.Cancel()
			ctx.SetValue(schemas.MCPContextKeyIncludeTools, []string{"alpha-echo_tool"})
			var stream chan *schemas.BifrostStreamChunk
			var err *schemas.BifrostError
			if responses {
				stream, err = client.ResponsesStreamRequest(ctx, &schemas.BifrostResponsesRequest{
					Provider: schemas.OpenAI, Model: "gpt-4o",
					Input: []schemas.ResponsesMessage{{Role: schemas.Ptr(schemas.ResponsesInputMessageRoleUser), Content: &schemas.ResponsesMessageContent{ContentStr: schemas.Ptr("Search")}}},
				})
			} else {
				stream, err = client.ChatCompletionStreamRequest(ctx, &schemas.BifrostChatRequest{
					Provider: schemas.OpenAI, Model: "gpt-4o",
					Input: []schemas.ChatMessage{{Role: schemas.ChatMessageRoleUser, Content: &schemas.ChatMessageContent{ContentStr: schemas.Ptr("Search")}}},
				})
			}
			require.Nil(t, err)
			var text strings.Builder
			for chunk := range stream {
				require.Nil(t, chunk.BifrostError)
				if response := chunk.BifrostChatResponse; response != nil {
					for _, choice := range response.Choices {
						if choice.ChatStreamResponseChoice != nil && choice.ChatStreamResponseChoice.Delta != nil {
							delta := choice.ChatStreamResponseChoice.Delta
							assert.Empty(t, delta.ToolCalls)
							if delta.Content != nil {
								text.WriteString(*delta.Content)
							}
						}
					}
				}
				if ev := chunk.BifrostResponsesStreamResponse; ev != nil {
					if ev.Item != nil && ev.Item.ResponsesToolMessage != nil && ev.Item.Name != nil {
						assert.NotEqual(t, "alpha-echo_tool", *ev.Item.Name)
					}
					if ev.Type == schemas.ResponsesStreamResponseTypeOutputTextDelta && ev.Delta != nil {
						text.WriteString(*ev.Delta)
					}
				}
			}
			assert.Equal(t, "Answered.", text.String())
			assert.Equal(t, 1, alpha.callCount("echo_tool"))
			assert.Equal(t, 0, alpha.callCount("add_tool"))
			bodies := recorder.snapshot()
			require.Len(t, bodies, 2)
			assert.Contains(t, bodies[0], "alpha-echo_tool")
			assert.NotContains(t, bodies[0], "alpha-add_tool")
			assert.Contains(t, bodies[1], "gateway-test-result")
			assert.NotContains(t, bodies[1], "requires explicit approval")
		})
	}
}

func TestAdmittedMCPStreamKeepsCallerTools(t *testing.T) {
	gateway := schemas.ChatTool{Type: schemas.ChatToolTypeFunction, Function: &schemas.ChatToolFunction{Name: "alpha-echo_tool"}, GatewayAutoExecute: true}
	local := schemas.ChatTool{Type: schemas.ChatToolTypeFunction, Function: &schemas.ChatToolFunction{Name: "local_read"}}
	manual := schemas.ChatTool{Type: schemas.ChatToolTypeFunction, Function: &schemas.ChatToolFunction{Name: "alpha-add_tool"}}
	chat := &schemas.BifrostChatRequest{Params: &schemas.ChatParameters{Tools: []schemas.ChatTool{local, gateway, manual}}}
	req := &schemas.BifrostRequest{RequestType: schemas.ChatCompletionStreamRequest, ChatRequest: chat}
	set := withAdmittedStreamingMCP(nil, req, 4)
	require.NotNil(t, set)
	assert.True(t, set.isInjected("alpha-echo_tool"))
	assert.False(t, set.isInjected("local_read"))
	assert.False(t, set.isInjected("alpha-add_tool"))
	assert.False(t, set.targets["alpha-echo_tool"].providerConfigured)
	assert.Same(t, chat, applyInjectedToolsChat(chat, set), "gateway-owned declarations need no rewrite")
	stream := newInjectedChatStream(set)
	emits := feed(stream, "local", chatDelta{call: callStart(0, "local-call", "local_read")}, chatDelta{call: callArgs(0, "{}")}, chatDelta{finish: "tool_calls"})
	require.False(t, stream.continues())
	require.NotEmpty(t, emits)
	require.Len(t, emits[len(emits)-1].resp.Choices[0].Delta.ToolCalls, 1)
	assert.Equal(t, "local_read", *emits[len(emits)-1].resp.Choices[0].Delta.ToolCalls[0].Function.Name)
}
