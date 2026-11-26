package bifrost

import (
	"context"
	"fmt"
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

type admittedFixtureCall struct {
	id, name, args string
}

// Send real OpenAI-shaped SSE through the provider parser; the fixture does not
// call the stream state machine directly or use an external provider.
func admittedMixedTurn(responses bool, turn int, calls []admittedFixtureCall, interrupted string) http.HandlerFunc {
	id := fmt.Sprintf("r%d", turn)
	var events []string
	if responses {
		events = append(events, fmt.Sprintf(`{"type":"response.created","sequence_number":0,"response":{"id":%q,"object":"response","created_at":1,"status":"in_progress","model":"gpt-4o","output":[]}}`, id))
		status, terminalType, details := "completed", "response.completed", ""
		if interrupted != "" {
			status, terminalType = "incomplete", "response.incomplete"
			details = fmt.Sprintf(`,"incomplete_details":{"reason":%q}`, interrupted)
		}
		var output []string
		for i, c := range calls {
			itemID := "fc_" + c.id
			item := func(status, args string) string {
				return fmt.Sprintf(`{"id":%q,"type":"function_call","status":%q,"call_id":%q,"name":%q,"arguments":%q}`, itemID, status, c.id, c.name, args)
			}
			events = append(events, fmt.Sprintf(`{"type":"response.output_item.added","sequence_number":%d,"output_index":%d,"item":%s}`, len(events), i, item("in_progress", "")))
			for _, fragment := range []string{c.args[:1], c.args[1:]} {
				events = append(events, fmt.Sprintf(`{"type":"response.function_call_arguments.delta","sequence_number":%d,"output_index":%d,"item_id":%q,"delta":%q}`, len(events), i, itemID, fragment))
			}
			events = append(events, fmt.Sprintf(`{"type":"response.output_item.done","sequence_number":%d,"output_index":%d,"item":%s}`, len(events), i, item(status, c.args)))
			output = append(output, item(status, c.args))
		}
		events = append(events, fmt.Sprintf(`{"type":%q,"sequence_number":%d,"response":{"id":%q,"object":"response","created_at":1,"status":%q,"model":"gpt-4o","output":[%s],"usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}%s}}`, terminalType, len(events), id, status, strings.Join(output, ","), details))
	} else {
		finish := "tool_calls"
		if interrupted != "" {
			finish = interrupted
		}
		for i, c := range calls {
			events = append(events, fmt.Sprintf(`{"id":%q,"object":"chat.completion.chunk","created":1,"model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":%d,"id":%q,"type":"function","function":{"name":%q,"arguments":""}}]},"finish_reason":null}]}`, id, i, c.id, c.name))
			for _, fragment := range []string{c.args[:1], c.args[1:]} {
				events = append(events, fmt.Sprintf(`{"id":%q,"object":"chat.completion.chunk","created":1,"model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":%d,"function":{"arguments":%q}}]},"finish_reason":null}]}`, id, i, fragment))
			}
		}
		events = append(events, fmt.Sprintf(`{"id":%q,"object":"chat.completion.chunk","created":1,"model":"gpt-4o","choices":[{"index":0,"delta":{},"finish_reason":%q}],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}`, id, finish))
	}
	return sseHandler(events...)
}

func TestAdmittedMCPMixedStream(t *testing.T) {
	auto := admittedFixtureCall{"auto-1", "alpha-echo_tool", `{"text":"gateway-first"}`}
	other := admittedFixtureCall{"auto-2", "alpha-echo_tool", `{"text":"gateway-second"}`}
	local := admittedFixtureCall{"caller-1", "local_read", `{"file":"sample"}`}
	manual := admittedFixtureCall{"manual-1", "alpha-add_tool", `{"a":2,"b":3}`}
	for _, responses := range []bool{false, true} {
		api := "chat"
		if responses {
			api = "responses"
		}
		for _, scenario := range []struct {
			name      string
			turns     [][]admittedFixtureCall
			autoCount int
		}{
			{"caller-first", [][]admittedFixtureCall{{local, auto, manual}}, 1},
			{"auto-first", [][]admittedFixtureCall{{auto, local, other, manual}}, 2},
			{"caller-after-auto-turn", [][]admittedFixtureCall{{auto}, {local, manual}}, 1},
		} {
			t.Run(api+"/"+scenario.name, func(t *testing.T) {
				alpha := newAlphaServer(t)
				recorder := &recordingServer{}
				recorder.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, _ := io.ReadAll(r.Body)
					n := recorder.record(string(body))
					if n > len(scenario.turns) {
						http.Error(w, "unexpected inference after caller action", http.StatusBadRequest)
						return
					}
					admittedMixedTurn(responses, n, scenario.turns[n-1], "")(w, r)
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
				ctx.SetValue(schemas.MCPContextKeyIncludeTools, []string{"alpha-echo_tool", "alpha-add_tool"})
				var stream chan *schemas.BifrostStreamChunk
				var err *schemas.BifrostError
				if responses {
					stream, err = client.ResponsesStreamRequest(ctx, &schemas.BifrostResponsesRequest{
						Provider: schemas.OpenAI, Model: "gpt-4o",
						Input:  []schemas.ResponsesMessage{{Role: schemas.Ptr(schemas.ResponsesInputMessageRoleUser), Content: &schemas.ResponsesMessageContent{ContentStr: schemas.Ptr("Read and report")}}},
						Params: &schemas.ResponsesParameters{Tools: []schemas.ResponsesTool{{Type: schemas.ResponsesToolTypeFunction, Name: schemas.Ptr("local_read")}}},
					})
				} else {
					stream, err = client.ChatCompletionStreamRequest(ctx, &schemas.BifrostChatRequest{
						Provider: schemas.OpenAI, Model: "gpt-4o",
						Input:  []schemas.ChatMessage{{Role: schemas.ChatMessageRoleUser, Content: &schemas.ChatMessageContent{ContentStr: schemas.Ptr("Read and report")}}},
						Params: &schemas.ChatParameters{Tools: []schemas.ChatTool{{Type: schemas.ChatToolTypeFunction, Function: &schemas.ChatToolFunction{Name: "local_read"}}}},
					})
				}
				require.Nil(t, err)
				var text strings.Builder
				var observed []admittedFixtureCall
				var terminalCount, nextSequence int
				for chunk := range stream {
					require.Nil(t, chunk.BifrostError)
					if resp := chunk.BifrostChatResponse; resp != nil {
						for _, c := range resp.Choices {
							if c.Delta != nil {
								if c.Delta.Content != nil {
									text.WriteString(*c.Delta.Content)
								}
								for _, call := range c.Delta.ToolCalls {
									require.NotNil(t, call.Function.Name)
									require.NotNil(t, call.ID)
									assert.Equal(t, uint16(len(observed)), call.Index)
									observed = append(observed, admittedFixtureCall{*call.ID, *call.Function.Name, call.Function.Arguments})
								}
							}
							if c.FinishReason != nil {
								terminalCount++
								assert.Equal(t, "tool_calls", *c.FinishReason)
								require.NotNil(t, resp.Usage)
								assert.Equal(t, 10*len(scenario.turns), resp.Usage.PromptTokens)
							}
						}
					}
					if ev := chunk.BifrostResponsesStreamResponse; ev != nil {
						assert.Equal(t, nextSequence, ev.SequenceNumber)
						nextSequence++
						if ev.Item != nil && ev.Item.ResponsesToolMessage != nil && ev.Item.Name != nil {
							assert.NotEqual(t, auto.name, *ev.Item.Name)
						}
						if ev.Type == schemas.ResponsesStreamResponseTypeOutputTextDelta && ev.Delta != nil {
							text.WriteString(*ev.Delta)
						}
						if ev.Type == schemas.ResponsesStreamResponseTypeCompleted {
							terminalCount++
							require.NotNil(t, ev.Response)
							require.NotNil(t, ev.Response.Usage)
							assert.Equal(t, 10*len(scenario.turns), ev.Response.Usage.InputTokens)
							for _, item := range ev.Response.Output {
								if isFunctionCall(item) {
									require.NotNil(t, item.CallID)
									require.NotNil(t, item.Name)
									require.NotNil(t, item.Arguments)
									observed = append(observed, admittedFixtureCall{*item.CallID, *item.Name, *item.Arguments})
								}
							}
							require.Len(t, ev.Response.Output, 3, "both caller calls and the assistant-visible result")
						}
					}
				}
				assert.Equal(t, []admittedFixtureCall{local, manual}, observed)
				assert.Equal(t, 1, terminalCount)
				assert.Contains(t, text.String(), "gateway-first")
				if scenario.autoCount == 2 {
					assert.Contains(t, text.String(), "gateway-second")
				}
				assert.Equal(t, scenario.autoCount, alpha.callCount("echo_tool"))
				assert.Equal(t, 0, alpha.callCount("add_tool"), "approval-required tools are never auto-executed")
				bodies := recorder.snapshot()
				require.Len(t, bodies, len(scenario.turns), "do not re-ask with unresolved caller calls")
				if len(bodies) > 1 {
					assert.Contains(t, bodies[1], "gateway-first")
				}
			})
		}
	}
}

func TestAdmittedMCPStreamPreservesCustomCall(t *testing.T) {
	gateway := schemas.ChatTool{Type: schemas.ChatToolTypeFunction, Function: &schemas.ChatToolFunction{Name: "alpha-echo_tool"}, GatewayAutoExecute: true}
	set := withAdmittedStreamingMCP(nil, &schemas.BifrostRequest{
		RequestType: schemas.ChatCompletionStreamRequest,
		ChatRequest: &schemas.BifrostChatRequest{Params: &schemas.ChatParameters{Tools: []schemas.ChatTool{gateway}}},
	}, 4)
	session := newInjectedChatStream(set)
	start := &schemas.ChatAssistantMessageToolCall{
		Index: 0, ID: schemas.Ptr("custom-call"), Type: schemas.Ptr("custom"),
		Custom: &schemas.ChatAssistantMessageToolCallCustom{Name: schemas.Ptr("alpha-echo_tool"), Input: "first "},
	}
	fragment := &schemas.ChatAssistantMessageToolCall{Index: 0, Custom: &schemas.ChatAssistantMessageToolCallCustom{Input: "second"}}
	emits := feed(session, "custom", chatDelta{call: start}, chatDelta{call: fragment}, chatDelta{finish: "tool_calls"})
	require.False(t, session.continues(), "custom calls must not execute a same-named gateway function")
	require.NotEmpty(t, emits)
	terminal := emits[len(emits)-1]
	require.True(t, terminal.terminal)
	require.Len(t, terminal.resp.Choices[0].Delta.ToolCalls, 1)
	got := terminal.resp.Choices[0].Delta.ToolCalls[0]
	require.NotNil(t, got.Custom)
	assert.Equal(t, "custom-call", *got.ID)
	assert.Equal(t, "alpha-echo_tool", *got.Custom.Name)
	assert.Equal(t, "first second", got.Custom.Input)
	wire, err := schemas.Marshal(terminal.resp)
	require.NoError(t, err)
	assert.Contains(t, string(wire), `"custom":`)
	assert.NotContains(t, string(wire), `"function":`)
}

// An upstream interruption is terminal even when a tool's JSON happens to be
// valid. Partial caller calls stay inspectable; no new tool or model turn runs.
func TestAdmittedMCPInterruptedStream(t *testing.T) {
	auto := admittedFixtureCall{"auto-1", "alpha-echo_tool", `{"text":"gateway-first"}`}
	partial := admittedFixtureCall{"caller-1", "local_read", `{"file":`}
	for _, responses := range []bool{false, true} {
		api, length := "chat", "length"
		if responses {
			api, length = "responses", "max_output_tokens"
		}
		for _, reason := range []string{length, "content_filter"} {
			for _, scenario := range []struct {
				name      string
				calls     []admittedFixtureCall
				priorAuto bool
			}{
				{"caller-only", []admittedFixtureCall{partial}, false},
				{"mixed", []admittedFixtureCall{auto, partial}, false},
				{"auto-only", []admittedFixtureCall{auto}, false},
				{"after-auto-turn", []admittedFixtureCall{auto, partial}, true},
			} {
				t.Run(api+"/"+reason+"/"+scenario.name, func(t *testing.T) {
					alpha := newAlphaServer(t)
					recorder := &recordingServer{}
					turnCount := 1
					if scenario.priorAuto {
						turnCount++
					}
					recorder.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						body, _ := io.ReadAll(r.Body)
						n := recorder.record(string(body))
						if n > turnCount {
							http.Error(w, "unexpected inference after interruption", http.StatusBadRequest)
							return
						}
						if scenario.priorAuto && n == 1 {
							admittedMixedTurn(responses, n, []admittedFixtureCall{auto}, "")(w, r)
							return
						}
						admittedMixedTurn(responses, n, scenario.calls, reason)(w, r)
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
							Input:  []schemas.ResponsesMessage{{Role: schemas.Ptr(schemas.ResponsesInputMessageRoleUser), Content: &schemas.ResponsesMessageContent{ContentStr: schemas.Ptr("Read and report")}}},
							Params: &schemas.ResponsesParameters{Tools: []schemas.ResponsesTool{{Type: schemas.ResponsesToolTypeFunction, Name: schemas.Ptr("local_read")}}},
						})
					} else {
						stream, err = client.ChatCompletionStreamRequest(ctx, &schemas.BifrostChatRequest{
							Provider: schemas.OpenAI, Model: "gpt-4o",
							Input:  []schemas.ChatMessage{{Role: schemas.ChatMessageRoleUser, Content: &schemas.ChatMessageContent{ContentStr: schemas.Ptr("Read and report")}}},
							Params: &schemas.ChatParameters{Tools: []schemas.ChatTool{{Type: schemas.ChatToolTypeFunction, Function: &schemas.ChatToolFunction{Name: "local_read"}}}},
						})
					}
					require.Nil(t, err)
					var observed []admittedFixtureCall
					var text strings.Builder
					terminalCount := 0
					for chunk := range stream {
						assert.Nil(t, chunk.BifrostError)
						if resp := chunk.BifrostChatResponse; resp != nil {
							for _, choice := range resp.Choices {
								if choice.Delta != nil {
									if choice.Delta.Content != nil {
										text.WriteString(*choice.Delta.Content)
									}
									for _, call := range choice.Delta.ToolCalls {
										require.NotNil(t, call.ID)
										require.NotNil(t, call.Function.Name)
										observed = append(observed, admittedFixtureCall{*call.ID, *call.Function.Name, call.Function.Arguments})
									}
								}
								if choice.FinishReason != nil {
									terminalCount++
									assert.Equal(t, reason, *choice.FinishReason)
									require.NotNil(t, resp.Usage)
									assert.Equal(t, 10*turnCount, resp.Usage.PromptTokens)
								}
							}
						}
						if ev := chunk.BifrostResponsesStreamResponse; ev != nil {
							if ev.Type == schemas.ResponsesStreamResponseTypeOutputTextDelta && ev.Delta != nil {
								text.WriteString(*ev.Delta)
							}
							if ev.Type == schemas.ResponsesStreamResponseTypeIncomplete || ev.Type == schemas.ResponsesStreamResponseTypeCompleted {
								terminalCount++
								assert.Equal(t, schemas.ResponsesStreamResponseTypeIncomplete, ev.Type)
								require.NotNil(t, ev.Response)
								require.NotNil(t, ev.Response.IncompleteDetails)
								assert.Equal(t, reason, string(ev.Response.IncompleteDetails.Reason))
								require.NotNil(t, ev.Response.Usage)
								assert.Equal(t, 10*turnCount, ev.Response.Usage.InputTokens)
								for _, item := range ev.Response.Output {
									if isFunctionCall(item) {
										require.NotNil(t, item.CallID)
										require.NotNil(t, item.Name)
										require.NotNil(t, item.Arguments)
										observed = append(observed, admittedFixtureCall{*item.CallID, *item.Name, *item.Arguments})
									}
								}
							}
						}
					}
					assert.Equal(t, 1, terminalCount)
					if scenario.name == "auto-only" {
						assert.Empty(t, observed)
					} else {
						assert.Equal(t, []admittedFixtureCall{partial}, observed)
					}
					assert.Len(t, recorder.snapshot(), turnCount)
					assert.Equal(t, turnCount-1, alpha.callCount("echo_tool"), "interrupted turns never execute tools")
					if scenario.priorAuto {
						assert.Contains(t, text.String(), "gateway-first", "retain results from earlier successful turns")
					}
				})
			}
		}
	}
}
