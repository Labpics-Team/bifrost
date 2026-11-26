package schemas

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGatewayAutoExecutionOwnershipIsNotWireData(t *testing.T) {
	chat := ChatTool{Type: ChatToolTypeFunction, Function: &ChatToolFunction{Name: "alpha-echo"}, GatewayAutoExecute: true}
	responses := chat.ToResponsesTool()
	assert.True(t, responses.GatewayAutoExecute)
	assert.True(t, responses.ToChatTool().GatewayAutoExecute)
	assert.True(t, DeepCopyChatTool(chat).GatewayAutoExecute)
	for _, value := range []any{&chat, responses} {
		wire, err := json.Marshal(value)
		require.NoError(t, err)
		assert.NotContains(t, string(wire), "GatewayAutoExecute")
		require.NoError(t, json.Unmarshal(wire, value))
	}
	assert.False(t, chat.GatewayAutoExecute)
	assert.False(t, responses.GatewayAutoExecute)
}
