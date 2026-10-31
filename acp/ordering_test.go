package acp

import (
	"testing"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

// 工具调用前的最后一段文本必须先于 tool_call 到达编辑器：钩子的通知交给 prompt 的事件循环按事件顺序发送。
func TestTextPrecedesToolCall(t *testing.T) {
	first := calls("lookup")
	first.parts = []string{"先查", "一下"}
	first.tail = "订单"
	model := newModel(first, say("查完了"))
	h := start(t, NewServer(toolFactory(model, nil, nil, only(echoTool("lookup", nil, "")))), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	_, err := h.prompt(t, h.newSession(t, t.TempDir()), "查订单")
	require.NoError(t, err)

	var before, after string
	seen := false
	for _, n := range h.editor.notifications() {
		switch u := n.Update; {
		case u.ToolCall != nil:
			seen = true
		case u.AgentMessageChunk != nil && u.AgentMessageChunk.Content.Text != nil:
			if seen {
				after += u.AgentMessageChunk.Content.Text.Text
			} else {
				before += u.AgentMessageChunk.Content.Text.Text
			}
		}
	}
	require.True(t, seen)
	require.Equal(t, "先查一下订单", before)
	require.Equal(t, "查完了", after)
}
