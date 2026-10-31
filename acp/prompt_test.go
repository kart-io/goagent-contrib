package acp

import (
	"context"
	"testing"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

func TestPromptStreamsText(t *testing.T) {
	model := newModel(say("你好", "，世界"))
	h := start(t, NewServer(plainFactory(model, nil)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, t.TempDir())

	resp, err := h.prompt(t, id, "打个招呼")
	require.NoError(t, err)
	require.Equal(t, acpsdk.StopReasonEndTurn, resp.StopReason)
	require.Equal(t, "你好，世界", h.editor.agentText())
	last := model.request(0).Messages
	require.Contains(t, last[len(last)-1].Content, "打个招呼")
}

func TestPromptText(t *testing.T) {
	got, err := promptText([]acpsdk.ContentBlock{
		acpsdk.TextBlock("看看这个文件"),
		acpsdk.ResourceLinkBlock("main.go", "file:///w/main.go"),
		acpsdk.ResourceBlock(acpsdk.EmbeddedResourceResource{TextResourceContents: &acpsdk.TextResourceContents{Uri: "file:///w/a.txt", Text: "内容"}}),
	})
	require.NoError(t, err)
	require.Equal(t, "看看这个文件\n\nfile:///w/main.go\n\n<file:///w/a.txt>\n内容", got)

	_, err = promptText([]acpsdk.ContentBlock{acpsdk.ImageBlock("AAAA", "image/png")})
	require.ErrorContains(t, err, "不支持")
}

func TestPromptErrors(t *testing.T) {
	h := start(t, NewServer(plainFactory(newModel(), nil)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, t.TempDir())

	_, err := h.prompt(t, "sess_missing", "hi")
	requireCode(t, err, -32602)

	_, err = h.client.Prompt(context.Background(), acpsdk.PromptRequest{SessionId: id, Prompt: []acpsdk.ContentBlock{acpsdk.ImageBlock("AAAA", "image/png")}})
	requireCode(t, err, -32602)

	// 模型没有可用回合，执行失败返回内部错误。
	_, err = h.prompt(t, id, "hi")
	requireCode(t, err, -32603)
	require.ErrorContains(t, err, "多余的模型请求")
}

func TestPromptCancel(t *testing.T) {
	model := newModel(waitTurn())
	h := start(t, NewServer(plainFactory(model, nil)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, t.TempDir())

	done := make(chan acpsdk.PromptResponse, 1)
	go func() {
		resp, err := h.prompt(t, id, "很慢的问题")
		require.NoError(t, err)
		done <- resp
	}()
	<-model.started
	require.NoError(t, h.client.Cancel(context.Background(), acpsdk.CancelNotification{SessionId: id}))
	require.Equal(t, acpsdk.StopReasonCancelled, (<-done).StopReason)
}

// 新 prompt 到达时 SDK 取消旧 prompt；新的执行在旧的返回后才开始。
func TestPromptsRunOneAtATime(t *testing.T) {
	model := newModel(waitTurn(), say("第二次"))
	h := start(t, NewServer(plainFactory(model, nil)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, t.TempDir())

	first := make(chan acpsdk.PromptResponse, 1)
	go func() {
		resp, err := h.prompt(t, id, "第一次")
		require.NoError(t, err)
		first <- resp
	}()
	<-model.started
	resp, err := h.prompt(t, id, "第二次")
	require.NoError(t, err)
	require.Equal(t, acpsdk.StopReasonEndTurn, resp.StopReason)
	require.Equal(t, acpsdk.StopReasonCancelled, (<-first).StopReason)
	require.Equal(t, 2, model.count())
}
