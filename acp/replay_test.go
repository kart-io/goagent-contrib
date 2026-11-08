package acp

import (
	"context"
	"errors"
	"github.com/kart-io/goagent/constants"
	"testing"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/kart-io/goagent/core/session"
	"github.com/kart-io/goagent/llm"
	"github.com/stretchr/testify/require"
)

func TestLoadSessionReplaysAndContinues(t *testing.T) {
	store := session.NewMemoryStore()
	dir := t.TempDir()
	tool := only(echoTool("lookup", nil, ""))

	first := newModel(calls("lookup"), say("订单 A1 已发货"))
	h := start(t, NewServer(toolFactory(first, nil, store, tool), WithSessionStore(store)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, dir)
	_, err := h.prompt(t, id, "查订单 A1")
	require.NoError(t, err)

	// 新连接（相当于编辑器重启）加载同一会话。
	second := newModel(say("还是 A1"))
	ed := newEditor()
	h2 := start(t, NewServer(toolFactory(second, nil, store, tool), WithSessionStore(store)), ed)
	h2.initialize(t, acpsdk.ClientCapabilities{})
	_, err = h2.client.LoadSession(context.Background(), acpsdk.LoadSessionRequest{SessionId: id, Cwd: dir, McpServers: []acpsdk.McpServer{}})
	require.NoError(t, err)

	var kinds []string
	for _, n := range ed.notifications() {
		switch u := n.Update; {
		case u.UserMessageChunk != nil:
			kinds = append(kinds, "user:"+u.UserMessageChunk.Content.Text.Text)
		case u.AgentMessageChunk != nil:
			kinds = append(kinds, "agent:"+u.AgentMessageChunk.Content.Text.Text)
		case u.ToolCall != nil:
			kinds = append(kinds, "tool_call:"+string(u.ToolCall.ToolCallId))
		case u.ToolCallUpdate != nil:
			kinds = append(kinds, "tool_update:"+string(*u.ToolCallUpdate.Status))
		}
	}
	require.Equal(t, []string{"user:查订单 A1", "tool_call:c1", "tool_update:completed", "agent:订单 A1 已发货"}, kinds)

	_, err = h2.prompt(t, id, "再确认一下")
	require.NoError(t, err)
	var history []string
	for _, m := range second.request(0).Messages {
		history = append(history, m.Role)
	}
	require.Contains(t, history, "tool", "继续对话时模型看到之前的工具结果")
}

func TestLoadAndListErrors(t *testing.T) {
	store := session.NewMemoryStore()
	h := start(t, NewServer(toolFactory(newModel(say("好")), nil, store, only(echoTool("lookup", nil, ""))), WithSessionStore(store)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	dir := t.TempDir()

	_, err := h.client.LoadSession(context.Background(), acpsdk.LoadSessionRequest{SessionId: "sess_missing", Cwd: dir, McpServers: []acpsdk.McpServer{}})
	requireCode(t, err, -32602)

	id := h.newSession(t, dir)
	_, err = h.prompt(t, id, "你好")
	require.NoError(t, err)
	list, err := h.client.ListSessions(context.Background(), acpsdk.ListSessionsRequest{})
	require.NoError(t, err)
	require.Len(t, list.Sessions, 1)
	require.Equal(t, id, list.Sessions[0].SessionId)
	require.Equal(t, dir, list.Sessions[0].Cwd)

	other := "/elsewhere"
	list, err = h.client.ListSessions(context.Background(), acpsdk.ListSessionsRequest{Cwd: &other})
	require.NoError(t, err)
	require.Empty(t, list.Sessions, "已知工作区不同的会话被过滤")
}

func replayKinds(ed *fakeEditor) []string {
	var kinds []string
	for _, n := range ed.notifications() {
		switch u := n.Update; {
		case u.UserMessageChunk != nil:
			kinds = append(kinds, "user:"+u.UserMessageChunk.Content.Text.Text)
		case u.AgentMessageChunk != nil:
			kinds = append(kinds, "agent:"+u.AgentMessageChunk.Content.Text.Text)
		case u.ToolCall != nil:
			kinds = append(kinds, "tool_call:"+string(u.ToolCall.ToolCallId))
		case u.ToolCallUpdate != nil:
			kinds = append(kinds, "tool_update:"+string(*u.ToolCallUpdate.Status))
		}
	}
	return kinds
}

func TestLoadSessionReloadsOpenedID(t *testing.T) {
	store := session.NewMemoryStore()
	dir := t.TempDir()
	h := start(t, NewServer(toolFactory(newModel(say("好")), nil, store, only(echoTool("lookup", nil, ""))), WithSessionStore(store)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, dir)
	_, err := h.prompt(t, id, "你好")
	require.NoError(t, err)
	_, err = h.client.LoadSession(context.Background(), acpsdk.LoadSessionRequest{SessionId: id, Cwd: dir, McpServers: []acpsdk.McpServer{}})
	require.NoError(t, err)
}

func TestLoadSessionFactoryFailureSendsNothing(t *testing.T) {
	store := session.NewMemoryStore()
	dir := t.TempDir()
	h := start(t, NewServer(toolFactory(newModel(say("好")), nil, store, only(echoTool("lookup", nil, ""))), WithSessionStore(store)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, dir)
	_, err := h.prompt(t, id, "你好")
	require.NoError(t, err)

	ed := newEditor()
	failing := func(context.Context, *Session) (Runner, error) { return nil, errors.New("构建失败") }
	h2 := start(t, NewServer(failing, WithSessionStore(store)), ed)
	h2.initialize(t, acpsdk.ClientCapabilities{})
	_, err = h2.client.LoadSession(context.Background(), acpsdk.LoadSessionRequest{SessionId: id, Cwd: dir, McpServers: []acpsdk.McpServer{}})
	requireCode(t, err, -32603)
	require.Empty(t, ed.notifications())
}

func TestLoadSessionReplaysFailedTool(t *testing.T) {
	store := session.NewMemoryStore()
	dir := t.TempDir()
	tool := only(echoTool("lookup", nil, ""))
	rejecting := newEditor()
	rejecting.answer = func(context.Context, acpsdk.RequestPermissionRequest) acpsdk.RequestPermissionResponse {
		return acpsdk.RequestPermissionResponse{Outcome: acpsdk.NewRequestPermissionOutcomeSelected("reject_once")}
	}
	h := start(t, NewServer(toolFactory(newModel(calls("lookup"), say("被拒绝")), askAll, store, tool), WithSessionStore(store)), rejecting)
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, dir)
	_, err := h.prompt(t, id, "查")
	require.NoError(t, err)

	ed := newEditor()
	h2 := start(t, NewServer(toolFactory(newModel(say("x")), nil, store, tool), WithSessionStore(store)), ed)
	h2.initialize(t, acpsdk.ClientCapabilities{})
	_, err = h2.client.LoadSession(context.Background(), acpsdk.LoadSessionRequest{SessionId: id, Cwd: dir, McpServers: []acpsdk.McpServer{}})
	require.NoError(t, err)
	require.Contains(t, replayKinds(ed), "tool_update:failed")
}

func TestLoadSessionSkipsRewound(t *testing.T) {
	store := session.NewMemoryStore()
	dir := t.TempDir()
	tool := only(echoTool("lookup", nil, ""))
	h := start(t, NewServer(toolFactory(newModel(say("第一轮"), say("第二轮")), nil, store, tool), WithSessionStore(store)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, dir)
	_, err := h.prompt(t, id, "问一")
	require.NoError(t, err)
	events, err := store.Read(context.Background(), string(id), 0)
	require.NoError(t, err)
	mid := events[len(events)-1].Seq
	_, err = h.prompt(t, id, "问二")
	require.NoError(t, err)
	events, err = store.Read(context.Background(), string(id), 0)
	require.NoError(t, err)
	_, err = session.Rewind(context.Background(), store, string(id), mid, events[len(events)-1].Seq)
	require.NoError(t, err)

	ed := newEditor()
	h2 := start(t, NewServer(toolFactory(newModel(say("x")), nil, store, tool), WithSessionStore(store)), ed)
	h2.initialize(t, acpsdk.ClientCapabilities{})
	_, err = h2.client.LoadSession(context.Background(), acpsdk.LoadSessionRequest{SessionId: id, Cwd: dir, McpServers: []acpsdk.McpServer{}})
	require.NoError(t, err)
	require.Equal(t, []string{"user:问一", "agent:第一轮"}, replayKinds(ed))
}

// 未结束的轮次（如进程在本轮中途退出）在下一轮开始时被标为中止，回放不包含其中的消息。
func TestLoadSessionSkipsAbortedTurn(t *testing.T) {
	store := session.NewMemoryStore()
	dir := t.TempDir()
	tool := only(echoTool("lookup", nil, ""))
	h := start(t, NewServer(toolFactory(newModel(say("第二轮")), nil, store, tool), WithSessionStore(store)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, dir)

	begin, err := session.NewEvent(constants.SessionEventTurnStart, session.TurnStart{Input: "问一"})
	require.NoError(t, err)
	user, err := session.NewEvent(constants.SessionEventUserMessage, session.MessageData{Message: llm.Message{Role: "user", Content: "问一"}})
	require.NoError(t, err)
	existing, err := store.Read(context.Background(), string(id), 0)
	require.NoError(t, err)
	_, err = store.Append(context.Background(), string(id), session.LastSeq(existing), begin, user)
	require.NoError(t, err)
	_, err = h.prompt(t, id, "问二")
	require.NoError(t, err)

	ed := newEditor()
	h2 := start(t, NewServer(toolFactory(newModel(say("x")), nil, store, tool), WithSessionStore(store)), ed)
	h2.initialize(t, acpsdk.ClientCapabilities{})
	_, err = h2.client.LoadSession(context.Background(), acpsdk.LoadSessionRequest{SessionId: id, Cwd: dir, McpServers: []acpsdk.McpServer{}})
	require.NoError(t, err)
	require.Equal(t, []string{"user:问二", "agent:第二轮"}, replayKinds(ed))
}
