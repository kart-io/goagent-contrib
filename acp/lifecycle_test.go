package acp

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/kart-io/goagent/builder"
	"github.com/kart-io/goagent/constants"
	"github.com/kart-io/goagent/core/session"
)

// recv 在有限时间内从 ch 取一个值，超时则测试失败。
func recv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatalf("等待%s超时", what)
		var zero T
		return zero
	}
}

func promptAsync(t *testing.T, h *harness, id acpsdk.SessionId, text string) <-chan promptResult {
	done := make(chan promptResult, 1)
	go func() {
		resp, err := h.prompt(t, id, text)
		done <- promptResult{resp, err}
	}()
	return done
}

// 重叠的 prompt 返回后，session/cancel 仍能取消新的 prompt（SDK 会删掉新 prompt 的取消函数）。
func TestCancelAfterOverlappingPrompts(t *testing.T) {
	model := newModel(waitTurn(), waitTurn())
	h := start(t, NewServer(plainFactory(model, nil)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, t.TempDir())

	first := promptAsync(t, h, id, "第一次")
	recv(t, model.started, "第一次模型请求")
	second := promptAsync(t, h, id, "第二次")
	res := recv(t, first, "第一次 prompt 返回")
	require.NoError(t, res.err)
	require.Equal(t, acpsdk.StopReasonCancelled, res.resp.StopReason)
	recv(t, model.started, "第二次模型请求")

	require.NoError(t, h.client.Cancel(context.Background(), acpsdk.CancelNotification{SessionId: id}))
	res = recv(t, second, "第二次 prompt 被取消")
	require.NoError(t, res.err)
	require.Equal(t, acpsdk.StopReasonCancelled, res.resp.StopReason)
}

// session/close 取消进行中的 prompt，之后该 ID 的 prompt 得到 invalid-params。
func TestCloseCancelsRunningPrompt(t *testing.T) {
	model := newModel(waitTurn())
	h := start(t, NewServer(plainFactory(model, nil)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, t.TempDir())

	done := promptAsync(t, h, id, "很慢的问题")
	recv(t, model.started, "模型请求")
	_, err := h.client.CloseSession(context.Background(), acpsdk.CloseSessionRequest{SessionId: id})
	require.NoError(t, err)
	res := recv(t, done, "prompt 被 close 取消")
	require.NoError(t, res.err)
	require.Equal(t, acpsdk.StopReasonCancelled, res.resp.StopReason)

	_, err = h.prompt(t, id, "还在吗")
	requireCode(t, err, -32602)
}

// 同一 ID 的并发 session/load 只有一个通过"已打开"检查。
func TestConcurrentLoadSameID(t *testing.T) {
	store := session.NewMemoryStore()
	dir := t.TempDir()
	tool := only(echoTool("lookup", nil, ""))
	h := start(t, NewServer(toolFactory(newModel(say("好")), nil, store, tool), WithSessionStore(store)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, dir)
	_, err := h.prompt(t, id, "你好")
	require.NoError(t, err)

	entered := make(chan struct{}, 2)
	gate := make(chan struct{})
	inner := toolFactory(newModel(), nil, store, tool)
	gated := func(ctx context.Context, s *Session) (Runner, error) {
		entered <- struct{}{}
		<-gate
		return inner(ctx, s)
	}
	h2 := start(t, NewServer(gated, WithSessionStore(store)), newEditor())
	h2.initialize(t, acpsdk.ClientCapabilities{})
	load := func() <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := h2.client.LoadSession(context.Background(), acpsdk.LoadSessionRequest{SessionId: id, Cwd: dir, McpServers: []acpsdk.McpServer{}})
			done <- err
		}()
		return done
	}
	first := load()
	recv(t, entered, "第一次加载进入 Factory")
	second := load()
	requireCode(t, recv(t, second, "第二次加载被拒绝"), -32602)
	close(gate)
	require.NoError(t, recv(t, first, "第一次加载完成"))
	require.Empty(t, entered, "第二次加载不应调用 Factory")
}

// closerRunner 记录 Close 的调用次数。
type closerRunner struct {
	Runner
	closed *atomic.Int32
}

func (r closerRunner) Close() error {
	r.closed.Add(1)
	return nil
}

func closing(f Factory, closed *atomic.Int32) Factory {
	return func(ctx context.Context, s *Session) (Runner, error) {
		r, err := f(ctx, s)
		if err != nil {
			return nil, err
		}
		return closerRunner{Runner: r, closed: closed}, nil
	}
}

func TestRunnerClosedOnSessionClose(t *testing.T) {
	var closed atomic.Int32
	h := start(t, NewServer(closing(plainFactory(newModel(), nil), &closed)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, t.TempDir())
	require.Zero(t, closed.Load())
	_, err := h.client.CloseSession(context.Background(), acpsdk.CloseSessionRequest{SessionId: id})
	require.NoError(t, err)
	require.EqualValues(t, 1, closed.Load())
}

// 回放失败时回滚加载，已构建的 Runner 被关闭。
func TestRunnerClosedOnLoadRollback(t *testing.T) {
	store := session.NewMemoryStore()
	bad := session.Event{Type: constants.SessionEventUserMessage, Time: time.Now(), Data: []byte(`"不是消息"`)}
	_, err := store.Append(context.Background(), "sess_bad", 0, bad)
	require.NoError(t, err)

	var closed atomic.Int32
	h := start(t, NewServer(closing(plainFactory(newModel(), nil), &closed), WithSessionStore(store)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	_, err = h.client.LoadSession(context.Background(), acpsdk.LoadSessionRequest{SessionId: "sess_bad", Cwd: t.TempDir(), McpServers: []acpsdk.McpServer{}})
	requireCode(t, err, -32603)
	require.EqualValues(t, 1, closed.Load())

	// 回滚后预留已释放：再次加载仍走到回放而不是"已打开"。
	_, err = h.client.LoadSession(context.Background(), acpsdk.LoadSessionRequest{SessionId: "sess_bad", Cwd: t.TempDir(), McpServers: []acpsdk.McpServer{}})
	requireCode(t, err, -32603)
}

// 对端断开时 Serve 返回 nil，并关闭全部会话的 Runner。
func TestServeReturnsNilOnDisconnectAndClosesRunners(t *testing.T) {
	var closed atomic.Int32
	srv := NewServer(closing(plainFactory(newModel(), nil), &closed))
	agentIn, editorOut := io.Pipe()
	editorIn, agentOut := io.Pipe()
	done := make(chan error, 1)
	go func() { done <- srv.Serve(context.Background(), agentIn, agentOut) }()
	client := acpsdk.NewClientSideConnection(newEditor(), editorOut, editorIn)
	_, err := client.Initialize(context.Background(), acpsdk.InitializeRequest{ProtocolVersion: acpsdk.ProtocolVersionNumber})
	require.NoError(t, err)
	for range 2 {
		_, err = client.NewSession(context.Background(), acpsdk.NewSessionRequest{Cwd: t.TempDir(), McpServers: []acpsdk.McpServer{}})
		require.NoError(t, err)
	}

	require.NoError(t, editorOut.Close())
	require.NoError(t, recv(t, done, "Serve 返回"))
	require.EqualValues(t, 2, closed.Load())
	_ = agentOut.Close()
}

// doneAfterCancel 在本轮被取消后才发出 done，确定性地模拟取消与正常结束同时发生。
type doneAfterCancel struct{ started chan struct{} }

func (r doneAfterCancel) ExecuteStream(ctx context.Context, _ any) (<-chan builder.AgentEvent, error) {
	ch := make(chan builder.AgentEvent, 1)
	go func() {
		defer close(ch)
		close(r.started)
		<-ctx.Done()
		ch <- builder.AgentEvent{Type: "done"}
	}()
	return ch, nil
}

// 收到 session/cancel 后即使 Runner 随即正常结束，也以 cancelled 返回（ACP 要求）。
func TestCancelRacingDoneReturnsCancelled(t *testing.T) {
	r := doneAfterCancel{started: make(chan struct{})}
	h := start(t, NewServer(func(context.Context, *Session) (Runner, error) { return r, nil }), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, t.TempDir())

	done := promptAsync(t, h, id, "问题")
	recv(t, r.started, "Runner 开始执行")
	require.NoError(t, h.client.Cancel(context.Background(), acpsdk.CancelNotification{SessionId: id}))
	res := recv(t, done, "prompt 返回")
	require.NoError(t, res.err)
	require.Equal(t, acpsdk.StopReasonCancelled, res.resp.StopReason)
}
