package acp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/kart-io/goagent/builder"
	"github.com/kart-io/goagent/constants"
	"github.com/kart-io/goagent/core/session"
)

func TestFactoryFailureReleasesRegisteredResources(t *testing.T) {
	var count atomic.Int32
	h := start(t, NewServer(func(_ context.Context, s *Session) (Runner, error) {
		require.NoError(t, s.OnClose(func(context.Context) error { count.Add(1); return nil }))
		return nil, errors.New("构造失败")
	}), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	_, err := h.client.NewSession(context.Background(), acpsdk.NewSessionRequest{Cwd: t.TempDir(), McpServers: []acpsdk.McpServer{}})
	requireCode(t, err, -32603)
	require.EqualValues(t, 1, count.Load())
}
func TestCloseReportsCleanupErrorsAndRunsAll(t *testing.T) {
	var order []int
	h := start(t, NewServer(func(_ context.Context, s *Session) (Runner, error) {
		require.NoError(t, s.OnClose(func(context.Context) error { order = append(order, 1); return nil }))
		require.NoError(t, s.OnClose(func(context.Context) error { order = append(order, 2); return errors.New("释放失败") }))
		return eventRunner(func(context.Context, any) (<-chan builder.AgentEvent, error) { return nil, nil }), nil
	}), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	_, err := h.client.CloseSession(context.Background(), acpsdk.CloseSessionRequest{SessionId: h.newSession(t, t.TempDir())})
	requireCode(t, err, -32603)
	require.Contains(t, err.Error(), "释放失败")
	require.Equal(t, []int{2, 1}, order)
}

func TestPromptWaitsForProducerAfterDone(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	h := start(t, NewServer(func(context.Context, *Session) (Runner, error) {
		return eventRunner(func(context.Context, any) (<-chan builder.AgentEvent, error) {
			ch := make(chan builder.AgentEvent, 1)
			ch <- builder.AgentEvent{Type: "done", Output: &builder.AgentOutput{Result: "答复"}}
			go func() { close(entered); <-release; close(ch) }()
			return ch, nil
		}), nil
	}), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, t.TempDir())
	done := promptAsync(t, h, id, "问题")
	recv(t, entered, "生产者进入")
	early := false
	select {
	case <-done:
		early = true
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if !early {
		require.NoError(t, recv(t, done, "生产者结束").err)
	}
	require.False(t, early, "不能在事件生产者退出前交还执行权")
}

func TestServerQueuedPromptHonorsCancellation(t *testing.T) {
	seen := make(chan *Session, 1)
	entered := make(chan struct{})
	release := make(chan struct{})
	h := start(t, NewServer(func(_ context.Context, s *Session) (Runner, error) {
		seen <- s
		return eventRunner(func(context.Context, any) (<-chan builder.AgentEvent, error) {
			ch := make(chan builder.AgentEvent)
			go func() { close(entered); <-release; close(ch) }()
			return ch, nil
		}), nil
	}), newEditor())
	defer close(release)
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, t.TempDir())
	s := <-seen
	first := promptAsync(t, h, id, "首轮")
	recv(t, entered, "首轮进入")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan promptResult, 1)
	go func() {
		resp, err := s.conn.Prompt(ctx, acpsdk.PromptRequest{SessionId: id, Prompt: []acpsdk.ContentBlock{acpsdk.TextBlock("等待轮")}})
		done <- promptResult{resp, err}
	}()
	result := recv(t, done, "服务端等待执行权时取消")
	require.NoError(t, result.err)
	require.Equal(t, acpsdk.StopReasonCancelled, result.resp.StopReason)
	_ = first
}

func TestRollbackKeepsIDReservedUntilCleanupFinishes(t *testing.T) {
	old := endTurnTimeout
	endTurnTimeout = 50 * time.Millisecond
	t.Cleanup(func() { endTurnTimeout = old })
	store := session.NewMemoryStore()
	_, err := store.Append(context.Background(), "blocked", 0, session.Event{Type: constants.SessionEventUserMessage, Data: []byte(`"坏消息"`)})
	require.NoError(t, err)
	gate := make(chan struct{})
	var calls atomic.Int32
	h := start(t, NewServer(func(_ context.Context, s *Session) (Runner, error) {
		if calls.Add(1) == 1 {
			require.NoError(t, s.OnClose(func(context.Context) error { <-gate; return nil }))
		}
		return eventRunner(func(context.Context, any) (<-chan builder.AgentEvent, error) { return nil, nil }), nil
	}, WithSessionStore(store)), newEditor())
	defer close(gate)
	h.initialize(t, acpsdk.ClientCapabilities{})
	load := func() error {
		_, err := h.client.LoadSession(context.Background(), acpsdk.LoadSessionRequest{SessionId: "blocked", Cwd: t.TempDir(), McpServers: []acpsdk.McpServer{}})
		return err
	}
	requireCode(t, load(), -32603)
	requireCode(t, load(), -32602)
	require.EqualValues(t, 1, calls.Load(), "旧资源清理中不得重新构造同 ID")
}
