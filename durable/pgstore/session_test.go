package pgstore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kart-io/goagent/builder"
	shared "github.com/kart-io/goagent/constants"
	"github.com/kart-io/goagent/core/durable"
	"github.com/kart-io/goagent/core/session"
	"github.com/kart-io/goagent/core/session/sessiontest"
	agentErrors "github.com/kart-io/goagent/errors"
	"github.com/kart-io/goagent/interfaces"
	"github.com/kart-io/goagent/llm"
	"github.com/kart-io/goagent/tools"
)

func newSessionStore(t *testing.T, dsn, schema string) *SessionStore {
	t.Helper()
	s, err := NewSessionStore(context.Background(), newStore(t, dsn, schema))
	require.NoError(t, err)
	return s
}

func TestSessionConformance(t *testing.T) {
	testDSN(t)
	sessiontest.Run(t, func(t *testing.T) (session.Store, session.Store) {
		dsn, schema := newSchema(t)
		return newSessionStore(t, dsn, schema), newSessionStore(t, dsn, schema)
	}, sessiontest.Options{Leases: &sessiontest.Leases{
		TTL: testTTL,
		Expire: func(t *testing.T, s session.Store, id, name string) {
			st := s.(*SessionStore).root
			stopRenew(st, sessionNS(id), name)
			_, err := st.cfg.Pool.Exec(context.Background(), `UPDATE goagent_durable_lease SET expires_at = now() - interval '1 second' WHERE ns = $1 AND id = $2`, sessionNS(id), name)
			require.NoError(t, err)
		},
		Crash: func(t *testing.T, s session.Store) { stopRenew(s.(*SessionStore).root, "") },
	}})
}

func TestSessionSchemaVersion(t *testing.T) {
	dsn, schema := newSchema(t)
	store := newStore(t, dsn, schema)
	_, err := store.cfg.Pool.Exec(context.Background(), `UPDATE goagent_session_schema SET version = 2`)
	require.NoError(t, err)
	_, err = NewSessionStore(context.Background(), store)
	require.ErrorContains(t, err, "会话日志表结构版本为 2")
	_, err = store.cfg.Pool.Exec(context.Background(), `DROP TABLE goagent_session_schema`)
	require.NoError(t, err)
	_, err = NewSessionStore(context.Background(), store)
	require.ErrorContains(t, err, "读取会话日志表结构版本失败", "只建了 Schema 时须报告缺少会话日志的表")
}

// 不持锁追加持久快照被拒，且整批不写入。
func TestSessionSnapshotRequiresLock(t *testing.T) {
	dsn, schema := newSchema(t)
	s := newSessionStore(t, dsn, schema)
	ctx := context.Background()
	msg, err := session.NewEvent(shared.SessionEventUserMessage, session.MessageData{Message: llm.UserMessage("一")})
	require.NoError(t, err)
	snap, err := session.NewEvent(shared.SessionEventBoundarySnapshot, session.BoundaryData{ID: "op", Snapshot: []byte(`{}`)})
	require.NoError(t, err)
	_, err = s.Append(ctx, "s", 0, msg, snap)
	require.ErrorIs(t, err, agentErrors.ErrLeaseLost)
	_, err = s.Read(ctx, "s", 0)
	require.ErrorIs(t, err, agentErrors.ErrSessionNotFound)

	unlock, err := s.Lock(ctx, "s", session.DurableLockName("op"))
	require.NoError(t, err)
	last, err := s.Append(ctx, "s", 0, msg, snap)
	require.NoError(t, err)
	require.EqualValues(t, 2, last)
	require.NoError(t, unlock())
}

// toolModel 第一次请求调用 lookup，之后给出答复，并记录请求次数。
type toolModel struct {
	mu    sync.Mutex
	calls int
}

func (m *toolModel) CompleteToolsStream(_ context.Context, req *llm.CompletionRequest, _ []interfaces.Tool) (<-chan *llm.StreamChunk, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	chunk := &llm.StreamChunk{Delta: "答", Done: true, FinishReason: "stop"}
	if req.Messages[len(req.Messages)-1].Role == "user" {
		call := llm.ToolCall{ID: "c1", Type: "function"}
		call.Function.Name, call.Function.Arguments = "lookup", `{}`
		chunk = &llm.StreamChunk{Done: true, FinishReason: "tool_calls", ToolCalls: []llm.ToolCall{call}}
	}
	ch := make(chan *llm.StreamChunk, 1)
	ch <- chunk
	close(ch)
	return ch, nil
}
func (*toolModel) CompleteStream(context.Context, *llm.CompletionRequest) (<-chan *llm.StreamChunk, error) {
	return nil, errors.New("unused")
}
func (*toolModel) Provider() shared.Provider { return shared.ProviderCustom }

// Agent 的会话日志与持久快照写入同一个 Postgres 会话：一个实例在工具执行中中断，另一个实例以同一执行键恢复，
// 只请求剩余的模型回合，日志中只有一轮、消息不重复。
func TestSessionLogResumeAcrossInstances(t *testing.T) {
	dsn, schema := newSchema(t)
	var runs atomic.Int32
	started := make(chan struct{}, 1)
	tool := tools.NewBaseTool("lookup", "查询", `{"type":"object"}`, func(ctx context.Context, _ *interfaces.ToolInput) (*interfaces.ToolOutput, error) {
		if runs.Add(1) == 1 {
			started <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return &interfaces.ToolOutput{Success: true, Result: "found"}, nil
	})
	run := func(ctx context.Context, store *SessionStore, model *toolModel) (string, error) {
		reg := tools.NewRegistry()
		require.NoError(t, reg.Register(tool))
		d, err := tools.NewDispatcher(reg, tools.DispatchConfig{})
		require.NoError(t, err)
		agent, err := builder.NewSimpleBuilder(model).WithSessionID("s").WithSessionLog(store).WithDurableScope("pg").
			WithTools(d.Tools()...).WithMaxIterations(4).Build()
		require.NoError(t, err)
		ctx = durable.WithJournal(durable.WithExecutionKey(ctx, "run-1"), session.DurableStore(store, "s"))
		out, err := agent.AsRunnable().Invoke(ctx, "查")
		if err != nil {
			return "", err
		}
		return out.Result.(string), nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := run(ctx, newSessionStore(t, dsn, schema), &toolModel{})
		done <- err
	}()
	<-started
	cancel()
	require.Error(t, <-done)

	other := newSessionStore(t, dsn, schema)
	model := &toolModel{}
	answer, err := run(context.Background(), other, model)
	require.NoError(t, err)
	require.Equal(t, "答", answer)
	require.Equal(t, 1, model.calls, "第一次模型回合从日志回放")

	events, err := other.Read(context.Background(), "s", 0)
	require.NoError(t, err)
	var types []string
	snapshots := 0
	for _, ev := range events {
		if ev.Type == shared.SessionEventBoundarySnapshot {
			snapshots++
			continue
		}
		types = append(types, ev.Type)
	}
	require.Equal(t, []string{
		shared.SessionEventTurnStart, shared.SessionEventToolDefinitions, shared.SessionEventUserMessage, shared.SessionEventAssistantMessage,
		shared.SessionEventToolResult, shared.SessionEventAssistantMessage, shared.SessionEventTurnEnd,
	}, types)
	require.Positive(t, snapshots)
	msgs, err := session.DeriveMessages(events)
	require.NoError(t, err)
	require.Len(t, msgs, 4)
	require.Equal(t, "found", msgs[2].Content)
}
