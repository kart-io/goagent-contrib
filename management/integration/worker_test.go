package integration_test

import (
	"context"
	"github.com/google/uuid"
	"github.com/kart-io/goagent/codec/json"
	mg "github.com/kart-io/goagent/management"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func (f *fixture) worker(t *testing.T) *mg.Worker {
	t.Helper()
	w := mg.NewWorker(f.s)
	ctx, cancel := context.WithCancel(f.ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case e := <-done:
			require.NoError(t, e)
		case <-time.After(5 * time.Second):
			t.Error("执行宿主未退出")
		}
	})
	return w
}
func (f *fixture) session(t *testing.T) mg.Session {
	t.Helper()
	a, _ := f.agent(t)
	se, err := f.s.CreateSession(f.ctx, f.actor, mg.CreateSessionRequest{IdempotencyKey: uuid.NewString(), AgentID: a.ID})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		v, e := f.s.GetSession(f.ctx, f.actor, se.ID)
		if e == nil {
			se = v
		}
		return e == nil && se.Status == "ready"
	}, 5*time.Second, 10*time.Millisecond)
	return se
}
func TestSubmitTaskReturnsAfterDurableAcceptance(t *testing.T) {
	f := setup(t, nil)
	f.worker(t)
	se := f.session(t)
	r := mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"处理退款"`)}
	task, err := f.s.SubmitTask(f.ctx, f.actor, r)
	require.NoError(t, err)
	again, err := f.s.SubmitTask(f.ctx, f.actor, r)
	require.NoError(t, err)
	require.Equal(t, task.ID, again.ID)
	require.Eventually(t, func() bool { v, e := f.s.GetTask(f.ctx, f.actor, task.ID); return e == nil && v.State == mg.Succeeded }, 5*time.Second, 10*time.Millisecond)
}
func TestQueuedCancelNeverBuildsRuntime(t *testing.T) {
	f := setup(t, nil)
	a, _ := f.agent(t)
	se, e := f.s.CreateSession(f.ctx, f.actor, mg.CreateSessionRequest{IdempotencyKey: uuid.NewString(), AgentID: a.ID})
	require.NoError(t, e) // 直接准备日志命令需要 worker，先等待创建再停宿主。
	w := mg.NewWorker(f.s)
	ctx, cancel := context.WithCancel(f.ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	require.Eventually(t, func() bool { v, e := f.s.GetSession(f.ctx, f.actor, se.ID); return e == nil && v.Status == "ready" }, 5*time.Second, 10*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	task, e := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"不执行"`)})
	require.NoError(t, e)
	task, e = f.s.CancelTask(f.ctx, f.actor, mg.CancelTaskRequest{IdempotencyKey: uuid.NewString(), TaskID: task.ID, ExpectedRevision: task.Revision})
	require.NoError(t, e)
	require.Equal(t, mg.Canceled, task.State)
}
