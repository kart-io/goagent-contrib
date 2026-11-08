package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	mg "github.com/kart-io/goagent-contrib/management"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

type blockingFactory struct {
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
	active  atomic.Int32
	max     atomic.Int32
}

func (f *blockingFactory) Validate(context.Context, mg.Version) (mg.Capabilities, error) {
	return mg.Capabilities{}, nil
}
func (f *blockingFactory) Build(context.Context, mg.Version, mg.Resources) (mg.Binding, error) {
	return mg.Binding{Runtime: f}, nil
}
func (f *blockingFactory) Inspect(context.Context, mg.RunSpec) (mg.RunEvidence, error) {
	return mg.RunEvidence{State: "absent"}, nil
}
func (f *blockingFactory) Run(ctx context.Context, _ mg.RunSpec) (mg.RunResult, error) {
	f.calls.Add(1)
	n := f.active.Add(1)
	defer f.active.Add(-1)
	for old := f.max.Load(); n > old && !f.max.CompareAndSwap(old, n); old = f.max.Load() {
	}
	select {
	case f.entered <- struct{}{}:
	default:
	}
	<-f.release
	if ctx.Err() != nil {
		return mg.RunResult{}, ctx.Err()
	}
	return mg.RunResult{Outcome: "completed", Output: json.RawMessage(`"已退出"`)}, nil
}
func TestCancelRequestWaitsForRealExit(t *testing.T) {
	b := &blockingFactory{entered: make(chan struct{}, 4), release: make(chan struct{})}
	f := setup(t, map[string]mg.Factory{"test": b})
	f.worker(t)
	se := f.session(t)
	task, e := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`1`)})
	require.NoError(t, e)
	select {
	case <-b.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("任务没有启动")
	}
	task, e = f.s.GetTask(f.ctx, f.actor, task.ID)
	require.NoError(t, e)
	task, e = f.s.CancelTask(f.ctx, f.actor, mg.CancelTaskRequest{IdempotencyKey: uuid.NewString(), TaskID: task.ID, ExpectedRevision: task.Revision})
	require.NoError(t, e)
	require.Equal(t, mg.Canceling, task.State)
	time.Sleep(60 * time.Millisecond)
	task, e = f.s.GetTask(f.ctx, f.actor, task.ID)
	require.NoError(t, e)
	require.Equal(t, mg.Canceling, task.State)
	close(b.release)
	require.Eventually(t, func() bool { v, e := f.s.GetTask(f.ctx, f.actor, task.ID); return e == nil && v.State == mg.Canceled }, 5*time.Second, 10*time.Millisecond)
}
func TestSameSessionNeverOverlaps(t *testing.T) {
	b := &blockingFactory{entered: make(chan struct{}, 4), release: make(chan struct{})}
	f := setup(t, map[string]mg.Factory{"test": b})
	f.worker(t)
	se := f.session(t)
	for i := 0; i < 2; i++ {
		_, e := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`1`)})
		require.NoError(t, e)
	}
	select {
	case <-b.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("任务没有启动")
	}
	time.Sleep(60 * time.Millisecond)
	require.Equal(t, int32(1), b.calls.Load())
	close(b.release)
	require.Eventually(t, func() bool {
		p, e := f.s.ListTasks(f.ctx, f.actor, mg.Query{SessionID: se.ID, Statuses: []string{"succeeded"}})
		return e == nil && len(p.Items) == 2
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, int32(1), b.max.Load())
}
func TestSecondWorkerRejected(t *testing.T) {
	f := setup(t, nil)
	f.worker(t)
	require.Eventually(t, func() bool {
		d, e := f.store.Get(f.ctx, "workers", "__system", "default")
		if e != nil {
			return false
		}
		var v mg.WorkerOwner
		_ = json.Unmarshal(d.Data, &v)
		return v.OwnerID != ""
	}, 5*time.Second, 10*time.Millisecond)
	other := mg.NewWorker(f.s)
	ctx, cancel := context.WithTimeout(f.ctx, time.Second)
	defer cancel()
	require.ErrorIs(t, other.Run(ctx), mg.ErrOwner)
}
func TestConcurrentDecisionsHaveOneWinner(t *testing.T) {
	var effects, requests atomic.Int32
	f := setup(t, map[string]mg.Factory{"test": nativeFactory(&effects, &requests)})
	f.worker(t)
	se := f.session(t)
	task, e := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"审核"`)})
	require.NoError(t, e)
	var ap mg.Approval
	require.Eventually(t, func() bool {
		p, e := f.s.ListApprovals(f.ctx, f.actor, mg.Query{TaskID: task.ID, Statuses: []string{"pending"}})
		if e == nil && len(p.Items) == 1 {
			ap = p.Items[0]
			return true
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)
	errs := make(chan error, 2)
	for _, decision := range []string{"approve", "reject"} {
		go func(decision string) {
			_, e := f.s.DecideApproval(f.ctx, f.actor, mg.DecisionRequest{IdempotencyKey: uuid.NewString(), ApprovalID: ap.ID, ExpectedRevision: ap.Revision, Decision: decision})
			errs <- e
		}(decision)
	}
	wins := 0
	for i := 0; i < 2; i++ {
		e := <-errs
		if e == nil {
			wins++
		} else {
			require.True(t, errors.Is(e, mg.ErrConflict), e)
		}
	}
	require.Equal(t, 1, wins)
}
func TestRejectDoesNotExecuteEffect(t *testing.T) {
	var effects, requests atomic.Int32
	f := setup(t, map[string]mg.Factory{"test": nativeFactory(&effects, &requests)})
	f.worker(t)
	se := f.session(t)
	task, e := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"拒绝退款"`)})
	require.NoError(t, e)
	var ap mg.Approval
	require.Eventually(t, func() bool {
		p, e := f.s.ListApprovals(f.ctx, f.actor, mg.Query{TaskID: task.ID})
		if e == nil && len(p.Items) == 1 {
			ap = p.Items[0]
			return true
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)
	_, e = f.s.DecideApproval(f.ctx, f.actor, mg.DecisionRequest{IdempotencyKey: uuid.NewString(), ApprovalID: ap.ID, ExpectedRevision: ap.Revision, Decision: "reject"})
	require.NoError(t, e)
	require.Eventually(t, func() bool { v, e := f.s.GetTask(f.ctx, f.actor, task.ID); return e == nil && v.State == mg.Succeeded }, 5*time.Second, 10*time.Millisecond)
	require.Zero(t, effects.Load())
}
