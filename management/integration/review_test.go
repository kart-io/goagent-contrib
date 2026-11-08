package integration_test

import (
	"context"
	"github.com/google/uuid"
	"github.com/kart-io/goagent/codec/json"
	agentErrors "github.com/kart-io/goagent/errors"
	"github.com/kart-io/goagent/interfaces"
	mg "github.com/kart-io/goagent/management"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

type reviewAttentionFactory struct{}

func (reviewAttentionFactory) Validate(context.Context, mg.Version) (mg.Capabilities, error) {
	return mg.Capabilities{}, nil
}
func (reviewAttentionFactory) Build(context.Context, mg.Version, mg.Resources) (mg.Binding, error) {
	return mg.Binding{Runtime: reviewAttentionRuntime{}}, nil
}

type reviewAttentionRuntime struct{}

func (reviewAttentionRuntime) Inspect(context.Context, mg.RunSpec) (mg.RunEvidence, error) {
	return mg.RunEvidence{State: "attention", Result: mg.RunResult{Outcome: "attention", ErrorKind: "unconfirmed_tool"}}, nil
}
func (reviewAttentionRuntime) Run(context.Context, mg.RunSpec) (mg.RunResult, error) {
	panic("未知效果不得重入")
}
func TestReviewCancelPreservesAttention(t *testing.T) {
	f := setup(t, map[string]mg.Factory{"test": reviewAttentionFactory{}})
	w := f.worker(t)
	se := f.session(t)
	require.NoError(t, w.Shutdown(f.ctx))
	task, err := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"x"`)})
	require.NoError(t, err)
	old := task.Revision
	task.Revision++
	task.State = mg.Canceling
	task.CancelRequested = true
	task.RecoveryNeeded = true
	raw, err := json.Marshal(task)
	require.NoError(t, err)
	require.NoError(t, f.store.WithinTx(f.ctx, func(tx mg.Tx) error {
		return tx.CAS(f.ctx, mg.Document{Kind: "tasks", TenantID: task.TenantID, ID: task.ID, Revision: task.Revision, CreatedAt: task.CreatedAt, Data: raw}, old)
	}))
	f.worker(t)
	require.Eventually(t, func() bool {
		cur, e := f.s.GetTask(f.ctx, f.actor, task.ID)
		return e == nil && (cur.State.Terminal() || cur.State == mg.Attention)
	}, 5*time.Second, 10*time.Millisecond)
	cur, err := f.s.GetTask(f.ctx, f.actor, task.ID)
	require.NoError(t, err)
	require.Equal(t, mg.Attention, cur.State, "未知外部效果必须保留责任标记")
	require.Equal(t, "unconfirmed_tool", cur.ErrorKind)
}

type reviewBlockingFactory struct {
	armed            atomic.Bool
	entered, release chan struct{}
}

func (f *reviewBlockingFactory) Validate(context.Context, mg.Version) (mg.Capabilities, error) {
	if f.armed.Load() {
		close(f.entered)
		<-f.release
	}
	return mg.Capabilities{}, nil
}
func (f *reviewBlockingFactory) Build(context.Context, mg.Version, mg.Resources) (mg.Binding, error) {
	return mg.Binding{Runtime: runtime{}}, nil
}
func TestReviewValidateHonorsShutdownDeadline(t *testing.T) {
	factory := &reviewBlockingFactory{entered: make(chan struct{}), release: make(chan struct{})}
	f := setup(t, map[string]mg.Factory{"test": factory})
	w := f.worker(t)
	se := f.session(t)
	factory.armed.Store(true)
	_, err := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"x"`)})
	require.NoError(t, err)
	select {
	case <-factory.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("校验未开始")
	}
	defer close(factory.release)
	ctx, cancel := context.WithTimeout(f.ctx, 30*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Shutdown(ctx) }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorIs(t, mg.NewWorker(f.s).Run(f.ctx), agentErrors.ErrManagementOwner)
	case <-time.After(200 * time.Millisecond):
		t.Error("Shutdown超过期限仍被BeginShutdown阻塞")
	}
}

type reviewUnattributedFactory struct{ known bool }

func (reviewUnattributedFactory) Validate(context.Context, mg.Version) (mg.Capabilities, error) {
	return mg.Capabilities{}, nil
}
func (f reviewUnattributedFactory) Build(_ context.Context, _ mg.Version, r mg.Resources) (mg.Binding, error) {
	return mg.Binding{Runtime: reviewUnattributedRuntime{r: r, known: f.known}}, nil
}

type reviewUnattributedRuntime struct {
	r     mg.Resources
	known bool
}

func (reviewUnattributedRuntime) Inspect(context.Context, mg.RunSpec) (mg.RunEvidence, error) {
	return mg.RunEvidence{State: "absent"}, nil
}
func (r reviewUnattributedRuntime) Run(ctx context.Context, _ mg.RunSpec) (mg.RunResult, error) {
	if r.known {
		if err := r.r.Budget.Reserve(ctx, "known-model"); err != nil {
			return mg.RunResult{}, err
		}
		if err := r.r.Budget.RecordUsage(ctx, "known-model", &interfaces.TokenUsage{PromptTokens: 10, TotalTokens: 10}); err != nil {
			return mg.RunResult{}, err
		}
	}
	err := r.r.Budget.Reserve(ctx, "unattributed-model")
	return mg.RunResult{Outcome: "completed", Output: json.RawMessage(`"ok"`)}, err
}
func TestReviewUnknownUnattributedIsNotZero(t *testing.T) {
	f := setup(t, map[string]mg.Factory{"test": reviewUnattributedFactory{}})
	w := f.worker(t)
	se := f.session(t)
	task, err := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"x"`)})
	require.NoError(t, err)
	require.Eventually(t, func() bool { cur, e := f.s.GetTask(f.ctx, f.actor, task.ID); return e == nil && cur.State.Terminal() }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, w.Shutdown(f.ctx))
	s, err := f.s.SummarizeRuns(f.ctx, f.actor, mg.Query{})
	require.NoError(t, err)
	require.Nil(t, s.Totals.KnownTokens)
	require.Nil(t, s.Unattributed, "未报告的未归属用量不能变成已知零")
}

func TestReviewMixedUnattributedKeepsPartialCounters(t *testing.T) {
	f := setup(t, map[string]mg.Factory{"test": reviewUnattributedFactory{known: true}})
	w := f.worker(t)
	se := f.session(t)
	task, err := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"混合证据"`)})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		current, e := f.s.GetTask(f.ctx, f.actor, task.ID)
		return e == nil && current.State == mg.Succeeded
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, w.Shutdown(f.ctx))
	summary, err := f.s.SummarizeRuns(f.ctx, f.actor, mg.Query{})
	require.NoError(t, err)
	require.NotNil(t, summary.Unattributed)
	require.Equal(t, 10, summary.Unattributed.TotalTokens)
	require.Equal(t, 1, summary.UnattributedKnownEntries)
	require.Equal(t, 1, summary.UnattributedUnknownEntries)
	require.False(t, summary.UnattributedComplete)
}
