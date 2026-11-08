package integration_test

import (
	"encoding/json"
	"github.com/google/uuid"
	mg "github.com/kart-io/goagent-contrib/management"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

func TestForkSourceAndTargetIsolated(t *testing.T) {
	var effects, requests atomic.Int32
	f := setup(t, map[string]mg.Factory{"test": nativeFactory(&effects, &requests)})
	f.worker(t)
	se := f.session(t)
	task, e := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"分叉前的任务"`)})
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
	active, e := f.s.GetSession(f.ctx, f.actor, se.ID)
	require.NoError(t, e)
	_, e = f.s.ForkSession(f.ctx, f.actor, mg.ForkSessionRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, ExpectedRevision: active.Revision})
	require.ErrorIs(t, e, mg.ErrConflict)
	_, e = f.s.DecideApproval(f.ctx, f.actor, mg.DecisionRequest{IdempotencyKey: uuid.NewString(), ApprovalID: ap.ID, ExpectedRevision: ap.Revision, Decision: "approve"})
	require.NoError(t, e)
	require.Eventually(t, func() bool { v, e := f.s.GetTask(f.ctx, f.actor, task.ID); return e == nil && v.State == mg.Succeeded }, 5*time.Second, 10*time.Millisecond)
	source, e := f.s.GetSession(f.ctx, f.actor, se.ID)
	require.NoError(t, e)
	branch, e := f.s.ForkSession(f.ctx, f.actor, mg.ForkSessionRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, ExpectedRevision: source.Revision})
	require.NoError(t, e)
	require.Equal(t, "v1", branch.Version)
	require.Eventually(t, func() bool { v, e := f.s.GetSession(f.ctx, f.actor, branch.ID); return e == nil && v.Status == "ready" }, 5*time.Second, 10*time.Millisecond)
	h1, e := f.s.History(f.ctx, f.actor, mg.HistoryRequest{SessionID: source.ID, Limit: 200})
	require.NoError(t, e)
	h2, e := f.s.History(f.ctx, f.actor, mg.HistoryRequest{SessionID: branch.ID, Limit: 200})
	require.NoError(t, e)
	require.Equal(t, h1.Items, h2.Items)
	require.Equal(t, int32(1), effects.Load())
	require.NotEqual(t, source.LogID, branch.LogID)
}
