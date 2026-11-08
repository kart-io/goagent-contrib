package integration_test

import (
	"context"
	"github.com/google/uuid"
	"github.com/kart-io/goagent/codec/json"
	"github.com/kart-io/goagent/core/session"
	agentErrors "github.com/kart-io/goagent/errors"
	"github.com/kart-io/goagent/execution"
	mg "github.com/kart-io/goagent/management"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

func TestNativeInspectCompletedDoesNotRepeatModel(t *testing.T) {
	var effects, requests atomic.Int32
	factory := nativeFactory(&effects, &requests)
	f := setup(t, map[string]mg.Factory{"test": factory})
	f.worker(t)
	se := f.session(t)
	task, e := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"检查完成回放"`)})
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
	_, e = f.s.DecideApproval(f.ctx, f.actor, mg.DecisionRequest{IdempotencyKey: uuid.NewString(), ApprovalID: ap.ID, ExpectedRevision: ap.Revision, Decision: "approve"})
	require.NoError(t, e)
	require.Eventually(t, func() bool {
		v, e := f.s.GetTask(f.ctx, f.actor, task.ID)
		task = v
		return e == nil && v.State == mg.Succeeded
	}, 5*time.Second, 10*time.Millisecond)
	doc, e := f.store.Get(f.ctx, "versions", f.actor.TenantID, se.AgentID+"@v1")
	require.NoError(t, e)
	var version mg.Version
	require.NoError(t, json.Unmarshal(doc.Data, &version))
	j := session.DurableStore(f.cfg.Sessions, se.LogID)
	scope, e := execution.NewScope(execution.Config{RunID: task.RunID, SessionID: se.LogID, Journal: j})
	require.NoError(t, e)
	ctx, cancel, e := scope.Bind(context.Background())
	require.NoError(t, e)
	defer cancel()
	binding, e := factory.Build(ctx, version, mg.Resources{Task: task, Session: se, Scope: scope, Journal: j, Sessions: f.cfg.Sessions, ApprovalReader: func(context.Context, string) (mg.Approval, error) {
		return mg.Approval{}, agentErrors.ErrManagementForbidden
	}})
	require.NoError(t, e)
	beforeEvents, e := f.cfg.Sessions.Read(ctx, se.LogID, 0)
	require.NoError(t, e)
	before := requests.Load()
	evidence, e := binding.Runtime.Inspect(ctx, mg.RunSpec{Task: task})
	require.NoError(t, e)
	require.Equal(t, "completed", evidence.State)
	require.JSONEq(t, string(task.Result), string(evidence.Result.Output))
	require.Equal(t, before, requests.Load())
	require.Equal(t, int32(1), effects.Load())
	afterEvents, e := f.cfg.Sessions.Read(ctx, se.LogID, 0)
	require.NoError(t, e)
	require.Equal(t, beforeEvents, afterEvents)
}
