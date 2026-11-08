package integration_test

import (
	"context"
	"fmt"
	"github.com/kart-io/goagent/codec/json"
	agentErrors "github.com/kart-io/goagent/errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	mg "github.com/kart-io/goagent/management"
	"github.com/kart-io/goagent/management/httpapi"
	"github.com/stretchr/testify/require"
)

type resourcePolicy struct {
	mg.TenantAuthorizer
	all                  bool
	agentIDs, sessionIDs []string
	revoked              atomic.Bool
	version              atomic.Int64
}

func (p *resourcePolicy) Authorize(ctx context.Context, a mg.Actor, action string, r mg.ResourceRef) error {
	if err := p.TenantAuthorizer.Authorize(ctx, a, action, r); err != nil {
		return err
	}
	if action == "approval.decide" && p.revoked.Load() {
		return agentErrors.ErrManagementForbidden
	}
	if p.all {
		return nil
	}
	for _, id := range p.agentIDs {
		if r.AgentID == id {
			return nil
		}
	}
	for _, id := range p.sessionIDs {
		if r.SessionID == id {
			return nil
		}
	}
	return agentErrors.ErrManagementForbidden
}
func (p *resourcePolicy) Scope(_ context.Context, a mg.Actor, _, _ string) (mg.Visibility, error) {
	return mg.Visibility{All: p.all, AgentIDs: p.agentIDs, SessionIDs: p.sessionIDs, PolicyVersion: fmt.Sprint(p.version.Load())}, mg.ValidateActor(a)
}
func pendingApproval(t *testing.T, f *fixture, taskID string) mg.Approval {
	t.Helper()
	var ap mg.Approval
	require.Eventually(t, func() bool {
		page, err := f.s.ListApprovals(f.ctx, f.actor, mg.Query{TaskID: taskID, Statuses: []string{"pending"}})
		if err == nil && len(page.Items) == 1 {
			ap = page.Items[0]
			return true
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)
	return ap
}
func TestTenantIsolationRejectsReadWriteAndEnumeration(t *testing.T) {
	f := setup(t, nil)
	f.worker(t)
	se := f.session(t)
	task, err := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"租户隔离"`)})
	require.NoError(t, err)
	outsider := mg.Actor{TenantID: uuid.NewString(), Subject: "另一租户"}
	_, err = f.s.GetAgent(f.ctx, outsider, se.AgentID)
	require.ErrorIs(t, err, agentErrors.ErrManagementNotFound)
	_, err = f.s.GetSession(f.ctx, outsider, se.ID)
	require.ErrorIs(t, err, agentErrors.ErrManagementNotFound)
	_, err = f.s.GetTask(f.ctx, outsider, task.ID)
	require.ErrorIs(t, err, agentErrors.ErrManagementNotFound)
	_, err = f.s.GetRun(f.ctx, outsider, task.ID)
	require.ErrorIs(t, err, agentErrors.ErrManagementNotFound)
	_, err = f.s.SubmitTask(f.ctx, outsider, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"伪造关联"`)})
	require.ErrorIs(t, err, agentErrors.ErrManagementNotFound)
	_, err = f.s.CancelTask(f.ctx, outsider, mg.CancelTaskRequest{IdempotencyKey: uuid.NewString(), TaskID: task.ID, ExpectedRevision: task.Revision})
	require.ErrorIs(t, err, agentErrors.ErrManagementNotFound)
	tasks, err := f.s.ListTasks(f.ctx, outsider, mg.Query{AgentID: se.AgentID})
	require.NoError(t, err)
	require.Empty(t, tasks.Items)
	sessions, err := f.s.ListSessions(f.ctx, outsider, mg.Query{})
	require.NoError(t, err)
	require.Empty(t, sessions.Items)
	summary, err := f.s.SummarizeRuns(f.ctx, outsider, mg.Query{})
	require.NoError(t, err)
	require.Zero(t, summary.Totals.Tasks)
}
func TestResourceVisibilityAndCursorPolicyAreEnforced(t *testing.T) {
	f := setup(t, nil)
	w := f.worker(t)
	first := f.session(t)
	other := f.session(t)
	for range 3 {
		_, err := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: first.ID, Input: json.RawMessage(`"可见"`)})
		require.NoError(t, err)
	}
	require.NoError(t, w.Shutdown(f.ctx))
	policy := &resourcePolicy{agentIDs: []string{first.AgentID}}
	cfg := f.cfg
	cfg.Authorizer = policy
	service, err := mg.New(cfg)
	require.NoError(t, err)
	_, err = service.GetAgent(f.ctx, f.actor, first.AgentID)
	require.NoError(t, err, "Agent自身必须携带AgentID权限关系")
	_, err = service.GetSession(f.ctx, f.actor, first.ID)
	require.NoError(t, err)
	_, err = service.GetAgent(f.ctx, f.actor, other.AgentID)
	require.ErrorIs(t, err, agentErrors.ErrManagementNotFound)
	summary, err := service.SummarizeRuns(f.ctx, f.actor, mg.Query{Limit: 1})
	require.NoError(t, err)
	require.Equal(t, 3, summary.Totals.Tasks)
	page, err := service.ListTasks(f.ctx, f.actor, mg.Query{Limit: 1})
	require.NoError(t, err)
	require.NotEmpty(t, page.NextCursor)
	policy.version.Add(1)
	_, err = service.ListTasks(f.ctx, f.actor, mg.Query{Limit: 1, Cursor: page.NextCursor})
	require.ErrorIs(t, err, agentErrors.ErrManagementInvalid)
	_, err = service.ListTasks(f.ctx, f.actor, mg.Query{Limit: 1, Cursor: page.NextCursor, SessionID: other.ID})
	require.ErrorIs(t, err, agentErrors.ErrManagementInvalid)
	handler, err := httpapi.New(service, httpapi.Config{ActorFromRequest: func(*http.Request) (mg.Actor, error) { return f.actor, nil }})
	require.NoError(t, err)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/v1/agents/"+other.AgentID, nil))
	require.Equal(t, 404, response.Code)
	sessionPolicy := &resourcePolicy{sessionIDs: []string{first.ID}}
	cfg.Authorizer = sessionPolicy
	service, err = mg.New(cfg)
	require.NoError(t, err)
	_, err = service.GetSession(f.ctx, f.actor, first.ID)
	require.NoError(t, err, "Session自身必须携带SessionID权限关系")
}
func TestRevokedPermissionAndDisabledAgentKeepDecisionPending(t *testing.T) {
	for _, mode := range []string{"permission", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			var effects, requests atomic.Int32
			f := setup(t, map[string]mg.Factory{"test": nativeFactory(&effects, &requests)})
			policy := &resourcePolicy{all: true}
			cfg := f.cfg
			cfg.Authorizer = policy
			var err error
			f.s, err = mg.New(cfg)
			require.NoError(t, err)
			w := f.worker(t)
			se := f.session(t)
			task, err := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"权限复核"`)})
			require.NoError(t, err)
			ap := pendingApproval(t, f, task.ID)
			require.NoError(t, w.Shutdown(f.ctx))
			ap, err = f.s.DecideApproval(f.ctx, f.actor, mg.DecisionRequest{IdempotencyKey: uuid.NewString(), ApprovalID: ap.ID, ExpectedRevision: ap.Revision, Decision: "approve", Reason: "保留原决定"})
			require.NoError(t, err)
			if mode == "permission" {
				policy.revoked.Store(true)
			} else {
				agent, e := f.s.GetAgent(f.ctx, f.actor, se.AgentID)
				require.NoError(t, e)
				_, e = f.s.SetAgentEnabled(f.ctx, f.actor, mg.SetEnabledRequest{IdempotencyKey: uuid.NewString(), AgentID: agent.ID, ExpectedRevision: agent.Revision, Enabled: false})
				require.NoError(t, e)
			}
			f.worker(t)
			require.Eventually(t, func() bool {
				command, e := f.s.GetCommand(f.ctx, f.actor, ap.CommandID)
				return e == nil && command.State == "blocked" && command.ErrorKind != ""
			}, 2*time.Second, 10*time.Millisecond)
			require.Zero(t, effects.Load())
			current, err := f.s.GetTask(f.ctx, f.actor, task.ID)
			require.NoError(t, err)
			require.Equal(t, mg.WaitingApproval, current.State)
			if mode == "permission" {
				policy.revoked.Store(false)
			} else {
				agent, e := f.s.GetAgent(f.ctx, f.actor, se.AgentID)
				require.NoError(t, e)
				_, e = f.s.SetAgentEnabled(f.ctx, f.actor, mg.SetEnabledRequest{IdempotencyKey: uuid.NewString(), AgentID: agent.ID, ExpectedRevision: agent.Revision, Enabled: true})
				require.NoError(t, e)
			}
			require.Eventually(t, func() bool {
				current, e := f.s.GetTask(f.ctx, f.actor, task.ID)
				return e == nil && current.State == mg.Succeeded
			}, 5*time.Second, 10*time.Millisecond)
			after, err := f.s.GetApproval(f.ctx, f.actor, ap.ID)
			require.NoError(t, err)
			require.Equal(t, ap.Actor, after.Actor)
			require.Equal(t, "approve", after.Decision)
			require.Equal(t, int32(1), effects.Load())
			command, err := f.s.GetCommand(f.ctx, f.actor, ap.CommandID)
			require.NoError(t, err)
			require.Equal(t, "applied", command.State)
			require.Empty(t, command.ErrorKind)
		})
	}
}
func TestMissingImplementationBlocksInsteadOfUsingAnotherFactory(t *testing.T) {
	f := setup(t, nil)
	w := f.worker(t)
	se := f.session(t)
	require.NoError(t, w.Shutdown(f.ctx))
	cfg := f.cfg
	cfg.Factories = map[string]mg.Factory{"other": factory{}}
	var err error
	f.s, err = mg.New(cfg)
	require.NoError(t, err)
	task, err := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"固定实现"`)})
	require.NoError(t, err)
	f.worker(t)
	require.Eventually(t, func() bool {
		docs, e := f.store.List(f.ctx, "commands", mg.Selection{TenantID: f.actor.TenantID, Query: mg.Query{TaskID: task.ID}, Visibility: mg.Visibility{All: true}})
		if e != nil || len(docs) != 1 {
			return false
		}
		var c mg.Command
		return json.Unmarshal(docs[0].Data, &c) == nil && c.State == "blocked" && c.ErrorKind == "implementation_missing"
	}, 2*time.Second, 10*time.Millisecond)
	current, err := f.s.GetTask(f.ctx, f.actor, task.ID)
	require.NoError(t, err)
	require.Equal(t, mg.Queued, current.State)
}
