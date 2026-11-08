package integration_test

import (
	"context"
	"github.com/kart-io/goagent/codec/json"
	agentErrors "github.com/kart-io/goagent/errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	db "github.com/kart-io/goagent-contrib/management/pgstore"
	mg "github.com/kart-io/goagent/management"
	"github.com/kart-io/goagent/management/httpapi"
	"github.com/stretchr/testify/require"
)

type unavailableWriteStore struct {
	mg.Store
	writes mg.Store
}

func (s unavailableWriteStore) WithinTx(ctx context.Context, fn func(mg.Tx) error) error {
	return s.writes.WithinTx(ctx, fn)
}
func TestDatabaseUnavailableLeavesNoFalseAcceptance(t *testing.T) {
	f := setup(t, nil)
	w := f.worker(t)
	se := f.session(t)
	require.NoError(t, w.Shutdown(f.ctx))
	pool, err := pgxpool.NewWithConfig(f.ctx, f.pool.Config())
	require.NoError(t, err)
	store, err := db.New(f.ctx, pool)
	require.NoError(t, err)
	pool.Close()
	cfg := f.cfg
	cfg.Store = unavailableWriteStore{Store: f.store, writes: store}
	service, err := mg.New(cfg)
	require.NoError(t, err)
	handler, err := httpapi.New(service, httpapi.Config{ActorFromRequest: func(*http.Request) (mg.Actor, error) { return f.actor, nil }})
	require.NoError(t, err)
	response := httptest.NewRecorder()
	request := httptest.NewRequest("POST", "/v1/tasks", strings.NewReader(`{"session_id":"`+se.ID+`","input":"故障期间"}`))
	request.Header.Set("Idempotency-Key", uuid.NewString())
	handler.ServeHTTP(response, request)
	require.Equal(t, 503, response.Code, response.Body.String())
	tasks, err := f.s.ListTasks(f.ctx, f.actor, mg.Query{SessionID: se.ID})
	require.NoError(t, err)
	require.Empty(t, tasks.Items)
	docs, err := f.store.List(f.ctx, "commands", mg.Selection{TenantID: f.actor.TenantID, Query: mg.Query{SessionID: se.ID}, Visibility: mg.Visibility{All: true}})
	require.NoError(t, err)
	for _, doc := range docs {
		var command mg.Command
		require.NoError(t, json.Unmarshal(doc.Data, &command))
		require.NotEqual(t, "start", command.Kind)
	}
}

type rollbackAfterCallbackStore struct{ mg.Store }

func (s rollbackAfterCallbackStore) WithinTx(ctx context.Context, fn func(mg.Tx) error) error {
	return s.Store.WithinTx(ctx, func(tx mg.Tx) error {
		if err := fn(tx); err != nil {
			return err
		}
		return agentErrors.ErrManagementUnavailable
	})
}
func TestAdmissionAndDecisionRollbackAllManagementRecords(t *testing.T) {
	var effects, requests atomic.Int32
	f := setup(t, map[string]mg.Factory{"test": nativeFactory(&effects, &requests)})
	f.worker(t)
	se := f.session(t)
	cfg := f.cfg
	cfg.Store = rollbackAfterCallbackStore{Store: f.store}
	failing, err := mg.New(cfg)
	require.NoError(t, err)
	request := mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"事务中止"`)}
	_, err = failing.SubmitTask(f.ctx, f.actor, request)
	require.ErrorIs(t, err, agentErrors.ErrManagementUnavailable)
	page, err := f.s.ListTasks(f.ctx, f.actor, mg.Query{SessionID: se.ID})
	require.NoError(t, err)
	require.Empty(t, page.Items)
	var count int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM mg_audit WHERE tenant_id=$1 AND data->>'operation'='submit'`, f.actor.TenantID).Scan(&count))
	require.Zero(t, count)
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM mg_commands WHERE tenant_id=$1 AND data->>'kind'='start'`, f.actor.TenantID).Scan(&count))
	require.Zero(t, count)
	task, err := f.s.SubmitTask(f.ctx, f.actor, request)
	require.NoError(t, err)
	ap := pendingApproval(t, f, task.ID)
	// 使用最新Task修订号，证明拒绝来自审批通道而不是修订冲突。
	current, err := f.s.GetTask(f.ctx, f.actor, task.ID)
	require.NoError(t, err)
	_, err = f.s.ResumeTask(f.ctx, f.actor, mg.ResumeTaskRequest{IdempotencyKey: uuid.NewString(), TaskID: task.ID, ExpectedRevision: current.Revision, WaitID: ap.Wait.ID, Input: json.RawMessage(`{"decision":"approve"}`)})
	require.ErrorIs(t, err, agentErrors.ErrManagementApprovalRequired)
	decision := mg.DecisionRequest{IdempotencyKey: uuid.NewString(), ApprovalID: ap.ID, ExpectedRevision: ap.Revision, Decision: "approve", Reason: "核验通过"}
	_, err = failing.DecideApproval(f.ctx, f.actor, decision)
	require.ErrorIs(t, err, agentErrors.ErrManagementUnavailable)
	after, err := f.s.GetApproval(f.ctx, f.actor, ap.ID)
	require.NoError(t, err)
	require.Equal(t, "pending", after.State)
	require.Empty(t, after.CommandID)
	require.Zero(t, effects.Load())
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM mg_audit WHERE tenant_id=$1 AND data->>'operation'='decide'`, f.actor.TenantID).Scan(&count))
	require.Zero(t, count)
	after, err = f.s.DecideApproval(f.ctx, f.actor, decision)
	require.NoError(t, err)
	require.Equal(t, f.actor, after.Actor)
	require.Equal(t, "核验通过", after.Reason)
	require.NotNil(t, after.DecidedAt)
	require.NotEmpty(t, after.CommandID)
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM mg_audit WHERE tenant_id=$1 AND data->>'operation'='decide'`, f.actor.TenantID).Scan(&count))
	require.Equal(t, 1, count)
}
