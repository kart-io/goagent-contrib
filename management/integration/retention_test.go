package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	mg "github.com/kart-io/goagent-contrib/management"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

type acknowledgedLateStore struct {
	mg.Store
	lose atomic.Bool
}

func (s *acknowledgedLateStore) WithinTx(ctx context.Context, fn func(mg.Tx) error) error {
	err := s.Store.WithinTx(ctx, fn)
	if err == nil && s.lose.CompareAndSwap(true, false) {
		return errors.New("注入提交成功但确认丢失")
	}
	return err
}
func TestCommitUnknownRetriesSameTaskWithoutLosingAdmission(t *testing.T) {
	f := setup(t, nil)
	w := f.worker(t)
	se := f.session(t)
	require.NoError(t, w.Shutdown(f.ctx))
	store := &acknowledgedLateStore{Store: f.store}
	cfg := f.cfg
	cfg.Store = store
	service, err := mg.New(cfg)
	require.NoError(t, err)
	request := mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"确认丢失"`)}
	store.lose.Store(true)
	_, err = service.SubmitTask(f.ctx, f.actor, request)
	require.Error(t, err)
	accepted, err := service.SubmitTask(f.ctx, f.actor, request)
	require.NoError(t, err)
	page, err := service.ListTasks(f.ctx, f.actor, mg.Query{SessionID: se.ID})
	require.NoError(t, err)
	require.Len(t, page.Items, 1)
	require.Equal(t, accepted.ID, page.Items[0].ID)
	docs, err := f.store.List(f.ctx, "commands", mg.Selection{TenantID: f.actor.TenantID, Query: mg.Query{TaskID: accepted.ID}, Visibility: mg.Visibility{All: true}})
	require.NoError(t, err)
	require.Len(t, docs, 1)
}
func TestRetentionKeepsLiveKeysAndUnconfirmedDependencies(t *testing.T) {
	f := setup(t, nil)
	w := f.worker(t)
	se := f.session(t)
	request := mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"保留期"`)}
	task, err := f.s.SubmitTask(f.ctx, f.actor, request)
	require.NoError(t, err)
	require.Eventually(t, func() bool { v, e := f.s.GetTask(f.ctx, f.actor, task.ID); return e == nil && v.State == mg.Succeeded }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, w.Shutdown(f.ctx))
	cfg := f.cfg
	cfg.Limits = mg.DefaultLimits()
	cfg.Limits.TaskRetention = time.Hour
	future := time.Now().Add(2 * time.Hour)
	cfg.Now = func() time.Time { return future }
	service, err := mg.New(cfg)
	require.NoError(t, err)
	counts, err := service.Prune(f.ctx, f.actor)
	require.NoError(t, err)
	require.Zero(t, counts["tasks"], "未确认outbox必须阻止清理")
	_, err = f.pool.Exec(f.ctx, `UPDATE mg_outbox SET revision=revision+1,data=jsonb_set(jsonb_set(data,'{state}','"done"'),'{revision}',to_jsonb(revision+1)) WHERE tenant_id=$1`, f.actor.TenantID)
	require.NoError(t, err)
	counts, err = service.Prune(f.ctx, f.actor)
	require.NoError(t, err)
	require.Equal(t, 1, counts["tasks"])
	_, err = service.GetTask(f.ctx, f.actor, task.ID)
	require.ErrorIs(t, err, mg.ErrNotFound)
	same, err := service.SubmitTask(f.ctx, f.actor, request)
	require.NoError(t, err)
	require.Equal(t, task.ID, same.ID, "清理结果后仍有效的幂等键必须复用原响应")
	// 过期键可以重新接纳；仍在执行的资源不得被Prune删除。
	future = future.Add(31 * 24 * time.Hour)
	next, err := service.SubmitTask(f.ctx, f.actor, request)
	require.NoError(t, err)
	require.NotEqual(t, task.ID, next.ID)
	future = future.Add(100 * 24 * time.Hour)
	_, err = service.Prune(f.ctx, f.actor)
	require.NoError(t, err)
	current, err := service.GetTask(f.ctx, f.actor, next.ID)
	require.NoError(t, err)
	require.Equal(t, mg.Queued, current.State)
}

func TestInvalidRetentionAndTypedNilTracerAreRejected(t *testing.T) {
	f := setup(t, nil)
	for _, name := range []string{"task_zero", "task_negative", "audit_zero", "audit_negative", "nil_tracer"} {
		t.Run(name, func(t *testing.T) {
			cfg := f.cfg
			cfg.Limits = mg.DefaultLimits()
			switch name {
			case "task_zero":
				cfg.Limits.TaskRetention = 0
			case "task_negative":
				cfg.Limits.TaskRetention = -time.Second
			case "audit_zero":
				cfg.Limits.AuditRetention = 0
			case "audit_negative":
				cfg.Limits.AuditRetention = -time.Second
			case "nil_tracer":
				var tracer *nilTracer
				cfg.Tracer = tracer
			}
			_, err := mg.New(cfg)
			require.ErrorIs(t, err, mg.ErrInvalid)
		})
	}
}

type nilTracer struct{ trace.Tracer }
