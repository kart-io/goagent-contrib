package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/kart-io/goagent/constants"
	"github.com/kart-io/goagent/core/session"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	mg "github.com/kart-io/goagent-contrib/management"
	"github.com/stretchr/testify/require"
)

// TestAuditRunQueryIsReadOnly 核对查询未启动任务时不能改写会话日志。
func TestAuditRunQueryIsReadOnly(t *testing.T) {
	f := setup(t, nil)
	w := f.worker(t)
	se := f.session(t)
	stop, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	require.NoError(t, w.Shutdown(stop))
	task, err := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{
		IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"未启动"`),
	})
	require.NoError(t, err)
	before, err := f.cfg.Sessions.Read(f.ctx, se.LogID, 0)
	require.NoError(t, err)
	run, err := f.s.GetRun(f.ctx, f.actor, task.ID)
	require.NoError(t, err)
	after, err := f.cfg.Sessions.Read(f.ctx, se.LogID, 0)
	require.NoError(t, err)
	t.Logf("任务状态=%s，查询前事件=%d，查询后事件=%d，complete=%t，usage存在=%t", run.Task.State, len(before), len(after), run.Complete, run.Usage != nil)
	require.Equal(t, before, after, "运行查询不应创建或改写预算日志")
}

type unavailableSessionStore struct {
	session.Store
	err error
}

func (s unavailableSessionStore) Read(context.Context, string, int64) ([]session.Event, error) {
	return nil, s.err
}

func TestRunUsageMissingAndUnavailableStayUnknown(t *testing.T) {
	f := setup(t, nil)
	w := f.worker(t)
	se := f.session(t)
	require.NoError(t, w.Shutdown(f.ctx))
	task, err := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{
		IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"等待执行"`),
	})
	require.NoError(t, err)
	run, err := f.s.GetRun(f.ctx, f.actor, task.ID)
	require.NoError(t, err)
	require.Nil(t, run.Usage, "缺失账本不应生成零用量")
	require.False(t, run.Complete)
	raw, err := json.Marshal(run)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"usage_status":"missing"`)

	cfg := f.cfg
	cfg.Sessions = unavailableSessionStore{Store: f.cfg.Sessions, err: errors.New("数据库暂时不可读")}
	service, err := mg.New(cfg)
	require.NoError(t, err)
	run, err = service.GetRun(f.ctx, f.actor, task.ID)
	require.NoError(t, err, "管理结果可查询，用量故障单独标记")
	require.Nil(t, run.Usage)
	require.False(t, run.Complete)
	raw, err = json.Marshal(run)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"usage_status":"unavailable"`)
}

func completeApprovedTask(t *testing.T, f *fixture, sessionID string) mg.Task {
	t.Helper()
	task, err := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: sessionID, Input: json.RawMessage(`"申请退款"`)})
	require.NoError(t, err)
	var ap mg.Approval
	require.Eventually(t, func() bool {
		page, err := f.s.ListApprovals(f.ctx, f.actor, mg.Query{TaskID: task.ID})
		if err == nil && len(page.Items) == 1 {
			ap = page.Items[0]
			return true
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)
	_, err = f.s.DecideApproval(f.ctx, f.actor, mg.DecisionRequest{IdempotencyKey: uuid.NewString(), ApprovalID: ap.ID, ExpectedRevision: ap.Revision, Decision: "approve"})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		current, err := f.s.GetTask(f.ctx, f.actor, task.ID)
		if err == nil {
			task = current
		}
		return err == nil && current.State == mg.Succeeded
	}, 5*time.Second, 10*time.Millisecond)
	return task
}

func TestCompletedRunUsageQueriesDoNotWriteAndDoNotDoubleCount(t *testing.T) {
	var effects, requests atomic.Int32
	f := setup(t, map[string]mg.Factory{"test": nativeFactory(&effects, &requests)})
	w := f.worker(t)
	se := f.session(t)
	task := completeApprovedTask(t, f, se.ID)
	require.NoError(t, w.Shutdown(f.ctx))
	before, err := f.cfg.Sessions.Read(f.ctx, se.LogID, 0)
	require.NoError(t, err)
	for range 3 {
		run, err := f.s.GetRun(f.ctx, f.actor, task.ID)
		require.NoError(t, err)
		require.NotNil(t, run.Usage)
		require.Equal(t, 10, run.Usage.Root.KnownTokens.TotalTokens)
		page, err := f.s.ListRuns(f.ctx, f.actor, mg.Query{SessionID: se.ID})
		require.NoError(t, err)
		require.Len(t, page.Items, 1)
	}
	after, err := f.cfg.Sessions.Read(f.ctx, se.LogID, 0)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, int32(2), requests.Load())
	require.Equal(t, int32(1), effects.Load())

	// 损坏的计量证据必须保留为未知，不能静默生成新的零账本。
	var boundary session.BoundaryData
	found := false
	for _, event := range before {
		if event.Type != constants.SessionEventBoundarySnapshot {
			continue
		}
		var current session.BoundaryData
		require.NoError(t, json.Unmarshal(event.Data, &current))
		if strings.HasPrefix(current.ID, "goagent/budget/") {
			boundary = current
			found = true
		}
	}
	require.True(t, found)
	boundary.Snapshot = json.RawMessage(`{"version":"损坏格式","run_id":"wrong","data":{}}`)
	ev, err := session.NewEvent(constants.SessionEventBoundarySnapshot, boundary)
	require.NoError(t, err)
	unlock, err := f.cfg.Sessions.Lock(f.ctx, se.LogID, session.DurableLockName(boundary.ID))
	require.NoError(t, err)
	defer func() { require.NoError(t, unlock()) }()
	_, err = f.cfg.Sessions.Append(f.ctx, se.LogID, before[len(before)-1].Seq, ev)
	require.NoError(t, err)
	run, err := f.s.GetRun(f.ctx, f.actor, task.ID)
	require.NoError(t, err)
	require.Nil(t, run.Usage)
	require.False(t, run.Complete)
}
