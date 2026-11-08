package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kart-io/goagent/codec/json"
	"github.com/kart-io/goagent/constants"
	"github.com/kart-io/goagent/core/durable"
	"github.com/kart-io/goagent/core/session"
	agentErrors "github.com/kart-io/goagent/errors"
	mg "github.com/kart-io/goagent/management"
	"github.com/kart-io/goagent/management/adapters"
	"github.com/kart-io/goagent/workflow"
	"github.com/stretchr/testify/require"
)

func TestExpandedForkRejectsDisabledAgent(t *testing.T) {
	f := setup(t, nil)
	w := f.worker(t)
	source := f.session(t)
	require.NoError(t, w.Shutdown(f.ctx))
	start, err := session.NewEvent(constants.SessionEventTurnStart, session.TurnStart{Input: "已导入的完整会话"})
	require.NoError(t, err)
	end, err := session.NewEvent(constants.SessionEventTurnEnd, session.TurnEnd{Answer: "已完成"})
	require.NoError(t, err)
	events, err := f.cfg.Sessions.Read(f.ctx, source.LogID, 0)
	require.NoError(t, err)
	_, err = f.cfg.Sessions.Append(f.ctx, source.LogID, session.LastSeq(events), start, end)
	require.NoError(t, err)
	agent, err := f.s.GetAgent(f.ctx, f.actor, source.AgentID)
	require.NoError(t, err)
	agent, err = f.s.SetAgentEnabled(f.ctx, f.actor, mg.SetEnabledRequest{IdempotencyKey: uuid.NewString(), AgentID: agent.ID, ExpectedRevision: agent.Revision, Enabled: false})
	require.NoError(t, err)
	request := mg.ForkSessionRequest{IdempotencyKey: uuid.NewString(), SessionID: source.ID, ExpectedRevision: source.Revision}
	_, err = f.s.ForkSession(f.ctx, f.actor, request)
	require.ErrorIs(t, err, agentErrors.ErrManagementConflict, "停用Agent不得通过Fork创建新Session")
	page, err := f.s.ListSessions(f.ctx, f.actor, mg.Query{AgentID: agent.ID})
	require.NoError(t, err)
	require.Len(t, page.Items, 1, "拒绝的Fork不得留下子会话")
	agent, err = f.s.SetAgentEnabled(f.ctx, f.actor, mg.SetEnabledRequest{IdempotencyKey: uuid.NewString(), AgentID: agent.ID, ExpectedRevision: agent.Revision, Enabled: true})
	require.NoError(t, err)
	branch, err := f.s.ForkSession(f.ctx, f.actor, request)
	require.NoError(t, err, "失败请求不得消耗幂等键，启用后可重试")
	f.worker(t)
	require.Eventually(t, func() bool {
		current, e := f.s.GetSession(f.ctx, f.actor, branch.ID)
		return e == nil && current.Status == "ready"
	}, 5*time.Second, 10*time.Millisecond)
	_, err = f.s.SetAgentEnabled(f.ctx, f.actor, mg.SetEnabledRequest{IdempotencyKey: uuid.NewString(), AgentID: agent.ID, ExpectedRevision: agent.Revision, Enabled: false})
	require.NoError(t, err)
	replayed, err := f.s.ForkSession(f.ctx, f.actor, request)
	require.NoError(t, err, "已成功接纳的幂等重投不属于停用后新建会话")
	require.Equal(t, branch, replayed, "应复用原始preparing响应，而不是另建或返回当前会话状态")
	page, err = f.s.ListSessions(f.ctx, f.actor, mg.Query{AgentID: agent.ID})
	require.NoError(t, err)
	require.Len(t, page.Items, 2)
}

func TestExpandedTenantIdempotencyKeysAreIndependent(t *testing.T) {
	f := setup(t, nil)
	other := mg.Actor{TenantID: uuid.NewString(), Subject: "另一个租户"}
	request := mg.RegisterAgentRequest{IdempotencyKey: "同一个业务请求键", Name: "租户自己的客服"}
	first, err := f.s.RegisterAgent(f.ctx, f.actor, request)
	require.NoError(t, err)
	second, err := f.s.RegisterAgent(f.ctx, other, request)
	require.NoError(t, err)
	require.NotEqual(t, first.ID, second.ID)
	for _, row := range []struct {
		actor mg.Actor
		id    string
	}{{f.actor, first.ID}, {other, second.ID}} {
		repeated, err := f.s.RegisterAgent(f.ctx, row.actor, request)
		require.NoError(t, err)
		require.Equal(t, row.id, repeated.ID)
		page, err := f.s.ListAgents(f.ctx, row.actor, mg.Query{})
		require.NoError(t, err)
		require.Len(t, page.Items, 1)
		require.Equal(t, row.id, page.Items[0].ID)
	}
	_, err = f.s.GetAgent(f.ctx, other, first.ID)
	require.ErrorIs(t, err, agentErrors.ErrManagementNotFound)
}

func TestExpandedQueueCapacityRecoversAfterQueuedCancel(t *testing.T) {
	f := setup(t, nil)
	w := f.worker(t)
	se := f.session(t)
	require.NoError(t, w.Shutdown(f.ctx))
	cfg := f.cfg
	cfg.Limits = mg.DefaultLimits()
	cfg.Limits.MaxPending = 1
	service, err := mg.New(cfg)
	require.NoError(t, err)
	f.s = service
	first, err := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"先排队"`)})
	require.NoError(t, err)
	request := mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"取消释放容量后执行"`)}
	_, err = f.s.SubmitTask(f.ctx, f.actor, request)
	require.ErrorIs(t, err, agentErrors.ErrManagementBusy)
	page, err := f.s.ListTasks(f.ctx, f.actor, mg.Query{SessionID: se.ID})
	require.NoError(t, err)
	require.Len(t, page.Items, 1, "容量拒绝不得留下未确认任务")
	_, err = f.s.CancelTask(f.ctx, f.actor, mg.CancelTaskRequest{IdempotencyKey: uuid.NewString(), TaskID: first.ID, ExpectedRevision: first.Revision})
	require.NoError(t, err)
	next, err := f.s.SubmitTask(f.ctx, f.actor, request)
	require.NoError(t, err, "释放容量后可用原请求键重试")
	f.worker(t)
	require.Eventually(t, func() bool {
		current, e := f.s.GetTask(f.ctx, f.actor, next.ID)
		return e == nil && current.State == mg.Succeeded
	}, 5*time.Second, 10*time.Millisecond)
	canceled, err := f.s.GetRun(f.ctx, f.actor, first.ID)
	require.NoError(t, err)
	require.Equal(t, mg.Canceled, canceled.Task.State)
	require.Empty(t, canceled.Attempts, "排队取消不得构建执行尝试")
	completed, err := f.s.GetRun(f.ctx, f.actor, next.ID)
	require.NoError(t, err)
	require.Len(t, completed.Attempts, 1)
}

func TestExpandedInputResumeAfterHostRebuild(t *testing.T) {
	f := setup(t, nil)
	_, err := f.pool.Exec(f.ctx, `CREATE TABLE input_case_effects(task_id text PRIMARY KEY,amount integer NOT NULL)`)
	require.NoError(t, err)
	inputFactory := adapters.GraphFactory[int]{
		Check: func(context.Context, mg.Version) error { return nil },
		Initial: func(raw json.RawMessage) (value int, err error) {
			err = json.Unmarshal(raw, &value)
			return
		},
		BuildGraph: func(ctx context.Context, version mg.Version, resources mg.Resources) (*workflow.DurableGraph[int], func(context.Context) error, error) {
			graph := workflow.NewGraph[int]()
			if err := graph.AddNode("collect", func(ctx context.Context, current int) (int, error) {
				raw, err := durable.AwaitInput(ctx, "amount", json.RawMessage(`{"question":"请输入正整数金额"}`), func(raw json.RawMessage) error {
					var amount int
					if json.Unmarshal(raw, &amount) != nil || amount <= 0 {
						return agentErrors.Newf(agentErrors.CodeInvalidInput, "金额必须为正整数")
					}
					return nil
				})
				if err != nil {
					return current, err
				}
				var amount int
				err = json.Unmarshal(raw, &amount)
				return amount, err
			}); err != nil {
				return nil, nil, err
			}
			if err := graph.AddNode("apply", func(ctx context.Context, amount int) (int, error) {
				_, err := f.pool.Exec(ctx, `INSERT INTO input_case_effects VALUES($1,$2)`, resources.Task.ID, amount)
				return amount, err
			}); err != nil {
				return nil, nil, err
			}
			for _, edge := range [][2]string{{constants.WorkflowStart, "collect"}, {"collect", "apply"}, {"apply", constants.WorkflowEnd}} {
				if err := graph.AddEdge(edge[0], edge[1]); err != nil {
					return nil, nil, err
				}
			}
			result, err := graph.CompileDurable(ctx, workflow.DurableConfig{Store: resources.Journal, Version: version.ConfigHash})
			return result, nil, err
		},
	}
	cfg := f.cfg
	cfg.Factories = map[string]mg.Factory{"test": inputFactory}
	f.s, err = mg.New(cfg)
	require.NoError(t, err)
	w := f.worker(t)
	se := f.session(t)
	task, err := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`0`)})
	require.NoError(t, err)
	var waiting mg.Task
	require.Eventually(t, func() bool {
		waiting, err = f.s.GetTask(f.ctx, f.actor, task.ID)
		return err == nil && waiting.State == mg.WaitingInput && len(waiting.Waits) == 1
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, "input", waiting.Waits[0].Kind)
	approvals, err := f.s.ListApprovals(f.ctx, f.actor, mg.Query{TaskID: task.ID})
	require.NoError(t, err)
	require.Empty(t, approvals.Items, "普通输入等待不应变成审批")
	require.NoError(t, w.Shutdown(f.ctx))
	f.s, err = mg.New(cfg)
	require.NoError(t, err)
	f.worker(t)
	_, err = f.s.ResumeTask(f.ctx, f.actor, mg.ResumeTaskRequest{IdempotencyKey: uuid.NewString(), TaskID: task.ID, ExpectedRevision: waiting.Revision, WaitID: "伪造等待点", Input: json.RawMessage(`42`)})
	require.ErrorIs(t, err, agentErrors.ErrManagementInvalid)
	_, err = f.s.ResumeTask(f.ctx, f.actor, mg.ResumeTaskRequest{IdempotencyKey: uuid.NewString(), TaskID: task.ID, ExpectedRevision: waiting.Revision, WaitID: waiting.Waits[0].ID, Input: json.RawMessage(`-1`)})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		waiting, err = f.s.GetTask(f.ctx, f.actor, task.ID)
		return err == nil && waiting.State == mg.WaitingInput && len(waiting.Waits) == 1 && waiting.Waits[0].Rejected
	}, 5*time.Second, 10*time.Millisecond)
	require.NotEmpty(t, waiting.Waits[0].Reason)
	var before int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM input_case_effects`).Scan(&before))
	require.Zero(t, before)
	request := mg.ResumeTaskRequest{IdempotencyKey: uuid.NewString(), TaskID: task.ID, ExpectedRevision: waiting.Revision, WaitID: waiting.Waits[0].ID, Input: json.RawMessage(`42`)}
	command, err := f.s.ResumeTask(f.ctx, f.actor, request)
	require.NoError(t, err)
	repeated, err := f.s.ResumeTask(f.ctx, f.actor, request)
	require.NoError(t, err)
	require.Equal(t, command.ID, repeated.ID)
	require.Eventually(t, func() bool {
		current, e := f.s.GetTask(f.ctx, f.actor, task.ID)
		return e == nil && current.State == mg.Succeeded && string(current.Result) == "42"
	}, 5*time.Second, 10*time.Millisecond)
	var count, amount int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*),sum(amount) FROM input_case_effects WHERE task_id=$1`, task.ID).Scan(&count, &amount))
	require.Equal(t, 1, count)
	require.Equal(t, 42, amount)
	run, err := f.s.GetRun(f.ctx, f.actor, task.ID)
	require.NoError(t, err)
	require.Len(t, run.Attempts, 3, "初次等待、拒绝输入和有效续跑各一次，幂等重投不新增尝试")
	_, err = f.s.ResumeTask(f.ctx, f.actor, mg.ResumeTaskRequest{IdempotencyKey: uuid.NewString(), TaskID: task.ID, ExpectedRevision: run.Task.Revision, WaitID: request.WaitID, Input: json.RawMessage(`99`)})
	require.ErrorIs(t, err, agentErrors.ErrManagementConflict)
}
