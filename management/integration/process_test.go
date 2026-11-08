package integration_test

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	mg "github.com/kart-io/goagent-contrib/management"
	"github.com/kart-io/goagent-contrib/management/adapters"
	"github.com/kart-io/goagent/builder"
	"github.com/kart-io/goagent/interfaces"
	"github.com/kart-io/goagent/llm"
	"github.com/kart-io/goagent/tools"
	"github.com/stretchr/testify/require"
	"os"
	"os/exec"
	"os/signal"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type persistentModel struct {
	*scriptModel
	pool   *pgxpool.Pool
	tenant string
}

func (m persistentModel) CompleteToolsStream(ctx context.Context, r *llm.CompletionRequest, ts []interfaces.Tool) (<-chan *llm.StreamChunk, error) {
	if _, e := m.pool.Exec(ctx, `INSERT INTO mg_case_counters VALUES($1,'model',1) ON CONFLICT(tenant,kind) DO UPDATE SET n=mg_case_counters.n+1`, m.tenant); e != nil {
		return nil, e
	}
	return m.scriptModel.CompleteToolsStream(ctx, r, ts)
}
func processFactory(pool *pgxpool.Pool, tenant string) mg.Factory {
	return adapters.NativeFactory{Check: func(context.Context, mg.Version) error { return nil }, BuildAgent: func(ctx context.Context, v mg.Version, r mg.Resources, ap tools.Approver) (*builder.SimpleAgentBuilder, func(context.Context) error, error) {
		reg := tools.NewRegistry()
		e := reg.Register(tools.NewBaseTool("refund", "退款", `{"type":"object"}`, func(ctx context.Context, in *interfaces.ToolInput) (*interfaces.ToolOutput, error) {
			_, e := pool.Exec(ctx, `INSERT INTO mg_case_counters VALUES($1,'effect',1) ON CONFLICT(tenant,kind) DO UPDATE SET n=mg_case_counters.n+1`, tenant)
			return &interfaces.ToolOutput{Success: e == nil, Result: "退款完成"}, e
		}))
		if e != nil {
			return nil, nil, e
		}
		d, e := tools.NewDispatcher(reg, tools.DispatchConfig{Pipeline: &tools.Pipeline{Pre: []tools.PreHook{func(context.Context, *tools.Call) (tools.PreDecision, error) {
			return tools.PreDecision{Kind: tools.PreAsk}, nil
		}}, Approver: ap}})
		if e != nil {
			return nil, nil, e
		}
		var requests atomic.Int32
		model := persistentModel{&scriptModel{requests: &requests, version: v.Version}, pool, tenant}
		return builder.NewSimpleBuilder(model).WithTools(d.Tools()...).WithMaxIterations(4), nil, nil
	}}
}
func TestRecoveryChild(t *testing.T) {
	tenant := os.Getenv("MG_RECOVERY_TENANT")
	if tenant == "" {
		return
	}
	f := setup(t, nil)
	cfg := f.cfg
	cfg.Factories = map[string]mg.Factory{"test": processFactory(f.pool, tenant)}
	s, e := mg.New(cfg)
	require.NoError(t, e)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	require.NoError(t, mg.NewWorker(s).Run(ctx))
}
func TestApprovalSurvivesProcessKillAndRestart(t *testing.T) {
	f := setup(t, nil)
	_, e := f.pool.Exec(f.ctx, `CREATE TABLE IF NOT EXISTS mg_case_counters(tenant text,kind text,n integer,PRIMARY KEY(tenant,kind))`)
	require.NoError(t, e)
	a, _ := f.agent(t)
	se, e := f.s.CreateSession(f.ctx, f.actor, mg.CreateSessionRequest{IdempotencyKey: uuid.NewString(), AgentID: a.ID})
	require.NoError(t, e)
	binary, e := os.Executable()
	require.NoError(t, e)
	start := func() *exec.Cmd {
		cmd := exec.Command(binary, "-test.run=^TestRecoveryChild$", "-test.timeout=40s")
		cmd.Env = append(os.Environ(), "MG_RECOVERY_TENANT="+f.actor.TenantID, "MG_RECOVERY_SCHEMA="+f.schema)
		require.NoError(t, cmd.Start())
		t.Cleanup(func() {
			if cmd.ProcessState == nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}
		})
		return cmd
	}
	child := start()
	require.Eventually(t, func() bool { v, e := f.s.GetSession(f.ctx, f.actor, se.ID); return e == nil && v.Status == "ready" }, 8*time.Second, 20*time.Millisecond)
	task, e := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"跨进程审核退款"`)})
	require.NoError(t, e)
	var ap mg.Approval
	require.Eventually(t, func() bool {
		p, e := f.s.ListApprovals(f.ctx, f.actor, mg.Query{TaskID: task.ID, Statuses: []string{"pending"}})
		if e == nil && len(p.Items) == 1 {
			ap = p.Items[0]
			return true
		}
		return false
	}, 8*time.Second, 20*time.Millisecond)
	require.NoError(t, child.Process.Kill())
	require.Error(t, child.Wait())
	cfg := f.cfg
	cfg.ConfirmWorkerStopped = func(context.Context, string, int64) error {
		if child.ProcessState == nil {
			return mg.ErrBusy
		}
		return nil
	}
	f.s, e = mg.New(cfg)
	require.NoError(t, e)
	doc, e := f.store.Get(f.ctx, "workers", "__system", "default")
	require.NoError(t, e)
	var owner mg.WorkerOwner
	require.NoError(t, json.Unmarshal(doc.Data, &owner))
	require.NoError(t, f.s.RecoverWorkerSlot(f.ctx, f.actor, owner.OwnerID, owner.Generation, "已回收被终止的旧进程"))
	_, e = f.s.DecideApproval(f.ctx, f.actor, mg.DecisionRequest{IdempotencyKey: uuid.NewString(), ApprovalID: ap.ID, ExpectedRevision: ap.Revision, Decision: "approve", Reason: "宕机后批准"})
	require.NoError(t, e)
	next := start()
	require.Eventually(t, func() bool { v, e := f.s.GetTask(f.ctx, f.actor, task.ID); return e == nil && v.State == mg.Succeeded }, 12*time.Second, 20*time.Millisecond)
	require.NoError(t, next.Process.Signal(os.Interrupt))
	require.NoError(t, next.Wait())
	var model, effect int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT n FROM mg_case_counters WHERE tenant=$1 AND kind='model'`, f.actor.TenantID).Scan(&model))
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT n FROM mg_case_counters WHERE tenant=$1 AND kind='effect'`, f.actor.TenantID).Scan(&effect))
	require.Equal(t, 2, model, "恢复不重复请求已完成的模型步骤")
	require.Equal(t, 1, effect, "恢复只执行一次退款")
}
