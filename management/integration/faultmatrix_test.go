package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	mg "github.com/kart-io/goagent-contrib/management"
	"github.com/kart-io/goagent/constants"
	"github.com/kart-io/goagent/core/durable"
	"github.com/kart-io/goagent/core/session"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type crashBoundaryStore struct {
	session.Store
	stage string
}

func (s crashBoundaryStore) Append(ctx context.Context, id string, expected int64, events ...session.Event) (int64, error) {
	matched := false
	for _, event := range events {
		if s.stage == "create_log" && event.Type == "management/session" {
			matched = true
		}
		if s.stage == "turn_end" && event.Type == constants.SessionEventTurnEnd {
			matched = true
		}
		if event.Type == constants.SessionEventBoundarySnapshot {
			var boundary session.BoundaryData
			var snapshot durable.Snapshot
			if json.Unmarshal(event.Data, &boundary) != nil || json.Unmarshal(boundary.Snapshot, &snapshot) != nil {
				continue
			}
			var record durable.OperationRecord
			if json.Unmarshal(snapshot.Data, &record) == nil && strings.HasPrefix(record.ID, "interrupt/management/approval/") && record.Status == "completed" {
				if s.stage == "answer_before" {
					os.Exit(42)
				}
				if s.stage == "answer_after" {
					matched = true
				}
			}
			if s.stage == "outcome" && snapshot.Version == "management/outcome/v1" {
				var envelope struct{ Result mg.RunResult }
				if json.Unmarshal(snapshot.Data, &envelope) == nil && envelope.Result.Outcome == "completed" {
					matched = true
				}
			}
		}
	}
	seq, err := s.Store.Append(ctx, id, expected, events...)
	if err == nil && matched {
		os.Exit(42)
	}
	return seq, err
}

type crashManagementStore struct {
	mg.Store
	stage string
}
type crashManagementTx struct {
	mg.Tx
	stage     string
	completed *bool
}

func (t crashManagementTx) CAS(ctx context.Context, d mg.Document, revision int64) error {
	if d.Kind == "tasks" {
		var task mg.Task
		if json.Unmarshal(d.Data, &task) == nil && task.State == mg.Succeeded {
			if t.stage == "sql_before" {
				os.Exit(42)
			}
			*t.completed = true
		}
	}
	return t.Tx.CAS(ctx, d, revision)
}
func (s crashManagementStore) WithinTx(ctx context.Context, fn func(mg.Tx) error) error {
	completed := false
	err := s.Store.WithinTx(ctx, func(tx mg.Tx) error { return fn(crashManagementTx{Tx: tx, stage: s.stage, completed: &completed}) })
	if err == nil && completed && s.stage == "sql_after" {
		os.Exit(42)
	}
	return err
}

type faultProcess struct {
	cmd     *exec.Cmd
	running bool
}

func startFaultProcess(t *testing.T, f *fixture, stage string, extra ...string) *faultProcess {
	t.Helper()
	binary, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.Command(binary, "-test.run=^TestRecoveryChild$", "-test.timeout=40s")
	cmd.Env = append(os.Environ(), "MG_RECOVERY_TENANT="+f.actor.TenantID, "MG_RECOVERY_SCHEMA="+f.schema, "MG_FAULT_STAGE="+stage)
	cmd.Env = append(cmd.Env, extra...)
	// 子进程诊断保留到父用例输出，失败时可定位具体断点。
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	child := &faultProcess{cmd: cmd, running: true}
	t.Cleanup(func() {
		if child.running {
			_ = child.cmd.Process.Kill()
			_ = child.cmd.Wait()
			child.running = false
		}
	})
	return child
}
func waitFaultProcess(t *testing.T, child *faultProcess, exit int) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- child.cmd.Wait() }()
	select {
	case err := <-done:
		child.running = false
		if exit == 0 {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	case <-time.After(15 * time.Second):
		_ = child.cmd.Process.Kill()
		<-done
		child.running = false
		t.Fatal("故障断点未在期限内触发")
	}
	require.Equal(t, exit, child.cmd.ProcessState.ExitCode())
}
func recoverFaultOwner(t *testing.T, f *fixture, child *faultProcess, slot string) {
	t.Helper()
	require.False(t, child.running)
	document, err := f.store.Get(f.ctx, "workers", "__system", slot)
	if errors.Is(err, mg.ErrNotFound) {
		return
	}
	require.NoError(t, err)
	var owner mg.WorkerOwner
	require.NoError(t, json.Unmarshal(document.Data, &owner))
	if owner.OwnerID == "" {
		return
	}
	cfg := f.cfg
	cfg.ConfirmWorkerStopped = func(context.Context, string, int64) error {
		if child.running {
			return mg.ErrBusy
		}
		return nil
	}
	service, err := mg.New(cfg)
	require.NoError(t, err)
	if slot == "outbox" {
		err = service.RecoverOutboxSlot(f.ctx, f.actor, owner.OwnerID, owner.Generation, "已等待旧子进程退出")
	} else {
		err = service.RecoverWorkerSlot(f.ctx, f.actor, owner.OwnerID, owner.Generation, "已等待旧子进程退出")
	}
	require.NoError(t, err)
}
func TestRestartAtEveryCommitBoundary(t *testing.T) {
	for _, stage := range []string{"admission", "create_log", "answer_before", "answer_after", "turn_end", "outcome", "sql_before", "sql_after", "effect_after_apply", "effect_after_apply_cancel"} {
		t.Run(stage, func(t *testing.T) {
			f := setup(t, nil)
			t.Cleanup(func() {
				if t.Failed() {
					owner, err := f.store.Get(f.ctx, "workers", "__system", "default")
					t.Logf("失败后的worker记录=%s，查询错误=%v", owner.Data, err)
				}
			})
			_, err := f.pool.Exec(f.ctx, `CREATE TABLE mg_case_counters(tenant text,kind text,n integer,PRIMARY KEY(tenant,kind))`)
			require.NoError(t, err)
			agent, _ := f.agent(t)
			se, err := f.s.CreateSession(f.ctx, f.actor, mg.CreateSessionRequest{IdempotencyKey: uuid.NewString(), AgentID: agent.ID})
			require.NoError(t, err)
			initialStage := stage
			if stage == "effect_after_apply_cancel" {
				initialStage = "effect_after_apply"
			}
			if stage == "admission" {
				initialStage = ""
			}
			child := startFaultProcess(t, f, initialStage)
			if stage == "create_log" {
				waitFaultProcess(t, child, 42)
				current, e := f.s.GetSession(f.ctx, f.actor, se.ID)
				require.NoError(t, e)
				require.Equal(t, "preparing", current.Status)
			} else {
				require.Eventually(t, func() bool {
					current, e := f.s.GetSession(f.ctx, f.actor, se.ID)
					return e == nil && current.Status == "ready"
				}, 8*time.Second, 10*time.Millisecond)
			}
			var task mg.Task
			if stage == "admission" {
				require.NoError(t, child.cmd.Process.Signal(os.Interrupt))
				waitFaultProcess(t, child, 0)
				key := uuid.NewString()
				child = startFaultProcess(t, f, stage, "MG_BOOTSTRAP_SESSION="+se.ID, "MG_BOOTSTRAP_KEY="+key)
				waitFaultProcess(t, child, 42)
				task, err = f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: key, SessionID: se.ID, Input: json.RawMessage(`"断点退款"`)})
				require.NoError(t, err)
			} else if stage != "create_log" {
				task, err = f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"断点退款"`)})
				require.NoError(t, err)
				ap := pendingApproval(t, f, task.ID)
				_, err = f.s.DecideApproval(f.ctx, f.actor, mg.DecisionRequest{IdempotencyKey: uuid.NewString(), ApprovalID: ap.ID, ExpectedRevision: ap.Revision, Decision: "approve"})
				require.NoError(t, err)
				waitFaultProcess(t, child, 42)
			}
			if stage == "effect_after_apply_cancel" {
				current, e := f.s.GetTask(f.ctx, f.actor, task.ID)
				require.NoError(t, e)
				_, e = f.s.CancelTask(f.ctx, f.actor, mg.CancelTaskRequest{IdempotencyKey: uuid.NewString(), TaskID: task.ID, ExpectedRevision: current.Revision, Reason: "中断后取消"})
				require.NoError(t, e)
			}
			recoverFaultOwner(t, f, child, "default")
			resumed := startFaultProcess(t, f, "")
			require.Eventually(t, func() bool {
				doc, e := f.store.Get(f.ctx, "workers", "__system", "default")
				var owner mg.WorkerOwner
				return e == nil && json.Unmarshal(doc.Data, &owner) == nil && owner.OwnerID != ""
			}, 5*time.Second, 10*time.Millisecond)
			if stage == "create_log" {
				require.Eventually(t, func() bool {
					current, e := f.s.GetSession(f.ctx, f.actor, se.ID)
					return e == nil && current.Status == "ready"
				}, 10*time.Second, 10*time.Millisecond)
				events, e := f.cfg.Sessions.Read(f.ctx, se.LogID, 0)
				require.NoError(t, e)
				require.Len(t, events, 1, "创建断点恢复不能重复写来源标记")
			} else {
				if stage == "admission" {
					ap := pendingApproval(t, f, task.ID)
					_, err = f.s.DecideApproval(f.ctx, f.actor, mg.DecisionRequest{IdempotencyKey: uuid.NewString(), ApprovalID: ap.ID, ExpectedRevision: ap.Revision, Decision: "approve"})
					require.NoError(t, err)
				}
				want := mg.Succeeded
				if strings.HasPrefix(stage, "effect_after_apply") {
					want = mg.Attention
				}
				if !assert.Eventually(t, func() bool {
					current, e := f.s.GetTask(f.ctx, f.actor, task.ID)
					return e == nil && current.State == want
				}, 12*time.Second, 10*time.Millisecond) {
					current, e := f.s.GetTask(f.ctx, f.actor, task.ID)
					t.Logf("恢复状态=%+v，查询错误=%v", current, e)
					t.FailNow()
				}
			}
			require.NoError(t, resumed.cmd.Process.Signal(os.Interrupt))
			waitFaultProcess(t, resumed, 0)
			if stage != "create_log" {
				var model, effect int
				require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT n FROM mg_case_counters WHERE tenant=$1 AND kind='model'`, f.actor.TenantID).Scan(&model))
				require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT n FROM mg_case_counters WHERE tenant=$1 AND kind='effect'`, f.actor.TenantID).Scan(&effect))
				require.Equal(t, 1, effect)
				expectedModels := 2
				if strings.HasPrefix(stage, "effect_after_apply") {
					expectedModels = 1
				}
				require.Equal(t, expectedModels, model)
			}
		})
	}
}

type crashOutboxSink struct {
	f     *fixture
	crash bool
}

func (s crashOutboxSink) Accept(ctx context.Context, d mg.Delivery) error {
	_, err := s.f.pool.Exec(ctx, `INSERT INTO mg_remote_delivery(id,calls) VALUES($1,1) ON CONFLICT(id) DO UPDATE SET calls=mg_remote_delivery.calls+1`, d.ID)
	if err == nil && s.crash {
		os.Exit(42)
	}
	return err
}
func TestOutboxCrashAfterRemoteAcceptanceKeepsStableDelivery(t *testing.T) {
	f := setup(t, nil)
	w := f.worker(t)
	se := f.session(t)
	_, err := f.pool.Exec(f.ctx, `CREATE TABLE mg_remote_delivery(id text PRIMARY KEY,calls integer)`)
	require.NoError(t, err)
	task, err := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"通知断点"`)})
	require.NoError(t, err)
	require.Eventually(t, func() bool { v, e := f.s.GetTask(f.ctx, f.actor, task.ID); return e == nil && v.State == mg.Succeeded }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, w.Shutdown(f.ctx))
	child := startFaultProcess(t, f, "outbox")
	waitFaultProcess(t, child, 42)
	recoverFaultOwner(t, f, child, "outbox")
	resumed := startFaultProcess(t, f, "outbox_resume")
	require.Eventually(t, func() bool {
		docs, e := f.store.List(f.ctx, "outbox", mg.Selection{TenantID: f.actor.TenantID, Visibility: mg.Visibility{All: true}})
		if e != nil || len(docs) != 1 {
			return false
		}
		var d mg.Delivery
		return json.Unmarshal(docs[0].Data, &d) == nil && d.State == "done"
	}, 8*time.Second, 10*time.Millisecond)
	require.NoError(t, resumed.cmd.Process.Signal(os.Interrupt))
	waitFaultProcess(t, resumed, 0)
	var records, calls int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*),sum(calls) FROM mg_remote_delivery`).Scan(&records, &calls))
	require.Equal(t, 1, records)
	require.Equal(t, 2, calls)
	run, err := f.s.GetRun(f.ctx, f.actor, task.ID)
	require.NoError(t, err)
	require.Len(t, run.Attempts, 1)
}
