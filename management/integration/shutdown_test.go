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
)

type lifecycleFactory struct {
	started chan struct{}
	release chan struct{}
	success bool
	calls   *atomic.Int32
}

func (lifecycleFactory) Validate(context.Context, mg.Version) (mg.Capabilities, error) {
	return mg.Capabilities{}, nil
}
func (f lifecycleFactory) Build(context.Context, mg.Version, mg.Resources) (mg.Binding, error) {
	return mg.Binding{Runtime: lifecycleRuntime{f}}, nil
}

type lifecycleRuntime struct{ f lifecycleFactory }

func (lifecycleRuntime) Inspect(context.Context, mg.RunSpec) (mg.RunEvidence, error) {
	return mg.RunEvidence{State: "absent"}, nil
}
func (r lifecycleRuntime) Run(ctx context.Context, _ mg.RunSpec) (mg.RunResult, error) {
	if r.f.calls != nil {
		r.f.calls.Add(1)
	}
	close(r.f.started)
	<-r.f.release
	if !r.f.success {
		return mg.RunResult{}, ctx.Err()
	}
	return mg.RunResult{Outcome: "completed", Output: json.RawMessage(`"真实成功"`)}, nil
}
func TestUncooperativeRuntimeRetainsOwnerUntilActualExit(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	f := setup(t, map[string]mg.Factory{"test": lifecycleFactory{started: started, release: release}})
	w := f.worker(t)
	se := f.session(t)
	task, err := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"不响应取消"`)})
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("未启动")
	}
	ctx, cancel := context.WithTimeout(f.ctx, 30*time.Millisecond)
	require.ErrorIs(t, w.Shutdown(ctx), context.DeadlineExceeded)
	cancel()
	require.ErrorIs(t, mg.NewWorker(f.s).Run(f.ctx), mg.ErrOwner)
	close(release)
	require.NoError(t, w.Shutdown(f.ctx))
	current, err := f.s.GetTask(f.ctx, f.actor, task.ID)
	require.NoError(t, err)
	require.Equal(t, mg.Queued, current.State)
	require.False(t, current.CancelRequested)
	require.True(t, current.RecoveryNeeded)
}
func TestCancelDisableShutdownCannotOverwriteSuccessfulResult(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	f := setup(t, map[string]mg.Factory{"test": lifecycleFactory{started: started, release: release, success: true}})
	w := f.worker(t)
	se := f.session(t)
	task, err := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"成功竞争"`)})
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("未启动")
	}
	current, err := f.s.GetTask(f.ctx, f.actor, task.ID)
	require.NoError(t, err)
	_, err = f.s.CancelTask(f.ctx, f.actor, mg.CancelTaskRequest{IdempotencyKey: uuid.NewString(), TaskID: task.ID, ExpectedRevision: current.Revision})
	require.NoError(t, err)
	agent, err := f.s.GetAgent(f.ctx, f.actor, se.AgentID)
	require.NoError(t, err)
	_, err = f.s.SetAgentEnabled(f.ctx, f.actor, mg.SetEnabledRequest{IdempotencyKey: uuid.NewString(), AgentID: agent.ID, ExpectedRevision: agent.Revision, Enabled: false})
	require.NoError(t, err)
	w.BeginShutdown()
	close(release)
	require.NoError(t, w.Shutdown(f.ctx))
	current, err = f.s.GetTask(f.ctx, f.actor, task.ID)
	require.NoError(t, err)
	require.Equal(t, mg.Succeeded, current.State)
	require.JSONEq(t, `"真实成功"`, string(current.Result))
}

type cleanupFactory struct {
	failBuild bool
	cleaned   *atomic.Int32
}

func (cleanupFactory) Validate(context.Context, mg.Version) (mg.Capabilities, error) {
	return mg.Capabilities{}, nil
}
func (f cleanupFactory) Build(context.Context, mg.Version, mg.Resources) (mg.Binding, error) {
	binding := mg.Binding{Runtime: runtime{}, Cleanup: func(context.Context) error { f.cleaned.Add(1); return errors.New("资源关闭失败") }}
	if f.failBuild {
		return binding, errors.New("部分构造失败")
	}
	return binding, nil
}
func TestCleanupFailureDoesNotReplaceOutcomeAndPartialBuildIsClosed(t *testing.T) {
	for _, failBuild := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "partial_build"}[failBuild], func(t *testing.T) {
			var cleaned atomic.Int32
			f := setup(t, map[string]mg.Factory{"test": cleanupFactory{failBuild: failBuild, cleaned: &cleaned}})
			reported := make(chan string, 4)
			cfg := f.cfg
			cfg.Report = func(kind string, _ error) { reported <- kind }
			var err error
			f.s, err = mg.New(cfg)
			require.NoError(t, err)
			f.worker(t)
			se := f.session(t)
			task, err := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"清理资源"`)})
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				current, e := f.s.GetTask(f.ctx, f.actor, task.ID)
				return e == nil && current.State.Terminal()
			}, 5*time.Second, 10*time.Millisecond)
			current, err := f.s.GetTask(f.ctx, f.actor, task.ID)
			require.NoError(t, err)
			want := mg.Succeeded
			if failBuild {
				want = mg.Failed
			}
			require.Equal(t, want, current.State)
			require.Equal(t, int32(1), cleaned.Load())
			select {
			case kind := <-reported:
				require.Equal(t, "cleanup", kind)
			case <-time.After(time.Second):
				t.Fatal("清理失败没有诊断")
			}
		})
	}
}
