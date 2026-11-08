package integration_test

import (
	"context"
	"github.com/google/uuid"
	"github.com/kart-io/goagent/builder"
	"github.com/kart-io/goagent/codec/json"
	"github.com/kart-io/goagent/constants"
	agentErrors "github.com/kart-io/goagent/errors"
	"github.com/kart-io/goagent/interfaces"
	"github.com/kart-io/goagent/llm"
	mg "github.com/kart-io/goagent/management"
	"github.com/kart-io/goagent/management/adapters"
	"github.com/kart-io/goagent/tools"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

type scriptModel struct {
	llm.Client
	requests *atomic.Int32
	version  string
}

func (m *scriptModel) Provider() constants.Provider { return constants.ProviderOpenAI }
func (m *scriptModel) CompleteToolsStream(_ context.Context, r *llm.CompletionRequest, _ []interfaces.Tool) (<-chan *llm.StreamChunk, error) {
	m.requests.Add(1)
	ch := make(chan *llm.StreamChunk, 1)
	finished := false
	for _, msg := range r.Messages {
		if msg.Role == "tool" {
			finished = true
		}
	}
	if finished {
		ch <- &llm.StreamChunk{Delta: m.version + "：退款操作已处理", Done: true, FinishReason: "stop", Usage: &llm.Usage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5}}
	} else {
		call := llm.ToolCall{ID: "refund-1", Type: "function"}
		call.Function.Name = "refund"
		call.Function.Arguments = `{"amount":50}`
		ch <- &llm.StreamChunk{Done: true, FinishReason: "tool_calls", ToolCalls: []llm.ToolCall{call}, Usage: &llm.Usage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5}}
	}
	close(ch)
	return ch, nil
}
func nativeFactory(effects, requests *atomic.Int32) mg.Factory {
	return adapters.NativeFactory{Check: func(context.Context, mg.Version) error { return nil }, BuildAgent: func(ctx context.Context, v mg.Version, r mg.Resources, approver tools.Approver) (*builder.SimpleAgentBuilder, func(context.Context) error, error) {
		reg := tools.NewRegistry()
		err := reg.Register(tools.NewBaseTool("refund", "退还金额", `{"type":"object","properties":{"amount":{"type":"integer"}},"required":["amount"],"additionalProperties":false}`, func(context.Context, *interfaces.ToolInput) (*interfaces.ToolOutput, error) {
			effects.Add(1)
			return &interfaces.ToolOutput{Success: true, Result: "退款凭证"}, nil
		}))
		if err != nil {
			return nil, nil, err
		}
		d, err := tools.NewDispatcher(reg, tools.DispatchConfig{Pipeline: &tools.Pipeline{Pre: []tools.PreHook{func(context.Context, *tools.Call) (tools.PreDecision, error) {
			return tools.PreDecision{Kind: tools.PreAsk, Feedback: "退款需审核"}, nil
		}}, Approver: approver}})
		if err != nil {
			return nil, nil, err
		}
		return builder.NewSimpleBuilder(&scriptModel{requests: requests, version: v.Version}).WithTools(d.Tools()...).WithMaxIterations(4), nil, nil
	}}
}
func TestHostRegisterPublishSessionTaskApprovalResume(t *testing.T) {
	var effects, requests atomic.Int32
	f := setup(t, map[string]mg.Factory{"test": nativeFactory(&effects, &requests)})
	f.worker(t)
	se := f.session(t)
	task, err := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"退还50元"`)})
	require.NoError(t, err)
	var ap mg.Approval
	require.Eventually(t, func() bool {
		page, e := f.s.ListApprovals(f.ctx, f.actor, mg.Query{TaskID: task.ID, Statuses: []string{"pending"}})
		if e == nil && len(page.Items) == 1 {
			ap = page.Items[0]
			return true
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)
	require.Zero(t, effects.Load())
	task, err = f.s.GetTask(f.ctx, f.actor, task.ID)
	require.NoError(t, err)
	_, err = f.s.ResumeTask(f.ctx, f.actor, mg.ResumeTaskRequest{IdempotencyKey: uuid.NewString(), TaskID: task.ID, ExpectedRevision: task.Revision, WaitID: ap.Wait.ID, Input: json.RawMessage(`"approve"`)})
	require.ErrorIs(t, err, agentErrors.ErrManagementApprovalRequired)
	_, err = f.s.DecideApproval(f.ctx, f.actor, mg.DecisionRequest{IdempotencyKey: uuid.NewString(), ApprovalID: ap.ID, ExpectedRevision: ap.Revision, Decision: "approve", Reason: "退款符合规则"})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		v, e := f.s.GetTask(f.ctx, f.actor, task.ID)
		task = v
		return e == nil && v.State == mg.Succeeded
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, int32(1), effects.Load())
	require.Equal(t, int32(2), requests.Load())
	require.JSONEq(t, `"v1：退款操作已处理"`, string(task.Result))
	run, err := f.s.GetRun(f.ctx, f.actor, task.ID)
	require.NoError(t, err)
	require.NotNil(t, run.Usage)
	require.Equal(t, 10, run.Usage.Root.KnownTokens.TotalTokens)
	history, err := f.s.History(f.ctx, f.actor, mg.HistoryRequest{SessionID: se.ID, Limit: 50})
	require.NoError(t, err)
	require.NotEmpty(t, history.Items)
	for _, ev := range history.Items {
		require.NotEqual(t, constants.SessionEventBoundarySnapshot, ev.Type)
	}
}
