package acp

import (
	"context"
	"testing"
	"time"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/kart-io/goagent/builder"
	"github.com/kart-io/goagent/interfaces"
	"github.com/kart-io/goagent/tools"
	"github.com/stretchr/testify/require"
)

func TestToolCallNotifications(t *testing.T) {
	var runs int
	model := newModel(calls("lookup"), say("查到了"))
	kind := WithToolKind(func(tool string) acpsdk.ToolKind { return acpsdk.ToolKindSearch })
	h := start(t, NewServer(toolFactory(model, nil, nil, only(echoTool("lookup", &runs, ""))), kind), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, t.TempDir())

	resp, err := h.prompt(t, id, "查订单")
	require.NoError(t, err)
	require.Equal(t, acpsdk.StopReasonEndTurn, resp.StopReason)
	require.Equal(t, 1, runs)
	require.Equal(t, []string{"start:pending", "in_progress", "completed"}, h.editor.statuses("call-1"))
	require.Empty(t, h.editor.permissions, "宿主未要求审批时不请求权限")

	var start *acpsdk.SessionUpdateToolCall
	for _, n := range h.editor.notifications() {
		if n.Update.ToolCall != nil {
			start = n.Update.ToolCall
		}
	}
	require.Equal(t, "lookup", start.Title)
	require.Equal(t, acpsdk.ToolKindSearch, start.Kind)
	require.Equal(t, map[string]any{"q": "A1"}, start.RawInput)
	last := h.editor.lastUpdate("call-1")
	require.Equal(t, "lookup 的结果", last.Content[0].Content.Content.Text.Text)
	require.Equal(t, "lookup 的结果", last.RawOutput)
	require.Equal(t, "查到了", h.editor.agentText())
}

func TestApprovalAllowSessionAsksOnce(t *testing.T) {
	var runs int
	ed := newEditor()
	ed.answer = func(_ context.Context, r acpsdk.RequestPermissionRequest) acpsdk.RequestPermissionResponse {
		return acpsdk.RequestPermissionResponse{Outcome: acpsdk.NewRequestPermissionOutcomeSelected("allow_always")}
	}
	model := newModel(calls("lookup"), calls("lookup"), say("两次都查了"))
	h := start(t, NewServer(toolFactory(model, askAll, nil, only(echoTool("lookup", &runs, "")))), ed)
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, t.TempDir())

	_, err := h.prompt(t, id, "查两次")
	require.NoError(t, err)
	require.Equal(t, 2, runs)
	require.Len(t, ed.permissions, 1, "本会话允许后同会话不再询问")
	req := ed.permissions[0]
	require.Equal(t, "call-1", string(req.ToolCall.ToolCallId))
	require.Equal(t, "lookup：需要确认", *req.ToolCall.Title)
	var kinds []acpsdk.PermissionOptionKind
	for _, o := range req.Options {
		kinds = append(kinds, o.Kind)
	}
	require.Equal(t, []acpsdk.PermissionOptionKind{acpsdk.PermissionOptionKindAllowOnce, acpsdk.PermissionOptionKindAllowAlways, acpsdk.PermissionOptionKindRejectOnce}, kinds)
	require.Contains(t, req.Options[1].Name, "工具 lookup")
}

func TestApprovalWithoutGrantHasNoSessionOption(t *testing.T) {
	ed := newEditor()
	noGrant := func(context.Context, *tools.Call) (tools.PreDecision, error) {
		return tools.PreDecision{Kind: tools.PreAsk}, nil
	}
	h := start(t, NewServer(toolFactory(newModel(calls("lookup"), say("好")), noGrant, nil, only(echoTool("lookup", nil, "")))), ed)
	h.initialize(t, acpsdk.ClientCapabilities{})
	_, err := h.prompt(t, h.newSession(t, t.TempDir()), "查")
	require.NoError(t, err)
	require.Len(t, ed.permissions[0].Options, 2)
}

func TestApprovalReject(t *testing.T) {
	var runs int
	ed := newEditor()
	ed.answer = func(context.Context, acpsdk.RequestPermissionRequest) acpsdk.RequestPermissionResponse {
		return acpsdk.RequestPermissionResponse{Outcome: acpsdk.NewRequestPermissionOutcomeSelected("reject_once")}
	}
	model := newModel(calls("lookup"), say("用户拒绝了"))
	h := start(t, NewServer(toolFactory(model, askAll, nil, only(echoTool("lookup", &runs, "")))), ed)
	h.initialize(t, acpsdk.ClientCapabilities{})

	_, err := h.prompt(t, h.newSession(t, t.TempDir()), "查")
	require.NoError(t, err)
	require.Zero(t, runs)
	require.Equal(t, []string{"start:pending", "failed"}, ed.statuses("call-1"))
	require.Contains(t, ed.lastUpdate("call-1").Content[0].Content.Content.Text.Text, "denied")
	require.Contains(t, model.request(1).Messages[len(model.request(1).Messages)-1].Content, "denied")
}

// 审批等待中取消 prompt：返回 cancelled，工具未执行，调用补发 failed。
func TestApprovalCancelledByPromptCancel(t *testing.T) {
	var runs int
	ed := newEditor()
	asked := make(chan struct{})
	ed.answer = func(ctx context.Context, _ acpsdk.RequestPermissionRequest) acpsdk.RequestPermissionResponse {
		close(asked)
		<-ctx.Done()
		return acpsdk.RequestPermissionResponse{Outcome: acpsdk.NewRequestPermissionOutcomeCancelled()}
	}
	h := start(t, NewServer(toolFactory(newModel(calls("lookup")), askAll, nil, only(echoTool("lookup", &runs, "")))), ed)
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, t.TempDir())

	done := make(chan promptResult, 1)
	go func() {
		resp, err := h.prompt(t, id, "查")
		done <- promptResult{resp, err}
	}()
	<-asked
	require.NoError(t, h.client.Cancel(context.Background(), acpsdk.CancelNotification{SessionId: id}))
	res := <-done
	require.NoError(t, res.err)
	require.Equal(t, acpsdk.StopReasonCancelled, res.resp.StopReason)
	require.Zero(t, runs)
	require.Equal(t, []string{"start:pending", "failed"}, ed.statuses("call-1"))
}

func TestToolErrorReportsFailed(t *testing.T) {
	h := start(t, NewServer(toolFactory(newModel(calls("lookup")), nil, nil, only(echoTool("lookup", nil, "数据库不可用")))), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	_, err := h.prompt(t, h.newSession(t, t.TempDir()), "查")
	requireCode(t, err, -32603)
	require.Equal(t, []string{"start:pending", "in_progress", "failed"}, h.editor.statuses("call-1"))
	require.Contains(t, h.editor.lastUpdate("call-1").Content[0].Content.Content.Text.Text, "数据库不可用")
}

func TestSessionIDMismatch(t *testing.T) {
	factory := func(_ context.Context, s *Session) (Runner, error) {
		reg := tools.NewRegistry()
		if err := reg.Register(echoTool("lookup", nil, "")); err != nil {
			return nil, err
		}
		d, err := tools.NewDispatcher(reg, tools.DispatchConfig{Pipeline: s.Pipeline()})
		if err != nil {
			return nil, err
		}
		agent, err := builder.NewSimpleBuilder(newModel(calls("lookup"))).WithSessionID("other").WithTools(d.Tools()...).Build()
		if err != nil {
			return nil, err
		}
		return agent, nil
	}
	h := start(t, NewServer(factory), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	_, err := h.prompt(t, h.newSession(t, t.TempDir()), "查")
	requireCode(t, err, -32603)
	require.ErrorContains(t, err, "WithSessionID")
	require.Empty(t, h.editor.statuses("call-1"))
}

func TestAnnounceCallID(t *testing.T) {
	seen := make(chan *Session, 1)
	h := start(t, NewServer(plainFactory(newModel(say("好")), seen)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	h.newSession(t, t.TempDir())
	s := <-seen

	_, err := s.announce(context.Background(), &tools.Call{Tool: "x", CallID: "turn/1/call/m1", SessionID: s.ID()})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(h.editor.statuses("turn/1/call/m1")) == 1 }, time.Second, 5*time.Millisecond)

	call := &tools.Call{Tool: "x", SessionID: s.ID()}
	_, err = s.announce(context.Background(), call)
	require.NoError(t, err)
	require.Equal(t, "call-1", call.CallID)
	require.Eventually(t, func() bool { return len(h.editor.statuses("call-1")) == 1 }, time.Second, 5*time.Millisecond)
}

func TestResultText(t *testing.T) {
	require.Equal(t, "纯文本", resultText(&interfaces.ToolOutput{Success: true, Result: "纯文本"}))
	require.Equal(t, `{"n":1}`, resultText(&interfaces.ToolOutput{Success: true, Result: map[string]int{"n": 1}}))
	require.Equal(t, "", resultText(&interfaces.ToolOutput{Success: true}))
}

type promptResult struct {
	resp acpsdk.PromptResponse
	err  error
}

// 非持久运行中替代调用另得编号，原调用无法关联，由轮末补发 failed。
func TestSubstitutedCallIsNumberedSeparately(t *testing.T) {
	var runs int
	subst := func(_ context.Context, c *tools.Call) (tools.PreDecision, error) {
		if c.Tool != "lookup" {
			return tools.PreDecision{}, nil
		}
		return tools.PreDecision{Kind: tools.PreSubstitute, Substitute: &tools.Substitution{Tool: "other", Args: map[string]any{}}}, nil
	}
	model := newModel(calls("lookup"), say("好"))
	h := start(t, NewServer(toolFactory(model, subst, nil, only(echoTool("lookup", nil, ""), echoTool("other", &runs, "")))), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	_, err := h.prompt(t, h.newSession(t, t.TempDir()), "查")
	require.NoError(t, err)
	require.Equal(t, 1, runs)
	require.Equal(t, []string{"start:pending", "in_progress", "completed"}, h.editor.statuses("call-2"))
	require.Equal(t, []string{"start:pending", "failed"}, h.editor.statuses("call-1"))
	require.Contains(t, h.editor.lastUpdate("call-1").Content[0].Content.Content.Text.Text, "可能已被替代")
}

func TestParallelCalls(t *testing.T) {
	model := newModel(calls("lookup", "lookup2"), say("好"))
	h := start(t, NewServer(toolFactoryParallel(2, model, nil, nil, only(echoTool("lookup", nil, ""), echoTool("lookup2", nil, "")))), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	_, err := h.prompt(t, h.newSession(t, t.TempDir()), "查两个")
	require.NoError(t, err)
	for _, id := range []string{"call-1", "call-2"} {
		require.Equal(t, []string{"start:pending", "in_progress", "completed"}, h.editor.statuses(id), id)
	}
}

// argFactory 以 ReturnArgumentErrors 注册工具，参数错误交还模型而不中止本轮。
func argFactory(model *scriptModel, tool interfaces.Tool) Factory {
	return func(_ context.Context, s *Session) (Runner, error) {
		reg := tools.NewRegistry()
		if err := reg.Register(tool); err != nil {
			return nil, err
		}
		d, err := tools.NewDispatcher(reg, tools.DispatchConfig{Pipeline: s.Pipeline(), ReturnArgumentErrors: true})
		if err != nil {
			return nil, err
		}
		agent, err := builder.NewSimpleBuilder(model).WithSessionID(s.ID()).WithTools(d.Tools()...).WithMaxIterations(4).Build()
		if err != nil {
			return nil, err
		}
		return agent, nil
	}
}

// 不满足 Schema 的参数在管线之前被拒绝并交还模型：编辑器不会看到这次调用。
func TestSchemaInvalidArgsNeverAnnounced(t *testing.T) {
	bad := calls("lookup")
	bad.calls[0].Function.Arguments = `{"q":1}`
	model := newModel(bad, say("改正了"))
	strict := tools.NewBaseTool("lookup", "查询", `{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}`, func(context.Context, *interfaces.ToolInput) (*interfaces.ToolOutput, error) {
		return &interfaces.ToolOutput{Success: true, Result: "不应执行"}, nil
	})
	h := start(t, NewServer(argFactory(model, strict)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	resp, err := h.prompt(t, h.newSession(t, t.TempDir()), "查")
	require.NoError(t, err)
	require.Equal(t, acpsdk.StopReasonEndTurn, resp.StopReason)
	for _, n := range h.editor.notifications() {
		require.Nil(t, n.Update.ToolCall, "参数不合法的调用不经过管线")
	}
	require.Equal(t, 2, model.count(), "参数错误交还模型")
}

// 工具执行时返回的 *tools.ArgumentError 经 ArgumentErrorResult 识别，调用标为 failed 而本轮继续。
func TestToolArgumentErrorReportsFailed(t *testing.T) {
	model := newModel(calls("lookup"), say("改正了"))
	picky := tools.NewBaseTool("lookup", "查询", `{"type":"object","properties":{"q":{"type":"string"}}}`, func(context.Context, *interfaces.ToolInput) (*interfaces.ToolOutput, error) {
		return nil, &tools.ArgumentError{Message: "订单号格式不对"}
	})
	h := start(t, NewServer(argFactory(model, picky)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	resp, err := h.prompt(t, h.newSession(t, t.TempDir()), "查")
	require.NoError(t, err)
	require.Equal(t, acpsdk.StopReasonEndTurn, resp.StopReason)
	require.Equal(t, []string{"start:pending", "in_progress", "failed"}, h.editor.statuses("call-1"))
	require.Contains(t, h.editor.lastUpdate("call-1").Content[0].Content.Content.Text.Text, "订单号格式不对")
}

// 执行中被取消的调用以"已取消"结束，而不是把 context 错误原文显示给用户。
func TestCancelledToolCallShowsCancelled(t *testing.T) {
	started := make(chan struct{})
	slow := tools.NewBaseTool("lookup", "测试工具 lookup", `{"type":"object","properties":{"q":{"type":"string"}}}`,
		func(ctx context.Context, _ *interfaces.ToolInput) (*interfaces.ToolOutput, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		})
	h := start(t, NewServer(toolFactory(newModel(calls("lookup")), nil, nil, only(slow))), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, t.TempDir())

	done := promptAsync(t, h, id, "查")
	recv(t, started, "工具开始执行")
	require.NoError(t, h.client.Cancel(context.Background(), acpsdk.CancelNotification{SessionId: id}))
	res := recv(t, done, "prompt 返回")
	require.NoError(t, res.err)
	require.Equal(t, acpsdk.StopReasonCancelled, res.resp.StopReason)
	require.Equal(t, []string{"start:pending", "in_progress", "failed"}, h.editor.statuses("call-1"))
	require.Equal(t, "已取消", h.editor.lastUpdate("call-1").Content[0].Content.Content.Text.Text)
}
