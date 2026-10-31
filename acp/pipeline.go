package acp

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/kart-io/goagent/interfaces"
	"github.com/kart-io/goagent/tools"
)

const (
	optionAllowOnce    = "allow_once"
	optionAllowSession = "allow_always"
	optionReject       = "reject_once"
)

// endTurnTimeout 限制一轮结束时补发通知的等待；此时 prompt 的 ctx 可能已取消。
const endTurnTimeout = 5 * time.Second

// Pipeline 返回已装好工具调用通知与审批的管线，交给宿主的 tools.Dispatcher。宿主追加返回 PreAsk 的
// PreHook 来要求审批，也可追加 Guards、Around、Post，但不应替换第 0 项与 Approver。
func (s *Session) Pipeline() *tools.Pipeline { return s.pipeline }

func (s *Session) newPipeline() *tools.Pipeline {
	return &tools.Pipeline{
		Pre:      []tools.PreHook{s.announce},
		Approver: tools.ApproverFunc(s.approve),
		Around:   []tools.Around{s.running},
		Post:     []tools.PostHook{s.finished},
	}
}

func (s *Session) check(call *tools.Call) error {
	if call.SessionID != s.id {
		return fmt.Errorf("acp: 工具调用属于会话 %q 而不是 ACP 会话 %q，构建 Agent 时须 WithSessionID(s.ID())", call.SessionID, s.id)
	}
	return nil
}

func (s *Session) announce(ctx context.Context, call *tools.Call) (tools.PreDecision, error) {
	if err := s.check(call); err != nil {
		return tools.PreDecision{}, err
	}
	// goagent 只在持久运行中设置 CallID，其余运行由会话编号补上。
	if call.CallID == "" {
		call.CallID = fmt.Sprintf("call-%d", s.nextCall.Add(1))
	}
	id := acpsdk.ToolCallId(call.CallID)
	kind := s.conn.srv.kind(call.Tool)
	// 持久运行中替代调用沿用原 CallID 再次经过管线，此时只更新标题与参数；普通运行的替代调用另得编号。
	if !s.calls.start(call.CallID) {
		return tools.PreDecision{}, s.update(ctx, acpsdk.UpdateToolCall(id, acpsdk.WithUpdateTitle(call.Tool), acpsdk.WithUpdateKind(kind), acpsdk.WithUpdateRawInput(call.Args)))
	}
	return tools.PreDecision{}, s.update(ctx, acpsdk.StartToolCall(id, call.Tool,
		acpsdk.WithStartKind(kind), acpsdk.WithStartStatus(acpsdk.ToolCallStatusPending), acpsdk.WithStartRawInput(call.Args)))
}

func (s *Session) approve(ctx context.Context, req tools.ApprovalRequest) (tools.ApprovalResult, error) {
	options := []acpsdk.PermissionOption{{Kind: acpsdk.PermissionOptionKindAllowOnce, Name: "允许一次", OptionId: optionAllowOnce}}
	if req.Grant != nil {
		options = append(options, acpsdk.PermissionOption{Kind: acpsdk.PermissionOptionKindAllowAlways, Name: "本会话允许：" + req.Grant.Describe(), OptionId: optionAllowSession})
	}
	options = append(options, acpsdk.PermissionOption{Kind: acpsdk.PermissionOptionKindRejectOnce, Name: "拒绝", OptionId: optionReject})
	title := req.Call.Tool
	if req.Reason != "" {
		title += "：" + req.Reason
	}
	resp, err := s.conn.client().RequestPermission(ctx, acpsdk.RequestPermissionRequest{
		SessionId: acpsdk.SessionId(s.id),
		ToolCall: acpsdk.ToolCallUpdate{
			ToolCallId: acpsdk.ToolCallId(req.Call.CallID),
			Title:      acpsdk.Ptr(title),
			Status:     acpsdk.Ptr(acpsdk.ToolCallStatusPending),
			RawInput:   req.Call.Args,
		},
		Options: options,
	})
	if err != nil {
		if ctx.Err() != nil {
			return tools.ApprovalResult{Outcome: tools.ApprovalCancelled}, nil
		}
		return tools.ApprovalResult{}, fmt.Errorf("acp: 请求审批：%w", err)
	}
	if resp.Outcome.Selected == nil {
		return tools.ApprovalResult{Outcome: tools.ApprovalCancelled}, nil
	}
	switch string(resp.Outcome.Selected.OptionId) {
	case optionAllowOnce:
		return tools.ApprovalResult{Outcome: tools.ApprovalAllowedOnce}, nil
	case optionAllowSession:
		return tools.ApprovalResult{Outcome: tools.ApprovalAllowedSession}, nil
	default:
		return tools.ApprovalResult{Outcome: tools.ApprovalRejected}, nil
	}
}

func (s *Session) running(ctx context.Context, call *tools.Call, next func(context.Context) (*interfaces.ToolOutput, error)) (*interfaces.ToolOutput, error) {
	id := acpsdk.ToolCallId(call.CallID)
	if err := s.update(ctx, acpsdk.UpdateToolCall(id, acpsdk.WithUpdateStatus(acpsdk.ToolCallStatusInProgress))); err != nil {
		return nil, err
	}
	out, err := next(withCallID(ctx, call.CallID))
	if err != nil {
		s.fail(ctx, call.CallID, err.Error())
	} else if msg, ok := tools.ArgumentErrorResult(out); ok {
		s.fail(ctx, call.CallID, msg)
	}
	return out, err
}

func (s *Session) finished(ctx context.Context, call *tools.Call, out *interfaces.ToolOutput) (tools.PostDecision, error) {
	id := acpsdk.ToolCallId(call.CallID)
	terminals, ok := s.calls.finish(call.CallID)
	if !ok {
		return tools.PostDecision{}, nil
	}
	if msg, rejected := tools.RejectedResult(out); rejected {
		return tools.PostDecision{}, s.settle(ctx, acpsdk.UpdateToolCall(id,
			acpsdk.WithUpdateStatus(acpsdk.ToolCallStatusFailed), acpsdk.WithUpdateContent(toolContent(terminals, msg))))
	}
	return tools.PostDecision{}, s.settle(ctx, acpsdk.UpdateToolCall(id,
		acpsdk.WithUpdateStatus(acpsdk.ToolCallStatusCompleted),
		acpsdk.WithUpdateContent(toolContent(terminals, resultText(out))),
		acpsdk.WithUpdateRawOutput(out.Result)))
}

// fail 发送 failed，发送失败不影响调用方的返回值。
func (s *Session) fail(ctx context.Context, callID, text string) {
	terminals, ok := s.calls.finish(callID)
	if !ok {
		return
	}
	_ = s.settle(ctx, acpsdk.UpdateToolCall(acpsdk.ToolCallId(callID),
		acpsdk.WithUpdateStatus(acpsdk.ToolCallStatusFailed), acpsdk.WithUpdateContent(toolContent(terminals, text))))
}

// settle 发送调用的终态通知。调用已从未结束表移除，ctx 此时可能已取消（如审批等待中取消 prompt），
// 发送失败就再无补发机会，所以改用不随之取消的有界 ctx。
func (s *Session) settle(ctx context.Context, u acpsdk.SessionUpdate) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), endTurnTimeout)
	defer cancel()
	return s.update(ctx, u)
}

// endTurn 为本轮已发出 tool_call 但未结束的调用补发 failed（准入阶段出错、终止或取消时 Around 与 Post 都不运行）。
func (s *Session) endTurn(ctx context.Context) {
	for _, id := range s.calls.drain() {
		s.fail(ctx, id, "调用未执行（可能已被替代或中止）")
	}
}

func toolContent(terminals []string, text string) []acpsdk.ToolCallContent {
	var out []acpsdk.ToolCallContent
	for _, t := range terminals {
		out = append(out, acpsdk.ToolTerminalRef(t))
	}
	if text != "" {
		out = append(out, acpsdk.ToolContent(acpsdk.TextBlock(text)))
	}
	return out
}

// resultText 把工具结果转为编辑器显示的文本：字符串原样，其他类型编码为 JSON。
func resultText(out *interfaces.ToolOutput) string {
	switch v := out.Result.(type) {
	case nil:
		return ""
	case string:
		return v
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprint(v)
		}
		return string(data)
	}
}

// callTracker 记录已发出 tool_call 但未结束的调用及其终端。
type callTracker struct {
	mu   sync.Mutex
	open map[string][]string
}

func newCallTracker() *callTracker { return &callTracker{open: map[string][]string{}} }

func (t *callTracker) start(id string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.open[id]; ok {
		return false
	}
	t.open[id] = nil
	return true
}

func (t *callTracker) addTerminal(id, term string) []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.open[id] = append(t.open[id], term)
	return append([]string(nil), t.open[id]...)
}

func (t *callTracker) finish(id string) ([]string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	terms, ok := t.open[id]
	delete(t.open, id)
	return terms, ok
}

func (t *callTracker) drain() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	ids := make([]string, 0, len(t.open))
	for id := range t.open {
		ids = append(ids, id)
	}
	return ids
}

type callIDKey struct{}

func withCallID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, callIDKey{}, id)
}

func callIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(callIDKey{}).(string)
	return id
}
