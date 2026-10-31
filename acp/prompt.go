package acp

import (
	"context"
	"errors"
	"fmt"
	"strings"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/kart-io/goagent/builder"
)

// Prompt 执行一轮对话。被取消（session/cancel、session/close、同会话的新 prompt、连接断开）时返回 cancelled 而非错误，这是协议要求。
func (c *conn) Prompt(ctx context.Context, p acpsdk.PromptRequest) (acpsdk.PromptResponse, error) {
	s, err := c.session(p.SessionId)
	if err != nil {
		return acpsdk.PromptResponse{}, err
	}
	input, err := promptText(p.Prompt)
	if err != nil {
		return acpsdk.PromptResponse{}, acpsdk.NewInvalidParams(map[string]any{"error": err.Error()})
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(c.base, cancel)()
	gen, ok := s.startTurn(cancel)
	if !ok {
		return acpsdk.PromptResponse{}, missing(p.SessionId)
	}
	defer s.leaveTurn(gen)

	s.run.Lock()
	defer s.run.Unlock()
	defer s.endTurn(ctx)
	cancelled := acpsdk.PromptResponse{StopReason: acpsdk.StopReasonCancelled}
	if ctx.Err() != nil {
		return cancelled, nil
	}
	events, err := s.runner.ExecuteStream(ctx, input)
	if err != nil {
		if ctx.Err() != nil {
			return cancelled, nil
		}
		return acpsdk.PromptResponse{}, internalError("执行 Agent", err)
	}
	// 本轮的通知都由这里发出：钩子经发件箱交来的通知在取下一个事件前发送，
	// 因此 builder 发出 tool_start 后钩子才发的 tool_call 必然排在之前的文本之后。
	// 这依赖 builder 事件通道容量为 1（builder/events.go）：容量更大时文本与 tool_start 可同在缓冲中。
	// 先于 endTurn 停止接收，之后的通知直接发送。
	ob := newOutbox()
	s.loop.Store(ob)
	defer func() {
		s.loop.Store(nil)
		close(ob.done)
	}()
	for {
		var ev builder.AgentEvent
		select {
		case m := <-ob.reqs:
			m.res <- s.send(m.ctx, m.u)
			continue
		case e, ok := <-events:
			if !ok {
				if ctx.Err() != nil {
					return cancelled, nil
				}
				return acpsdk.PromptResponse{}, internalError("执行 Agent", errors.New("事件流在 done 之前结束"))
			}
			ev = e
		}
		switch ev.Type {
		case "text":
			if ev.Delta == "" {
				continue
			}
			if err := s.send(ctx, acpsdk.UpdateAgentMessageText(ev.Delta)); err != nil && ctx.Err() == nil {
				return acpsdk.PromptResponse{}, internalError("发送回答", err)
			}
		case "error":
			if ctx.Err() != nil {
				return cancelled, nil
			}
			cause := ev.Error
			if cause == nil {
				cause = errors.New(ev.Message)
			}
			return acpsdk.PromptResponse{}, internalError("执行 Agent", cause)
		case "done":
			return acpsdk.PromptResponse{StopReason: acpsdk.StopReasonEndTurn}, nil
		}
	}
}

// promptText 把内容块拼成 Agent 输入：文本原样，资源链接只写 URI，嵌入的文本资源以 <uri> 加内容附上。
func promptText(blocks []acpsdk.ContentBlock) (string, error) {
	var b strings.Builder
	for i, blk := range blocks {
		if i > 0 {
			b.WriteString("\n\n")
		}
		switch {
		case blk.Text != nil:
			b.WriteString(blk.Text.Text)
		case blk.ResourceLink != nil:
			b.WriteString(blk.ResourceLink.Uri)
		case blk.Resource != nil && blk.Resource.Resource.TextResourceContents != nil:
			r := blk.Resource.Resource.TextResourceContents
			fmt.Fprintf(&b, "<%s>\n%s", r.Uri, r.Text)
		default:
			return "", errors.New("acp: 不支持的内容块，只支持 text、resource_link 与文本 resource")
		}
	}
	return b.String(), nil
}
