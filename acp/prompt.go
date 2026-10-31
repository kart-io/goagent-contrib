package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/kart-io/goagent/builder"
)

// Prompt 执行一轮对话。被取消（session/cancel、session/close、同会话的新 prompt、连接断开）时返回 cancelled 而非错误，这是协议要求。
func (c *conn) Prompt(ctx context.Context, p acpsdk.PromptRequest) (response acpsdk.PromptResponse, err error) {
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
	defer context.AfterFunc(s.root, cancel)()
	gen, ok := s.startTurn(cancel)
	if !ok {
		return acpsdk.PromptResponse{}, missing(p.SessionId)
	}
	defer s.leaveTurn(gen)

	select {
	case s.run <- struct{}{}:
	case <-ctx.Done():
		return acpsdk.PromptResponse{StopReason: acpsdk.StopReasonCancelled}, nil
	}
	releaseExecution := true
	defer func() {
		if releaseExecution {
			<-s.run
		}
	}()
	defer s.endTurn(ctx)
	defer func() {
		saveCtx, stop := cleanupCtx(ctx)
		defer stop()
		if saveErr := s.persistMetadata(saveCtx); saveErr != nil {
			s.recordFailure(saveErr)
			if response.StopReason != acpsdk.StopReasonCancelled {
				err = internalError("保存会话元数据", errors.Join(err, saveErr))
			}
		}
	}()
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
		cancel()
		// 执行终态不代表生产者已经退出，关闭资源前仍须等通道关闭。
		drain := func(wait context.Context) bool {
			for {
				select {
				case _, ok := <-events:
					if !ok {
						return true
					}
				case m := <-ob.reqs:
					m.res <- s.send(m.ctx, m.u)
				case <-wait.Done():
					return false
				}
			}
		}
		finish := func() { s.loop.CompareAndSwap(ob, nil); close(ob.done) }
		wait, stop := cleanupCtx(ctx)
		defer stop()
		if drain(wait) {
			finish()
			return
		}
		s.recordFailure(wait.Err())
		if response.StopReason != acpsdk.StopReasonCancelled {
			err = internalError("等待执行生产者退出", wait.Err())
		}
		s.turnMu.Lock()
		s.closed = true
		s.turnMu.Unlock()
		releaseExecution = false
		go func() { drain(context.Background()); finish(); <-s.run }()
		go func() { _ = s.close() }()
	}()
	var sent strings.Builder
	for {
		var ev builder.AgentEvent
		select {
		case <-ctx.Done():
			return cancelled, nil
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
		if ctx.Err() != nil {
			return cancelled, nil
		}
		switch ev.Type {
		case "text":
			if ev.Delta == "" {
				continue
			}
			if err := s.send(ctx, acpsdk.UpdateAgentMessageText(ev.Delta)); err != nil && ctx.Err() == nil {
				return acpsdk.PromptResponse{}, internalError("发送回答", err)
			}
			sent.WriteString(ev.Delta)
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
			if ev.Output != nil {
				final, err := finalText(ev.Output.Result)
				if err != nil {
					return acpsdk.PromptResponse{}, internalError("编码最终答案", err)
				}
				if suffix := answerSuffix(sent.String(), final); suffix != "" {
					if err := s.send(ctx, acpsdk.UpdateAgentMessageText(suffix)); err != nil {
						if ctx.Err() != nil {
							return cancelled, nil
						}
						return acpsdk.PromptResponse{}, internalError("发送最终答案", err)
					}
				}
			}
			if ctx.Err() != nil {
				return cancelled, nil
			}
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

func finalText(result any) (string, error) {
	if result == nil {
		return "", nil
	}
	if text, ok := result.(string); ok {
		return text, nil
	}
	data, err := json.Marshal(result)
	return string(data), err
}

func answerSuffix(sent, final string) string {
	if final == "" || strings.HasSuffix(sent, final) {
		return ""
	}
	if strings.HasPrefix(final, sent) {
		return strings.TrimPrefix(final, sent)
	}
	return "\n\n最终答复：\n" + final
}
