package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/kart-io/goagent/core/session"
	agentErrors "github.com/kart-io/goagent/errors"
)

// LoadSession 回放会话日志中的对话，再以同一 ID 构建 Agent 续接。
func (c *conn) LoadSession(ctx context.Context, p acpsdk.LoadSessionRequest) (acpsdk.LoadSessionResponse, error) {
	if c.srv.store == nil {
		return acpsdk.LoadSessionResponse{}, acpsdk.NewMethodNotFound(acpsdk.AgentMethodSessionLoad)
	}
	id := string(p.SessionId)
	// 在锁内预留 ID，使并发的同 ID 加载只有一个通过检查；任何返回路径都释放预留。
	c.mu.Lock()
	old := c.sessions[id]
	opened := c.loading[id] || c.closed
	if old != nil {
		old.turnMu.Lock()
		opened = opened || old.closed
		old.turnMu.Unlock()
	}
	for pending := range c.pending {
		if pending.id == id {
			opened = true
			break
		}
	}
	if !opened {
		c.loading[id] = true
	}
	c.mu.Unlock()
	if opened {
		return acpsdk.LoadSessionResponse{}, acpsdk.NewInvalidParams(map[string]any{"error": fmt.Sprintf("acp: 会话 %q 正在加载或清理，暂不能重载", id)})
	}
	defer func() {
		c.mu.Lock()
		delete(c.loading, id)
		c.mu.Unlock()
	}()
	// 客户端可直接重载现有会话，但新旧资源必须先后交接。
	if old != nil {
		if err := old.close(); err != nil {
			return acpsdk.LoadSessionResponse{}, internalError("关闭待重载会话", err)
		}
		c.mu.Lock()
		if c.sessions[id] == old {
			delete(c.sessions, id)
		}
		c.mu.Unlock()
	}
	events, err := c.srv.store.Read(ctx, id, 0)
	if errors.Is(err, agentErrors.ErrSessionNotFound) {
		return acpsdk.LoadSessionResponse{}, missing(p.SessionId)
	}
	if err != nil {
		return acpsdk.LoadSessionResponse{}, internalError("读取会话日志", err)
	}
	s, err := c.build(ctx, id, p.Cwd, p.McpServers)
	if err != nil {
		return acpsdk.LoadSessionResponse{}, err
	}
	// 回放完成后才登记，失败时无需从会话表回滚。
	if err := s.replay(ctx, events); err != nil {
		cleanupErr := c.discard(s)
		return acpsdk.LoadSessionResponse{}, internalError("回放会话", errors.Join(err, cleanupErr))
	}
	if err := s.persistMetadata(ctx); err != nil {
		cleanupErr := c.discard(s)
		return acpsdk.LoadSessionResponse{}, internalError("保存会话元数据", errors.Join(err, cleanupErr))
	}
	if err := c.register(s); err != nil {
		return acpsdk.LoadSessionResponse{}, err
	}
	return acpsdk.LoadSessionResponse{}, nil
}

// replay 按 session.Derive 推导的顺序发送历史；被退回与中止轮次中的消息已被 Derive 排除。
func (s *Session) replay(ctx context.Context, events []session.Event) error {
	entries, err := session.Derive(events)
	if err != nil {
		return err
	}
	for _, e := range entries {
		m := e.Message
		var updates []acpsdk.SessionUpdate
		switch m.Role {
		case "user":
			updates = append(updates, acpsdk.UpdateUserMessageText(m.Content))
		case "assistant":
			if m.Content != "" {
				updates = append(updates, acpsdk.UpdateAgentMessageText(m.Content))
			}
			for _, tc := range m.ToolCalls {
				if tc.ID == "" {
					continue
				}
				var args any
				if json.Unmarshal([]byte(tc.Function.Arguments), &args) != nil {
					args = tc.Function.Arguments
				}
				updates = append(updates, acpsdk.StartToolCall(acpsdk.ToolCallId(tc.ID), tc.Function.Name,
					acpsdk.WithStartKind(s.conn.srv.kind(tc.Function.Name)),
					acpsdk.WithStartStatus(acpsdk.ToolCallStatusPending), acpsdk.WithStartRawInput(args)))
			}
		case "tool":
			if m.ToolCallID == "" {
				continue
			}
			// 旧日志或无工具元数据时无法判断失败，按完成处理。
			status := acpsdk.ToolCallStatusCompleted
			if e.Tool != nil && !e.Tool.Success {
				status = acpsdk.ToolCallStatusFailed
			}
			updates = append(updates, acpsdk.UpdateToolCall(acpsdk.ToolCallId(m.ToolCallID),
				acpsdk.WithUpdateStatus(status), acpsdk.WithUpdateContent(toolContent(nil, m.Content))))
		}
		for _, u := range updates {
			if err := s.update(ctx, u); err != nil {
				return err
			}
		}
	}
	return nil
}
