package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/kart-io/goagent/core/session"
)

// LoadSession 回放会话日志中的对话，再以同一 ID 构建 Agent 续接。
func (c *conn) LoadSession(ctx context.Context, p acpsdk.LoadSessionRequest) (acpsdk.LoadSessionResponse, error) {
	if c.srv.store == nil {
		return acpsdk.LoadSessionResponse{}, acpsdk.NewMethodNotFound(acpsdk.AgentMethodSessionLoad)
	}
	id := string(p.SessionId)
	events, err := c.srv.store.Read(ctx, id, 0)
	if errors.Is(err, session.ErrNotFound) {
		return acpsdk.LoadSessionResponse{}, acpsdk.NewInvalidParams(map[string]any{"error": fmt.Sprintf("acp: 会话 %q 不存在", id)})
	}
	if err != nil {
		return acpsdk.LoadSessionResponse{}, internalError("读取会话日志", err)
	}
	s, err := c.open(ctx, id, p.Cwd, p.McpServers)
	if err != nil {
		return acpsdk.LoadSessionResponse{}, err
	}
	if err := s.replay(ctx, events); err != nil {
		return acpsdk.LoadSessionResponse{}, internalError("回放会话", err)
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
				var args any
				if json.Unmarshal([]byte(tc.Function.Arguments), &args) != nil {
					args = tc.Function.Arguments
				}
				updates = append(updates, acpsdk.StartToolCall(acpsdk.ToolCallId(tc.ID), tc.Function.Name,
					acpsdk.WithStartKind(s.conn.srv.kind(tc.Function.Name)),
					acpsdk.WithStartStatus(acpsdk.ToolCallStatusPending), acpsdk.WithStartRawInput(args)))
			}
		case "tool":
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

// ListSessions 列出会话日志中的会话；工作区只对本连接新建或加载过的会话已知，未知时为空且不参与 cwd 过滤。
func (c *conn) ListSessions(ctx context.Context, p acpsdk.ListSessionsRequest) (acpsdk.ListSessionsResponse, error) {
	if c.srv.store == nil {
		return acpsdk.ListSessionsResponse{}, acpsdk.NewMethodNotFound(acpsdk.AgentMethodSessionList)
	}
	ids, err := c.srv.store.List(ctx)
	if err != nil {
		return acpsdk.ListSessionsResponse{}, internalError("列出会话", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := acpsdk.ListSessionsResponse{Sessions: []acpsdk.SessionInfo{}}
	for _, id := range ids {
		cwd, known := c.cwds[id]
		if p.Cwd != nil && known && cwd != *p.Cwd {
			continue
		}
		out.Sessions = append(out.Sessions, acpsdk.SessionInfo{SessionId: acpsdk.SessionId(id), Cwd: cwd})
	}
	return out, nil
}
