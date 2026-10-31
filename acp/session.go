package acp

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/kart-io/goagent/harness/env"
	"github.com/kart-io/goagent/tools"
)

// Session 是一个 ACP 会话：Factory 据此构建 Agent。字段由本包维护，对宿主只读。
type Session struct {
	id    string
	cwd   string
	mcp   []acpsdk.McpServer
	conn  *conn
	local *env.LocalFS
	fs    env.FileSystem
	exec  env.Executor

	runner Runner
	run    sync.Mutex // 同一会话的 prompt 依次执行

	pipeline *tools.Pipeline
	calls    *callTracker
	nextCall atomic.Int64
}

func newSession(c *conn, id, cwd string, mcp []acpsdk.McpServer) (*Session, error) {
	if !filepath.IsAbs(cwd) {
		return nil, fmt.Errorf("acp: cwd 必须是绝对路径：%q", cwd)
	}
	local, err := env.NewLocalFS(cwd)
	if err != nil {
		return nil, fmt.Errorf("acp: 打开工作区 %q：%w", cwd, err)
	}
	s := &Session{id: id, cwd: cwd, mcp: mcp, conn: c, local: local}
	caps := c.capabilities().Fs
	if caps.ReadTextFile && caps.WriteTextFile {
		s.fs = &clientFS{s: s}
	} else {
		s.fs = local
	}
	if c.capabilities().Terminal {
		s.exec = &clientExecutor{s: s}
	} else {
		s.exec = env.LocalExecutor{Root: cwd}
	}
	s.calls = newCallTracker()
	s.pipeline = s.newPipeline()
	return s, nil
}

// ID 是 ACP 会话 ID，构建 Agent 时须 WithSessionID(s.ID())。
func (s *Session) ID() string { return s.id }

// Cwd 是编辑器给出的工作区绝对路径。
func (s *Session) Cwd() string { return s.cwd }

// MCPServers 原样返回编辑器要求连接的 MCP 服务器，宿主可用 mcp/remote 连接。
func (s *Session) MCPServers() []acpsdk.McpServer { return s.mcp }

// FileSystem 返回工作区文件系统：客户端支持时读写经编辑器，其余操作在本地。
func (s *Session) FileSystem() env.FileSystem { return s.fs }

// Executor 返回命令执行器：客户端支持终端时经编辑器执行，否则在本地执行。
func (s *Session) Executor() env.Executor { return s.exec }

func (s *Session) update(ctx context.Context, u acpsdk.SessionUpdate) error {
	return s.conn.client().SessionUpdate(ctx, acpsdk.SessionNotification{SessionId: acpsdk.SessionId(s.id), Update: u})
}
