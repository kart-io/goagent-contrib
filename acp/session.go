package acp

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

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

	runner    Runner
	run       sync.Mutex // 同一会话的 prompt 依次执行
	closeOnce sync.Once

	// 当前 prompt 的取消由会话自己掌握：SDK 在重叠的 prompt 返回时会删掉新 prompt 的取消函数。
	turnMu     sync.Mutex
	turnGen    uint64
	turnCancel context.CancelFunc
	closed     bool

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

// startTurn 登记 prompt 的取消函数并取消上一个 prompt；会话已关闭时返回 false。
func (s *Session) startTurn(cancel context.CancelFunc) (uint64, bool) {
	s.turnMu.Lock()
	defer s.turnMu.Unlock()
	if s.closed {
		return 0, false
	}
	if s.turnCancel != nil {
		s.turnCancel()
	}
	s.turnGen++
	s.turnCancel = cancel
	return s.turnGen, true
}

// leaveTurn 只移除自己的登记，之后到达的 prompt 的登记保留。
func (s *Session) leaveTurn(gen uint64) {
	s.turnMu.Lock()
	defer s.turnMu.Unlock()
	if s.turnGen == gen {
		s.turnCancel = nil
	}
}

func (s *Session) cancelTurn() {
	s.turnMu.Lock()
	defer s.turnMu.Unlock()
	if s.turnCancel != nil {
		s.turnCancel()
	}
}

// close 拒绝新的 prompt、取消当前 prompt，在其释放后关闭 Runner；最多等待 endTurnTimeout。
func (s *Session) close() {
	s.turnMu.Lock()
	s.closed = true
	if s.turnCancel != nil {
		s.turnCancel()
	}
	s.turnMu.Unlock()
	released := make(chan struct{})
	go func() {
		s.run.Lock()
		s.closeRunner()
		s.run.Unlock()
		close(released)
	}()
	select {
	case <-released:
	case <-time.After(endTurnTimeout):
		// 不响应取消的 Runner 不应让 session/close 一直挂起；它结束后上面的 goroutine 仍会关闭 Runner。
	}
}

// closeRunner 在 Runner 实现 io.Closer 时关闭它，只关闭一次；此时已无人使用它，关闭错误无处上报，忽略。
func (s *Session) closeRunner() {
	s.closeOnce.Do(func() {
		if c, ok := s.runner.(io.Closer); ok {
			_ = c.Close()
		}
	})
}
