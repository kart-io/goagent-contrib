package acp

import (
	"context"
	"errors"
	"fmt"
	"io"
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

	runner     Runner
	run        chan struct{} // 获取执行权时支持 context 取消
	closeOnce  sync.Once
	closeDone  chan struct{}
	closeErr   error
	cleanups   []func(context.Context) error
	failures   error
	root       context.Context
	rootCancel context.CancelFunc

	// 当前 prompt 的取消由会话自己掌握：SDK 在重叠的 prompt 返回时会删掉新 prompt 的取消函数。
	turnMu     sync.Mutex
	turnGen    uint64
	turnCancel context.CancelFunc
	closed     bool

	// loop 是正在消费事件流的 prompt 的发件箱；非空时本会话的通知都由该 prompt 的 goroutine 发出。
	loop atomic.Pointer[outbox]

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
	root, rootCancel := context.WithCancel(c.base)
	s := &Session{id: id, cwd: filepath.Clean(cwd), mcp: append([]acpsdk.McpServer(nil), mcp...), conn: c, local: local,
		run: make(chan struct{}, 1), closeDone: make(chan struct{}), root: root, rootCancel: rootCancel}
	caps := c.capabilities().Fs
	if caps.ReadTextFile && caps.WriteTextFile {
		s.fs = &clientFS{s: s}
	} else {
		s.fs = local
	}
	if c.capabilities().Terminal {
		if c.srv.terminalCaps == nil {
			rootCancel()
			return nil, errors.New("acp: 编辑器终端需要 WithTerminalCapabilities 显式声明执行环境")
		}
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
func (s *Session) MCPServers() []acpsdk.McpServer { return append([]acpsdk.McpServer(nil), s.mcp...) }

// FileSystem 返回工作区文件系统：客户端支持时读写经编辑器，其余操作在本地。
func (s *Session) FileSystem() env.FileSystem { return s.fs }

// Executor 返回命令执行器：客户端支持终端时经编辑器执行，否则在本地执行。
func (s *Session) Executor() env.Executor { return s.exec }

// update 发送一条会话通知。prompt 正在消费事件流时交给它发送并等待发出，使通知顺序与事件顺序一致。
func (s *Session) update(ctx context.Context, u acpsdk.SessionUpdate) error {
	if ob := s.loop.Load(); ob != nil {
		if ok, err := ob.send(ctx, u); ok {
			return err
		}
	}
	return s.send(ctx, u)
}

// send 直接发送通知，只由持有发件箱的 prompt 或没有 prompt 消费事件时使用。
func (s *Session) send(ctx context.Context, u acpsdk.SessionUpdate) error {
	return s.conn.client().SessionUpdate(ctx, acpsdk.SessionNotification{SessionId: acpsdk.SessionId(s.id), Update: u})
}

// outbox 把钩子的通知交给 prompt 的事件循环发送。
type outbox struct {
	reqs chan outMsg
	done chan struct{} // 事件循环不再接收时关闭
}

type outMsg struct {
	ctx context.Context
	u   acpsdk.SessionUpdate
	res chan error
}

func newOutbox() *outbox { return &outbox{reqs: make(chan outMsg), done: make(chan struct{})} }

// send 返回 false 表示事件循环已退出，调用方应直接发送；事件循环接收后必定回复。
func (o *outbox) send(ctx context.Context, u acpsdk.SessionUpdate) (bool, error) {
	m := outMsg{ctx: ctx, u: u, res: make(chan error, 1)}
	select {
	case o.reqs <- m:
		return true, <-m.res
	case <-o.done:
		return false, nil
	case <-ctx.Done():
		return true, ctx.Err()
	}
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

// OnClose 注册宿主资源的释放动作；关闭开始后拒绝新增资源责任。
func (s *Session) OnClose(cleanup func(context.Context) error) error {
	s.turnMu.Lock()
	defer s.turnMu.Unlock()
	if cleanup == nil || s.closed {
		return errors.New("acp: 会话已关闭或清理回调为空")
	}
	s.cleanups = append(s.cleanups, cleanup)
	return nil
}

func (s *Session) recordFailure(err error) {
	if err == nil {
		return
	}
	s.turnMu.Lock()
	s.failures = errors.Join(s.failures, err)
	s.turnMu.Unlock()
}

// close 保留尚未停止的 Runner 的资源责任，超时可重试，实际释放只执行一次。
func (s *Session) close() error {
	s.closeOnce.Do(func() {
		s.turnMu.Lock()
		s.closed = true
		if s.turnCancel != nil {
			s.turnCancel()
		}
		s.rootCancel()
		s.turnMu.Unlock()
		go func() {
			s.run <- struct{}{}
			defer func() { <-s.run; close(s.closeDone) }()
			var errs []error
			if c, ok := s.runner.(io.Closer); ok {
				errs = append(errs, c.Close())
			}
			for i := len(s.cleanups) - 1; i >= 0; i-- {
				ctx, cancel := cleanupCtx(s.root)
				errs = append(errs, s.cleanups[i](ctx))
				cancel()
			}
			s.turnMu.Lock()
			errs = append(errs, s.failures)
			s.turnMu.Unlock()
			s.closeErr = errors.Join(errs...)
		}()
	})
	ctx, cancel := cleanupCtx(s.root)
	defer cancel()
	select {
	case <-s.closeDone:
		return s.closeErr
	case <-ctx.Done():
		return fmt.Errorf("acp: 等待会话清理：%w", ctx.Err())
	}
}
