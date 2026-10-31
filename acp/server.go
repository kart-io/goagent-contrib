package acp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/google/uuid"
	"github.com/kart-io/goagent/builder"
	"github.com/kart-io/goagent/core/session"
	"github.com/kart-io/goagent/harness/env"
)

// Factory 为每个 ACP 会话构建一个 Agent。构建时必须 WithSessionID(s.ID())，工具经 s.Pipeline()
// 所在的 tools.Dispatcher 注册；配置了会话存储时还须 WithSessionLog 使用同一个 Store。
// ctx 只覆盖构建过程：SDK 在 session/new、session/load 返回时取消它，长期资源不能绑定到它上面。
type Factory func(ctx context.Context, s *Session) (Runner, error)

// Runner 是本包对 Agent 的最小要求，*builder.ConfigurableAgent[C, S] 满足它。
// Runner 还实现 io.Closer 时，本包在 session/close、session/load 失败回滚与连接结束时调用一次 Close，
// 仅在该会话的事件生产者已经退出后调用，等待超时仍保留资源责任。
type Runner interface {
	ExecuteStream(ctx context.Context, input any) (<-chan builder.AgentEvent, error)
}

// Option 配置 Server。
type Option func(*Server)

// WithAgentInfo 设置 initialize 回报的 Agent 名称与版本，默认 goagent、dev。
func WithAgentInfo(name, version string) Option {
	return func(s *Server) { s.name, s.version = name, version }
}

// WithSessionStore 开启 session/load 与 session/list；Factory 须以同一 Store 调用 WithSessionLog。
func WithSessionStore(store session.Store) Option {
	return func(s *Server) { s.store = store }
}

// WithToolKind 决定工具调用通知中的 kind，默认全部为 other。
func WithToolKind(kind func(tool string) acpsdk.ToolKind) Option {
	return func(s *Server) { s.kind = kind }
}

// Server 把 Factory 构建的 Agent 暴露为 ACP Agent。
type Server struct {
	factory       Factory
	name, version string
	store         session.Store
	kind          func(string) acpsdk.ToolKind
	fileWrites    chan struct{}
	terminalCaps  *env.Capabilities
}

// NewServer 创建 Server。
func NewServer(f Factory, opts ...Option) *Server {
	s := &Server{fileWrites: make(chan struct{}, 1), factory: f, name: "goagent", version: "dev", kind: func(string) acpsdk.ToolKind { return acpsdk.ToolKindOther }}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Serve 返回前取消并回收已发布及构造中的会话，释放错误与 ctx.Err() 合并返回。
// 不配合取消的宿主保留清理责任；SDK 读循环须由调用方关闭输入后结束。
func (s *Server) Serve(ctx context.Context, in io.Reader, out io.Writer) (err error) {
	if s.factory == nil {
		return errors.New("acp: Factory 不能为空")
	}
	base, cancel := context.WithCancel(ctx)
	c := &conn{srv: s, base: base, ready: make(chan struct{}), sessions: map[string]*Session{}, cwds: map[string]string{}, loading: map[string]bool{}, pending: map[*Session]bool{}}
	defer func() { err = errors.Join(err, c.shutdown(cancel)) }()
	c.peer = acpsdk.NewAgentSideConnection(c, out, in)
	close(c.ready)
	select {
	case <-c.peer.Done():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// shutdown 取消连接上的全部工作，回收已发布和构造中的会话并报告清理失败。
func (c *conn) shutdown(cancel context.CancelFunc) error {
	cancel()
	c.mu.Lock()
	c.closed = true
	sessions := make([]*Session, 0, len(c.sessions))
	for _, s := range c.sessions {
		sessions = append(sessions, s)
	}
	for s := range c.pending {
		sessions = append(sessions, s)
	}
	c.mu.Unlock()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	for _, s := range sessions {
		wg.Go(func() { err := s.close(); mu.Lock(); errs = append(errs, err); mu.Unlock() })
	}
	wg.Wait()
	return errors.Join(errs...)
}

// conn 是一条 ACP 连接上的 Agent 实现。
type conn struct {
	srv   *Server
	base  context.Context
	peer  *acpsdk.AgentSideConnection
	ready chan struct{} // peer 赋值后关闭，SDK 处理请求的 goroutine 经 client() 读取 peer

	mu       sync.Mutex
	caps     acpsdk.ClientCapabilities
	sessions map[string]*Session
	cwds     map[string]string // 本连接新建或加载过的会话的工作区，供 session/list
	loading  map[string]bool   // 正在 session/load 的 ID，与 sessions 一起保证同一 ID 只打开一次
	closed   bool              // 连接已结束
	pending  map[*Session]bool
}

var (
	_ acpsdk.Agent       = (*conn)(nil)
	_ acpsdk.AgentLoader = (*conn)(nil)
)

func (c *conn) client() *acpsdk.AgentSideConnection {
	<-c.ready
	return c.peer
}

func (c *conn) capabilities() acpsdk.ClientCapabilities {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.caps
}

func internalError(op string, err error) error {
	return acpsdk.NewInternalError(map[string]any{"error": fmt.Sprintf("acp: %s：%v", op, err)})
}

func (c *conn) Initialize(_ context.Context, p acpsdk.InitializeRequest) (acpsdk.InitializeResponse, error) {
	c.mu.Lock()
	c.caps = p.ClientCapabilities
	c.mu.Unlock()
	caps := acpsdk.AgentCapabilities{
		LoadSession:         c.srv.store != nil,
		PromptCapabilities:  acpsdk.PromptCapabilities{EmbeddedContext: true},
		SessionCapabilities: acpsdk.SessionCapabilities{Close: &acpsdk.SessionCloseCapabilities{}},
	}
	if c.srv.store != nil {
		caps.SessionCapabilities.List = &acpsdk.SessionListCapabilities{}
	}
	return acpsdk.InitializeResponse{
		ProtocolVersion:   acpsdk.ProtocolVersionNumber,
		AgentCapabilities: caps,
		AgentInfo:         &acpsdk.Implementation{Name: c.srv.name, Version: c.srv.version},
		AuthMethods:       []acpsdk.AuthMethod{},
	}, nil
}

// Authenticate 不声明认证方式，任何请求都视为成功。
func (c *conn) Authenticate(context.Context, acpsdk.AuthenticateRequest) (acpsdk.AuthenticateResponse, error) {
	return acpsdk.AuthenticateResponse{}, nil
}

func (c *conn) NewSession(ctx context.Context, p acpsdk.NewSessionRequest) (acpsdk.NewSessionResponse, error) {
	id := "sess_" + uuid.NewString()
	s, err := c.build(ctx, id, p.Cwd, p.McpServers)
	if err != nil {
		return acpsdk.NewSessionResponse{}, err
	}
	if err := s.persistMetadata(ctx); err != nil {
		cleanupErr := c.discard(s)
		return acpsdk.NewSessionResponse{}, internalError("保存会话元数据", errors.Join(err, cleanupErr))
	}
	if err := c.register(s); err != nil {
		return acpsdk.NewSessionResponse{}, err
	}
	return acpsdk.NewSessionResponse{SessionId: acpsdk.SessionId(id)}, nil
}

// build 构造会话并调用 Factory，尚不登记。
func (c *conn) build(ctx context.Context, id, cwd string, mcp []acpsdk.McpServer) (*Session, error) {
	s, err := newSession(c, id, cwd, mcp)
	if err != nil {
		return nil, acpsdk.NewInvalidParams(map[string]any{"error": err.Error()})
	}
	// 构造期间也登记所有权；关闭必须等待 Factory 返回后才能释放资源。
	s.run <- struct{}{}
	c.mu.Lock()
	closed := c.closed
	if !closed {
		c.pending[s] = true
	}
	c.mu.Unlock()
	buildCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.root, cancel)
	var runner Runner
	if !closed {
		runner, err = c.srv.factory(buildCtx, s)
	} else {
		err = errors.New("连接已结束")
	}
	stop()
	cancel()
	s.runner = runner
	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	if err == nil && runner == nil {
		err = errors.New("Factory 返回了 nil Runner")
	}
	<-s.run
	// 成功对象由 register 移出 pending，避免构造与登记之间的断开窗口。
	if err != nil {
		cleanupErr := c.discard(s)
		return nil, internalError("构建 Agent", errors.Join(err, cleanupErr))
	}
	return s, nil
}

// register 登记会话；连接已结束时关闭它并报错。
func (c *conn) register(s *Session) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return internalError("打开会话", errors.Join(errors.New("连接已结束"), c.discard(s)))
	}
	delete(c.pending, s)
	c.sessions[s.id] = s
	c.cwds[s.id] = s.cwd
	c.mu.Unlock()
	return nil
}

func missing(id acpsdk.SessionId) error {
	return acpsdk.NewInvalidParams(map[string]any{"error": fmt.Sprintf("acp: 会话 %q 不存在", id)})
}

func (c *conn) session(id acpsdk.SessionId) (*Session, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.sessions[string(id)]
	if !ok || c.loading[string(id)] {
		return nil, missing(id)
	}
	return s, nil
}

// CloseSession 取消进行中的 prompt 并关闭会话，之后才注销；等待期间会话仍登记，同 ID 的 session/load 被拒绝。
func (c *conn) CloseSession(_ context.Context, p acpsdk.CloseSessionRequest) (acpsdk.CloseSessionResponse, error) {
	s, err := c.session(p.SessionId)
	if err != nil {
		return acpsdk.CloseSessionResponse{}, err
	}
	if err := s.close(); err != nil {
		return acpsdk.CloseSessionResponse{}, internalError("关闭会话", err)
	}
	c.mu.Lock()
	if c.sessions[string(p.SessionId)] == s {
		delete(c.sessions, string(p.SessionId))
	}
	c.mu.Unlock()
	return acpsdk.CloseSessionResponse{}, nil
}

// Cancel 取消该会话当前的 prompt；不能依赖 SDK 记录的取消函数，重叠的 prompt 会让它丢失。未知会话忽略。
func (c *conn) Cancel(_ context.Context, n acpsdk.CancelNotification) error {
	if s, err := c.session(n.SessionId); err == nil {
		s.cancelTurn()
	}
	return nil
}

func (c *conn) Logout(context.Context, acpsdk.LogoutRequest) (acpsdk.LogoutResponse, error) {
	return acpsdk.LogoutResponse{}, acpsdk.NewMethodNotFound(acpsdk.AgentMethodLogout)
}

func (c *conn) SetSessionMode(context.Context, acpsdk.SetSessionModeRequest) (acpsdk.SetSessionModeResponse, error) {
	return acpsdk.SetSessionModeResponse{}, acpsdk.NewMethodNotFound("session/set_mode")
}

func (c *conn) SetSessionConfigOption(context.Context, acpsdk.SetSessionConfigOptionRequest) (acpsdk.SetSessionConfigOptionResponse, error) {
	return acpsdk.SetSessionConfigOptionResponse{}, acpsdk.NewMethodNotFound("session/set_config_option")
}

func (c *conn) ResumeSession(context.Context, acpsdk.ResumeSessionRequest) (acpsdk.ResumeSessionResponse, error) {
	return acpsdk.ResumeSessionResponse{}, acpsdk.NewMethodNotFound("session/resume")
}

// discard 清理未发布的对象，清理超时仍保留同 ID 的所有权，直到实际释放完成。
func (c *conn) discard(s *Session) error {
	err := s.close()
	remove := func() { c.mu.Lock(); delete(c.pending, s); c.mu.Unlock() }
	select {
	case <-s.closeDone:
		remove()
	default:
		go func() { <-s.closeDone; remove() }()
	}
	return err
}

// WithTerminalCapabilities 由宿主显式声明编辑器终端的实际执行环境。
func WithTerminalCapabilities(caps env.Capabilities) Option {
	return func(s *Server) { copy := caps; s.terminalCaps = &copy }
}
