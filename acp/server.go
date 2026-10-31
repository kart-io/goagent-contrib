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
)

// Factory 为每个 ACP 会话构建一个 Agent。构建时必须 WithSessionID(s.ID())，工具经 s.Pipeline()
// 所在的 tools.Dispatcher 注册；配置了会话存储时还须 WithSessionLog 使用同一个 Store。
type Factory func(ctx context.Context, s *Session) (Runner, error)

// Runner 是本包对 Agent 的最小要求，*builder.ConfigurableAgent[C, S] 满足它。
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
}

// NewServer 创建 Server。
func NewServer(f Factory, opts ...Option) *Server {
	s := &Server{factory: f, name: "goagent", version: "dev", kind: func(string) acpsdk.ToolKind { return acpsdk.ToolKindOther }}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Serve 处理一条连接，直到对端断开（返回 nil）或 ctx 结束（返回 ctx.Err()）。返回时取消全部进行中的 prompt；
// SDK 的读循环在 in 关闭后才退出，ctx 结束后调用方应关闭 in。
func (s *Server) Serve(ctx context.Context, in io.Reader, out io.Writer) error {
	if s.factory == nil {
		return errors.New("acp: Factory 不能为空")
	}
	base, cancel := context.WithCancel(ctx)
	defer cancel()
	c := &conn{srv: s, base: base, ready: make(chan struct{}), sessions: map[string]*Session{}, cwds: map[string]string{}}
	c.peer = acpsdk.NewAgentSideConnection(c, out, in)
	close(c.ready)
	select {
	case <-c.peer.Done():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
	if _, err := c.open(ctx, id, p.Cwd, p.McpServers); err != nil {
		return acpsdk.NewSessionResponse{}, err
	}
	return acpsdk.NewSessionResponse{SessionId: acpsdk.SessionId(id)}, nil
}

// open 构造会话并调用 Factory，成功后登记；同 ID 的旧会话被替换。
func (c *conn) open(ctx context.Context, id, cwd string, mcp []acpsdk.McpServer) (*Session, error) {
	s, err := newSession(c, id, cwd, mcp)
	if err != nil {
		return nil, acpsdk.NewInvalidParams(map[string]any{"error": err.Error()})
	}
	runner, err := c.srv.factory(ctx, s)
	if err != nil {
		return nil, internalError("构建 Agent", err)
	}
	if runner == nil {
		return nil, internalError("构建 Agent", errors.New("Factory 返回了 nil Runner"))
	}
	s.runner = runner
	c.mu.Lock()
	c.sessions[id] = s
	c.cwds[id] = cwd
	c.mu.Unlock()
	return s, nil
}

func (c *conn) session(id acpsdk.SessionId) (*Session, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.sessions[string(id)]
	if !ok {
		return nil, acpsdk.NewInvalidParams(map[string]any{"error": fmt.Sprintf("acp: 会话 %q 不存在", id)})
	}
	return s, nil
}

func (c *conn) CloseSession(_ context.Context, p acpsdk.CloseSessionRequest) (acpsdk.CloseSessionResponse, error) {
	if _, err := c.session(p.SessionId); err != nil {
		return acpsdk.CloseSessionResponse{}, err
	}
	c.mu.Lock()
	delete(c.sessions, string(p.SessionId))
	c.mu.Unlock()
	return acpsdk.CloseSessionResponse{}, nil
}

// Cancel 无需处理：SDK 收到 session/cancel 时已取消该会话 prompt 的 ctx。
func (c *conn) Cancel(context.Context, acpsdk.CancelNotification) error { return nil }

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

func (c *conn) LoadSession(context.Context, acpsdk.LoadSessionRequest) (acpsdk.LoadSessionResponse, error) {
	return acpsdk.LoadSessionResponse{}, acpsdk.NewMethodNotFound(acpsdk.AgentMethodSessionLoad)
}

func (c *conn) ListSessions(context.Context, acpsdk.ListSessionsRequest) (acpsdk.ListSessionsResponse, error) {
	return acpsdk.ListSessionsResponse{}, acpsdk.NewMethodNotFound(acpsdk.AgentMethodSessionList)
}

// Prompt 在 Task 2 实现，此处先满足接口。
func (c *conn) Prompt(context.Context, acpsdk.PromptRequest) (acpsdk.PromptResponse, error) {
	return acpsdk.PromptResponse{}, acpsdk.NewMethodNotFound(acpsdk.AgentMethodSessionPrompt)
}
