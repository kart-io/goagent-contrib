package acp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"testing"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/kart-io/goagent/builder"
	"github.com/kart-io/goagent/core/session"
	"github.com/kart-io/goagent/interfaces"
	"github.com/kart-io/goagent/llm"
	"github.com/kart-io/goagent/llm/constants"
	"github.com/kart-io/goagent/tools"
	"github.com/stretchr/testify/require"
)

// fakeEditor 是测试中的 ACP 客户端：记录收到的通知与请求，按配置回答审批、文件与终端请求。
type fakeEditor struct {
	mu          sync.Mutex
	updates     []acpsdk.SessionNotification
	permissions []acpsdk.RequestPermissionRequest
	// answer 为空时选择 allow_once。
	answer func(context.Context, acpsdk.RequestPermissionRequest) acpsdk.RequestPermissionResponse
	files  map[string]string // 编辑器缓冲区：绝对路径 -> 内容
	writes []acpsdk.WriteTextFileRequest
	reads  []string
	// terminal 决定终端命令的输出与退出码；hang 为真时 wait_for_exit 阻塞到请求取消。
	terminal  func(acpsdk.CreateTerminalRequest) (output string, exit int)
	hang      bool
	created   []acpsdk.CreateTerminalRequest
	termCalls []string
}

func newEditor() *fakeEditor { return &fakeEditor{files: map[string]string{}} }

var _ acpsdk.Client = (*fakeEditor)(nil)

func (e *fakeEditor) SessionUpdate(_ context.Context, n acpsdk.SessionNotification) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.updates = append(e.updates, n)
	return nil
}

func (e *fakeEditor) RequestPermission(ctx context.Context, r acpsdk.RequestPermissionRequest) (acpsdk.RequestPermissionResponse, error) {
	e.mu.Lock()
	e.permissions = append(e.permissions, r)
	answer := e.answer
	e.mu.Unlock()
	if answer == nil {
		return acpsdk.RequestPermissionResponse{Outcome: acpsdk.NewRequestPermissionOutcomeSelected("allow_once")}, nil
	}
	return answer(ctx, r), nil
}

func (e *fakeEditor) ReadTextFile(_ context.Context, r acpsdk.ReadTextFileRequest) (acpsdk.ReadTextFileResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.reads = append(e.reads, r.Path)
	content, ok := e.files[r.Path]
	if !ok {
		return acpsdk.ReadTextFileResponse{}, acpsdk.NewInternalError(map[string]any{"error": "no buffer"})
	}
	return acpsdk.ReadTextFileResponse{Content: content}, nil
}

func (e *fakeEditor) WriteTextFile(_ context.Context, r acpsdk.WriteTextFileRequest) (acpsdk.WriteTextFileResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.writes = append(e.writes, r)
	e.files[r.Path] = r.Content
	return acpsdk.WriteTextFileResponse{}, nil
}

func (e *fakeEditor) CreateTerminal(_ context.Context, r acpsdk.CreateTerminalRequest) (acpsdk.CreateTerminalResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.created = append(e.created, r)
	e.termCalls = append(e.termCalls, "create")
	return acpsdk.CreateTerminalResponse{TerminalId: "term-1"}, nil
}

func (e *fakeEditor) result() (string, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.terminal == nil || len(e.created) == 0 {
		return "", 0
	}
	return e.terminal(e.created[len(e.created)-1])
}

func (e *fakeEditor) record(call string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.termCalls = append(e.termCalls, call)
}

func (e *fakeEditor) WaitForTerminalExit(ctx context.Context, _ acpsdk.WaitForTerminalExitRequest) (acpsdk.WaitForTerminalExitResponse, error) {
	e.record("wait")
	e.mu.Lock()
	hang := e.hang
	e.mu.Unlock()
	if hang {
		<-ctx.Done()
		return acpsdk.WaitForTerminalExitResponse{}, ctx.Err()
	}
	_, code := e.result()
	return acpsdk.WaitForTerminalExitResponse{ExitCode: acpsdk.Ptr(code)}, nil
}

func (e *fakeEditor) TerminalOutput(context.Context, acpsdk.TerminalOutputRequest) (acpsdk.TerminalOutputResponse, error) {
	e.record("output")
	out, _ := e.result()
	return acpsdk.TerminalOutputResponse{Output: out}, nil
}

func (e *fakeEditor) KillTerminal(context.Context, acpsdk.KillTerminalRequest) (acpsdk.KillTerminalResponse, error) {
	e.record("kill")
	return acpsdk.KillTerminalResponse{}, nil
}

func (e *fakeEditor) ReleaseTerminal(context.Context, acpsdk.ReleaseTerminalRequest) (acpsdk.ReleaseTerminalResponse, error) {
	e.record("release")
	return acpsdk.ReleaseTerminalResponse{}, nil
}

func (e *fakeEditor) notifications() []acpsdk.SessionNotification {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.updates)
}

// agentText 拼接全部 agent_message_chunk 的文本。
func (e *fakeEditor) agentText() string {
	var out string
	for _, n := range e.notifications() {
		if c := n.Update.AgentMessageChunk; c != nil && c.Content.Text != nil {
			out += c.Content.Text.Text
		}
	}
	return out
}

type harness struct {
	editor *fakeEditor
	client *acpsdk.ClientSideConnection
}

// start 用两对 io.Pipe 连接 Server 与 fakeEditor，测试结束时断开并等待 Serve 返回。
func start(t *testing.T, srv *Server, ed *fakeEditor) *harness {
	t.Helper()
	agentIn, editorOut := io.Pipe()
	editorIn, agentOut := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, agentIn, agentOut) }()
	h := &harness{editor: ed, client: acpsdk.NewClientSideConnection(ed, editorOut, editorIn)}
	t.Cleanup(func() {
		_ = editorOut.Close()
		_ = agentOut.Close()
		cancel()
		<-done
	})
	return h
}

func (h *harness) initialize(t *testing.T, caps acpsdk.ClientCapabilities) acpsdk.InitializeResponse {
	t.Helper()
	resp, err := h.client.Initialize(context.Background(), acpsdk.InitializeRequest{ProtocolVersion: acpsdk.ProtocolVersionNumber, ClientCapabilities: caps})
	require.NoError(t, err)
	return resp
}

func (h *harness) newSession(t *testing.T, cwd string) acpsdk.SessionId {
	t.Helper()
	resp, err := h.client.NewSession(context.Background(), acpsdk.NewSessionRequest{Cwd: cwd, McpServers: []acpsdk.McpServer{}})
	require.NoError(t, err)
	return resp.SessionId
}

func (h *harness) prompt(t *testing.T, id acpsdk.SessionId, text string) (acpsdk.PromptResponse, error) {
	t.Helper()
	return h.client.Prompt(context.Background(), acpsdk.PromptRequest{SessionId: id, Prompt: []acpsdk.ContentBlock{acpsdk.TextBlock(text)}})
}

// requireCode 断言 err 是指定码的 JSON-RPC 错误。
func requireCode(t *testing.T, err error, code int) {
	t.Helper()
	var re *acpsdk.RequestError
	require.True(t, errors.As(err, &re), "期望 JSON-RPC 错误，得到 %v", err)
	require.Equal(t, code, re.Code, "%v", err)
}

// turn 是一次模型请求的结果：parts 逐块作为增量发出；wait 为真时阻塞到 ctx 结束。
type turn struct {
	parts []string
	calls []llm.ToolCall
	wait  bool
}

func say(parts ...string) turn { return turn{parts: parts} }

func calls(names ...string) turn {
	var cs []llm.ToolCall
	for i, name := range names {
		c := llm.ToolCall{ID: "c" + string(rune('1'+i)), Type: "function"}
		c.Function.Name = name
		c.Function.Arguments = `{"q":"A1"}`
		cs = append(cs, c)
	}
	return turn{calls: cs}
}

func waitTurn() turn { return turn{wait: true} }

// scriptModel 按顺序给出回合并记录请求；started 在每次请求开始时收到一个信号。
type scriptModel struct {
	mu       sync.Mutex
	turns    []turn
	requests []*llm.CompletionRequest
	started  chan struct{}
}

func newModel(turns ...turn) *scriptModel {
	return &scriptModel{turns: turns, started: make(chan struct{}, 16)}
}

func (m *scriptModel) Provider() constants.Provider { return constants.ProviderCustom }
func (m *scriptModel) IsAvailable() bool            { return true }
func (m *scriptModel) Complete(context.Context, *llm.CompletionRequest) (*llm.CompletionResponse, error) {
	return nil, errors.New("测试模型只支持流式")
}
func (m *scriptModel) Chat(context.Context, []llm.Message) (*llm.CompletionResponse, error) {
	return nil, errors.New("测试模型只支持流式")
}
func (m *scriptModel) CompleteStream(ctx context.Context, req *llm.CompletionRequest) (<-chan *llm.StreamChunk, error) {
	return m.CompleteToolsStream(ctx, req, nil)
}
func (m *scriptModel) ChatStream(ctx context.Context, ms []llm.Message) (<-chan *llm.StreamChunk, error) {
	return m.CompleteToolsStream(ctx, &llm.CompletionRequest{Messages: ms}, nil)
}
func (m *scriptModel) CompleteToolsStream(ctx context.Context, req *llm.CompletionRequest, _ []interfaces.Tool) (<-chan *llm.StreamChunk, error) {
	m.mu.Lock()
	copied := *req
	copied.Messages = slices.Clone(req.Messages)
	m.requests = append(m.requests, &copied)
	i := len(m.requests) - 1
	m.mu.Unlock()
	m.started <- struct{}{}
	if i >= len(m.turns) {
		return nil, errors.New("发生了多余的模型请求")
	}
	t := m.turns[i]
	ch := make(chan *llm.StreamChunk, len(t.parts)+1)
	if t.wait {
		go func() {
			<-ctx.Done()
			ch <- &llm.StreamChunk{Error: ctx.Err()}
			close(ch)
		}()
		return ch, nil
	}
	var content string
	for _, p := range t.parts {
		content += p
		ch <- &llm.StreamChunk{Delta: p, Content: content}
	}
	last := &llm.StreamChunk{Content: content, Done: true, ToolCalls: t.calls, FinishReason: "stop", Usage: &llm.Usage{TotalTokens: 2}}
	if len(t.calls) > 0 {
		last.FinishReason = "tool_calls"
	}
	ch <- last
	close(ch)
	return ch, nil
}

func (m *scriptModel) request(i int) *llm.CompletionRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests[i]
}

func (m *scriptModel) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.requests)
}

// plainFactory 构建不带工具的 Agent，并把收到的会话交给 seen（可为 nil）。
func plainFactory(model llm.Client, seen chan<- *Session) Factory {
	return func(_ context.Context, s *Session) (Runner, error) {
		if seen != nil {
			seen <- s
		}
		agent, err := builder.NewSimpleBuilder(model).WithSessionID(s.ID()).WithMaxIterations(4).Build()
		if err != nil {
			return nil, err
		}
		return agent, nil
	}
}

// toolFactory 构建经 s.Pipeline() 注册工具的 Agent；ask 非 nil 时作为宿主的审批钩子追加，store 非 nil 时开启会话日志。
func toolFactory(model llm.Client, ask tools.PreHook, store session.Store, build func(*Session) []interfaces.Tool) Factory {
	return func(_ context.Context, s *Session) (Runner, error) {
		p := s.Pipeline()
		if ask != nil {
			p.Pre = append(p.Pre, ask)
		}
		reg := tools.NewRegistry()
		for _, tool := range build(s) {
			if err := reg.Register(tool); err != nil {
				return nil, err
			}
		}
		d, err := tools.NewDispatcher(reg, tools.DispatchConfig{Pipeline: p})
		if err != nil {
			return nil, err
		}
		b := builder.NewSimpleBuilder(model).WithSessionID(s.ID()).WithTools(d.Tools()...).WithMaxIterations(4)
		if store != nil {
			b = b.WithSessionLog(store)
		}
		agent, err := b.Build()
		if err != nil {
			return nil, err
		}
		return agent, nil
	}
}

// echoTool 返回固定结果，每次执行把 runs 加一；fail 非空时返回该错误。
func echoTool(name string, runs *int, fail string) interfaces.Tool {
	return tools.NewBaseTool(name, "测试工具 "+name, `{"type":"object","properties":{"q":{"type":"string"}}}`, func(context.Context, *interfaces.ToolInput) (*interfaces.ToolOutput, error) {
		if runs != nil {
			*runs++
		}
		if fail != "" {
			return nil, fmt.Errorf("%s", fail)
		}
		return &interfaces.ToolOutput{Success: true, Result: name + " 的结果"}, nil
	})
}

func only(ts ...interfaces.Tool) func(*Session) []interfaces.Tool {
	return func(*Session) []interfaces.Tool { return ts }
}

// askAll 是要求每次调用都审批的宿主钩子，带按工具名放行的会话授权。
func askAll(_ context.Context, c *tools.Call) (tools.PreDecision, error) {
	return tools.PreDecision{Kind: tools.PreAsk, Feedback: "需要确认", Grant: toolGrant(c.Tool)}, nil
}

type toolGrant string

func (g toolGrant) Describe() string           { return "工具 " + string(g) }
func (g toolGrant) Matches(c *tools.Call) bool { return c.Tool == string(g) }

// statuses 返回某个调用收到的状态序列：tool_call 记为 "start:<status>"，tool_call_update 记为状态。
func (e *fakeEditor) statuses(id string) []string {
	var out []string
	for _, n := range e.notifications() {
		switch u := n.Update; {
		case u.ToolCall != nil && string(u.ToolCall.ToolCallId) == id:
			out = append(out, "start:"+string(u.ToolCall.Status))
		case u.ToolCallUpdate != nil && string(u.ToolCallUpdate.ToolCallId) == id && u.ToolCallUpdate.Status != nil:
			out = append(out, string(*u.ToolCallUpdate.Status))
		}
	}
	return out
}

// lastUpdate 返回某个调用的最后一条 tool_call_update。
func (e *fakeEditor) lastUpdate(id string) *acpsdk.SessionToolCallUpdate {
	var last *acpsdk.SessionToolCallUpdate
	for _, n := range e.notifications() {
		if u := n.Update.ToolCallUpdate; u != nil && string(u.ToolCallId) == id {
			last = u
		}
	}
	return last
}
