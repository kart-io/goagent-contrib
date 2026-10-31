package acp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/kart-io/goagent/core/session"
	"github.com/stretchr/testify/require"
)

func TestInitializeReportsCapabilities(t *testing.T) {
	h := start(t, NewServer(plainFactory(newModel(), nil), WithAgentInfo("demo", "1.2.3")), newEditor())
	resp := h.initialize(t, acpsdk.ClientCapabilities{})
	require.EqualValues(t, acpsdk.ProtocolVersionNumber, resp.ProtocolVersion)
	require.Equal(t, "demo", resp.AgentInfo.Name)
	require.Equal(t, "1.2.3", resp.AgentInfo.Version)
	require.False(t, resp.AgentCapabilities.LoadSession, "未配置会话存储时不支持 session/load")
	require.Nil(t, resp.AgentCapabilities.SessionCapabilities.List)
	require.NotNil(t, resp.AgentCapabilities.SessionCapabilities.Close)
	require.True(t, resp.AgentCapabilities.PromptCapabilities.EmbeddedContext)
	require.False(t, resp.AgentCapabilities.PromptCapabilities.Image)

	h2 := start(t, NewServer(plainFactory(newModel(), nil), WithSessionStore(session.NewMemoryStore())), newEditor())
	resp = h2.initialize(t, acpsdk.ClientCapabilities{})
	require.True(t, resp.AgentCapabilities.LoadSession)
	require.NotNil(t, resp.AgentCapabilities.SessionCapabilities.List)
}

func TestNewSessionBuildsAgentPerSession(t *testing.T) {
	seen := make(chan *Session, 2)
	h := start(t, NewServer(plainFactory(newModel(), seen)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), []byte("磁盘内容"), 0o644))

	id1 := h.newSession(t, dir)
	id2 := h.newSession(t, dir)
	require.NotEqual(t, id1, id2)
	s := <-seen
	require.Equal(t, string(id1), s.ID())
	require.Equal(t, dir, s.Cwd())
	require.Empty(t, s.MCPServers())
	<-seen

	// 客户端未声明 fs 与 terminal 能力时使用本地实现。
	data, _, err := s.FileSystem().ReadFile(context.Background(), "a.txt")
	require.NoError(t, err)
	require.Equal(t, "磁盘内容", string(data))
	require.False(t, s.Executor().Capabilities().Remote)
}

func TestNewSessionErrors(t *testing.T) {
	h := start(t, NewServer(func(context.Context, *Session) (Runner, error) { return nil, errors.New("模型配置缺失") }), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})

	_, err := h.client.NewSession(context.Background(), acpsdk.NewSessionRequest{Cwd: "relative/dir", McpServers: []acpsdk.McpServer{}})
	requireCode(t, err, -32602)

	_, err = h.client.NewSession(context.Background(), acpsdk.NewSessionRequest{Cwd: t.TempDir(), McpServers: []acpsdk.McpServer{}})
	requireCode(t, err, -32603)
	require.ErrorContains(t, err, "模型配置缺失")
}

func TestCloseSessionAndUnsupportedMethods(t *testing.T) {
	h := start(t, NewServer(plainFactory(newModel(), nil)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	id := h.newSession(t, t.TempDir())

	_, err := h.client.CloseSession(context.Background(), acpsdk.CloseSessionRequest{SessionId: id})
	require.NoError(t, err)
	_, err = h.client.CloseSession(context.Background(), acpsdk.CloseSessionRequest{SessionId: id})
	requireCode(t, err, -32602)

	_, err = h.client.Authenticate(context.Background(), acpsdk.AuthenticateRequest{MethodId: "none"})
	require.NoError(t, err)
	_, err = h.client.Logout(context.Background(), acpsdk.LogoutRequest{})
	requireCode(t, err, -32601)
	_, err = h.client.LoadSession(context.Background(), acpsdk.LoadSessionRequest{SessionId: id, Cwd: t.TempDir(), McpServers: []acpsdk.McpServer{}})
	requireCode(t, err, -32601)
	_, err = h.client.ListSessions(context.Background(), acpsdk.ListSessionsRequest{})
	requireCode(t, err, -32601)
}

func TestServeRequiresFactoryAndReturnsOnDisconnect(t *testing.T) {
	require.ErrorContains(t, NewServer(nil).Serve(context.Background(), nil, nil), "Factory")
}
