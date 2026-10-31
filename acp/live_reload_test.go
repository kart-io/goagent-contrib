package acp

import (
	"context"
	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/kart-io/goagent/core/session"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
)

func TestLiveClientCanReloadCurrentSession(t *testing.T) {
	store := session.NewMemoryStore()
	h := start(t, NewServer(plainFactory(newModel(say("旧回答")), nil), WithSessionStore(store)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	dir := t.TempDir()
	id := h.newSession(t, dir)
	_, err := h.prompt(t, id, "你好")
	require.NoError(t, err)
	_, err = h.client.LoadSession(context.Background(), acpsdk.LoadSessionRequest{SessionId: id, Cwd: dir, McpServers: []acpsdk.McpServer{}})
	require.NoError(t, err, "ACP 客户端可直接重载已存在的会话")
}

func TestReloadWaitsForOldResourcesBeforeFactory(t *testing.T) {
	store := session.NewMemoryStore()
	var factories atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	h := start(t, NewServer(func(ctx context.Context, s *Session) (Runner, error) {
		if factories.Add(1) == 1 {
			require.NoError(t, s.OnClose(func(context.Context) error { close(entered); <-release; return nil }))
		}
		return plainFactory(newModel(), nil)(ctx, s)
	}, WithSessionStore(store)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	dir := t.TempDir()
	id := h.newSession(t, dir)
	done := make(chan error, 1)
	go func() {
		_, err := h.client.LoadSession(context.Background(), acpsdk.LoadSessionRequest{SessionId: id, Cwd: dir, McpServers: []acpsdk.McpServer{}})
		done <- err
	}()
	// 测试失败时也解除等待，不让清理遗留阻塞 goroutine。
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	recv(t, entered, "旧会话资源释放")
	require.EqualValues(t, 1, factories.Load(), "旧资源释放前不能调用新 Factory")
	close(release)
	require.NoError(t, recv(t, done, "同连接重载"))
	require.EqualValues(t, 2, factories.Load())
}
