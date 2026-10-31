package httpagent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/kart-io/goagent/builder"
	"github.com/stretchr/testify/require"
)

func dialWS(t *testing.T, server *httptest.Server) *websocket.Conn {
	t.Helper()
	d := websocket.Dialer{Subprotocols: []string{"goagent.v1"}}
	c, _, err := d.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/ws", nil)
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	return c
}
func readWS(t *testing.T, c *websocket.Conn) map[string]any {
	t.Helper()
	require.NoError(t, c.SetReadDeadline(time.Now().Add(time.Second)))
	var frame map[string]any
	require.NoError(t, c.ReadJSON(&frame))
	return frame
}
func TestWebSocketConsecutiveRunsWaitForReady(t *testing.T) {
	h, err := New(Config{Factory: answerFactory})
	require.NoError(t, err)
	server := httptest.NewServer(h)
	defer server.Close()
	defer h.Shutdown(context.Background())
	c := dialWS(t, server)
	for _, id := range []string{"first", "second"} {
		require.NoError(t, c.WriteJSON(map[string]any{"type": "run", "request_id": id, "input": id}))
		ack := readWS(t, c)
		require.Equal(t, "accepted", ack["type"])
		require.Equal(t, id, ack["request_id"])
		ev := readWS(t, c)
		require.Equal(t, "done", ev["event"].(map[string]any)["type"])
		require.Equal(t, id, ev["event"].(map[string]any)["output"].(map[string]any)["result"])
		require.Equal(t, "ready", readWS(t, c)["type"])
	}
}
func TestWebSocketBusyCancelAndDisconnect(t *testing.T) {
	stopped := make(chan struct{}, 2)
	started := make(chan struct{}, 2)
	h, err := New(Config{Factory: func(context.Context, RunRequest) (Binding, Cleanup, error) {
		return Answer(func(ctx context.Context, _ any) (*builder.AgentOutput, error) {
			started <- struct{}{}
			<-ctx.Done()
			stopped <- struct{}{}
			return nil, ctx.Err()
		}), nil, nil
	}})
	require.NoError(t, err)
	server := httptest.NewServer(h)
	defer server.Close()
	defer h.Shutdown(context.Background())
	c := dialWS(t, server)
	require.NoError(t, c.WriteJSON(map[string]any{"type": "run", "request_id": "a", "input": 1}))
	id := readWS(t, c)["run_id"].(string)
	<-started
	require.NoError(t, c.WriteJSON(map[string]any{"type": "run", "request_id": "b", "input": 2}))
	require.Equal(t, "busy", readWS(t, c)["error"].(map[string]any)["code"])
	require.NoError(t, c.WriteJSON(map[string]any{"type": "cancel", "run_id": id}))
	event := readWS(t, c)["event"].(map[string]any)
	require.Equal(t, "cancelled", event["error"].(map[string]any)["code"])
	require.Equal(t, "ready", readWS(t, c)["type"])
	<-stopped
	require.NoError(t, c.WriteJSON(map[string]any{"type": "run", "request_id": "c", "input": 3}))
	require.Equal(t, "accepted", readWS(t, c)["type"])
	<-started
	c.Close()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("断线未取消执行")
	}
}
func TestWebSocketShutdownClosesHijackedConnection(t *testing.T) {
	h, err := New(Config{Factory: answerFactory, MaxConnections: 1})
	require.NoError(t, err)
	server := httptest.NewServer(h)
	defer server.Close()
	c := dialWS(t, server)
	d := websocket.Dialer{Subprotocols: []string{"goagent.v1"}}
	_, resp, err := d.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/ws", nil)
	require.Error(t, err)
	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	resp.Body.Close()
	require.NoError(t, h.Shutdown(context.Background()))
	_, _, err = c.ReadMessage()
	require.Error(t, err)
}
func TestWebSocketHeartbeatCancelsUnresponsivePeer(t *testing.T) {
	stopped := make(chan struct{})
	h, err := New(Config{Factory: func(context.Context, RunRequest) (Binding, Cleanup, error) {
		return Answer(func(ctx context.Context, _ any) (*builder.AgentOutput, error) {
			<-ctx.Done()
			close(stopped)
			return nil, ctx.Err()
		}), nil, nil
	}, PingInterval: 10 * time.Millisecond, PongTimeout: 45 * time.Millisecond})
	require.NoError(t, err)
	server := httptest.NewServer(h)
	defer server.Close()
	c := dialWS(t, server)
	require.NoError(t, c.WriteJSON(map[string]any{"type": "run", "request_id": "heartbeat", "input": 1}))
	require.Equal(t, "accepted", readWS(t, c)["type"])
	// 不再读取客户端帧，因而不会处理 Ping 或发送 Pong。
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("心跳期限没有取消运行")
	}
	require.NoError(t, h.Shutdown(context.Background()))
}

func TestWebSocketOversizedInputNeverInvokesFactory(t *testing.T) {
	var calls atomic.Int32
	h, err := New(Config{Factory: func(ctx context.Context, r RunRequest) (Binding, Cleanup, error) {
		calls.Add(1)
		return answerFactory(ctx, r)
	}, MaxInputBytes: 128})
	require.NoError(t, err)
	server := httptest.NewServer(h)
	defer server.Close()
	defer h.Shutdown(context.Background())
	c := dialWS(t, server)
	require.NoError(t, c.WriteJSON(map[string]any{"type": "run", "request_id": "large", "input": strings.Repeat("x", 256)}))
	_, _, err = c.ReadMessage()
	require.Error(t, err)
	require.Zero(t, calls.Load())
}
func TestWebSocketOversizedResultHasOneErrorAndReady(t *testing.T) {
	h, err := New(Config{Factory: func(context.Context, RunRequest) (Binding, Cleanup, error) {
		return Answer(func(context.Context, any) (*builder.AgentOutput, error) {
			return &builder.AgentOutput{Result: strings.Repeat("x", 2048)}, nil
		}), nil, nil
	}, MaxEventBytes: 512})
	require.NoError(t, err)
	server := httptest.NewServer(h)
	defer server.Close()
	defer h.Shutdown(context.Background())
	c := dialWS(t, server)
	require.NoError(t, c.WriteJSON(map[string]any{"type": "run", "request_id": "large", "input": 1}))
	require.Equal(t, "accepted", readWS(t, c)["type"])
	event := readWS(t, c)["event"].(map[string]any)
	require.Equal(t, "error", event["type"])
	require.Equal(t, "output_too_large", event["error"].(map[string]any)["code"])
	require.Equal(t, "ready", readWS(t, c)["type"])
}
