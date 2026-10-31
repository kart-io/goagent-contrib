package httpagent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kart-io/goagent/builder"
	"github.com/stretchr/testify/require"
)

// This transport deliberately has no Gorilla connection or framing dependency.
type memoryWebSocket struct {
	incoming chan []byte
	outgoing chan []byte
	closed   chan struct{}
	once     sync.Once
	limit    atomic.Int64
}

func newMemoryWebSocket() *memoryWebSocket {
	return &memoryWebSocket{incoming: make(chan []byte, 8), outgoing: make(chan []byte, 16), closed: make(chan struct{})}
}
func (c *memoryWebSocket) ReadText() ([]byte, error) {
	select {
	case b := <-c.incoming:
		return b, nil
	case <-c.closed:
		return nil, io.EOF
	}
}
func (c *memoryWebSocket) WriteText(b []byte) error {
	select {
	case c.outgoing <- append([]byte(nil), b...):
		return nil
	case <-c.closed:
		return io.ErrClosedPipe
	}
}
func (c *memoryWebSocket) Ping(time.Time) error             { return nil }
func (c *memoryWebSocket) SetReadLimit(n int64)             { c.limit.Store(n) }
func (c *memoryWebSocket) SetReadDeadline(time.Time) error  { return nil }
func (c *memoryWebSocket) SetWriteDeadline(time.Time) error { return nil }
func (c *memoryWebSocket) SetPongHandler(func() error)      {}
func (c *memoryWebSocket) Close() error                     { c.once.Do(func() { close(c.closed) }); return nil }
func (c *memoryWebSocket) send(t *testing.T, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	c.incoming <- b
}
func (c *memoryWebSocket) receive(t *testing.T) map[string]any {
	t.Helper()
	select {
	case b := <-c.outgoing:
		var f map[string]any
		require.NoError(t, json.Unmarshal(b, &f))
		return f
	case <-time.After(time.Second):
		t.Fatal("no frame from injected transport")
		return nil
	}
}

func TestCustomUpgraderUsesSharedWebSocketRuntime(t *testing.T) {
	conn := newMemoryWebSocket()
	called := make(chan WebSocketUpgradeOptions, 1)
	h, err := New(Config{Factory: answerFactory, WebSocketUpgrader: WebSocketUpgraderFunc(func(w http.ResponseWriter, r *http.Request, o WebSocketUpgradeOptions) (WebSocketConn, error) {
		called <- o
		return conn, nil
	})})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodGet, "http://agent.test/ws", nil)
	req.Header.Set("Sec-WebSocket-Protocol", "other, goagent.v1")
	done := make(chan struct{})
	go func() { defer close(done); h.ServeHTTP(httptest.NewRecorder(), req) }()
	options := <-called
	require.Equal(t, "goagent.v1", options.Subprotocol)
	require.Equal(t, 10*time.Second, options.HandshakeTimeout)
	conn.send(t, map[string]any{"type": "run", "request_id": "custom", "input": "framework input"})
	require.Equal(t, "accepted", conn.receive(t)["type"])
	frame := conn.receive(t)
	require.Equal(t, "framework input", frame["event"].(map[string]any)["output"].(map[string]any)["result"])
	require.Equal(t, "ready", conn.receive(t)["type"])
	require.EqualValues(t, 1<<20, conn.limit.Load())
	require.NoError(t, h.Shutdown(context.Background()))
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("custom transport not closed on shutdown")
	}
}

func TestFrameworkManagedConnectionCancelsAndIsOwned(t *testing.T) {
	for _, stop := range []string{"context", "shutdown"} {
		t.Run(stop, func(t *testing.T) {
			started, stopped := make(chan struct{}), make(chan struct{})
			h, err := New(Config{Factory: func(context.Context, RunRequest) (Binding, Cleanup, error) {
				return Answer(func(ctx context.Context, _ any) (*builder.AgentOutput, error) {
					close(started)
					<-ctx.Done()
					close(stopped)
					return nil, ctx.Err()
				}), nil, nil
			}})
			require.NoError(t, err)
			conn := newMemoryWebSocket()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- h.ServeWebSocket(ctx, conn) }()
			conn.send(t, map[string]any{"type": "run", "request_id": "framework", "input": "task"})
			require.Equal(t, "accepted", conn.receive(t)["type"])
			<-started
			if stop == "context" {
				cancel()
			} else {
				require.NoError(t, h.Shutdown(context.Background()))
			}
			select {
			case <-stopped:
			case <-time.After(time.Second):
				t.Fatal("framework connection left run active")
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("ServeWebSocket did not return")
			}
			require.NoError(t, h.Shutdown(context.Background()))
			select {
			case <-conn.closed:
			default:
				t.Fatal("framework connection not closed")
			}
		})
	}
}

func TestCustomUpgradeIsTrackedUntilItReturns(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	conn := newMemoryWebSocket()
	h, err := New(Config{Factory: answerFactory, WebSocketUpgrader: WebSocketUpgraderFunc(func(http.ResponseWriter, *http.Request, WebSocketUpgradeOptions) (WebSocketConn, error) {
		close(entered)
		<-release
		return conn, nil
	})})
	require.NoError(t, err)
	req := httptest.NewRequest("GET", "http://agent.test/ws", nil)
	req.Header.Set("Sec-WebSocket-Protocol", "goagent.v1")
	done := make(chan struct{})
	go func() { defer close(done); h.ServeHTTP(httptest.NewRecorder(), req) }()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, h.Shutdown(ctx), context.DeadlineExceeded)
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("late upgrade was not reclaimed")
	}
	require.NoError(t, h.Shutdown(context.Background()))
	select {
	case <-conn.closed:
	default:
		t.Fatal("late connection leaked")
	}
}

func TestCustomUpgraderCannotBypassHandshakePolicy(t *testing.T) {
	var calls atomic.Int32
	h, err := New(Config{Factory: answerFactory, WebSocketUpgrader: WebSocketUpgraderFunc(func(http.ResponseWriter, *http.Request, WebSocketUpgradeOptions) (WebSocketConn, error) {
		calls.Add(1)
		return nil, io.EOF
	})})
	require.NoError(t, err)
	for _, tc := range []struct {
		origin, protocol string
		status           int
	}{{"https://other.test", "goagent.v1", 403}, {"", "other", 426}, {"%%%", "goagent.v1", 403}} {
		req := httptest.NewRequest("GET", "http://agent.test/ws", nil)
		req.Header.Set("Sec-WebSocket-Protocol", tc.protocol)
		if tc.origin != "" {
			req.Header.Set("Origin", tc.origin)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		require.Equal(t, tc.status, w.Code)
	}
	require.Zero(t, calls.Load())
}

func TestFrameworkManagedConnectionRejectedAfterShutdown(t *testing.T) {
	h, err := New(Config{Factory: answerFactory})
	require.NoError(t, err)
	require.NoError(t, h.Shutdown(context.Background()))
	conn := newMemoryWebSocket()
	err = h.ServeWebSocket(context.Background(), conn)
	var f *Failure
	require.ErrorAs(t, err, &f)
	require.Equal(t, "unavailable", f.Code)
	select {
	case <-conn.closed:
	default:
		t.Fatal("rejected connection not closed")
	}
}

func TestInjectedUpgraderWithRealHTTPConnection(t *testing.T) {
	var upgrades atomic.Int32
	var origins atomic.Int32
	h, err := New(Config{Factory: answerFactory,
		CheckOrigin: func(*http.Request) bool { origins.Add(1); return true },
		WebSocketUpgrader: WebSocketUpgraderFunc(func(w http.ResponseWriter, r *http.Request, o WebSocketUpgradeOptions) (WebSocketConn, error) {
			upgrades.Add(1)
			return (gorillaWebSocketUpgrader{}).Upgrade(w, r, o)
		}),
	})
	require.NoError(t, err)
	s := httptest.NewServer(h)
	defer s.Close()
	defer h.Shutdown(context.Background())
	conn := dialWS(t, s)
	require.NoError(t, conn.WriteJSON(map[string]any{"type": "run", "request_id": "injected", "input": "actual wire"}))
	require.Equal(t, "accepted", readWS(t, conn)["type"])
	event := readWS(t, conn)
	require.Equal(t, "actual wire", event["event"].(map[string]any)["output"].(map[string]any)["result"])
	require.Equal(t, "ready", readWS(t, conn)["type"])
	require.EqualValues(t, 1, upgrades.Load())
	require.EqualValues(t, 1, origins.Load())
}

func TestFrameworkConnectionSharesCapacityAndContextAdmission(t *testing.T) {
	h, err := New(Config{Factory: answerFactory, MaxConnections: 1})
	require.NoError(t, err)
	first := newMemoryWebSocket()
	done := make(chan error, 1)
	go func() { done <- h.ServeWebSocket(context.Background(), first) }()
	first.send(t, map[string]any{"type": "run", "request_id": "one", "input": 1})
	require.Equal(t, "accepted", first.receive(t)["type"])
	require.Equal(t, "event", first.receive(t)["type"])
	require.Equal(t, "ready", first.receive(t)["type"])
	second := newMemoryWebSocket()
	err = h.ServeWebSocket(context.Background(), second)
	var f *Failure
	require.ErrorAs(t, err, &f)
	require.Equal(t, "busy", f.Code)
	select {
	case <-second.closed:
	default:
		t.Fatal("over-capacity connection leaked")
	}
	require.NoError(t, h.Shutdown(context.Background()))
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("connection not released")
	}

	h, err = New(Config{Factory: answerFactory})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	closed := newMemoryWebSocket()
	require.ErrorIs(t, h.ServeWebSocket(ctx, closed), context.Canceled)
	select {
	case <-closed.closed:
	default:
		t.Fatal("cancelled admission leaked connection")
	}
	require.Error(t, h.ServeWebSocket(context.Background(), nil))
	require.NoError(t, h.Shutdown(context.Background()))
}

func TestCustomTransportCannotRunOversizedMessage(t *testing.T) {
	var calls atomic.Int32
	h, err := New(Config{MaxInputBytes: 64, Factory: func(ctx context.Context, r RunRequest) (Binding, Cleanup, error) {
		calls.Add(1)
		return answerFactory(ctx, r)
	}})
	require.NoError(t, err)
	conn := newMemoryWebSocket()
	done := make(chan error, 1)
	go func() { done <- h.ServeWebSocket(context.Background(), conn) }()
	// The runtime also checks complete-message size even if a host adapter fails
	// to enforce its read limit. The adapter must still bound frame allocation.
	conn.incoming <- make([]byte, 128)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("oversized message was not rejected")
	}
	require.Zero(t, calls.Load())
	require.NoError(t, h.Shutdown(context.Background()))
}

func TestInvalidUpgradeResultDoesNotLookLikeHTTP200(t *testing.T) {
	h, err := New(Config{Factory: answerFactory, WebSocketUpgrader: WebSocketUpgraderFunc(func(http.ResponseWriter, *http.Request, WebSocketUpgradeOptions) (WebSocketConn, error) {
		return nil, nil
	})})
	require.NoError(t, err)
	req := httptest.NewRequest("GET", "http://agent.test/ws", nil)
	req.Header.Set("Sec-WebSocket-Protocol", "goagent.v1")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Contains(t, w.Body.String(), "websocket_upgrade_failed")
	require.NoError(t, h.Shutdown(context.Background()))
}
