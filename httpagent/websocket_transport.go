package httpagent

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// WebSocketConn is a framework-neutral, text-message WebSocket connection.
// ReadText handles control frames and rejects binary messages. Implementations
// must enforce limits/deadlines and support one reader plus one writer. Close
// must be safe concurrently, unblock reads/writes, and tolerate repeated calls.
// The reader invokes the pong handler; Ping must not wait synchronously for Pong.
type WebSocketConn interface {
	ReadText() ([]byte, error)
	WriteText([]byte) error
	Ping(deadline time.Time) error
	SetReadLimit(int64)
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	SetPongHandler(func() error)
	Close() error
}

// WebSocketUpgradeOptions supplies protocol negotiation and the handshake bound.
// Handler checks origin and requested subprotocol before invoking the upgrader.
type WebSocketUpgradeOptions struct {
	Subprotocol      string
	HandshakeTimeout time.Duration
}

// WebSocketUpgrader integrates a host's HTTP upgrade implementation. It must
// negotiate the supplied subprotocol, honor the handshake timeout, and write
// an HTTP error response on failure. A successful connection belongs to Handler.
type WebSocketUpgrader interface {
	Upgrade(http.ResponseWriter, *http.Request, WebSocketUpgradeOptions) (WebSocketConn, error)
}

// WebSocketUpgraderFunc adapts a host function to WebSocketUpgrader.
type WebSocketUpgraderFunc func(http.ResponseWriter, *http.Request, WebSocketUpgradeOptions) (WebSocketConn, error)

func (f WebSocketUpgraderFunc) Upgrade(w http.ResponseWriter, r *http.Request, o WebSocketUpgradeOptions) (WebSocketConn, error) {
	return f(w, r, o)
}

// ServeWebSocket serves an already upgraded connection and closes it on return.
// It supports frameworks which do not expose net/http upgrade primitives. The
// host must authenticate, validate Origin, negotiate goagent.v1, and manage the
// pending handshake before calling. Runtime capacity, cancellation, heartbeat,
// cleanup and shutdown are shared with /ws. This call blocks until disconnect.
func (h *Handler) ServeWebSocket(ctx context.Context, conn WebSocketConn) error {
	if conn == nil {
		return errors.New("httpagent: WebSocket connection is nil")
	}
	p, err := h.reserveWebSocket(ctx)
	if err != nil {
		_ = conn.Close()
		return err
	}
	defer h.releaseWebSocket(p)
	return h.runWebSocket(p, conn)
}

func (h *Handler) reserveWebSocket(ctx context.Context) (*wsPeer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	p := &wsPeer{h: h, ctx: ctx, cancel: cancel, queue: make(chan wsFrame, h.cfg.WSQueueEvents)}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closing {
		cancel()
		return nil, failure("unavailable", "服务正在关闭", nil)
	}
	if len(h.connections) >= h.cfg.MaxConnections {
		cancel()
		return nil, failure("busy", "WebSocket 连接容量已满", nil)
	}
	h.connections[p] = struct{}{}
	h.signalLocked()
	return p, nil
}
func (h *Handler) releaseWebSocket(p *wsPeer) {
	_ = p.Close()
	h.mu.Lock()
	delete(h.connections, p)
	h.signalLocked()
	h.mu.Unlock()
}
