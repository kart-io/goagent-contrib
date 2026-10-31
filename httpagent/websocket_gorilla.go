package httpagent

import (
	"errors"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// Gorilla remains the zero-configuration default; runtime code uses interfaces.
type gorillaWebSocketUpgrader struct{}

func (gorillaWebSocketUpgrader) Upgrade(w http.ResponseWriter, r *http.Request, o WebSocketUpgradeOptions) (WebSocketConn, error) {
	up := websocket.Upgrader{Subprotocols: []string{o.Subprotocol}, HandshakeTimeout: o.HandshakeTimeout, CheckOrigin: func(*http.Request) bool { return true }}
	// Origin has already been checked by Handler, exactly once for all adapters.
	conn, err := up.Upgrade(w, r, nil)
	if err != nil {
		return nil, err
	}
	return &gorillaWebSocketConn{conn: conn, controlTimeout: o.HandshakeTimeout}, nil
}

type gorillaWebSocketConn struct {
	conn           *websocket.Conn
	controlTimeout time.Duration
}

var _ WebSocketConn = (*gorillaWebSocketConn)(nil)
var _ WebSocketUpgrader = gorillaWebSocketUpgrader{}

func (c *gorillaWebSocketConn) ReadText() ([]byte, error) {
	kind, data, err := c.conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	if kind != websocket.TextMessage {
		_ = c.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseUnsupportedData, "仅支持 JSON 文本"), time.Now().Add(c.controlTimeout))
		return nil, errors.New("WebSocket requires JSON text messages")
	}
	return data, nil
}
func (c *gorillaWebSocketConn) WriteText(data []byte) error {
	return c.conn.WriteMessage(websocket.TextMessage, data)
}
func (c *gorillaWebSocketConn) Ping(deadline time.Time) error {
	return c.conn.WriteControl(websocket.PingMessage, nil, deadline)
}
func (c *gorillaWebSocketConn) SetReadLimit(n int64)               { c.conn.SetReadLimit(n) }
func (c *gorillaWebSocketConn) SetReadDeadline(t time.Time) error  { return c.conn.SetReadDeadline(t) }
func (c *gorillaWebSocketConn) SetWriteDeadline(t time.Time) error { return c.conn.SetWriteDeadline(t) }
func (c *gorillaWebSocketConn) SetPongHandler(f func() error) {
	c.conn.SetPongHandler(func(string) error { return f() })
}
func (c *gorillaWebSocketConn) Close() error { return c.conn.Close() }
