package httpagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type wsFrame struct {
	data   []byte
	ack    chan error
	before func()
}
type wsPeer struct {
	h           *Handler
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	conn        *websocket.Conn
	closed      bool
	queuedBytes int64
	queuedCount int
	queue       chan wsFrame
	activeMu    sync.Mutex
	active      *owner
}

func (p *wsPeer) Close() error {
	p.cancel()
	p.mu.Lock()
	p.closed = true
	c := p.conn
	p.mu.Unlock()
	if c != nil {
		return c.Close()
	}
	return nil
}
func (p *wsPeer) enqueue(payload any, before func()) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if int64(len(data)) > p.h.cfg.MaxEventBytes {
		return failure("output_too_large", "WebSocket 消息超出容量", nil)
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return context.Canceled
	}
	if p.queuedCount >= p.h.cfg.WSQueueEvents || p.queuedBytes+int64(len(data)) > p.h.cfg.WSQueueBytes {
		p.mu.Unlock()
		_ = p.Close()
		return failure("slow_consumer", "WebSocket 输出队列已满", nil)
	}
	p.queuedCount++
	p.queuedBytes += int64(len(data))
	p.mu.Unlock()
	f := wsFrame{data: data, ack: make(chan error, 1), before: before}
	select {
	case p.queue <- f:
	case <-p.ctx.Done():
		p.releaseFrame(f)
		return p.ctx.Err()
	}
	select {
	case err = <-f.ack:
		return err
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}
func (p *wsPeer) releaseFrame(f wsFrame) {
	p.mu.Lock()
	p.queuedCount--
	p.queuedBytes -= int64(len(f.data))
	p.mu.Unlock()
}
func (p *wsPeer) writer(done chan<- struct{}) {
	defer close(done)
	defer p.Close()
	ping := time.NewTicker(p.h.cfg.PingInterval)
	defer ping.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case f := <-p.queue:
			if f.before != nil {
				f.before()
			}
			err := p.conn.SetWriteDeadline(time.Now().Add(p.h.cfg.WriteTimeout))
			if err == nil {
				err = p.conn.WriteMessage(websocket.TextMessage, f.data)
			}
			p.releaseFrame(f)
			f.ack <- err
			if err != nil {
				return
			}
		case <-ping.C:
			if err := p.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(p.h.cfg.WriteTimeout)); err != nil {
				return
			}
		}
	}
}
func (p *wsPeer) problem(requestID string, f *Failure) {
	if err := p.enqueue(map[string]any{"type": "error", "request_id": requestID, "error": f}, nil); err != nil {
		_ = p.Close()
	}
}
func (p *wsPeer) watch(o *owner) {
	select {
	case <-o.unconfirmed:
		_ = p.Close()
		return
	case <-p.ctx.Done():
		o.cancel()
		return
	case <-o.ready:
	}
	select {
	case <-p.ctx.Done():
		return
	case <-o.executionDone:
	}
	err := p.enqueue(map[string]any{"type": "ready"}, func() {
		p.activeMu.Lock()
		if p.active == o {
			p.active = nil
		}
		p.activeMu.Unlock()
	})
	if err != nil {
		_ = p.Close()
	}
}
func (h *Handler) serveWS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeProblem(w, 405, "", failure("method_not_allowed", "WebSocket 握手要求 GET", nil))
		return
	}
	selected := false
	for _, sub := range websocket.Subprotocols(r) {
		if sub == "goagent.v1" {
			selected = true
		}
	}
	if !selected {
		w.Header().Set("Sec-WebSocket-Protocol", "goagent.v1")
		writeProblem(w, 426, "", failure("unsupported_protocol", "要求 goagent.v1 子协议", nil))
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	p := &wsPeer{h: h, ctx: ctx, cancel: cancel, queue: make(chan wsFrame, h.cfg.WSQueueEvents)}
	h.mu.Lock()
	if h.closing {
		h.mu.Unlock()
		cancel()
		writeProblem(w, 503, "", failure("unavailable", "服务正在关闭", nil))
		return
	}
	if len(h.connections) >= h.cfg.MaxConnections {
		h.mu.Unlock()
		cancel()
		writeProblem(w, 429, "", failure("busy", "WebSocket 连接容量已满", nil))
		return
	}
	h.connections[p] = struct{}{}
	h.signalLocked()
	h.mu.Unlock()
	defer func() { _ = p.Close(); h.mu.Lock(); delete(h.connections, p); h.signalLocked(); h.mu.Unlock() }()
	up := websocket.Upgrader{Subprotocols: []string{"goagent.v1"}, HandshakeTimeout: h.cfg.WriteTimeout, CheckOrigin: h.cfg.CheckOrigin}
	c, err := up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = c.Close()
		return
	}
	p.conn = c
	p.mu.Unlock()
	c.SetReadLimit(h.cfg.MaxInputBytes)
	if err = c.SetReadDeadline(time.Now().Add(h.cfg.PongTimeout)); err != nil {
		return
	}
	c.SetPongHandler(func(string) error { return c.SetReadDeadline(time.Now().Add(h.cfg.PongTimeout)) })
	writerDone := make(chan struct{})
	go p.writer(writerDone)
	defer func() { _ = p.Close(); <-writerDone }()
	for {
		kind, data, err := c.ReadMessage()
		if err != nil {
			p.activeMu.Lock()
			if p.active != nil {
				p.active.cancel()
			}
			p.activeMu.Unlock()
			return
		}
		if kind != websocket.TextMessage {
			_ = c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseUnsupportedData, "仅支持 JSON 文本"), time.Now().Add(h.cfg.WriteTimeout))
			return
		}
		var message struct {
			Type      string          `json:"type"`
			RequestID string          `json:"request_id"`
			RunID     string          `json:"run_id"`
			Input     json.RawMessage `json:"input"`
		}
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		err = dec.Decode(&message)
		var extra any
		if err == nil && !errors.Is(dec.Decode(&extra), io.EOF) {
			err = errors.New("只能包含一个消息")
		}
		if err != nil {
			p.problem(message.RequestID, failure("invalid_request", "WebSocket JSON 无效", err))
			continue
		}
		switch message.Type {
		case "run":
			if message.RequestID == "" || message.RunID != "" || len(message.Input) == 0 || bytes.Equal(bytes.TrimSpace(message.Input), []byte("null")) {
				p.problem(message.RequestID, failure("invalid_request", "run 必须提供 request_id 和非空 input", nil))
				continue
			}
			var input any
			decode := json.NewDecoder(bytes.NewReader(message.Input))
			decode.UseNumber()
			if err = decode.Decode(&input); err != nil {
				p.problem(message.RequestID, failure("invalid_request", "input 无效", err))
				continue
			}
			p.activeMu.Lock()
			if p.active != nil {
				p.activeMu.Unlock()
				p.problem(message.RequestID, failure("busy", "连接仍有活动运行，请等待 ready", nil))
				continue
			}
			o, err := h.start(ctx, RunRequest{Input: input, Source: "ws"}, func(e Event, data []byte) error {
				return p.enqueue(map[string]any{"type": "event", "run_id": e.RunID, "event": json.RawMessage(data)}, nil)
			}, false, func(o *owner) error {
				return p.enqueue(map[string]any{"type": "accepted", "schema_version": 1, "request_id": message.RequestID, "run_id": o.req.ID}, nil)
			})
			if err != nil {
				p.activeMu.Unlock()
				p.problem(message.RequestID, classify(err))
				continue
			}
			p.active = o
			p.activeMu.Unlock()
			go p.watch(o)
		case "cancel":
			p.activeMu.Lock()
			o := p.active
			p.activeMu.Unlock()
			if o == nil || o.req.ID != message.RunID {
				p.problem(message.RequestID, failure("not_found", "此连接没有匹配的运行", nil))
				continue
			}
			if !o.requestCancel() {
				p.problem(message.RequestID, failure("already_finished", "运行终态已经确认", nil))
			}
		default:
			p.problem(message.RequestID, failure("invalid_request", "未知消息类型", nil))
		}
	}
}
