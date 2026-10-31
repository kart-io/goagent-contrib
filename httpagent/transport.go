package httpagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

func statusOf(err error) int {
	switch classify(err).Code {
	case "invalid_request":
		return 400
	case "input_too_large":
		return 413
	case "conflict":
		return 409
	case "busy":
		return 429
	case "unavailable":
		return 503
	case "unsupported_capability", "unsupported_transport":
		return 501
	case "timeout", "execution_unconfirmed":
		return 504
	default:
		return 500
	}
}
func writeProblem(w http.ResponseWriter, status int, id string, f *Failure) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"run_id": id, "error": f})
}
func (h *Handler) writeProblem(w http.ResponseWriter, status int, id string, f *Failure) {
	h.writeJSON(w, status, map[string]any{"run_id": id, "error": f})
}
func (h *Handler) writeJSON(w http.ResponseWriter, status int, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		h.report("", "encode_response", err)
		writeProblem(w, 500, "", failure("output_encoding_failed", "响应编码失败", err))
		return
	}
	if err = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(h.cfg.WriteTimeout)); err != nil {
		h.report("", "write_deadline", err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, err = w.Write(data)
	h.report("", "write_response", err)
}
func hasFlush(w http.ResponseWriter) bool {
	for i := 0; i < 16; i++ {
		if _, ok := w.(interface{ FlushError() error }); ok {
			return true
		}
		if _, ok := w.(http.Flusher); ok {
			return true
		}
		u, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return false
		}
		w = u.Unwrap()
	}
	return false
}
func checkWriter(w http.ResponseWriter, stream bool) error {
	if stream && !hasFlush(w) {
		return http.ErrNotSupported
	}
	return http.NewResponseController(w).SetWriteDeadline(time.Time{})
}
func (h *Handler) decodeInput(w http.ResponseWriter, r *http.Request) (any, error) {
	r.Body = http.MaxBytesReader(w, r.Body, h.cfg.MaxInputBytes)
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	dec.DisallowUnknownFields()
	var body struct {
		Input json.RawMessage `json:"input"`
	}
	if err := dec.Decode(&body); err != nil {
		return nil, inputError(err)
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("输入必须只有一个 JSON 对象")
		}
		return nil, inputError(err)
	}
	if len(body.Input) == 0 || string(body.Input) == "null" {
		return nil, failure("invalid_request", "input 必须存在且非 null", nil)
	}
	var input any
	decode := json.NewDecoder(bytes.NewReader(body.Input))
	decode.UseNumber()
	if err := decode.Decode(&input); err != nil {
		return nil, inputError(err)
	}
	return input, nil
}
func inputError(err error) error {
	var limit *http.MaxBytesError
	if errors.As(err, &limit) {
		return failure("input_too_large", "输入超出容量限制", err)
	}
	return failure("invalid_request", "输入 JSON 无效", err)
}

type httpFrame struct {
	event Event
	data  []byte
	ack   chan error
}

func (h *Handler) serveSSE(w http.ResponseWriter, r *http.Request, input any) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	frames := make(chan httpFrame)
	sink := func(e Event, data []byte) error {
		f := httpFrame{event: e, data: data, ack: make(chan error, 1)}
		select {
		case frames <- f:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case err := <-f.ack:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	o, err := h.start(ctx, RunRequest{Input: input, Source: "stream"}, sink, true, nil)
	if err != nil {
		h.writeProblem(w, statusOf(err), "", classify(err))
		return
	}
	w.Header().Set("X-GoAgent-Run-ID", o.req.ID)
	committed := false
	heartbeat := time.NewTicker(h.cfg.Heartbeat)
	defer heartbeat.Stop()
	rc := http.NewResponseController(w)
	write := func(data []byte) error {
		if err := rc.SetWriteDeadline(time.Now().Add(h.cfg.WriteTimeout)); err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
		if err := rc.Flush(); err != nil {
			return err
		}
		return rc.SetWriteDeadline(time.Time{})
	}
	for {
		select {
		case f := <-frames:
			if !committed && f.event.Type == "error" {
				h.writeProblem(w, statusOf(f.event.Error), o.req.ID, f.event.Error)
				committed = true
				f.ack <- nil
				continue
			}
			if !committed {
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("Cache-Control", "no-cache")
				w.WriteHeader(200)
				committed = true
			}
			err := write([]byte(fmt.Sprintf("event: %s\ndata: %s\n\n", f.event.Type, f.data)))
			f.ack <- err
			if err != nil {
				o.cancel()
				return
			}
		case <-o.ready:
			if !committed {
				h.writeProblem(w, statusOf(o.err), o.req.ID, classify(o.err))
			}
			return
		case <-o.unconfirmed:
			if !committed {
				h.writeProblem(w, 504, o.req.ID, failure("execution_unconfirmed", "执行尚未确认退出", nil))
			}
			return
		case <-ctx.Done():
			o.cancel()
			return
		case <-heartbeat.C:
			if !committed {
				select {
				case <-o.prepared:
					w.Header().Set("Content-Type", "text/event-stream")
					w.Header().Set("Cache-Control", "no-cache")
					w.WriteHeader(200)
					committed = true
				default:
					continue
				}
			}
			if err := write([]byte(": heartbeat\n\n")); err != nil {
				o.cancel()
				return
			}
		}
	}
}
