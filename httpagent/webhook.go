package httpagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Incoming 是宿主已验证并映射的源事件。PayloadHash 由适配器按原始字节计算。
type Incoming struct {
	Source, EventID, Tenant, Receiver, PayloadHash string
	Input                                          any
	Reply                                          *WebhookReply
}
type Receipt struct {
	RunID     string `json:"run_id"`
	Duplicate bool   `json:"duplicate"`
}
type WebhookReply struct {
	Status int
	Body   any
}

// Acceptor 必须持久保存接纳及去重结果，重复事件复用 RunID，重复 ID 不同 payload 须拒绝。
type Acceptor interface {
	Accept(context.Context, Incoming) (Receipt, error)
}
type AcceptorFunc func(context.Context, Incoming) (Receipt, error)

func (f AcceptorFunc) Accept(ctx context.Context, in Incoming) (Receipt, error) { return f(ctx, in) }

type Receiver struct {
	Mode         string
	Decode       func(context.Context, *http.Request, []byte) (Incoming, error)
	Acceptor     Acceptor
	AcceptStatus int
}
type DeliverySink interface {
	Deliver(context.Context, RunEvent) error
}
type Target struct {
	URL     string
	Headers http.Header
	Events  []string
}
type SenderConfig struct {
	Targets []Target
	Client  *http.Client
	Timeout time.Duration
}
type Sender struct {
	targets []Target
	client  *http.Client
	timeout time.Duration
}

func NewSender(cfg SenderConfig) (*Sender, error) {
	if cfg.Timeout < 0 {
		return nil, errors.New("webhook 超时不能为负数")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	client := http.Client{}
	if cfg.Client != nil {
		client = *cfg.Client
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	s := &Sender{client: &client, timeout: cfg.Timeout}
	for _, target := range cfg.Targets {
		u, err := url.Parse(target.URL)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
			return nil, errors.New("webhook 目标必须是有效 HTTP URL")
		}
		target.Headers = target.Headers.Clone()
		target.Events = append([]string(nil), target.Events...)
		s.targets = append(s.targets, target)
	}
	return s, nil
}
func (s *Sender) Deliver(ctx context.Context, e RunEvent) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	var errs []error
	for _, target := range s.targets {
		selected := len(target.Events) == 0 && e.EventType != "run.started"
		for _, kind := range target.Events {
			if kind == e.EventType {
				selected = true
			}
		}
		if !selected {
			continue
		}
		if err = ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		func() {
			attempt, cancel := context.WithTimeout(ctx, s.timeout)
			defer cancel()
			req, err := http.NewRequestWithContext(attempt, http.MethodPost, target.URL, bytes.NewReader(data))
			if err != nil {
				errs = append(errs, err)
				return
			}
			req.Header = target.Headers.Clone()
			if req.Header == nil {
				req.Header = make(http.Header)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-GoAgent-Event-ID", e.EventID)
			resp, err := s.client.Do(req)
			if err != nil {
				errs = append(errs, err)
				return
			}
			_, readErr := io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			closeErr := resp.Body.Close()
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				errs = append(errs, fmt.Errorf("webhook 响应状态 %d", resp.StatusCode))
			}
			errs = append(errs, readErr, closeErr)
		}()
	}
	return errors.Join(errs...)
}
func cloneNotice(e RunEvent) RunEvent {
	data, _ := json.Marshal(e)
	var clone RunEvent
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	_ = d.Decode(&clone)
	return clone
}
func (h *Handler) publish(o *owner, e RunEvent) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(o.ctx), h.cfg.DeliveryBudget)
	defer cancel()
	h.publishContext(ctx, o, e)
}
func (h *Handler) publishContext(ctx context.Context, o *owner, e RunEvent) {
	h.observe(ctx, o, cloneNotice(e))
	if h.cfg.Delivery != nil {
		func() {
			defer func() {
				if p := recover(); p != nil {
					h.report(o.req.ID, "delivery", panicError(p))
				}
			}()
			if err := ctx.Err(); err != nil {
				h.report(o.req.ID, "delivery", err)
				return
			}
			h.report(o.req.ID, "delivery", h.cfg.Delivery.Deliver(ctx, cloneNotice(e)))
		}()
	}
}
func (h *Handler) decodeWebhook(ctx context.Context, r *http.Request, body []byte, name string, receiver Receiver) (Incoming, error) {
	in, err := receiver.Decode(ctx, r, body)
	if err != nil {
		return Incoming{}, failure("invalid_request", "Webhook 映射失败", err)
	}
	in.Receiver = name
	sum := sha256.Sum256(body)
	in.PayloadHash = hex.EncodeToString(sum[:])
	if in.Reply != nil {
		return in, nil
	}
	if in.Input == nil {
		return Incoming{}, failure("invalid_request", "Webhook 缺少业务输入", nil)
	}
	if receiver.Mode == "queued" && (in.Source == "" || in.EventID == "") {
		return Incoming{}, failure("invalid_request", "queued Webhook 必须提供 source 与 event_id", nil)
	}
	return in, nil
}
func (h *Handler) serveWebhook(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/webhooks/")
	receiver, ok := h.cfg.Receivers[name]
	if !ok {
		writeProblem(w, 404, "", failure("not_found", "Webhook 接收器不存在", nil))
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeProblem(w, 405, "", failure("method_not_allowed", "Webhook 要求 POST", nil))
		return
	}
	if err := checkWriter(w, false); err != nil {
		writeProblem(w, 501, "", failure("unsupported_transport", "HTTP writer 不支持写入期限", err))
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.cfg.MaxInputBytes))
	if err != nil {
		writeProblem(w, statusOf(inputError(err)), "", classify(inputError(err)))
		return
	}
	if receiver.Mode == "inline" {
		var reply *WebhookReply
		o, err := h.start(r.Context(), RunRequest{Source: "webhook", Webhook: name}, nil, false, func(o *owner) error {
			in, err := h.decodeWebhook(o.ctx, r, body, name, receiver)
			if err != nil {
				return err
			}
			if in.Reply != nil {
				reply = in.Reply
				return failure("webhook_reply", "Webhook 接收器应答", nil)
			}
			o.req.Input = in.Input
			return nil
		})
		if err != nil {
			h.writeProblem(w, statusOf(err), "", classify(err))
			return
		}
		select {
		case <-o.ready:
			o.waitNotice(r.Context())
			if reply != nil {
				h.writeJSON(w, reply.Status, reply.Body)
			} else {
				h.respondResult(w, o)
			}
		case <-o.unconfirmed:
			h.writeProblem(w, 504, o.req.ID, failure("execution_unconfirmed", "Webhook 执行尚未确认退出", nil))
		case <-r.Context().Done():
			o.cancel()
		}
		return
	}
	admission, cancel := context.WithTimeout(r.Context(), h.cfg.AdmissionTimeout)
	defer cancel()
	o, err := h.reserve(admission, RunRequest{Source: "admission", Webhook: name}, nil)
	if err != nil {
		h.writeProblem(w, statusOf(err), "", classify(err))
		return
	}
	type accepted struct {
		receipt Receipt
		reply   *WebhookReply
		err     error
	}
	done := make(chan accepted, 1)
	go func() {
		defer o.finish()
		stop := context.AfterFunc(o.ctx, o.note)
		defer stop()
		defer o.returned()
		result := accepted{}
		func() {
			defer func() {
				if p := recover(); p != nil {
					result.err = panicError(p)
				}
			}()
			in, err := h.decodeWebhook(o.ctx, r, body, name, receiver)
			if err != nil {
				result.err = err
				return
			}
			if in.Reply != nil {
				result.reply = in.Reply
				return
			}
			result.receipt, result.err = receiver.Acceptor.Accept(o.ctx, in)
			if result.err != nil {
				var f *Failure
				if !errors.As(result.err, &f) {
					result.err = failure("unavailable", "Webhook 接纳结果未确认", result.err)
				}
			} else if result.receipt.RunID == "" {
				result.err = failure("unavailable", "接纳器没有持久运行 ID", nil)
			}
		}()
		done <- result
	}()
	select {
	case result := <-done:
		if result.err != nil {
			h.writeProblem(w, statusOf(result.err), "", classify(result.err))
			return
		}
		if result.reply != nil {
			h.writeJSON(w, result.reply.Status, result.reply.Body)
			return
		}
		w.Header().Set("X-GoAgent-Run-ID", result.receipt.RunID)
		status := receiver.AcceptStatus
		if status == 0 {
			status = 202
		}
		h.writeJSON(w, status, result.receipt)
	case <-admission.Done():
		h.writeProblem(w, 503, "", failure("unavailable", "接纳期限已到，结果未确认", admission.Err()))
	}
}
