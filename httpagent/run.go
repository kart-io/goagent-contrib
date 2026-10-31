package httpagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/kart-io/goagent/builder"
)

type owner struct {
	outcomeMu sync.Mutex
	frozen    bool

	h                                           *Handler
	req                                         RunRequest
	ctx                                         context.Context
	cancel                                      context.CancelFunc
	ready, executionDone, finished, unconfirmed chan struct{}
	noteOnce                                    sync.Once
	timerMu                                     sync.Mutex
	waitTimer                                   *time.Timer
	runReturned                                 bool
	sink                                        func(Event, []byte) error
	out                                         *Output
	err                                         error
	seq                                         uint64
}

func (o *owner) requestCancel() bool {
	o.outcomeMu.Lock()
	defer o.outcomeMu.Unlock()
	if o.frozen {
		return false
	}
	o.cancel()
	return true
}
func panicError(p any) error {
	return failure("host_panic", fmt.Sprintf("宿主执行发生 panic：%v", p), nil)
}
func (o *owner) note() {
	o.noteOnce.Do(func() {
		o.timerMu.Lock()
		defer o.timerMu.Unlock()
		if o.runReturned {
			return
		}
		o.waitTimer = time.AfterFunc(o.h.cfg.DrainTimeout, func() {
			o.timerMu.Lock()
			ended := o.runReturned
			o.timerMu.Unlock()
			if ended {
				return
			}
			o.cancel()
			close(o.unconfirmed)
			o.h.report(o.req.ID, "execution_unconfirmed", errors.New("生产者/构造尚未确认退出"))
		})
	})
}
func (o *owner) returned() {
	o.timerMu.Lock()
	o.runReturned = true
	if o.waitTimer != nil {
		o.waitTimer.Stop()
	}
	o.timerMu.Unlock()
}
func (h *Handler) reserve(parent context.Context, req RunRequest, sink func(Event, []byte) error) (*owner, error) {
	h.mu.Lock()
	if h.closing {
		h.mu.Unlock()
		return nil, failure("unavailable", "服务正在关闭", nil)
	}
	if len(h.runs) >= h.cfg.MaxActiveRuns {
		h.mu.Unlock()
		return nil, failure("busy", "活动运行容量已满", nil)
	}
	if req.ID == "" {
		req.ID = uuid.NewString()
	}
	if _, ok := h.runs[req.ID]; ok {
		h.mu.Unlock()
		return nil, failure("busy", "此运行仍在执行", nil)
	}
	ctx, cancel := context.WithTimeout(parent, h.cfg.Timeout)
	o := &owner{h: h, req: req, ctx: ctx, cancel: cancel, sink: sink, ready: make(chan struct{}), executionDone: make(chan struct{}), finished: make(chan struct{}), unconfirmed: make(chan struct{})}
	h.runs[req.ID] = o
	h.signalLocked()
	h.mu.Unlock()
	return o, nil
}

func (h *Handler) start(parent context.Context, req RunRequest, sink func(Event, []byte) error, stream bool, before func(*owner) error) (*owner, error) {
	o, err := h.reserve(parent, req, sink)
	if err != nil {
		return nil, err
	}
	go o.work(stream, before)
	return o, nil
}
func (o *owner) finish() {
	o.cancel()
	o.h.mu.Lock()
	delete(o.h.runs, o.req.ID)
	o.h.signalLocked()
	o.h.mu.Unlock()
	close(o.finished)
}

// Run 供已持久接纳的 worker 使用同一执行管道。ID 由宿主接纳记录提供。
func (h *Handler) Run(ctx context.Context, req RunRequest) (*Output, error) {
	if req.Source == "" {
		req.Source = "worker"
	}
	o, err := h.start(ctx, req, nil, false, nil)
	if err != nil {
		return nil, err
	}
	select {
	case <-o.ready:
		return o.out, o.err
	case <-o.unconfirmed:
		return nil, failure("execution_unconfirmed", "执行尚未确认退出", nil)
	case <-ctx.Done():
		o.cancel()
		return nil, ctx.Err()
	}
}
func (o *owner) event(ev builder.AgentEvent) (Event, []byte, error) {
	switch ev.Type {
	case "sources", "step", "text", "usage", "done", "error":
	default:
		return Event{}, nil, failure("invalid_event", "未知执行事件类型", nil)
	}
	o.seq++
	e := Event{SchemaVersion: 1, RunID: o.req.ID, Seq: o.seq, Type: ev.Type, Sources: ev.Sources, Step: ev.Step, Tool: ev.Tool, Delta: ev.Delta, Usage: ev.Usage}
	if ev.Type == "done" {
		e.Output = outputOf(ev.Output)
	}
	if ev.Type == "error" {
		e.Error = classify(ev.Error)
		if ev.Error == nil {
			e.Error = failure("execution_failed", ev.Message, nil)
		}
	}
	data, err := json.Marshal(e)
	if err != nil {
		return Event{}, nil, failure("output_encoding_failed", "输出不能编码为 JSON", err)
	}
	if int64(len(data)) > o.h.cfg.MaxEventBytes {
		return Event{}, nil, failure("output_too_large", "输出事件超出容量限制", nil)
	}
	return e, data, nil
}
func (o *owner) emit(ev builder.AgentEvent) error {
	e, data, err := o.event(ev)
	if err != nil {
		return err
	}
	if o.sink != nil {
		return o.sink(e, data)
	}
	return nil
}
func (o *owner) lifecycle(kind string, data any) RunEvent {
	raw, err := json.Marshal(data)
	if err != nil {
		raw = []byte(`{}`)
	}
	return RunEvent{SchemaVersion: 1, EventID: uuid.NewString(), RunID: o.req.ID, EventType: kind, OccurredAt: time.Now().UTC(), Data: json.RawMessage(raw)}
}
func (o *owner) work(stream bool, before func(*owner) error) {
	defer o.finish()
	stop := context.AfterFunc(o.ctx, o.note)
	var cleanup Cleanup
	var output *builder.AgentOutput
	var runErr error
	started := false
	func() {
		defer func() {
			if p := recover(); p != nil {
				runErr = panicError(p)
			}
		}()
		if before != nil {
			if err := before(o); err != nil {
				runErr = err
				return
			}
		}
		if err := o.ctx.Err(); err != nil {
			runErr = err
			return
		}
		started = true
		o.h.publish(o, o.lifecycle("run.started", map[string]any{"source": o.req.Source, "webhook": o.req.Webhook}))
		ctx := context.WithValue(o.ctx, producerKey{}, func() { o.note() })
		binding, release, err := o.h.cfg.Factory(ctx, o.req)
		cleanup = release
		if err != nil {
			runErr = err
			return
		}
		if err = o.ctx.Err(); err != nil {
			runErr = err
			return
		}
		if binding.Run == nil {
			runErr = failure("invalid_binding", "Factory 未提供 Run", nil)
			return
		}
		if stream && !binding.Streaming {
			runErr = failure("unsupported_capability", "此执行绑定不支持流式", nil)
			return
		}
		output, runErr = binding.Run(ctx, o.req.Input, o.emit)
	}()
	stop()
	o.returned()
	if err := o.ctx.Err(); err != nil {
		runErr = err
	}
	if runErr == nil && output == nil {
		runErr = failure("invalid_terminal", "执行没有最终结果", nil)
	}
	ev := builder.AgentEvent{Type: "done", Output: output}
	if runErr != nil {
		ev = builder.AgentEvent{Type: "error", Error: runErr}
	}
	e, data, err := o.event(ev)
	if err != nil {
		runErr = err
		e, data, _ = o.event(builder.AgentEvent{Type: "error", Error: err})
	}
	if runErr == nil {
		var frozen Event
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.UseNumber()
		if err = dec.Decode(&frozen); err != nil {
			runErr = failure("output_encoding_failed", "最终结果快照失败", err)
		} else {
			o.out = frozen.Output
		}
	}
	o.outcomeMu.Lock()
	if latest := o.ctx.Err(); latest != nil && runErr == nil {
		runErr = latest
		o.out = nil
		e, data, _ = o.event(builder.AgentEvent{Type: "error", Error: latest})
	}
	o.err = runErr
	o.frozen = true
	o.outcomeMu.Unlock()
	if o.sink != nil {
		if err = o.sink(e, data); err != nil {
			o.h.report(o.req.ID, "transport", err)
		}
	}
	close(o.ready)
	if cleanup != nil {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(o.ctx), o.h.cfg.CleanupTimeout)
		timer := context.AfterFunc(ctx, func() { o.h.report(o.req.ID, "cleanup", ctx.Err()) })
		func() {
			defer func() {
				if p := recover(); p != nil {
					o.h.report(o.req.ID, "cleanup", panicError(p))
				}
			}()
			o.h.report(o.req.ID, "cleanup", cleanup(ctx))
		}()
		timer()
		cancel()
	}
	close(o.executionDone)
	kind := "run.completed"
	var payload any = o.out
	if o.err != nil {
		kind = "run.failed"
		if errors.Is(o.err, context.Canceled) || classify(o.err).Code == "cancelled" {
			kind = "run.cancelled"
		}
		payload = map[string]any{"error": classify(o.err)}
	}
	if started {
		o.h.publish(o, o.lifecycle(kind, payload))
	}
}
