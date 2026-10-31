package httpagent

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"
)

// Config 的零值字段使用文档默认值，负数配置在构造时拒绝。
type Config struct {
	Factory                                                                        Factory
	Timeout, DrainTimeout, CleanupTimeout, WriteTimeout, Heartbeat, DeliveryBudget time.Duration
	MaxInputBytes, MaxEventBytes                                                   int64
	MaxActiveRuns                                                                  int
	Observer                                                                       func(context.Context, RunEvent) error
	Report                                                                         func(context.Context, Report)
}

// Report 是独立运维错误；不改变业务终态。
type Report struct {
	RunID, Stage string
	Err          error
}

// Handler 持有运行及升级连接。宿主停服时必须显式 Shutdown。
type Handler struct {
	cfg         Config
	mu          sync.Mutex
	closing     bool
	runs        map[string]*owner
	connections map[io.Closer]struct{}
	changed     chan struct{}
}

func New(cfg Config) (*Handler, error) {
	if cfg.Factory == nil {
		return nil, errors.New("httpagent: Factory 不能为空")
	}
	defaults := []struct {
		p     *time.Duration
		value time.Duration
	}{{&cfg.Timeout, 5 * time.Minute}, {&cfg.DrainTimeout, 5 * time.Second}, {&cfg.CleanupTimeout, 5 * time.Second}, {&cfg.WriteTimeout, 10 * time.Second}, {&cfg.Heartbeat, 15 * time.Second}, {&cfg.DeliveryBudget, 5 * time.Second}}
	for _, d := range defaults {
		if *d.p < 0 {
			return nil, errors.New("httpagent: 超时配置不能为负数")
		}
		if *d.p == 0 {
			*d.p = d.value
		}
	}
	if cfg.MaxInputBytes < 0 || cfg.MaxEventBytes < 0 || cfg.MaxActiveRuns < 0 {
		return nil, errors.New("httpagent: 容量配置不能为负数")
	}
	if cfg.MaxInputBytes == 0 {
		cfg.MaxInputBytes = 1 << 20
	}
	if cfg.MaxEventBytes == 0 {
		cfg.MaxEventBytes = 1 << 20
	}
	if cfg.MaxActiveRuns == 0 {
		cfg.MaxActiveRuns = 64
	}
	return &Handler{cfg: cfg, runs: map[string]*owner{}, connections: map[io.Closer]struct{}{}, changed: make(chan struct{})}, nil
}
func (h *Handler) signalLocked() { close(h.changed); h.changed = make(chan struct{}) }

// BeginShutdown 先关闭准入再取消，不等待 HTTP 请求自行结束。
func (h *Handler) BeginShutdown() {
	h.mu.Lock()
	h.closing = true
	runs := make([]*owner, 0, len(h.runs))
	for _, o := range h.runs {
		runs = append(runs, o)
	}
	conns := make([]io.Closer, 0, len(h.connections))
	for c := range h.connections {
		conns = append(conns, c)
	}
	h.signalLocked()
	h.mu.Unlock()
	for _, o := range runs {
		o.cancel()
	}
	for _, c := range conns {
		_ = c.Close()
	}
}

// Shutdown 等待构造、运行、cleanup 和通知所有者退出，超时不释放仍在使用的资源。
func (h *Handler) Shutdown(ctx context.Context) error {
	h.BeginShutdown()
	for {
		h.mu.Lock()
		empty := len(h.runs) == 0 && len(h.connections) == 0
		changed := h.changed
		h.mu.Unlock()
		if empty {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}
func (h *Handler) report(run, stage string, err error) {
	if err == nil || h.cfg.Report == nil {
		return
	}
	defer func() { _ = recover() }()
	ctx, cancel := context.WithTimeout(context.Background(), h.cfg.CleanupTimeout)
	defer cancel()
	h.cfg.Report(ctx, Report{RunID: run, Stage: stage, Err: err})
}
func (h *Handler) observe(o *owner, e RunEvent) {
	if h.cfg.Observer == nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			h.report(o.req.ID, "observer", panicError(p))
		}
	}()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(o.ctx), h.cfg.DeliveryBudget)
	defer cancel()
	if err := h.cfg.Observer(ctx, e); err != nil {
		h.report(o.req.ID, "observer", err)
	}
}
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/invoke" && r.URL.Path != "/stream" {
		writeProblem(w, 404, "", failure("not_found", "入口不存在", nil))
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeProblem(w, 405, "", failure("method_not_allowed", "此入口要求 POST", nil))
		return
	}
	input, err := h.decodeInput(w, r)
	if err != nil {
		writeProblem(w, statusOf(err), "", classify(err))
		return
	}
	if err = checkWriter(w, r.URL.Path == "/stream"); err != nil {
		writeProblem(w, 501, "", failure("unsupported_transport", "HTTP writer 不支持所需写入控制", err))
		return
	}
	if r.URL.Path == "/stream" {
		h.serveSSE(w, r, input)
		return
	}
	o, err := h.start(r.Context(), RunRequest{Input: input, Source: "invoke"}, nil, false, nil)
	if err != nil {
		writeProblem(w, statusOf(err), "", classify(err))
		return
	}
	w.Header().Set("X-GoAgent-Run-ID", o.req.ID)
	select {
	case <-o.ready:
		if o.err != nil {
			h.writeProblem(w, statusOf(o.err), o.req.ID, classify(o.err))
		} else {
			h.writeJSON(w, 200, map[string]any{"schema_version": 1, "run_id": o.req.ID, "output": o.out})
		}
	case <-o.unconfirmed:
		h.writeProblem(w, 504, o.req.ID, failure("execution_unconfirmed", "执行尚未确认退出，不应自动重新提交", nil))
	case <-r.Context().Done():
		o.cancel()
	}
}
