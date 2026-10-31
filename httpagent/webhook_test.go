package httpagent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kart-io/goagent/builder"
	"github.com/stretchr/testify/require"
)

// 磁盘接纳器是宿主契约的测试实现，重建实例后仍读取同一持久记录。
type diskAcceptor struct {
	mu   sync.Mutex
	path string
}

func (a *diskAcceptor) Accept(_ context.Context, in Incoming) (Receipt, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if data, err := os.ReadFile(a.path); err == nil {
		var old Receipt
		if err = json.Unmarshal(data, &old); err != nil {
			return Receipt{}, err
		}
		old.Duplicate = true
		return old, nil
	}
	r := Receipt{RunID: "persisted-run-1"}
	data, _ := json.Marshal(r)
	return r, os.WriteFile(a.path, data, 0600)
}
func decodeWebhook(_ context.Context, _ *http.Request, body []byte) (Incoming, error) {
	var x map[string]any
	if err := json.Unmarshal(body, &x); err != nil {
		return Incoming{}, err
	}
	return Incoming{Source: "orders", EventID: "evt-1", Input: x["input"]}, nil
}
func TestQueuedAcknowledgesPersistentAdmissionWithoutRunningAgent(t *testing.T) {
	var runs atomic.Int32
	cfg := Config{Factory: func(context.Context, RunRequest) (Binding, Cleanup, error) {
		runs.Add(1)
		return answerFactory(context.Background(), RunRequest{Input: "ok"})
	}, Receivers: map[string]Receiver{"orders": {Mode: "queued", Decode: decodeWebhook, Acceptor: &diskAcceptor{path: filepath.Join(t.TempDir(), "accepted.json")}}}}
	h, err := New(cfg)
	require.NoError(t, err)
	server := httptest.NewServer(h)
	defer server.Close()
	resp := post(t, server, "/webhooks/orders", `{"input":"订单"}`)
	require.Equal(t, 202, resp.StatusCode)
	require.Zero(t, runs.Load())
	var first Receipt
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&first))
	require.Equal(t, "persisted-run-1", first.RunID)
	// 独立接纳器实例验证从磁盘恢复重复投递，不是内存计数假象。
	cfg.Receivers["orders"] = Receiver{Mode: "queued", Decode: decodeWebhook, Acceptor: &diskAcceptor{path: cfg.Receivers["orders"].Acceptor.(*diskAcceptor).path}}
	second, err := New(cfg)
	require.NoError(t, err)
	s2 := httptest.NewServer(second)
	defer s2.Close()
	retry := post(t, s2, "/webhooks/orders", `{"input":"订单"}`)
	var again Receipt
	require.NoError(t, json.NewDecoder(retry.Body).Decode(&again))
	require.Equal(t, first.RunID, again.RunID)
	require.True(t, again.Duplicate)
	out, err := h.Run(context.Background(), RunRequest{ID: first.RunID, Input: "已接纳事件", Source: "worker"})
	require.NoError(t, err)
	require.Equal(t, "ok", out.Result)
	require.EqualValues(t, 1, runs.Load())
}
func TestInlineUsesBindingAndSenderFailureCannotRepeatBusiness(t *testing.T) {
	var calls, delivered atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var e RunEvent
		require.NoError(t, json.NewDecoder(r.Body).Decode(&e))
		require.Equal(t, "run.completed", e.EventType)
		require.NotEmpty(t, e.EventID)
		delivered.Add(1)
		http.Error(w, "失败", 500)
	}))
	defer receiver.Close()
	sender, err := NewSender(SenderConfig{Targets: []Target{{URL: receiver.URL}}})
	require.NoError(t, err)
	h, err := New(Config{Factory: func(context.Context, RunRequest) (Binding, Cleanup, error) {
		return Answer(func(context.Context, any) (*builder.AgentOutput, error) {
			calls.Add(1)
			return &builder.AgentOutput{Result: "完成"}, nil
		}), nil, nil
	}, Receivers: map[string]Receiver{"orders": {Mode: "inline", Decode: decodeWebhook}}, Delivery: sender})
	require.NoError(t, err)
	server := httptest.NewServer(h)
	defer server.Close()
	require.Equal(t, 200, post(t, server, "/webhooks/orders", `{"input":"x"}`).StatusCode)
	require.NoError(t, h.Shutdown(context.Background()))
	require.EqualValues(t, 1, calls.Load())
	require.EqualValues(t, 1, delivered.Load())
}
func TestQueuedRequiresAcceptorAndAdmissionErrorIsNotAcknowledged(t *testing.T) {
	_, err := New(Config{Factory: answerFactory, Receivers: map[string]Receiver{"bad": {Mode: "queued", Decode: decodeWebhook}}})
	require.Error(t, err)
	h, err := New(Config{Factory: answerFactory, Receivers: map[string]Receiver{"bad": {Mode: "queued", Decode: decodeWebhook, Acceptor: AcceptorFunc(func(context.Context, Incoming) (Receipt, error) { return Receipt{}, errors.New("存储不可用") })}}})
	require.NoError(t, err)
	server := httptest.NewServer(h)
	defer server.Close()
	require.Equal(t, 503, post(t, server, "/webhooks/bad", `{"input":1}`).StatusCode)
}
func TestSenderRejectsRedirectAndRespectsSharedBudget(t *testing.T) {
	var redirected atomic.Int32
	final := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
	defer final.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, final.URL, 307) }))
	defer origin.Close()
	sender, err := NewSender(SenderConfig{Targets: []Target{{URL: origin.URL}}})
	require.NoError(t, err)
	require.Error(t, sender.Deliver(context.Background(), RunEvent{EventType: "run.completed"}))
	require.Zero(t, redirected.Load())
	stalled := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.Copy(io.Discard, r.Body); <-r.Context().Done() }))
	defer stalled.Close()
	sender, err = NewSender(SenderConfig{Targets: []Target{{URL: stalled.URL}, {URL: final.URL}}})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	require.Error(t, sender.Deliver(ctx, RunEvent{EventType: "run.completed"}))
	require.Zero(t, redirected.Load())
}
