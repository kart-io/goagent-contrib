package httpagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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

type admissionRecord struct {
	Receipt Receipt
	Hash    string
	Event   Incoming
}

func (a *diskAcceptor) Accept(_ context.Context, in Incoming) (Receipt, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	records := map[string]admissionRecord{}
	if data, err := os.ReadFile(a.path); err == nil {
		if err = json.Unmarshal(data, &records); err != nil {
			return Receipt{}, err
		}
	} else if !os.IsNotExist(err) {
		return Receipt{}, err
	}
	keyBytes, _ := json.Marshal([]string{in.Tenant, in.Source, in.Receiver, in.EventID})
	key := string(keyBytes)
	if old, ok := records[key]; ok {
		if old.Hash != in.PayloadHash {
			return Receipt{}, failure("conflict", "相同源事件ID具有不同payload", nil)
		}
		r := old.Receipt
		r.Duplicate = true
		return r, nil
	}
	r := Receipt{RunID: fmt.Sprintf("persisted-run-%d", len(records)+1)}
	records[key] = admissionRecord{Receipt: r, Hash: in.PayloadHash, Event: in}
	data, err := json.Marshal(records)
	if err != nil {
		return Receipt{}, err
	}
	temp := a.path + ".tmp"
	if err = os.WriteFile(temp, data, 0600); err != nil {
		return Receipt{}, err
	}
	return r, os.Rename(temp, a.path)
}
func decodeWebhook(_ context.Context, _ *http.Request, body []byte) (Incoming, error) {
	var x map[string]any
	if err := json.Unmarshal(body, &x); err != nil {
		return Incoming{}, err
	}
	eventID, _ := x["event_id"].(string)
	if eventID == "" {
		eventID = "evt-1"
	}
	tenant, _ := x["tenant"].(string)
	return Incoming{Source: "orders", EventID: eventID, Tenant: tenant, Input: x["input"]}, nil
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

func TestQueuedEventConflictAndScopeIsolation(t *testing.T) {
	acceptor := &diskAcceptor{path: filepath.Join(t.TempDir(), "queue.json")}
	receiver := Receiver{Mode: "queued", Decode: decodeWebhook, Acceptor: acceptor}
	h, err := New(Config{Factory: answerFactory, Receivers: map[string]Receiver{"orders": receiver, "other": receiver}})
	require.NoError(t, err)
	server := httptest.NewServer(h)
	defer server.Close()
	defer h.Shutdown(context.Background())
	response := post(t, server, "/webhooks/orders", `{"input":"one","event_id":"shared","tenant":"a"}`)
	var first Receipt
	require.NoError(t, json.NewDecoder(response.Body).Decode(&first))
	require.Equal(t, 202, response.StatusCode)
	conflict := post(t, server, "/webhooks/orders", `{"input":"changed","event_id":"shared","tenant":"a"}`)
	require.Equal(t, 409, conflict.StatusCode)
	other := post(t, server, "/webhooks/other", `{"input":"one","event_id":"shared","tenant":"a"}`)
	var otherReceipt Receipt
	require.NoError(t, json.NewDecoder(other.Body).Decode(&otherReceipt))
	require.NotEqual(t, first.RunID, otherReceipt.RunID)
	tenant := post(t, server, "/webhooks/orders", `{"input":"one","event_id":"shared","tenant":"b"}`)
	var tenantReceipt Receipt
	require.NoError(t, json.NewDecoder(tenant.Body).Decode(&tenantReceipt))
	require.NotEqual(t, first.RunID, tenantReceipt.RunID)
	data, err := os.ReadFile(acceptor.path)
	require.NoError(t, err)
	var records map[string]admissionRecord
	require.NoError(t, json.Unmarshal(data, &records))
	require.Len(t, records, 3)
	for _, record := range records {
		require.NotEmpty(t, record.Hash)
		require.NotEmpty(t, record.Event.Receiver)
		require.Equal(t, "one", record.Event.Input)
	}
}
func TestConcurrentQueuedRetriesAreAtomicallyAdmittedOnce(t *testing.T) {
	backend := &diskAcceptor{path: filepath.Join(t.TempDir(), "queue.json")}
	h, err := New(Config{Factory: answerFactory, Receivers: map[string]Receiver{"orders": {Mode: "queued", Decode: decodeWebhook, Acceptor: backend}}})
	require.NoError(t, err)
	server := httptest.NewServer(h)
	defer server.Close()
	defer h.Shutdown(context.Background())
	ids := make(chan string, 16)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := server.Client().Post(server.URL+"/webhooks/orders", "application/json", strings.NewReader(`{"input":"once","event_id":"e1"}`))
			if err != nil {
				t.Error(err)
				return
			}
			defer resp.Body.Close()
			var receipt Receipt
			if err = json.NewDecoder(resp.Body).Decode(&receipt); err != nil {
				t.Error(err)
				return
			}
			ids <- receipt.RunID
		}()
	}
	wg.Wait()
	close(ids)
	for id := range ids {
		require.Equal(t, "persisted-run-1", id)
	}
	data, err := os.ReadFile(backend.path)
	require.NoError(t, err)
	var records map[string]admissionRecord
	require.NoError(t, json.Unmarshal(data, &records))
	require.Len(t, records, 1)
}
func TestAcceptedWorkerSurvivesReceiverRequestContext(t *testing.T) {
	acceptedContexts := make(chan context.Context, 1)
	backend := &diskAcceptor{path: filepath.Join(t.TempDir(), "queue.json")}
	h, err := New(Config{Factory: answerFactory, Receivers: map[string]Receiver{"orders": {Mode: "queued", Decode: decodeWebhook, Acceptor: AcceptorFunc(func(ctx context.Context, in Incoming) (Receipt, error) {
		acceptedContexts <- ctx
		return backend.Accept(ctx, in)
	})}}})
	require.NoError(t, err)
	server := httptest.NewServer(h)
	defer server.Close()
	defer h.Shutdown(context.Background())
	response := post(t, server, "/webhooks/orders", `{"input":"from-receiver"}`)
	var receipt Receipt
	require.NoError(t, json.NewDecoder(response.Body).Decode(&receipt))
	response.Body.Close()
	acceptedCtx := <-acceptedContexts
	select {
	case <-acceptedCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("接收请求未退出")
	}
	data, err := os.ReadFile(backend.path)
	require.NoError(t, err)
	var records map[string]admissionRecord
	require.NoError(t, json.Unmarshal(data, &records))
	for _, record := range records {
		out, err := h.Run(context.Background(), RunRequest{ID: record.Receipt.RunID, Input: record.Event.Input, Source: "worker"})
		require.NoError(t, err)
		require.Equal(t, "from-receiver", out.Result)
	}
}

func TestWebhookUnsupportedWriterIsRejectedBeforeAdmission(t *testing.T) {
	var accepted atomic.Int32
	h, err := New(Config{Factory: answerFactory, Receivers: map[string]Receiver{"orders": {Mode: "queued", Decode: decodeWebhook, Acceptor: AcceptorFunc(func(context.Context, Incoming) (Receipt, error) {
		accepted.Add(1)
		return Receipt{RunID: "accepted"}, nil
	})}}})
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.ServeHTTP(opaqueWriter{w}, r) }))
	defer server.Close()
	resp := post(t, server, "/webhooks/orders", `{"input":1}`)
	require.Equal(t, 501, resp.StatusCode)
	require.Zero(t, accepted.Load())
}

func TestNotificationTargetsShareIDAndCannotSeeObserverMutation(t *testing.T) {
	delivered := make(chan RunEvent, 2)
	receive := func(w http.ResponseWriter, r *http.Request) {
		var event RunEvent
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Error(err)
		}
		delivered <- event
		w.WriteHeader(204)
	}
	a, b := httptest.NewServer(http.HandlerFunc(receive)), httptest.NewServer(http.HandlerFunc(receive))
	defer a.Close()
	defer b.Close()
	sender, err := NewSender(SenderConfig{Targets: []Target{{URL: a.URL}, {URL: b.URL}}})
	require.NoError(t, err)
	noticeID := make(chan string, 1)
	h, err := New(Config{Factory: func(context.Context, RunRequest) (Binding, Cleanup, error) {
		return Answer(func(context.Context, any) (*builder.AgentOutput, error) {
			return &builder.AgentOutput{Result: map[string]any{"value": "original"}}, nil
		}), nil, nil
	}, Delivery: sender, Observer: func(_ context.Context, event RunEvent) error {
		if event.EventType == "run.completed" {
			noticeID <- event.EventID
			event.Data.(map[string]any)["result"].(map[string]any)["value"] = "observer-mutated"
		}
		return nil
	}})
	require.NoError(t, err)
	defer h.Shutdown(context.Background())
	output, err := h.Run(context.Background(), RunRequest{Input: 1})
	require.NoError(t, err)
	require.Equal(t, "original", output.Result.(map[string]any)["value"])
	first, second := <-delivered, <-delivered
	require.Equal(t, <-noticeID, first.EventID)
	require.Equal(t, first.EventID, second.EventID)
	for _, event := range []RunEvent{first, second} {
		require.Equal(t, "original", event.Data.(map[string]any)["result"].(map[string]any)["value"])
	}
}
