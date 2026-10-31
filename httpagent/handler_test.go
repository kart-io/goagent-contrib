package httpagent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kart-io/goagent/builder"
	"github.com/stretchr/testify/require"
)

func answerFactory(_ context.Context, r RunRequest) (Binding, Cleanup, error) {
	return Answer(func(context.Context, any) (*builder.AgentOutput, error) {
		return &builder.AgentOutput{Result: r.Input}, nil
	}), nil, nil
}
func post(t *testing.T, server *httptest.Server, path, body string) *http.Response {
	t.Helper()
	resp, err := server.Client().Post(server.URL+path, "application/json", strings.NewReader(body))
	require.NoError(t, err)
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}
func TestJSONSSEAndInputBoundaries(t *testing.T) {
	h, err := New(Config{Factory: answerFactory, MaxInputBytes: 128})
	require.NoError(t, err)
	defer h.Shutdown(context.Background())
	server := httptest.NewServer(h)
	defer server.Close()
	for _, tc := range []struct {
		path, body string
		status     int
	}{{"/invoke", `{"input":{"n":9007199254740993}}`, 200}, {"/invoke", `{"input":null}`, 400}, {"/invoke", `{"input":"a","extra":1}`, 400}, {"/invoke", `{"input":1} {}`, 400}, {"/invoke", `{"input":"` + strings.Repeat("a", 128) + `"}`, 413}, {"/stream", `{"input":"hello"}`, 501}} {
		resp := post(t, server, tc.path, tc.body)
		data, _ := io.ReadAll(resp.Body)
		require.Equal(t, tc.status, resp.StatusCode, string(data))
		if tc.status == 200 {
			require.Contains(t, string(data), "9007199254740993")
			require.NotEmpty(t, resp.Header.Get("X-GoAgent-Run-ID"))
		}
	}
}
func TestSSEUsesUnifiedRunAndHasSingleTerminal(t *testing.T) {
	h, err := New(Config{Factory: func(context.Context, RunRequest) (Binding, Cleanup, error) {
		return Native(func(context.Context, any) (<-chan builder.AgentEvent, error) {
			ch := make(chan builder.AgentEvent, 2)
			ch <- builder.AgentEvent{Type: "text", Delta: "中文"}
			ch <- builder.AgentEvent{Type: "done", Output: &builder.AgentOutput{Result: "最终"}}
			close(ch)
			return ch, nil
		}), nil, nil
	}})
	require.NoError(t, err)
	server := httptest.NewServer(h)
	defer server.Close()
	resp := post(t, server, "/stream", `{"input":"hello"}`)
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(data), "event: text\n")
	require.Contains(t, string(data), "event: done\n")
	require.Equal(t, 1, strings.Count(string(data), "event: done\n"))
	require.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))
}
func TestShutdownOwnsConstructingFactoryAndLateCleanup(t *testing.T) {
	entered, release, cleaned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	h, err := New(Config{Factory: func(context.Context, RunRequest) (Binding, Cleanup, error) {
		close(entered)
		<-release
		return Answer(func(context.Context, any) (*builder.AgentOutput, error) {
			t.Error("关闭后不能执行")
			return nil, nil
		}), func(context.Context) error { close(cleaned); return nil }, nil
	}, DrainTimeout: 20 * time.Millisecond})
	require.NoError(t, err)
	server := httptest.NewServer(h)
	defer server.Close()
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		resp, err := server.Client().Post(server.URL+"/invoke", "application/json", strings.NewReader(`{"input":"x"}`))
		if err == nil {
			resp.Body.Close()
		}
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	require.Error(t, h.Shutdown(ctx))
	select {
	case <-cleaned:
		t.Fatal("构造未退出，不能清理")
	default:
	}
	require.Equal(t, 503, post(t, server, "/invoke", `{"input":1}`).StatusCode)
	close(release)
	require.NoError(t, h.Shutdown(context.Background()))
	<-cleaned
	<-returned
}
func TestProducerTimeoutIsUnconfirmedUntilActualExit(t *testing.T) {
	events := make(chan builder.AgentEvent, 1)
	events <- builder.AgentEvent{Type: "done", Output: &builder.AgentOutput{Result: "结果"}}
	var notices atomic.Int32
	h, err := New(Config{Factory: func(context.Context, RunRequest) (Binding, Cleanup, error) {
		return Native(func(context.Context, any) (<-chan builder.AgentEvent, error) { return events, nil }), nil, nil
	}, DrainTimeout: 15 * time.Millisecond, MaxActiveRuns: 1, Observer: func(_ context.Context, e RunEvent) error {
		if e.EventType != "run.started" {
			notices.Add(1)
		}
		return nil
	}})
	require.NoError(t, err)
	server := httptest.NewServer(h)
	defer server.Close()
	resp := post(t, server, "/invoke", `{"input":1}`)
	data, _ := io.ReadAll(resp.Body)
	require.Equal(t, 504, resp.StatusCode)
	require.Contains(t, string(data), "execution_unconfirmed")
	require.Zero(t, notices.Load())
	require.Equal(t, 429, post(t, server, "/invoke", `{"input":1}`).StatusCode)
	close(events)
	require.NoError(t, h.Shutdown(context.Background()))
	require.EqualValues(t, 1, notices.Load())
}
func TestOutputEncodingFailureAndObserverCannotRepeatRun(t *testing.T) {
	var count atomic.Int32
	h, err := New(Config{Factory: func(context.Context, RunRequest) (Binding, Cleanup, error) {
		return Answer(func(context.Context, any) (*builder.AgentOutput, error) {
			count.Add(1)
			return &builder.AgentOutput{Result: make(chan int)}, nil
		}), nil, nil
	}, Observer: func(context.Context, RunEvent) error { panic("观察者失败") }})
	require.NoError(t, err)
	server := httptest.NewServer(h)
	defer server.Close()
	resp := post(t, server, "/invoke", `{"input":1}`)
	var data map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&data))
	require.Equal(t, 500, resp.StatusCode)
	require.EqualValues(t, 1, count.Load())
	require.Contains(t, data["error"].(map[string]any)["code"], "output_encoding_failed")
}

func TestOversizedTrailingBodyIs413(t *testing.T) {
	h, err := New(Config{Factory: answerFactory, MaxInputBytes: 32})
	require.NoError(t, err)
	server := httptest.NewServer(h)
	defer server.Close()
	require.Equal(t, 413, post(t, server, "/invoke", `{"input":1}`+strings.Repeat(" ", 40)).StatusCode)
}
