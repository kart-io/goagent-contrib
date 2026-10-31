package httpagent

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/kart-io/goagent/builder"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReviewWSFrameBoundary(t *testing.T) {
	h, err := New(Config{MaxEventBytes: 512, Factory: func(context.Context, RunRequest) (Binding, Cleanup, error) {
		return Answer(func(context.Context, any) (*builder.AgentOutput, error) {
			return &builder.AgentOutput{Result: strings.Repeat("x", 300)}, nil
		}), nil, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewServer(h)
	defer s.Close()
	defer h.Shutdown(context.Background())
	c := dialWS(t, s)
	c.WriteJSON(map[string]any{"type": "run", "request_id": "boundary", "input": 1})
	first := readWS(t, c)
	second := readWS(t, c)
	t.Logf("frames: first=%v second=%v", first, second)
	if second["type"] != "event" {
		t.Fatalf("terminal missing: received %v", second)
	}
}

func TestReviewWorkerOutputCannotMutateFrozenNotice(t *testing.T) {
	cleanupEntered, release := make(chan struct{}), make(chan struct{})
	notices := make(chan RunEvent, 1)
	h, _ := New(Config{Factory: func(context.Context, RunRequest) (Binding, Cleanup, error) {
		return Answer(func(context.Context, any) (*builder.AgentOutput, error) {
			return &builder.AgentOutput{Result: map[string]any{"value": "original"}}, nil
		}), func(context.Context) error { close(cleanupEntered); <-release; return nil }, nil
	}, Observer: func(_ context.Context, e RunEvent) error {
		if e.EventType == "run.completed" {
			notices <- e
		}
		return nil
	}})
	out, err := h.Run(context.Background(), RunRequest{Input: 1})
	if err != nil {
		t.Fatal(err)
	}
	<-cleanupEntered
	out.Result.(map[string]any)["value"] = "mutated after Run returned"
	close(release)
	e := <-notices
	data, _ := json.Marshal(e)
	t.Log(string(data))
	if strings.Contains(string(data), "mutated") {
		t.Error("frozen notice was changed by caller")
	}
	h.Shutdown(context.Background())
}

func TestReviewNotificationBeforeCleanupExit(t *testing.T) {
	release := make(chan struct{})
	cleaned := make(chan struct{})
	notices := make(chan RunEvent, 1)
	h, _ := New(Config{CleanupTimeout: 10 * time.Millisecond, Factory: func(context.Context, RunRequest) (Binding, Cleanup, error) {
		return Answer(func(context.Context, any) (*builder.AgentOutput, error) {
			return &builder.AgentOutput{Result: "completed"}, nil
		}), func(context.Context) error { <-release; close(cleaned); return nil }, nil
	}, Observer: func(_ context.Context, e RunEvent) error {
		if e.EventType == "run.completed" {
			notices <- e
		}
		return nil
	}})
	_, err := h.Run(context.Background(), RunRequest{Input: 1})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-notices:
	case <-time.After(50 * time.Millisecond):
		t.Error("client has result, but completed notice waits for uncooperative cleanup")
	}
	close(release)
	<-cleaned
	h.Shutdown(context.Background())
}

func TestReviewShutdownTracksTimeoutReporter(t *testing.T) {
	reportEntered, reportRelease := make(chan struct{}), make(chan struct{})
	cleanupRelease := make(chan struct{})
	h, _ := New(Config{CleanupTimeout: 10 * time.Millisecond, Factory: func(context.Context, RunRequest) (Binding, Cleanup, error) {
		return Answer(func(context.Context, any) (*builder.AgentOutput, error) {
			return &builder.AgentOutput{Result: "completed"}, nil
		}), func(context.Context) error { <-cleanupRelease; return nil }, nil
	}, Report: func(_ context.Context, r Report) {
		if r.Stage == "cleanup" {
			close(reportEntered)
			<-reportRelease
		}
	}})
	h.Run(context.Background(), RunRequest{Input: 1})
	<-reportEntered
	close(cleanupRelease)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := h.Shutdown(ctx)
	close(reportRelease)
	if err == nil {
		t.Error("Shutdown succeeded with timeout Report still running")
	}
}

func TestReviewSSEHeartbeatBeforeFirstEvent(t *testing.T) {
	release := make(chan struct{})
	h, _ := New(Config{Heartbeat: 10 * time.Millisecond, Factory: func(context.Context, RunRequest) (Binding, Cleanup, error) {
		return Native(func(context.Context, any) (<-chan builder.AgentEvent, error) {
			ch := make(chan builder.AgentEvent)
			go func() {
				<-release
				ch <- builder.AgentEvent{Type: "done", Output: &builder.AgentOutput{Result: "ok"}}
				close(ch)
			}()
			return ch, nil
		}), nil, nil
	}})
	s := httptest.NewServer(h)
	defer s.Close()
	defer h.Shutdown(context.Background())
	response := make(chan struct{})
	go func() {
		resp, err := s.Client().Post(s.URL+"/stream", "application/json", strings.NewReader(`{"input":1}`))
		if err == nil {
			resp.Body.Close()
		}
		close(response)
	}()
	select {
	case <-response:
	case <-time.After(50 * time.Millisecond):
		t.Error("no headers or heartbeat after five heartbeat periods")
	}
	close(release)
	<-response
}

func TestReviewSmallLimitProducesTerminal(t *testing.T) {
	h, err := New(Config{MaxEventBytes: 1, Factory: func(context.Context, RunRequest) (Binding, Cleanup, error) {
		return Native(func(context.Context, any) (<-chan builder.AgentEvent, error) {
			ch := make(chan builder.AgentEvent, 1)
			ch <- builder.AgentEvent{Type: "done", Output: &builder.AgentOutput{Result: "ok"}}
			close(ch)
			return ch, nil
		}), nil, nil
	}})
	if err != nil {
		return
	}
	s := httptest.NewServer(h)
	defer s.Close()
	defer h.Shutdown(context.Background())
	resp := post(t, s, "/stream", `{"input":1}`)
	var b strings.Builder
	buf := make([]byte, 1024)
	for {
		n, err := resp.Body.Read(buf)
		b.Write(buf[:n])
		if err != nil {
			break
		}
	}
	t.Logf("body=%q", b.String())
	if !strings.Contains(b.String(), "error") {
		t.Error("accepted configuration cannot encode even fallback terminal")
	}
}

func TestHeartbeatIsASeparateSSEComment(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	h, err := New(Config{Heartbeat: 5 * time.Millisecond, Factory: func(context.Context, RunRequest) (Binding, Cleanup, error) {
		return Native(func(ctx context.Context, _ any) (<-chan builder.AgentEvent, error) {
			ch := make(chan builder.AgentEvent)
			go func() {
				defer close(ch)
				select {
				case <-release:
				case <-ctx.Done():
					return
				}
				select {
				case ch <- builder.AgentEvent{Type: "done", Output: &builder.AgentOutput{Result: "ok"}}:
				case <-ctx.Done():
				}
			}()
			return ch, nil
		}), nil, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	defer server.Close()
	client := server.Client()
	client.Timeout = 100 * time.Millisecond
	resp, err := client.Post(server.URL+"/stream", "application/json", strings.NewReader(`{"input":1}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil {
		t.Fatalf("心跳必须作为独立SSE行送达：%v", err)
	}
	if !strings.HasPrefix(line, ":") {
		t.Fatalf("预期注释心跳，实际%q", line)
	}
}
