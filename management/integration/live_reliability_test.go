package integration_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kart-io/goagent/codec/json"
	"github.com/kart-io/goagent/interfaces"
	"github.com/kart-io/goagent/llm"
	mg "github.com/kart-io/goagent/management"
	"github.com/stretchr/testify/require"
)

// 这里只注入传输故障测试观察器，不能作为真实模型业务验收结果。
type liveFaultSource struct {
	llm.Client
	stream func(context.Context) (<-chan *llm.StreamChunk, error)
}

func (s liveFaultSource) CompleteToolsStream(ctx context.Context, _ *llm.CompletionRequest, _ []interfaces.Tool) (<-chan *llm.StreamChunk, error) {
	return s.stream(ctx)
}

func TestLiveObserverDeadlineAndEarlyEOF(t *testing.T) {
	for _, test := range []struct {
		name string
		wait bool
		want error
	}{{"提前断流", false, io.ErrUnexpectedEOF}, {"截止时间", true, context.DeadlineExceeded}} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			source := liveFaultSource{stream: func(ctx context.Context) (<-chan *llm.StreamChunk, error) {
				ch := make(chan *llm.StreamChunk)
				if test.wait {
					go func() { <-ctx.Done(); close(ch) }()
				} else {
					close(ch)
				}
				return ch, nil
			}}
			model := &liveObservedModel{ToolStreamClient: source, ledger: &liveLedger{}, taskID: "故障任务"}
			stream, err := model.CompleteToolsStream(ctx, &llm.CompletionRequest{MaxTokens: 1024}, nil)
			require.NoError(t, err)
			var terminal *llm.StreamChunk
			for chunk := range stream {
				terminal = chunk
			}
			require.NotNil(t, terminal, "断流必须保留明确错误终态，不能只关闭通道")
			require.True(t, terminal.Done)
			require.ErrorIs(t, terminal.Error, test.want)
			require.NotEqual(t, "completed", model.ledger.snapshot()[0].Status)
			require.Nil(t, model.ledger.snapshot()[0].Usage, "未知用量不能填零")
		})
	}
}

func TestLiveHTTPPollFailureReturnsImmediately(t *testing.T) {
	for _, mode := range []string{"session", "task"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLiveHTTPPollFailureChild$", "-test.v")
			cmd.Env = append(os.Environ(), "GOAGENT_LIVE_POLL_CHILD="+mode)
			raw, err := cmd.CombinedOutput()
			require.Error(t, err, "子用例应因HTTP 503失败")
			require.NoError(t, ctx.Err(), "HTTP断言失败不应等待Eventually超时：%s", raw)
			require.Contains(t, string(raw), "503")
		})
	}
}

func TestLiveHTTPPollFailureChild(t *testing.T) {
	mode := os.Getenv("GOAGENT_LIVE_POLL_CHILD")
	if mode == "" {
		return
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"故障注入"}`))
	}))
	defer server.Close()
	c := &liveCase{t: t, f: &fixture{ctx: context.Background()}, server: server}
	if mode == "session" {
		c.ready("故障会话")
	} else {
		c.wait("故障任务", mg.Succeeded)
	}
}

func TestLiveProviderPreflightRejectsPartialConfiguration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLiveManagement$", "-test.v")
	for _, env := range os.Environ() {
		key, _, _ := strings.Cut(env, "=")
		if !strings.HasPrefix(key, "GOAGENT_MANAGEMENT_LIVE") && key != "GOAGENT_PGSTORE_DSN" && key != "DEEPSEEK_API_KEY" && key != "GEMINI_API_KEY" {
			cmd.Env = append(cmd.Env, env)
		}
	}
	cmd.Env = append(cmd.Env, "GOAGENT_MANAGEMENT_LIVE=1", "GOAGENT_MANAGEMENT_LIVE_PROVIDERS=deepseek,gemini", "DEEPSEEK_API_KEY=前检占位值不发请求", "GOAGENT_PGSTORE_DSN=postgres://postgres:test@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=1")
	raw, err := cmd.CombinedOutput()
	require.Error(t, err)
	require.NoError(t, ctx.Err())
	require.Contains(t, string(raw), "GEMINI_API_KEY")
	require.NotContains(t, string(raw), "connect:", "应先验证全部配置，不能先执行其他提供方案例")
}

func TestLiveObserverPropagatesRateLimitWithoutRetry(t *testing.T) {
	requestError := &llm.ProviderError{StatusCode: http.StatusTooManyRequests, Err: errors.New("故障注入：请求限流")}
	source := liveFaultSource{stream: func(context.Context) (<-chan *llm.StreamChunk, error) { return nil, requestError }}
	model := &liveObservedModel{ToolStreamClient: source, ledger: &liveLedger{}}
	_, err := model.CompleteToolsStream(context.Background(), &llm.CompletionRequest{MaxTokens: 1024}, nil)
	require.True(t, errors.Is(err, requestError))
	require.Len(t, model.ledger.snapshot(), 1)
	require.Equal(t, llm.ReasonRateLimit, model.ledger.snapshot()[0].ErrorReason)
	require.Nil(t, model.ledger.snapshot()[0].Usage)
}

func TestLiveJournalSurvivesAbruptExitBeforeScenarioCleanup(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLiveJournalCrashChild$")
	cmd.Env = append(os.Environ(), "GOAGENT_LIVE_JOURNAL_CHILD="+dir)
	raw, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit, "%s", raw)
	require.Equal(t, 42, exit.ExitCode())
	require.NoError(t, ctx.Err())
	data, err := os.ReadFile(filepath.Join(dir, "crash-requests.jsonl"))
	require.NoError(t, err)
	var row liveJournalEntry
	require.NoError(t, json.Unmarshal(data, &row))
	require.Equal(t, "started", row.Call.Status)
	require.Equal(t, "退出前已记录", row.Call.TaskID)
	require.Nil(t, row.Call.Usage)
	require.Zero(t, row.Call.UsageFields)
}

func TestLiveJournalCrashChild(t *testing.T) {
	dir := os.Getenv("GOAGENT_LIVE_JOURNAL_CHILD")
	if dir == "" {
		return
	}
	ledger, err := newLiveLedger(dir, "crash")
	require.NoError(t, err)
	source := liveFaultSource{stream: func(context.Context) (<-chan *llm.StreamChunk, error) {
		os.Exit(42)
		return nil, nil
	}}
	model := &liveObservedModel{ToolStreamClient: source, ledger: ledger, taskID: "退出前已记录"}
	_, _ = model.CompleteToolsStream(context.Background(), &llm.CompletionRequest{MaxTokens: 1024}, nil)
	t.Fatal("故障注入未执行")
}

func TestLiveJournalFailurePreventsRequestAndRefusesOverwrite(t *testing.T) {
	dir := t.TempDir()
	ledger, err := newLiveLedger(dir, "case")
	require.NoError(t, err)
	_, err = newLiveLedger(dir, "case")
	require.ErrorIs(t, err, os.ErrExist)
	require.NoError(t, ledger.close())
	var invoked atomic.Int32
	source := liveFaultSource{stream: func(context.Context) (<-chan *llm.StreamChunk, error) {
		invoked.Add(1)
		return nil, errors.New("不应调用")
	}}
	model := &liveObservedModel{ToolStreamClient: source, ledger: ledger}
	_, err = model.CompleteToolsStream(context.Background(), &llm.CompletionRequest{MaxTokens: 1024}, nil)
	require.Error(t, err)
	require.Zero(t, invoked.Load(), "无法保存请求前证据时不得请求模型")
}

func TestLiveJournalFlushesRateLimitTerminal(t *testing.T) {
	dir := t.TempDir()
	ledger, err := newLiveLedger(dir, "limited")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ledger.close()) })
	source := liveFaultSource{stream: func(context.Context) (<-chan *llm.StreamChunk, error) {
		return nil, &llm.ProviderError{StatusCode: 429, Err: errors.New("不应写入原始错误文本")}
	}}
	model := &liveObservedModel{ToolStreamClient: source, ledger: ledger}
	_, err = model.CompleteToolsStream(context.Background(), &llm.CompletionRequest{MaxTokens: 1024}, nil)
	require.Error(t, err)
	data, err := os.ReadFile(filepath.Join(dir, "limited-requests.jsonl"))
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	require.Len(t, lines, 2)
	var row liveJournalEntry
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &row))
	require.Equal(t, "request_error", row.Call.Status)
	require.Equal(t, llm.ReasonRateLimit, row.Call.ErrorReason)
	require.Nil(t, row.Call.Usage)
	require.NotContains(t, string(data), "不应写入原始错误文本")
}

func TestLiveObserverCanceledContextDoesNotStartRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ledger := &liveLedger{}
	source := liveFaultSource{stream: func(context.Context) (<-chan *llm.StreamChunk, error) { t.Error("不应调用模型"); return nil, nil }}
	model := &liveObservedModel{ToolStreamClient: source, ledger: ledger}
	_, err := model.CompleteToolsStream(ctx, &llm.CompletionRequest{MaxTokens: 1024}, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, ledger.snapshot())
}

func TestLiveObserverCancellationAndNonterminalError(t *testing.T) {
	t.Run("流中取消保留原因且释放生产者", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		source := liveFaultSource{stream: func(ctx context.Context) (<-chan *llm.StreamChunk, error) {
			ch := make(chan *llm.StreamChunk)
			go func() {
				defer close(ch)
				defer close(done)
				select {
				case ch <- &llm.StreamChunk{Delta: "测试片段"}:
				case <-ctx.Done():
					return
				}
				<-ctx.Done()
			}()
			return ch, nil
		}}
		model := &liveObservedModel{ToolStreamClient: source, ledger: &liveLedger{}}
		stream, err := model.CompleteToolsStream(ctx, &llm.CompletionRequest{MaxTokens: 1024}, nil)
		require.NoError(t, err)
		require.Equal(t, "测试片段", (<-stream).Delta)
		cancel()
		var terminal *llm.StreamChunk
		for chunk := range stream {
			terminal = chunk
		}
		require.NotNil(t, terminal)
		require.ErrorIs(t, terminal.Error, context.Canceled)
		require.Equal(t, "canceled", model.ledger.snapshot()[0].Status)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("取消后上游生产者未退出")
		}
	})
	t.Run("非Done错误帧不计成功", func(t *testing.T) {
		original := &llm.StreamChunk{Error: &llm.ProviderError{StatusCode: 429, Err: errors.New("错误帧")}}
		source := liveFaultSource{stream: func(context.Context) (<-chan *llm.StreamChunk, error) {
			ch := make(chan *llm.StreamChunk, 1)
			ch <- original
			close(ch)
			return ch, nil
		}}
		model := &liveObservedModel{ToolStreamClient: source, ledger: &liveLedger{}}
		stream, err := model.CompleteToolsStream(context.Background(), &llm.CompletionRequest{MaxTokens: 1024}, nil)
		require.NoError(t, err)
		var chunks []*llm.StreamChunk
		for chunk := range stream {
			chunks = append(chunks, chunk)
		}
		require.Equal(t, []*llm.StreamChunk{original}, chunks)
		require.Equal(t, "stream_error", model.ledger.snapshot()[0].Status)
		require.Equal(t, llm.ReasonRateLimit, model.ledger.snapshot()[0].ErrorReason)
	})
}

func TestLiveObserverInternalTimeoutDoesNotWaitForConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := liveFaultSource{stream: func(ctx context.Context) (<-chan *llm.StreamChunk, error) {
		ch := make(chan *llm.StreamChunk)
		go func() {
			defer close(ch)
			for range 2 {
				select {
				case ch <- &llm.StreamChunk{Delta: "未消费片段"}:
				case <-ctx.Done():
					return
				}
			}
			<-ctx.Done()
		}()
		return ch, nil
	}}
	model := &liveObservedModel{ToolStreamClient: source, ledger: &liveLedger{}, timeout: 30 * time.Millisecond}
	stream, err := model.CompleteToolsStream(ctx, &llm.CompletionRequest{MaxTokens: 1024}, nil)
	require.NoError(t, err)
	select {
	case <-model.ledger.snapshot()[0].finished:
	case <-time.After(time.Second):
		t.Fatal("不消费正文且父context存活时，内部超时仍必须退出")
	}
	chunk := <-stream
	require.NotNil(t, chunk)
	require.ErrorIs(t, chunk.Error, context.DeadlineExceeded, "失败时必须留出错误投递位置，不能仍只得到未消费正文")
	select {
	case _, ok := <-stream:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("内部超时后观察器未退出")
	}
}

func TestLiveProviderPreflightRejectsLegacyReportDirectory(t *testing.T) {
	dir := t.TempDir()
	old := filepath.Join(dir, "deepseek-批准与宿主重建.json")
	require.NoError(t, os.WriteFile(old, []byte("旧报告必须保留"), 0600))
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLiveManagement$", "-test.v")
	for _, env := range os.Environ() {
		key, _, _ := strings.Cut(env, "=")
		if !strings.HasPrefix(key, "GOAGENT_MANAGEMENT_LIVE") && key != "GOAGENT_PGSTORE_DSN" && key != "DEEPSEEK_API_KEY" {
			cmd.Env = append(cmd.Env, env)
		}
	}
	cmd.Env = append(cmd.Env, "GOAGENT_MANAGEMENT_LIVE=1", "GOAGENT_MANAGEMENT_LIVE_PROVIDERS=deepseek", "DEEPSEEK_API_KEY=前检占位值不发请求", "GOAGENT_MANAGEMENT_LIVE_REPORT_DIR="+dir, "GOAGENT_PGSTORE_DSN=postgres://postgres:test@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=1")
	raw, err := cmd.CombinedOutput()
	require.Error(t, err)
	require.NoError(t, ctx.Err())
	require.Contains(t, string(raw), "报告目录必须不存在")
	require.NotContains(t, string(raw), "connect:")
	contents, err := os.ReadFile(old)
	require.NoError(t, err)
	require.Equal(t, "旧报告必须保留", string(contents))
}

func TestLiveObserverPreservesTerminalBehindSlowConsumer(t *testing.T) {
	for _, test := range []struct {
		name     string
		terminal *llm.StreamChunk
		status   string
	}{
		{"上游错误", &llm.StreamChunk{Error: &llm.ProviderError{StatusCode: 429, Err: errors.New("上游限流")}}, "stream_error"},
		{"上游成功终态", &llm.StreamChunk{Done: true, Content: "已完成"}, "completed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := liveFaultSource{stream: func(context.Context) (<-chan *llm.StreamChunk, error) {
				ch := make(chan *llm.StreamChunk, 2)
				ch <- &llm.StreamChunk{Delta: "未消费正文"}
				ch <- test.terminal
				close(ch)
				return ch, nil
			}}
			model := &liveObservedModel{ToolStreamClient: source, ledger: &liveLedger{}, timeout: 30 * time.Millisecond}
			stream, err := model.CompleteToolsStream(context.Background(), &llm.CompletionRequest{MaxTokens: 1024}, nil)
			require.NoError(t, err)
			select {
			case <-model.ledger.snapshot()[0].finished:
			case <-time.After(time.Second):
				t.Fatal("内部超时后观察器未退出")
			}
			require.Same(t, test.terminal, <-stream, "慢消费者不能导致原上游终态丢失")
			require.Equal(t, test.status, model.ledger.snapshot()[0].Status)
		})
	}
}
