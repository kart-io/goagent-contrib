package httpagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kart-io/goagent/builder"
	"github.com/kart-io/goagent/interfaces"
	"github.com/kart-io/goagent/llm"
	"github.com/kart-io/goagent/llm/providers"
	"github.com/kart-io/goagent/plugin"
	"github.com/kart-io/goagent/tools"
	"github.com/stretchr/testify/require"
)

func TestNativeWaitsForProducerBeforeReturning(t *testing.T) {
	events := make(chan builder.AgentEvent, 1)
	events <- builder.AgentEvent{Type: "done", Output: &builder.AgentOutput{Result: "最终答案"}}
	binding := Native(func(context.Context, any) (<-chan builder.AgentEvent, error) { return events, nil })
	returned := make(chan *builder.AgentOutput, 1)
	go func() {
		out, err := binding.Run(context.Background(), "输入", func(builder.AgentEvent) error { return nil })
		if err != nil {
			t.Error(err)
		}
		returned <- out
	}()
	select {
	case <-returned:
		t.Fatal("生产者尚未关闭，不能归还资源")
	case <-time.After(15 * time.Millisecond):
	}
	close(events)
	require.Equal(t, "最终答案", (<-returned).Result)
	require.True(t, binding.Streaming)
}
func TestNativeRejectsMissingOrDuplicateTerminal(t *testing.T) {
	for _, events := range [][]builder.AgentEvent{{{Type: "text", Delta: "未完成"}}, {{Type: "done", Output: &builder.AgentOutput{}}, {Type: "done", Output: &builder.AgentOutput{}}}} {
		ch := make(chan builder.AgentEvent, len(events))
		for _, e := range events {
			ch <- e
		}
		close(ch)
		_, err := Native(func(context.Context, any) (<-chan builder.AgentEvent, error) { return ch, nil }).Run(context.Background(), "", func(builder.AgentEvent) error { return nil })
		require.Error(t, err)
	}
}
func TestNativeEmitFailureCancelsAndWaitsForProducer(t *testing.T) {
	stopped := make(chan struct{})
	binding := Native(func(ctx context.Context, _ any) (<-chan builder.AgentEvent, error) {
		ch := make(chan builder.AgentEvent)
		go func() {
			defer close(ch)
			ch <- builder.AgentEvent{Type: "text", Delta: "文本"}
			<-ctx.Done()
			close(stopped)
		}()
		return ch, nil
	})
	_, err := binding.Run(context.Background(), nil, func(builder.AgentEvent) error { return errors.New("写入失败") })
	require.ErrorContains(t, err, "写入失败")
	select {
	case <-stopped:
	default:
		t.Fatal("生产者未退出")
	}
}
func TestAnswerDoesNotClaimStreaming(t *testing.T) {
	binding := Answer(func(context.Context, any) (*builder.AgentOutput, error) {
		return &builder.AgentOutput{Result: "普通回答"}, nil
	})
	require.False(t, binding.Streaming)
	out, err := binding.Run(context.Background(), "", func(builder.AgentEvent) error { t.Fatal("不应伪造增量"); return nil })
	require.NoError(t, err)
	require.Equal(t, "普通回答", out.Result)
}

type toolPlugin struct{ tool interfaces.Tool }

func (toolPlugin) Name() string    { return "probe" }
func (toolPlugin) Version() string { return "1" }
func (p toolPlugin) Contribute(context.Context, *plugin.Env) (plugin.Contribution, error) {
	return plugin.Contribution{Tools: []interfaces.Tool{p.tool}}, nil
}

// 真正运行 Builder、Plugin、Provider 和工具，仅模型响应由本地 HTTP 服务脚本化。
func probeClient(t *testing.T) *providers.OpenAIProvider {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Stream   bool          `json:"stream"`
			Messages []llm.Message `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if !request.Stream {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"id":"probe","model":"probe","choices":[{"index":0,"message":{"role":"assistant","content":"同步文本"},"finish_reason":"stop"}]}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		hasResult := false
		for _, m := range request.Messages {
			if m.Role == "tool" {
				hasResult = true
			}
		}
		if hasResult {
			fmt.Fprint(w, "data: {\"id\":\"probe\",\"model\":\"probe\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"工具结果\"},\"finish_reason\":\"stop\"}]}\n\n")
		} else {
			fmt.Fprint(w, "data: {\"id\":\"probe\",\"model\":\"probe\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"type\":\"function\",\"function\":{\"name\":\"probe_tool\",\"arguments\":\"{}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n")
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(server.Close)
	client, err := providers.NewOpenAIWithOptions(llm.WithAPIKey("local-probe"), llm.WithModel("probe"), llm.WithBaseURL(server.URL))
	if err != nil {
		t.Fatal(err)
	}

	return client
}
func TestNativeKeepsPluginToolExecution(t *testing.T) {
	client := probeClient(t)
	var invoked atomic.Int32
	tool := tools.NewBaseTool("probe_tool", "探针工具", `{"type":"object","properties":{}}`, func(context.Context, *interfaces.ToolInput) (*interfaces.ToolOutput, error) {
		invoked.Add(1)
		return &interfaces.ToolOutput{Success: true, Result: "工具执行成功"}, nil
	})
	host, err := plugin.NewHost(toolPlugin{tool: tool})
	if err != nil {
		t.Fatal(err)
	}
	if err = host.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer host.Stop(context.Background())
	b := builder.NewSimpleBuilder(client).WithMaxIterations(4)
	if err = plugin.Install(context.Background(), b, host); err != nil {
		t.Fatal(err)
	}
	agent, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	out, err := agent.Execute(context.Background(), "请执行工具")
	if err != nil {
		t.Fatal(err)
	}
	if invoked.Load() != 0 {
		t.Fatal("同步 Execute 意外执行工具")
	}
	t.Logf("Execute: result=%v tool_calls=%d", out.Result, invoked.Load())
	out, err = Native(agent.ExecuteStream).Run(context.Background(), "请执行工具", func(builder.AgentEvent) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if invoked.Load() != 1 {
		t.Fatalf("原生执行预期一次工具调用，实际 %d", invoked.Load())
	}
	t.Logf("AsRunnable.Invoke: result=%v tool_calls=%d", out.Result, invoked.Load())
}
