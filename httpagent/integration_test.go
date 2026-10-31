package httpagent

import (
	"context"
	"io"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kart-io/goagent/builder"
	"github.com/kart-io/goagent/core/budget"
	"github.com/kart-io/goagent/core/durable"
	"github.com/kart-io/goagent/core/session"
	"github.com/kart-io/goagent/execution"
	"github.com/kart-io/goagent/interfaces"
	"github.com/kart-io/goagent/plugin"
	"github.com/kart-io/goagent/tools"
	"github.com/stretchr/testify/require"
)

type governedPlugin struct {
	toolPlugin
	hooks *atomic.Int32
}

func (p governedPlugin) Contribute(ctx context.Context, env *plugin.Env) (plugin.Contribution, error) {
	c, err := p.toolPlugin.Contribute(ctx, env)
	c.PreSteps = []plugin.PreStep{{Identity: "count-hook", Hook: func(context.Context, *builder.Step) (builder.PreStepResult, error) {
		p.hooks.Add(1)
		return builder.PreStepResult{}, nil
	}}}
	return c, err
}
func TestAllEntrypointsKeepToolsBudgetJournalAndNativeHooks(t *testing.T) {
	client := probeClient(t)
	var calls, hooks atomic.Int32
	tool := tools.NewBaseTool("probe_tool", "真实受治理工具", `{"type":"object","properties":{}}`, func(context.Context, *interfaces.ToolInput) (*interfaces.ToolOutput, error) {
		calls.Add(1)
		return &interfaces.ToolOutput{Success: true, Result: "governed"}, nil
	})
	host, err := plugin.NewHost(governedPlugin{toolPlugin: toolPlugin{tool: tool}, hooks: &hooks})
	require.NoError(t, err)
	require.NoError(t, host.Start(context.Background()))
	defer host.Stop(context.Background())
	root := t.TempDir()
	journal, err := durable.NewFileStore(filepath.Join(root, "journal"))
	require.NoError(t, err)
	logs, err := session.NewFileStore(filepath.Join(root, "sessions"))
	require.NoError(t, err)
	var mu sync.Mutex
	controllers := map[string]*budget.Controller{}
	h, err := New(Config{Factory: func(ctx context.Context, r RunRequest) (Binding, Cleanup, error) {
		controller, err := budget.NewDurable(budget.Config{RootKey: r.ID, Version: "http-integration-v1", MaxModelRequests: 2, MaxToolCalls: 1}, journal)
		if err != nil {
			return Binding{}, nil, err
		}
		scope, err := execution.NewScope(execution.Config{RunID: r.ID, SessionID: r.ID, Journal: journal, Budget: controller})
		if err != nil {
			return Binding{}, nil, err
		}
		b, err := execution.ConfigureBuilder(builder.NewSimpleBuilder(client).WithSessionLog(logs).WithMaxIterations(4).WithTimeout(10*time.Minute), scope, "agent")
		if err != nil {
			return Binding{}, nil, err
		}
		if err = plugin.Install(ctx, b, host, plugin.Dispatch(tools.DispatchConfig{})); err != nil {
			return Binding{}, nil, err
		}
		agent, err := b.BuildContext(ctx)
		if err != nil {
			return Binding{}, nil, err
		}
		binding := Native(agent.ExecuteStream)
		native := binding.Run
		binding.Run = func(ctx context.Context, input any, emit EmitFunc) (*builder.AgentOutput, error) {
			bound, cancel, err := scope.Bind(ctx)
			if err != nil {
				return nil, err
			}
			defer cancel()
			return native(bound, input, emit)
		}
		mu.Lock()
		controllers[r.ID] = controller
		mu.Unlock()
		return binding, nil, nil
	}, Receivers: map[string]Receiver{"inline": {Mode: "inline", Decode: decodeWebhook}}})
	require.NoError(t, err)
	server := httptest.NewServer(h)
	defer server.Close()
	defer h.Shutdown(context.Background())
	var ids []string
	for _, path := range []string{"/invoke", "/stream", "/webhooks/inline"} {
		resp := post(t, server, path, `{"input":"执行工具"}`)
		data, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode, string(data))
		ids = append(ids, resp.Header.Get("X-GoAgent-Run-ID"))
	}
	c := dialWS(t, server)
	require.NoError(t, c.WriteJSON(map[string]any{"type": "run", "request_id": "governed", "input": "执行工具"}))
	ack := readWS(t, c)
	require.Equal(t, "accepted", ack["type"])
	ids = append(ids, ack["run_id"].(string))
	for {
		frame := readWS(t, c)
		if frame["type"] == "ready" {
			break
		}
	}
	c.Close()
	workerID := uuid.NewString()
	out, err := h.Run(context.Background(), RunRequest{ID: workerID, Input: "执行工具", Source: "worker"})
	require.NoError(t, err)
	require.NotNil(t, out)
	ids = append(ids, workerID)
	require.NoError(t, h.Shutdown(context.Background()))
	require.EqualValues(t, 5, calls.Load())
	require.EqualValues(t, 10, hooks.Load())
	restored, err := session.NewFileStore(filepath.Join(root, "sessions"))
	require.NoError(t, err)
	for _, id := range ids {
		require.NotEmpty(t, id)
		mu.Lock()
		controller := controllers[id]
		mu.Unlock()
		require.NotNil(t, controller)
		snapshot, err := controller.Snapshot(context.Background())
		require.NoError(t, err)
		require.Equal(t, 2, snapshot.ReservedRequests)
		require.Equal(t, 1, snapshot.ReservedToolCalls)
		events, err := restored.Read(context.Background(), id, 0)
		require.NoError(t, err)
		require.NotEmpty(t, events)
		recovered, err := budget.NewDurable(controller.Configuration(), journal)
		require.NoError(t, err)
		persisted, err := recovered.Snapshot(context.Background())
		require.NoError(t, err)
		require.Equal(t, 2, persisted.ReservedRequests)
	}
}
