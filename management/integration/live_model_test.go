package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kart-io/goagent/builder"
	"github.com/kart-io/goagent/codec/json"
	"github.com/kart-io/goagent/constants"
	"github.com/kart-io/goagent/core/usage"
	"github.com/kart-io/goagent/interfaces"
	"github.com/kart-io/goagent/llm"
	"github.com/kart-io/goagent/llm/providers"
	mg "github.com/kart-io/goagent/management"
	"github.com/kart-io/goagent/management/adapters"
	"github.com/kart-io/goagent/management/httpapi"
	"github.com/kart-io/goagent/tools"
	"github.com/stretchr/testify/require"
)

// 显式启用后，缺凭据或数据库必须失败，不能以跳过冒充真实模型验收。
func TestLiveManagement(t *testing.T) {
	if os.Getenv("GOAGENT_MANAGEMENT_LIVE") != "1" {
		t.Skip("真实模型付费验收需显式设置GOAGENT_MANAGEMENT_LIVE=1")
	}
	require.NotEmpty(t, os.Getenv("GOAGENT_PGSTORE_DSN"), "真实模型验收需要专用Postgres")
	selected := os.Getenv("GOAGENT_MANAGEMENT_LIVE_PROVIDERS")
	if selected == "" {
		selected = "deepseek,gemini"
	}
	for _, name := range strings.Split(selected, ",") {
		t.Run(name, func(t *testing.T) {
			ledger := &liveLedger{}
			var model string
			var create func() (llm.ToolStreamClient, error)
			switch name {
			case "deepseek":
				require.NotEmpty(t, os.Getenv("DEEPSEEK_API_KEY"))
				model = os.Getenv("DEEPSEEK_MODEL")
				if model == "" {
					model = "deepseek-flash"
				}
				create = func() (llm.ToolStreamClient, error) {
					return providers.NewDeepSeekWithOptions(llm.WithModel(model), llm.WithMaxTokens(1024), llm.WithTemperature(0), llm.WithTimeout(60*time.Second))
				}
			case "gemini":
				require.NotEmpty(t, os.Getenv("GEMINI_API_KEY"))
				model = os.Getenv("GEMINI_MODEL")
				if model == "" {
					model = "gemini-2.5-flash"
				}
				create = func() (llm.ToolStreamClient, error) {
					return providers.NewGeminiWithOptions(llm.WithModel(model), llm.WithMaxTokens(1024), llm.WithTemperature(0), llm.WithTimeout(60*time.Second))
				}
			default:
				t.Fatalf("不支持的真实模型提供方：%s", name)
			}
			for _, scenario := range []struct {
				name string
				run  func(*liveCase)
			}{
				{"批准与宿主重建", liveApproveAfterRebuild},
				{"拒绝审批", liveRejectApproval},
				{"等待审批时取消", liveCancelApproval},
				{"多轮上下文与精确Fork", liveConversationFork},
				{"发布版本与会话绑定", liveVersionBinding},
			} {
				t.Run(scenario.name, func(t *testing.T) {
					c := newLiveCase(t, name, model, scenario.name, ledger, create)
					scenario.run(c)
					c.verifyUsage()
				})
			}
		})
	}
}

type liveCall struct {
	TaskID        string          `json:"task_id"`
	ResponseModel string          `json:"response_model,omitempty"`
	FinishReason  string          `json:"finish_reason,omitempty"`
	ToolCalls     []llm.ToolCall  `json:"tool_calls,omitempty"`
	Usage         *llm.Usage      `json:"usage"`
	UsageFields   uint64          `json:"usage_fields"`
	Status        string          `json:"status"`
	ErrorReason   llm.ErrorReason `json:"error_reason,omitempty"`
	MaxTokens     int             `json:"max_output_tokens"`
}

type liveLedger struct {
	mu    sync.Mutex
	calls []liveCall
}

func (l *liveLedger) snapshot() []liveCall {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]liveCall(nil), l.calls...)
}

// 包装器只限额、记录并原样透传，不注入工具调用或模拟用量。
type liveObservedModel struct {
	llm.ToolStreamClient
	ledger *liveLedger
	taskID string
}

func (m *liveObservedModel) CompleteToolsStream(ctx context.Context, req *llm.CompletionRequest, available []interfaces.Tool) (<-chan *llm.StreamChunk, error) {
	if req == nil || req.MaxTokens != 1024 {
		return nil, fmt.Errorf("真实验收必须显式限制每次输出为1024Token")
	}
	m.ledger.mu.Lock()
	if len(m.ledger.calls) >= 20 {
		m.ledger.mu.Unlock()
		return nil, fmt.Errorf("真实验收达到每提供方20次请求上限")
	}
	index := len(m.ledger.calls)
	m.ledger.calls = append(m.ledger.calls, liveCall{TaskID: m.taskID, Status: "started", MaxTokens: req.MaxTokens})
	m.ledger.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	upstream, err := m.ToolStreamClient.CompleteToolsStream(ctx, req, available)
	if err != nil {
		cancel()
		m.ledger.mu.Lock()
		m.ledger.calls[index].Status = "request_error"
		m.ledger.calls[index].ErrorReason = llm.ClassifyError(err).Reason
		m.ledger.mu.Unlock()
		return nil, err
	}
	out := make(chan *llm.StreamChunk)
	go func() {
		defer close(out)
		defer cancel()
		defer func() {
			m.ledger.mu.Lock()
			defer m.ledger.mu.Unlock()
			if m.ledger.calls[index].Status == "started" {
				m.ledger.calls[index].Status = "incomplete"
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case chunk, ok := <-upstream:
				if !ok {
					return
				}
				if chunk == nil {
					continue
				}
				m.ledger.mu.Lock()
				call := &m.ledger.calls[index]
				if chunk.ResponseModel != "" {
					call.ResponseModel = chunk.ResponseModel
				}
				if chunk.Done {
					call.Status = "completed"
					if chunk.Error != nil {
						call.Status = "stream_error"
						call.ErrorReason = llm.ClassifyError(chunk.Error).Reason
					}
					call.FinishReason = chunk.FinishReason
					call.ToolCalls = append([]llm.ToolCall(nil), chunk.ToolCalls...)
					if chunk.Usage != nil {
						copyUsage := *chunk.Usage
						call.Usage = &copyUsage
						call.UsageFields = uint64(chunk.Usage.Fields())
					}
				}
				m.ledger.mu.Unlock()
				select {
				case out <- chunk:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

type liveCase struct {
	t          *testing.T
	f          *fixture
	worker     *mg.Worker
	server     *httptest.Server
	ledger     *liveLedger
	start      int
	agent      mg.Agent
	requests   int
	runs       []mg.Run
	assertions []string
	effects    *int
}

func newLiveCase(t *testing.T, provider, model, scenario string, ledger *liveLedger, create func() (llm.ToolStreamClient, error)) *liveCase {
	t.Helper()
	c := &liveCase{t: t, ledger: ledger, start: len(ledger.snapshot())}
	t.Cleanup(func() {
		report := struct {
			Provider, Model, Scenario string
			Passed                    bool
			Calls                     []liveCall
			Runs                      []mg.Run
			HTTPRequests              int
			Effects                   *int
			Assertions                []string
		}{provider, model, scenario, !t.Failed(), ledger.snapshot()[c.start:], c.runs, c.requests, c.effects, c.assertions}
		raw, err := json.Marshal(report, json.Indent("  "))
		require.NoError(t, err)
		if dir := os.Getenv("GOAGENT_MANAGEMENT_LIVE_REPORT_DIR"); dir != "" {
			require.NoError(t, os.MkdirAll(dir, 0700))
			path := filepath.Join(dir, provider+"-"+scenario+".json")
			file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			require.NoError(t, err, "证据路径必须不存在，避免覆盖上次结果")
			_, err = file.Write(append(raw, '\n'))
			require.NoError(t, err)
			require.NoError(t, file.Close())
		}
		t.Logf("真实模型验收：%s/%s，成功=%t，模型请求=%d，HTTP请求=%d，SQL核对完成=%t", provider, scenario, report.Passed, len(report.Calls), report.HTTPRequests, report.Effects != nil)
	})
	c.f = setup(t, nil)
	_, err := c.f.pool.Exec(c.f.ctx, `CREATE TABLE live_refunds(execution_key text PRIMARY KEY,task_id text UNIQUE NOT NULL,amount integer NOT NULL CHECK(amount=50))`)
	require.NoError(t, err)
	c.f.cfg.Factories = map[string]mg.Factory{"test": adapters.NativeFactory{
		Check: func(_ context.Context, v mg.Version) error {
			if v.ImplementationVersion != "code-v1" && v.ImplementationVersion != "code-v2" {
				return fmt.Errorf("未知案例实现版本")
			}
			return nil
		},
		BuildAgent: func(_ context.Context, v mg.Version, resources mg.Resources, approver tools.Approver) (*builder.SimpleAgentBuilder, func(context.Context) error, error) {
			model, err := create()
			if err != nil {
				return nil, nil, err
			}
			registry := tools.NewRegistry()
			err = registry.Register(tools.NewBaseTool("refund", "申请退款50元；需要人工审批，只有收到成功回执才能声称已退款。", `{"type":"object","properties":{"amount":{"type":"integer","enum":[50]}},"required":["amount"],"additionalProperties":false}`, func(ctx context.Context, in *interfaces.ToolInput) (*interfaces.ToolOutput, error) {
				raw, err := json.Marshal(in.Args)
				if err != nil {
					return nil, err
				}
				var args struct {
					Amount int `json:"amount"`
				}
				if json.Unmarshal(raw, &args) != nil || args.Amount != 50 || in.ExecutionKey == "" {
					return nil, fmt.Errorf("退款参数或持久执行键无效")
				}
				_, err = c.f.pool.Exec(ctx, `INSERT INTO live_refunds VALUES($1,$2,$3) ON CONFLICT(execution_key) DO NOTHING`, in.ExecutionKey, resources.Task.ID, args.Amount)
				return &interfaces.ToolOutput{Success: err == nil, Result: "退款成功；凭证 LIVE-REFUND-" + resources.Task.ID}, err
			}))
			if err != nil {
				return nil, nil, err
			}
			dispatcher, err := tools.NewDispatcher(registry, tools.DispatchConfig{Pipeline: &tools.Pipeline{Pre: []tools.PreHook{func(context.Context, *tools.Call) (tools.PreDecision, error) {
				return tools.PreDecision{Kind: tools.PreAsk, Feedback: "退款必须由人工审批"}, nil
			}}, Approver: approver}})
			if err != nil {
				return nil, nil, err
			}
			prompt := "你是管理验收助手。用户明确要求退款时必须调用refund工具一次，amount为50，不得直接声称完成。工具拒绝时不要重试，明确回复未执行退款；成功时逐字复制完整凭证。其他记忆或版本查询不要调用工具，直接简短回答。用户更新记忆后以最新值为准。当前配置版本标识是 RELEASE-" + v.Version + "；查询版本时只回答此标识。"
			observed := &liveObservedModel{ToolStreamClient: model, ledger: ledger, taskID: resources.Task.ID}
			return builder.NewSimpleBuilder(observed).WithSystemPrompt(prompt).WithTools(dispatcher.Tools()...).WithMaxTokens(1024).WithMaxIterations(3), nil, nil
		},
	}}
	c.startHost()
	c.call("POST", "/v1/agents", map[string]any{"name": "真实模型验收助手"}, "", 201, &c.agent)
	c.publish("v1")
	return c
}

func (c *liveCase) startHost() {
	c.t.Helper()
	var err error
	c.f.s, err = mg.New(c.f.cfg)
	require.NoError(c.t, err)
	handler, err := httpapi.New(c.f.s, httpapi.Config{ActorFromRequest: func(r *http.Request) (mg.Actor, error) {
		if r.Header.Get("Authorization") != "Bearer live-case" {
			return mg.Actor{}, fmt.Errorf("缺少案例认证")
		}
		return c.f.actor, nil
	}})
	require.NoError(c.t, err)
	c.server = httptest.NewServer(handler)
	c.server.Client().Timeout = 10 * time.Second
	c.t.Cleanup(c.server.Close)
	c.worker = c.f.worker(c.t)
}

func (c *liveCase) call(method, path string, value any, key string, status int, out any) {
	c.t.Helper()
	var body io.Reader
	if value != nil {
		raw, err := json.Marshal(value)
		require.NoError(c.t, err)
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(c.f.ctx, method, c.server.URL+path, body)
	require.NoError(c.t, err)
	req.Header.Set("Authorization", "Bearer live-case")
	req.Header.Set("Content-Type", "application/json")
	if method == "POST" {
		if key == "" {
			key = uuid.NewString()
		}
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := c.server.Client().Do(req)
	require.NoError(c.t, err)
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	require.NoError(c.t, err)
	c.requests++
	require.Equal(c.t, status, resp.StatusCode, "%s %s：%s", method, path, raw)
	if out != nil {
		require.NoError(c.t, json.Unmarshal(raw, out))
	}
}

func (c *liveCase) publish(version string) {
	c.call("POST", "/v1/agents/"+c.agent.ID+"/versions", map[string]any{"version": version, "runner_kind": "test", "implementation_version": "code-" + version, "config": map[string]any{}}, "", 201, nil)
	c.call("GET", "/v1/agents/"+c.agent.ID, nil, "", 200, &c.agent)
	c.call("POST", "/v1/agents/"+c.agent.ID+"/publish", map[string]any{"version": version, "expected_revision": c.agent.Revision}, "", 200, &c.agent)
}

func (c *liveCase) session() mg.Session {
	c.t.Helper()
	var se mg.Session
	c.call("POST", "/v1/sessions", map[string]any{"agent_id": c.agent.ID}, "", 202, &se)
	return c.ready(se.ID)
}

func (c *liveCase) ready(id string) mg.Session {
	c.t.Helper()
	var se mg.Session
	require.Eventually(c.t, func() bool {
		c.call("GET", "/v1/sessions/"+id, nil, "", 200, &se)
		return se.Status == "ready"
	}, 10*time.Second, 25*time.Millisecond)
	return se
}

func (c *liveCase) submit(sessionID, text, key string) mg.Task {
	c.t.Helper()
	var task mg.Task
	c.call("POST", "/v1/tasks", map[string]any{"session_id": sessionID, "input": text}, key, 202, &task)
	return task
}

func (c *liveCase) wait(id string, state mg.TaskState) mg.Task {
	c.t.Helper()
	var task mg.Task
	require.Eventually(c.t, func() bool {
		c.call("GET", "/v1/tasks/"+id, nil, "", 200, &task)
		return task.State == state || task.State.Terminal()
	}, 90*time.Second, 50*time.Millisecond)
	require.Equal(c.t, state, task.State, "真实模型任务意外终止：错误类别=%s", task.ErrorKind)
	return task
}

func (c *liveCase) pending(task mg.Task) (mg.Task, mg.Approval) {
	c.t.Helper()
	task = c.wait(task.ID, mg.WaitingApproval)
	var page mg.Page[mg.Approval]
	c.call("GET", "/v1/approvals?task_id="+task.ID+"&status=pending", nil, "", 200, &page)
	require.Len(c.t, page.Items, 1)
	require.Equal(c.t, 0, c.effectCount(task.ID), "审批之前不能写入退款记录")
	return task, page.Items[0]
}

func (c *liveCase) effectCount(taskID string) int {
	c.t.Helper()
	var count int
	require.NoError(c.t, c.f.pool.QueryRow(c.f.ctx, `SELECT count(*) FROM live_refunds WHERE task_id=$1`, taskID).Scan(&count))
	return count
}

func (c *liveCase) decide(ap mg.Approval, decision, key string, status int) mg.Approval {
	c.t.Helper()
	var result mg.Approval
	c.call("POST", "/v1/approvals/"+ap.ID+"/decision", map[string]any{"expected_revision": ap.Revision, "decision": decision, "reason": "真实模型案例人工决定"}, key, status, &result)
	return result
}

func liveApproveAfterRebuild(c *liveCase) {
	se := c.session()
	key := uuid.NewString()
	task := c.submit(se.ID, "请调用工具退还50元，并返回完整退款凭证。", key)
	require.Equal(c.t, task.ID, c.submit(se.ID, "请调用工具退还50元，并返回完整退款凭证。", key).ID)
	_, ap := c.pending(task)
	require.Len(c.t, c.ledger.snapshot()[c.start:], 1)
	require.NoError(c.t, c.worker.Shutdown(c.f.ctx))
	c.server.Close()
	c.startHost()
	var after mg.Approval
	c.call("GET", "/v1/approvals/"+ap.ID, nil, "", 200, &after)
	require.Equal(c.t, ap.Wait.IntentHash, after.Wait.IntentHash)
	decisionKey := uuid.NewString()
	decided := c.decide(after, "approve", decisionKey, 200)
	replayed := c.decide(after, "approve", decisionKey, 200)
	require.Equal(c.t, decided.DecidedAt, replayed.DecidedAt)
	task = c.wait(task.ID, mg.Succeeded)
	require.Contains(c.t, string(task.Result), "LIVE-REFUND-"+task.ID)
	require.Equal(c.t, 1, c.effectCount(task.ID))
	require.Len(c.t, c.ledger.snapshot()[c.start:], 2, "恢复不能再次发送审批前的模型请求")
	require.Equal(c.t, task.ID, c.submit(se.ID, "请调用工具退还50元，并返回完整退款凭证。", key).ID)
	// 幂等响应是最初的queued快照；重新读取当前任务，检查重投没有使终态倒退。
	var afterReplay mg.Task
	c.call("GET", "/v1/tasks/"+task.ID, nil, "", 200, &afterReplay)
	require.Equal(c.t, mg.Succeeded, afterReplay.State)
	require.Equal(c.t, task.Revision, afterReplay.Revision)
	require.Equal(c.t, task.Result, afterReplay.Result)
	var starts int
	require.NoError(c.t, c.f.pool.QueryRow(c.f.ctx, `SELECT count(*) FROM mg_commands WHERE data->>'task_id'=$1 AND data->>'kind'='start'`, task.ID).Scan(&starts))
	require.Equal(c.t, 1, starts, "终态幂等重投不能再接纳启动命令")
	c.assertions = append(c.assertions, "审批前0效果", "重建Service和Worker后批准续跑", "重复提交及重复审批均复用原记录", "模型请求恰好2次且SQL效果仅1次", "模型返回真实工具回执")
}

func liveRejectApproval(c *liveCase) {
	task := c.submit(c.session().ID, "请调用工具退还50元。", "")
	_, ap := c.pending(task)
	c.decide(ap, "reject", "", 200)
	task = c.wait(task.ID, mg.Succeeded)
	require.Equal(c.t, 0, c.effectCount(task.ID))
	require.NotContains(c.t, string(task.Result), "LIVE-REFUND-")
	text := string(task.Result)
	require.True(c.t, strings.Contains(text, "拒绝") || strings.Contains(text, "未执行") || strings.Contains(text, "未退款"), "模型应说明退款未执行：%s", text)
	require.Len(c.t, c.ledger.snapshot()[c.start:], 2)
	c.assertions = append(c.assertions, "真实模型提出工具调用", "拒绝后模型明确拒绝且不再次申请", "SQL效果0")
}

func liveCancelApproval(c *liveCase) {
	task := c.submit(c.session().ID, "请调用工具退还50元。", "")
	task, ap := c.pending(task)
	c.call("POST", "/v1/tasks/"+task.ID+"/cancel", map[string]any{"expected_revision": task.Revision, "reason": "案例取消"}, "", 202, nil)
	c.wait(task.ID, mg.Canceled)
	c.decide(ap, "approve", "", 409)
	require.Equal(c.t, 0, c.effectCount(task.ID))
	require.Len(c.t, c.ledger.snapshot()[c.start:], 1, "取消和迟到审批不能再次请求模型")
	c.assertions = append(c.assertions, "真实模型已生成工具请求并等待审批", "等待时取消且迟到批准409", "SQL效果0且不追加模型请求")
}

func liveConversationFork(c *liveCase) {
	se := c.session()
	oldMarker := "LIVE-" + uuid.NewString()
	newMarker := "LIVE-" + uuid.NewString()
	task := c.submit(se.ID, "请记住当前验收代码为"+oldMarker+"。只回复该代码，不要调用工具。", "")
	first := c.wait(task.ID, mg.Succeeded)
	require.Contains(c.t, string(first.Result), oldMarker)
	var history mg.HistoryPage
	c.call("GET", "/v1/sessions/"+se.ID+"/history?limit=100", nil, "", 200, &history)
	var boundary int64
	for _, event := range history.Items {
		if event.Type == constants.SessionEventTurnEnd {
			boundary = event.Seq
		}
	}
	require.Positive(c.t, boundary)
	task = c.submit(se.ID, "现在把验收代码更新为"+newMarker+"。只回复新代码，不要调用工具。", "")
	require.Contains(c.t, string(c.wait(task.ID, mg.Succeeded).Result), newMarker)
	se = c.ready(se.ID)
	var branch mg.Session
	c.call("POST", "/v1/sessions/"+se.ID+"/fork", map[string]any{"expected_revision": se.Revision, "up_to": boundary}, "", 202, &branch)
	c.ready(branch.ID)
	task = c.submit(branch.ID, "当前验收代码是什么？只回复代码，不要调用工具。", "")
	childResult := c.wait(task.ID, mg.Succeeded)
	require.Contains(c.t, string(childResult.Result), oldMarker)
	require.NotContains(c.t, string(childResult.Result), newMarker)
	task = c.submit(se.ID, "当前验收代码是什么？只回复代码，不要调用工具。", "")
	parentResult := c.wait(task.ID, mg.Succeeded)
	require.Contains(c.t, string(parentResult.Result), newMarker)
	require.NotContains(c.t, string(parentResult.Result), oldMarker)
	require.Len(c.t, c.ledger.snapshot()[c.start:], 4)
	c.assertions = append(c.assertions, "模型通过持久日志读到多轮上下文", "指定完整轮次Fork仅继承旧代码", "父会话仍读到新代码", "全部代码每轮随机生成且查询不携带答案")
}

func liveVersionBinding(c *liveCase) {
	old := c.session()
	c.publish("v2")
	newSession := c.session()
	for _, row := range []struct {
		session mg.Session
		version string
	}{{old, "v1"}, {newSession, "v2"}} {
		task := c.submit(row.session.ID, "请只回答当前配置版本标识，不要调用工具。", "")
		task = c.wait(task.ID, mg.Succeeded)
		require.Equal(c.t, row.version, task.Version)
		var text string
		require.NoError(c.t, json.Unmarshal(task.Result, &text))
		require.Contains(c.t, text, "RELEASE-"+row.version)
		other := "v1"
		if row.version == "v1" {
			other = "v2"
		}
		require.NotContains(c.t, text, "RELEASE-"+other)
	}
	require.Len(c.t, c.ledger.snapshot()[c.start:], 2)
	c.assertions = append(c.assertions, "发布v2后旧Session真实执行v1指令", "新Session真实执行v2指令", "调用真实模型输出版本而非仅检查元数据")
}

func (c *liveCase) verifyUsage() {
	c.t.Helper()
	require.NoError(c.t, c.worker.Shutdown(c.f.ctx))
	calls := c.ledger.snapshot()[c.start:]
	require.NotEmpty(c.t, calls)
	byTask := map[string]interfaces.TokenUsage{}
	counts := map[string]int{}
	var total interfaces.TokenUsage
	for _, call := range calls {
		require.Equal(c.t, "completed", call.Status)
		require.NotNil(c.t, call.Usage, "缺失用量不能冒充零Token")
		require.Equal(c.t, usage.BasicFields, usage.Fields(call.UsageFields)&usage.BasicFields, "未报告完整基础用量不能判定计量通过")
		require.Positive(c.t, call.Usage.TotalTokens)
		value := byTask[call.TaskID]
		value.PromptTokens += call.Usage.PromptTokens
		value.CompletionTokens += call.Usage.CompletionTokens
		value.TotalTokens += call.Usage.TotalTokens
		byTask[call.TaskID] = value
		counts[call.TaskID]++
		total.PromptTokens += call.Usage.PromptTokens
		total.CompletionTokens += call.Usage.CompletionTokens
		total.TotalTokens += call.Usage.TotalTokens
	}
	before := len(c.ledger.snapshot())
	for taskID, tokens := range byTask {
		var run mg.Run
		c.call("GET", "/v1/runs/"+taskID, nil, "", 200, &run)
		require.NotNil(c.t, run.Usage)
		require.Equal(c.t, counts[taskID], run.Usage.Root.ReservedRequests)
		require.Equal(c.t, tokens.PromptTokens, run.Usage.Root.KnownTokens.PromptTokens)
		require.Equal(c.t, tokens.CompletionTokens, run.Usage.Root.KnownTokens.CompletionTokens)
		require.Equal(c.t, tokens.TotalTokens, run.Usage.Root.KnownTokens.TotalTokens)
		require.Zero(c.t, run.Usage.Root.UnknownUsageEntries)
		c.runs = append(c.runs, run)
	}
	for range 2 {
		var summary mg.RunSummary
		c.call("GET", "/v1/runs/summary?limit=1", nil, "", 200, &summary)
		require.Equal(c.t, len(byTask), summary.Totals.Tasks)
		require.NotNil(c.t, summary.Totals.KnownTokens)
		require.Equal(c.t, total.PromptTokens, summary.Totals.KnownTokens.PromptTokens)
		require.Equal(c.t, total.CompletionTokens, summary.Totals.KnownTokens.CompletionTokens)
		require.Equal(c.t, total.TotalTokens, summary.Totals.KnownTokens.TotalTokens)
		require.Zero(c.t, summary.Totals.UnknownRuns)
	}
	require.Len(c.t, c.ledger.snapshot(), before, "只读查询不能触发真实模型请求")
	var effects, sum, uniqueTasks, uniqueKeys int
	require.NoError(c.t, c.f.pool.QueryRow(c.f.ctx, `SELECT count(*),count(DISTINCT task_id),count(DISTINCT execution_key),COALESCE(sum(amount),0) FROM live_refunds`).Scan(&effects, &uniqueTasks, &uniqueKeys, &sum))
	c.effects = &effects
	require.Equal(c.t, effects, uniqueTasks)
	require.Equal(c.t, effects, uniqueKeys)
	require.Equal(c.t, effects*50, sum)
	c.assertions = append(c.assertions, "提供方实际prompt/completion/total用量逐Task等于GetRun", "跨Session汇总与提供方实际用量一致", "重复只读查询不产生模型请求", "独立SQL核对效果与唯一执行键")
}
