// 管理服务的离线宿主案例：真实 HTTP、Postgres、工具审批和固定版本执行。
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	journal "github.com/kart-io/goagent-contrib/durable/pgstore"
	db "github.com/kart-io/goagent-contrib/management/pgstore"
	"github.com/kart-io/goagent/builder"
	"github.com/kart-io/goagent/codec/json"
	"github.com/kart-io/goagent/constants"
	agentErrors "github.com/kart-io/goagent/errors"
	"github.com/kart-io/goagent/interfaces"
	"github.com/kart-io/goagent/llm"
	mg "github.com/kart-io/goagent/management"
	"github.com/kart-io/goagent/management/adapters"
	"github.com/kart-io/goagent/management/httpapi"
	"github.com/kart-io/goagent/tools"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type model struct {
	llm.Client
	version string
}

func (m *model) Provider() constants.Provider { return constants.ProviderOpenAI }
func (m *model) CompleteToolsStream(_ context.Context, r *llm.CompletionRequest, _ []interfaces.Tool) (<-chan *llm.StreamChunk, error) {
	finished, rejected := false, false
	for _, msg := range r.Messages {
		if msg.Role == "user" {
			finished = false
			rejected = false
		}
		if msg.Role == "tool" {
			finished = true
			rejected = strings.Contains(msg.Content, "拒绝") || strings.Contains(msg.Content, "denied")
		}
	}
	ch := make(chan *llm.StreamChunk, 1)
	if finished {
		text := m.version + "：退款凭证已生成"
		if rejected {
			text = m.version + "：审批拒绝，未执行退款"
		}
		ch <- &llm.StreamChunk{Delta: text, Done: true, FinishReason: "stop", Usage: &llm.Usage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5}}
	} else {
		call := llm.ToolCall{ID: "refund-request", Type: "function"}
		call.Function.Name = "refund"
		call.Function.Arguments = `{"amount":50}`
		ch <- &llm.StreamChunk{Done: true, FinishReason: "tool_calls", ToolCalls: []llm.ToolCall{call}, Usage: &llm.Usage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5}}
	}
	close(ch)
	return ch, nil
}
func factory(pool *pgxpool.Pool) mg.Factory {
	return adapters.NativeFactory{Check: func(_ context.Context, v mg.Version) error {
		if v.ImplementationVersion != "demo-v1" && v.ImplementationVersion != "demo-v2" {
			return agentErrors.ErrManagementUnsupported
		}
		return nil
	}, BuildAgent: func(ctx context.Context, v mg.Version, r mg.Resources, ap tools.Approver) (*builder.SimpleAgentBuilder, func(context.Context) error, error) {
		registry := tools.NewRegistry()
		err := registry.Register(tools.NewBaseTool("refund", "退款50元", `{"type":"object","properties":{"amount":{"type":"integer"}},"required":["amount"],"additionalProperties":false}`, func(ctx context.Context, in *interfaces.ToolInput) (*interfaces.ToolOutput, error) {
			_, err := pool.Exec(ctx, `INSERT INTO demo_management_refunds(execution_key,tenant_id,task_id,amount) VALUES($1,$2,$3,50) ON CONFLICT(execution_key) DO NOTHING`, in.ExecutionKey, r.Task.TenantID, r.Task.ID)
			return &interfaces.ToolOutput{Success: err == nil, Result: "50元退款凭证"}, err
		}))
		if err != nil {
			return nil, nil, err
		}
		dispatcher, err := tools.NewDispatcher(registry, tools.DispatchConfig{Pipeline: &tools.Pipeline{Pre: []tools.PreHook{func(context.Context, *tools.Call) (tools.PreDecision, error) {
			return tools.PreDecision{Kind: tools.PreAsk, Feedback: "50元退款需要人工审批"}, nil
		}}, Approver: ap}})
		if err != nil {
			return nil, nil, err
		}
		return builder.NewSimpleBuilder(&model{version: v.Version}).WithTools(dispatcher.Tools()...).WithMaxIterations(4), nil, nil
	}}
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() (runErr error) {
	address := flag.String("listen", "127.0.0.1:8097", "管理 HTTP 监听地址")
	migrate := flag.Bool("migrate", false, "显式初始化案例数据库表")
	verify := flag.Bool("verify", false, "通过 HTTP 验证完整案例后退出")
	flag.Parse()
	dsn := os.Getenv("GOAGENT_PGSTORE_DSN")
	if dsn == "" {
		return errors.New("请设置 GOAGENT_PGSTORE_DSN 指向本地案例数据库")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	if *migrate {
		if err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			if _, e := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(80261109)`); e != nil {
				return e
			}
			var exists bool
			if e := tx.QueryRow(ctx, `SELECT to_regclass('goagent_durable_schema') IS NOT NULL`).Scan(&exists); e != nil {
				return e
			}
			if !exists {
				if _, e := tx.Exec(ctx, journal.Schema+journal.SessionSchema); e != nil {
					return e
				}
			}
			return nil
		}); err != nil {
			return err
		}
		if err = db.Migrate(ctx, pool); err != nil {
			return err
		}
		if _, err = pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS demo_management_refunds(execution_key text PRIMARY KEY,tenant_id text NOT NULL,task_id text NOT NULL,amount integer NOT NULL)`); err != nil {
			return err
		}
	}
	store, err := db.New(ctx, pool)
	if err != nil {
		return err
	}
	j, err := journal.New(ctx, journal.Config{Pool: pool})
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, j.Close(context.Background())) }()
	sessions, err := journal.NewSessionStore(ctx, j)
	if err != nil {
		return err
	}
	service, err := mg.New(mg.Config{Store: store, Sessions: sessions, HistoryReader: store, Authorizer: mg.TenantAuthorizer{}, Factories: map[string]mg.Factory{"refund": factory(pool)}, Report: func(stage string, err error) { fmt.Fprintln(os.Stderr, "宿主诊断：", stage, err) }})
	if err != nil {
		return err
	}
	actor := mg.Actor{TenantID: "demo-" + uuid.NewString(), Subject: "案例审批人"}
	if !*verify {
		actor.TenantID = "demo"
	}
	token := os.Getenv("GOAGENT_DEMO_TOKEN")
	if token == "" {
		token = "local-demo"
	}
	handler, err := httpapi.New(service, httpapi.Config{ActorFromRequest: func(r *http.Request) (mg.Actor, error) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			return mg.Actor{}, agentErrors.ErrManagementUnauthenticated
		}
		return actor, nil
	}})
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	worker := mg.NewWorker(service)
	workCtx, cancelWorker := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(workCtx) }()
	httpDone := make(chan error, 1)
	go func() { httpDone <- server.Serve(listener) }()
	defer func() {
		worker.BeginShutdown()
		cancelWorker()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
		_ = worker.Shutdown(shutdown)
	}()
	if *verify {
		return verifyCase(ctx, "http://"+listener.Addr().String(), token, pool, actor)
	}
	fmt.Println("管理 API 已启动：", listener.Addr().String())
	select {
	case <-ctx.Done():
		return nil
	case err := <-done:
		return err
	case err := <-httpDone:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

type client struct {
	url, token string
	http       *http.Client
}

func (c client) call(ctx context.Context, method, path string, value any, key string, out any) error {
	var body io.Reader
	if value != nil {
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.url+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Idempotency-Key", key)
	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return fmt.Errorf("HTTP %d：%s", res.StatusCode, raw)
	}
	return json.Decode(res.Body, out)
}
func (c client) wait(ctx context.Context, path string, done func(json.RawMessage) bool) error {
	for {
		var raw json.RawMessage
		if err := c.call(ctx, "GET", path, nil, "", &raw); err != nil {
			return err
		}
		if done(raw) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}
func verifyCase(parent context.Context, url, token string, pool *pgxpool.Pool, actor mg.Actor) error {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	c := client{url, token, &http.Client{Timeout: 5 * time.Second}}
	var agent mg.Agent
	if err := c.call(ctx, "POST", "/v1/agents", map[string]any{"name": "退款客服"}, uuid.NewString(), &agent); err != nil {
		return err
	}
	for _, v := range []string{"v1", "v2"} {
		var version mg.Version
		if err := c.call(ctx, "POST", "/v1/agents/"+agent.ID+"/versions", map[string]any{"version": v, "runner_kind": "refund", "implementation_version": "demo-" + v, "config": map[string]any{}}, uuid.NewString(), &version); err != nil {
			return err
		}
	}
	publish := func(v string) error {
		return c.call(ctx, "POST", "/v1/agents/"+agent.ID+"/publish", map[string]any{"version": v, "expected_revision": agent.Revision}, uuid.NewString(), &agent)
	}
	if err := publish("v1"); err != nil {
		return err
	}
	var se mg.Session
	if err := c.call(ctx, "POST", "/v1/sessions", map[string]any{"agent_id": agent.ID}, uuid.NewString(), &se); err != nil {
		return err
	}
	if err := c.wait(ctx, "/v1/sessions/"+se.ID, func(raw json.RawMessage) bool { _ = json.Unmarshal(raw, &se); return se.Status == "ready" }); err != nil {
		return err
	}
	var task mg.Task
	key := uuid.NewString()
	body := map[string]any{"session_id": se.ID, "input": "请退还50元"}
	if err := c.call(ctx, "POST", "/v1/tasks", body, key, &task); err != nil {
		return err
	}
	var duplicate mg.Task
	if err := c.call(ctx, "POST", "/v1/tasks", body, key, &duplicate); err != nil {
		return err
	}
	if duplicate.ID != task.ID {
		return errors.New("重复提交产生了新任务")
	}
	var approvals mg.Page[mg.Approval]
	if err := c.wait(ctx, "/v1/approvals?task_id="+task.ID+"&status=pending", func(raw json.RawMessage) bool { _ = json.Unmarshal(raw, &approvals); return len(approvals.Items) == 1 }); err != nil {
		return err
	}
	ap := approvals.Items[0]
	var decision mg.Approval
	if err := c.call(ctx, "POST", "/v1/approvals/"+ap.ID+"/decision", map[string]any{"expected_revision": ap.Revision, "decision": "approve", "reason": "金额与业务事实符合"}, uuid.NewString(), &decision); err != nil {
		return err
	}
	if err := c.wait(ctx, "/v1/tasks/"+task.ID, func(raw json.RawMessage) bool { _ = json.Unmarshal(raw, &task); return task.State.Terminal() }); err != nil {
		return err
	}
	if task.State != mg.Succeeded {
		return fmt.Errorf("任务失败：%s", task.ErrorKind)
	}
	if err := publish("v2"); err != nil {
		return err
	}
	var fresh mg.Session
	if err := c.call(ctx, "POST", "/v1/sessions", map[string]any{"agent_id": agent.ID}, uuid.NewString(), &fresh); err != nil {
		return err
	}
	if err := c.call(ctx, "GET", "/v1/sessions/"+se.ID, nil, "", &se); err != nil {
		return err
	}
	if se.Version != "v1" || fresh.Version != "v2" {
		return errors.New("版本隔离验证失败")
	}
	var branch mg.Session
	if err := c.call(ctx, "POST", "/v1/sessions/"+se.ID+"/fork", map[string]any{"up_to": 0, "expected_revision": se.Revision}, uuid.NewString(), &branch); err != nil {
		return err
	}
	if err := c.wait(ctx, "/v1/sessions/"+branch.ID, func(raw json.RawMessage) bool { _ = json.Unmarshal(raw, &branch); return branch.Status == "ready" }); err != nil {
		return err
	}
	var run mg.Run
	if err := c.call(ctx, "GET", "/v1/runs/"+task.ID, nil, "", &run); err != nil {
		return err
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM demo_management_refunds WHERE tenant_id=$1`, actor.TenantID).Scan(&count); err != nil {
		return err
	}
	if count != 1 || run.Usage == nil || run.Usage.Root.KnownTokens.TotalTokens != 10 {
		return errors.New("效果次数或用量去重验证失败")
	}
	summary := map[string]any{"验证": "通过", "Agent": agent.ID, "Task": task.ID, "结果": task.Result, "退款次数": count, "Token": run.Usage.Root.KnownTokens.TotalTokens, "旧会话版本": se.Version, "新会话版本": fresh.Version, "分支版本": branch.Version, "重复提交复用任务": duplicate.ID == task.ID}
	raw, _ := json.Marshal(summary, json.Pretty)
	fmt.Println(string(raw))
	return nil
}
