# HTTP Agent 使用指南

适用代码：`httpagent` 提交 `cfb3e85`，依赖 GoAgent v0.32.2，Go 1.25。该提交已推送，尚未发布模块 tag。接口与默认值见 [API 参考](API_REFERENCE.md)，生命周期与实现取舍见 [设计文档](DESIGN.md)。

## 1. 选择接入方式

| 需求 | 入口 | 宿主需要提供 |
| --- | --- | --- |
| 同步调用 Agent，返回完整结果 | POST /invoke | Factory |
| 增量文本、工具事件与最终结果 | POST /stream | 支持原生事件流的 Binding |
| 同一连接多次执行、显式取消 | GET /ws | 默认Gorilla，或注入WebSocket适配器 |
| 自有系统同步事件回调 | POST /webhooks/{name}，inline | Decode |
| 事件先持久接纳，独立worker执行 | 同路径，queued | Decode、持久Acceptor、宿主worker |
| 框架自行完成WebSocket握手 | Handler.ServeWebSocket | 实现WebSocketConn |
| 给运行结果发送外部通知 | Config.Delivery | Sender目标，或宿主outbox |

模块本身不提供Web页面。页面与客户端在 [goagent-examples 的HTTP案例](https://github.com/kart-io/goagent-examples/tree/main/orchestration/httpagent)。入口适配不改变GoAgent的定位，业务Agent、工具、workflow、存储和模型由宿主装配。

## 2. 先运行离线案例

```sh
git clone -b main https://github.com/kart-io/goagent-examples.git
cd goagent-examples/orchestration/httpagent
GOWORK=off go run .
```

打开 http://127.0.0.1:8097 。另一个终端在同目录执行：

```sh
GOWORK=off go run ./cmd/client -case all -input '查询订单 E100'
```

预期4条completed和1条cancelled。默认仅模型回答由离线脚本控制，Builder、Plugin、Dispatcher、工具与网络都实际执行。完整的三终端双向Webhook、SSE、WS及宿主升级步骤见案例的 [SCENARIOS.md](https://github.com/kart-io/goagent-examples/blob/main/orchestration/httpagent/SCENARIOS.md)。无需本地contrib工作树；修改扩展并联调时才需要workspace。

## 3. 在自己的服务中装配

创建独立目录并安装已经推送的扩展提交：

```sh
go mod init example.com/httpagent-demo
go get github.com/kart-io/goagent-contrib/httpagent@v0.0.0-20261031151618-cfb3e85df18a
```

将以下完整程序保存为main.go。它使用实际OpenAI兼容模型、一个Plugin工具、inline Webhook、Observer，以及可选出站通知。

```go
package main

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "log"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/kart-io/goagent-contrib/httpagent"
    "github.com/kart-io/goagent/builder"
    "github.com/kart-io/goagent/interfaces"
    "github.com/kart-io/goagent/llm"
    "github.com/kart-io/goagent/llm/providers"
    "github.com/kart-io/goagent/plugin"
    "github.com/kart-io/goagent/tools"
)

type echoPlugin struct{}

func (echoPlugin) Name() string { return "echo-demo" }
func (echoPlugin) Version() string { return "1.0.0" }
func (echoPlugin) Contribute(context.Context, *plugin.Env) (plugin.Contribution, error) {
    tool := tools.NewBaseTool("echo_input", "返回输入文本",
        `{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}`,
        func(ctx context.Context, in *interfaces.ToolInput) (*interfaces.ToolOutput, error) {
            if err := ctx.Err(); err != nil { return nil, err }
            return &interfaces.ToolOutput{Success: true, Result: fmt.Sprint(in.Args["text"])}, nil
        })
    return plugin.Contribution{
        Tools: []interfaces.Tool{tool},
        PreSteps: []plugin.PreStep{{Identity: "request-audit", Hook: func(ctx context.Context, _ *builder.Step) (builder.PreStepResult, error) {
            if err := ctx.Err(); err != nil { return builder.PreStepResult{}, err }
            log.Print("原生Hook：模型请求前")
            return builder.PreStepResult{}, nil
        }}},
    }, nil
}

func run() error {
    for _, key := range []string{"OPENAI_API_KEY", "OPENAI_BASE_URL", "OPENAI_MODEL"} {
        if os.Getenv(key) == "" { return fmt.Errorf("请配置 %s", key) }
    }
    client, err := providers.NewOpenAIWithOptions(
        llm.WithAPIKey(os.Getenv("OPENAI_API_KEY")),
        llm.WithBaseURL(os.Getenv("OPENAI_BASE_URL")),
        llm.WithModel(os.Getenv("OPENAI_MODEL")), llm.WithTemperature(0))
    if err != nil { return err }
    if !llm.HasToolStreaming(client) { return errors.New("模型客户端不支持原生工具流") }
    host, err := plugin.NewHost(echoPlugin{})
    if err != nil { return err }
    if err = host.Start(context.Background()); err != nil { return err }
    cfg := httpagent.Config{
        Factory: func(ctx context.Context, r httpagent.RunRequest) (httpagent.Binding, httpagent.Cleanup, error) {
            b := builder.NewSimpleBuilder(client).WithSessionID(r.ID).
                WithTemperature(0).WithMaxIterations(4).WithTimeout(10 * time.Minute)
            if err := plugin.Install(ctx, b, host, plugin.Dispatch(tools.DispatchConfig{})); err != nil {
                return httpagent.Binding{}, nil, err
            }
            agent, err := b.BuildContext(ctx)
            if err != nil { return httpagent.Binding{}, nil, err }
            return httpagent.Native(agent.ExecuteStream), nil, nil
        },
        Receivers: map[string]httpagent.Receiver{
            "demo": {Mode: "inline", Decode: func(_ context.Context, _ *http.Request, raw []byte) (httpagent.Incoming, error) {
                var body struct { Message string `json:"message"` }
                if err := json.Unmarshal(raw, &body); err != nil { return httpagent.Incoming{}, err }
                if body.Message == "" { return httpagent.Incoming{}, errors.New("message不能为空") }
                return httpagent.Incoming{Source: "demo", Input: body.Message}, nil
            }},
        },
        Observer: func(_ context.Context, e httpagent.RunEvent) error {
            log.Printf("event=%s run_id=%s event_id=%s", e.EventType, e.RunID, e.EventID)
            return nil
        },
        Report: func(_ context.Context, r httpagent.Report) {
            log.Printf("run_id=%s stage=%s error=%v", r.RunID, r.Stage, r.Err)
        },
    }
    if target := os.Getenv("WEBHOOK_URL"); target != "" {
        cfg.Delivery, err = httpagent.NewSender(httpagent.SenderConfig{
            Targets: []httpagent.Target{{URL: target}},
        })
        if err != nil { _ = host.Stop(context.Background()); return err }
    }
    handler, err := httpagent.New(cfg)
    if err != nil { _ = host.Stop(context.Background()); return err }
    mux := http.NewServeMux()
    mux.Handle("/api/", http.StripPrefix("/api", handler))
    addr := os.Getenv("HTTP_ADDR")
    if addr == "" { addr = "127.0.0.1:8097" }
    server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
    stop, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer cancel()
    serveErr := make(chan error, 1)
    go func() { serveErr <- server.ListenAndServe() }()
    var listenErr error
    select {
    case <-stop.Done():
    case listenErr = <-serveErr:
        if errors.Is(listenErr, http.ErrServerClosed) { listenErr = nil }
    }
    handler.BeginShutdown()
    shutdown, release := context.WithTimeout(context.Background(), 10 * time.Second)
    defer release()
    if err := errors.Join(server.Shutdown(shutdown), handler.Shutdown(shutdown)); err != nil {
        return err // 仍有所有者时不提前关闭共享Host。
    }
    return errors.Join(listenErr, host.Stop(shutdown))
}

func main() {
    if err := run(); err != nil { log.Fatal(err) }
}
```

执行 `go mod tidy` 后配置自己的兼容模型。若本机Ollama已经运行并安装llama3.1:8b，可使用：

```sh
OPENAI_API_KEY=local-ollama \
OPENAI_BASE_URL=http://127.0.0.1:11434/v1 \
OPENAI_MODEL=llama3.1:8b go run .
```

该程序真实调用模型；客户端的工具流能力声明不保证远端任意模型都正确支持工具。没有本地模型时，可先使用上一节离线案例。

Factory为每个运行新建Builder/Agent，共享Plugin Host；返回的Cleanup只释放请求级资源。若采用普通同步回答，可显式返回 `httpagent.Answer(agent.Execute)`，但 `/stream` 会在Run前返回501，不会自动降级重执行业务。Native在各入口使用同一ExecuteStream循环，保留原生工具执行；它不凭方法存在性判断模型能力。

## 4. 调用JSON、SSE和WebSocket

以下路径含宿主挂载前缀 `/api`；Handler内部路由本身没有此前缀。

```sh
curl -H 'Content-Type: application/json' \
  -d '{"input":"请调用echo_input，text为订单E101"}' http://127.0.0.1:8097/api/invoke
curl -N -H 'Content-Type: application/json' \
  -d '{"input":"请调用echo_input，text为订单E102"}' http://127.0.0.1:8097/api/stream
curl -H 'Content-Type: application/json' \
  -d '{"message":"请查询订单E103"}' http://127.0.0.1:8097/api/webhooks/demo
```

SSE客户端先检查状态与Content-Type：首次提交流响应前的失败可能是JSON错误；一旦HTTP200流已提交，失败通过error事件表达。解析UTF-8网络分块与完整空行分隔帧，注释心跳不是业务事件。没有done/error的EOF表示未确认，不能自动重新提交可能已有副作用的任务。

WS连接需请求 `goagent.v1` 子协议。先发送run，收到accepted后保存服务端run_id；以相同run_id发送cancel。必须收到业务终态后再等ready，之后才能提交下一次运行。accepted只是进程内登记；seq不是恢复游标。完整帧格式见 [协议参考](API_REFERENCE.md)。

## 5. Hooks、Observer与出站Webhook

| 扩展点 | 使用场景 |
| --- | --- |
| 标准HTTP中间件/框架中间件 | 认证、租户上下文、CORS与宿主路由 |
| Plugin的PreSteps/RequestErrors/TurnStopping等 | 模型请求、重试和业务循环裁决 |
| Config.Observer | run.started/completed/failed/cancelled观察 |
| Config.Delivery | 同一生命周期信封的外部通知或宿主outbox接纳 |
| Config.Report | 观察、投递、传输和清理的独立诊断 |

Plugin Hook与Builder.Callback保留核心语义，不能用构建期的Callback.OnStart代替运行生命周期。Observer先于Delivery调用，错误/panic独立报告；它应及时返回并遵守context。阻塞Observer会拖延当前发布；started发布还位于Factory之前，不能把它当异步消息队列。

设置 `WEBHOOK_URL` 后，完整程序向目标发送终态POST；目标2xx表示本次成功，失败只报告，不重跑Agent。接收端可用event_id识别同一事件。默认Sender每目标只做一次应用层尝试、不跟随重定向；没有持久重投保证。设置 `Target.Events` 可显式订阅run.started。DeliveryBudget是每次生命周期发布的总预算，Observer与所有目标共享；started与终态分别开始预算。可靠通知需宿主实现DeliverySink并使用持久outbox。

## 6. queued接纳与独立worker

inline等待业务完成才应答。第三方平台要求快速ACK时，选择queued并提供持久Acceptor。以下完整装配函数不会创建队列；参数acceptor必须由宿主实现。宿主在进入Handler前已验证平台身份/签名，并将可信租户写入context。

```go
package wiring

import (
    "context"
    "encoding/json"
    "errors"
    "net/http"
    "github.com/kart-io/goagent-contrib/httpagent"
)

type TenantKey struct{}

func Queued(factory httpagent.Factory, acceptor httpagent.Acceptor) (*httpagent.Handler, error) {
    return httpagent.New(httpagent.Config{Factory: factory,
        Receivers: map[string]httpagent.Receiver{"orders": {
            Mode: "queued", Acceptor: acceptor,
            Decode: func(ctx context.Context, _ *http.Request, raw []byte) (httpagent.Incoming, error) {
                tenant, _ := ctx.Value(TenantKey{}).(string)
                if tenant == "" { return httpagent.Incoming{}, errors.New("缺少可信租户") }
                var body struct { EventID string `json:"event_id"`; Input string `json:"input"` }
                if err := json.Unmarshal(raw, &body); err != nil { return httpagent.Incoming{}, err }
                return httpagent.Incoming{Source: "orders", Tenant: tenant, EventID: body.EventID, Input: body.Input}, nil
            },
        }},
    })
}

func ExecuteAccepted(workerCtx context.Context, h *httpagent.Handler, in httpagent.Incoming, receipt httpagent.Receipt) error {
    _, err := h.Run(workerCtx, httpagent.RunRequest{
        ID: receipt.RunID, Input: in.Input, Source: "worker", Webhook: in.Receiver,
    })
    return err
}
```

Acceptor必须在返回前持久保存事件、业务输入、去重键、PayloadHash和稳定RunID，成功后才ACK202（也可配置200）；RunID还须满足worker入口最长128字节的约束。去重范围至少包含租户/source/receiver/event_id；同键同payload复用RunID，不同payload返回Failure{Code:"conflict"}。适配器计算原始body的SHA-256并设置Receiver名。平台challenge可由Decode返回WebhookReply，当前只通过JSON响应。

worker从宿主持久存储读取输入及RunID，用独立生命周期context调用Run。运行结束后如何确认任务、重试和恢复由宿主负责。Run只防止相同ID当前并发执行，不持久缓存已完成结果；稳定RunID不等于重试只执行一次。需要恢复时组合现有durable执行层与业务幂等，不能把测试中的磁盘Acceptor当生产队列后端。

## 7. 接入其他框架

保留net/http升级过程时，设置 `Config.WebSocketUpgrader` 或 `WebSocketUpgraderFunc`。Handler先校验Origin/子协议并登记，适配器只负责正确完成升级，按options协商goagent.v1和设置握手期限。未注入时仍使用Gorilla。

框架自行升级时，实现 `WebSocketConn`，然后阻塞调用 `h.ServeWebSocket(ctx, conn)`。宿主负责升级前的认证、Origin、子协议与尚未交付的连接；交付后Handler负责关闭、容量、取消和停服。不可让框架和Handler同时读写同一连接。已有连接被容量/停服拒绝时返回错误并关闭，不再返回握手前的HTTP状态。

接口方法及适配要求见 [WebSocket API](API_REFERENCE.md)；可运行包装见案例的 [internal/hostws](https://github.com/kart-io/goagent-examples/tree/main/orchestration/httpagent/internal/hostws)。`GOAGENT_WS_HOST=1`可运行宿主升级模式。该例以Gorilla演示宿主包装，没有声称已适配全部框架。

## 8. 取消、配置与停服

HTTP/SSE断开会取消派生context；WS可以显式cancel并等待cancelled业务终态。生产者在终态后迟迟不退出，或Factory忽略取消时，默认再等DrainTimeout=5秒就报告execution_unconfirmed：未提交的HTTP响应可返回504，已提交的SSE/WS关闭而不伪造终态。所有者仍保留容量，真实返回后再清理和通知。

结果冻结不等于所有资源已释放。JSON/inline/Run在通知预算内等待尝试；WS的ready等必要cleanup；全局运行槽位等cleanup、通知和相关报告都退出后释放。忽略context的Go函数无法强停，Shutdown可能超时；不应提前关闭共享Host。

停服顺序是BeginShutdown → 等待HTTP Shutdown与Handler.Shutdown → 二者成功后关闭共享Plugin Host/存储/模型资源。完整程序已按此顺序处理。所有配置零值表示默认值，并非禁用；负值会拒绝，详见 [配置表](API_REFERENCE.md)。

## 9. 排查

| 现象 | 检查 |
| --- | --- |
| 501 | /stream绑定是否声明Streaming；中间件是否保留Flush/Unwrap/写期限能力 |
| WS 426/403 | 子协议是否goagent.v1；Origin是否符合标准入口或宿主握手策略 |
| WS busy | 是否在上一次ready之前发起新运行；连接或运行容量是否占满 |
| 504/无终态EOF | 区分已知timeout与未确认退出；按run_id查Report，不自动重提 |
| queued 503/409 | 检查持久接纳完成情况、重复payload冲突和接纳期限；不要直接换event_id重试 |
| 返回200但出站失败 | 业务结果与通知分开，查看Report；可靠重投需outbox |
| Shutdown超时 | Factory/Run/Cleanup/Observer/Delivery/Report是否遵守context，已升级连接是否交付Handler |

多个Agent实例共享业务SessionID时，宿主仍需会话锁/租约。默认按r.ID隔离；本例没有持久聊天会话、自动恢复或跨连接取消。
