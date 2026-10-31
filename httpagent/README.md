# httpagent

将 GoAgent 暴露为 HTTP JSON、SSE、WebSocket 服务，并提供双向 Webhook 和独立生命周期观察者。独立模块；依赖 GoAgent v0.32.2，主库不依赖本模块。

## 文档入口

- [使用指南](docs/USER_GUIDE.md)：离线运行、完整宿主程序、原生工具、Webhook/worker、框架接入、取消与排查。
- [设计文档](docs/DESIGN.md)：模块边界、执行绑定、所有者状态、时序图、接纳/通知与停服责任。
- [API与协议参考](docs/API_REFERENCE.md)：全部Config字段、默认值、公开接口、JSON/SSE/WS帧及错误映射。
- [文档验证](docs/VERIFICATION.md)：代码块编译、链接检查和源码核对范围。

安装已推送版本：`go get github.com/kart-io/goagent-contrib/httpagent@v0.0.0-20261031151618-cfb3e85df18a`。尚未打模块tag；该版本无需本地workspace。

## 装配

```go
host, err := plugin.NewHost(businessPlugin)
if err != nil { return err }
if err := host.Start(ctx); err != nil { return err }
h, err := httpagent.New(httpagent.Config{
    Factory: func(ctx context.Context, r httpagent.RunRequest) (httpagent.Binding, httpagent.Cleanup, error) {
        if !llm.HasToolStreaming(client) {
            return httpagent.Binding{}, nil, &httpagent.Failure{Code: "unsupported_capability", Message: "需要原生工具流"}
        }
        b := builder.NewSimpleBuilder(client).WithSessionID(r.ID)
        if err := plugin.Install(ctx, b, host, plugin.Dispatch(tools.DispatchConfig{})); err != nil { return httpagent.Binding{}, nil, err }
        a, err := b.BuildContext(ctx)
        if err != nil { return httpagent.Binding{}, nil, err }
        return httpagent.Native(a.ExecuteStream), nil, nil
    },
})
```

宿主检查所有错误，再把 h 挂到自己的 http.Server。Factory 返回的 cleanup 只释放请求级资源，不能关闭共享 Plugin Host。部分构建失败也可以返回 cleanup；生产者/Factory 实际退出前不会调用它。

`Native` 在 HTTP 同步和流式路径都使用同一工具执行循环，等待真正 EOF 后归还执行权。普通同步模型可显式使用 `Answer(agent.Execute)`；它不承诺原生工具循环，且 `/stream` 在执行前返回 501。不要根据 Agent 有没有 ExecuteStream 方法判断模型能力，也不要在流式失败后自动重执行。

## 入口与版本

| 入口 | 输入与输出 |
| --- | --- |
| POST /invoke | `{"input":"问题"}` → `{schema_version:1,run_id,output}` |
| POST /stream | 相同输入 → SSE，含 text/step/usage/sources 及唯一 done/error |
| GET /ws | 子协议 `goagent.v1`，JSON run/cancel，返回 accepted/event/ready/error |
| POST /webhooks/{name} | 宿主显式注册的 inline 或 queued 接收器 |

普通 HTTP 输入必须存在且非 null，支持 JSON 对象/数组/数值；数字保留精度。Output 包含 result、metadata、duration_ms、timestamp、token_usage，不序列化 AgentState。错误为 `{error:{code,message},run_id}`；input 400、接纳冲突409、超限413、容量429、关闭503、无流式501、已知期限504。未知无终态EOF为 stream_incomplete。

```sh
curl -H 'Content-Type: application/json' -d '{"input":"查询订单"}' http://127.0.0.1:8097/api/invoke
curl -N -H 'Content-Type: application/json' -d '{"input":"查询订单"}' http://127.0.0.1:8097/api/stream
```

### WebSocket

```json
{"type":"run","request_id":"client-1","input":"查询订单"}
{"type":"cancel","run_id":"accepted返回的运行ID"}
```

accepted 是进程内登记，不表示持久接纳。一次连接同时一个运行，收到 ready 后再提交；忙时返回 busy。升级前纳管，断线/心跳/写入超时取消当前运行。消息类型 error 是协议错误；event 中的 done/error 才是运行终态。没有自动重连重执行业务、跨连接取消或断线回放。seq 只是当前事件序号。

### 接入其他 Web 框架或 WebSocket 库

协议与生命周期仅依赖 `WebSocketConn`，不暴露 Gorilla 类型。Gorilla 保留为默认实现，因此已有 `New(Config{Factory: ...})` 无需修改。

```go
type WebSocketConn interface {
    ReadText() ([]byte, error)
    WriteText([]byte) error
    Ping(deadline time.Time) error
    SetReadLimit(int64)
    SetReadDeadline(time.Time) error
    SetWriteDeadline(time.Time) error
    SetPongHandler(func() error)
    Close() error
}

type WebSocketUpgrader interface {
    Upgrade(http.ResponseWriter, *http.Request, WebSocketUpgradeOptions) (WebSocketConn, error)
}
```

两种装配方式：

1. 宿主保留 net/http：将适配器传入 `Config.WebSocketUpgrader`，也可使用 `WebSocketUpgraderFunc` 包装函数。`WebSocketUpgradeOptions` 包含 `Subprotocol` 和 `HandshakeTimeout`。Handler 在调用升级器前统一检查 goagent.v1、Origin、停服和连接容量；默认无 Origin 允许，有 Origin 要求单值HTTP/HTTPS且Host与请求Host匹配，可通过 `CheckOrigin` 调整。自定义升级器必须协商指定子协议、遵守握手期限，失败时写HTTP错误并释放其资源，成功返回非空连接。
2. 框架自行升级：完成鉴权、Origin检查和 goagent.v1 协商后，调用 `h.ServeWebSocket(ctx, conn)`。此入口不依赖框架的HTTP类型；适配器将框架连接转换为 `WebSocketConn` 即可。调用阻塞至连接结束，Handler 接管连接关闭，并复用运行准入、容量、取消、心跳、清理及停服逻辑。连接/停服容量不足时返回错误并关闭已经升级的连接；升级前的握手与准入应答由宿主管理。已有连接不能交给框架和 Handler 同时读写。

适配器须真实实现消息大小限制和期限，不能使用无操作方法占位。ReadText 消费控制帧、响应 Ping、触发 Pong 回调，并拒绝二进制消息；WriteText 发送一个完整文本消息。允许单reader与单writer并行，Close必须并发安全、幂等并解除阻塞读写，Ping不能同步等待Pong。框架只有context式读写接口时，需把期限和关闭正确映射为操作context的取消。

`ServeWebSocket` 使用宿主传入的连接生命周期context，不能使用处理器返回后即失效的临时context。异步框架必须保留该context直到服务调用结束；停服时先 BeginShutdown，再等待 Handler.Shutdown，框架负责仍未交给 Handler 的握手。Handler 不会为宿主已经完成的升级重新执行Origin或鉴权检查。

宿主升级的完整可运行案例位于 goagent-examples/orchestration/httpagent/internal/hostws，使用 `GOAGENT_WS_HOST=1` 启动案例服务。该案例选择 Gorilla 做宿主帧实现，但只通过公开接口调用 Handler，可按上述契约替换为其他库。未声称已验证所有第三方框架。

### Hooks 与通知

使用标准 HTTP 中间件注入 context。Agent/Plugin Hook 和 Dispatcher Pipeline 保持原有业务裁决与持久回放语义；它们与 Config.Observer 是不同接口。Observer 只观察 run.started/completed/failed/cancelled；它的错误和 panic 独立报告，不改变业务结果。Callback.OnStart 在 Builder 构建期触发，不能用来代替请求生命周期。

```go
sender, err := httpagent.NewSender(httpagent.SenderConfig{
    Targets: []httpagent.Target{{URL: "http://127.0.0.1:9000/events"}},
})
// 检查 err 后将 sender 传给 Config.Delivery；Config.Report 接收独立错误。
```

默认投递终态，信封含 schema_version、event_id、run_id、event_type、occurred_at、data。目标可显式订阅 started；所有目标复用同一终态 event_id。只进行一次应用层尝试，不跟随重定向；只有2xx算本次成功。失败不重新执行 Agent。默认 best-effort 不承诺崩溃后重投或 exactly-once；可靠通知可注入实现 DeliverySink 的宿主 outbox。

### 入站 Webhook 与 worker

每个 Receiver 必须明确 `Mode: "inline"` 或 `Mode: "queued"`，并提供 Decode(ctx, request, rawBody)。Decode 返回 Incoming{Source,EventID,Tenant,Input}，也可返回 Reply 处理供应商 challenge；框架计算原始 body 的 PayloadHash 和 Receiver 名称。

inline 直接运行并返回完整结果，适合自有系统同步调用，断线会取消。它不承诺满足第三方平台快速确认时限。

queued 必须提供 Acceptor。Accept 必须将事件、去重范围及稳定 RunID 持久化后返回 Receipt；重复 ID/相同 payload 返回同一 RunID，重复 ID/不同 payload 应拒绝。去重范围至少含租户、source、接收器和事件 ID。未配置 Acceptor 时构造失败；接纳成功才返回202（接收器可配置200）；失败/超时返回503，绝不使用无持久性的 goroutine 冒充接纳。

宿主 worker 读取持久任务后调用：

```go
out, err := h.Run(workerCtx, httpagent.RunRequest{
    ID: receipt.RunID, Input: persistedInput, Source: "worker", Webhook: "orders",
})
```

workerCtx 必须来自宿主 worker 生命周期，不能沿用已结束的 Webhook 请求 ctx。该入口复用同一 Factory、Hooks、Observer 和出站通知。队列交付/重试、业务会话锁、工具副作用恢复仍由宿主与现有 durable 执行层承担。模块不自造队列后端；磁盘接纳契约在 webhook_test.go 中实测。

## 取消、期限与停服

收到 done 后仍等待生产者真正关闭。退出等待超时只向客户端报告 execution_unconfirmed（SSE/WS 无伪终态并关闭流），继续保留全局槽位和清理责任；实际退出后才冻结终态。HTTP断开后的结果可能无法确认，客户端不能自动重提业务。

先调用 `h.BeginShutdown()` 关闭准入并取消所有者/升级连接，再等待 `server.Shutdown(ctx)` 和 `h.Shutdown(ctx)`，两者完成后关闭 Plugin Host。不要先等 HTTP 自行结束再取消。DeadlineExceeded 只代表已知期限；隐藏的 Runner 内部期限可能表现为 stream_incomplete，示例使用共同外层期限。

默认：执行5分钟，生产者退出/cleanup各5秒，写10秒，接纳2秒，单次通知总预算5秒，SSE心跳15秒，WS ping/pong为30/60秒；输入/事件1MiB（含传输事件封装，最小配置512字节），WS队列64项/4MiB，运行64，连接128。可通过 Config 调整；负值和不一致配置在 New 拒绝。自定义 Hook、Factory、Run、cleanup 均须配合 context，框架不能强制停止任意 Go 函数。

SSE 中间件应保留 ResponseController 的 Unwrap/Flush/Deadline 能力；跨域 CORS/OPTIONS、Origin策略和代理长连接配置由宿主装配。共享 SessionID 的多个 Agent 实例需宿主会话锁/租约，单实例 executionGate 不等于跨实例互斥。

## 验证

```sh
go test -race -count=1 ./...
go vet ./...
```

包含真实 HTTP/WS、真实 Builder/Plugin 工具、持久接纳记录重建、取消、迟到清理、心跳、通知错误与大小上限测试。模型响应由本地脚本服务控制，不是线上模型质量测试。Web 页面示例位于 goagent-examples/orchestration/httpagent。

### 审阅后的生命周期约束

终态通知在结果冻结时生成不可变快照，与 cleanup 独立推进；worker 获取的 Output 是独立副本。通知、cleanup 及超时报告回调都属于同一运行所有者，Shutdown 不会在其仍活动时返回成功。JSON 在总投递预算内等待尝试完成；SSE/WS 终态可先发送，WS ready 等待必要 cleanup，活动运行槽位等所有工作结束后释放。

SSE 在能力检查完成后，即使尚无首个业务事件也发送注释心跳。WS 在冻结终态前检查完整封装大小，不能因封装超限丢弃终态后继续发送 ready。页面须拒绝没有 done/error 的 ready；极小、无法容纳错误终态的容量配置在 New 拒绝。
