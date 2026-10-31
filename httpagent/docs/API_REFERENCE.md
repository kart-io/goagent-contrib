# HTTP Agent API 与协议参考

基线：提交cfb3e85，GoAgent v0.32.2。安装与完整代码见 [使用指南](USER_GUIDE.md)，实现约束见 [设计](DESIGN.md)。包路径：`github.com/kart-io/goagent-contrib/httpagent`。以下HTTP路径不含宿主挂载前缀；Web页面不在此包中。

## 1. 执行与生命周期接口

```go
func New(Config) (*Handler, error)
func Native(StreamFunc) Binding
func Answer(InvokeFunc) Binding

type EmitFunc func(builder.AgentEvent) error
type RunFunc func(context.Context, any, EmitFunc) (*builder.AgentOutput, error)
type Cleanup func(context.Context) error
type Factory func(context.Context, RunRequest) (Binding, Cleanup, error)
type StreamFunc func(context.Context, any) (<-chan builder.AgentEvent, error)
type InvokeFunc func(context.Context, any) (*builder.AgentOutput, error)

type Binding struct { Run RunFunc; Streaming bool; Mode string }
type RunRequest struct { ID string; Input any; Source string; Webhook string }
```

| Handler方法 | 契约 |
| --- | --- |
| ServeHTTP(http.ResponseWriter, *http.Request) | JSON/SSE/WS与注册Webhook路由 |
| Run(context.Context, RunRequest) (*Output, error) | 宿主worker入口；Source为空用worker；返回独立结果副本 |
| ServeWebSocket(context.Context, WebSocketConn) error | 已升级连接入口；阻塞到断连并接管关闭 |
| BeginShutdown() | 关闭准入、取消运行并关闭受管连接；不等待所有者退出 |
| Shutdown(context.Context) error | 幂等等待运行、连接、清理、通知和受管报告；期限到返回context错误 |

Native返回Mode=unified、Streaming=true，捕获原生终态后等事件流EOF；重复终态或终态后额外业务事件失败。Answer返回Mode=answer_only、Streaming=false。自定义Binding.Run必须遵守context、返回最终Output，并通过Emit发送中间事件；最终done/error由Handler统一冻结和发送，不应自己重复提交终态。Mode为说明字段，能力检查使用Streaming，不能靠字符串或方法集猜测能力。

RunRequest.ID为空自动生成UUID，显式提供时最长128字节；同一ID仍在活动中返回busy。HTTP普通请求不接受客户端run_id；跨进程恢复和已完成结果重放不由此身份自动提供。

## 2. Config全部字段

Factory必填。其余容量/期限的0使用默认值，负值拒绝；0不能用于关闭心跳或禁用上限。

| 字段 | 默认值 | 用途与校验 |
| --- | --- | --- |
| Factory | 必填 | 登记运行后构建Binding及可选Cleanup |
| Timeout | 5分钟 | 每次运行派生context期限；父context可能更早结束 |
| DrainTimeout | 5秒 | 开始等待生产者结束/已取消构造后，报告退出未确认的等待时间 |
| CleanupTimeout | 5秒 | 清理context期限，也用于Report context；不会强停Go函数 |
| WriteTimeout | 10秒 | HTTP/WS写期限，默认/自定义HTTP WS握手options亦使用此值 |
| Heartbeat | 15秒 | SSE注释心跳；Binding能力检查完成后才能首次提交 |
| DeliveryBudget | 5秒 | 每个生命周期事件发布的总context预算，Observer和所有目标共享 |
| AdmissionTimeout | 2秒 | queued Decode/Accept接纳期限；还受运行及请求context约束 |
| MaxInputBytes | 1MiB | HTTP请求体/WS完整输入消息上限 |
| MaxEventBytes | 1MiB | 编码后的事件上限，WS包含外层event封装；最小512字节 |
| MaxActiveRuns | 64 | 包含构造、接纳、执行、延迟清理/通知的所有者 |
| MaxConnections | 128 | 升级中与已接管WS连接数 |
| WSQueueEvents | 64 | 每连接待写消息项上限 |
| WSQueueBytes | 4MiB | 每连接待写消息字节上限 |
| PingInterval | 30秒 | WS Ping间隔 |
| PongTimeout | 60秒 | WS读期限；必须严格大于PingInterval，Pong延长期限 |
| CheckOrigin | nil | 标准WS入口默认无Origin允许；带Origin须单值http/https且Host与请求Host相等（忽略大小写），函数可覆盖 |
| WebSocketUpgrader | 内置Gorilla | 可注入标准HTTP握手适配器；只影响Handler /ws入口 |
| Receivers | nil | map[string]Receiver，接收器名非空且不含 `/` |
| Observer | nil | func(context.Context, RunEvent) error；独立生命周期观察 |
| Delivery | nil | DeliverySink，终态/订阅事件出站投递 |
| Report | nil | func(context.Context, Report)，独立诊断，需遵守context |

容量不提前释放意味着客户端已收到结果时仍可能占据运行槽位；不配合取消的代码可能一直占用。WS队列采取有界发送和写完成确认，并不保证能缓存恰好64个业务事件后才发生慢消费者关闭。

## 3. HTTP与SSE

| 路径 | 方法 | 应答 |
| --- | --- | --- |
| /invoke | POST | 完整JSON结果 |
| /stream | POST | SSE；首次提交前错误可返回JSON |
| /ws | GET | goagent.v1升级 |
| /webhooks/{name} | POST | inline结果、queued收据或Decode返回的JSON Reply |

普通输入只允许一个JSON对象且只有input字段；input必须存在、非null，支持string/object/array/number/bool，数字按json.Number保留精度。未知字段、第二个JSON值或无效JSON返回400。HTTP Content-Type本身不代替JSON解析校验。错误方法405，未知路径404。没有自动提供OPTIONS/CORS路由。

```json
{"input":"查询订单 E100"}
```

普通成功：HTTP200，带X-GoAgent-Run-ID。

```json
{"schema_version":1,"run_id":"run-123","output":{"result":"订单已处理","duration_ms":120,"timestamp":"2026-10-31T15:00:00Z"}}
```

Output字段：result（any）、metadata（可选map）、duration_ms（整数毫秒）、timestamp（时间）、token_usage（可选核心TokenUsage）。不返回AgentState、Go error或内部执行状态。Go投影类型如下；HTTP失败信封不含schema_version。

```go
type Output struct {
    Result any `json:"result"`
    Metadata map[string]any `json:"metadata,omitempty"`
    DurationMS int64 `json:"duration_ms"`
    Timestamp time.Time `json:"timestamp"`
    TokenUsage *interfaces.TokenUsage `json:"token_usage,omitempty"`
}
type Failure struct {
    Code string `json:"code"`
    Message string `json:"message"`
    Cause error `json:"-"`
}
```

Failure实现error及Unwrap；Cause只用于宿主诊断，不进入JSON。失败信封示例：

```json
{"run_id":"run-123","error":{"code":"execution_failed","message":"执行失败"}}
```

未登记运行的请求错误run_id为空。原生事件对外类型为sources、step、text、usage、done、error。SSE每帧由空行结束，data是一个Event；下例的step和最终输出字段按实际事件类型出现：

```text
event: step
data: {"schema_version":1,"run_id":"run-123","seq":1,"type":"step","step":"tool_start","tool":"lookup_order"}

event: done
data: {"schema_version":1,"run_id":"run-123","seq":2,"type":"done","output":{"result":"订单已处理","duration_ms":120,"timestamp":"2026-10-31T15:00:00Z"}}

```

Event共有schema_version、run_id、seq、type；可选sources、step、tool、delta、usage、output、error。seq按本次运行递增，编码失败等路径可以有间隙，不是持久恢复游标。心跳为 `: heartbeat` 注释帧，无seq。客户端必须等完整done/error；不提供Last-Event-ID恢复。

### 状态与错误码

以下为HTTP尚未提交时的映射；SSE已提交后用error事件，WS升级后用协议/业务帧或关闭表达。

| HTTP | code | 情况 |
| --- | --- | --- |
| 400 | invalid_request | 无效输入、过长宿主RunID、Webhook映射失败 |
| 403 | forbidden_origin | 标准WS入口Origin拒绝 |
| 404 | not_found | 未知HTTP路径/接收器 |
| 405 | method_not_allowed | 错误HTTP方法 |
| 409 | conflict | Acceptor显式返回相同键不同payload冲突 |
| 413 | input_too_large | 输入超过上限 |
| 426 | unsupported_protocol | 握手未请求goagent.v1 |
| 429 | busy | 全局运行/连接容量或相同活动RunID占用 |
| 501 | unsupported_capability / unsupported_transport | 无流式能力，或HTTP writer不支持Flush/写期限 |
| 503 | unavailable | 正在停服；接纳普通失败、空RunID或接纳期限到达 |
| 504 | timeout / execution_unconfirmed | 已知context期限，或生产者/构造尚未确认退出 |
| 500 | 其他 | execution_failed、stream_incomplete、invalid_terminal、invalid_binding、invalid_event、output_encoding_failed、output_too_large、host_panic、websocket_upgrade_failed等 |

context.Canceled归类cancelled；若仍尝试HTTP错误应答，通用映射为500，不能假设存在499。断线后通常无法再应答。Decoder错误统一映射invalid_request；Acceptor返回Failure时保留其code并使用通用HTTP映射。注入Upgrader的正常握手失败由该升级器写HTTP状态。

## 4. WebSocket帧

客户端请求 `Sec-WebSocket-Protocol: goagent.v1`，只使用文本JSON帧。

```json
{"type":"run","request_id":"client-1","input":"查询订单"}
```

request_id必填且最长128字节，run不携带服务端run_id。accepted示例：

```json
{"type":"accepted","schema_version":1,"request_id":"client-1","run_id":"run-123"}
```

```json
{"type":"event","run_id":"run-123","event":{"schema_version":1,"run_id":"run-123","seq":1,"type":"text","delta":"正在查询"}}
```

done/error仍在event内，是业务终态。必要cleanup完成后：

```json
{"type":"ready"}
```

取消当前连接的运行：

```json
{"type":"cancel","run_id":"run-123"}
```

cancel不保证抢在已冻结终态之前成功。不存在的当前运行返回not_found，已冻结返回already_finished；忙时新run返回busy，均为协议error帧，不是另一个运行的业务终态：

```json
{"type":"error","request_id":"client-2","error":{"code":"busy","message":"连接仍有活动运行，请等待ready"}}
```

每连接一次一个运行；received done/error仍须等ready后重用。不支持跨连接cancel、自动重连重执行业务或断线回放。队列满、写期限、无Pong、二进制/超限输入、停服或未确认退出会关闭连接并取消关联执行。

## 5. WebSocket适配接口

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
type WebSocketUpgradeOptions struct {
    Subprotocol string
    HandshakeTimeout time.Duration
}
type WebSocketUpgrader interface {
    Upgrade(http.ResponseWriter, *http.Request, WebSocketUpgradeOptions) (WebSocketConn, error)
}
type WebSocketUpgraderFunc func(http.ResponseWriter, *http.Request, WebSocketUpgradeOptions) (WebSocketConn, error)
```

不暴露Gorilla类型。允许单reader/单writer并行；ReadText须处理Ping/Pong/Close并拒绝二进制，Pong回调延长读期限。Ping不等待Pong。Close并发安全、可重复且解除阻塞读写；读写期限和读限制须真实生效。自定义Upgrader成功必须返回非空连接且协商指定协议，失败写HTTP错误。nil连接却无error为无效实现，标准入口报告并尝试500。

ServeWebSocket不重新检查HTTP握手策略；宿主认证、Origin与子协议必须在交付前完成，连接ctx需持续到服务调用结束。Handler只跟踪已交付的框架连接，未交付的握手由宿主管理。

## 6. Webhook与通知类型

```go
type Incoming struct {
    Source, EventID, Tenant, Receiver, PayloadHash string
    Input any
    Reply *WebhookReply
}
type Receipt struct { RunID string `json:"run_id"`; Duplicate bool `json:"duplicate"` }
type WebhookReply struct { Status int; Body any }
type Acceptor interface { Accept(context.Context, Incoming) (Receipt, error) }
type AcceptorFunc func(context.Context, Incoming) (Receipt, error)
type Receiver struct {
    Mode string
    Decode func(context.Context, *http.Request, []byte) (Incoming, error)
    Acceptor Acceptor
    AcceptStatus int
}
```

Mode只能inline或queued，Decode必填；queued还必须配置Acceptor并返回非空Source/EventID及非nil的Input。框架覆盖Receiver为注册名、PayloadHash为原始body SHA-256；Tenant由宿主可信身份取得。Reply优先返回JSON，不调用Factory；它不是原样文本/XML challenge响应接口。

queued成功直接返回Receipt，不套output或schema_version，默认202，AcceptStatus可选200/202：

```json
{"run_id":"durable-run-123","duplicate":false}
```

成功只表示持久接纳，不表示Agent已执行；重复事件也要由Acceptor决定稳定RunID/Duplicate。接纳未结束时返回503可能表示结果未确认，而不表示一定没入库。重复业务须沿用稳定事件身份重查/重试接纳。

```go
type RunEvent struct {
    SchemaVersion int `json:"schema_version"`
    EventID string `json:"event_id"`
    RunID string `json:"run_id"`
    EventType string `json:"event_type"`
    OccurredAt time.Time `json:"occurred_at"`
    Data any `json:"data"`
}
type DeliverySink interface { Deliver(context.Context, RunEvent) error }
type Target struct { URL string; Headers http.Header; Events []string }
type SenderConfig struct { Targets []Target; Client *http.Client; Timeout time.Duration }
func NewSender(SenderConfig) (*Sender, error)
func (*Sender) Deliver(context.Context, RunEvent) error
type Report struct { RunID, Stage string; Err error }
```

Handler发布run.started和冻结后的run.completed/failed/cancelled；接纳阶段、拒绝和Reply不冒充一次Agent运行。started的data含source/webhook，completed的data是Output，failed/cancelled的data为{error:{code,message}}。Observer与Delivery各获JSON独立副本。

Sender默认选择Handler的非started事件，Target.Events可显式筛选。目标URL必须HTTP/HTTPS且不含用户信息；目标串行、同次事件共用event_id及context总预算，Header带X-GoAgent-Event-ID。Timeout默认5秒，负值拒绝，每目标尝试还受上层DeliveryBudget约束；不跟随重定向，仅2xx成功。零Targets不会自动建立队列。直接调用Sender.Deliver时，总context预算由调用方负责。
