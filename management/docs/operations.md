# 统一管理服务运行与恢复

## 部署和依赖

首批支持多个 API 共用管理数据库、一个活动执行 worker；没有心跳超时自动抢占、多 worker 分片或管理前端。管理写事务使用短 advisory 事务锁统一处理跨实体约束；Agent 执行和模型/工具 I/O 均在事务之外。宿主 `Factory.Validate/Check` 必须是纯配置与能力校验，不在管理事务中访问网络或重入 Service。

`management/pgstore.Migrate` 只初始化/校验独立管理表，`New` 只核对 schema 版本。会话表与 durable 表由既有 contrib pgstore schema 初始化。连接池直连可写主库；pgstore 租约、同步提交和旧主隔离要求仍然有效，管理模块不削弱这些约束。

当前源码兼容调整把 contrib pgstore 升到 GoAgent v0.50.0 的 Session 事件常量。management 的发布依赖仍使用可下载的旧兼容组合；仓库 workspace 会选择当前 pgstore 与核心源码。正式发布前先发布修改后的 pgstore，再更新 management 依赖锁定并重复两套验证。

## 身份、权限与版本

Authorizer 同时实现逐资源 `Authorize` 和可下推查询的 `Scope`。Visibility 的 All/ResourceIDs/AgentIDs/SessionIDs 与可信 TenantID 取交集；PolicyVersion 进入分页游标。读权限必须与 Scope 一致，不能先取一页再做任意过滤。`TenantAuthorizer` 只用于宿主已自行完成权限校验的场景和本地案例。

所有请求主体由宿主生成，恢复前再次核对发起人的当前权限。Agent 停用阻止新会话、新任务和开始/恢复，已有运行不自动取消。配置版本不可覆盖，Session 绑定的实现、工具和策略必须仍可重建；发布新版本不能替换在途任务的实现。

凭据在 Factory 中按 SecretRef 解析，不放进 Config、审批摘要、输入、审计或 effects payload。Runtime.Inspect 不请求模型、工具或 Reconcile；它只核对已保存的完成/等待证据。Native 已完成轮次的重建使用只读回放，缺失证据则报告 attention。

## 状态和一致性

- 202 表示管理事务已接纳；Command applied、Runtime 真正开始和 Task succeeded 是不同事实。
- `canceling` 表示取消请求已记录但生产者可能还在退出。只有实际退出后才冻结取消结果；成功先确定时仍保留成功。
- 等待释放执行槽位但保留 Session 的逻辑占用，后续 Task 不能越过它修改历史。
- SQL 事务不覆盖任意 session/durable.Store。创建/Fork/完成结果通过稳定键、来源摘要和完成信封补偿，提交确认丢失不能直接重做业务。
- 每个等待点保留自己的稳定 ID/epoch。消费 A 不改变并行 B 的身份与决定；普通 resume 不能向审批点提交任意载荷。
- 外部效果不承诺恰好一次。不可确认的写入进入 attention；外部系统须按稳定操作键去重或提供可靠 Reconcile。

## 关闭顺序

1. 停止 API 接纳或让新请求返回 503。
2. Worker.BeginShutdown 关闭新执行准入、取消本轮构造/运行。
3. 并行等待 HTTP server.Shutdown 和 Worker.Shutdown；共享宿主总期限。
4. Runtime/producer 与 Cleanup、通知投递均退出后，再关闭 Plugin Host、模型、SessionStore、durable.Store 和数据库池。

不响应 context 的用户 Runtime/Cleanup 仍由原 worker 持有。Shutdown 到期返回诊断，不意味着资源已经安全释放。宿主停服只打断执行尝试，不是业务取消；下次仍使用原 Task、输入、版本、执行键和预算。

## worker 异常退出

`mg_workers` 的 owner 不因心跳失联自动过期，第二个执行者不能直接抢占。正常关闭会释放 owner；崩溃后先通过宿主进程管理确认旧执行者已经退出。

宿主提供 `Config.ConfirmWorkerStopped(ctx,ownerID,generation)`，然后由受信运维身份调用 `RecoverWorkerSlot(ctx,actor,expectedOwner,expectedGeneration,reason)`。未提供确认器时返回 unsupported；owner/generation 已改变时返回 conflict。此接口不向普通管理 HTTP 开放。确认器必须来自进程管理或隔离证据，不能只检查“心跳旧了”。

恢复 worker 会找回 applying 命令与未结束 Task，先检查完成信封/核心日志。已经成功只补索引；审批决定已落盘就按原等待实例应用；外部效果仍不确定则保留 attention。不能删除 owner 行、换 TaskID 或改版本来绕过恢复校验。

OutboxDispatcher 使用独立 outbox slot。它异常退出后的回收入口为 `RecoverOutboxSlot`，同样要求停止确认、owner/generation CAS 和审计；不能调用 default worker 的回收方法冒充已恢复通知投递。

## attention 与通知

attention 的恢复需要版本能力显式声明 Reconcile，可信 `ReconciliationReader` 提供绑定租户、Task、版本和具体 OperationRefs 的不可变记录，并由 Runtime 实现 `RuntimeReconciler`。Command 保存引用与证据摘要，worker 在应用前重新读取、匹配并交给适配器；缺少任一协议时拒绝接纳或保持 attention。内置 Native/Graph 适配器不凭通用记录擅自核对外部业务，需业务宿主提供专用实现。不能把一句“确认没执行”当作证明，也不绕过核心 OperationPolicy 的重试限制。

需要通知时注入 DeliverySink 并启动 OutboxDispatcher；未配置投递器时，业务执行不等待远端通知，outbox 保留待处理。投递至少一次，接收端按 Delivery.ID 去重。通知失败不改变业务终态。可选 Harness Sink 只把 Delivery 转入 effects 队列，SQL outbox 和 effects 入队不是跨存储原子事务；接收系统依旧需要幂等或核对。

## 查询和保留

历史在数据库按 seq 和 LIMIT 读取，默认隐藏 boundary/snapshot 与管理内部事件。HTTP 只移除管理结构中的内部键，不改写业务 input/result/config 的同名字段。运行用量读取持久预算的 self/inclusive/unknown 证据，不累计每次恢复返回的账本副本；CPU、内存和费用尚无来源时保持未知。

默认幂等有效期 30 天，Task/结果保留 90 天，审计 180 天。宿主显式调用 `Service.Prune` 回收过期记录；有未确认命令、通知或未结束任务时延后清理。Session 日志、版本快照和外部业务表不由此方法自动删除，需按实际引用和业务保留策略处理。保留期到期后客户端幂等键可以重新接纳，不是永久去重承诺。

首批没有吞吐、自动故障切换时间或线上模型质量承诺。上线前按实际会话长度、租户数、模型延迟和数据库池配置做容量验证。
