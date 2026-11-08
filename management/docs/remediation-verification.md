# 统一管理服务审计修复与验收

日期：2026-11-08。修复基于 `9e49c89`，在 `feature/unified-management` 隔离分支完成；保留[修复前审计](completion-audit.md)及其失败证据。本报告将逐项记录修复和本地验证，远端发布与主线合并另行记录。

## 修复内容

| 原问题 | 当前处理 | 主要回归 |
| --- | --- | --- |
| GetRun 查询写入会话日志 | 只读取单个源快照，在内存副本复用核心预算验证；不持源租约、不写源日志 | TestAuditRunQueryIsReadOnly、TestCompletedRunUsageQueriesDoNotWriteAndDoNotDoubleCount |
| 缺失用量变成已知零值 | usage_status 区分 missing/unavailable/partial/available；缺失、损坏和不可读时 usage=null、complete=false | TestRunUsageMissingAndUnavailableStayUnknown、TestRunSummaryUsesSelfAndPreservesUnknownEvidence |
| 运行观测缺项 | 提供真实 TraceID、排队/等待/执行/总时长，以及跨 Task 的 Agent/Session 汇总与嵌套 Agent self 分项 | TestRunDurationsAndTraceAreDerivedFromAttempts、两组 RunSummary 回归 |
| Agent/Session 自身权限关系缺失 | 详情、列表与写入统一资源关系；不可见详情返回 not_found | TestResourceVisibilityAndCursorPolicyAreEnforced、TestTenantIsolationRejectsReadWriteAndEnumeration |
| 已批准但策略失效时没有阻塞原因 | 命令 blocked 保留 permission_denied/agent_disabled/implementation_missing，原决定不丢失；条件恢复后继续，成功清除旧错误 | TestRevokedPermissionAndDisabledAgentKeepDecisionPending、TestMissingImplementationBlocksInsteadOfUsingAnotherFactory |
| 未知工具效果可能被普通恢复重做 | Native Inspect 对未完成轮次中的 pending/被取消工具记录报告 attention；不把它当成 absent 重入 | TestRestartAtEveryCommitBoundary/effect_after_apply：退款 1 次、模型 1 次、Task attention |
| 保留策略缺少保护验证 | 验证结果清理后仍复用有效幂等响应、未确认通知阻止清理、过期键可新接纳；禁止非正保留期和 typed-nil Tracer | 两组 Retention 回归、配置拒绝回归 |
| 关闭和故障矩阵缺口 | 实际终止 OS 子进程，覆盖接纳、创建日志、答复消费、轮次结束、信封、SQL、通知确认；保留非合作 Runtime 所有权 | RestartAtEveryCommitBoundary、OutboxCrashAfterRemoteAcceptance、UncooperativeRuntime、CancelDisableShutdown |
| 脱离 workspace 构建失败 | 开发模块以可移植相对 replace 指向同仓当前 pgstore；GOWORK=off 构建和测试使用当前接口 | 完整仓库独立 build/vet/race |

预算、审批、版本与状态语义仍复用核心接口，不恢复旧版兼容，不修改 GoAgent 核心执行代码。Native 通用适配器不能核对任意第三方效果，未知时保持 attention；业务核对仍由宿主专用 RuntimeReconciler 提供。

## 验证入口

在 contrib 仓库根目录，准备专用 Postgres 并设置 `GOAGENT_PGSTORE_DSN`：

```bash
GOWORK=off go -C management build ./...
GOWORK=off go -C management test -race -count=1 -timeout 240s ./...
GOWORK=off go -C management vet ./...
GOWORK=off go -C management/harness test -race -count=1 ./...
GOWORK=off go -C management/harness vet ./...
GOWORK=off go -C management run ./examples/host -migrate -verify -listen 127.0.0.1:0
```

现有 scripts/with-local-modules.sh 仍可联调当前核心源码；设置 GOAGENT_CORE_ROOT 后，所有模块使用指定的当前核心工作树。数据库与进程故障测试必须实际执行，不能以 SKIP 代替验收。

退款宿主验证：批准前不退款、批准后 1 次、重复提交复用 Task、Token 10；旧会话/Fork v1，新会话 v2。它保留真实 HTTP、worker 和 Postgres，只用脚本模型消除模型网络及密钥依赖。

## 首批需求追踪

“已验证”限下列实际本地场景，不能推断为线上容量或任意指令位置的容灾保证。

| 需求 | 实现入口 | 验收证据 |
| --- | --- | --- |
| AG-01 注册与查询 | agent.go、query.go | HTTP 路由、完整退款、资源可见性与租户隔离 |
| AG-02 配置校验发布 | SaveVersion、PublishVersion | 版本不可覆盖、退款 v1/v2 发布、发布前 Factory.Validate |
| AG-03 启停 | SetAgentEnabled、Worker.start | 停用审批保留/恢复、取消停用停服成功竞争 |
| AG-04 固定版本 | Session.Version、版本工厂 | 旧会话/Fork v1、新会话 v2；实现缺失保持 queued/blocked |
| SE-01 创建查询 | CreateSession、applySession | preparing/ready、create_log 子进程断点只保留一条来源标记 |
| SE-02 历史 | History、pgstore.History | 默认快照隐藏、源分支历史隔离、SQL LIMIT 与可见性下推 |
| SE-03 Fork | ForkSession、applySession | 活动会话拒绝、源分支隔离、复制后退出恢复且不重复日志 |
| SE-04 会话顺序 | active_task_id、Worker.start | SameSessionNeverOverlaps、审批等待保留会话占用 |
| TA-01 幂等持久接纳 | mutate、SubmitTask | 同键复用、数值精度、提交确认丢失后同 Task、admission 进程断点 |
| TA-02 查询筛选 | ListTasks、SQL Query | HTTP 查询、Agent/Session 筛选、状态待办、游标策略失效 |
| TA-03 取消 | CancelTask、worker 控制通道 | queued 不执行、真实退出前 canceling、成功不被取消覆盖 |
| TA-04 恢复 | ResumeTask、worker/Runtime | 图输入拒绝保留等待、审批专用决定、固定原版本与执行键 |
| TA-05 重启 | recover、retryUnconfirmed | 10 类进程断点、原审批杀进程恢复、完成只读重建 |
| TA-06 未知效果 | Native Inspect、RuntimeReconciler | effect_after_apply 不重做退款；不可变核对证据持久传递与协议缺失拒绝 |
| TA-07 关闭资源 | BeginShutdown、Shutdown、Cleanup | 非合作 Runtime 保留 owner、部分构造关闭、清理错误不覆盖结果 |
| AP-01 审批待办 | 待办投影、ListApprovals | 完整退款、按 Task 查询、权限范围与稳定等待身份 |
| AP-02 决定审计 | DecideApproval、CAS、Audit | 并发决定单赢家；Actor/理由/时间与决定在事务中保存 |
| AP-03 续跑 | 决定与 Command 原子提交 | 答复消费前后子进程退出恢复、审批处杀进程恢复 |
| AP-04 拒绝 | Native/Graph 适配 | 原生拒绝不执行退款，图 approve/reject 两分支 |
| AP-05 隔离与当前权限 | Wait.ID/epoch/intent、权限复核 | 并行审批隔离、图下一等待拒绝旧键、撤销权限后 blocked |
| OB-01 运行与错误 | Run、Attempt、Command.ErrorKind | 时长/真实 TraceID、命令阻塞原因、只读详情/列表 |
| OB-02 用量汇总 | readUsage、SummarizeRuns | 分页跨 Session 汇总、重复查询无写入或重复计量、嵌套 self 不加 inclusive |
| OB-03 未知与未归属 | UsageStatus、unknown_runs、Unattributed | 缺失/损坏/不可读证据为未知；嵌套 Agent self 分项；CPU/内存/费用无来源不填零 |
| CO-01 隔离 | Authorizer、SQL 租户外键 | 真实跨租户读写/枚举拒绝、外键阻止跨租户版本引用、可见性下推 |
| CO-02 原子与恢复 | WithinTx、命令与日志核对 | 实体/审计/命令/幂等与决定整组回滚、普通resume拒绝审批、提交确认丢失、创建/Fork/信封/SQL 故障断点 |
| CO-03 分页和上限 | Limits、游标、HTTP decode | 请求体限制、重复/未知 JSON 字段、权限在 LIMIT 前、条件或策略变更使游标失效 |
| CO-04 保留期 | Prune、Idempotency.ExpiresAt | 未确认通知阻止删除、结果删除后有效键仍复用、过期键可重新接纳 |
| CO-05 拒绝伪完成 | Runtime 退出与 finish/outbox | HTTP 断开不取消接纳、取消等真实退出、通知失败不重跑、未知效果 attention |

## 发布与范围边界

相对 replace 是完整仓库开发入口，不是远端依赖发布。正式发布仍须先发布迁移后的 pgstore，移除开发替换并锁定真实发布版本，再验证外部消费者安装；没有推送、发布 tag 或合并原 master。未覆盖线上模型质量、长期容量、数据库主备隔离、多机自动接管；管理前端、权限后台、多 worker 分片仍属于原定后续阶段。

## 独立代码审阅与修复

独立审阅覆盖 `9e49c89..1a85bde`，0 Critical、4 Important、0 Minor。四项均实际补入正式测试、观察失败后修复；不将作者自查冒充二次独立审阅。

| 重要发现 | 修复 | 正式回归 |
| --- | --- | --- |
| 取消掩盖未知工具效果 | Inspect 的未知效果先于取消保留 attention/error_kind；不释放未知效果责任 | TestReviewCancelPreservesAttention、effect_after_apply_cancel 真实进程断点 |
| 崩溃 Attempt 执行时长重叠 | 历史未结束区间不计入已知执行时长，保留 duration_complete=false/unfinished_attempts；汇总单独计不完整运行 | TestReviewRecoveredDuration |
| Validate 卡住 Shutdown | 校验移到生命周期锁和 SQL 事务外；先保留准备槽位与 owner，提交前重检状态；Shutdown 可发取消并按期限返回 | TestReviewValidateHonorsShutdownDeadline |
| 未归属未知消耗伪零 | 没有已知证据时保留 null，混合证据只显示已知部分与 known/unknown/partial 计数、完整性 | TestReviewUnknownUnattributedIsNotZero、TestReviewMixedUnattributedKeepsPartialCounters |

审阅未判断的两项均有明确范围依据：不恢复旧版兼容是用户要求；相对 replace 只作为当前开发构建方案，正式发布仍需联合发布并移除替换。没有延期 Minor。

## 最终本地结果

验证代码提交：`3a029b9a7766fb66d67ab374f1aa740c089b5eed`。机器可读结果见[本次汇总](validation/remediation-summary.json)，独立审阅摘要见[审阅记录](validation/remediation-review.md)，另保留[四项失败证据](validation/remediation-review-red.log)和[转绿输出](validation/remediation-review-green.log)。

| 检查 | 结果 |
| --- | --- |
| 完整仓库 GOWORK=off，真实数据库 race 全套 | 91 个通过测试事件（含子用例），0 失败，0 测试跳过 |
| 当前核心源码 workspace，真实数据库 race 全套 | 同为 91 个通过事件，0 失败，0 测试跳过 |
| 可选 Harness race | 5 个通过事件，0 失败或跳过 |
| 主模块与 Harness build、vet | 通过 |
| 两个模块 golangci-lint v2.15.0 | 均为 0 issues |
| 文档本地链接、git diff --check | 通过 |

examples/host 没有 `_test.go`，由真实 CLI 验收，不算数据库用例跳过。最终退款结果另存[案例输出](validation/remediation-example.json)。

提交顺序：`f9afe52` 查询只读/未知证据 → `c15ed97` 观测汇总 → `cdda32a` 权限与故障验收 → `1a85bde` 当前存储构建绑定 → `3a029b9` 四项独立审阅修复。后续文档归档不改变以上代码。

首批代码修复与本地验收已完成；尚未进行远端依赖发布或主线合并，不将开发构建成功描述成已经可从远端安装管理模块。
