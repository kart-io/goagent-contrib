# 统一管理服务实施与验证报告

> 本文保留首轮交付的历史结果。用户随后取消旧版兼容要求；双依赖基线与取舍第 8 项已被[当前接口验证记录](current-api-verification.md)取代，复跑以更新后的 README 和验收案例为准。

日期：2026-11-08。结论：首批管理服务已实现并通过本地验收，保留在 `feature/unified-management` 分支。

验证源码提交：`de0bb5fb78b0e6e80b2d29e5e19d29e8032f3d2b`。后续文档提交不改变该实现。机器可读结果见[验证汇总](validation/summary.json)与[退款案例输出](validation/example-result.json)。

## 交付

- Agent 注册、不可变配置、发布/回滚发布、启停与当前版本。
- 固定版本 Session、分页历史、静止正常轮次 Fork 和复制后崩溃恢复。
- 持久 Task/Command、精确 JSON 幂等、会话串行、取消、输入/审批恢复、完成证据补偿。
- 审批待办、可信处理人/理由/时间、并发决定 CAS、审批与命令原子提交。
- Run/Attempt/错误分类、持久预算归属、已知/未知证据，不重复累加恢复用量。
- 24 条 HTTP 路由、严格输入与内部键投影、OpenAPI 3.1。
- 独立 Postgres 管理表、显式迁移、SQL outbox、受信 owner 回收和数据保留入口。
- 可选独立 Harness 子模块、真实 HTTP＋Postgres 的可运行退款宿主。

## 实际检查

| 检查 | 结果 |
| --- | --- |
| management `go test -race -count=1 -timeout 180s ./...` | 43 条测试事件通过，0 失败，0 测试跳过 |
| management `go vet ./...` | 通过 |
| management golangci-lint v2.15.0 | 0 问题 |
| 可选 harness `go test -race`、lint | 通过，0 lint 问题 |
| 当前核心源码 workspace | 管理测试与实际退款案例通过 |
| `make compatibility` | 17/17 关联模块通过，汇总记录 1785 条通过事件 |
| 文档链接与 `git diff --check` | 通过 |

management 的案例包没有 `_test.go`，通过真实 `go run -migrate -verify` 验证；这不是数据库测试 SKIP。跨模块兼容汇总另有 102 项条件跳过，完整名称记录在 JSON：它们涉及各模块的外部服务、模型、特定容器与数据库故障环境，不能宣称这些场景都已验证。

已发布依赖模式使用 `GOWORK=off`；当前核心与同仓库 pgstore 使用临时 workspace，两者均验证。没有提交绝对路径 replace，也没有发布、推送或合并至原仓库主分支。

## 已实际运行的案例

| 案例 | 观察结果 | 证据 |
| --- | --- | --- |
| 真实 HTTP 退款闭环 | 审批前无退款，批准后 1 次；重复提交同 Task；Token 10 | `go run ./examples/host -migrate -verify` |
| 配置发布与 Fork | 旧会话 v1，新会话 v2，分支继承 v1 | 同一退款案例输出与 Fork 组合测试 |
| worker 强制终止 | 等待审批时杀 OS 进程，确认旧进程退出后由新进程续跑；模型总请求 2、退款 1 | `TestApprovalSurvivesProcessKillAndRestart` |
| Fork 复制后崩溃 | 底层复制完成、SQL preparing 尚未 ready 时子进程以 42 退出；重启比对前缀补 ready，日志未重复写入 | `TestForkCrashAfterLogCopy` |
| 拒绝/并发审批 | 拒绝后效果 0；并发批准/拒绝只有一个赢家 | `TestRejectDoesNotExecuteEffect`、`TestConcurrentDecisionsHaveOneWinner` |
| 取消与会话顺序 | 真实退出前保持 canceling；同 Session 实际并发峰值 1 | `TestCancelRequestWaitsForRealExit`、`TestSameSessionNeverOverlaps` |
| 通知重投 | 首次投递失败后使用同一 DeliveryID；Task 仍成功且只有一个 Attempt | `TestNotifyFailureDoesNotRerunTask` |
| 完成只读核对 | 不增加模型请求/效果次数，源会话事件完全不变 | `TestNativeInspectCompletedDoesNotRepeatModel` |

复跑命令和详细流程见[验收案例](acceptance.md)。所有上述案例均不使用外部模型密钥。

## 独立代码审阅

独立审阅范围为基础提交 `634b5d8` 至 `c83f2f5`，发现 0 Critical、7 Important；没有延期 Minor。以下问题全部补失败测试后修复，随后运行完整 race 测试、vet、lint 和退款案例。

| 问题 | 修复与回归 |
| --- | --- |
| 取消被随后返回的等待覆盖 | 生产者退出后服从最新取消标志并释放会话；成功结果仍优先；`TestReviewCancelWaitingRace` |
| unknown 证据触发普通 Run | 显式证据状态检查，未知枚举进入 attention；`TestReviewUnknownEvidence` |
| 图推进后重交旧审批键 | 只回应当前等待实例；旧键已消费时投影新等待；`TestReviewGraphLostConfirmationAtNextGate` |
| 图输入验证失败变 failed | 保留等待与拒绝原因，命令记 validation_rejected；`TestReviewGraphInputValidation` |
| 核对记录未交运行适配器 | 持久引用/摘要、匹配 OperationRefs、再次核对、RuntimeReconciler；缺少协议不接纳；两个 Reconciliation 回归 |
| outbox owner 无法恢复 | 新增受信、带停止确认与 generation CAS 的 RecoverOutboxSlot；`TestOutboxOwnerHasRecoveryProtocol` |
| 恢复取消先于完成核对 | 先用不受业务取消中断的有界只读检查核对完成，再决定取消；`TestCompletedEvidenceWinsRecoveredCancel` |

补充验证发现预算配置也属于模型操作意图。Native 完成重建保留原预算配置，把回放计量写入限制在内存日志副本，避免意图冲突或修改源日志；已用真实 Postgres 回归验证。

## 实施取舍记录

1. 原 contrib 工作区有未提交修改，使用独立工作树和分支开发。成本是后续需集成分支，收益是保留原修改。
2. 技能脚本不能识别计划的中文 T0 标题，按原任务编号维护执行账本；测试、提交和证据仍逐项留存。
3. 固定 Kind 的 Document 存储接口替代重复的每实体 CRUD；保持独立表、租户外键、修订 CAS。代价是新实体须进入固定 Kind 白名单。
4. 管理短事务统一串行，首批只有一个活动执行宿主。代价是写吞吐需实测，不能宣传多 worker 水平扩展。
5. Scope 强制根执行键与 RunID 一致，统一使用 TaskID，替代草案额外派生根哈希；恢复身份与预算匹配。
6. Pipeline 合并同会话/工具/参数的并发审批，管理层按稳定 WaitRef 去重；独立操作的并行实验使用不同参数，未复制另一套工具审批引擎。
7. 一组组合案例覆盖多个原计划命名测试，不将尚不存在的测试名称算作已运行。代价是故障归属要结合案例步骤判断。
8. management 保留可下载兼容依赖基线，当前核心通过临时 workspace 验证；pgstore 与 ACP 同步迁移 v0.50.0 常量。正式安装新模块仍需后续发布，不伪称已发布。
9. 可选 Harness 使用泛型 Sink，不反向依赖尚未发布的 management 模块；宿主需提供 Delivery→Effect 编码器与目标去重。
10. 数据库组合测试隔离 schema，子进程显式共享自己的 schema；避免上个案例残留任务进入下个案例的 Factory。
11. attention 的恢复要求业务专用核对协议，缺少能力时明确 unsupported。成本是业务需实现自己的外部状态核对；收益是不会凭通用理由重做未知效果。
12. core/contrib 的主工作区未被合并或覆盖；原有修改继续保留。验证临时数据库可以清理，报告与案例源码保留在分支中。

## 范围限制

首批是共享 API/数据库、单活动执行宿主。没有自动超时抢占、跨 worker 分片或管理界面；恢复必须先确认旧进程已停止。长期容量、线上模型内容质量、第三方效果去重和数据库主备隔离仍需业务环境验收。CPU、内存和实际金额没有证据来源时不提供虚构的零值。
