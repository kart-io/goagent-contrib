# 统一管理服务验证案例

当前管理服务、adapters、httpapi 已迁入 GoAgent；使用核心 `management` 和 `errors.ErrManagement*` 入口。跨仓验证须按 [扩展说明](../README.md) 显式指定包含新包的 GOAGENT_CORE_ROOT。

以下命令均从 contrib 仓库根目录运行，使用当前同仓源码 workspace。使用专用本地 Postgres 17，设置 `GOAGENT_PGSTORE_DSN`。完整案例命令见[模块 README](../README.md)。测试不依赖模型网络或密钥；Agent 循环、工具、审批、日志、预算、HTTP 与数据库都使用真实组件。

## 案例一：可运行退款客服

```bash
bash scripts/with-local-modules.sh go -C management run ./examples/host -migrate -verify -listen 127.0.0.1:0
```

该命令启动实际 TCP HTTP 服务、持久 worker 和脚本模型，通过 API 完成注册、发布、会话、幂等提交、审批、退款、用量查询、版本切换与 Fork。成功输出包含：

```json
{
  "验证": "通过",
  "退款次数": 1,
  "Token": 10,
  "旧会话版本": "v1",
  "新会话版本": "v2",
  "分支版本": "v1",
  "重复提交复用任务": true
}
```

随机 Agent/Task ID 和结果文本也会输出。程序内部检查上述值，失败退出码非零；数据库退款表按工具 execution_key 唯一，不靠控制台输出证明效果。

## 案例二：审批后杀进程并恢复

```bash
bash scripts/with-local-modules.sh go -C management test -race -count=1 -v \
  -run TestApprovalSurvivesProcessKillAndRestart ./integration
```

测试父进程提交任务，子进程执行 Agent 并停在审批处。父进程强制终止并回收子进程，确认旧进程已退出后回收 owner；在没有 worker 时批准，再启动新的独立子进程恢复。SQL 计数必须是模型请求总数 2、退款调用 1：第一次模型调用从日志恢复，没有多发，业务只执行一次。新进程最后通过信号正常退出并释放 owner。

这个案例实际终止 OS 进程，不是只在内存中重建一个对象。它验证审批等待处的恢复；任意指令位置、数据库主备切换和多机自动接管仍需额外故障测试。

## 案例三：Fork 复制后进程退出

```bash
bash scripts/with-local-modules.sh go -C management test -race -count=1 -v -run TestForkCrashAfterLogCopy ./integration
```

子进程在底层 Fork 已提交、管理会话仍 preparing 时以退出码42终止。确认旧进程退出后，新进程比对复制前缀并补成 ready，目标日志前后完全一致，不覆盖源会话或再次追加复制内容。

## 案例四：拒绝、竞争、取消与会话顺序

```bash
bash scripts/with-local-modules.sh go -C management test -race -count=1 -v \
  -run 'Test(RejectDoesNotExecuteEffect|ConcurrentDecisionsHaveOneWinner|CancelRequestWaitsForRealExit|SameSessionNeverOverlaps|SecondWorkerRejected)' ./integration
```

- 拒绝审批后 Agent 可继续给出答复，退款效果为 0。
- 两人并发批准/拒绝，只有一个决定生效。
- 不响应取消的运行保持 canceling，直到测试释放生产者才变 canceled。
- 同会话两个任务的实际执行并发峰值为 1。
- 已有 owner 时第二个 worker 明确被拒绝。

## 案例五：图审批、Fork 与协议

```bash
bash scripts/with-local-modules.sh go -C management test -race -count=1 -v \
  -run 'Test(GraphDecisionAfterLostAck|ForkSourceAndTargetIsolated|AllManagementRoutes|HostRegisterPublishSessionTaskApprovalResume)' ./integration
bash scripts/with-local-modules.sh go -C management test -race -count=1 ./pgstore
env GOWORK=off go -C "$GOAGENT_CORE_ROOT" test -race -count=1 ./management/httpapi
```

验证图的批准/拒绝分支、重复决定不重复效果、活动会话不能 Fork、分支历史隔离、HTTP 管理路由和持久计量。协议测试另外检查重复 JSON 键、未知字段、体积限制、内部键隐藏，以及业务字段不被错误脱敏。Postgres 单元测试验证事务回滚与权限过滤先于 LIMIT。

## 实施与验收的限制

多 API 共享接纳/查询、单执行宿主是当前边界。并行审批的原生传播有单独实验；尚未把跨机隔离、真实线上模型质量、长时间压力或真实数据库主备切换视为已通过。没有 DSN 的 SKIP 不等于数据库验收。

需求追踪在原始实施计划中保留；实现将重复的命名测试合并为上述更完整的组合案例。验证报告记录实际执行的命令、测试数和独立审阅问题，不能把计划中的测试名称自动算成已运行。

## 审计修复回归

```bash
bash scripts/with-local-modules.sh go -C management test -race -count=1 -timeout 240s -v \
  -run 'Test(RestartAtEveryCommitBoundary|OutboxCrashAfterRemoteAcceptance|RevokedPermissionAndDisabledAgent|TenantIsolation|ResourceVisibility|Retention|UncooperativeRuntime|CancelDisableShutdown|AuditRunQuery|RunSummary|CompletedRunUsage)' ./integration
```

进程故障矩阵覆盖接纳、创建日志、审批答复固定前后、轮次结束、完成信封和 SQL 终态前后。另在工具外部效果已经写入但尚未保存结果时退出：恢复必须 attention，模型请求 1 次、效果 1 次；已确认完成的其他断点则成功且模型 2 次、效果 1 次。通知接收后确认丢失时重投相同 ID，业务 Attempt 不增加。

这些用例只终止测试自身子进程；不引入自动 owner 抢占。完整需求映射与本次结果见[修复报告](remediation-verification.md)。

## 黑盒业务脚本

[本轮案例实测](case-test-results.md)提供真实 TCP HTTP 的 8 组业务场景，以及 SQL 副作用和日志内容核对。可运行脚本为 [test_cases.py](../examples/host/test_cases.py)，不依赖第三方 Python 包；复跑命令与本次原始结果都在该报告中。
