# 统一管理服务业务案例实测

日期：2026-11-08。被测生产代码：`b52983a1cb617b2b7a8709a5171c46b6f87b42be`。本轮未修改生产行为，新增可复跑黑盒脚本及案例证据。

使用 Go 1.27.3、专用 Postgres 17.6、真实 TCP HTTP 和持久 worker；模型为离线脚本，退款效果真实写入专用案例表，不调用真实支付系统。

## 实际结果

内置退款宿主 `-migrate -verify` 退出码为 0：批准后退款记录 1 条，重复提交复用 Task，Token 10，旧会话/Fork v1、新会话 v2。见[内置输出](validation/case-test-builtin.json)。

新[黑盒脚本](../examples/host/test_cases.py)独立启动宿主，通过 API 创建资源，以 SQL 核对效果，8 组场景全部通过：

| 案例 | 实际结果 |
| --- | --- |
| 未认证请求 | HTTP 401 |
| 幂等提交与审批绕过 | 同键同内容复用 Task；同键不同内容 409；普通 resume 伪造批准返回 409/approval_decision_required；审批前退款 0 |
| 批准与重复决定 | 原决定的处理人、理由和时间保留；重复批准复用响应，退款 1；迟到反向决定 409 |
| 拒绝审批 | 明确回复“未执行退款”，数据库退款 0 |
| 等待中取消 | Task canceled；迟到批准 409，退款 0 |
| 停用与重新启用 | 决定已保存，命令 blocked/agent_disabled，期间退款 0；启用后沿用原决定，退款 1 |
| v2 发布与 Fork | 旧会话 v1、新会话 v2、分支 v1；分支能独立执行，源历史保持不变 |
| 只读查询与分页汇总 | 连续三次查询后，源事件数 30→30，全部事件内容摘要未变；limit=1 仍统计 6 个 Task、55 Token |

本次 HTTP 脚本发出 121 个请求（包含等待轮询），宿主正常退出码为 0。按本轮 Agent/Task 精确核对：6 个 Task 中 5 个 succeeded、1 个 canceled；退款记录共 4 条、金额合计 200。拒绝审批的 Task 正常完成了拒绝答复，succeeded 不表示执行了退款。

原始[HTTP 请求状态与场景证据](validation/case-test-http.json)记录资源 ID 和各项结果；请求数受轮询时序影响，复跑无需与 121 严格相等。

## 故障、并发与恢复

另在真实 Postgres 上执行 27 个通过测试事件（含子用例），0 失败、0 测试跳过，覆盖：

- 审批等待时杀掉 OS 进程，批准后由新进程恢复：SQL 计数为模型请求 2 次、退款调用 1 次。
- Fork 日志已复制、元数据未 ready 时退出：重启补 ready，日志没有重复。
- 10 个提交边界：接纳、创建日志、审批答复消费前后、轮次结束、完成信封、SQL 前后，以及工具效果未确认、工具效果未确认并收到取消。
- 已确认完成的断点恢复成功，模型总请求 2、退款调用 1；两种未知工具效果场景都保持 attention，模型 1、退款调用 1，不因恢复或取消重复效果。
- 通知远端已接收、SQL 尚未确认时退出：同一 Delivery ID 被调用 2 次，接收记录只有 1 条，业务 Attempt 仍为 1。
- 并发审批单赢家、取消等待真实退出、同 Session 不重叠、图批准/拒绝、未知用量和 self 汇总、权限撤销/停用恢复、阻塞校验的 Shutdown 截止时间。

见[进程测试原始事件](validation/case-test-process.jsonl)和[机器可读汇总](validation/case-test-summary.json)。这些是本次新执行的结果，不沿用上一轮 91 项全套回归的记录。

## 复跑

从完整 contrib 仓库根目录运行。先准备专用数据库，等待 ready，再执行案例：

```bash
docker run -d --name goagent-management-cases \
  -e POSTGRES_PASSWORD=management-local \
  -p 127.0.0.1:55439:5432 postgres:17.6-alpine

docker exec goagent-management-cases pg_isready -U postgres
export GOAGENT_PGSTORE_DSN='postgres://postgres:management-local@127.0.0.1:55439/postgres?sslmode=disable'
mkdir -p .claude
env -u GOROOT GOTOOLCHAIN=go1.27.3 GOWORK=off \
  go -C management build -o "$PWD/.claude/case-host" ./examples/host

.claude/case-host -migrate -verify -listen 127.0.0.1:0
python3 management/examples/host/test_cases.py \
  --host-binary .claude/case-host \
  --postgres-container goagent-management-cases \
  --output .claude/http-business-cases.json
```

脚本只依赖 Python 标准库、Docker CLI 和上一步构建的宿主。它按本轮 Task ID 核对 SQL，即使同一专用库中有前次案例记录，也不把其他任务算入本轮结果；自行启动和停止宿主。

本次进程案例的完整筛选命令：

```bash
case_pattern='^Test(ApprovalSurvivesProcessKillAndRestart|ForkCrashAfterLogCopy|RestartAtEveryCommitBoundary|OutboxCrashAfterRemoteAcceptanceKeepsStableDelivery|ConcurrentDecisionsHaveOneWinner|CancelRequestWaitsForRealExit|SameSessionNeverOverlaps|GraphDecisionAfterLostAck|RunSummaryUsesSelfAndPreservesUnknownEvidence|RevokedPermissionAndDisabledAgentKeepDecisionPending|ReviewValidateHonorsShutdownDeadline)$'
env -u GOROOT GOTOOLCHAIN=go1.27.3 GOWORK=off \
  go -C management test -race -count=1 -timeout 240s -v \
  -run "$case_pattern" ./integration
```

结束后可删除本轮专用容器。这里验证管理服务和持久恢复语义，不覆盖线上模型质量、真实支付网关或跨机数据库故障接管。
