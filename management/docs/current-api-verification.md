# 当前接口清理与验证

> 修复进展：本文为历史记录；当前修复、回归与发布边界以[审计修复报告](remediation-verification.md)为准。

> 完成度复核：既有用例通过不代表原计划全部验收完成；最新审计复现了运行查询写日志，并确认验收和发布缺口。整体状态以[完成度复核](completion-audit.md)为准。


日期：2026-11-08。用户明确取消旧版兼容要求。本次变更位于 `feature/unified-management`，以 `d9ec65d` 为起点；本记录取代原报告中的双依赖基线决策。

## 变更

- management 与 management/harness 的 GoAgent 直接依赖及校验和统一为 v0.50.0，删除 v0.49.3 兼容锁定。
- 原生执行核对、Fork 边界、历史过滤、SQL 查询与图案例直接引用核心 `constants.SessionEvent*`、`constants.WorkflowStart/End`，删除为旧接口保留的协议字符串替代。
- README、运行文档与验收入口统一使用仓库既有 workspace 装配同仓当前源码；原计划和首轮验证结果留作历史记录，并标记旧决策已取消。

Agent 配置版本、Session 绑定版本和 Fork 继承属于需求中的业务能力，继续保留。完成证据核对、只读回放与审批恢复也继续使用当前核心接口。

## 本地验证结果

[机器可读汇总](validation/current-api-summary.json)和[退款案例输出](validation/current-api-example.json)保存本次实际结果。

| 检查 | 结果 |
| --- | --- |
| 当前核心源码＋同仓 pgstore：management `go test -race -count=1 -timeout 180s -json ./...` | 43 条测试事件通过，0 失败，0 测试跳过 |
| 当前核心源码：可选 harness `go test -race -count=1 -json ./...` | 1 条测试通过 |
| GoAgent v0.50.0＋同仓 pgstore：两个模块 `go vet ./...` | 通过 |
| GoAgent v0.50.0：两个模块 golangci-lint v2.15.0 | 均为 0 issues |
| GoAgent v0.50.0＋同仓 pgstore：真实 HTTP 退款宿主 | 退出码 0，退款 1 次、Token 10、幂等提交复用任务 |
| 旧核心直接依赖、事件和端点字符串替代扫描 | 无残留 |

全部持久化测试使用专用 Postgres 17.6 容器；未跳过数据库或进程恢复测试。examples/host 没有 `_test.go`，由真实 `go run` 单独验收，不算数据库测试跳过。

具体业务证据：

- `TestApprovalSurvivesProcessKillAndRestart`：等待审批时终止旧 OS 进程，批准后新进程完成，模型请求 2 次、退款 1 次。
- `TestForkCrashAfterLogCopy`：复制日志后退出，重启恢复 ready，日志无重复。
- `TestNativeInspectCompletedDoesNotRepeatModel`：完成结果核对不再请求模型，不改变源日志。
- 退款宿主验证旧会话与 Fork 为 v1、新会话为 v2，说明依赖兼容清理保留了配置发布与会话绑定功能。

## 复跑与发布

按[验收案例](acceptance.md)从 contrib 根目录执行命令；完整 race 测试可设置 `GOAGENT_CORE_ROOT=/path/to/goagent` 使用本地 v0.50.0 源码，不设置时使用锁定的 v0.50.0 发布版。两者使用同一套当前接口，不验证旧版组合。

迁移后的 pgstore 尚未发布，因此必须使用同仓源码 workspace。`go.mod` 中记录的 pgstore 发布版本不是当前源码的替代构建入口，也不承担旧版兼容。正式发布需先发布 pgstore，再更新 management 版本锁定。本次没有发布或推送，也未引入路径 replace、虚构版本或旧接口回退。
