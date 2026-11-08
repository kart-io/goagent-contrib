# 统一管理服务完成度复核

> 修复进展：本文为历史记录；当前修复、回归与发布边界以[审计修复报告](remediation-verification.md)为准。

日期：2026-11-08。被审计源码：`9e49c89179b8ec05a48ce7fbeab4253ddf538861`（准确提交号以[汇总](validation/completion-audit-summary.json)为准）。

结论：当前接口清理和主要业务闭环已经交付，但不能宣称原计划全部完成。此前归档中将 T0–T10 全部标记 complete 的结论过于宽泛；本次复核确认存在实际缺陷、验收缺口和发布依赖阻塞。本轮仅分析与重现，没有修改业务实现。

## 本次重新执行

| 检查 | 结果 |
| --- | --- |
| 当前核心源码与同仓 pgstore，真实 Postgres，management race 全套 | 43 条通过事件，0 失败，0 测试跳过 |
| 当前接口 workspace，harness race | 通过 |
| GoAgent v0.50.0 与同仓 pgstore，真实 HTTP 退款例子 | 通过；退款 1 次、Token 10、重复提交同一 Task、旧会话/Fork v1、新会话 v2 |
| management 脱离 workspace 构建 | 失败：已锁定的 pgstore 引用不存在的 `session.TypeBoundarySnapshot` |
| 新增只读查询审计用例 | 失败：查询使会话事件从 1 条变成 2 条 |

源码全套命令：在 contrib 根目录设置专用 `GOAGENT_PGSTORE_DSN`，执行：

```bash
GOAGENT_CORE_ROOT=/path/to/goagent bash scripts/with-local-modules.sh \
  go -C management test -race -count=1 -timeout 180s -json ./...
bash scripts/with-local-modules.sh go -C management/harness test -race -count=1 ./...
bash scripts/with-local-modules.sh go -C management run ./examples/host -migrate -verify -listen 127.0.0.1:0
```

本次未重复 lint/vet；其历史通过结果仍记录在[当前接口验证记录](current-api-verification.md)。本报告不把历史检查写成本次重新执行。

## 已复现的缺陷：运行查询写入日志

位置：[management/query.go](../query.go) 的 `GetRun` 调用 `budget.NewDurable`。核心构造器用写事务初始化/保存账本，Session durable store 将它追加为快照事件。因此读取运行详情会修改源日志。

重现步骤：创建 ready 会话 → 停止 worker → 提交 queued 任务 → 读取源日志 → GetRun → 再读取源日志。观察结果：

```text
任务状态=queued，查询前事件=1，查询后事件=2，complete=true，usage存在=true
FAIL: TestAuditRunQueryIsReadOnly
```

多出的事件为 `boundary/snapshot`。`complete` 在这里是用量证据字段，不代表 Task 业务已经完成；但查询创建账本本身违反只读投影要求。原有测试没有覆盖这一场景。

复现源码：[审计用例](validation/completion-audit-run-query_test.go.txt)；完整失败输出：[日志](validation/completion-audit-run-query.log)。该用例保存为文本，未混入现有通过的回归集。要复跑，先复制为 `management/integration/zz_completion_audit_test.go`，再运行：

```bash
GOAGENT_CORE_ROOT=/path/to/goagent bash scripts/with-local-modules.sh \
  go -C management test -race -count=1 -timeout 60s -v \
  -run '^TestAuditRunQueryIsReadOnly$' ./integration
```

当前预期退出码为 1；运行后删除临时测试文件。后续修复应让它转为通过，并覆盖用量证据缺失时不生成账本、不误报已知零值。

## 对照计划的完成状态

| 范围 | 已有实现或证据 | 未闭环项 |
| --- | --- | --- |
| T0、T5、T6：执行、任务和审批 | 持久接纳、取消、审批双分支、并发决定、等待审批杀进程恢复、只读完成重建 | 答复消费前后、完成信封后/索引前等全部提交边界的独立进程断点验收未完整覆盖 |
| T1、T2：权限、管理事务和保留 | SQL 租户约束、事务回滚、LIMIT 前权限过滤、Prune 实现 | 真实跨租户枚举/写入隔离、权限撤销、保留期、提交确认丢失和连接释放等验收证据不完整 |
| T3、T4：Agent 与 Session | 不可变版本、发布、启停、旧会话绑定、Fork 隔离/复制后崩溃恢复 | 停用与恢复竞争、旧实现不可获取、创建日志断点等未逐项验收 |
| T7：运行观测 | 按条件查询 Run、单 Run 预算 self/inclusive 证据 | 查询写日志缺陷；尚无跨 Task 的 Agent/Session 总量接口；未知证据与聚合去重测试不足 |
| T8：HTTP | 路由与 OpenAPI 一致性、严格 JSON、请求体积、完整退款流程 | 不可见资源、游标策略变更和部分错误状态的端到端验证不足 |
| T9：可选 Harness | effects 入队、子任务记录投影、通知重投同 ID | unknown effect 核对、子工作投影和保留期组合验证不足 |
| T10：宿主交付 | 可运行 HTTP 案例、审批/Fork 的真实进程恢复、运维文档 | 非合作 Runtime 关闭、完整故障矩阵、逐需求验收回填仍需补齐 |
| 取消旧版兼容 | 两个模块锁定 v0.50.0，使用当前常量，无旧版回退 | 发布依赖尚未闭环，不能把 workspace 成功当作独立安装成功 |

原计划列出 78 个不同命名测试，现有源码有 27 个同名测试；其余部分确实被组合案例覆盖，不能把“未同名”直接解释为功能未实现。上述缺口按实际测试内容判断。例如 [TestCrossTenantReferencesRejected](../types_test.go) 只验证 Actor 字段非空，不能作为跨租户读写隔离的证据；完整崩溃矩阵也不能由审批处和 Fork 处两个杀进程场景替代。

运行视图目前返回 Task、Attempts、Usage、Complete；[设计第 8 节](planning/2026-11-08-unified-management-design.md)列出的时长、TraceID 和跨 Agent 汇总仍未完整落实。此处指跨 Task 的管理统计，核心 AttributionReport 在单 Run 内已有按 Agent 的归属分项。

## 集成与后续范围

实现仍在独立工作树 `goagent-contrib-management` 的 `feature/unified-management` 分支；原 contrib 主工作区仍在 master，未合并。发布需要先发布迁移后的 pgstore，再更新 management 的真实依赖版本；不应恢复旧版兼容或填入虚构版本绕过发布依赖。

管理界面、权限配置后台、多 worker 分片和自动故障接管已明确列为后续阶段，不计作首批漏做。线上模型质量、长期容量和数据库主备切换也尚未验收。

建议完成顺序：先修复只读查询缺陷并补回归，再补高风险验收矩阵和需求追踪，最后完成 pgstore 依赖发布及主线集成。满足这些条件后，才可将首批实现标记为全部完成。
