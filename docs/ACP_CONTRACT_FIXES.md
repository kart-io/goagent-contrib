# ACP 契约修复执行记录

计划：goagent/docs/superpowers/plans/2026-10-31-acp-agent.md；基线 contrib a21455c；分支 fix/acp-contracts-20261031。

基线：go test -race -count=1 ./... 通过（2.719 秒）。

裁定：远端已有完整 ACP 与部分修复，按现有实现补齐，不重建模块。现有 io.Closer 约定保留并报告错误，新增 OnClose 支持 Factory 部分失败释放；不要对同一资源同时注册两种清理。现有同连接 load 已打开 ID 时拒绝的行为保留，它能保证不重叠执行，规格将同步明确需要先 close。真实编辑器联调不在本机伪造结果；使用真实 SDK/io.Pipe 完成自动集成验证。

执行项：最终答案；文件写契约与协调；终端回收错误；生命周期；持久元数据；案例适配与验证。


阶段结果：
- 最终答案与不支持写入 RED→GREEN：/tmp/goagent-acp-red-answer-fs.log、green-answer-fs.log。
- OnClose/Factory 部分失败 RED→GREEN：red-resources.log、green-resources.log。
- 跨 Session 条件写 RED→GREEN：red-fs-concurrency.log、green-fs-concurrency.log。
- 元数据/列表/分页 RED→GREEN：red-metadata.log，完整回归的旧测试改为按实际 LastSeq 追加，保留中止轮次语义。
- 终端释放错误/缺失退出状态 RED→GREEN；新增实际 5.1 秒长命令正常/取消测试。
- done 后生产者仍存活 RED→GREEN：red-producer.log、green-producer.log。完整 ACP race 通过 13.212 秒，go vet 通过。

裁定：终端 kill/wait/output/release 各阶段分别从开始时计时 5 秒，避免早期清理失败耗尽 release 机会；代价是总回收时间可能超过 5 秒，关闭接口超时仍保留资源责任。lint v2.6.2 不支持 Go 1.27 导出格式，采用子进程 GOTOOLCHAIN=go1.25.0 与仓库 go.mod 对齐，不改全局工具链。


## 最终交付

实际修改位于本仓 acp 模块，GoAgent 核心 API 和两个项目的 go.mod/go.sum 未修改。新增/扩展测试覆盖最终答案、非 UTF-8/权限拒绝、跨会话条件写、Factory 失败回滚、关闭错误、等待执行权取消、生产者退出、长命令清理、退出状态、会话列表持久化/分页和追加冲突。

- ACP：65 个顶层测试（含多个子测试），最终 Go 1.25 的完整 race 通过，13.622 秒；Go 1.27 上完整 race 亦通过。
- 静态检查：go vet 通过；golangci-lint v2.6.2 配合 GOTOOLCHAIN=go1.25.0，0 issues。
- contrib：现有 scripts/test-all.sh 对 10 个模块的 vet/race 全部返回 0；其他模块在末轮 ACP 专项复审前已完成，本轮仅 ACP 改动，之后专项再完整验证。
- examples：独立工作树 /Users/costalong/.codex/worktrees/goagent-examples-acp-contracts，分支 fix/acp-contracts-20261031；3 项协议集成测试通过（1.620 秒），包含本地执行、参数错误交还模型、编辑器终端转发；go vet 通过。
- examples 使用 /tmp/goagent-acp-integration.work 引用本地修复模块；新 WithTerminalCapabilities 尚未发布，发布后再更新 examples 依赖并单独验证 GOWORK=off。没有提交机器路径到 go.mod。

[原始验证日志](evaluation/acp-contract-fixes/final-go125-race.log)、[lint](evaluation/acp-contract-fixes/final-lint.log)、[10 模块检查](evaluation/acp-contract-fixes/contrib-all-modules.log)、[examples 集成](evaluation/acp-contract-fixes/examples-integration.log)；同目录 red 日志保留修复前的真实失败，重跑当前代码会通过。

独立复审发现 1 项 P2：回滚超时后 ID 过早复用；另发现终态通知失败未登记到关闭错误的验收遗漏。两项均新增测试先观察失败再修复，最终完整 race 与 lint 通过；未进行重复复审替代测试。没有遗留评审中的 P0/P1/P2 或刻意延期的小问题。

## 裁定与适用边界

1. 基于远端已有 ACP 补齐差异，不从空模块重建；旧 SDK 行为由适配层隔离，未修改第三方 SDK。
2. 保留既有 io.Closer 和同连接先 close 再 load 的约定；新增 OnClose 支持部分构造失败回滚。宿主不能对同一资源重复注册两种关闭方式，否则会重复释放。
3. 每个终端回收阶段单独 5 秒期限，确保前阶段失败仍尝试 release；代价是总回收可能超过 5 秒。关闭超时保持资源责任与 ID 占用，实际退出后继续释放。
4. lint 使用仓库要求的 Go 1.25 子进程，避免旧分析器读取 Go 1.27 导出格式失败；没有更改全局工具链，同时保留 Go 1.27 测试证据。
5. 已验证真实 SDK/io.Pipe 协议链路，没有声称完成真实 Zed/JetBrains 联调，也没有额外配置外部数据库/检索服务作为生产验证。

工作保留在修复分支供审阅，未提交、合并、推送或打 tag。examples 原主工作区的 README/docs 改动保持不变；主仓只同步规格和验证说明。


## 后续真实验证与约定修正

已完成 Neovim/CodeCompanion + Ollama 的 9 项实测与官方 TCK（57 passed、17 skipped、CONFORMANT）。TCK 证实此前“保留先 close 后 load”的裁定不符合客户端互操作流程；已改为在同连接自动安全关闭旧 Session 再重载，清理中仍保留 ID 占用。新增失败测试后修复，当前 67 项顶层测试 race 通过（13.075 秒），lint0issues、vet通过。

[完整实测报告](evaluation/acp-live-2026-10-31/REPORT.md)保留初次失败、最后通过、真实命令 PID 停止证据、客户端能力限制及温度 0 回落的问题。新约定取代上文历史裁定中的先 close 要求；旧验证数字保留历史含义。仍未提交、推送或发布。
