# Changelog

本文件记录 `github.com/kart-io/goagent-contrib/acp` 模块的重要变更，tag 形如 `acp/vX.Y.Z`。

格式基于 [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)，版本号遵循 [Semantic Versioning](https://semver.org/spec/v2.0.0.html)。

## [Unreleased]

### Fixed

- 修复最终答案缺失、取消与生产者退出、Factory 部分失败释放、关闭错误追踪、会话重载和持久元数据契约。
- 明确文件条件写入与跨会话协调，补齐终端能力声明、长命令清理及退出状态检查。
- 增加 SDK 协议回归、真实 Neovim/CodeCompanion 与模型验证及官方 ACP TCK 证据。

## [0.1.1] - 2026-10-31

### Fixed

- 收到 `session/cancel` 后本轮一律以 `cancelled` 结束。此前取消与 Runner 正常结束同时发生时返回 `end_turn`，不符合 ACP 对取消的要求。
- 执行中被取消的工具调用标为 failed 时内容为"已取消"，不再显示 context 错误原文（`context canceled`）。

## [0.1.0] - 2026-10-31

### Added

- `acp.NewServer`、`Server.Serve`：把任意 `builder` 构建的 Agent 经 stdio 暴露为 ACP Agent（协议版本 1，基于 coder/acp-go-sdk v0.13.5），每个会话调用一次 Factory 构建 Runner。
- 支持 `initialize`、`session/new`、`session/prompt`、`session/cancel`、`session/close`；配置 `acp.WithSessionStore` 时支持 `session/load`（按会话日志回放）与 `session/list`。
- 工具调用通知：经 `Session.Pipeline()` 注册的工具发出 `tool_call` 与状态更新；`tools.PreAsk` 触发编辑器审批，带 `Grant` 时提供"本会话允许"。
- `Session.FileSystem()`、`Session.Executor()`：客户端声明相应能力时，文件读写与命令执行转发给编辑器（文件需同时声明读写能力），终端嵌入所属工具调用；否则使用本地实现。
- Runner 实现 `io.Closer` 时，在 `session/close`、`session/load` 失败回滚与连接结束时关闭。
- `acp.WithAgentInfo`、`acp.WithToolKind`。

[0.1.1]: https://github.com/kart-io/goagent-contrib/compare/acp/v0.1.0...acp/v0.1.1
[0.1.0]: https://github.com/kart-io/goagent-contrib/releases/tag/acp/v0.1.0
