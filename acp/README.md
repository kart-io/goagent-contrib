# acp — 以 ACP Agent 身份运行 goagent

[Agent Client Protocol](https://agentclientprotocol.com)（ACP）是编辑器与编码 Agent 之间的 stdio JSON-RPC 协议。本模块把任意 `builder` 构建的 Agent 暴露为 ACP Agent，供 Zed、JetBrains、Neovim 等编辑器驱动。协议实现基于 [coder/acp-go-sdk](https://github.com/coder/acp-go-sdk) v0.13.5（协议版本 1）。

```go
srv := acp.NewServer(func(ctx context.Context, s *acp.Session) (acp.Runner, error) {
    p := s.Pipeline()
    p.Pre = append(p.Pre, askBeforeWrite) // 返回 tools.PreAsk 的钩子触发编辑器审批
    reg := tools.NewRegistry()
    _ = reg.Register(readFileTool(s.FileSystem()))
    _ = reg.Register(runCommandTool(s.Executor()))
    d, err := tools.NewDispatcher(reg, tools.DispatchConfig{Pipeline: p})
    if err != nil {
        return nil, err
    }
    agent, err := builder.NewSimpleBuilder(model).
        WithSessionID(s.ID()). // 必须：工具通知按会话 ID 路由
        WithSessionLog(store). // 与 WithSessionStore 同一个 Store
        WithTools(d.Tools()...).
        Build()
    if err != nil {
        return nil, err // 不要直接 return Build()：err 非空时会得到非 nil 接口里的 nil 指针
    }
    return agent, nil
}, acp.WithSessionStore(store), acp.WithTerminalCapabilities(env.Capabilities{Network:true}), acp.WithAgentInfo("my-agent", "1.0.0"))
log.SetOutput(os.Stderr) // stdout 被协议占用
if err := srv.Serve(ctx, os.Stdin, os.Stdout); err != nil {
    log.Fatal(err)
}
```

- stdout 只能承载协议消息。SDK 的连接诊断经 `slog.Default()` 输出（默认落到 `log` 包的 stderr），宿主不要把 `slog` 默认 handler 或 `log` 输出指向 stdout。
- `Serve` 返回前取消并回收会话（包括构造中的会话）；正常断开且回收成功返回 nil，ctx 结束保留 ctx.Err()，释放失败或超时合并返回。SDK 的读循环在输入关闭后才退出，调用方负责关闭 in；不响应取消的宿主代码不能被 Go 强制终止。

## 装配约定

- 每个 ACP 会话调用一次 Factory，构建时必须 `WithSessionID(s.ID())`，否则工具调用报错。
- Factory 收到的 ctx 只覆盖构建过程：SDK 在 `session/new`、`session/load` 返回时取消它，连接、子进程等长期资源不能绑定到它上面。
- Factory 获得 MCP、插件等资源后立即用 `s.OnClose(func(ctx context.Context) error)` 注册释放，按逆序各执行一次；Factory 部分失败也会回滚。关闭开始后不接受新回调，注册失败须自行释放。Runner 的既有 io.Closer 仍保留并先执行，不要对同一资源再注册 OnClose；关闭错误会报告。生产者尚未退出时保留资源所有权，关闭超时返回错误并拒绝新任务，实际退出后继续回收。
- 工具须经 `s.Pipeline()` 所在的 `tools.Dispatcher` 注册，才有工具通知与审批；本模块无法检测遗漏。
- 审批：宿主向 `s.Pipeline().Pre` 追加返回 `tools.PreAsk` 的钩子；不追加则直接执行。"本会话允许"选项仅在 PreAsk 决定带 `Grant` 时出现。不要替换已装好的第 0 项与 `Approver`；宿主追加的 Post 替换的结果不反映在编辑器通知中。
- `acp.WithSessionStore` 开启 `session/load` 与 `session/list`，Factory 须用同一 Store 调用 `WithSessionLog`（会话日志只支持原生工具流）。
- `s.MCPServers()` 是编辑器要求连接的 MCP 服务器，可交给 goagent 的 `mcp/remote`。

## 工具调用通知

- 调用 ID：goagent 只在持久运行中设置 `tools.Call.CallID`，形如 `turn/<轮次>/call/<模型给的调用 ID>`；模型在不同 prompt 间复用调用 ID 时，编辑器会把同 ID 的卡片合并。为空时本模块按会话分配 `call-<n>` 并写回 `Call`。`session/load` 的回放使用会话日志中记录的 ID，与实时显示的 ID 不同。
- 替代：非持久运行中，宿主 PreHook 返回 `PreSubstitute` 会在编辑器里显示为两个调用，原调用在本轮结束时标为失败"调用未执行（可能已被替代或中止）"。
- 顺序：prompt 进行中本会话的全部 `session/update` 由同一个 goroutine 按事件顺序发出，工具调用前的文本总在 `tool_call` 之前到达。
- 失败：工具返回错误会中止本轮（`session/prompt` 返回 JSON-RPC internal error），该调用标为失败；审批被拒绝（Rejected）则作为结果交还模型。

## 方法支持

| ACP | 支持 |
|---|---|
| `initialize`、`session/new`、`session/prompt` | 是 |
| `session/cancel` | 是；收到后本轮一律以 `cancelled` 结束，执行中被取消的工具调用标为 failed，内容为"已取消" |
| `session/close` | 是；取消该会话进行中的 prompt，之后该 ID 的请求返回 invalid-params |
| `session/load`、`session/list` | 配置 `WithSessionStore` 时 |
| `authenticate` | 不声明认证方式，返回空响应 |
| `logout`、`session/set_mode`、`session/set_config_option`、`session/resume` | method-not-found |
| prompt 内容 | `text`、`resource_link`、文本 `resource`；图片与音频返回 invalid-params |

- `session/load` 支持直接重载本连接已有会话：先取消并等待旧对象及资源关闭，再重建与回放。正在加载、关闭或回滚清理中的 ID 被拒绝，避免新旧实例重叠。
- `session/list` 从同一 Store 的私有 acp/session_metadata 事件还原 cwd 和更新时间，重连与重开 FileStore 后仍可用；首轮 prompt 前也可列出。每页 100，按时间降序、ID 升序，严格按 cwd 过滤；使用 nextCursor 继续。旧日志无元数据时不列出，显式 load 成功后补建；损坏元数据报错。元数据不进入模型历史。

## 文件与命令

- `s.FileSystem()`：客户端须同时声明 `fs.readTextFile` 与 `fs.writeTextFile`，`ReadFile`/`WriteFile` 才经编辑器（缓冲区可见性由客户端实现决定，写入经客户端处理）；只声明其一则全部在本地。存在性、`Stat`、`Walk`、`Remove` 与路径限定始终使用以工作区为根的 `env.LocalFS`。同一 Server 的转发写与删除参与可取消的串行协调，保护版本检查到 RPC 写入；编辑器手动改动、终端、其他 Server 和绕过本接口的 LocalFS 写入不参与，不承诺外部 CAS。只存在于编辑器、未落盘的新缓冲区读不到。
- 写入会在磁盘上补建缺失的父目录；对不存在的文件使用 `IfVersion` 返回 `env.ErrStale`；目标是目录时报错；`Perm` 非零和非 UTF-8 数据在副作用前返回 errors.ErrUnsupported，不静默忽略权限或损坏二进制。
- 工作区内的符号链接按解析后的目标路径转发给编辑器，编辑器看到的是目标文件而不是链接路径。
- `s.Executor()`：客户端声明 terminal 时必须用 WithTerminalCapabilities 显式配置实际环境；模块不猜测执行位置或网络。命令运行中嵌入工具调用；未知终态或取消时 kill 并 wait 确认退出、取得输出，再 release。各回收阶段开始时分别新建独立的 5 秒 context，前一阶段超时不妨碍后续尝试；错误合并返回并保留给 close/Serve。缺少退出码和信号时报错，信号退出码为 -1；未声明 terminal 则 LocalExecutor。创建响应丢失时报告远端状态未知，不自动重试。

## 编辑器配置（Zed）

```json
{
  "agent_servers": {
    "my-agent": {
      "type": "custom",
      "command": "/path/to/my-agent",
      "args": [],
      "env": {}
    }
  }
}
```

## 测试

```bash
go test -race ./...
```

测试用 SDK 的客户端连接模拟编辑器，不访问网络。

## 最终答复与生命周期

普通 text 增量原样发出，done.Output 是最终答案。没有增量的缓存/PreStep 答案会补发；增量是最终答案前缀时仅补后缀，已包含最终答案时不重复。最终结果与草稿不同则以“最终答复：”明确标识。取消优先于 done；归还执行权前等待事件生产者关闭，不能仅收到 done 就释放资源。

Factory、Runner、io.Closer 和 OnClose 回调必须遵守其 context/关闭约定；忽略取消时无法保证及时释放，但不会把超时冒充释放成功。文件与终端的转发默认依赖双方共享工作区磁盘视图，ACP 本身不提供沙盒。

## 真实客户端验证

已用 Neovim 0.12.6、CodeCompanion v20.0.1 和本机 Ollama 完成真实进程联调，并通过固定版本官方 ACP TCK。CodeCompanion 本次 read_text_file 读取磁盘，未提供未保存缓冲区内容；它未声明 terminal，因此命令走实际 LocalExecutor，不能据此宣称 terminal RPC 已在真实编辑器验证。详细日志、限制与复跑步骤见 [实测报告](../docs/evaluation/acp-live-2026-10-31/REPORT.md)。
