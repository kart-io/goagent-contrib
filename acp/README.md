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
}, acp.WithSessionStore(store), acp.WithAgentInfo("my-agent", "1.0.0"))
log.SetOutput(os.Stderr) // stdout 被协议占用
if err := srv.Serve(ctx, os.Stdin, os.Stdout); err != nil {
    log.Fatal(err)
}
```

## 装配约定

- 每个 ACP 会话调用一次 Factory，构建时必须 `WithSessionID(s.ID())`，否则工具调用报错。
- 工具须经 `s.Pipeline()` 所在的 `tools.Dispatcher` 注册，才有工具通知与审批；本模块无法检测遗漏。
- 审批：宿主向 `s.Pipeline().Pre` 追加返回 `tools.PreAsk` 的钩子；不追加则直接执行。"本会话允许"选项仅在 PreAsk 决定带 `Grant` 时出现。不要替换已装好的第 0 项与 `Approver`；宿主追加的 Post 替换的结果不反映在编辑器通知中。
- `WithSessionStore` 开启 `session/load` 与 `session/list`，Factory 须用同一 Store 调用 `WithSessionLog`（会话日志只支持原生工具流）。
- `s.MCPServers()` 是编辑器要求连接的 MCP 服务器，可交给 goagent 的 `mcp/remote`。

## 工具调用通知

- 调用 ID：goagent 只在持久运行中设置 `tools.Call.CallID`；为空时本模块按会话分配 `call-<n>` 并写回 `Call`。`session/load` 的回放使用会话日志中记录的 ID，与实时显示的 ID 不同。
- 替代：非持久运行中，宿主 PreHook 返回 `PreSubstitute` 会在编辑器里显示为两个调用，原调用标为失败"调用未执行（可能已被替代或中止）"。
- 失败：工具返回错误会中止本轮（`session/prompt` 返回 JSON-RPC internal error），该调用标为失败；审批被拒绝（Rejected）则作为结果交还模型。

## 方法支持

| ACP | 支持 |
|---|---|
| `initialize`、`session/new`、`session/prompt`、`session/cancel`、`session/close` | 是 |
| `session/load`、`session/list` | 配置 `WithSessionStore` 时 |
| `authenticate` | 不声明认证方式，返回空响应 |
| `logout`、`session/set_mode`、`session/set_config_option`、`session/resume` | method-not-found |
| prompt 内容 | `text`、`resource_link`、文本 `resource`；图片与音频返回 invalid-params |

- `session/load` 拒绝本连接上已打开的会话 ID，需先 `session/close`。
- `session/list`：只有本连接新建或加载过的会话才知道 cwd；未知时 cwd 为空，且不会被 cwd 过滤排除。

## 文件与命令

- `s.FileSystem()`：客户端须同时声明 `fs.readTextFile` 与 `fs.writeTextFile`，`ReadFile`/`WriteFile` 才经编辑器（可读到未保存的缓冲区，写入出现在编辑器中）；只声明其一则全部在本地。存在性、`Stat`、`Walk`、`Remove` 与路径限定始终使用以工作区为根的 `env.LocalFS`。编辑器不参与写入协调，`CreateIfAbsent`、`IfVersion` 只是尽力而为；只存在于编辑器、未落盘的新缓冲区读不到。
- 写入会在磁盘上补建缺失的父目录；对不存在的文件使用 `IfVersion` 返回 `env.ErrStale`；目标是目录时报错；`Perm` 不转发。
- `s.Executor()`：客户端声明 terminal 能力时命令在编辑器终端中执行，工具调用内执行的终端嵌入该调用；ctx 结束时 kill 并 release，清理使用清理时新建的、不随原 ctx 取消的有界 ctx。被信号终止的命令 `ExitCode` 为 -1。否则使用 `env.LocalExecutor`。

## 编辑器配置（Zed）

```json
{
  "agent_servers": {
    "my-agent": { "command": "/path/to/my-agent", "args": [], "env": {} }
  }
}
```

## 测试

```bash
go test -race ./...
```

测试用 SDK 的客户端连接模拟编辑器，不访问网络。
