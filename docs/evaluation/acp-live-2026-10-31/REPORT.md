# ACP 真实进程、真实模型与编辑器客户端验证

日期：2026-10-31。结果：Neovim/CodeCompanion 的 9 项真实链路检查通过；官方 ACP TCK 本次报告 CONFORMANT，57 个测试通过、17 个跳过、0 个失败。另发现并修复同连接 session/load 的协议兼容问题。

这不是 FakeEditor/MockStreamClient 单元测试：使用实际 Neovim 进程、未修改的 CodeCompanion ACP 客户端、独立 Go Agent 子进程、Ollama HTTP 推理、真实磁盘文件与 OS 命令。Neovim 使用 headless 模式驱动其原生客户端 API；审批由测试回调自动选择允许/拒绝，没有声称验证 GUI 按钮或人工操作。

## 环境与固定版本

| 组件 | 实测版本 |
| --- | --- |
| Neovim | 0.12.6 |
| CodeCompanion | v20.0.1，142eadffdaa8dd06060646b6f8ef74e99c42ab06 |
| Plenary | 74b06c6c75e4eeb3108ec01852001636d85a932b |
| ACP TCK | 0.2.0，b7f40fb3c003c45926547be5a1f7e2b374035815 |
| ACP SDK | github.com/coder/acp-go-sdk v0.13.5 |
| GoAgent | 已发布 v0.32.0 |
| ACP 实现 | 本地 contrib 的 fix/acp-contracts-20261031 分支，基线 a21455c 加本轮修复 |
| 模型 | 本机 Ollama 的 llama3.1:8b；实际 HTTP 请求，不使用云密钥 |
| 请求配置 | Builder 与客户端均配置 temperature=0.01、max_tokens=512 |

[版本、模型摘要、二进制摘要和 ACP 源文件哈希](versions.json)标识本次实际验证对象。Agent 经本地记录代理请求 127.0.0.1:11434，日志记录实际响应状态，见 [模型请求日志](nvim-agent-stderr.log)。验证不下载新模型，不更改用户已有编辑器配置。

## Neovim 实测

| 检查 | 结果 |
| --- | --- |
| 握手、能力协商、会话建立 | 客户端启动实际 Go 二进制，取得进程 PID 与 SessionID |
| 真实模型回答 | 收到 LIVE_MODEL_OK |
| 文件读取 | 经真实 fs/read_text_file 取得 DISK_OLD；未保存缓冲区的不同值未写盘 |
| 写入与允许审批 | 经 request_permission 允许，生成 nested/result.txt，内容 WRITE_REAL_482901 |
| 命令执行 | 允许审批后执行 printf，返回 EXEC_REAL_529103 |
| 拒绝审批 | 拒绝 write_file，确认 denied.txt 不存在 |
| 运行中取消 | 观察到 Agent 的真实 sleep 30 子进程 PID 42383；客户端发 cancel，服务端返回 cancelled；kill(pid,0) 确认进程已消失 |
| 同连接重载 | 旧会话安全关闭后重建，回放 21 条历史更新，SessionID 不变 |
| 跨进程恢复与继续对话 | 关闭原会话/进程，新进程连接同一 FileStore，回放 21 条历史；真实模型继续回答 DISK_OLD |

完整 [结构化结果](nvim-report.json)、[客户端输出](nvim-live.log)和 [实际 Agent stdout JSON-RPC](nvim-agent-stdout.jsonl)。取消判断等待服务端响应，不把客户端本地取消回调当成服务端已停止。

### 客户端能力边界

CodeCompanion v20.0.1 本次声明文件读写能力，**没有声明 terminal**。因此本次命令走实际 LocalExecutor；不能把这次结果说成“编辑器 terminal RPC 转发已真实联调”。终端转发的原有 SDK 集成测试仍是另一层证据。

其 read_text_file 实现读取磁盘，本次实测没有读取未保存的缓冲区。GoAgent 正确转发了读取请求，但“转发给编辑器”不保证任意客户端都会返回缓冲区内容；已同步修正文档表述。未验证 Zed/JetBrains 的 GUI 或终端能力。

## 官方 TCK

本次对实际 Agent 二进制运行完整 v1 套件，使用真实模型；每个测试独立启动 Agent 进程。最终 [TCK 日志](tck-run.log)和 [JSON 报告](tck-report.json)显示：57 passed、17 skipped，退出码 0，耗时 56.43 秒。

下表是要求级别统计，与测试数量不同（一个要求可能绑定多个测试）：

| 要求级别 | PASS | FAIL | SKIPPED |
| --- | ---: | ---: | ---: |
| MANDATORY | 31 | 0 | 0 |
| CAPABILITY | 7 | 0 | 16 |
| ADVISORY | 12 | 0 | 1 |
| INFORMATIONAL | 5 | 0 | 0 |

跳过项覆盖本 Agent 未声明/未启用的认证流程、图片音频、resume/delete、额外工作区、配置/模式等，不计算成“已实现”。验证程序按 Factory 约定连接 TCK 的实际 stdio MCP 子进程，并用 Session.OnClose 释放，MCP 建议项也通过；这没有给 ACP 核心添加隐式 MCP 连接。

TCK 是[官方组织下的实验性兼容工具](https://github.com/agentclientprotocol/acp-tck)，其 README 明确不提供完整覆盖，结果不是官方认证。这里只报告固定版本工具对本次二进制的实际结果。

## 实测暴露并修复的产品问题

初次 TCK：4 failed、53 passed、17 skipped，其中两个能力要求失败，verdict NOT CONFORMANT，见 [初次报告](tck-initial-report.json)。失败原因包括原有“同连接加载当前 SessionID 必须先 close”的限制；TCK 的有效恢复流程会直接 session/load。

已经修复 contrib/acp/replay.go：在连接锁内预留 ID，等待旧 Session 的执行/资源实际关闭后再读取最终日志并构造新对象。并发加载、正在关闭、回滚清理中的 ID 仍被拒绝，避免新旧资源重叠。server.session 也拒绝加载期间的新 prompt。

新增 TestLiveClientCanReloadCurrentSession、TestReloadWaitsForOldResourcesBeforeFactory，先观察 [失败](reload-red.log)，修复后 [完整 67 项顶层测试的 race](reload-race.log)通过（13.075 秒），[lint](lint.log)为 0 issues，go vet 通过。原 TestLoadSessionRejectsOpenedID 改为验证安全重载，不再要求违反互操作预期的报错。

初次另一个建议项失败来自验证应用尚未连接客户端提供的 MCP；在应用 Factory 中用现有 mcp/remote 和 OnClose 正确装配后通过，未修改第三方测试。

## 验证过程中的其他发现

- 初始驱动缺少 CodeCompanion 的 `_meta.sent` 消息元数据，客户端过滤后发送了空 prompt，Ollama 返回 400。已修正验证驱动，未将此归咎 ACP；[初始结果](nvim-report-initial.json)保留。
- 未保存缓冲区测试最初按通用假设断言失败，实际客户端读取接口只读盘；[实测记录](nvim-report-buffer-check.json)保留，并据此明确能力边界。
- 初期真实模型出现过拒绝命令/改写工具结果，[记录](nvim-report-model-refusal.json)保留。本轮进一步检查实际请求，发现 v0.32.0 的 GetTemperature 将显式 0 回落为默认 0.7；Builder 默认值也会覆盖仅配置客户端的值。[相应失败记录](nvim-report-default-temperature.json)保留。最终在两层均设置 0.01，抓取的真实请求确认 0.01/512 后完整链路通过。**温度 0 的主库语义问题未在本轮 ACP 修复中修改，应独立修复；不把一次模型通过当作业务答复质量保证。**

## 复跑

前提：Python >=3.14、Go >=1.25、Neovim >=0.12、Git，可访问 GitHub/Go 模块/Python 包源；Ollama 本地服务可用且已有 llama3.1:8b。只使用独立工作目录，不加载用户 init.lua。

在独立目录准备下列固定源码：

```sh
# ROOT 是本次验证专用目录，CONTRIB 指向待验证的 goagent-contrib 工作树。
mkdir -p "$ROOT/agent" "$ROOT/workspace" "$ROOT/state"
git clone --branch v20.0.1 --depth 1 https://github.com/olimorris/codecompanion.nvim.git "$ROOT/codecompanion"
git init "$ROOT/plenary"
git -C "$ROOT/plenary" fetch --depth 1 https://github.com/nvim-lua/plenary.nvim.git 74b06c6c75e4eeb3108ec01852001636d85a932b
git -C "$ROOT/plenary" checkout --detach FETCH_HEAD
git init "$ROOT/acp-tck"
git -C "$ROOT/acp-tck" fetch --depth 1 https://github.com/agentclientprotocol/acp-tck.git b7f40fb3c003c45926547be5a1f7e2b374035815
git -C "$ROOT/acp-tck" checkout --detach FETCH_HEAD
```

复制本目录 [Agent 验证程序](agent.go.txt) 为 `$ROOT/agent/main.go`、[模块声明](agent.go.mod.txt)为 `$ROOT/agent/go.mod`、[Neovim 驱动](nvim-live.lua)为 `$ROOT/nvim-live.lua`。在 agent 目录执行：

```sh
go mod edit -replace="github.com/kart-io/goagent-contrib/acp=$CONTRIB/acp"
env -u GOROOT GOWORK=off go mod tidy
env -u GOROOT GOWORK=off go build -o ../goagent-acp-live .
```

在 ROOT 执行：

```sh
GOAGENT_LIVE_ROOT="$ROOT" NVIM_APPNAME=goagent-acp-validation nvim --headless -u NONE -l nvim-live.lua
python3 -m venv venv
venv/bin/python -m pip install ./acp-tck
venv/bin/acp-tck --agent-cwd "$ROOT/workspace" --timeout 45 --startup-timeout 60 --test-timeout 120 --cancel-prompt '请调用 run_command，argv 为 ["sleep","30"]，执行结束后只回复完成。' --report-json tck-report.json -- "$ROOT/goagent-acp-live"
```

官方客户端与 TCK 源码没有修改；验证 Agent 是临时宿主应用，通过本地 replace 引入待测 ACP；没有改主库依赖或假造客户端响应。模型输出仍可能随硬件/采样变化，驱动失败会保留实际结果而非自动改判通过。

## 后续：主库显式零温已修复（同日）

上述 `0.01` 是历史验证配置。随后已在主库 `fix/explicit-zero-temperature-20261031` 修复配置复制、Builder、JSON 与 Provider 发送的零值语义，使用本地修复主库重新构建真实 Agent，并将两层温度改为 `0`。实际 HTTP 请求包含 `"temperature":0`，Neovim 九项检查通过；TCK 再次 57 passed、17 skipped、0 failed（CONFORMANT）。完整证据见相邻主库的 [零温修复报告](../../../../goagent/docs/evaluation/2026-10-31-zero-temperature/README.md)。此修复尚未发布，不表示已发布 v0.32.0 自动具备该行为。
