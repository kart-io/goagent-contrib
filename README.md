# goagent-contrib

[GoAgent](https://github.com/kart-io/goagent) 的外围扩展。每个子目录是独立的 Go 模块，有自己的 `go.mod`，主模块因此不承担这些扩展的依赖。

| 模块 | 说明 | 主要第三方依赖 |
| --- | --- | --- |
| [`httpagent`](httpagent) | HTTP JSON/SSE、WebSocket 入口，双向 Webhook 接纳/通知与生命周期观察，配合 Plugin 使用 | net/http、gorilla/websocket |
| [`acp`](acp) | 以 Agent Client Protocol（ACP）Agent 身份经 stdio 运行 Agent，供 Zed 等编辑器驱动：流式回答、工具通知与审批、会话恢复、文件与终端转发 | coder/acp-go-sdk |
| [`durable/pgstore`](durable/pgstore) | 基于 Postgres 的 `durable.Store`、`durable.Scoped` 与会话日志 `session.Store`，支持多个实例共享持久运行与会话 | pgx |
| [`retrieval`](retrieval) | 检索器（BM25/TF-IDF、混合检索 RRF、多查询、重排、MMR）、内存向量库、RAG、OpenAI/HuggingFace 嵌入；满足 `interfaces.DocumentRetriever`，可传给 `builder.WithRetriever` | go-openai |
| [`retrieval/qdrant`](retrieval/qdrant) | Qdrant 向量库 | qdrant go-client |
| [`retrieval/cohere`](retrieval/cohere) | Cohere 嵌入与重排；空导入即向 `retrieval.NewEmbedder` 注册 | cohere-go |
| [`retrieval/vertexai`](retrieval/vertexai) | Vertex AI 嵌入；空导入即注册 | aiplatform |
| [`document`](document) | 文本、Markdown、JSON、网页加载器；字符、token、代码切分器 | — |
| [`document/pdf`](document/pdf) | PDF 加载器 | ledongthuc/pdf |
| [`tools/practical`](tools/practical) | 文件读写/管理/压缩/监听、API 调用、网页抓取工具（`interfaces.Tool`） | resty、goquery |
| [`tools/database`](tools/database) | 数据库查询工具 | MySQL、PostgreSQL、SQLite（cgo）驱动 |

`retrieval`、`document`、`tools/practical`、`tools/database` 迁自 goagent v0.16.1（goagent v0.17.0 起删除），行为与原包一致；拆分后的差异：Cohere 与 Vertex AI 嵌入器须空导入对应子包才能经 `retrieval.NewEmbedder` 创建，PDF 加载器的包名为 `pdf`。

## HTTP Agent 文档

- [使用指南](httpagent/docs/USER_GUIDE.md)：HTTP/SSE/WS、Web页面案例、双向Webhook、Hooks与框架接入。
- [设计](httpagent/docs/DESIGN.md)、[API参考](httpagent/docs/API_REFERENCE.md)、[文档验证](httpagent/docs/VERIFICATION.md)。

## durable/pgstore

```go
pool, _ := pgxpool.New(ctx, dsn) // 直连主库
store, err := pgstore.New(ctx, pgstore.Config{Pool: pool})
defer store.Close(ctx)
// 与 durable.FileStore 用法相同：Runner、DurableGraph、budget.NewDurable、contextpolicy.NewDurableResultStore 等
```

- **建表**：`pgstore.Schema` 由宿主纳入自己的迁移流程，`New` 只校验 `pgstore.SchemaVersion`。
- **前置条件**：直连主库（不能指向读副本）；高可用部署须同步复制并隔离旧主；`LeaseTTL`（默认 30 秒）须大于主备切换时长。详见包文档。
- **从 FileStore 迁移**：停止所有实例，等进行中与等待审批的运行结束，用 `pgstore.ImportFileStore` 离线导入根命名空间的快照，再切换。两种 Store 不能混跑；作用域数据（上下文证据）无法导入。
- **会话日志**：`pgstore.NewSessionStore(ctx, store)` 返回 `session.Store`，与 `store` 共享连接池与租约，传给 `builder.WithSessionLog`；持久运行用 `session.DurableStore(sessions, id)`，对话与操作快照位于同一会话。须另建 `pgstore.SessionSchema`（依赖 `Schema` 的租约表），`NewSessionStore` 校验 `pgstore.SessionSchemaVersion`。追加持久快照时校验租约，被接管后的旧持有者写入返回 `durable.ErrLeaseLost`，因此快照只能在持锁时追加。
- **设计**：[多实例 durable.Store 设计](https://github.com/kart-io/goagent/blob/master/docs/superpowers/specs/2026-10-29-multi-instance-durable-store.md)。

### 测试

```bash
./scripts/test-all.sh   # 对全部模块运行 go vet 与 go test -race（不连接外部服务）
./scripts/lint-all.sh   # 对全部模块运行 golangci-lint（与 CI 相同，v2.15.0，Go 1.26）
```

pgstore 的 Postgres 测试：

```bash
./scripts/test-pgstore.sh   # 需要 Docker；启动临时 Postgres 容器，运行全部测试后删除
```

也可以指定已有数据库：`GOAGENT_PGSTORE_DSN=postgres://... go test ./...`（在 `durable/pgstore` 目录下）；未设置时 Postgres 测试被跳过。

CI 对每个模块在 Go 1.26 下运行 vet、`go test -race` 与 golangci-lint，pgstore 连接服务容器中的 Postgres；数据库重启与同步提交测试需要专用容器，只在本地 `test-pgstore.sh` 中运行。

### 版本

依赖 goagent v0.26.0 及以上（`core/session/sessiontest` 与 `session.DurableLockName` 自该版本提供）。模块 tag 形如 `durable/pgstore/v0.2.0`：

```bash
go get github.com/kart-io/goagent-contrib/durable/pgstore@v0.2.0
```
