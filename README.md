# goagent-contrib

[GoAgent](https://github.com/kart-io/goagent) 的外围扩展。每个子目录是独立的 Go 模块，有自己的 `go.mod`，主模块因此不承担这些扩展的依赖。

| 模块 | 说明 | 主要第三方依赖 |
| --- | --- | --- |
| [`management`](management) | 核心 management 的 PostgreSQL 实现、真实数据库集成测试与退款案例；通用管理和 HTTP Handler 已迁入 GoAgent | pgx、GoAgent |
| [`httpagent`](httpagent) | HTTP JSON/SSE、WebSocket 入口，双向 Webhook 接纳/通知与生命周期观察，配合 Plugin 使用 | net/http、gorilla/websocket |
| [`acp`](acp) | 以 Agent Client Protocol（ACP）Agent 身份经 stdio 运行 Agent，供 Zed 等编辑器驱动：流式回答、工具通知与审批、会话恢复、文件与终端转发 | coder/acp-go-sdk |
| [`durable/pgstore`](durable/pgstore) | 基于 Postgres 的 `durable.Store`、`durable.Scoped` 与会话日志 `session.Store`，支持多个实例共享持久运行与会话 | pgx |
| [`cache/redis`](cache/redis) | Redis 版 `cache.Bytes` 与 `cache.Tagged`，多个实例共享模型调用缓存，一个实例遗忘记忆后其他实例的缓存条目被物理删除；单机、哨兵与 Cluster | go-redis |
| [`memory/pgvector`](memory/pgvector) | pgvector 版 `memory.Index`，多个实例共享长期记忆的向量索引 | pgx、pgvector-go |
| [`memory/qdrant`](memory/qdrant) | Qdrant 版 `memory.Index`，多个实例共享长期记忆的向量索引；每个嵌入维度一个集合 | qdrant go-client |
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

## cache/redis 与 memory/pgvector

多个进程共享同一份长期记忆时，把模型调用缓存与向量索引换成共享后端（部署须知见 GoAgent 的 [memory/README.md](https://github.com/kart-io/goagent/blob/master/memory/README.md) "分布式部署"小节）：

```go
rc, err := redis.New(ctx, redis.Config{Client: goredis.NewClient(&goredis.Options{Addr: addr})}) // 连接主节点
index, err := pgvector.New(ctx, pgvector.Config{Pool: pool})                                     // 可与 pgstore 共用连接池

mc := builder.ModelCache{Cache: rc, TTL: time.Minute, TagKey: tagKey, Erase: builder.EraseRequired} // 所有实例同一个 TagKey（≥32 字节）
mem, err := memory.New(memory.Config{Durable: store, Index: index, Embedder: embedder, OnErase: mc.Eraser()})
agent, err := builder.NewSimpleBuilder(client).WithSessionLog(sessions).WithLongTermMemory(mem).WithModelCache(mc).Build()
```

- **Redis 要求**：Redis 7 及以上；连接主节点（不能读从节点，`New` 会拒绝）；`maxmemory-policy` 建议 `noeviction`，否则 `New` 记录告警，被淘汰的标签集合会让部分条目只能等 TTL 回收。不支持 Ring 客户端。配置 `WaitReplicas` 时，令牌已删除后的重试会临时写入一个键，`noeviction` 且内存已满时返回 OOM 错误，释放内存后重试即可。
- **`Prefix` 不要含 `{`、`}`**：Cluster 把花括号内的部分当作 hash tag，全部键会集中到一个槽；`New` 会记录告警。
- **pgvector 要求**：预先执行 `CREATE EXTENSION vector`（需要较高权限，不在 Schema 中）；`pgvector.Schema` 由宿主纳入迁移流程；连接主库。
- **pgvector 规模**：命名空间内精确扫描、不建 ANN 索引，适用规模与超出时的做法见 `memory/pgvector` 包文档。

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
./scripts/lint-all.sh   # 对全部模块运行 golangci-lint（与 CI 相同，v2.15.0，Go 1.27.3）
```

`test-all.sh`、`lint-all.sh` 和 CI 使用临时工作区，使本仓库模块相互引用当前源码；没有把绝对 `replace` 写入发布模块。只验证单模块的已发布依赖时使用 `GOWORK=off`。本地核心联调可运行 `GOAGENT_CORE_ROOT=../goagent bash scripts/test-all.sh -short`。同仓库依赖修改必须联合发布后，才更新它们的版本锁定。

pgstore 的 Postgres 测试：

```bash
./scripts/test-pgstore.sh   # 需要 Docker；启动临时 Postgres 容器，运行全部测试后删除
```

也可以指定已有数据库：`GOAGENT_PGSTORE_DSN=postgres://... go test ./...`（在 `durable/pgstore` 目录下）；未设置时 Postgres 测试被跳过。

Redis、pgvector 与端到端测试（均需要 Docker，启动临时容器、运行后删除）：

```bash
./scripts/test-redis.sh             # cache/redis，单机 Redis 7.4（端口 56379/56380，可用 REDIS_TEST_PORT、REDIS_TEST_REPLICA_PORT 覆盖）
./scripts/test-redis.sh --cluster   # cache/redis，3 主 3 从 Cluster（host 网络，端口 57001–57006）
./scripts/test-pgvector.sh          # memory/pgvector，pgvector 0.8.0 + Postgres 17（端口 55434，可用 PGVECTOR_TEST_PORT 覆盖）
./scripts/test-memory-qdrant.sh     # memory/qdrant，Qdrant v1.20.2，存储在 tmpfs（gRPC 端口 56354、HTTP 56353，可用 QDRANT_TEST_PORT、QDRANT_TEST_HTTP_PORT 覆盖）
./scripts/test-distributed.sh       # integration/distributedmemory，同时起 Redis 与 pgvector（端口 56389/55444，可用 DISTRIBUTED_REDIS_PORT、DISTRIBUTED_PG_PORT 覆盖）
```

连接变量为 `GOAGENT_REDIS_ADDR`（Cluster 为逗号分隔地址）、`GOAGENT_PGVECTOR_DSN` 与 `GOAGENT_QDRANT_ADDR`（memory/qdrant 的 gRPC 地址 host:port），未设置时对应测试被跳过。`integration/distributedmemory` 是只含测试、不发布的模块：用两个实例共享同一 Redis 与 Postgres，验证一个实例 `Forget` 后其他实例写入的缓存条目被物理删除、召回能用到其他实例写入的向量；它让 `cache/redis` 的使用者不必引入 pgx 与 pgstore。

CI 对每个模块在 Go 1.27.3 下运行 vet、`go test -race` 与 golangci-lint，pgstore 连接服务容器中的 Postgres；数据库重启与同步提交测试需要专用容器，只在本地 `test-pgstore.sh` 中运行。

### 版本

采用新编解码入口的8个模块最低依赖 goagent v0.49.2、Go 1.27.0，开发/CI工具链为Go1.27.3；其余模块维持既有最低依赖。`llm.Client` 使用 `CompleteStream`；完整响应通过 `llm.Complete` / `llm.Chat` 收集。模块 tag 形如 `durable/pgstore/v0.2.0`：

```bash
go get github.com/kart-io/goagent-contrib/durable/pgstore@v0.2.0
```


### 编解码入口迁移

JSON调用统一从`github.com/kart-io/goagent/codec/json`导入，现有Marshal/Unmarshal用法保持。不要恢复已删除的utils/json路径。临时工作区使用Go1.27.0并声明go1.27.3工具链；`GOAGENT_CORE_ROOT`可显式选择核心工作树，不把本机绝对replace写入发布模块。

本次改动同时影响retrieval、document及其子模块等多个独立模块。同仓源码使用临时workspace联合验证；`GOWORK=off`仅表示验证锁定的已发布依赖，不能把本地联调成功等同于发布依赖已包含新源码。发布时先发布迁移后的retrieval，再更新并发布依赖它的document、cohere、qdrant、vertexai，最后发布PDF模块；实际tag和版本锁定须在联合发布时更新，不能指向尚未发布的虚构版本。
