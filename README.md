# goagent-contrib

[GoAgent](https://github.com/kart-io/goagent) 的外围扩展。每个子目录是独立的 Go 模块，有自己的 `go.mod`，主模块因此不承担这些扩展的依赖。

| 模块 | 说明 |
| --- | --- |
| [`durable/pgstore`](durable/pgstore) | 基于 Postgres 的 `durable.Store` 与 `durable.Scoped`，支持多个实例共享持久运行 |

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
- **设计**：[多实例 durable.Store 设计](https://github.com/kart-io/goagent/blob/master/docs/superpowers/specs/2026-10-29-multi-instance-durable-store.md)。

### 测试

```bash
./scripts/test-pgstore.sh   # 需要 Docker；启动临时 Postgres 容器，运行全部测试后删除
```

也可以指定已有数据库：`GOAGENT_PGSTORE_DSN=postgres://... go test ./...`（在 `durable/pgstore` 目录下）；未设置时 Postgres 测试被跳过。

### 版本

依赖尚未发布的 goagent（`core/durable/storetest` 与租约用例）。开发期间 `go.mod` 用 `replace` 指向同级目录 `../goagent`，发布前须改为正式版本。
