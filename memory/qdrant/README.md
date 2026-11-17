# memory/qdrant

基于 Qdrant 的 `memory.Index` 实现，供多个实例共享长期记忆的向量索引。

```go
client, err := qdrant.NewClient(&qdrant.Config{Host: "127.0.0.1", Port: 6334})
index, err := memqdrant.New(ctx, memqdrant.Config{Client: client}) // Prefix 默认 goagent_memory
```

## 前置条件

- Qdrant 的 gRPC 端口（默认 6334）可达；`Client` 由宿主创建并负责关闭。
- 同一 `Prefix` 下的集合只由本包写入；不同部署或环境用不同 `Prefix` 隔离。
- 无需预先建集合：首次写入某个维度时创建，并发创建时对方已建好视为成功。

## 映射规则

- **集合**：每个维度一个集合 `<Prefix>_d<维度>`，距离为 Cosine。换嵌入模型后新旧维度的向量分属不同集合，`Search` 只查与查询同维度的集合。
- **点 ID**：`uuid.NewSHA1(uuid.NewSHA1(固定命名空间, nsKey), id)`，其中 `nsKey` 为命名空间各组件以 NUL 连接。分两层派生是因为键可以含 NUL，直接拼接会让不同的（命名空间, 键）撞 ID。payload 存 `ns`（即 `nsKey`，建 keyword 索引）与 `id`。
- **Upsert**：校验命名空间、归一化后写入本维度集合，再从本前缀的其他维度集合删除同一点，保证同 ID 只保留最新向量。两步之间失败会让旧维度残留一份，下次写入同一 ID 或 `Memory.Reindex` 时清理。
- **Search**：按 `ns` 精确过滤并精确检索（`exact`），排序与 `memory.MemoryIndex` 一致：分数降序，并列按 ID 字节序升序。Qdrant 不保证并列分数的返回顺序，第 k 名之后可能有并列时，以第 k 名分数减 1e-6 为阈值取出全部候选，在本地排序后截断。分数为 float32 精度，与 `MemoryIndex` 相差在 1e-5 内。
- **IDs / Clear / Delete**：遍历本前缀的全部维度集合，按 `ns` 精确过滤，不涉及子命名空间。

## 运行测试

需要 Docker。存储放在 tmpfs 上，避免机械盘让建集合变慢：

```bash
docker run -d --rm --name qd-mem --tmpfs /qdrant/storage -p 127.0.0.1:56344:6334 qdrant/qdrant:v1.20.2
GOAGENT_QDRANT_ADDR=127.0.0.1:56344 go test -count=1 ./...
docker rm -f qd-mem
```

未设置 `GOAGENT_QDRANT_ADDR` 时，依赖 Qdrant 的测试跳过。
