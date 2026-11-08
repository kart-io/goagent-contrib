# GoAgent 统一管理服务

可嵌入 Go 宿主的 Agent、Session、Task、Approval 和运行查询服务。管理 API 持久接纳请求，由一个活动 worker 执行；模型、工具、日志、预算与恢复复用 GoAgent。可选 [Harness 适配](harness) 不进入主模块依赖。

- [HTTP 接口](httpapi/openapi.yaml)：OpenAPI 3.1，文件采用 JSON 形式的合法 YAML。
- [运行与恢复说明](docs/operations.md)：部署边界、权限、停服、异常 owner 回收与数据保留。
- [验证案例](docs/acceptance.md)：真实 Postgres、HTTP、审批、取消、Fork 和跨进程恢复的复跑方式。
- [实施与验证报告](docs/VERIFICATION.md)：独立审阅修复、43 条测试、17 个关联模块与实际案例结果。
- [需求](docs/planning/2026-11-08-unified-management-requirements.md)、[调研](docs/planning/2026-11-08-unified-management-feasibility.md)、[设计](docs/planning/2026-11-08-unified-management-design.md)、[计划归档](docs/planning/2026-11-08-unified-management.md)。
- [案例源码](examples/host/main.go)：无需模型服务或 API 密钥的退款 Agent；业务退款记录仍真实写入 Postgres。

## 运行退款案例

准备专用本地数据库。以下密码只用于本地案例容器。

```bash
docker run -d --name goagent-management-demo \
  -e POSTGRES_PASSWORD=management-local \
  -p 127.0.0.1:55439:5432 postgres:17.6-alpine

export GOAGENT_PGSTORE_DSN='postgres://postgres:management-local@127.0.0.1:55439/postgres?sslmode=disable'
GOWORK=off go run ./examples/host -migrate -verify -listen 127.0.0.1:0
```

等待 `docker exec goagent-management-demo pg_isready -U postgres` 成功后运行 Go 命令。`-migrate` 明确授权案例初始化管理表、GoAgent 日志表和案例业务表；普通服务构造器不会隐式改表。

案例使用真实 HTTP 依次执行：注册 → 保存 v1/v2 → 发布 v1 → 创建会话 → 幂等提交任务 → 查询待审 → 批准退款 → 查询结果/用量 → 发布 v2 → 检查旧/新会话版本 → Fork。退出前断言：退款 1 次、重复提交同一 Task、Token 总量 10、旧会话/分支 v1、新会话 v2。

保留服务供手动操作：

```bash
GOWORK=off go run ./examples/host -listen 127.0.0.1:8097
```

案例通过 `Authorization: Bearer local-demo` 生成固定可信操作者；可用 `GOAGENT_DEMO_TOKEN` 修改。生产宿主必须替换身份认证和 Authorizer，不能把客户端 TenantID、ActorID 或内部恢复键当作可信输入。

## 嵌入与接口

宿主准备 `management.Config`：管理 Store、`session.Store`、有界 HistoryReader、Authorizer 与 `map[string]Factory`。调用 `management.New` 后，用 `NewWorker(service).Run(ctx)` 启动执行者；使用 `httpapi.New` 挂载标准 `http.Handler`。后者不监听端口、不为受管 Task 再启动一个请求级执行。

原生 Builder 使用 `adapters.NativeFactory`，宿主提供纯配置 `Check` 和固定版本 `BuildAgent`。适配器绑定 Session、Agent 与 durable scope，并通过宿主 Approver 处理持久单次审批。图使用 `adapters.GraphFactory[T]`，宿主必须提供与版本匹配的 ResumeHandler 和批准/拒绝分支。

每次创建和命令 POST 都带 `Idempotency-Key`。相同键/相同内容复用原响应；不同内容返回 409。输入数值保留精度，重复 JSON 键和未知字段拒绝。修订更新必须提交 `expected_revision`。

| 资源 | 路由 |
| --- | --- |
| Agent | `/v1/agents`、`/{id}`、`/{id}/versions`、`/{id}/publish`、`/{id}/enable`、`/{id}/disable` |
| Session | `/v1/sessions`、`/{id}`、`/{id}/history`、`/{id}/fork` |
| Task | `/v1/tasks`、`/{id}`、`/{id}/cancel`、`/{id}/resume` |
| Approval | `/v1/approvals`、`/{id}`、`/{id}/decision` |
| Run/Command | `/v1/runs`、`/{id}`；`/v1/commands/{id}` |

Task 的 `resume` 只能回应普通输入或可信核对后的 attention。审批必须通过 `decision` 提交，决定与续跑命令原子保存；Approval 查询中的 `command_state` 区分决定已记录与实际已应用。

Session 创建/Fork 返回 202 preparing，ready 后才可提交 Task。Fork 限没有未结束任务的完整正常轮次边界，不迁移 Agent 版本。发布新版本只影响新会话默认版本；版本实现缺失时不能自动改用最新版本。

## 验证

```bash
export GOAGENT_PGSTORE_DSN='postgres://postgres:management-local@127.0.0.1:55439/postgres?sslmode=disable'
GOWORK=off go test -race -count=1 -timeout 180s ./...
GOWORK=off go vet ./...
```

未设置 DSN 时数据库测试会 SKIP，不构成持久恢复验收。测试为组合案例创建独立 schema 并清理；进程恢复测试会启动并终止自身的测试子进程，不操作宿主已有 worker。`harness/` 是独立模块，需要在该目录单独执行同样的测试与 vet。

本模块的发布依赖保持可下载的兼容基线；验证当前 GoAgent v0.50.0 源码和同仓库 pgstore 时，使用仓库现有临时 workspace：

```bash
GOAGENT_CORE_ROOT=/path/to/goagent bash scripts/with-local-modules.sh bash -c \
  'cd management && go test -race -count=1 ./...'
```

上条命令在 contrib 仓库根目录运行，核心路径由实际克隆目录替换，不向 go.mod 写入机器绝对路径 replace。本模块尚未发布，不能把工作树验证描述为已经能从远端安装新模块。
