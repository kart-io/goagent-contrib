# GoAgent 管理扩展与验收宿主

通用管理服务已迁入 `github.com/kart-io/goagent/management`，原生/Graph 适配和 HTTP Handler 也使用核心新入口。这里不再提供旧根包或兼容转发。

| 保留目录 | 职责 |
| --- | --- |
| `pgstore` | management.Store 与 HistoryReader 的 PostgreSQL 实现 |
| `integration` | 真实数据库、HTTP、审批、进程恢复与并发集成测试 |
| `examples/host` | 离线退款 Agent 的可运行宿主及 8 组 HTTP 黑盒案例 |
| `harness` | 可选独立模块，对接 background 和 effects |

- [核心管理说明](https://github.com/kart-io/goagent/blob/master/management/README.md)
- [核心迁移指南](https://github.com/kart-io/goagent/blob/master/docs/guides/MANAGEMENT_MIGRATION.md)
- [当前 OpenAPI](https://github.com/kart-io/goagent/blob/master/management/httpapi/openapi.yaml)
- [运行与恢复](docs/operations.md)、[验收案例](docs/acceptance.md)
- docs 中原规划、案例日志和合并记录保留为迁移前的历史证据，不代表当前包归属。

## 联合开发与验证

必须使用包含新 management 包的 GoAgent 源码；v0.50.0 没有该包。本模块已锁定实际迁移提交 `db7fffed9a84` 对应的 Go 伪版本；它目前仅存在本地，须先将该提交推送到核心远端，外部消费者才能下载。远端 CI 同样锁定该核心提交。当前跨仓验证显式使用临时 workspace，不在核心提交本机路径或旧接口回退。

```bash
# 在 contrib 根目录运行；先准备专用 Postgres 17.6 并等待就绪。
export GOAGENT_CORE_ROOT=/path/to/goagent
export GOAGENT_PGSTORE_DSN='postgres://postgres:management-local@127.0.0.1:55441/postgres?sslmode=disable'
bash scripts/with-local-modules.sh go -C management test -race -count=1 -timeout 240s ./...
bash scripts/with-local-modules.sh go -C management run ./examples/host -migrate -verify -listen 127.0.0.1:0
```

`-migrate` 显式初始化管理表、执行日志表和案例退款表；构造器不隐式建表。案例断言批准退款只执行一次、重复提交复用 Task、Token 为 10、旧 Session/Fork 绑定 v1、新 Session 绑定 v2。8 组黑盒案例由 examples/host/test_cases.py 驱动真实 TCP HTTP 并查询数据库核对。

宿主认证通过 Authorization Bearer local-demo；生产接入必须替换可信身份和 Authorizer。所有创建/命令使用 Idempotency-Key，修订更新使用 expected_revision。领域错误改为核心 `errors.ErrManagement*`；JSON 类型与编解码采用核心 `codec/json`。

未设置 DSN 的测试跳过不构成持久验收；数据库重启和同步提交故障测试只允许在指定专用容器执行。原 pgstore 相对开发绑定仍保留在扩展模块，不进入核心依赖图。
