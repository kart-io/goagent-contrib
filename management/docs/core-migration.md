# 通用管理层迁入核心

通用 Agent、Session、Task、Approval、Worker、原生/Graph 适配和管理 HTTP 已迁入 `github.com/kart-io/goagent/management`。contrib 保留 SQL 实现、数据库集成测试、退款案例及独立 Harness 适配；旧根包和通用子包已删除，没有兼容转发。

- 核心受检实现：`db7fffed9a84`；本模块 go.mod 与 CI 同时锁定该实际提交。
- 原93个管理测试事件分为核心21个和扩展72个，全部保留。
- 核心全库4771通过/9既有跳过，覆盖率76.0%；扩展13模块972通过/12既有跳过，lint均无问题。
- 关闭workspace后，使用从实际核心提交生成的模块制品，真实数据库72个测试事件通过、0跳过。
- 内置退款案例及8组真实HTTP案例通过。独立审阅0 Critical、0 Important、0 Minor，Yes。

完整报告及原始案例证据位于核心仓库 `docs/evaluation/2026-11-08-management-core-migration/REPORT.md`；[核心迁移指南](https://github.com/kart-io/goagent/blob/master/docs/guides/MANAGEMENT_MIGRATION.md)说明新入口、集中错误和严格JSON规则。

本轮按既有要求只同步本地主线，不推送或发布。外部机器和远端CI必须先能获取上述核心提交，才能拉取该扩展的锁定依赖；当前pgstore开发替换的发布步骤仍需单独完成。没有将本地模块代理或关闭校验的测试环境写入项目配置。
