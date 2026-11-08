# SDD ledger — plan: /Users/costalong/code/go/src/github.com/kart/goagent/docs/superpowers/plans/2026-11-08-unified-management.md

T0–T10：执行中。

Ruling: 目标仓库有既有未提交变更，采用独立worktree /tmp/goagent-management-20261108，保留原变更。
Ruling: 计划任务标题为T0中文格式，技能脚本仅支持Task格式，使用本账本按原编号记录任务、RED/GREEN、提交和证据。
Pre-flight: T0→T1 Runtime/Factory、T1→T2 Store/Tx、T2→T3/T4事务、T3/T4→T5版本会话、T5→T6/T7命令/状态、T6/T7→T8 HTTP、T5/T6→T9 outbox、全部→T10组合验收，未发现需另问用户的接口冲突。

T0: 原生批准/拒绝、并行隔离、完成只读回放 RED→GREEN；独立进程与图路径并入后续组合验收，尚不宣称T0全部完成。
T1: 类型状态/默认值/身份/JSON精度测试 RED→GREEN，go test -race . 通过。
Ruling: 存储采用Document包封装的固定Kind表接口，保持独立实体表、SQL外键和租户约束，避免重复的每实体CRUD实现。
Ruling: Pipeline合并同工具同参数的并发审批，管理投影按稳定WaitRef去重；独立操作用不同参数实验。
Ruling: Scope规定根执行键等于RunID，采用TaskID作为两者，替代设计中另算一次hash的ExecutionKey，保持预算和Scope身份不冲突。

T2: 真实Postgres事务回滚、权限分页 RED→GREEN；管理表外键与CAS已实现。
T3/T4: 不可变版本、旧Session固定、preparing→ready、Fork拒绝活动会话和源分支隔离已通过真实数据库测试。
T5/T6/T7/T8: 原生退款批准/拒绝、任务幂等、10 Token持久归属、HTTP路由/输入限制、并发决定单赢家、真实退出后取消、同会话串行通过。
T0/T10: 强制杀掉实际worker子进程，审批后由新进程恢复，SQL计数证明模型2次、退款1次；图审批双分支与重复决定也通过。
T9: 可选Harness泛型Sink独立模块不依赖未发布的management模块，GOWORK=off通过，主模块不引用Harness。
Ruling: 为同时验证已发布依赖与当前核心，management保持已发布旧兼容基线；本地pgstore迁移常量并升核心v0.50.0，临时workspace验证当前源码。
Ruling: 各数据库组合测试使用独立schema，防止前一测试遗留未完成业务被后续factory接管，子进程显式共用该schema。

Final review: c83f2f5 全分支独立审阅，0 Critical / 7 Important / 0 Minor。7项均按实际影响接受，逐项补回归RED→GREEN：取消/等待、unknown证据、图旧审批键、图输入拒绝、核对协议、outbox回收、恢复取消前核对完成。独立审阅设置之外事项均与既有取舍相符，不新增延期项。

Final: fixed 7 Important — 7个回归均已观察RED→GREEN；图旧键/输入拒绝保留等待，unknown不执行，取消等待冻结取消，完成证据先读，缺少RuntimeReconciler不接纳attention恢复，outbox有停止确认的回收接口。
Final: additional native完成核对回归先发现预算参与模型意图导致重放冲突；用原预算配置＋内存日志副本恢复，只读源日志且不重复模型/工具，新增真实数据库回归通过。
Ruling: 关联模块compatibility的ACP旧Session常量测试也须迁移，并升其核心依赖v0.50.0；只改隔离工作树，不覆盖原仓库现有未提交修订。
Ruling: attention必须有版本能力与RuntimeReconciler，证据引用/摘要持久化并严格匹配OperationRefs；内置Native/Graph没有通用业务核对能力时明确unsupported，不假装续跑。
T4/E5: 真实子进程在Fork复制后以42退出，重启比对前缀补ready，日志无二次写入，已PASS。
T9: 通知首次失败再投相同DeliveryID，Task仅一个Attempt且终态不变，已PASS。
Task T0: complete — 已交付对应接口/存储/适配/案例；实际测试与替代命名见management/docs/VERIFICATION.md，源码de0bb5f。
Task T1: complete — 已交付对应接口/存储/适配/案例；实际测试与替代命名见management/docs/VERIFICATION.md，源码de0bb5f。
Task T2: complete — 已交付对应接口/存储/适配/案例；实际测试与替代命名见management/docs/VERIFICATION.md，源码de0bb5f。
Task T3: complete — 已交付对应接口/存储/适配/案例；实际测试与替代命名见management/docs/VERIFICATION.md，源码de0bb5f。
Task T4: complete — 已交付对应接口/存储/适配/案例；实际测试与替代命名见management/docs/VERIFICATION.md，源码de0bb5f。
Task T5: complete — 已交付对应接口/存储/适配/案例；实际测试与替代命名见management/docs/VERIFICATION.md，源码de0bb5f。
Task T6: complete — 已交付对应接口/存储/适配/案例；实际测试与替代命名见management/docs/VERIFICATION.md，源码de0bb5f。
Task T7: complete — 已交付对应接口/存储/适配/案例；实际测试与替代命名见management/docs/VERIFICATION.md，源码de0bb5f。
Task T8: complete — 已交付对应接口/存储/适配/案例；实际测试与替代命名见management/docs/VERIFICATION.md，源码de0bb5f。
Task T9: complete — 已交付对应接口/存储/适配/案例；实际测试与替代命名见management/docs/VERIFICATION.md，源码de0bb5f。
Task T10: complete — 已交付对应接口/存储/适配/案例；实际测试与替代命名见management/docs/VERIFICATION.md，源码de0bb5f。
Final suite: management 43 passed / 0 failed / 0 test skipped；vet通过；golangci-lint v2.15.0 0 issues；可选Harness独立验证通过；make compatibility 17/17 passed（条件跳过详见JSON）；真实HTTP退款案例通过。
Final review: 独立审阅7 Important全部RED→GREEN；无延期Minor。
