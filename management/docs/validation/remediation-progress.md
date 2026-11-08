# 审计修复账本

计划：management/docs/planning/2026-11-08-management-remediation.md，起点9e49c89。
Task 1: complete — 3个查询回归RED→GREEN；全套46个通过事件，0失败/测试跳过。
Task 2: complete — 时长、TraceID、分页汇总与self/unknown回归RED→GREEN，全套51个通过事件。
Task 3: complete — 权限/保留/关闭/10类进程断点及outbox重投、事务原子性已实际验证。
Task 4: complete — 同仓相对replace后，GOWORK=off build/vet通过，standalone真实数据库race75个通过事件。
Task 5: complete — 本地两种构建方式91/91、Harness5/5、build/vet/lint通过，独立审阅四项全部RED→GREEN。
Ruling: 沿用现有隔离工作树与项目.claude执行账本，不创建第二个工作树；本轮无发布或主分支合并授权，先完成本地可审核实现。

Task 1 ruling: 复用核心预算控制器验证单个内存快照副本，不改核心API，不持源锁、不写源日志；缺失和不可读证据分别标记missing/unavailable。

Ruling: pgstore远端master仍634b5d8，未含当前接口迁移。采用可移植的同仓相对replace修复完整克隆中的GOWORK=off构建，不捏造远端版本、不写机器绝对路径；正式远端发布仍须先发布pgstore并移除该开发替换。

Task 3 config: 非正Task/Audit保留期与typed-nil Tracer回归RED→GREEN；所有构造现在沿用Scope验证Tracer，不引入第二套反射校验。

Final review: 独立审阅1a85bde相对9e49c89，0 Critical / 4 Important / 0 Minor；四项全接收：未知效果被取消覆盖、崩溃Attempt时长重叠、Validate阻塞Shutdown、未知未归属伪零。正式回归均已观察RED→GREEN；新增真实效果断点加取消、混合未归属证据回归通过。最终全套运行中。
Ruling: 崩溃旧Attempt无可靠结束时间，不伪造退出时刻；只累计已知执行区间并标duration_complete=false与unfinished_attempts。若误判将少计而非重复计执行时长，保留原Attempt供核查。
Final review declined: 不恢复旧版兼容，符合用户要求；同仓相对replace只作开发构建，正式发布必须联合发布pgstore并移除替换，未授权的远端发布与主线合并本轮不做。
