# 租约事务与提交不确定清理

`tryAcquire` 与 `deleteLease` 在同一 `(ns, id)` 上使用事务级 advisory 排他锁，直到提交或回滚才释放。获取租约的 COMMIT 响应丢失时，清理等待该事务结束，再按完整 `(ns, id, token)` 删除，保留其他持有者的 token。

获取、清理、Save/SaveAll、RemoveScope、PruneRemoved 和 SessionStore 写事务均显式使用 READ COMMITTED，不依赖连接默认隔离级别。清理的 DELETE 和 scope 墓碑检查必须是获锁后的独立语句，不能与加锁合并到同一 SQL/CTE：PostgreSQL 的语句快照在等待前建立。仅拆成两条 SQL 也不足以支持 REPEATABLE READ 的事务快照。依据：[事务隔离](https://www.postgresql.org/docs/current/transaction-iso.html)、[advisory 锁](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS)。

RemoveScope 与 PruneRemoved 获锁后重新判定墓碑，重复操作不会因连接默认 RR 使用旧快照而报序列化错误。SessionStore 的追加、批量比较与 Fork 同样依赖获行锁后的已提交状态：追加冲突返回 `ErrSessionConflict`，Fork 复制完整前缀；token 条件和事务行锁仍保护快照写入。这些操作覆盖整个写事务的隔离级别，不修改连接或服务器的默认配置。

获取顺序为 scope 共享锁 → lease 排他锁 → 行锁；清理只取 lease 锁与行锁，不请求 scope 锁，也不因 scope 墓碑跳过清理。lease 使用双 int 键，scope 使用单 bigint 键，两种锁空间互不重叠；哈希碰撞仅增加串行等待，不改变实际行匹配。

清理仍受 `OpTimeout` 限制。数据库不可用、等待超时或清理自身提交结果不确定时，通过 `OnError("delete_lease", err)` 报告，残留租约由 TTL 兜底；这不是无限等待或保证清理必达的协议。串行化要求并发获取者也使用相同租约锁，混用旧实现时不具备该保证。

Lock 的获取尝试同时受调用方 context 和 `OpTimeout` 限制，连接池、scope/lease advisory 等待可以被取消。尚未尝试 INSERT 的失败无需清理；一旦尝试写入，任何错误仍触发独立 `OpTimeout` 的按 token 补偿，调用方取消不会取消补偿。COMMIT 已成功而调用方在登记持有者前取消时，也先清理自身 token 再释放 gate，不登记 owners。取消返回的错误同时包装 `ErrLockUnavailable` 和调用方的 `context.Canceled` / `context.DeadlineExceeded`。因此发生写入不确定性时，返回可能晚于调用方期限，最长还要等待一次补偿期限；成功登记之后的取消仍由调用方负责 unlock。

回归测试使用隔离 schema 与真实事务，观察 `pg_locks` / `pg_blocking_pids` 确认清理在获取事务结束前阻塞，并验证提交、回滚、其他 token、默认 REPEATABLE READ、获取端锁协议及 scope 锁顺序。`TestAcquireUncertainCommitCleanup` 继续覆盖 COMMIT 发出后断开连接的故障。上述测试不重启数据库、不改全局同步复制配置。

`TestLockCancellationDuringAdvisoryWait` 覆盖公开 Lock 的真实 scope/lease 等待、取消及截止时间，确认在阻塞事务释放前返回并且 owners/gates/lease 无残留。`TestLockCancelledAfterCommit` 覆盖收到提交成功和丢失提交响应后的取消及独立补偿。`TestSaveRejectsTombstoneAfterScopeWait`、`TestScopeMaintenanceAfterConcurrentTransaction`、`TestSessionWritesAfterRowWait` 用 RC/RR 默认值对照，验证等锁期间提交后的墓碑拒写、重复删除/回收及会话行锁语义。
