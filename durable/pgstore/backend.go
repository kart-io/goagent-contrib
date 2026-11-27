package pgstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kart-io/goagent/core/durable/leasestore"
	agentErrors "github.com/kart-io/goagent/errors"
)

// removeBatch 是 RemoveScope 每批删除的行数，避免单个长事务阻塞该作用域。
const removeBatch = 1000

// backend 是 leasestore.Backend 与 leasestore.SessionBackend 的 PostgreSQL 实现，本包的全部 SQL 都在这里。
// opTimeout 同时是写事务的 lock_timeout 与 statement_timeout。
type backend struct {
	pool      *pgxpool.Pool
	opTimeout time.Duration
}

var (
	_ leasestore.Backend        = (*backend)(nil)
	_ leasestore.SessionBackend = (*backend)(nil)
)

// AcquireLease 的读与写都响应 ctx。一旦尝试写入，错误可能意味着 INSERT/COMMIT 结果不确定，
// 用 leasestore.Uncertain 交给引擎补偿。
func (b *backend) AcquireLease(ctx context.Context, l leasestore.Lease, owner string, ttl time.Duration) (bool, error) {
	var removed, busy bool
	err := b.pool.QueryRow(ctx, `SELECT
  EXISTS (SELECT 1 FROM goagent_durable_scope WHERE ns = $1),
  COALESCE((SELECT expires_at >= now() FROM goagent_durable_lease WHERE ns = $1 AND id = $2), false)`, l.NS, l.ID).Scan(&removed, &busy)
	if err != nil {
		return false, err
	}
	if removed {
		return false, agentErrors.ErrScopeRemoved
	}
	if busy {
		return false, nil
	}
	acquired := false
	writeAttempted := false
	err = pgx.BeginTxFunc(ctx, b.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		if err := checkScope(ctx, tx, l.NS); err != nil {
			return err
		}
		// 顺序固定为 scope 锁 -> lease 锁 -> 行锁；清理租约不再取 scope 锁。
		if err := lockLease(ctx, tx, l.NS, l.ID); err != nil {
			return err
		}
		var got string
		writeAttempted = true
		err := tx.QueryRow(ctx, `INSERT INTO goagent_durable_lease (ns, id, token, owner, expires_at)
VALUES ($1, $2, $3, $4, now() + $5 * interval '1 millisecond')
ON CONFLICT (ns, id) DO UPDATE SET token = EXCLUDED.token, owner = EXCLUDED.owner, expires_at = EXCLUDED.expires_at
WHERE goagent_durable_lease.expires_at < now()
RETURNING token`, l.NS, l.ID, l.Token, owner, ttl.Milliseconds()).Scan(&got)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		acquired = err == nil
		return err
	})
	if err != nil && writeAttempted {
		// 引擎在事务结束后清理；lease 锁保证清理看见原事务的最终结果。
		return false, leasestore.Uncertain(err)
	}
	return acquired, err
}

// checkScope 在事务内对作用域取共享锁并检查墓碑，与 RemoveScope 的排他锁互斥；根命名空间跳过。
func checkScope(ctx context.Context, tx pgx.Tx, ns string) error {
	if ns == "" {
		return nil
	}
	// 墓碑须用另一条语句查询：READ COMMITTED 的快照在语句开始时取得，与加锁同一语句会看不到等锁期间提交的墓碑。
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))`, ns); err != nil {
		return err
	}
	var removed bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM goagent_durable_scope WHERE ns = $1)`, ns).Scan(&removed)
	if err == nil && removed {
		err = agentErrors.ErrScopeRemoved
	}
	return err
}

// lockLease 串行化同一 (ns, id) 的获取与清理，持有到事务提交或回滚。
// 双 int 键与 scope 使用的单 bigint 键属于不同锁空间，避免锁别名引入死锁。
// 哈希碰撞只会额外串行化；实际读写仍使用完整的 ns、id 与 token。
func lockLease(ctx context.Context, tx pgx.Tx, ns, id string) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))`, ns, id)
	return err
}

func (b *backend) ReleaseLease(ctx context.Context, l leasestore.Lease) error {
	return pgx.BeginTxFunc(ctx, b.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		if err := lockLease(ctx, tx, l.NS, l.ID); err != nil {
			return err
		}
		// 必须在获锁后的独立语句中 DELETE：新快照才能看到等待期间提交的租约。
		// token 限制保证不误删已接管的持有者；不检查 scope，墓碑不能阻止清理。
		_, err := tx.Exec(ctx, `DELETE FROM goagent_durable_lease WHERE ns = $1 AND id = $2 AND token = $3`, l.NS, l.ID, l.Token)
		return err
	})
}

func (b *backend) RenewLeases(ctx context.Context, leases []leasestore.Lease, ttl time.Duration) error {
	nss, ids, tokens := make([]string, len(leases)), make([]string, len(leases)), make([]string, len(leases))
	for i, l := range leases {
		nss[i], ids[i], tokens[i] = l.NS, l.ID, l.Token
	}
	_, err := b.pool.Exec(ctx, `UPDATE goagent_durable_lease AS l
SET expires_at = now() + $4 * interval '1 millisecond'
FROM unnest($1::text[], $2::text[], $3::text[]) AS h(ns, id, token)
WHERE l.ns = h.ns AND l.id = h.id AND l.token = h.token`, nss, ids, tokens, ttl.Milliseconds())
	return err
}

func (b *backend) DeleteExpiredLeases(ctx context.Context, grace time.Duration) error {
	_, err := b.pool.Exec(ctx, `DELETE FROM goagent_durable_lease WHERE expires_at < now() - $1 * interval '1 millisecond'`, grace.Milliseconds())
	return err
}

func (b *backend) LoadSnapshot(ctx context.Context, ns, id string) ([]byte, bool, error) {
	var removed bool
	var data []byte
	err := b.pool.QueryRow(ctx, `SELECT
  EXISTS (SELECT 1 FROM goagent_durable_scope WHERE ns = $1),
  (SELECT data FROM goagent_durable_snapshot WHERE ns = $1 AND id = $2)`, ns, id).Scan(&removed, &data)
	if err != nil {
		return nil, false, err
	}
	if removed {
		return nil, false, agentErrors.ErrScopeRemoved
	}
	return data, data != nil, nil
}

// writeTx 以同步提交执行写事务：快照或其中的持久快照事件在主备切换中丢失会让已完成的操作重跑。
// READ COMMITTED 让锁等待后的检查读取已提交版本，不继承连接的 REPEATABLE READ 默认值。
func (b *backend) writeTx(ctx context.Context, fn func(pgx.Tx) error) error {
	return pgx.BeginTxFunc(ctx, b.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		ms := fmt.Sprint(b.opTimeout.Milliseconds())
		if _, err := tx.Exec(ctx, `SELECT set_config('synchronous_commit', 'on', true), set_config('lock_timeout', $1, true), set_config('statement_timeout', $1, true)`, ms); err != nil {
			return err
		}
		return fn(tx)
	})
}

func (b *backend) SaveSnapshots(ctx context.Context, ns string, writes []leasestore.SnapshotWrite, ttl time.Duration) error {
	return b.writeTx(ctx, func(tx pgx.Tx) error {
		if err := checkScope(ctx, tx, ns); err != nil {
			return err
		}
		for _, w := range writes {
			// 只校验令牌：接管必然改写令牌，过期但未被接管时写入是安全的；UPDATE 持有租约行锁直到提交，同时顺带续约。
			tag, err := tx.Exec(ctx, `WITH l AS (
  UPDATE goagent_durable_lease SET expires_at = now() + $5 * interval '1 millisecond'
  WHERE ns = $1 AND id = $2 AND token = $3 RETURNING 1
)
INSERT INTO goagent_durable_snapshot (ns, id, data, updated_at)
SELECT $1, $2, $4, now() WHERE EXISTS (SELECT 1 FROM l)
ON CONFLICT (ns, id) DO UPDATE SET data = EXCLUDED.data, updated_at = EXCLUDED.updated_at`,
				ns, w.Lease.ID, w.Lease.Token, w.Data, ttl.Milliseconds())
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				return fmt.Errorf("%w：%q", agentErrors.ErrLeaseLost, w.Lease.ID)
			}
		}
		return nil
	})
}

// RemoveScope 分两段：先在排他锁内提交墓碑，再分批删除快照与租约。
func (b *backend) RemoveScope(ctx context.Context, ns string) error {
	err := pgx.BeginTxFunc(ctx, b.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		// 墓碑在主备切换中丢失会让已删除的作用域重新接受写入，须同步提交。
		if _, err := tx.Exec(ctx, `SELECT set_config('synchronous_commit', 'on', true), pg_advisory_xact_lock(hashtextextended($1, 0))`, ns); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO goagent_durable_scope (ns, removed_at) VALUES ($1, now()) ON CONFLICT (ns) DO NOTHING`, ns)
		return err
	})
	if err != nil {
		return err
	}
	for _, table := range []string{"goagent_durable_snapshot", "goagent_durable_lease"} {
		for {
			tag, err := b.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %[1]s WHERE ctid IN (SELECT ctid FROM %[1]s WHERE ns = $1 LIMIT %[2]d)`, table, removeBatch), ns)
			if err != nil {
				return err
			}
			if tag.RowsAffected() < removeBatch {
				break
			}
		}
	}
	return nil
}

func (b *backend) PruneScopes(ctx context.Context, olderThan time.Duration) error {
	ms := olderThan.Milliseconds()
	rows, err := b.pool.Query(ctx, `SELECT ns FROM goagent_durable_scope WHERE removed_at < now() - $1 * interval '1 millisecond'`, ms)
	if err != nil {
		return err
	}
	expired, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	for _, ns := range expired {
		err := pgx.BeginTxFunc(ctx, b.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, ns); err != nil {
				return err
			}
			// 持锁后以新语句复核，墓碑可能已被并发的 PruneScopes 回收。
			tag, err := tx.Exec(ctx, `DELETE FROM goagent_durable_scope WHERE ns = $1 AND removed_at < now() - $2 * interval '1 millisecond'`, ns, ms)
			if err != nil || tag.RowsAffected() == 0 {
				return err
			}
			if _, err := tx.Exec(ctx, `DELETE FROM goagent_durable_snapshot WHERE ns = $1`, ns); err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `DELETE FROM goagent_durable_lease WHERE ns = $1`, ns)
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// lastSeq 读取会话的最后序号，会话不存在时为 0。
func lastSeq(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, id string) (int64, error) {
	var last int64
	err := q.QueryRow(ctx, `SELECT last_seq FROM goagent_session WHERE id = $1`, id).Scan(&last)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return last, err
}

func (b *backend) LastSeq(ctx context.Context, id string) (int64, error) {
	return lastSeq(ctx, b.pool, id)
}

func (b *backend) AppendEvents(ctx context.Context, batches []leasestore.EventBatch, ttl time.Duration) error {
	return b.writeTx(ctx, func(tx pgx.Tx) error {
		for _, batch := range batches {
			if err := appendBatch(ctx, tx, batch, ttl); err != nil {
				return err
			}
		}
		return nil
	})
}

// appendBatch 在事务内校验租约与序号并写入事件。会话行的条件更新持有行锁到提交，并发追加按期望序号串行判定。
// 事件按原样的 JSON 存为 bytea：jsonb 会改写 Data 的键序与数字写法。
func appendBatch(ctx context.Context, tx pgx.Tx, p leasestore.EventBatch, ttl time.Duration) error {
	for _, l := range p.Fences {
		// 只校验令牌，同 SaveSnapshots；UPDATE 持有租约行锁直到提交，同时顺带续约。
		tag, err := tx.Exec(ctx, `UPDATE goagent_durable_lease SET expires_at = now() + $4 * interval '1 millisecond'
WHERE ns = $1 AND id = $2 AND token = $3`, l.NS, l.ID, l.Token, ttl.Milliseconds())
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("%w：会话 %q 中 %q", agentErrors.ErrLeaseLost, p.SessionID, l.ID)
		}
	}
	if len(p.Seqs) == 0 {
		// 空批次的比较也须保持到整个事务提交。不存在的会话用临时行
		// 取得唯一键锁，之后删除；并发 Append/Fork 仍须等待提交。
		created := false
		if p.Expected == 0 {
			tag, err := tx.Exec(ctx, `INSERT INTO goagent_session (id, last_seq) VALUES ($1, 0) ON CONFLICT (id) DO NOTHING`, p.SessionID)
			if err != nil {
				return err
			}
			created = tag.RowsAffected() == 1
		}
		var cur int64
		err := tx.QueryRow(ctx, `SELECT last_seq FROM goagent_session WHERE id = $1 FOR UPDATE`, p.SessionID).Scan(&cur)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if cur != p.Expected {
			return leasestore.Conflict(p.SessionID, p.Expected, cur)
		}
		if created {
			_, err = tx.Exec(ctx, `DELETE FROM goagent_session WHERE id = $1`, p.SessionID)
			return err
		}
		return nil
	}
	if p.Expected == 0 {
		if _, err := tx.Exec(ctx, `INSERT INTO goagent_session (id, last_seq) VALUES ($1, 0) ON CONFLICT (id) DO NOTHING`, p.SessionID); err != nil {
			return err
		}
	}
	last := p.Seqs[len(p.Seqs)-1]
	tag, err := tx.Exec(ctx, `UPDATE goagent_session SET last_seq = $3 WHERE id = $1 AND last_seq = $2`, p.SessionID, p.Expected, last)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		cur, err := lastSeq(ctx, tx, p.SessionID)
		if err != nil {
			return err
		}
		return leasestore.Conflict(p.SessionID, p.Expected, cur)
	}
	_, err = tx.Exec(ctx, `INSERT INTO goagent_session_event (session_id, seq, type, event)
SELECT $1, * FROM unnest($2::bigint[], $3::text[], $4::bytea[])`, p.SessionID, p.Seqs, p.Types, p.Data)
	return err
}

func (b *backend) ReadEvents(ctx context.Context, id string, after int64) ([][]byte, error) {
	rows, err := b.pool.Query(ctx, `SELECT event FROM goagent_session_event WHERE session_id = $1 AND seq > $2 ORDER BY seq`, id, after)
	if err != nil {
		return nil, err
	}
	raws, err := pgx.CollectRows(rows, pgx.RowTo[[]byte])
	if err != nil {
		return nil, err
	}
	if len(raws) == 0 {
		var exists bool
		if err := b.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM goagent_session WHERE id = $1)`, id).Scan(&exists); err != nil {
			return nil, err
		}
		if !exists {
			return nil, agentErrors.ErrSessionNotFound
		}
	}
	return raws, nil
}

// ListSessions 按字节序（与 sort.Strings 相同）返回。
func (b *backend) ListSessions(ctx context.Context) ([]string, error) {
	rows, err := b.pool.Query(ctx, `SELECT id FROM goagent_session ORDER BY id COLLATE "C"`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// DeleteSession 先锁会话行，与并发的追加和分叉串行。
func (b *backend) DeleteSession(ctx context.Context, id string) error {
	return b.writeTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT 1 FROM goagent_session WHERE id = $1 FOR UPDATE`, id); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM goagent_session_event WHERE session_id = $1`, id); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DELETE FROM goagent_session WHERE id = $1`, id)
		return err
	})
}

// ForkSession 复制期间对来源会话行取共享锁，复制的是一个完整的前缀。
func (b *backend) ForkSession(ctx context.Context, src, dst string, upTo int64) error {
	return b.writeTx(ctx, func(tx pgx.Tx) error {
		var last int64
		err := tx.QueryRow(ctx, `SELECT last_seq FROM goagent_session WHERE id = $1 FOR SHARE`, src).Scan(&last)
		if errors.Is(err, pgx.ErrNoRows) {
			return agentErrors.ErrSessionNotFound
		}
		if err != nil {
			return err
		}
		if upTo > 0 {
			last = min(last, upTo)
		}
		tag, err := tx.Exec(ctx, `INSERT INTO goagent_session (id, last_seq) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING`, dst, last)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return agentErrors.ErrSessionExists
		}
		_, err = tx.Exec(ctx, `INSERT INTO goagent_session_event (session_id, seq, type, event)
SELECT $1, seq, type, event FROM goagent_session_event WHERE session_id = $2 AND seq <= $3`, dst, src, last)
		return err
	})
}
