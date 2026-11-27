package gormstore

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"strings"
	"time"

	"gorm.io/gorm"

	"github.com/kart-io/goagent/core/durable/leasestore"
	agentErrors "github.com/kart-io/goagent/errors"
)

const (
	// removeBatch 是 RemoveScope 每批删除的行数，避免单个长事务阻塞该作用域。
	removeBatch = 1000
	// insertBatch 是一条 INSERT 写入的事件数，避免超出参数个数上限。
	insertBatch = 500
	// leaseBatch 是一条语句续约或清理的租约数。
	leaseBatch = 500
)

// backend 是 leasestore.Backend 与 leasestore.SessionBackend 的 gorm 实现，本包的全部 SQL 都在这里与 dialect.go。
// opTimeout 是写事务的时限。
type backend struct {
	db        *gorm.DB
	d         dialect
	opTimeout time.Duration
}

var (
	_ leasestore.Backend        = (*backend)(nil)
	_ leasestore.SessionBackend = (*backend)(nil)
)

// 行锁只在事务内有效，且等锁后的检查要读到已提交的版本，所以一律 READ COMMITTED，不继承连接的默认隔离级别
// （MySQL 默认的 REPEATABLE READ 还会带来间隙锁）。
var readCommitted = &sql.TxOptions{Isolation: sql.LevelReadCommitted}

func (b *backend) tx(ctx context.Context, fn func(tx *gorm.DB) error) error {
	return b.db.WithContext(ctx).Transaction(fn, readCommitted)
}

func (b *backend) writeTx(ctx context.Context, fn func(tx *gorm.DB) error) error {
	ctx, cancel := context.WithTimeout(ctx, b.opTimeout)
	defer cancel()
	return b.tx(ctx, fn)
}

// 闸门是预置行上的行锁，代替各数据库互不兼容的咨询锁。哈希碰撞只会额外串行化，实际读写仍使用完整的 ns 与 id。
// 加锁顺序固定为作用域闸门 -> 租约闸门 -> 数据行。
func scopeSlot(ns string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(ns))
	return h.Sum32() % scopeSlots
}

func leaseSlot(ns, id string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(ns))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(id))
	return scopeSlots + h.Sum32()%leaseSlots
}

func lockGate(tx *gorm.DB, slot uint32, mode string) error {
	var got uint32
	return tx.Raw(`SELECT slot FROM goagent_gorm_gate WHERE slot = ? `+mode, slot).Row().Scan(&got)
}

// checkScope 在事务内对作用域闸门取共享锁并检查墓碑，与 RemoveScope 的排他锁互斥；根命名空间跳过。
func checkScope(tx *gorm.DB, ns string) error {
	if ns == "" {
		return nil
	}
	if err := lockGate(tx, scopeSlot(ns), "FOR SHARE"); err != nil {
		return err
	}
	// 墓碑须在获锁之后的语句里查询，才能看到等锁期间提交的墓碑。
	var removed bool
	err := tx.Raw(`SELECT EXISTS (SELECT 1 FROM goagent_gorm_scope WHERE ns = ?)`, ns).Row().Scan(&removed)
	if err == nil && removed {
		err = agentErrors.ErrScopeRemoved
	}
	return err
}

// AcquireLease 一旦尝试写入，错误可能意味着写入或提交的结果不确定，用 leasestore.Uncertain 交给引擎补偿。
func (b *backend) AcquireLease(ctx context.Context, l leasestore.Lease, owner string, ttl time.Duration) (bool, error) {
	var removed, busy bool
	err := b.db.WithContext(ctx).Raw(`SELECT
  EXISTS (SELECT 1 FROM goagent_gorm_scope WHERE ns = ?),
  COALESCE((SELECT expires_at >= `+b.d.now+` FROM goagent_gorm_lease WHERE ns = ? AND id = ?), false)`,
		l.NS, l.NS, l.ID).Row().Scan(&removed, &busy)
	if err != nil {
		return false, err
	}
	if removed {
		return false, agentErrors.ErrScopeRemoved
	}
	if busy {
		return false, nil
	}
	acquired, attempted := false, false
	err = b.tx(ctx, func(tx *gorm.DB) error {
		if err := checkScope(tx, l.NS); err != nil {
			return err
		}
		if err := lockGate(tx, leaseSlot(l.NS, l.ID), "FOR UPDATE"); err != nil {
			return err
		}
		var held bool
		err := tx.Raw(`SELECT expires_at >= `+b.d.now+` FROM goagent_gorm_lease WHERE ns = ? AND id = ?`, l.NS, l.ID).Row().Scan(&held)
		if errors.Is(err, sql.ErrNoRows) {
			// 闸门排除了同一 (ns, id) 的其他获取者，此处插入不会撞键。
			attempted = true
			err = tx.Exec(`INSERT INTO goagent_gorm_lease (ns, id, token, owner, expires_at) VALUES (?, ?, ?, ?, `+b.d.after+`)`,
				l.NS, l.ID, l.Token, owner, ttl.Milliseconds()).Error
			acquired = err == nil
			return err
		}
		if err != nil || held {
			return err
		}
		// 条件里再判一次过期：持有者的续约不经过闸门，可能在上一条语句之后提交。
		attempted = true
		res := tx.Exec(`UPDATE goagent_gorm_lease SET token = ?, owner = ?, expires_at = `+b.d.after+`
WHERE ns = ? AND id = ? AND expires_at < `+b.d.now, l.Token, owner, ttl.Milliseconds(), l.NS, l.ID)
		acquired = res.Error == nil && res.RowsAffected == 1
		return res.Error
	})
	if err != nil && attempted {
		// 引擎在事务结束后用 ReleaseLease 清理；租约闸门保证清理看见原事务的最终结果。
		return false, leasestore.Uncertain(err)
	}
	return acquired && err == nil, err
}

func (b *backend) ReleaseLease(ctx context.Context, l leasestore.Lease) error {
	return b.tx(ctx, func(tx *gorm.DB) error {
		if err := lockGate(tx, leaseSlot(l.NS, l.ID), "FOR UPDATE"); err != nil {
			return err
		}
		// token 限制保证不误删已接管的持有者；不检查作用域，墓碑不能阻止清理。
		return tx.Exec(`DELETE FROM goagent_gorm_lease WHERE ns = ? AND id = ? AND token = ?`, l.NS, l.ID, l.Token).Error
	})
}

// leaseKeys 把一批租约写成按主键升序的 OR 条件。多行语句与 SaveSnapshots 都按这个顺序取租约行锁，不会互相死锁；
// 不用行构造器 IN：MySQL 对它不一定走主键范围。
func leaseKeys(leases []leasestore.Lease, withToken bool) (string, []any) {
	sorted := slices.Clone(leases)
	slices.SortFunc(sorted, func(x, y leasestore.Lease) int {
		return cmp.Or(cmp.Compare(x.NS, y.NS), cmp.Compare(x.ID, y.ID))
	})
	cond, args := "(ns = ? AND id = ?)", make([]any, 0, 3*len(sorted))
	if withToken {
		cond = "(ns = ? AND id = ? AND token = ?)"
	}
	for _, l := range sorted {
		args = append(args, l.NS, l.ID)
		if withToken {
			args = append(args, l.Token)
		}
	}
	return strings.TrimSuffix(strings.Repeat(cond+" OR ", len(sorted)), " OR "), args
}

func (b *backend) RenewLeases(ctx context.Context, leases []leasestore.Lease, ttl time.Duration) error {
	for start := 0; start < len(leases); start += leaseBatch {
		where, args := leaseKeys(leases[start:min(start+leaseBatch, len(leases))], true)
		err := b.tx(ctx, func(tx *gorm.DB) error {
			return tx.Exec(`UPDATE goagent_gorm_lease SET expires_at = `+b.d.after+` WHERE `+where,
				append([]any{ttl.Milliseconds()}, args...)...).Error
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteExpiredLeases 先不加锁地读出过期租约，再按主键删除并复核过期条件。
// 直接按 expires_at 范围删除会先锁二级索引再锁主键，与持有者的写入（先主键后索引）在 MySQL 上死锁。
func (b *backend) DeleteExpiredLeases(ctx context.Context, grace time.Duration) error {
	ms := grace.Milliseconds()
	rows, err := b.db.WithContext(ctx).Raw(`SELECT ns, id FROM goagent_gorm_lease WHERE expires_at < `+b.d.before, ms).Rows()
	if err != nil {
		return err
	}
	var expired []leasestore.Lease
	for rows.Next() {
		var l leasestore.Lease
		if err := rows.Scan(&l.NS, &l.ID); err != nil {
			_ = rows.Close()
			return err
		}
		expired = append(expired, l)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	for start := 0; start < len(expired); start += leaseBatch {
		where, args := leaseKeys(expired[start:min(start+leaseBatch, len(expired))], false)
		err := b.tx(ctx, func(tx *gorm.DB) error {
			return tx.Exec(`DELETE FROM goagent_gorm_lease WHERE (`+where+`) AND expires_at < `+b.d.before, append(args, ms)...).Error
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *backend) LoadSnapshot(ctx context.Context, ns, id string) ([]byte, bool, error) {
	var removed bool
	var data []byte
	err := b.db.WithContext(ctx).Raw(`SELECT
  EXISTS (SELECT 1 FROM goagent_gorm_scope WHERE ns = ?),
  (SELECT data FROM goagent_gorm_snapshot WHERE ns = ? AND id = ?)`, ns, ns, id).Row().Scan(&removed, &data)
	if err != nil {
		return nil, false, err
	}
	if removed {
		return nil, false, agentErrors.ErrScopeRemoved
	}
	return data, data != nil, nil
}

// fence 校验租约令牌并续约，持有租约行锁到事务结束；令牌不符时返回 false。
// 只校验令牌：接管必然改写令牌，过期但未被接管时写入是安全的。
// 先锁定再更新，而不看 UPDATE 的影响行数：MySQL 在值未变化时报告 0 行。
func (b *backend) fence(tx *gorm.DB, l leasestore.Lease, ttl time.Duration) (bool, error) {
	var one int
	err := tx.Raw(`SELECT 1 FROM goagent_gorm_lease WHERE ns = ? AND id = ? AND token = ? FOR UPDATE`, l.NS, l.ID, l.Token).Row().Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, tx.Exec(`UPDATE goagent_gorm_lease SET expires_at = `+b.d.after+` WHERE ns = ? AND id = ?`,
		ttl.Milliseconds(), l.NS, l.ID).Error
}

func (b *backend) SaveSnapshots(ctx context.Context, ns string, writes []leasestore.SnapshotWrite, ttl time.Duration) error {
	return b.writeTx(ctx, func(tx *gorm.DB) error {
		if err := checkScope(tx, ns); err != nil {
			return err
		}
		// 按 ID 升序取租约行锁，与 leaseKeys 的顺序一致。
		writes := slices.Clone(writes)
		slices.SortFunc(writes, func(x, y leasestore.SnapshotWrite) int { return cmp.Compare(x.Lease.ID, y.Lease.ID) })
		for _, w := range writes {
			ok, err := b.fence(tx, leasestore.Lease{NS: ns, ID: w.Lease.ID, Token: w.Lease.Token}, ttl)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("%w：%q", agentErrors.ErrLeaseLost, w.Lease.ID)
			}
			if err := tx.Exec(b.d.upsertSnapshot, ns, w.Lease.ID, w.Data).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// RemoveScope 分两段：先在排他闸门内提交墓碑，再分批删除快照与租约。
func (b *backend) RemoveScope(ctx context.Context, ns string) error {
	err := b.tx(ctx, func(tx *gorm.DB) error {
		if err := lockGate(tx, scopeSlot(ns), "FOR UPDATE"); err != nil {
			return err
		}
		return tx.Exec(b.d.addTombstone, ns).Error
	})
	if err != nil {
		return err
	}
	for _, table := range []string{"goagent_gorm_snapshot", "goagent_gorm_lease"} {
		for {
			var n int64
			err := b.tx(ctx, func(tx *gorm.DB) error {
				res := tx.Exec(fmt.Sprintf(b.d.deleteBatch, table, removeBatch), ns)
				n = res.RowsAffected
				return res.Error
			})
			if err != nil {
				return err
			}
			if n < removeBatch {
				break
			}
		}
	}
	return nil
}

func (b *backend) PruneScopes(ctx context.Context, olderThan time.Duration) error {
	ms := olderThan.Milliseconds()
	expired, err := b.strings(b.db.WithContext(ctx), `SELECT ns FROM goagent_gorm_scope WHERE removed_at < `+b.d.before, ms)
	if err != nil {
		return err
	}
	for _, ns := range expired {
		err := b.tx(ctx, func(tx *gorm.DB) error {
			if err := lockGate(tx, scopeSlot(ns), "FOR UPDATE"); err != nil {
				return err
			}
			// 持锁后复核，墓碑可能已被并发的 PruneScopes 回收。
			res := tx.Exec(`DELETE FROM goagent_gorm_scope WHERE ns = ? AND removed_at < `+b.d.before, ns, ms)
			if res.Error != nil || res.RowsAffected == 0 {
				return res.Error
			}
			if err := tx.Exec(`DELETE FROM goagent_gorm_snapshot WHERE ns = ?`, ns).Error; err != nil {
				return err
			}
			return tx.Exec(`DELETE FROM goagent_gorm_lease WHERE ns = ?`, ns).Error
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *backend) strings(q *gorm.DB, query string, args ...any) ([]string, error) {
	rows, err := q.Raw(query, args...).Rows()
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// lastSeq 读取会话的最后序号，会话不存在时为 0；lock 非空时同时锁定会话行。
func lastSeq(q *gorm.DB, id, lock string) (int64, error) {
	var last int64
	err := q.Raw(`SELECT last_seq FROM goagent_gorm_session WHERE id = ? `+lock, id).Row().Scan(&last)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return last, err
}

func (b *backend) LastSeq(ctx context.Context, id string) (int64, error) {
	return lastSeq(b.db.WithContext(ctx), id, "")
}

func (b *backend) AppendEvents(ctx context.Context, batches []leasestore.EventBatch, ttl time.Duration) error {
	return b.writeTx(ctx, func(tx *gorm.DB) error {
		for _, batch := range batches {
			if err := b.appendBatch(tx, batch, ttl); err != nil {
				return err
			}
		}
		return nil
	})
}

// appendBatch 在事务内校验租约与序号并写入事件。
//
// 已提交的会话行序号至少为 1：追加把它推进到最后一个事件，分叉复制的前缀非空。序号为 0 的行只存在于
// 建它的事务之内，所以期望序号为 0 时先建这样一行，以它的行锁与并发的追加、分叉串行，不必区分是否新建。
func (b *backend) appendBatch(tx *gorm.DB, p leasestore.EventBatch, ttl time.Duration) error {
	for _, l := range p.Fences {
		ok, err := b.fence(tx, l, ttl)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w：会话 %q 中 %q", agentErrors.ErrLeaseLost, p.SessionID, l.ID)
		}
	}
	if p.Expected == 0 {
		if err := tx.Exec(b.d.ensureSession, p.SessionID).Error; err != nil {
			return err
		}
	}
	cur, err := lastSeq(tx, p.SessionID, "FOR UPDATE")
	if err != nil {
		return err
	}
	if cur != p.Expected {
		return leasestore.Conflict(p.SessionID, p.Expected, cur)
	}
	if len(p.Seqs) == 0 {
		// 只比较的批次不留下会话行；行锁随删除保持到事务提交。
		return tx.Exec(`DELETE FROM goagent_gorm_session WHERE id = ? AND last_seq = 0`, p.SessionID).Error
	}
	if err := tx.Exec(`UPDATE goagent_gorm_session SET last_seq = ? WHERE id = ?`, p.Seqs[len(p.Seqs)-1], p.SessionID).Error; err != nil {
		return err
	}
	for start := 0; start < len(p.Seqs); start += insertBatch {
		end := min(start+insertBatch, len(p.Seqs))
		args := make([]any, 0, 4*(end-start))
		for i := start; i < end; i++ {
			args = append(args, p.SessionID, p.Seqs[i], p.Types[i], p.Data[i])
		}
		values := strings.TrimSuffix(strings.Repeat("(?, ?, ?, ?),", end-start), ",")
		if err := tx.Exec(`INSERT INTO goagent_gorm_session_event (session_id, seq, type, event) VALUES `+values, args...).Error; err != nil {
			return err
		}
	}
	return nil
}

func (b *backend) ReadEvents(ctx context.Context, id string, after int64) ([][]byte, error) {
	db := b.db.WithContext(ctx)
	rows, err := db.Raw(`SELECT event FROM goagent_gorm_session_event WHERE session_id = ? AND seq > ? ORDER BY seq`, id, after).Rows()
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	raws := [][]byte{}
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		raws = append(raws, raw)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(raws) == 0 {
		var exists bool
		if err := db.Raw(`SELECT EXISTS (SELECT 1 FROM goagent_gorm_session WHERE id = ?)`, id).Row().Scan(&exists); err != nil {
			return nil, err
		}
		if !exists {
			return nil, agentErrors.ErrSessionNotFound
		}
	}
	return raws, nil
}

func (b *backend) ListSessions(ctx context.Context) ([]string, error) {
	return b.strings(b.db.WithContext(ctx), b.d.listSessions)
}

// DeleteSession 先锁会话行，与并发的追加和分叉串行。
func (b *backend) DeleteSession(ctx context.Context, id string) error {
	return b.writeTx(ctx, func(tx *gorm.DB) error {
		if _, err := lastSeq(tx, id, "FOR UPDATE"); err != nil {
			return err
		}
		if err := tx.Exec(`DELETE FROM goagent_gorm_session_event WHERE session_id = ?`, id).Error; err != nil {
			return err
		}
		return tx.Exec(`DELETE FROM goagent_gorm_session WHERE id = ?`, id).Error
	})
}

// ForkSession 复制期间对来源会话行取共享锁，复制的是一个完整的前缀。
func (b *backend) ForkSession(ctx context.Context, src, dst string, upTo int64) error {
	return b.writeTx(ctx, func(tx *gorm.DB) error {
		var last int64
		err := tx.Raw(`SELECT last_seq FROM goagent_gorm_session WHERE id = ? FOR SHARE`, src).Row().Scan(&last)
		if errors.Is(err, sql.ErrNoRows) {
			return agentErrors.ErrSessionNotFound
		}
		if err != nil {
			return err
		}
		if upTo > 0 {
			last = min(last, upTo)
		}
		// 目标已存在时序号至少为 1，见 appendBatch。
		if err := tx.Exec(b.d.ensureSession, dst).Error; err != nil {
			return err
		}
		cur, err := lastSeq(tx, dst, "FOR UPDATE")
		if err != nil {
			return err
		}
		if cur != 0 {
			return agentErrors.ErrSessionExists
		}
		if err := tx.Exec(`UPDATE goagent_gorm_session SET last_seq = ? WHERE id = ?`, last, dst).Error; err != nil {
			return err
		}
		return tx.Exec(`INSERT INTO goagent_gorm_session_event (session_id, seq, type, event)
SELECT ?, seq, type, event FROM goagent_gorm_session_event WHERE session_id = ? AND seq <= ?`, dst, src, last).Error
	})
}
