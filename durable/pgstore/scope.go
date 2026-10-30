package pgstore

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/kart-io/goagent/core/durable"
)

// removeBatch 是 RemoveScope 每批删除的行数，避免单个长事务阻塞该作用域。
const removeBatch = 1000

// scope 是 Store 的作用域，与根 Store 共享进程内闸门、持有表与续约。
type scope struct {
	root *Store
	ns   string
}

// Scope 实现 durable.Scoped：返回独立命名空间，不产生任何写入。
func (s *Store) Scope(name string) durable.Store { return &scope{root: s, ns: scopeNS(name)} }

func (c *scope) Lock(ctx context.Context, id string) (func() error, error) {
	return c.root.lock(ctx, c.ns, id)
}

func (c *scope) Load(ctx context.Context, id string) (*durable.Snapshot, error) {
	return c.root.load(ctx, c.ns, id)
}

func (c *scope) Save(ctx context.Context, id string, snap *durable.Snapshot) error {
	return c.root.save(ctx, c.ns, id, snap)
}

// RemoveScope 实现 durable.Scoped，分两段：先在排他锁内提交墓碑（墓碑已存在时不刷新时间），
// 墓碑生效后数据即不可读、写入被拒；再分批删除快照与租约。中途失败时重复调用可续删。不等待进行中的持锁者。
func (s *Store) RemoveScope(ctx context.Context, name string) error {
	ns := scopeNS(name)
	err := pgx.BeginFunc(ctx, s.cfg.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, ns); err != nil {
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
			tag, err := s.cfg.Pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %[1]s WHERE ctid IN (SELECT ctid FROM %[1]s WHERE ns = $1 LIMIT %[2]d)`, table, removeBatch), ns)
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

// PruneRemoved 实现 durable.Scoped：回收墓碑时间早于 now-olderThan 的作用域，在排他锁内删除残留数据与墓碑。
// 墓碑回收后同名作用域迟到的写入会成功，olderThan 须大于最长的重试或恢复窗口。
func (s *Store) PruneRemoved(ctx context.Context, olderThan time.Duration) error {
	if olderThan < 0 {
		return fmt.Errorf("墓碑保留时长不能为负：%v", olderThan)
	}
	ms := olderThan.Milliseconds()
	rows, err := s.cfg.Pool.Query(ctx, `SELECT ns FROM goagent_durable_scope WHERE removed_at < now() - $1 * interval '1 millisecond'`, ms)
	if err != nil {
		return err
	}
	expired, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	for _, ns := range expired {
		err := pgx.BeginFunc(ctx, s.cfg.Pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, ns); err != nil {
				return err
			}
			// 持锁后以新语句复核，墓碑可能已被并发的 PruneRemoved 回收。
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
