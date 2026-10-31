package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/kart-io/goagent/core/durable"
	"github.com/kart-io/goagent/core/session"
)

// SessionStore 实现 session.Store，供多个实例共享会话日志；与创建它的 Store 共享连接池、租约与续约，
// 关闭 Store 即停止它的锁。
//
// Lock 是会话命名空间中的租约，语义同 Store.Lock。追加 boundary/snapshot 事件时，在同一事务内校验本进程持有
// session.DurableLockName(快照 ID) 的租约，被接管后的旧持有者写入返回 durable.ErrLeaseLost；
// 因此 session.DurableStore 视图与 Store 一样拒绝过期持有者的写入，而快照只能在持锁时追加。
type SessionStore struct {
	root *Store
}

var _ session.Store = (*SessionStore)(nil)

// NewSessionStore 校验会话日志的表结构版本。
func NewSessionStore(ctx context.Context, root *Store) (*SessionStore, error) {
	if root == nil {
		return nil, fmt.Errorf("pgstore.NewSessionStore 需要 Store")
	}
	var version int
	if err := root.cfg.Pool.QueryRow(ctx, `SELECT version FROM goagent_session_schema`).Scan(&version); err != nil {
		return nil, fmt.Errorf("读取会话日志表结构版本失败：%w", err)
	}
	if version != SessionSchemaVersion {
		return nil, fmt.Errorf("会话日志表结构版本为 %d，需要 %d", version, SessionSchemaVersion)
	}
	return &SessionStore{root: root}, nil
}

// sessionNS 是会话锁的命名空间，与根命名空间 "" 和作用域的 "s:" 前缀区分。
func sessionNS(id string) string { return "session:" + id }

func validSessionID(id string) error {
	if id == "" {
		return errors.New("会话 ID 不能为空")
	}
	return nil
}

// Lock 实现 session.Store。
func (s *SessionStore) Lock(ctx context.Context, id, name string) (func() error, error) {
	if err := validSessionID(id); err != nil {
		return nil, err
	}
	return s.root.lock(ctx, sessionNS(id), name)
}

// writeTx 以同步提交执行写事务：事件中有持久快照，主备切换中丢失会让已完成的操作重跑。
func (s *SessionStore) writeTx(ctx context.Context, fn func(pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, s.root.cfg.Pool, func(tx pgx.Tx) error {
		ms := fmt.Sprint(s.root.cfg.OpTimeout.Milliseconds())
		if _, err := tx.Exec(ctx, `SELECT set_config('synchronous_commit', 'on', true), set_config('lock_timeout', $1, true), set_config('statement_timeout', $1, true)`, ms); err != nil {
			return err
		}
		return fn(tx)
	})
}

// fences 返回 events 中各持久快照对应的本进程租约；未持有时返回 durable.ErrLeaseLost。
func (s *SessionStore) fences(id string, events []session.Event) ([]*hold, error) {
	var holds []*hold
	for _, ev := range events {
		if ev.Type != session.TypeBoundarySnapshot {
			continue
		}
		var d session.BoundaryData
		if err := json.Unmarshal(ev.Data, &d); err != nil {
			return nil, fmt.Errorf("快照事件损坏：%w", err)
		}
		name := session.DurableLockName(d.ID)
		s.root.mu.Lock()
		h := s.root.owners[key(sessionNS(id), name)]
		s.root.mu.Unlock()
		if h == nil {
			return nil, fmt.Errorf("%w：本进程未持有会话 %q 中 %q 的锁", durable.ErrLeaseLost, id, name)
		}
		holds = append(holds, h)
	}
	return holds, nil
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

func conflict(expected, cur int64) error {
	return fmt.Errorf("%w：期望序号 %d，当前 %d", session.ErrConflict, expected, cur)
}

// Append 实现 session.Store。会话行的条件更新持有行锁到提交，并发追加按期望序号串行判定。
// 事件按原样的 JSON 存为 bytea：jsonb 会改写 Data 的键序与数字写法。
func (s *SessionStore) Append(ctx context.Context, id string, expected int64, events ...session.Event) (int64, error) {
	if err := validSessionID(id); err != nil {
		return 0, err
	}
	if len(events) == 0 {
		cur, err := lastSeq(ctx, s.root.cfg.Pool, id)
		if err == nil && cur != expected {
			err = conflict(expected, cur)
		}
		return cur, err
	}
	holds, err := s.fences(id, events)
	if err != nil {
		return 0, err
	}
	now := time.Now().UTC()
	seqs, types, data := make([]int64, len(events)), make([]string, len(events)), make([][]byte, len(events))
	for i, ev := range events {
		ev.Seq = expected + int64(i) + 1
		if ev.Time.IsZero() {
			ev.Time = now
		}
		seqs[i], types[i] = ev.Seq, ev.Type
		if data[i], err = json.Marshal(ev); err != nil {
			return 0, err
		}
	}
	last := expected + int64(len(events))
	err = s.writeTx(ctx, func(tx pgx.Tx) error {
		for _, h := range holds {
			// 只校验令牌，同 Store.Save：接管必然改写令牌；UPDATE 持有租约行锁直到提交，同时顺带续约。
			tag, err := tx.Exec(ctx, `UPDATE goagent_durable_lease SET expires_at = now() + $4 * interval '1 millisecond'
WHERE ns = $1 AND id = $2 AND token = $3`, h.ns, h.id, h.token, s.root.cfg.LeaseTTL.Milliseconds())
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				return fmt.Errorf("%w：会话 %q 中 %q", durable.ErrLeaseLost, id, h.id)
			}
		}
		if expected == 0 {
			if _, err := tx.Exec(ctx, `INSERT INTO goagent_session (id, last_seq) VALUES ($1, 0) ON CONFLICT (id) DO NOTHING`, id); err != nil {
				return err
			}
		}
		tag, err := tx.Exec(ctx, `UPDATE goagent_session SET last_seq = $3 WHERE id = $1 AND last_seq = $2`, id, expected, last)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			cur, err := lastSeq(ctx, tx, id)
			if err != nil {
				return err
			}
			return conflict(expected, cur)
		}
		_, err = tx.Exec(ctx, `INSERT INTO goagent_session_event (session_id, seq, type, event)
SELECT $1, * FROM unnest($2::bigint[], $3::text[], $4::bytea[])`, id, seqs, types, data)
		return err
	})
	if err != nil {
		return 0, err
	}
	return last, nil
}

// Read 实现 session.Store。
func (s *SessionStore) Read(ctx context.Context, id string, after int64) ([]session.Event, error) {
	rows, err := s.root.cfg.Pool.Query(ctx, `SELECT event FROM goagent_session_event WHERE session_id = $1 AND seq > $2 ORDER BY seq`, id, after)
	if err != nil {
		return nil, err
	}
	raws, err := pgx.CollectRows(rows, pgx.RowTo[[]byte])
	if err != nil {
		return nil, err
	}
	if len(raws) == 0 {
		var exists bool
		if err := s.root.cfg.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM goagent_session WHERE id = $1)`, id).Scan(&exists); err != nil {
			return nil, err
		}
		if !exists {
			return nil, session.ErrNotFound
		}
	}
	events := make([]session.Event, len(raws))
	for i, raw := range raws {
		if err := json.Unmarshal(raw, &events[i]); err != nil {
			return nil, fmt.Errorf("会话 %q 的事件已损坏：%w", id, err)
		}
	}
	return events, nil
}

// List 实现 session.Store，按字节序（与 sort.Strings 相同）返回。
func (s *SessionStore) List(ctx context.Context) ([]string, error) {
	rows, err := s.root.cfg.Pool.Query(ctx, `SELECT id FROM goagent_session ORDER BY id COLLATE "C"`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// Fork 实现 session.Store。复制期间对来源会话行取共享锁，复制的是一个完整的前缀。
func (s *SessionStore) Fork(ctx context.Context, src, dst string, upTo int64) error {
	if err := validSessionID(dst); err != nil {
		return err
	}
	return s.writeTx(ctx, func(tx pgx.Tx) error {
		var last int64
		err := tx.QueryRow(ctx, `SELECT last_seq FROM goagent_session WHERE id = $1 FOR SHARE`, src).Scan(&last)
		if errors.Is(err, pgx.ErrNoRows) {
			return session.ErrNotFound
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
			return session.ErrExists
		}
		_, err = tx.Exec(ctx, `INSERT INTO goagent_session_event (session_id, seq, type, event)
SELECT $1, seq, type, event FROM goagent_session_event WHERE session_id = $2 AND seq <= $3`, dst, src, last)
		return err
	})
}
