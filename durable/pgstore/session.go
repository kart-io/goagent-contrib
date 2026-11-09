package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/kart-io/goagent/constants"
	"github.com/kart-io/goagent/core/session"
	agentErrors "github.com/kart-io/goagent/errors"
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
// READ COMMITTED 让行锁等待后的序号和令牌检查读取已提交版本，保持冲突错误与复制前缀语义。
func (s *SessionStore) writeTx(ctx context.Context, fn func(pgx.Tx) error) error {
	return pgx.BeginTxFunc(ctx, s.root.cfg.Pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
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
		if ev.Type != constants.SessionEventBoundarySnapshot {
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
			return nil, fmt.Errorf("%w：本进程未持有会话 %q 中 %q 的锁", agentErrors.ErrLeaseLost, id, name)
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
	return fmt.Errorf("%w：期望序号 %d，当前 %d", agentErrors.ErrSessionConflict, expected, cur)
}

// Append 实现 session.Store。事件按原样的 JSON 存为 bytea：jsonb 会改写 Data 的键序与数字写法。
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
	plan, err := s.planAppend(session.AppendBatch{ID: id, Expected: expected, Events: events})
	if err != nil {
		return 0, err
	}
	if err := s.writeTx(ctx, func(tx pgx.Tx) error { return plan.apply(ctx, tx, s.root.cfg.LeaseTTL) }); err != nil {
		return 0, err
	}
	return plan.last, nil
}

// appendPlan 是准备好写入事务的一次追加。
type appendPlan struct {
	id       string
	expected int64
	last     int64
	holds    []*hold
	seqs     []int64
	types    []string
	data     [][]byte
}

func (s *SessionStore) planAppend(batch session.AppendBatch) (*appendPlan, error) {
	holds, err := s.fences(batch.ID, batch.Events)
	if err != nil {
		return nil, err
	}
	// 同一会话可能包含多个快照，按租约名称取行锁，避免批次内快照顺序相反时死锁。
	slices.SortFunc(holds, func(a, b *hold) int { return strings.Compare(a.id, b.id) })
	now := time.Now().UTC()
	n := len(batch.Events)
	plan := &appendPlan{id: batch.ID, expected: batch.Expected, last: batch.Expected + int64(n), holds: holds,
		seqs: make([]int64, n), types: make([]string, n), data: make([][]byte, n)}
	for i, ev := range batch.Events {
		ev.Seq = batch.Expected + int64(i) + 1
		if ev.Time.IsZero() {
			ev.Time = now
		}
		plan.seqs[i], plan.types[i] = ev.Seq, ev.Type
		if plan.data[i], err = json.Marshal(ev); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

// apply 在事务内校验租约与序号并写入事件。会话行的条件更新持有行锁到提交，并发追加按期望序号串行判定。
func (p *appendPlan) apply(ctx context.Context, tx pgx.Tx, ttl time.Duration) error {
	for _, h := range p.holds {
		// 只校验令牌，同 Store.Save：接管必然改写令牌；UPDATE 持有租约行锁直到提交，同时顺带续约。
		tag, err := tx.Exec(ctx, `UPDATE goagent_durable_lease SET expires_at = now() + $4 * interval '1 millisecond'
WHERE ns = $1 AND id = $2 AND token = $3`, h.ns, h.id, h.token, ttl.Milliseconds())
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("%w：会话 %q 中 %q", agentErrors.ErrLeaseLost, p.id, h.id)
		}
	}
	if len(p.seqs) == 0 {
		// 空批次的比较也须保持到整个事务提交。不存在的会话用临时行
		// 取得唯一键锁，之后删除；并发 Append/Fork 仍须等待提交。
		created := false
		if p.expected == 0 {
			tag, err := tx.Exec(ctx, `INSERT INTO goagent_session (id, last_seq) VALUES ($1, 0) ON CONFLICT (id) DO NOTHING`, p.id)
			if err != nil {
				return err
			}
			created = tag.RowsAffected() == 1
		}
		var cur int64
		err := tx.QueryRow(ctx, `SELECT last_seq FROM goagent_session WHERE id = $1 FOR UPDATE`, p.id).Scan(&cur)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if cur != p.expected {
			return conflict(p.expected, cur)
		}
		if created {
			_, err = tx.Exec(ctx, `DELETE FROM goagent_session WHERE id = $1`, p.id)
			return err
		}
		return nil
	}
	if p.expected == 0 {
		if _, err := tx.Exec(ctx, `INSERT INTO goagent_session (id, last_seq) VALUES ($1, 0) ON CONFLICT (id) DO NOTHING`, p.id); err != nil {
			return err
		}
	}
	tag, err := tx.Exec(ctx, `UPDATE goagent_session SET last_seq = $3 WHERE id = $1 AND last_seq = $2`, p.id, p.expected, p.last)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		cur, err := lastSeq(ctx, tx, p.id)
		if err != nil {
			return err
		}
		return conflict(p.expected, cur)
	}
	_, err = tx.Exec(ctx, `INSERT INTO goagent_session_event (session_id, seq, type, event)
SELECT $1, * FROM unnest($2::bigint[], $3::text[], $4::bytea[])`, p.id, p.seqs, p.types, p.data)
	return err
}

// AppendAll 实现 session.Store：在一个事务内应用全部批次。按会话 ID 排序写入，避免并发批量追加互相等待行锁。
func (s *SessionStore) AppendAll(ctx context.Context, batches ...session.AppendBatch) ([]int64, error) {
	if err := session.ValidateBatches(batches); err != nil {
		return nil, err
	}
	if len(batches) == 0 {
		return nil, nil
	}
	plans := make([]*appendPlan, len(batches))
	lasts := make([]int64, len(batches))
	for i, batch := range batches {
		plan, err := s.planAppend(batch)
		if err != nil {
			return nil, err
		}
		plans[i], lasts[i] = plan, plan.last
	}
	slices.SortFunc(plans, func(a, b *appendPlan) int { return strings.Compare(a.id, b.id) })
	err := s.writeTx(ctx, func(tx pgx.Tx) error {
		for _, plan := range plans {
			if err := plan.apply(ctx, tx, s.root.cfg.LeaseTTL); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return lasts, nil
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
			return nil, agentErrors.ErrSessionNotFound
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
