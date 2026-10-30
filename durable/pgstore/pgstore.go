// Package pgstore 是基于 Postgres 的 durable.Store 与 durable.Scoped 实现，供多个实例共享持久运行。
//
// 锁实现为带过期时间的租约：后台批量续约，持有者崩溃后租约在 LeaseTTL 后过期，其他实例即可接管。
// 每次写入在同一事务内校验租约令牌，被接管后的旧持有者写入返回 durable.ErrLeaseLost。
// 同一进程内每个 ID 另有一道闸门，持有到该持有者解锁为止，旧持有者不会借用新持有者的令牌。
//
// 前置条件：
//   - Pool 直连主库，不能指向读副本或读写分离代理；经过 pgbouncer 事务模式时须关闭预编译语句缓存。
//   - 高可用部署须同步复制并隔离旧主；Save 与 RemoveScope 的墓碑事务会 SET LOCAL synchronous_commit = on。
//     租约的获取、续约与删除不强制同步：切换中丢失只会让某个持有者的写入因令牌不符被拒，不会出现两个可写的持有者。
//   - LeaseTTL 须大于一次主备切换的时长。
//   - 表结构见 Schema，由宿主建表。
package pgstore

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kart-io/goagent/core/durable"
)

// ErrClosed 表示 Store 已关闭，不再接受新的 Lock。
var ErrClosed = errors.New("pgstore 已关闭")

// Config 配置 Store。时长为 0 时使用默认值。
type Config struct {
	// Pool 承载加锁、读写与作用域操作，须直连主库。
	Pool *pgxpool.Pool
	// RenewPool 是续约专用的小连接池；为 nil 时使用 Pool，高峰期续约可能在连接池中排队。
	RenewPool *pgxpool.Pool
	// LeaseTTL 默认 30s，须大于主备切换时长。
	LeaseTTL time.Duration
	// RenewInterval 默认 LeaseTTL/3。
	RenewInterval time.Duration
	// PollInterval 默认 50ms，叠加最多一倍的随机抖动；只用于等待其他实例释放。
	PollInterval time.Duration
	// OpTimeout 是获取租约、解锁、续约等内部操作的超时，默认 3s；写事务的 statement_timeout 也取此值。
	OpTimeout time.Duration
	// ExpiredGrace 是过期租约保留多久后被清理，默认 10*LeaseTTL。
	ExpiredGrace time.Duration
	// Owner 写入租约行，仅用于诊断，默认 hostname:pid。
	Owner string
	// OnError 接收后台与解锁中被吞掉的错误（续约、清理、解锁时删除租约），可为 nil。
	OnError func(op string, err error)
}

// Store 实现 durable.Store 与 durable.Scoped。
type Store struct {
	cfg Config

	mu      sync.Mutex
	closed  bool
	gates   map[string]*gate
	owners  map[string]*hold
	holders sync.WaitGroup

	stop      chan struct{}
	renewDone chan struct{}
}

// gate 是进程内某个 (ns, id) 的闸门，refs 计数等待者与持有者，归零时从表中删除。
type gate struct {
	sem  chan struct{}
	refs int
}

// hold 是本进程持有的一个租约；renew 由 mu 保护，置 false 后不再续约。
type hold struct {
	ns, id, token string
	renew         bool
}

var (
	_ durable.Store  = (*Store)(nil)
	_ durable.Scoped = (*Store)(nil)
)

// New 校验表结构版本并启动后台续约。
func New(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.Pool == nil {
		return nil, fmt.Errorf("pgstore 需要 Pool")
	}
	if cfg.RenewPool == nil {
		cfg.RenewPool = cfg.Pool
	}
	if cfg.LeaseTTL == 0 {
		cfg.LeaseTTL = 30 * time.Second
	}
	if cfg.RenewInterval == 0 {
		cfg.RenewInterval = cfg.LeaseTTL / 3
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 50 * time.Millisecond
	}
	if cfg.OpTimeout == 0 {
		cfg.OpTimeout = 3 * time.Second
	}
	if cfg.ExpiredGrace == 0 {
		cfg.ExpiredGrace = 10 * cfg.LeaseTTL
	}
	if cfg.LeaseTTL < 0 || cfg.RenewInterval <= 0 || cfg.RenewInterval >= cfg.LeaseTTL || cfg.PollInterval < 0 || cfg.OpTimeout < 0 || cfg.ExpiredGrace < 0 {
		return nil, fmt.Errorf("pgstore 配置无效：须满足 0 < RenewInterval < LeaseTTL，其余时长非负")
	}
	if cfg.Owner == "" {
		host, _ := os.Hostname()
		cfg.Owner = fmt.Sprintf("%s:%d", host, os.Getpid())
	}
	var version int
	if err := cfg.Pool.QueryRow(ctx, `SELECT version FROM goagent_durable_schema`).Scan(&version); err != nil {
		return nil, fmt.Errorf("读取 pgstore 表结构版本失败：%w", err)
	}
	if version != SchemaVersion {
		return nil, fmt.Errorf("pgstore 表结构版本为 %d，需要 %d", version, SchemaVersion)
	}
	s := &Store{cfg: cfg, gates: map[string]*gate{}, owners: map[string]*hold{}, stop: make(chan struct{}), renewDone: make(chan struct{})}
	go s.background()
	return s, nil
}

func key(ns, id string) string { return ns + "\x00" + id }

// scopeNS 给作用域名加前缀，与根命名空间 "" 区分。
func scopeNS(name string) string { return "s:" + name }

func (s *Store) report(op string, err error) {
	if err != nil && s.cfg.OnError != nil {
		s.cfg.OnError(op, err)
	}
}

func (s *Store) opContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.cfg.OpTimeout)
}

func lockUnavailable(ctx context.Context) error {
	return fmt.Errorf("%w: %w", durable.ErrLockUnavailable, ctx.Err())
}

// Lock 实现 durable.Store。
func (s *Store) Lock(ctx context.Context, id string) (func() error, error) {
	return s.lock(ctx, "", id)
}

// Load 实现 durable.Store。
func (s *Store) Load(ctx context.Context, id string) (*durable.Snapshot, error) {
	return s.load(ctx, "", id)
}

// Save 实现 durable.Store。
func (s *Store) Save(ctx context.Context, id string, snap *durable.Snapshot) error {
	return s.save(ctx, "", id, snap)
}

func (s *Store) enterGate(k string) *gate {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.gates[k]
	if g == nil {
		g = &gate{sem: make(chan struct{}, 1)}
		s.gates[k] = g
	}
	g.refs++
	return g
}

func (s *Store) leaveGate(k string, g *gate, held bool) {
	if held {
		<-g.sem
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if g.refs--; g.refs == 0 {
		delete(s.gates, k)
	}
}

func (s *Store) lock(ctx context.Context, ns, id string) (func() error, error) {
	s.mu.Lock()
	closed := s.closed
	if !closed {
		s.holders.Add(1)
	}
	s.mu.Unlock()
	if closed {
		return nil, ErrClosed
	}
	unlock, err := s.acquire(ctx, ns, id)
	if err != nil {
		s.holders.Done()
	}
	return unlock, err
}

func (s *Store) acquire(ctx context.Context, ns, id string) (func() error, error) {
	k := key(ns, id)
	g := s.enterGate(k)
	select {
	case g.sem <- struct{}{}:
	case <-ctx.Done():
		s.leaveGate(k, g, false)
		return nil, lockUnavailable(ctx)
	}
	for {
		token := crand.Text()
		ok, err := s.tryAcquire(ns, id, token)
		if err != nil {
			if !errors.Is(err, durable.ErrScopeRemoved) {
				// 提交结果不确定时租约可能已写入，按自己的令牌删除，避免留下无人持有的租约。
				s.deleteLease(ns, id, token)
			}
			s.leaveGate(k, g, true)
			return nil, err
		}
		if ok {
			h := &hold{ns: ns, id: id, token: token, renew: true}
			s.mu.Lock()
			s.owners[k] = h
			s.mu.Unlock()
			return s.unlocker(k, g, h), nil
		}
		wait := s.cfg.PollInterval + rand.N(s.cfg.PollInterval+1)
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			s.leaveGate(k, g, true)
			return nil, lockUnavailable(ctx)
		}
	}
}

// tryAcquire 使用内部超时而非调用方 ctx，避免调用方取消导致提交结果不确定。
func (s *Store) tryAcquire(ns, id, token string) (bool, error) {
	ctx, cancel := s.opContext()
	defer cancel()
	var removed, busy bool
	err := s.cfg.Pool.QueryRow(ctx, `SELECT
  EXISTS (SELECT 1 FROM goagent_durable_scope WHERE ns = $1),
  COALESCE((SELECT expires_at >= now() FROM goagent_durable_lease WHERE ns = $1 AND id = $2), false)`, ns, id).Scan(&removed, &busy)
	if err != nil {
		return false, err
	}
	if removed {
		return false, durable.ErrScopeRemoved
	}
	if busy {
		return false, nil
	}
	acquired := false
	err = pgx.BeginFunc(ctx, s.cfg.Pool, func(tx pgx.Tx) error {
		if err := checkScope(ctx, tx, ns); err != nil {
			return err
		}
		var got string
		err := tx.QueryRow(ctx, `INSERT INTO goagent_durable_lease (ns, id, token, owner, expires_at)
VALUES ($1, $2, $3, $4, now() + $5 * interval '1 millisecond')
ON CONFLICT (ns, id) DO UPDATE SET token = EXCLUDED.token, owner = EXCLUDED.owner, expires_at = EXCLUDED.expires_at
WHERE goagent_durable_lease.expires_at < now()
RETURNING token`, ns, id, token, s.cfg.Owner, s.cfg.LeaseTTL.Milliseconds()).Scan(&got)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		acquired = err == nil
		return err
	})
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
		err = durable.ErrScopeRemoved
	}
	return err
}

func (s *Store) deleteLease(ns, id, token string) {
	ctx, cancel := s.opContext()
	defer cancel()
	_, err := s.cfg.Pool.Exec(ctx, `DELETE FROM goagent_durable_lease WHERE ns = $1 AND id = $2 AND token = $3`, ns, id, token)
	s.report("delete_lease", err)
}

// unlocker 返回的函数可重复调用。租约已被接管或删除失败时返回 nil，经 OnError 报告：此前的写入都受令牌保护。
func (s *Store) unlocker(k string, g *gate, h *hold) func() error {
	var once sync.Once
	return func() error {
		once.Do(func() {
			s.mu.Lock()
			if s.owners[k] == h {
				delete(s.owners, k)
			}
			s.mu.Unlock()
			s.deleteLease(h.ns, h.id, h.token)
			s.leaveGate(k, g, true)
			s.holders.Done()
		})
		return nil
	}
}

func (s *Store) load(ctx context.Context, ns, id string) (*durable.Snapshot, error) {
	var removed bool
	var data []byte
	err := s.cfg.Pool.QueryRow(ctx, `SELECT
  EXISTS (SELECT 1 FROM goagent_durable_scope WHERE ns = $1),
  (SELECT data FROM goagent_durable_snapshot WHERE ns = $1 AND id = $2)`, ns, id).Scan(&removed, &data)
	if err != nil {
		return nil, err
	}
	if removed {
		return nil, durable.ErrScopeRemoved
	}
	if data == nil {
		return nil, fmt.Errorf("持久快照 %q 不存在：%w", id, os.ErrNotExist)
	}
	var snap durable.Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("持久快照 %q 已损坏：%w", id, err)
	}
	return &snap, nil
}

func (s *Store) save(ctx context.Context, ns, id string, snap *durable.Snapshot) error {
	s.mu.Lock()
	h := s.owners[key(ns, id)]
	s.mu.Unlock()
	if h == nil {
		return fmt.Errorf("%w：本进程未持有 %q 的锁", durable.ErrLeaseLost, id)
	}
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, s.cfg.Pool, func(tx pgx.Tx) error {
		ms := fmt.Sprint(s.cfg.OpTimeout.Milliseconds())
		if _, err := tx.Exec(ctx, `SELECT set_config('synchronous_commit', 'on', true), set_config('lock_timeout', $1, true), set_config('statement_timeout', $1, true)`, ms); err != nil {
			return err
		}
		if err := checkScope(ctx, tx, ns); err != nil {
			return err
		}
		// 只校验令牌：接管必然改写令牌，过期但未被接管时写入是安全的；UPDATE 持有租约行锁直到提交，同时顺带续约。
		tag, err := tx.Exec(ctx, `WITH l AS (
  UPDATE goagent_durable_lease SET expires_at = now() + $5 * interval '1 millisecond'
  WHERE ns = $1 AND id = $2 AND token = $3 RETURNING 1
)
INSERT INTO goagent_durable_snapshot (ns, id, data, updated_at)
SELECT $1, $2, $4, now() WHERE EXISTS (SELECT 1 FROM l)
ON CONFLICT (ns, id) DO UPDATE SET data = EXCLUDED.data, updated_at = EXCLUDED.updated_at`,
			ns, id, h.token, data, s.cfg.LeaseTTL.Milliseconds())
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("%w：%q", durable.ErrLeaseLost, id)
		}
		return nil
	})
}

// background 定期批量续约本进程持有的租约，并清理过期已久的租约行。
func (s *Store) background() {
	defer close(s.renewDone)
	ticker := time.NewTicker(s.cfg.RenewInterval)
	defer ticker.Stop()
	lastCleanup := time.Now()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
		}
		s.renew()
		if time.Since(lastCleanup) >= s.cfg.ExpiredGrace {
			lastCleanup = time.Now()
			s.cleanupExpired()
		}
	}
}

func (s *Store) renew() {
	var nss, ids, tokens []string
	s.mu.Lock()
	for _, h := range s.owners {
		if h.renew {
			nss, ids, tokens = append(nss, h.ns), append(ids, h.id), append(tokens, h.token)
		}
	}
	s.mu.Unlock()
	if len(tokens) == 0 {
		return
	}
	ctx, cancel := s.opContext()
	defer cancel()
	_, err := s.cfg.RenewPool.Exec(ctx, `UPDATE goagent_durable_lease AS l
SET expires_at = now() + $4 * interval '1 millisecond'
FROM unnest($1::text[], $2::text[], $3::text[]) AS h(ns, id, token)
WHERE l.ns = h.ns AND l.id = h.id AND l.token = h.token`, nss, ids, tokens, s.cfg.LeaseTTL.Milliseconds())
	s.report("renew", err)
}

// cleanupExpired 删除过期已久的租约：持久操作 ID 由执行键派生，崩溃留下的租约可能永远不会再被获取。
// 删除是安全的，旧持有者写入时因令牌不存在而被拒。
func (s *Store) cleanupExpired() {
	ctx, cancel := s.opContext()
	defer cancel()
	_, err := s.cfg.RenewPool.Exec(ctx, `DELETE FROM goagent_durable_lease WHERE expires_at < now() - $1 * interval '1 millisecond'`, s.cfg.ExpiredGrace.Milliseconds())
	s.report("cleanup", err)
}

// Close 拒绝新的 Lock，在 ctx 截止前等待现有持有者解锁，然后停止续约并释放剩余租约。
// 不关闭 Pool。超时返回 ctx 错误，此时仍在运行的持有者之后的写入可能因租约被接管而失败。
func (s *Store) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	done := make(chan struct{})
	go func() {
		s.holders.Wait()
		close(done)
	}()
	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = ctx.Err()
	}
	close(s.stop)
	<-s.renewDone
	s.mu.Lock()
	remaining := make([]*hold, 0, len(s.owners))
	for _, h := range s.owners {
		h.renew = false
		remaining = append(remaining, h)
	}
	s.mu.Unlock()
	for _, h := range remaining {
		s.deleteLease(h.ns, h.id, h.token)
	}
	return err
}
