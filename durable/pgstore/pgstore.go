// Package pgstore 是基于 Postgres 的 durable.Store、durable.Scoped 与 session.Store（SessionStore）实现，供多个实例共享持久运行与会话日志。
//
// 租约、进程内闸门与续约由 goagent 的 core/durable/leasestore 实现，本包只提供它的 PostgreSQL 后端（backend.go），
// 表结构固定为 Schema。要用自己的表、ORM 或其他数据库时，在自己的仓库实现 leasestore.Backend，不修改本包。
//
// 锁实现为带过期时间的租约：后台批量续约，持有者崩溃后租约在 LeaseTTL 后过期，其他实例即可接管。
// 每次写入在同一事务内校验租约令牌，被接管后的旧持有者写入返回 durable.ErrLeaseLost。
// 同一进程内每个 ID 另有一道闸门，持有到该持有者解锁为止，旧持有者不会借用新持有者的令牌。
//
// 前置条件：
//   - Pool 直连主库，不能指向读副本或读写分离代理；经过 pgbouncer 事务模式时须关闭预编译语句缓存。
//   - 高可用部署须同步复制并隔离旧主；Save、SaveAll 与 RemoveScope 的墓碑事务会 SET LOCAL synchronous_commit = on。
//     租约的获取、续约与删除不强制同步：切换中丢失只会让某个持有者的写入因令牌不符被拒，不会出现两个可写的持有者。
//   - LeaseTTL 须大于一次主备切换的时长。
//   - 表结构见 Schema，由宿主建表；使用 SessionStore 时另建 SessionSchema。
package pgstore

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kart-io/goagent/core/durable/leasestore"
)

// ErrClosed 表示 Store 已关闭，不再接受新的 Lock。
var ErrClosed = leasestore.ErrClosed

// Config 配置 Store。时长为 0 时使用默认值。
type Config struct {
	// Pool 承载加锁、读写与作用域操作，须直连主库。
	Pool *pgxpool.Pool
	// RenewPool 是续约专用的小连接池；为 nil 时使用 Pool，高峰期续约可能在连接池中排队。
	RenewPool *pgxpool.Pool
	// LeaseTTL defaults to 30s and must exceed a primary failover. 0 selects
	// the default; it cannot mean "no expiry", because a lease that never
	// expires would keep a crashed holder's lock forever.
	LeaseTTL time.Duration
	// RenewInterval defaults to LeaseTTL/3. 0 selects the default; it cannot
	// mean "never renew", because a lease must be renewed before LeaseTTL
	// (New requires 0 < RenewInterval < LeaseTTL).
	RenewInterval time.Duration
	// PollInterval defaults to 50ms (plus up to the same again in jitter) and
	// only paces waiting for another instance to release a lock. 0 selects the
	// default; it cannot mean "unlimited", because a zero interval would
	// busy-loop on the database.
	PollInterval time.Duration
	// OpTimeout bounds internal operations (acquire, unlock, renew) and is also
	// the statement_timeout of write transactions; the default is 3s. 0 selects
	// the default; it cannot mean "no timeout", because an unbounded database
	// call would stall lease renewal and shutdown.
	OpTimeout time.Duration
	// ExpiredGrace is how long an expired lease row is kept before cleanup; the
	// default is 10*LeaseTTL. 0 selects the default; it cannot mean "keep
	// forever" or "clean immediately", because the former leaks rows and the
	// latter races a slow holder's renewal.
	ExpiredGrace time.Duration
	// Owner 写入租约行，仅用于诊断，默认 hostname:pid。
	Owner string
	// OnError 接收后台与解锁中被吞掉的错误（续约、清理、解锁时删除租约），可为 nil。
	OnError func(op string, err error)
}

// Store 实现 durable.Store 与 durable.Scoped。
type Store struct {
	*leasestore.Store
	cfg     Config
	backend *backend
}

// New 校验表结构版本并启动后台续约。
func New(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.Pool == nil {
		return nil, fmt.Errorf("pgstore 需要 Pool")
	}
	if cfg.RenewPool == nil {
		cfg.RenewPool = cfg.Pool
	}
	if cfg.OpTimeout == 0 {
		cfg.OpTimeout = 3 * time.Second
	}
	var version int
	if err := cfg.Pool.QueryRow(ctx, `SELECT version FROM goagent_durable_schema`).Scan(&version); err != nil {
		return nil, fmt.Errorf("读取 pgstore 表结构版本失败：%w", err)
	}
	if version != SchemaVersion {
		return nil, fmt.Errorf("pgstore 表结构版本为 %d，需要 %d", version, SchemaVersion)
	}
	b := &backend{pool: cfg.Pool, opTimeout: cfg.OpTimeout}
	inner, err := leasestore.New(leasestore.Config{
		Backend:       b,
		RenewBackend:  &backend{pool: cfg.RenewPool, opTimeout: cfg.OpTimeout},
		LeaseTTL:      cfg.LeaseTTL,
		RenewInterval: cfg.RenewInterval,
		PollInterval:  cfg.PollInterval,
		OpTimeout:     cfg.OpTimeout,
		ExpiredGrace:  cfg.ExpiredGrace,
		Owner:         cfg.Owner,
		OnError:       cfg.OnError,
	})
	if err != nil {
		return nil, err
	}
	return &Store{Store: inner, cfg: cfg, backend: b}, nil
}
