// Package gormstore 是基于 gorm 的 durable.Store、durable.Scoped 与 session.Store（SessionStore）实现，
// 供已经用 gorm 连接 MySQL 8 或 PostgreSQL 的服务让多个实例共享持久运行与会话日志。
//
// 租约、进程内闸门与续约由 goagent 的 core/durable/leasestore 实现，本包只提供它的后端（backend.go）。
// 表结构固定，按数据库分别公布为 SchemaMySQL、SchemaPostgres；要用自己的表时在自己的仓库实现 leasestore.Backend，不修改本包。
//
// 与 durable/pgstore 的差别：本包不用 PostgreSQL 的咨询锁，改用预置表 goagent_gorm_gate 的行锁，因此两种数据库写法一致；
// 也不设置 synchronous_commit，高可用部署须由数据库自身的默认配置保证已提交事务不在主备切换中丢失。
// 只用 PostgreSQL 且不需要 gorm 的服务用 pgstore。
//
// 前置条件：
//   - DB 直连主库，不能指向读副本或读写分离代理。
//   - MySQL 须为 8.0 及以上的 InnoDB，并保持默认的严格模式。
//   - LeaseTTL 须大于一次主备切换的时长。
//   - 表由宿主建好；使用 SessionStore 时另建会话日志的表。
package gormstore

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/kart-io/goagent/core/durable/leasestore"
)

// ErrClosed 表示 Store 已关闭，不再接受新的 Lock。
var ErrClosed = leasestore.ErrClosed

// Config 配置 Store。时长为 0 时使用 leasestore 的默认值，含义见 leasestore.Config。
type Config struct {
	// DB 承载加锁、读写与作用域操作，须直连主库；方言须为 mysql 或 postgres。
	DB *gorm.DB
	// RenewDB 是续约专用的连接；为 nil 时使用 DB，高峰期续约可能在连接池中排队。
	RenewDB       *gorm.DB
	LeaseTTL      time.Duration
	RenewInterval time.Duration
	PollInterval  time.Duration
	// OpTimeout 同时是写事务的时限，默认 3 秒。
	OpTimeout    time.Duration
	ExpiredGrace time.Duration
	// Owner 写入租约行，仅用于诊断，默认 hostname:pid。
	Owner string
	// OnError 接收后台与解锁中被吞掉的错误，可为 nil。
	OnError func(op string, err error)
}

// Store 实现 durable.Store 与 durable.Scoped。
type Store struct {
	*leasestore.Store
	backend *backend
}

// New 校验表结构版本并启动后台续约。
func New(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.DB == nil {
		return nil, fmt.Errorf("gormstore 需要 DB")
	}
	if cfg.RenewDB == nil {
		cfg.RenewDB = cfg.DB
	}
	if cfg.OpTimeout == 0 {
		cfg.OpTimeout = 3 * time.Second
	}
	name := cfg.DB.Name()
	d, ok := dialects[name]
	if !ok || cfg.RenewDB.Name() != name {
		return nil, fmt.Errorf("gormstore 不支持方言 %q（RenewDB 为 %q），只支持 mysql 与 postgres", name, cfg.RenewDB.Name())
	}
	if err := checkVersion(ctx, cfg.DB, "goagent_gorm_schema", SchemaVersion); err != nil {
		return nil, err
	}
	b := &backend{db: cfg.DB, d: d, opTimeout: cfg.OpTimeout}
	inner, err := leasestore.New(leasestore.Config{
		Backend:       b,
		RenewBackend:  &backend{db: cfg.RenewDB, d: d, opTimeout: cfg.OpTimeout},
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
	return &Store{Store: inner, backend: b}, nil
}

func checkVersion(ctx context.Context, db *gorm.DB, table string, want int) error {
	var version int
	if err := db.WithContext(ctx).Raw(`SELECT version FROM ` + table).Row().Scan(&version); err != nil {
		return fmt.Errorf("读取 %s 的表结构版本失败：%w", table, err)
	}
	if version != want {
		return fmt.Errorf("%s 的表结构版本为 %d，需要 %d", table, version, want)
	}
	return nil
}

// SessionStore 实现 session.Store 与 session.Deleter，与创建它的 Store 共享连接、租约与续约，
// 关闭 Store 即停止它的锁。语义见 leasestore.SessionStore。
type SessionStore struct {
	*leasestore.SessionStore
}

// NewSessionStore 校验会话日志的表结构版本。
func NewSessionStore(ctx context.Context, root *Store) (*SessionStore, error) {
	if root == nil {
		return nil, fmt.Errorf("gormstore.NewSessionStore 需要 Store")
	}
	if err := checkVersion(ctx, root.backend.db, "goagent_gorm_session_schema", SessionSchemaVersion); err != nil {
		return nil, err
	}
	inner, err := leasestore.NewSessionStore(root.Store, root.backend)
	if err != nil {
		return nil, err
	}
	return &SessionStore{SessionStore: inner}, nil
}
