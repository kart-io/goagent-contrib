package gormstore

import (
	"context"
	crand "crypto/rand"
	"os"
	"strings"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/kart-io/goagent/core/durable"
	"github.com/kart-io/goagent/core/durable/leasestore"
	"github.com/kart-io/goagent/core/durable/leasestore/backendtest"
	"github.com/kart-io/goagent/core/session"
)

// testDB 是一种待测数据库：DSN 来自环境变量，isolate 建一个独立的库（或 schema）并返回指向它的 DSN。
type testDB struct {
	name, env string
	schema    string
	open      func(dsn string) gorm.Dialector
	isolate   func(t *testing.T, admin *gorm.DB, dsn, name string) string
}

// GOAGENT_GORMSTORE_MYSQL_DSN=root:test@tcp(127.0.0.1:53306)/mysql
// GOAGENT_GORMSTORE_PG_DSN=postgres://postgres:test@127.0.0.1:55435/postgres?sslmode=disable
var testDBs = []testDB{
	{
		name: "mysql", env: "GOAGENT_GORMSTORE_MYSQL_DSN", schema: SchemaMySQL + SessionSchemaMySQL, open: mysql.Open,
		isolate: func(t *testing.T, admin *gorm.DB, dsn, name string) string {
			require.NoError(t, admin.Exec(`CREATE DATABASE `+name).Error)
			t.Cleanup(func() { admin.Exec(`DROP DATABASE ` + name) })
			cfg, err := mysqldriver.ParseDSN(dsn)
			require.NoError(t, err)
			cfg.DBName = name
			return cfg.FormatDSN()
		},
	},
	{
		name: "postgres", env: "GOAGENT_GORMSTORE_PG_DSN", schema: SchemaPostgres + SessionSchemaPostgres, open: postgres.Open,
		isolate: func(t *testing.T, admin *gorm.DB, dsn, name string) string {
			require.NoError(t, admin.Exec(`CREATE SCHEMA `+name).Error)
			t.Cleanup(func() { admin.Exec(`DROP SCHEMA ` + name + ` CASCADE`) })
			sep := "?"
			if strings.Contains(dsn, "?") {
				sep = "&"
			}
			return dsn + sep + "search_path=" + name
		},
	},
}

// eachDB 对每种配置了 DSN 的数据库运行 fn。
func eachDB(t *testing.T, fn func(t *testing.T, td testDB)) {
	for _, td := range testDBs {
		t.Run(td.name, func(t *testing.T) {
			if os.Getenv(td.env) == "" {
				t.Skipf("未设置 %s，跳过 %s 测试", td.env, td.name)
			}
			fn(t, td)
		})
	}
}

func (td testDB) connect(t *testing.T, dsn string) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(td.open(dsn), &gorm.Config{Logger: logger.Discard})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	return db
}

// newDB 返回一个建好全部表的空库。
func (td testDB) newDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv(td.env)
	db := td.connect(t, td.isolate(t, td.connect(t, dsn), dsn, "t_"+strings.ToLower(crand.Text()[:12])))
	for _, stmt := range strings.Split(td.schema, ";\n") {
		if strings.TrimSpace(stmt) != "" {
			require.NoError(t, db.Exec(stmt).Error)
		}
	}
	return db
}

func (td testDB) newBackend(t *testing.T) *backend {
	return &backend{db: td.newDB(t), d: dialects[td.name], opTimeout: 3 * time.Second}
}

func TestConformance(t *testing.T) {
	eachDB(t, func(t *testing.T, td testDB) {
		backendtest.Run(t, func(t *testing.T) backendtest.Backends {
			b := td.newBackend(t)
			return backendtest.Backends{Backend: b, Session: b, Expire: func(t *testing.T, ns, id string) {
				require.NoError(t, b.db.Exec(`UPDATE goagent_gorm_lease SET expires_at = `+b.d.before+` WHERE ns = ? AND id = ?`, 1000, ns, id).Error)
			}}
		})
	})
}

// 引擎在结果不确定的获取之后用 ReleaseLease 补偿：它必须等那次获取的事务结束，并删掉其后提交的租约。
func TestReleaseWaitsForUnfinishedAcquire(t *testing.T) {
	eachDB(t, func(t *testing.T, td testDB) {
		b := td.newBackend(t)
		l := leasestore.Lease{NS: "ns", ID: "k", Token: "tok"}
		acquiring := b.db.Begin(readCommitted)
		require.NoError(t, lockGate(acquiring, leaseSlot(l.NS, l.ID), "FOR UPDATE"))
		require.NoError(t, acquiring.Exec(`INSERT INTO goagent_gorm_lease (ns, id, token, owner, expires_at) VALUES (?, ?, ?, 'o', `+b.d.after+`)`,
			l.NS, l.ID, l.Token, 60000).Error)

		released := make(chan error, 1)
		go func() { released <- b.ReleaseLease(context.Background(), l) }()
		select {
		case err := <-released:
			t.Fatalf("ReleaseLease 没有等待未结束的获取：%v", err)
		case <-time.After(300 * time.Millisecond):
		}
		require.NoError(t, acquiring.Commit().Error)
		require.NoError(t, <-released)

		var left int
		require.NoError(t, b.db.Raw(`SELECT count(*) FROM goagent_gorm_lease`).Row().Scan(&left))
		require.Zero(t, left)
	})
}

func TestStoreRoundTrip(t *testing.T) {
	eachDB(t, func(t *testing.T, td testDB) {
		ctx := context.Background()
		db := td.newDB(t)
		store, err := New(ctx, Config{DB: db})
		require.NoError(t, err)
		t.Cleanup(func() { _ = store.Close(context.Background()) })
		var _ durable.Scoped = store

		unlock, err := store.Lock(ctx, "run")
		require.NoError(t, err)
		require.NoError(t, store.Save(ctx, "run", &durable.Snapshot{RunID: "r1"}))
		got, err := store.Load(ctx, "run")
		require.NoError(t, err)
		require.Equal(t, "r1", got.RunID)
		require.NoError(t, unlock())

		sessions, err := NewSessionStore(ctx, store)
		require.NoError(t, err)
		var _ session.Deleter = sessions
		ids, err := sessions.List(ctx)
		require.NoError(t, err)
		require.Empty(t, ids)
	})
}

func TestSchemaVersionMismatch(t *testing.T) {
	eachDB(t, func(t *testing.T, td testDB) {
		ctx := context.Background()
		db := td.newDB(t)
		require.NoError(t, db.Exec(`UPDATE goagent_gorm_session_schema SET version = 99`).Error)
		store, err := New(ctx, Config{DB: db})
		require.NoError(t, err)
		t.Cleanup(func() { _ = store.Close(context.Background()) })
		_, err = NewSessionStore(ctx, store)
		require.ErrorContains(t, err, "99")

		require.NoError(t, db.Exec(`UPDATE goagent_gorm_schema SET version = 99`).Error)
		_, err = New(ctx, Config{DB: db})
		require.ErrorContains(t, err, "99")
	})
}

func TestNewRejectsMissingDB(t *testing.T) {
	_, err := New(context.Background(), Config{})
	require.Error(t, err)
}
