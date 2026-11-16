package pgvector

import (
	"context"
	crand "crypto/rand"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/kart-io/goagent/memory"
	"github.com/kart-io/goagent/memory/memorytest"
)

// 需要装有 pgvector 的 Postgres，且连接角色可执行 CREATE EXTENSION 与 CREATE DATABASE：
// GOAGENT_PGVECTOR_DSN=postgres://postgres:test@127.0.0.1:55434/postgres?sslmode=disable
const dsnEnv = "GOAGENT_PGVECTOR_DSN"

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skipf("未设置 %s，跳过 pgvector 测试", dsnEnv)
	}
	return dsn
}

func newPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

// newDatabase 建一个独立数据库，返回其连接串；ext 为 true 时安装 vector 扩展。
func newDatabase(t *testing.T, ext bool) string {
	t.Helper()
	dsn := testDSN(t)
	ctx := context.Background()
	admin := newPool(t, dsn)
	name := "t_" + strings.ToLower(crand.Text()[:12])
	_, err := admin.Exec(ctx, `CREATE DATABASE `+name)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), `DROP DATABASE `+name+` WITH (FORCE)`) })
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	u.Path = "/" + name
	if ext {
		pool := newPool(t, u.String())
		_, err = pool.Exec(ctx, `CREATE EXTENSION vector`)
		require.NoError(t, err)
	}
	return u.String()
}

func TestConformance(t *testing.T) {
	dsn := newDatabase(t, true)
	pool := newPool(t, dsn)
	ctx := context.Background()
	_, err := pool.Exec(ctx, Schema)
	require.NoError(t, err)
	memorytest.RunIndex(t, func(t *testing.T) memory.Index {
		_, err := pool.Exec(ctx, `TRUNCATE goagent_memory_vectors`)
		require.NoError(t, err)
		x, err := New(ctx, Config{Pool: pool})
		require.NoError(t, err)
		return x
	})
}

func TestNewWithoutExtension(t *testing.T) {
	pool := newPool(t, newDatabase(t, false))
	_, err := New(context.Background(), Config{Pool: pool})
	require.ErrorContains(t, err, "CREATE EXTENSION vector")
}

func TestSchemaVersionMismatch(t *testing.T) {
	pool := newPool(t, newDatabase(t, true))
	ctx := context.Background()
	_, err := New(ctx, Config{Pool: pool})
	require.Error(t, err, "未建表")

	_, err = pool.Exec(ctx, Schema)
	require.NoError(t, err)
	_, err = New(ctx, Config{Pool: pool})
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `UPDATE goagent_memory_vectors_schema SET version = 99`)
	require.NoError(t, err)
	_, err = New(ctx, Config{Pool: pool})
	require.ErrorContains(t, err, "表结构版本")
}

func TestNewRequiresPool(t *testing.T) {
	_, err := New(context.Background(), Config{})
	require.Error(t, err)
}

func TestSharedAcrossInstances(t *testing.T) {
	dsn := newDatabase(t, true)
	ctx := context.Background()
	a, b := newPool(t, dsn), newPool(t, dsn)
	_, err := a.Exec(ctx, Schema)
	require.NoError(t, err)
	xa, err := New(ctx, Config{Pool: a})
	require.NoError(t, err)
	xb, err := New(ctx, Config{Pool: b})
	require.NoError(t, err)

	ns := []string{"t", "alice"}
	require.NoError(t, xa.Upsert(ctx, ns, "m1", []float32{1, 0}))
	hits, err := xb.Search(ctx, ns, []float32{1, 0}, 3)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, "m1", hits[0].ID)
	require.InDelta(t, 1, hits[0].Score, 1e-6)
}
