package pgstore

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/kart-io/goagent/core/durable"
	"github.com/kart-io/goagent/core/durable/storetest"
)

// 需要一个可写的 Postgres：GOAGENT_PGSTORE_DSN=postgres://postgres:test@127.0.0.1:55432/postgres?sslmode=disable
const dsnEnv = "GOAGENT_PGSTORE_DSN"

const testTTL = time.Second

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skipf("未设置 %s，跳过 Postgres 测试", dsnEnv)
	}
	return dsn
}

// newSchema 建一个独立 schema 并建表，返回绑定该 schema 的连接池工厂。
func newSchema(t *testing.T) (dsn, schema string) {
	t.Helper()
	dsn = testDSN(t)
	schema = "t_" + strings.ToLower(crand.Text()[:12])
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
	})
	_, err = admin.Exec(ctx, `CREATE SCHEMA `+schema)
	require.NoError(t, err)
	pool := newPool(t, dsn, schema)
	for _, stmt := range strings.Split(Schema, ";") {
		if strings.TrimSpace(stmt) != "" {
			_, err = pool.Exec(ctx, stmt)
			require.NoError(t, err)
		}
	}
	return dsn, schema
}

func newPool(t *testing.T, dsn, schema string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func newStore(t *testing.T, dsn, schema string) *Store {
	t.Helper()
	s, err := New(context.Background(), Config{
		Pool:          newPool(t, dsn, schema),
		LeaseTTL:      testTTL,
		RenewInterval: testTTL / 4,
		PollInterval:  10 * time.Millisecond,
		OnError:       func(op string, err error) { t.Logf("pgstore %s: %v", op, err) },
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.Close(ctx)
	})
	return s
}

// stopRenew 停止续约且不释放，模拟持有者暂停或崩溃。ids 为空时停止全部。
func stopRenew(s *Store, ns string, ids ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, h := range s.owners {
		if len(ids) == 0 {
			h.renew = false
		}
		for _, id := range ids {
			if h.ns == ns && h.id == id {
				h.renew = false
			}
		}
	}
}

func TestConformance(t *testing.T) {
	testDSN(t)
	storetest.Run(t, func(t *testing.T) (durable.Store, durable.Store) {
		dsn, schema := newSchema(t)
		return newStore(t, dsn, schema), newStore(t, dsn, schema)
	}, storetest.Options{Scoped: true, Leases: &storetest.Leases{
		TTL: testTTL,
		Expire: func(t *testing.T, s durable.Store, id string) {
			st := s.(*Store)
			stopRenew(st, "", id)
			_, err := st.cfg.Pool.Exec(context.Background(), `UPDATE goagent_durable_lease SET expires_at = now() - interval '1 second' WHERE ns = '' AND id = $1`, id)
			require.NoError(t, err)
		},
		Crash: func(t *testing.T, s durable.Store) { stopRenew(s.(*Store), "") },
	}})
}

func TestSchemaVersionMismatch(t *testing.T) {
	dsn, schema := newSchema(t)
	pool := newPool(t, dsn, schema)
	_, err := pool.Exec(context.Background(), `UPDATE goagent_durable_schema SET version = 2`)
	require.NoError(t, err)
	_, err = New(context.Background(), Config{Pool: pool})
	require.ErrorContains(t, err, "表结构版本为 2")
}

func TestClosedRejectsLock(t *testing.T) {
	dsn, schema := newSchema(t)
	s := newStore(t, dsn, schema)
	require.NoError(t, s.Close(context.Background()))
	_, err := s.Lock(context.Background(), "run")
	require.ErrorIs(t, err, ErrClosed)
}

func TestCloseReleasesAfterTimeout(t *testing.T) {
	dsn, schema := newSchema(t)
	a, b := newStore(t, dsn, schema), newStore(t, dsn, schema)
	_, err := a.Lock(context.Background(), "run")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, a.Close(ctx), context.DeadlineExceeded)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel2()
	unlock, err := b.Lock(ctx2, "run")
	require.NoError(t, err, "Close 超时后应释放剩余租约，其他实例无需等待 TTL")
	require.NoError(t, unlock())
}

// TestExpiredLeaseCleanup：崩溃留下、过期已久的租约被后台清理。
func TestExpiredLeaseCleanup(t *testing.T) {
	dsn, schema := newSchema(t)
	pool := newPool(t, dsn, schema)
	_, err := pool.Exec(context.Background(), `INSERT INTO goagent_durable_lease VALUES ('', 'orphan', 'x', 'dead', now() - interval '1 hour')`)
	require.NoError(t, err)
	s, err := New(context.Background(), Config{Pool: pool, LeaseTTL: testTTL, RenewInterval: 50 * time.Millisecond, ExpiredGrace: 100 * time.Millisecond})
	require.NoError(t, err)
	defer func() { _ = s.Close(context.Background()) }()
	require.Eventually(t, func() bool {
		var n int
		return pool.QueryRow(context.Background(), `SELECT count(*) FROM goagent_durable_lease`).Scan(&n) == nil && n == 0
	}, 2*time.Second, 50*time.Millisecond)
}

const childEnv = "GOAGENT_PGSTORE_CHILD"

// TestProcessCrashTakeover：子进程持锁写入后直接退出，不解锁；父进程的另一实例在租约过期后接管并续跑。
func TestProcessCrashTakeover(t *testing.T) {
	if spec := os.Getenv(childEnv); spec != "" {
		dsn, schema, _ := strings.Cut(spec, "|")
		s := newStore(t, dsn, schema)
		runner := &durable.Runner{Store: s, Version: "v1", Steps: crashSteps(nil)}
		_, err := runner.Run(context.Background(), "order", json.RawMessage(`{}`), "")
		require.ErrorIs(t, err, durable.ErrApprovalRequired)
		_, err = s.Lock(context.Background(), "order")
		require.NoError(t, err)
		os.Exit(3)
	}
	dsn, schema := newSchema(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProcessCrashTakeover$", "-test.count=1")
	cmd.Env = append(os.Environ(), childEnv+"="+dsn+"|"+schema)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit, string(out))
	require.Equal(t, 3, exit.ExitCode(), string(out))

	b := newStore(t, dsn, schema)
	snap, err := b.Load(context.Background(), "order")
	require.NoError(t, err)
	require.True(t, snap.Waiting)
	calls := map[string]int{}
	start := time.Now()
	runner := &durable.Runner{Store: b, Version: "v1", Steps: crashSteps(calls)}
	snap, err = runner.Run(context.Background(), "order", nil, snap.Key)
	require.NoError(t, err)
	require.Less(t, time.Since(start), 3*testTTL, "租约应在 TTL 后被接管")
	require.Equal(t, 2, snap.Next)
	require.Equal(t, map[string]int{"ship": 1}, calls, "崩溃前已完成的步骤不重复执行")
}

func crashSteps(calls map[string]int) []durable.Step {
	step := func(name string) func(context.Context, durable.StepInput) (json.RawMessage, error) {
		return func(_ context.Context, in durable.StepInput) (json.RawMessage, error) {
			if calls != nil {
				calls[name]++
			}
			return in.Data, nil
		}
	}
	return []durable.Step{{Name: "charge", Run: step("charge")}, {Name: "ship", RequireApproval: true, Run: step("ship")}}
}

// TestPrimaryRestart：持锁期间数据库重启，恢复后令牌未变，写入照常成功。
// 需要 GOAGENT_PGSTORE_CONTAINER 指定可被 docker restart 的容器名。
func TestPrimaryRestart(t *testing.T) {
	container := os.Getenv("GOAGENT_PGSTORE_CONTAINER")
	if container == "" {
		t.Skip("未设置 GOAGENT_PGSTORE_CONTAINER，跳过数据库重启测试")
	}
	dsn, schema := newSchema(t)
	s := newStore(t, dsn, schema)
	unlock, err := s.Lock(context.Background(), "run")
	require.NoError(t, err)
	out, err := exec.Command("docker", "restart", container).CombinedOutput()
	require.NoError(t, err, string(out))
	snap := &durable.Snapshot{Version: "v1", RunID: "run", Data: json.RawMessage(`{}`)}
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := s.Save(ctx, "run", snap)
		if err != nil {
			t.Logf("重启期间写入：%v", err)
		}
		return err == nil
	}, 30*time.Second, 200*time.Millisecond)
	require.NoError(t, unlock())
}

// dropAfterCommit 在 armed 时把首个 COMMIT 写出后立即断开连接：服务端已提交，客户端只看到连接错误。
type dropAfterCommit struct {
	net.Conn
	armed *atomic.Bool
}

func (c *dropAfterCommit) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if bytes.Contains(bytes.ToLower(b), []byte("commit")) && c.armed.CompareAndSwap(true, false) {
		_ = c.Conn.Close()
	}
	return n, err
}

// TestAcquireUncertainCommitCleanup：获取租约的提交结果不确定时，按自己的令牌删除已写入的租约。
func TestAcquireUncertainCommitCleanup(t *testing.T) {
	dsn, schema := newSchema(t)
	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	armed := &atomic.Bool{}
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &dropAfterCommit{Conn: conn, armed: armed}, nil
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	s, err := New(context.Background(), Config{Pool: pool, LeaseTTL: time.Minute})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	armed.Store(true)
	_, err = s.Lock(context.Background(), "run")
	require.Error(t, err)
	require.False(t, armed.Load(), "COMMIT 应已发出")
	var n int
	require.NoError(t, newPool(t, dsn, schema).QueryRow(context.Background(), `SELECT count(*) FROM goagent_durable_lease WHERE id = 'run'`).Scan(&n))
	require.Zero(t, n, "提交结果不确定时应删除自己写入的租约，否则其他实例要等 TTL 过期")
}

// TestSyncCommit：会话默认关闭同步提交时，Save 与 RemoveScope 的墓碑仍等待同步备库确认。
// 用一个不存在的同步备库模拟：同步提交会一直等待，异步提交立即返回。会修改服务器配置，只在专用容器中运行。
func TestSyncCommit(t *testing.T) {
	if os.Getenv("GOAGENT_PGSTORE_CONTAINER") == "" {
		t.Skip("未设置 GOAGENT_PGSTORE_CONTAINER，跳过需要修改服务器配置的同步提交测试")
	}
	dsn, schema := newSchema(t)
	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.ConnConfig.RuntimeParams["synchronous_commit"] = "off"
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	s, err := New(context.Background(), Config{Pool: pool, LeaseTTL: time.Minute})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	unlock, err := s.Lock(context.Background(), "run")
	require.NoError(t, err)
	t.Cleanup(func() { _ = unlock() })

	admin := newPool(t, dsn, schema)
	setStandby := func(names string) {
		_, err := admin.Exec(context.Background(), `ALTER SYSTEM SET synchronous_standby_names = '`+names+`'`)
		require.NoError(t, err)
		_, err = admin.Exec(context.Background(), `SELECT pg_reload_conf()`)
		require.NoError(t, err)
	}
	// 晚于建表注册，先于删 schema 执行，避免清理时的同步提交被挂住。
	t.Cleanup(func() { setStandby("") })
	setStandby("ghost")

	const wait = 300 * time.Millisecond
	blocked := func(f func(context.Context) error) bool {
		ctx, cancel := context.WithTimeout(context.Background(), wait)
		defer cancel()
		start := time.Now()
		_ = f(ctx)
		return time.Since(start) >= wait
	}
	write := func(p *pgxpool.Pool) func(context.Context) error {
		return func(ctx context.Context) error {
			_, err := p.Exec(ctx, `UPDATE goagent_durable_schema SET version = version`)
			return err
		}
	}
	require.Eventually(t, func() bool { return blocked(write(admin)) }, 5*time.Second, 50*time.Millisecond, "同步备库配置未生效")
	require.False(t, blocked(write(pool)), "会话关闭同步提交时普通写入不应等待")

	snap := &durable.Snapshot{Version: "v1", RunID: "run", Data: json.RawMessage(`{}`)}
	require.True(t, blocked(func(ctx context.Context) error { return s.Save(ctx, "run", snap) }), "Save 应同步提交")
	require.True(t, blocked(func(ctx context.Context) error { return s.RemoveScope(ctx, "tenant") }), "墓碑应同步提交")
}
