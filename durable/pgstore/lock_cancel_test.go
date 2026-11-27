package pgstore

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	agentErrors "github.com/kart-io/goagent/errors"
)

// 续约间隔远长于用例，并发时序仅由测试中的事务决定。
func newIdleStore(t *testing.T, pool *pgxpool.Pool) *Store {
	t.Helper()
	s, err := New(context.Background(), Config{Pool: pool, OpTimeout: 5 * time.Second, LeaseTTL: time.Minute})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.Close(ctx)
	})
	return s
}

// 进程内的闸门与持有者登记由 leasestore 自己的测试覆盖，这里只看数据库。
func requireNoLockState(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM goagent_durable_lease`).Scan(&n))
	require.Zero(t, n, "取消后不能残留自己的租约")
}

func TestLockCancellationDuringAdvisoryWait(t *testing.T) {
	for _, waitOn := range []string{"lease", "scope"} {
		for _, expire := range []bool{false, true} {
			name := waitOn + "/cancel"
			if expire {
				name = waitOn + "/deadline"
			}
			t.Run(name, func(t *testing.T) {
				dsn, schema := newSchema(t)
				pool := newPool(t, dsn, schema)
				s := newIdleStore(t, pool)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				tx, err := pool.Begin(ctx)
				require.NoError(t, err)
				defer tx.Rollback(context.Background())
				view := s.Scope("tenant")
				if waitOn == "lease" {
					err = lockLease(ctx, tx, scopeNS("tenant"), "run")
				} else {
					_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, scopeNS("tenant"))
				}
				require.NoError(t, err)
				caller, stop := context.WithCancel(ctx)
				wantErr := context.Canceled
				if expire {
					stop()
					caller, stop = context.WithTimeout(ctx, time.Second)
					wantErr = context.DeadlineExceeded
				}
				defer stop()
				var unlock func() error
				var lockErr error
				done := make(chan struct{})
				go func() { unlock, lockErr = view.Lock(caller, "run"); close(done) }()
				defer func() {
					_ = tx.Rollback(context.Background())
					<-done
					if unlock != nil {
						_ = unlock()
					}
				}()
				waitLeaseBlocked(t, pool, tx.Conn().PgConn().PID(), done)
				if !expire {
					stop()
				}
				<-caller.Done()
				// 获得实际 advisory 等待的证据后取消，且不先解除阻塞。
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("取消后仍等待 advisory 锁或无必要的补偿")
				}
				require.Nil(t, unlock)
				require.ErrorIs(t, lockErr, agentErrors.ErrLockUnavailable)
				require.ErrorIs(t, lockErr, wantErr)
				require.NoError(t, tx.Commit(ctx))
				requireNoLockState(t, pool)
				// 同一个 Store 可以立即重试，无本地 gate 或数据库租约残留。
				retry, err := view.Lock(ctx, "run")
				require.NoError(t, err)
				require.NoError(t, retry())
				requireNoLockState(t, pool)
			})
		}
	}
}

type commitQueryKey struct{}

// 在 pgx 收到 COMMIT 结果后、Lock 发布持有者前取消，覆盖成功与不确定提交分支。
type cancelAfterCommit struct {
	once   sync.Once
	cancel context.CancelFunc
}

func (c *cancelAfterCommit) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, commitQueryKey{}, strings.EqualFold(d.SQL, "commit"))
}

func (c *cancelAfterCommit) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if commit, _ := ctx.Value(commitQueryKey{}).(bool); commit {
		c.once.Do(c.cancel)
	}
}

func TestLockCancelledAfterCommit(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		name := "committed"
		if uncertain {
			name = "uncertain"
		}
		t.Run(name, func(t *testing.T) {
			dsn, schema := newSchema(t)
			observer := newPool(t, dsn, schema)
			caller, cancel := context.WithCancel(context.Background())
			defer cancel()
			cfg, err := pgxpool.ParseConfig(dsn)
			require.NoError(t, err)
			cfg.ConnConfig.RuntimeParams["search_path"] = schema
			cfg.ConnConfig.Tracer = &cancelAfterCommit{cancel: cancel}
			armed := &atomic.Bool{}
			if uncertain {
				armed.Store(true)
				cfg.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
					conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
					if err != nil {
						return nil, err
					}
					return &dropAfterCommit{Conn: conn, armed: armed}, nil
				}
			}
			pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
			require.NoError(t, err)
			t.Cleanup(pool.Close)
			s := newIdleStore(t, pool)
			unlock, err := s.Lock(caller, "run")
			if unlock != nil {
				defer unlock()
			}
			require.ErrorIs(t, caller.Err(), context.Canceled)
			require.Nil(t, unlock)
			require.ErrorIs(t, err, agentErrors.ErrLockUnavailable)
			require.ErrorIs(t, err, context.Canceled)
			require.False(t, armed.Load(), "故障必须发生在发出 COMMIT 后")
			requireNoLockState(t, observer)
		})
	}
}
