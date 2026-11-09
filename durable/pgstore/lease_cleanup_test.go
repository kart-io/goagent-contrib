package pgstore

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	agentErrors "github.com/kart-io/goagent/errors"
)

// waitLeaseBlocked observes an actual database lock wait, not a scheduling delay.
func waitLeaseBlocked(t *testing.T, pool *pgxpool.Pool, blocker uint32, done <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-done:
			t.Fatal("lease operation returned before the acquiring transaction finished")
		case <-ctx.Done():
			t.Fatal("lease operation did not wait on the acquiring transaction")
		case <-tick.C:
			var blocked bool
			err := pool.QueryRow(ctx, `SELECT EXISTS (
  SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted
  AND $1::integer = ANY(pg_blocking_pids(pid)))`, blocker).Scan(&blocked)
			require.NoError(t, err)
			if blocked {
				return
			}
		}
	}
}

func TestDeleteLeaseWaitsForAcquireTransaction(t *testing.T) {
	for _, ns := range []string{"", scopeNS("tenant")} {
		for _, isolation := range []string{"read committed", "repeatable read"} {
			for _, outcome := range []string{"commit", "rollback", "other-token"} {
				t.Run(ns+"/"+isolation+"/"+outcome, func(t *testing.T) {
					dsn, schema := newSchema(t)
					observer := newPool(t, dsn, schema)
					cfg, err := pgxpool.ParseConfig(dsn)
					require.NoError(t, err)
					cfg.ConnConfig.RuntimeParams["search_path"] = schema
					cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = isolation
					pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
					require.NoError(t, err)
					t.Cleanup(pool.Close)
					errors := make(chan error, 1)
					// No background renewer: only the acquiring transaction can finish this lease.
					s := &Store{cfg: Config{Pool: pool, OpTimeout: 5 * time.Second,
						OnError: func(_ string, err error) { errors <- err }}}
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					tx, err := observer.Begin(ctx)
					require.NoError(t, err)
					defer tx.Rollback(context.Background())
					require.NoError(t, checkScope(ctx, tx, ns))
					// Two-int advisory keys have a disjoint lock space from the scope's bigint key.
					_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))`, ns, "run")
					require.NoError(t, err)
					token := "ours"
					if outcome == "other-token" {
						token = "successor"
					}
					_, err = tx.Exec(ctx, `INSERT INTO goagent_durable_lease (ns, id, token, owner, expires_at)
VALUES ($1, 'run', $2, 'test', now() + interval '1 minute')`, ns, token)
					require.NoError(t, err)
					done := make(chan struct{})
					go func() {
						s.deleteLease(ns, "run", "ours")
						close(done)
					}()
					// Ensure the worker ends even on an assertion failure before releasing the transaction.
					defer func() {
						_ = tx.Rollback(context.Background())
						<-done
					}()
					waitLeaseBlocked(t, observer, tx.Conn().PgConn().PID(), done)
					if outcome == "rollback" {
						require.NoError(t, tx.Rollback(ctx))
					} else {
						require.NoError(t, tx.Commit(ctx))
					}
					select {
					case <-done:
					case <-ctx.Done():
						t.Fatal("cleanup did not finish after the transaction ended")
					}
					select {
					case err := <-errors:
						t.Fatalf("cleanup failed: %v", err)
					default:
					}
					var tokens []string
					rows, err := observer.Query(ctx, `SELECT token FROM goagent_durable_lease WHERE ns = $1 AND id = 'run'`, ns)
					require.NoError(t, err)
					tokens, err = pgx.CollectRows(rows, pgx.RowTo[string])
					require.NoError(t, err)
					if outcome == "other-token" {
						require.Equal(t, []string{"successor"}, tokens)
					} else {
						require.Empty(t, tokens, "cleanup must use a fresh snapshot after waiting")
					}
				})
			}
		}
	}
}

func TestTryAcquireUsesLeaseTransactionLock(t *testing.T) {
	for _, ns := range []string{"", scopeNS("tenant")} {
		t.Run(ns, func(t *testing.T) {
			dsn, schema := newSchema(t)
			pool := newPool(t, dsn, schema)
			s := &Store{cfg: Config{Pool: pool, OpTimeout: 5 * time.Second, LeaseTTL: time.Minute}}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			tx, err := pool.Begin(ctx)
			require.NoError(t, err)
			defer tx.Rollback(context.Background())
			_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))`, ns, "run")
			require.NoError(t, err)
			done := make(chan struct{})
			var acquired bool
			var acquireErr error
			go func() {
				acquired, acquireErr = s.tryAcquire(ctx, ns, "run", "ours")
				close(done)
			}()
			defer func() {
				_ = tx.Rollback(context.Background())
				<-done
			}()
			waitLeaseBlocked(t, pool, tx.Conn().PgConn().PID(), done)
			require.NoError(t, tx.Commit(ctx))
			<-done
			require.NoError(t, acquireErr)
			require.True(t, acquired)
			s.deleteLease(ns, "run", "ours")
		})
	}
}

func TestLeaseCleanupDoesNotWaitForScopeLock(t *testing.T) {
	dsn, schema := newSchema(t)
	pool := newPool(t, dsn, schema)
	errors := make(chan error, 1)
	s := &Store{cfg: Config{Pool: pool, OpTimeout: 5 * time.Second, LeaseTTL: time.Minute,
		OnError: func(_ string, err error) { errors <- err }}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ns := scopeNS("tenant")
	_, err := pool.Exec(ctx, `INSERT INTO goagent_durable_lease (ns, id, token, owner, expires_at)
VALUES ($1, 'run', 'ours', 'test', now() + interval '1 minute')`, ns)
	require.NoError(t, err)
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())
	_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, ns)
	require.NoError(t, err)
	done := make(chan struct{})
	var acquired bool
	var acquireErr error
	go func() {
		acquired, acquireErr = s.tryAcquire(ctx, ns, "next", "next-token")
		close(done)
	}()
	defer func() {
		_ = tx.Rollback(context.Background())
		<-done
	}()
	pid := tx.Conn().PgConn().PID()
	waitLeaseBlocked(t, pool, pid, done)
	var leaseLocked bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT EXISTS (
  SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND granted AND objsubid = 2
  AND $1::integer = ANY(pg_blocking_pids(pid)))`, pid).Scan(&leaseLocked))
	require.False(t, leaseLocked, "acquisition must take the scope lock before the lease lock")

	// Cleanup must finish while the exclusive scope lock is still held.
	s.deleteLease(ns, "run", "ours")
	select {
	case err := <-errors:
		t.Fatalf("cleanup failed while scope was locked: %v", err)
	default:
	}
	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM goagent_durable_lease WHERE ns = $1 AND id = 'run'`, ns).Scan(&n))
	require.Zero(t, n)
	_, err = tx.Exec(ctx, `INSERT INTO goagent_durable_scope (ns, removed_at) VALUES ($1, now())`, ns)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
	<-done
	require.False(t, acquired)
	require.ErrorIs(t, acquireErr, agentErrors.ErrScopeRemoved)
}
