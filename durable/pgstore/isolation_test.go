package pgstore

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/kart-io/goagent/core/durable"
	"github.com/kart-io/goagent/core/session"
	agentErrors "github.com/kart-io/goagent/errors"
)

func isolationPool(t *testing.T, dsn, schema, isolation string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = isolation
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func TestSaveRejectsTombstoneAfterScopeWait(t *testing.T) {
	for _, isolation := range []string{"read committed", "repeatable read"} {
		for _, batch := range []bool{false, true} {
			name := isolation + "/Save"
			if batch {
				name += "All"
			}
			t.Run(name, func(t *testing.T) {
				dsn, schema := newSchema(t)
				observer := newPool(t, dsn, schema)
				s := newIdleStore(isolationPool(t, dsn, schema, isolation))
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				view := s.Scope("tenant")
				for _, id := range []string{"a", "z"} {
					unlock, err := view.Lock(ctx, id)
					require.NoError(t, err)
					defer unlock()
					require.NoError(t, view.Save(ctx, id, &durable.Snapshot{Version: "before"}))
				}
				tx, err := observer.Begin(ctx)
				require.NoError(t, err)
				defer tx.Rollback(context.Background())
				_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, scopeNS("tenant"))
				require.NoError(t, err)
				var saveErr error
				done := make(chan struct{})
				go func() {
					if batch {
						saveErr = view.SaveAll(ctx,
							durable.Write{ID: "a", Snapshot: &durable.Snapshot{Version: "after"}},
							durable.Write{ID: "z", Snapshot: &durable.Snapshot{Version: "after"}})
					} else {
						saveErr = view.Save(ctx, "a", &durable.Snapshot{Version: "after"})
					}
					close(done)
				}()
				defer func() { _ = tx.Rollback(context.Background()); <-done }()
				waitLeaseBlocked(t, observer, tx.Conn().PgConn().PID(), done)
				// 模拟墓碑已提交、批量清理中断，仍保留合法 token 与旧快照。
				_, err = tx.Exec(ctx, `INSERT INTO goagent_durable_scope (ns, removed_at) VALUES ($1, now())`, scopeNS("tenant"))
				require.NoError(t, err)
				require.NoError(t, tx.Commit(ctx))
				<-done
				require.ErrorIs(t, saveErr, agentErrors.ErrScopeRemoved)
				for _, id := range []string{"a", "z"} {
					var raw []byte
					require.NoError(t, observer.QueryRow(ctx, `SELECT data FROM goagent_durable_snapshot WHERE ns=$1 AND id=$2`, scopeNS("tenant"), id).Scan(&raw))
					var snapshot durable.Snapshot
					require.NoError(t, json.Unmarshal(raw, &snapshot))
					require.Equal(t, "before", snapshot.Version)
				}
			})
		}
	}
}

func TestScopeMaintenanceAfterConcurrentTransaction(t *testing.T) {
	for _, isolation := range []string{"read committed", "repeatable read"} {
		for _, prune := range []bool{false, true} {
			name := isolation + "/RemoveScope"
			if prune {
				name = isolation + "/PruneRemoved"
			}
			t.Run(name, func(t *testing.T) {
				dsn, schema := newSchema(t)
				observer := newPool(t, dsn, schema)
				s := newIdleStore(isolationPool(t, dsn, schema, isolation))
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				ns := scopeNS("tenant")
				if prune {
					_, err := observer.Exec(ctx, `INSERT INTO goagent_durable_scope VALUES ($1, now() - interval '1 hour')`, ns)
					require.NoError(t, err)
				}
				tx, err := observer.Begin(ctx)
				require.NoError(t, err)
				defer tx.Rollback(context.Background())
				_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, ns)
				require.NoError(t, err)
				var opErr error
				done := make(chan struct{})
				go func() {
					if prune {
						opErr = s.PruneRemoved(ctx, time.Minute)
					} else {
						opErr = s.RemoveScope(ctx, "tenant")
					}
					close(done)
				}()
				defer func() { _ = tx.Rollback(context.Background()); <-done }()
				waitLeaseBlocked(t, observer, tx.Conn().PgConn().PID(), done)
				if prune {
					// 另一回收者先完成，等待者必须重新读取墓碑，安全地无操作返回。
					_, err = tx.Exec(ctx, `DELETE FROM goagent_durable_scope WHERE ns=$1`, ns)
				} else {
					// 另一删除者先提交墓碑；重复删除不应报序列化错误或刷新时间。
					_, err = tx.Exec(ctx, `INSERT INTO goagent_durable_scope VALUES ($1, '2000-01-01T00:00:00Z')`, ns)
				}
				require.NoError(t, err)
				require.NoError(t, tx.Commit(ctx))
				<-done
				require.NoError(t, opErr)
				if prune {
					var n int
					require.NoError(t, observer.QueryRow(ctx, `SELECT count(*) FROM goagent_durable_scope WHERE ns=$1`, ns).Scan(&n))
					require.Zero(t, n)
				} else {
					var removed time.Time
					require.NoError(t, observer.QueryRow(ctx, `SELECT removed_at FROM goagent_durable_scope WHERE ns=$1`, ns).Scan(&removed))
					require.Equal(t, 2000, removed.Year())
				}
			})
		}
	}
}

func TestSessionWritesAfterRowWait(t *testing.T) {
	for _, isolation := range []string{"read committed", "repeatable read"} {
		for _, op := range []string{"Append", "AppendAll", "compare", "Fork"} {
			t.Run(isolation+"/"+op, func(t *testing.T) {
				dsn, schema := newSchema(t)
				observer := newPool(t, dsn, schema)
				s := &SessionStore{root: newIdleStore(isolationPool(t, dsn, schema, isolation))}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				event := session.Event{Type: "test", Data: json.RawMessage(`{}`)}
				_, err := s.Append(ctx, "session", 0, event)
				require.NoError(t, err)
				tx, err := observer.Begin(ctx)
				require.NoError(t, err)
				defer tx.Rollback(context.Background())
				plan, err := s.planAppend(session.AppendBatch{ID: "session", Expected: 1, Events: []session.Event{event}})
				require.NoError(t, err)
				require.NoError(t, plan.apply(ctx, tx, time.Minute))
				var writeErr error
				done := make(chan struct{})
				go func() {
					switch op {
					case "Append":
						_, writeErr = s.Append(ctx, "session", 1, event)
					case "AppendAll":
						_, writeErr = s.AppendAll(ctx, session.AppendBatch{ID: "session", Expected: 1, Events: []session.Event{event}})
					case "compare":
						_, writeErr = s.AppendAll(ctx, session.AppendBatch{ID: "session", Expected: 1})
					case "Fork":
						writeErr = s.Fork(ctx, "session", "copy", 0)
					}
					close(done)
				}()
				defer func() { _ = tx.Rollback(context.Background()); <-done }()
				pid := tx.Conn().PgConn().PID()
				require.Eventually(t, func() bool {
					var blocked bool
					err := observer.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE NOT granted AND $1::integer = ANY(pg_blocking_pids(pid)))`, pid).Scan(&blocked)
					return err == nil && blocked
				}, 3*time.Second, 5*time.Millisecond, "必须观察到真实行锁等待")
				require.NoError(t, tx.Commit(ctx))
				<-done
				if op == "Fork" {
					require.NoError(t, writeErr)
					events, err := s.Read(ctx, "copy", 0)
					require.NoError(t, err)
					require.Len(t, events, 2, "复制获锁后完整的已提交前缀")
				} else {
					require.ErrorIs(t, writeErr, agentErrors.ErrSessionConflict)
				}
				events, err := s.Read(ctx, "session", 0)
				require.NoError(t, err)
				require.Len(t, events, 2)
			})
		}
	}
}
