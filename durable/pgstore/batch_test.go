package pgstore

import (
	"context"
	"encoding/json"
	"github.com/kart-io/goagent/constants"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kart-io/goagent/core/durable"
	"github.com/kart-io/goagent/core/session"
	agentErrors "github.com/kart-io/goagent/errors"
)

// A failure on the second database write must roll back the first write, not
// merely reject malformed batches before starting a transaction.
func TestSaveAllDatabaseRollback(t *testing.T) {
	dsn, schema := newSchema(t)
	s := newStore(t, dsn, schema)
	ctx := context.Background()
	for _, id := range []string{"a", "z"} {
		unlock, err := s.Lock(ctx, id)
		require.NoError(t, err)
		defer unlock()
		require.NoError(t, s.Save(ctx, id, &durable.Snapshot{Version: "old"}))
	}
	_, err := s.cfg.Pool.Exec(ctx, `ALTER TABLE goagent_durable_snapshot ADD CONSTRAINT reject_z CHECK (id <> 'z' OR position('new'::bytea in data) = 0)`)
	require.NoError(t, err)
	err = s.SaveAll(ctx,
		durable.Write{ID: "a", Snapshot: &durable.Snapshot{Version: "new"}},
		durable.Write{ID: "z", Snapshot: &durable.Snapshot{Version: "new"}},
	)
	require.Error(t, err)
	for _, id := range []string{"a", "z"} {
		snap, err := s.Load(ctx, id)
		require.NoError(t, err)
		require.Equal(t, "old", snap.Version)
	}
}

func TestScopedSaveAllFencingAndIsolation(t *testing.T) {
	dsn, schema := newSchema(t)
	a, b := newStore(t, dsn, schema), newStore(t, dsn, schema)
	ctx := context.Background()
	view := a.Scope("tenant")
	for _, id := range []string{"a", "z"} {
		unlock, err := view.Lock(ctx, id)
		require.NoError(t, err)
		defer unlock()
	}
	require.NoError(t, view.SaveAll(ctx,
		durable.Write{ID: "a", Snapshot: &durable.Snapshot{Version: "old"}},
		durable.Write{ID: "z", Snapshot: &durable.Snapshot{Version: "old"}},
	))
	_, err := b.Load(ctx, "a")
	require.ErrorIs(t, err, os.ErrNotExist)
	stopRenew(a, scopeNS("tenant"), "z")
	_, err = a.cfg.Pool.Exec(ctx, `UPDATE goagent_durable_lease SET expires_at = now() - interval '1 second' WHERE ns = $1 AND id = 'z'`, scopeNS("tenant"))
	require.NoError(t, err)
	unlock, err := b.Scope("tenant").Lock(ctx, "z")
	require.NoError(t, err)
	defer unlock()
	err = view.SaveAll(ctx,
		durable.Write{ID: "a", Snapshot: &durable.Snapshot{Version: "new"}},
		durable.Write{ID: "z", Snapshot: &durable.Snapshot{Version: "new"}},
	)
	require.ErrorIs(t, err, agentErrors.ErrLeaseLost)
	for _, id := range []string{"a", "z"} {
		snap, err := b.Scope("tenant").Load(ctx, id)
		require.NoError(t, err)
		require.Equal(t, "old", snap.Version)
	}
}

func TestAppendAllDatabaseRollback(t *testing.T) {
	dsn, schema := newSchema(t)
	s := newSessionStore(t, dsn, schema)
	ctx := context.Background()
	_, err := s.root.cfg.Pool.Exec(ctx, `ALTER TABLE goagent_session_event ADD CONSTRAINT reject_z CHECK (session_id <> 'z')`)
	require.NoError(t, err)
	_, err = s.AppendAll(ctx,
		session.AppendBatch{ID: "a", Events: []session.Event{{Type: "test", Data: json.RawMessage(`{}`)}}},
		session.AppendBatch{ID: "z", Events: []session.Event{{Type: "test", Data: json.RawMessage(`{}`)}}},
	)
	require.Error(t, err)
	for _, id := range []string{"a", "z"} {
		_, err := s.Read(ctx, id, 0)
		require.ErrorIs(t, err, agentErrors.ErrSessionNotFound)
	}
	ids, err := s.List(ctx)
	require.NoError(t, err)
	require.Empty(t, ids)
}

func TestAppendAllCompareOnlyAndEncoding(t *testing.T) {
	dsn, schema := newSchema(t)
	s := newSessionStore(t, dsn, schema)
	ctx := context.Background()
	lasts, err := s.AppendAll(ctx, session.AppendBatch{ID: "empty"})
	require.NoError(t, err)
	require.Equal(t, []int64{0}, lasts)
	_, err = s.Read(ctx, "empty", 0)
	require.ErrorIs(t, err, agentErrors.ErrSessionNotFound)
	_, err = s.AppendAll(ctx,
		session.AppendBatch{ID: "a", Events: []session.Event{{Type: "test", Data: json.RawMessage(`{}`)}}},
		session.AppendBatch{ID: "z", Events: []session.Event{{Type: "test", Data: json.RawMessage(`{`)}}},
	)
	require.Error(t, err)
	_, err = s.Read(ctx, "a", 0)
	require.ErrorIs(t, err, agentErrors.ErrSessionNotFound)
}

func TestAppendAllConcurrentReverseOrder(t *testing.T) {
	dsn, schema := newSchema(t)
	a, b := newSessionStore(t, dsn, schema), newSessionStore(t, dsn, schema)
	ctx := context.Background()
	events := []session.Event{{Type: "test", Data: json.RawMessage(`{}`)}}
	start := make(chan struct{})
	done := make(chan error, 2)
	for i, store := range []*SessionStore{a, b} {
		ids := []string{"a", "z"}
		if i == 1 {
			ids = []string{"z", "a"}
		}
		go func() {
			<-start
			_, err := store.AppendAll(ctx, session.AppendBatch{ID: ids[0], Events: events}, session.AppendBatch{ID: ids[1], Events: events})
			done <- err
		}()
	}
	close(start)
	first, second := <-done, <-done
	if first == nil {
		require.ErrorIs(t, second, agentErrors.ErrSessionConflict)
	} else {
		require.ErrorIs(t, first, agentErrors.ErrSessionConflict)
		require.NoError(t, second)
	}
	for _, id := range []string{"a", "z"} {
		events, err := a.Read(ctx, id, 0)
		require.NoError(t, err)
		require.Len(t, events, 1)
		require.EqualValues(t, 1, events[0].Seq)
	}
}

func TestAppendAllSnapshotLeaseFencing(t *testing.T) {
	dsn, schema := newSchema(t)
	a, b := newSessionStore(t, dsn, schema), newSessionStore(t, dsn, schema)
	ctx := context.Background()
	name := session.DurableLockName("op")
	unlock, err := a.Lock(ctx, "z", name)
	require.NoError(t, err)
	defer unlock()
	stopRenew(a.root, sessionNS("z"), name)
	_, err = a.root.cfg.Pool.Exec(ctx, `UPDATE goagent_durable_lease SET expires_at = now() - interval '1 second' WHERE ns = $1 AND id = $2`, sessionNS("z"), name)
	require.NoError(t, err)
	takeover, err := b.Lock(ctx, "z", name)
	require.NoError(t, err)
	defer takeover()
	snapshot, err := session.NewEvent(constants.SessionEventBoundarySnapshot, session.BoundaryData{ID: "op", Snapshot: []byte(`{}`)})
	require.NoError(t, err)
	_, err = a.AppendAll(ctx,
		session.AppendBatch{ID: "a", Events: []session.Event{{Type: "test", Data: json.RawMessage(`{}`)}}},
		session.AppendBatch{ID: "z", Events: []session.Event{snapshot}},
	)
	require.ErrorIs(t, err, agentErrors.ErrLeaseLost)
	for _, id := range []string{"a", "z"} {
		_, err := b.Read(ctx, id, 0)
		require.ErrorIs(t, err, agentErrors.ErrSessionNotFound)
	}
}

// An empty batch must keep its comparison stable until the whole cross-session
// transaction commits, including while another session is blocked on a row lock.
func TestAppendAllCompareOnlyHoldsUntilCommit(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := "missing"
		if existing {
			name = "existing"
		}
		t.Run(name, func(t *testing.T) {
			dsn, schema := newSchema(t)
			a, b := newSessionStore(t, dsn, schema), newSessionStore(t, dsn, schema)
			ctx := context.Background()
			event := session.Event{Type: "test", Data: json.RawMessage(`{}`)}
			var expected int64
			if existing {
				var err error
				expected, err = a.Append(ctx, "a", 0, event)
				require.NoError(t, err)
			}
			_, err := a.Append(ctx, "z", 0, event)
			require.NoError(t, err)
			blocker, err := a.root.cfg.Pool.Begin(ctx)
			require.NoError(t, err)
			defer blocker.Rollback(ctx)
			_, err = blocker.Exec(ctx, `SELECT 1 FROM goagent_session WHERE id = 'z' FOR UPDATE`)
			require.NoError(t, err)
			var blockerPID int
			require.NoError(t, blocker.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&blockerPID))
			done := make(chan error, 1)
			go func() {
				_, err := a.AppendAll(ctx,
					session.AppendBatch{ID: "a", Expected: expected},
					session.AppendBatch{ID: "z", Expected: 1, Events: []session.Event{event}},
				)
				done <- err
			}()
			defer func() {
				require.NoError(t, blocker.Rollback(ctx))
				require.NoError(t, <-done)
			}()
			require.Eventually(t, func() bool {
				var blocked bool
				err := a.root.cfg.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)))`, blockerPID).Scan(&blocked)
				return err == nil && blocked
			}, 2*time.Second, 10*time.Millisecond)
			appendCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			defer cancel()
			_, err = b.Append(appendCtx, "a", expected, event)
			require.ErrorIs(t, err, context.DeadlineExceeded, "comparison must exclude a concurrent append until batch commit")
		})
	}
}
