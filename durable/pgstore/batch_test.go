package pgstore

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kart-io/goagent/core/durable"
	"github.com/kart-io/goagent/core/session"
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
	require.ErrorIs(t, err, durable.ErrLeaseLost)
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
		require.ErrorIs(t, err, session.ErrNotFound)
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
	require.ErrorIs(t, err, session.ErrNotFound)
	_, err = s.AppendAll(ctx,
		session.AppendBatch{ID: "a", Events: []session.Event{{Type: "test", Data: json.RawMessage(`{}`)}}},
		session.AppendBatch{ID: "z", Events: []session.Event{{Type: "test", Data: json.RawMessage(`{`)}}},
	)
	require.Error(t, err)
	_, err = s.Read(ctx, "a", 0)
	require.ErrorIs(t, err, session.ErrNotFound)
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
		require.ErrorIs(t, second, session.ErrConflict)
	} else {
		require.ErrorIs(t, first, session.ErrConflict)
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
	snapshot, err := session.NewEvent(session.TypeBoundarySnapshot, session.BoundaryData{ID: "op", Snapshot: []byte(`{}`)})
	require.NoError(t, err)
	_, err = a.AppendAll(ctx,
		session.AppendBatch{ID: "a", Events: []session.Event{{Type: "test", Data: json.RawMessage(`{}`)}}},
		session.AppendBatch{ID: "z", Events: []session.Event{snapshot}},
	)
	require.ErrorIs(t, err, durable.ErrLeaseLost)
	for _, id := range []string{"a", "z"} {
		_, err := b.Read(ctx, id, 0)
		require.ErrorIs(t, err, session.ErrNotFound)
	}
}
