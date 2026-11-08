package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
	mg "github.com/kart-io/goagent-contrib/management"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

func database(t *testing.T) (context.Context, *Store) {
	t.Helper()
	ctx := context.Background()
	dsn := os.Getenv("GOAGENT_PGSTORE_DSN")
	if dsn == "" {
		t.Skip("需要本地 Postgres")
	}
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, Migrate(ctx, pool))
	s, err := New(ctx, pool)
	require.NoError(t, err)
	return ctx, s
}
func TestAdmissionRollsBackAllRecords(t *testing.T) {
	ctx, s := database(t)
	id := time.Now().Format("150405.000000000")
	a := mg.Agent{Meta: mg.Meta{ID: id, TenantID: id, Revision: 1, CreatedAt: time.Now().UTC()}, Name: "回滚", Enabled: true}
	raw, _ := json.Marshal(a)
	d := mg.Document{Kind: "agents", TenantID: id, ID: id, Revision: 1, CreatedAt: a.CreatedAt, Data: raw}
	sentinel := errors.New("注入失败")
	require.ErrorIs(t, s.WithinTx(ctx, func(tx mg.Tx) error { require.NoError(t, tx.Insert(ctx, d)); return sentinel }), sentinel)
	_, err := s.Get(ctx, "agents", id, id)
	require.ErrorIs(t, err, mg.ErrNotFound)
}
func TestPaginationAppliesVisibilityBeforeLimit(t *testing.T) {
	ctx, s := database(t)
	tenant := time.Now().Format("150405.000000000")
	for _, id := range []string{"a", "b", "c"} {
		a := mg.Agent{Meta: mg.Meta{ID: id, TenantID: tenant, Revision: 1, CreatedAt: time.Now().UTC()}, Name: id}
		raw, _ := json.Marshal(a)
		require.NoError(t, s.WithinTx(ctx, func(tx mg.Tx) error {
			return tx.Insert(ctx, mg.Document{Kind: "agents", TenantID: tenant, ID: id, Revision: 1, CreatedAt: a.CreatedAt, Data: raw})
		}))
	}
	docs, err := s.List(ctx, "agents", mg.Selection{TenantID: tenant, Query: mg.Query{Limit: 1}, Visibility: mg.Visibility{ResourceIDs: []string{"c"}}})
	require.NoError(t, err)
	require.Len(t, docs, 1)
	require.Equal(t, "c", docs[0].ID)
}

func TestCanceledTransactionReleasesConnection(t *testing.T) {
	ctx, s := database(t)
	canceled, cancel := context.WithCancel(ctx)
	require.ErrorIs(t, s.WithinTx(canceled, func(mg.Tx) error { cancel(); return canceled.Err() }), context.Canceled)
	check, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	require.NoError(t, s.WithinTx(check, func(mg.Tx) error { return nil }))
	require.Zero(t, s.pool.Stat().AcquiredConns())
}
func TestConcurrentRevisionCASHasOneWinnerAndTenantForeignKeyIsEnforced(t *testing.T) {
	ctx, s := database(t)
	id := time.Now().Format("150405.000000000")
	agent := mg.Agent{Meta: mg.Meta{ID: id, TenantID: id, Revision: 1, CreatedAt: time.Now().UTC()}, Name: "CAS"}
	raw, err := json.Marshal(agent)
	require.NoError(t, err)
	doc := mg.Document{Kind: "agents", TenantID: id, ID: id, Revision: 1, CreatedAt: agent.CreatedAt, Data: raw}
	require.NoError(t, s.WithinTx(ctx, func(tx mg.Tx) error { return tx.Insert(ctx, doc) }))
	outcomes := make(chan error, 2)
	for range 2 {
		go func() {
			next := agent
			next.Revision = 2
			data, e := json.Marshal(next)
			if e != nil {
				outcomes <- e
				return
			}
			candidate := doc
			candidate.Revision = 2
			candidate.Data = data
			outcomes <- s.WithinTx(ctx, func(tx mg.Tx) error { return tx.CAS(ctx, candidate, 1) })
		}()
	}
	successes, conflicts := 0, 0
	for range 2 {
		err := <-outcomes
		if err == nil {
			successes++
		} else if errors.Is(err, mg.ErrConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, 1, conflicts)
	version := mg.Version{Meta: mg.Meta{ID: id + "@v1", TenantID: "other-" + id, Revision: 1, CreatedAt: time.Now().UTC()}, AgentID: id, Version: "v1"}
	raw, err = json.Marshal(version)
	require.NoError(t, err)
	require.ErrorIs(t, s.WithinTx(ctx, func(tx mg.Tx) error {
		return tx.Insert(ctx, mg.Document{Kind: "versions", TenantID: version.TenantID, ID: version.ID, Revision: 1, CreatedAt: version.CreatedAt, Data: raw})
	}), mg.ErrConflict)
}
