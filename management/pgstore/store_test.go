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
