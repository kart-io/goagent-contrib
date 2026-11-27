package pgstore

import (
	"context"
	"testing"
	"time"

	durablepg "github.com/kart-io/goagent-contrib/durable/pgstore"
	"github.com/kart-io/goagent/core/session"
	mg "github.com/kart-io/goagent/management"
	"github.com/kart-io/goagent/management/storetest"
	"github.com/stretchr/testify/require"
)

func TestListConformance(t *testing.T) {
	storetest.RunListConformance(t, func(t *testing.T) mg.Store {
		_, s := database(t)
		// RunListConformance wants an empty store; its rows live under one tenant.
		_, err := s.pool.Exec(t.Context(), `DELETE FROM mg_outbox WHERE tenant_id=$1`, "storetest-tenant")
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

func TestHistoryConformance(t *testing.T) {
	storetest.RunHistoryConformance(t, func(t *testing.T) (mg.HistoryReader, session.Store) {
		ctx, s := database(t)
		for table, schema := range map[string]string{"goagent_durable_schema": durablepg.Schema, "goagent_session_event": durablepg.SessionSchema} {
			var present bool
			require.NoError(t, s.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&present))
			if !present {
				_, err := s.pool.Exec(ctx, schema)
				require.NoError(t, err)
			}
		}
		root, err := durablepg.New(ctx, durablepg.Config{Pool: s.pool, LeaseTTL: time.Minute})
		require.NoError(t, err)
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = root.Close(ctx)
		})
		sessions, err := durablepg.NewSessionStore(ctx, root)
		require.NoError(t, err)
		return s, sessions
	})
}

func TestReconciliationConformance(t *testing.T) {
	storetest.RunReconciliationConformance(t, func(t *testing.T) mg.ReconciliationStore {
		_, s := database(t)
		return s
	})
}
