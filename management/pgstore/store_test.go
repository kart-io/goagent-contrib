package pgstore

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
	durablepg "github.com/kart-io/goagent-contrib/durable/pgstore"
	"github.com/kart-io/goagent/codec/json"
	agentErrors "github.com/kart-io/goagent/errors"
	mg "github.com/kart-io/goagent/management"
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
	require.ErrorIs(t, err, agentErrors.ErrManagementNotFound)
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
		} else if errors.Is(err, agentErrors.ErrManagementConflict) {
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
	}), agentErrors.ErrManagementConflict)
}

func seedSessionEvents(t *testing.T, ctx context.Context, s *Store, id string, n int) {
	t.Helper()
	var present bool
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT to_regclass('goagent_session_event') IS NOT NULL`).Scan(&present))
	if !present {
		_, err := s.pool.Exec(ctx, durablepg.SessionSchema)
		require.NoError(t, err)
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO goagent_session(id,last_seq) VALUES($1,$2)`, id, n)
	require.NoError(t, err)
	_, err = s.pool.Exec(ctx, `INSERT INTO goagent_session_event(session_id,seq,type,event) SELECT $1,g,'message/user',convert_to('{"seq":'||g||',"type":"message/user","time":"2026-01-01T00:00:00Z","data":{}}','UTF8') FROM generate_series(1,$2::int) g`, id, n)
	require.NoError(t, err)
}

func TestHistoryPageBeyond200(t *testing.T) {
	ctx, s := database(t)
	const total = 2500
	id := "hist-" + time.Now().Format("150405.000000000")
	seedSessionEvents(t, ctx, s, id, total)
	for _, tc := range []struct {
		name  string
		limit int
		want  int
	}{{"above former cap", 201, 201}, {"far above", 1000, 1000}, {"beyond total", total + 5, total}, {"zero is unlimited", 0, total}} {
		t.Run(tc.name, func(t *testing.T) {
			page, err := s.History(ctx, id, 0, tc.limit, nil)
			require.NoError(t, err)
			require.Len(t, page.Items, tc.want)
			require.Equal(t, int64(tc.want), page.NextSeq)
			for i, ev := range page.Items {
				require.Equal(t, int64(i+1), ev.Seq)
			}
		})
	}
	page, err := s.History(ctx, id, 2400, 0, nil)
	require.NoError(t, err)
	require.Len(t, page.Items, 100)
	_, err = s.History(ctx, id, 0, -1, nil)
	require.ErrorIs(t, err, agentErrors.ErrManagementInvalid)
}

func TestListWithoutLimitReturnsEveryRow(t *testing.T) {
	ctx, s := database(t)
	tenant := "list-" + time.Now().Format("150405.000000000")
	const total = 2300
	base := time.Now().UTC()
	_, err := s.pool.Exec(ctx, `INSERT INTO mg_agents(tenant_id,id,revision,created_at,data) SELECT $1,'a'||lpad(g::text,5,'0'),1,$2::timestamptz+g*interval '1 millisecond',jsonb_build_object('id','a'||lpad(g::text,5,'0'),'tenant_id',$1::text,'revision',1) FROM generate_series(1,$3::int) g`, tenant, base, total)
	require.NoError(t, err)
	sel := func(limit int) mg.Selection {
		return mg.Selection{TenantID: tenant, Query: mg.Query{Limit: limit}, Visibility: mg.Visibility{All: true}}
	}
	docs, err := s.List(ctx, "agents", sel(0))
	require.NoError(t, err)
	require.Len(t, docs, total)
	for i := 1; i < len(docs); i++ {
		require.Less(t, docs[i-1].ID, docs[i].ID)
	}
	docs, err = s.List(ctx, "agents", sel(10001+5))
	require.NoError(t, err)
	require.Len(t, docs, total, "the former 10001 cap must be gone")
	_, err = s.List(ctx, "agents", sel(-1))
	require.ErrorIs(t, err, agentErrors.ErrManagementInvalid)
}
