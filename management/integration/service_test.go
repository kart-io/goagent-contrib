package integration_test

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	journal "github.com/kart-io/goagent-contrib/durable/pgstore"
	mg "github.com/kart-io/goagent-contrib/management"
	db "github.com/kart-io/goagent-contrib/management/pgstore"
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
	"time"
)

type factory struct{}

func (factory) Validate(context.Context, mg.Version) (mg.Capabilities, error) {
	return mg.Capabilities{Approval: true, Fork: true, Input: true}, nil
}
func (factory) Build(context.Context, mg.Version, mg.Resources) (mg.Binding, error) {
	return mg.Binding{Runtime: runtime{}}, nil
}

type runtime struct{}

func (runtime) Run(context.Context, mg.RunSpec) (mg.RunResult, error) {
	return mg.RunResult{Outcome: "completed", Output: json.RawMessage(`"已完成"`), FinishedAt: time.Now()}, nil
}
func (runtime) Inspect(context.Context, mg.RunSpec) (mg.RunEvidence, error) {
	return mg.RunEvidence{State: "absent"}, nil
}

type fixture struct {
	schema string
	cfg    mg.Config
	ctx    context.Context
	s      *mg.Service
	store  *db.Store
	pool   *pgxpool.Pool
	actor  mg.Actor
}

func setup(t *testing.T, factories map[string]mg.Factory) *fixture {
	t.Helper()
	ctx := context.Background()
	dsn := os.Getenv("GOAGENT_PGSTORE_DSN")
	if dsn == "" {
		t.Skip("需要本地 Postgres")
	}
	schema := os.Getenv("MG_RECOVERY_SCHEMA")
	if schema == "" {
		schema = "case_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		admin, e := pgxpool.New(ctx, dsn)
		require.NoError(t, e)
		_, e = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize())
		require.NoError(t, e)
		t.Cleanup(func() {
			_, e := admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
			require.NoError(t, e)
			admin.Close()
		})
	}
	poolCfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	poolCfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(80261109)`); e != nil {
			return e
		}
		var exists bool
		if e := tx.QueryRow(ctx, `SELECT to_regclass('goagent_durable_schema') IS NOT NULL`).Scan(&exists); e != nil {
			return e
		}
		if !exists {
			if _, e := tx.Exec(ctx, journal.Schema+journal.SessionSchema); e != nil {
				return e
			}
		}
		return nil
	}))
	require.NoError(t, db.Migrate(ctx, pool))
	store, err := db.New(ctx, pool)
	require.NoError(t, err)
	j, err := journal.New(ctx, journal.Config{Pool: pool, LeaseTTL: 3 * time.Second, RenewInterval: 500 * time.Millisecond})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, j.Close(ctx)) })
	sessions, err := journal.NewSessionStore(ctx, j)
	require.NoError(t, err)
	if factories == nil {
		factories = map[string]mg.Factory{"test": factory{}}
	}
	cfg := mg.Config{Store: store, Sessions: sessions, HistoryReader: store, Authorizer: mg.TenantAuthorizer{}, Factories: factories}
	s, err := mg.New(cfg)
	require.NoError(t, err)
	return &fixture{schema, cfg, ctx, s, store, pool, mg.Actor{TenantID: uuid.NewString(), Subject: "验收操作人"}}
}
func (f *fixture) agent(t *testing.T) (mg.Agent, mg.Version) {
	t.Helper()
	a, e := f.s.RegisterAgent(f.ctx, f.actor, mg.RegisterAgentRequest{IdempotencyKey: uuid.NewString(), Name: "客服助手"})
	require.NoError(t, e)
	v, e := f.s.SaveVersion(f.ctx, f.actor, mg.SaveVersionRequest{IdempotencyKey: uuid.NewString(), AgentID: a.ID, Version: "v1", RunnerKind: "test", ImplementationVersion: "code-v1", Config: json.RawMessage(`{}`)})
	require.NoError(t, e)
	a, e = f.s.PublishVersion(f.ctx, f.actor, mg.PublishRequest{IdempotencyKey: uuid.NewString(), AgentID: a.ID, Version: "v1", ExpectedRevision: a.Revision})
	require.NoError(t, e)
	return a, v
}
func TestVersionCannotBeOverwritten(t *testing.T) {
	f := setup(t, nil)
	a, _ := f.agent(t)
	_, err := f.s.SaveVersion(f.ctx, f.actor, mg.SaveVersionRequest{IdempotencyKey: uuid.NewString(), AgentID: a.ID, Version: "v1", RunnerKind: "test", ImplementationVersion: "code-v2", Config: json.RawMessage(`{}`)})
	require.ErrorIs(t, err, mg.ErrConflict)
}
func TestSessionKeepsPublishedVersion(t *testing.T) {
	f := setup(t, nil)
	a, _ := f.agent(t)
	s, err := f.s.CreateSession(f.ctx, f.actor, mg.CreateSessionRequest{IdempotencyKey: uuid.NewString(), AgentID: a.ID})
	require.NoError(t, err)
	require.Equal(t, "preparing", s.Status)
	_, err = f.s.SaveVersion(f.ctx, f.actor, mg.SaveVersionRequest{IdempotencyKey: uuid.NewString(), AgentID: a.ID, Version: "v2", RunnerKind: "test", ImplementationVersion: "code-v2", Config: json.RawMessage(`{}`)})
	require.NoError(t, err)
	_, err = f.s.PublishVersion(f.ctx, f.actor, mg.PublishRequest{IdempotencyKey: uuid.NewString(), AgentID: a.ID, Version: "v2", ExpectedRevision: a.Revision})
	require.NoError(t, err)
	got, err := f.s.GetSession(f.ctx, f.actor, s.ID)
	require.NoError(t, err)
	require.Equal(t, "v1", got.Version)
}
