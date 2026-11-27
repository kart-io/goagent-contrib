// Package pgstore 实现管理实体的独立 SQL 事务，不把 Agent 运行包在长事务中。
package pgstore

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	durablepg "github.com/kart-io/goagent-contrib/durable/pgstore"
	"github.com/kart-io/goagent/codec/json"
	"github.com/kart-io/goagent/constants"
	"github.com/kart-io/goagent/core/session"
	agentErrors "github.com/kart-io/goagent/errors"
	mg "github.com/kart-io/goagent/management"
	"strings"
)

type executor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}
type Store struct{ pool *pgxpool.Pool }
type transaction struct{ db executor }

func table(kind string) (string, error) {
	for _, k := range mg.Kinds {
		if k == kind {
			return "mg_" + k, nil
		}
	}
	return "", agentErrors.ErrManagementInvalid
}

// Migrate 必须由宿主显式调用；版本检查在 New 中完成。
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(80261108)`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS mg_schema(version integer PRIMARY KEY CHECK(version=1)); INSERT INTO mg_schema VALUES(1) ON CONFLICT DO NOTHING`); err != nil {
			return err
		}
		for _, kind := range mg.Kinds {
			tbl, _ := table(kind)
			_, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+tbl+`(tenant_id text NOT NULL,id text NOT NULL,revision bigint NOT NULL CHECK(revision>0),created_at timestamptz NOT NULL,data jsonb NOT NULL,PRIMARY KEY(tenant_id,id),CHECK(data->>'id'=id),CHECK(data->>'tenant_id'=tenant_id),CHECK((data->>'revision')::bigint=revision)); CREATE INDEX IF NOT EXISTS `+tbl+`_created ON `+tbl+`(tenant_id,created_at,id)`)
			if err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, reconciliationSchema); err != nil {
			return err
		}
		for _, link := range []struct{ child, field, parent string }{{"versions", "agent_id", "agents"}, {"sessions", "agent_id", "agents"}, {"tasks", "session_id", "sessions"}, {"tasks", "agent_id", "agents"}, {"approvals", "task_id", "tasks"}, {"attempts", "task_id", "tasks"}} {
			name := "mg_" + link.child
			col := link.field
			if _, err := tx.Exec(ctx, `ALTER TABLE `+name+` ADD COLUMN IF NOT EXISTS `+col+` text GENERATED ALWAYS AS (data->>'`+col+`') STORED`); err != nil {
				return err
			}
			constraint := name + "_" + col + "_fk"
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conname=$1 AND conrelid=$2::regclass)`, constraint, name).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				if _, err := tx.Exec(ctx, `ALTER TABLE `+name+` ADD CONSTRAINT `+constraint+` FOREIGN KEY(tenant_id,`+col+`) REFERENCES mg_`+link.parent+`(tenant_id,id)`); err != nil {
					return err
				}
			}
		}
		for _, k := range []string{"sessions", "tasks"} {
			name := "mg_" + k
			if _, err := tx.Exec(ctx, `ALTER TABLE `+name+` ADD COLUMN IF NOT EXISTS version_id text GENERATED ALWAYS AS ((data->>'agent_id')||'@'||(data->>'version')) STORED`); err != nil {
				return err
			}
			var exists bool
			constraint := name + "_version_fk"
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conname=$1 AND conrelid=$2::regclass)`, constraint, name).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				if _, err := tx.Exec(ctx, `ALTER TABLE `+name+` ADD CONSTRAINT `+constraint+` FOREIGN KEY(tenant_id,version_id) REFERENCES mg_versions(tenant_id,id)`); err != nil {
					return err
				}
			}
		}
		for _, kind := range []string{"tasks", "commands", "approvals", "outbox"} {
			name := "mg_" + kind
			if _, err := tx.Exec(ctx, `CREATE INDEX IF NOT EXISTS `+name+`_state ON `+name+`(tenant_id,(data->>'state'),created_at,id)`); err != nil {
				return err
			}
		}
		return nil
	})
}
func New(ctx context.Context, pool *pgxpool.Pool) (*Store, error) {
	if pool == nil {
		return nil, agentErrors.ErrManagementInvalid
	}
	var v int
	if err := pool.QueryRow(ctx, `SELECT version FROM mg_schema`).Scan(&v); err != nil {
		return nil, err
	}
	if v != 1 {
		return nil, agentErrors.ErrManagementUnsupported
	}
	return &Store{pool}, nil
}
func (s *Store) WithinTx(ctx context.Context, fn func(mg.Tx) error) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// 首批单执行宿主，管理写入使用有界短事务串行锁，避免跨实体锁顺序倒置。
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(80261108)`); err != nil {
			return err
		}
		return fn(transaction{tx})
	})
}
func (s *Store) Get(ctx context.Context, kind, tenant, id string) (mg.Document, error) {
	return transaction{s.pool}.Get(ctx, kind, tenant, id)
}
func (s *Store) List(ctx context.Context, kind string, q mg.Selection) ([]mg.Document, error) {
	return transaction{s.pool}.List(ctx, kind, q)
}
func (t transaction) Get(ctx context.Context, kind, tenant, id string) (d mg.Document, err error) {
	tbl, err := table(kind)
	if err != nil {
		return d, err
	}
	d.Kind = kind
	err = t.db.QueryRow(ctx, `SELECT tenant_id,id,revision,created_at,data FROM `+tbl+` WHERE tenant_id=$1 AND id=$2`, tenant, id).Scan(&d.TenantID, &d.ID, &d.Revision, &d.CreatedAt, &d.Data)
	if errors.Is(err, pgx.ErrNoRows) {
		err = agentErrors.ErrManagementNotFound
	}
	return
}
func (t transaction) Insert(ctx context.Context, d mg.Document) error {
	tbl, err := table(d.Kind)
	if err != nil {
		return err
	}
	_, err = t.db.Exec(ctx, `INSERT INTO `+tbl+`(tenant_id,id,revision,created_at,data)VALUES($1,$2,$3,$4,$5)`, d.TenantID, d.ID, d.Revision, d.CreatedAt, d.Data)
	var pe *pgconn.PgError
	if errors.As(err, &pe) && (pe.Code == "23505" || pe.Code == "23503") {
		return agentErrors.ErrManagementConflict
	}
	return err
}
func (t transaction) CAS(ctx context.Context, d mg.Document, expected int64) error {
	tbl, err := table(d.Kind)
	if err != nil {
		return err
	}
	tag, err := t.db.Exec(ctx, `UPDATE `+tbl+` SET revision=$1,data=$2 WHERE tenant_id=$3 AND id=$4 AND revision=$5`, d.Revision, d.Data, d.TenantID, d.ID, expected)
	if err == nil && tag.RowsAffected() != 1 {
		return agentErrors.ErrManagementConflict
	}
	return err
}
func (t transaction) Delete(ctx context.Context, kind, tenant, id string, rev int64) error {
	tbl, err := table(kind)
	if err != nil {
		return err
	}
	tag, err := t.db.Exec(ctx, `DELETE FROM `+tbl+` WHERE tenant_id=$1 AND id=$2 AND revision=$3`, tenant, id, rev)
	if err == nil && tag.RowsAffected() != 1 {
		return agentErrors.ErrManagementConflict
	}
	return err
}

// scanPage is the internal page size for unlimited scans; each page is a
// separate short query keyed on (created_at,id), so no single read is unbounded.
const scanPage = 1000

// List returns every matching row when Query.Limit is 0 and exactly Limit
// rows otherwise; a negative Limit is invalid.
func (t transaction) List(ctx context.Context, kind string, sel mg.Selection) ([]mg.Document, error) {
	if sel.Query.Limit < 0 {
		return nil, agentErrors.ErrManagementInvalid
	}
	if sel.Query.Limit > 0 {
		return t.listPage(ctx, kind, sel, sel.Query.Limit)
	}
	var out []mg.Document
	for {
		page, err := t.listPage(ctx, kind, sel, scanPage)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if len(page) < scanPage {
			if out == nil {
				out = []mg.Document{}
			}
			return out, nil
		}
		last := page[len(page)-1]
		sel.Query.Cursor = mg.FormatCursor(last.CreatedAt, last.ID)
	}
}
func (t transaction) listPage(ctx context.Context, kind string, sel mg.Selection, limit int) ([]mg.Document, error) {
	tbl, err := table(kind)
	if err != nil {
		return nil, err
	}
	args := []any{}
	where := []string{"true"}
	add := func(value any) string { args = append(args, value); return fmt.Sprintf("$%d", len(args)) }
	if sel.TenantID != "" {
		where = append(where, "tenant_id="+add(sel.TenantID))
	} else if !sel.Visibility.All {
		return nil, agentErrors.ErrManagementForbidden
	}
	q := sel.Query
	for _, f := range []struct{ field, value string }{{"agent_id", q.AgentID}, {"session_id", q.SessionID}, {"task_id", q.TaskID}} {
		if f.value != "" {
			where = append(where, "data->>'"+f.field+"'="+add(f.value))
		}
	}
	if len(q.Statuses) > 0 {
		where = append(where, "COALESCE(data->>'state',data->>'status')=ANY("+add(q.Statuses)+")")
	}
	if !q.CreatedFrom.IsZero() {
		where = append(where, "created_at>="+add(q.CreatedFrom))
	}
	if !q.CreatedTo.IsZero() {
		where = append(where, "created_at<="+add(q.CreatedTo))
	}
	if q.Cursor != "" {
		at, id, err := mg.ParseCursor(q.Cursor)
		if err != nil {
			return nil, err
		}
		where = append(where, "(created_at,id)>("+add(at)+","+add(id)+")")
	}
	v := sel.Visibility
	if !v.All {
		vis := []string{"false"}
		if len(v.ResourceIDs) > 0 {
			vis = append(vis, "id=ANY("+add(v.ResourceIDs)+")")
		}
		if len(v.AgentIDs) > 0 {
			col := "data->>'agent_id'"
			if kind == "agents" {
				col = "id"
			}
			vis = append(vis, col+"=ANY("+add(v.AgentIDs)+")")
		}
		if len(v.SessionIDs) > 0 {
			col := "data->>'session_id'"
			if kind == "sessions" {
				col = "id"
			}
			vis = append(vis, col+"=ANY("+add(v.SessionIDs)+")")
		}
		where = append(where, "("+strings.Join(vis, " OR ")+")")
	}
	rows, err := t.db.Query(ctx, `SELECT tenant_id,id,revision,created_at,data FROM `+tbl+` WHERE `+strings.Join(where, " AND ")+` ORDER BY created_at,id LIMIT `+add(limit), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []mg.Document{}
	for rows.Next() {
		d := mg.Document{Kind: kind}
		if err = rows.Scan(&d.TenantID, &d.ID, &d.Revision, &d.CreatedAt, &d.Data); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// History implements management.HistoryReader. The type filter and the page
// size are applied by the database; the page is assembled by
// management.FilterHistory so it has the same shape as every other reader.
// Unlimited reads go through seq-ordered pages of scanPage.
func (s *Store) History(ctx context.Context, id string, after int64, limit int, types []string) (mg.HistoryPage, error) {
	empty := mg.HistoryPage{Items: []session.Event{}, NextSeq: after}
	if limit < 0 {
		return empty, agentErrors.ErrManagementInvalid
	}
	size := limit
	if limit == 0 {
		size = scanPage
	}
	filter := durablepg.EventFilter{Types: types, ExcludeTypes: []string{constants.SessionEventBoundarySnapshot}, ExcludePrefix: mg.HistoryHiddenPrefix}
	var events []session.Event
	next := after
	for {
		rows, err := durablepg.ReadEventPage(ctx, s.pool, id, next, size, filter)
		if err != nil {
			return empty, err
		}
		for _, row := range rows {
			var ev session.Event
			if err = json.Unmarshal(row.Event, &ev); err != nil {
				return empty, err
			}
			events = append(events, ev)
			next = row.Seq
		}
		if limit > 0 || len(rows) < size {
			return mg.FilterHistory(events, after, limit, types)
		}
	}
}
