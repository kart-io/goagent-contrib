package pgstore

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EventFilter 是下推到查询的事件类型筛选，零值不筛选。
type EventFilter struct {
	// Types 非空时只返回这些类型。
	Types []string
	// ExcludeTypes 与 ExcludePrefix 排除指定类型和以该前缀开头的类型。
	ExcludeTypes  []string
	ExcludePrefix string
}

// EventRow 是 ReadEventPage 返回的一条事件：序号与写入时的原文。
type EventRow struct {
	Seq   int64
	Event []byte
}

// ReadEventPage 按 seq 升序返回会话中 seq > after 且通过筛选的事件，最多 limit 条（limit 须为正）。
// 会话不存在时返回空结果。它只读、不经过租约，供需要在数据库侧筛选和分页的读取方使用，
// 这样其他模块不必自己写本包的表名。
func ReadEventPage(ctx context.Context, pool *pgxpool.Pool, id string, after int64, limit int, f EventFilter) ([]EventRow, error) {
	// nil 切片会绑定成 NULL，使 cardinality 与 ANY 的结果变成 NULL。
	types, exclude := f.Types, f.ExcludeTypes
	if types == nil {
		types = []string{}
	}
	if exclude == nil {
		exclude = []string{}
	}
	rows, err := pool.Query(ctx, `SELECT seq, event FROM goagent_session_event
WHERE session_id = $1 AND seq > $2
  AND (cardinality($3::text[]) = 0 OR type = ANY($3::text[]))
  AND NOT (type = ANY($4::text[]))
  AND ($5 = '' OR NOT starts_with(type, $5))
ORDER BY seq LIMIT $6`, id, after, types, exclude, f.ExcludePrefix, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (r EventRow, err error) {
		err = row.Scan(&r.Seq, &r.Event)
		return r, err
	})
}
