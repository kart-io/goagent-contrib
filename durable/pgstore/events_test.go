package pgstore

import (
	"context"
	"testing"

	"github.com/kart-io/goagent/codec/json"
	"github.com/kart-io/goagent/core/session"
	"github.com/stretchr/testify/require"
)

func TestReadEventPage(t *testing.T) {
	dsn, schema := newSchema(t)
	ctx := context.Background()
	s := newSessionStore(t, dsn, schema)
	pool := newPool(t, dsn, schema)
	// 序号 1..6；类型里的 % 和 _ 用来确认前缀按字面匹配。
	types := []string{"a/x", "b/y", "a/z", "a%/x", "b/y", "a_/x"}
	events := make([]session.Event, len(types))
	for i, typ := range types {
		events[i] = session.Event{Type: typ, Data: json.RawMessage(`{}`)}
	}
	_, err := s.Append(ctx, "s1", 0, events...)
	require.NoError(t, err)

	cases := []struct {
		name   string
		id     string
		after  int64
		limit  int
		filter EventFilter
		want   []int64
	}{
		{name: "零值筛选返回全部", id: "s1", limit: 10, want: []int64{1, 2, 3, 4, 5, 6}},
		{name: "after 不含本身", id: "s1", after: 4, limit: 10, want: []int64{5, 6}},
		{name: "limit 截断", id: "s1", limit: 2, want: []int64{1, 2}},
		{name: "只取指定类型", id: "s1", limit: 10, filter: EventFilter{Types: []string{"b/y"}}, want: []int64{2, 5}},
		{name: "排除指定类型", id: "s1", limit: 10, filter: EventFilter{ExcludeTypes: []string{"b/y", "a/z"}}, want: []int64{1, 4, 6}},
		{name: "排除前缀", id: "s1", limit: 10, filter: EventFilter{ExcludePrefix: "a/"}, want: []int64{2, 4, 5, 6}},
		{name: "前缀中的通配符按字面匹配", id: "s1", limit: 10, filter: EventFilter{ExcludePrefix: "a%"}, want: []int64{1, 2, 3, 5, 6}},
		{name: "limit 只计通过筛选的事件", id: "s1", limit: 2, filter: EventFilter{ExcludePrefix: "a/"}, want: []int64{2, 4}},
		{name: "筛选条件叠加", id: "s1", after: 2, limit: 10, filter: EventFilter{Types: []string{"b/y", "a/z"}, ExcludeTypes: []string{"a/z"}}, want: []int64{5}},
		{name: "会话不存在返回空", id: "missing", limit: 10, want: []int64{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rows, err := ReadEventPage(ctx, pool, c.id, c.after, c.limit, c.filter)
			require.NoError(t, err)
			seqs := make([]int64, len(rows))
			for i, row := range rows {
				seqs[i] = row.Seq
				var ev session.Event
				require.NoError(t, json.Unmarshal(row.Event, &ev))
				require.Equal(t, row.Seq, ev.Seq)
				require.Equal(t, types[row.Seq-1], ev.Type)
			}
			require.Equal(t, c.want, seqs)
		})
	}
}
