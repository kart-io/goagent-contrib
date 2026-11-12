package pgstore

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kart-io/goagent/core/durable"
)

// 快照列的 lz4 调优：迁移前写入的 pglz 值、迁移后写入的 lz4 值、回滚后再写入的值，新旧值都能读出；
// 每一步之后存储契约测试的大值往返不受影响。
func TestSnapshotCompressionTuning(t *testing.T) {
	dsn, schema := newSchema(t)
	ctx := context.Background()
	pool := newPool(t, dsn, schema)
	s := newStore(t, dsn, schema)
	// 可压缩的大值，超过 TOAST 阈值。
	payload := func(tag string) *durable.Snapshot {
		return &durable.Snapshot{Version: "v", Plan: "p", RunID: tag, Data: []byte(`"` + strings.Repeat(tag+"可压缩的内容", 4000) + `"`)}
	}
	compression := func(id string) string {
		var c *string
		require.NoError(t, pool.QueryRow(ctx, `SELECT pg_column_compression(data) FROM goagent_durable_snapshot WHERE ns = '' AND id = $1`, id).Scan(&c))
		if c == nil {
			return ""
		}
		return *c
	}
	write := func(id string) {
		unlock, err := s.Lock(ctx, id)
		require.NoError(t, err)
		require.NoError(t, s.Save(ctx, id, payload(id)))
		require.NoError(t, unlock())
	}
	check := func(ids ...string) {
		for _, id := range ids {
			got, err := s.Load(ctx, id)
			require.NoError(t, err)
			require.Equal(t, string(payload(id).Data), string(got.Data))
		}
	}

	write("before")
	require.Equal(t, "pglz", compression("before"))
	_, err := pool.Exec(ctx, SnapshotCompressionLZ4)
	require.NoError(t, err)
	write("after")
	require.Equal(t, "lz4", compression("after"))
	require.Equal(t, "pglz", compression("before"), "已有的值保持原压缩方式")
	check("before", "after")

	_, err = pool.Exec(ctx, SnapshotCompressionPGLZ)
	require.NoError(t, err)
	write("rollback")
	require.Equal(t, "pglz", compression("rollback"))
	check("before", "after", "rollback")
}
