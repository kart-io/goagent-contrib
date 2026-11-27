package pgstore

import (
	"strings"
	"testing"

	mg "github.com/kart-io/goagent/management"
	"github.com/stretchr/testify/require"
)

// Schema 是手写的常量；核心新增实体种类时这里必须跟着加表。
func TestSchemaCoversEveryKind(t *testing.T) {
	for _, kind := range mg.Kinds {
		require.Contains(t, Schema, "CREATE TABLE IF NOT EXISTS mg_"+kind+" (", kind)
	}
	require.Equal(t, len(mg.Kinds)+2, strings.Count(Schema, "CREATE TABLE IF NOT EXISTS "), "mg_schema、mg_reconciliations 加每种实体各一张表")
}

func TestSchemaIsRepeatable(t *testing.T) {
	ctx, s := database(t)
	for range 2 {
		_, err := s.pool.Exec(ctx, Schema)
		require.NoError(t, err)
	}
	// 约束按名字判断是否存在，重复执行不能叠加出第二份。
	var n int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM pg_constraint WHERE conrelid='mg_tasks'::regclass AND contype='f'`).Scan(&n))
	require.Equal(t, 3, n)
}
