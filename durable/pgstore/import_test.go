package pgstore

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kart-io/goagent/core/durable"
)

// TestImportFileStore：FileStore 上停在审批等待、并含一条已完成持久操作的运行，导入后在 pgstore 上续跑，已完成的工作只回放。
func TestImportFileStore(t *testing.T) {
	dsn, schema := newSchema(t)
	ctx := context.Background()
	dir := t.TempDir()
	files, err := durable.NewFileStore(dir)
	require.NoError(t, err)

	effects := 0
	steps := []durable.Step{
		{Name: "charge", Run: func(ctx context.Context, in durable.StepInput) (json.RawMessage, error) {
			record, err := durable.Do(ctx, "payment", json.RawMessage(`{}`), durable.OperationPolicy{}, func(context.Context) (json.RawMessage, error) {
				effects++
				return json.RawMessage(`"paid"`), nil
			})
			if err != nil {
				return nil, err
			}
			return record.Value, nil
		}},
		{Name: "ship", RequireApproval: true, Run: func(_ context.Context, in durable.StepInput) (json.RawMessage, error) {
			return json.RawMessage(`"shipped"`), nil
		}},
	}
	snap, err := (&durable.Runner{Store: files, Version: "v1", Steps: steps}).Run(ctx, "order", json.RawMessage(`{}`), "")
	require.ErrorIs(t, err, durable.ErrApprovalRequired)
	approvalKey := snap.Key
	// 作用域数据不导入。
	unlock, err := files.Scope("evidence").Lock(ctx, "e")
	require.NoError(t, err)
	require.NoError(t, files.Scope("evidence").Save(ctx, "e", &durable.Snapshot{RunID: "e", Version: "v1", Data: json.RawMessage(`{}`)}))
	require.NoError(t, unlock())

	pool := newPool(t, dsn, schema)
	result, err := ImportFileStore(ctx, pool, dir)
	require.NoError(t, err)
	require.Equal(t, ImportResult{Imported: 2}, result, "运行快照与持久操作记录各一条")
	result, err = ImportFileStore(ctx, pool, dir)
	require.NoError(t, err)
	require.Equal(t, ImportResult{Existing: 2}, result, "重复导入不覆盖")

	s := newStore(t, dsn, schema)
	_, err = s.Scope("evidence").Load(ctx, "e")
	require.ErrorContains(t, err, "不存在")
	snap, err = (&durable.Runner{Store: s, Version: "v1", Steps: steps}).Run(ctx, "order", nil, approvalKey)
	require.NoError(t, err)
	require.JSONEq(t, `"shipped"`, string(snap.Data))
	require.Equal(t, 1, effects)

	// 直接核对持久操作记录：导入前在 FileStore 上完成的操作，在 pgstore 上以同一执行键调用时回放。
	rootFiles := durable.WithJournal(durable.WithExecutionKey(ctx, "root"), files)
	_, err = durable.Do(rootFiles, "refund", json.RawMessage(`{}`), durable.OperationPolicy{}, func(context.Context) (json.RawMessage, error) {
		return json.RawMessage(`"refunded"`), nil
	})
	require.NoError(t, err)
	_, err = ImportFileStore(ctx, pool, dir)
	require.NoError(t, err)
	rootPG := durable.WithJournal(durable.WithExecutionKey(ctx, "root"), s)
	record, err := durable.Do(rootPG, "refund", json.RawMessage(`{}`), durable.OperationPolicy{}, func(context.Context) (json.RawMessage, error) {
		t.Fatal("导入的已完成操作不应重新执行")
		return nil, nil
	})
	require.NoError(t, err)
	require.True(t, record.Replayed)
	require.JSONEq(t, `"refunded"`, string(record.Value))
}
