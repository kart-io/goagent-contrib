package harness

import (
	"context"
	"encoding/json"
	"github.com/kart-io/goagent-harness/background"
	"github.com/kart-io/goagent-harness/effects"
	"github.com/kart-io/goagent/core/durable"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

type delivery struct{ ID string }

func TestOutboxRedeliveryKeepsDeliveryID(t *testing.T) {
	store, e := durable.NewFileStore(t.TempDir())
	require.NoError(t, e)
	q, e := effects.New(effects.Config{Store: store, Name: "通知", Kinds: map[string]effects.Kind{"notify": {Run: func(context.Context, effects.Effect) error { return nil }, Idempotent: true}}})
	require.NoError(t, e)
	sink, e := NewSink(q, func(d delivery) (effects.Effect, error) { return effects.Effect{Kind: "notify", Key: d.ID}, nil })
	require.NoError(t, e)
	require.NoError(t, sink.Accept(context.Background(), delivery{"稳定通知"}))
	require.NoError(t, sink.Accept(context.Background(), delivery{"稳定通知"}))
	records, e := q.Records(context.Background())
	require.NoError(t, e)
	require.Len(t, records, 1)
	require.Equal(t, "稳定通知", records[0].Key)
}

func TestUnknownEffectRequiresReconcileBeforeRetry(t *testing.T) {
	for _, withReconcile := range []bool{false, true} {
		t.Run(map[bool]string{false: "attention", true: "reconciled"}[withReconcile], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			store, err := durable.NewFileStore(t.TempDir())
			require.NoError(t, err)
			var calls, checks atomic.Int32
			kind := effects.Kind{Run: func(context.Context, effects.Effect) error { calls.Add(1); return effects.ErrUnconfirmed }}
			if withReconcile {
				kind.Reconcile = func(context.Context, effects.Effect) (bool, error) { checks.Add(1); return true, nil }
			}
			queue, err := effects.New(effects.Config{Store: store, Name: "核对队列", Poll: time.Millisecond, Kinds: map[string]effects.Kind{"notify": kind}})
			require.NoError(t, err)
			sink, err := NewSink(queue, func(d delivery) (effects.Effect, error) {
				return effects.Effect{Kind: "notify", Key: d.ID, Payload: json.RawMessage(`{}`)}, nil
			})
			require.NoError(t, err)
			require.NoError(t, sink.Accept(ctx, delivery{"稳定待办"}))
			done := make(chan error, 1)
			go func() { done <- queue.Run(ctx) }()
			t.Cleanup(func() { cancel(); require.NoError(t, <-done) })
			want := effects.StateAttention
			if withReconcile {
				want = effects.StateDone
			}
			require.Eventually(t, func() bool {
				records, e := queue.Records(ctx)
				return e == nil && len(records) == 1 && records[0].State == want
			}, time.Second, time.Millisecond)
			require.Equal(t, int32(1), calls.Load(), "结果未知不能直接重做外部效果")
			if withReconcile {
				require.Equal(t, int32(1), checks.Load())
			}
		})
	}
}
func TestChildProjectionDoesNotLaunchWorkAndRetentionIsSeparate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, err := durable.NewFileStore(t.TempDir())
	require.NoError(t, err)
	manager, err := background.New(background.Config{Store: store, Name: "子工作", RetainFinished: 1, Poll: time.Millisecond, Notify: func(context.Context, background.Notice) error { return nil }})
	require.NoError(t, err)
	var calls atomic.Int32
	manager.Register("test", func(context.Context, background.Task) (string, error) { calls.Add(1); return "完成", nil })
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	t.Cleanup(func() { cancel(); require.NoError(t, <-done) })
	require.NoError(t, manager.Ready(ctx))
	for _, id := range []string{"child-a", "child-b"} {
		_, err = manager.Spawn(ctx, background.Task{ID: id, Owner: "top-task", Runner: "test", Mode: background.ModeBackground})
		require.NoError(t, err)
		result, e := manager.Wait(ctx, id)
		require.NoError(t, e)
		require.Equal(t, background.StatusCompleted, result.Status)
	}
	for range 3 {
		children, e := Children(ctx, manager, "top-task")
		require.NoError(t, e)
		require.Len(t, children, 1)
		require.Equal(t, "child-b", children[0].ChildID)
		require.Equal(t, "top-task", children[0].TaskID)
	}
	require.Equal(t, int32(2), calls.Load(), "查询和裁剪历史不能启动新的工作")
}
