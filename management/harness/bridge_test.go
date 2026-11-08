package harness

import (
	"context"
	"github.com/kart-io/goagent-harness/effects"
	"github.com/kart-io/goagent/core/durable"
	"github.com/stretchr/testify/require"
	"testing"
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
