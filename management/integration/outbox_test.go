package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	mg "github.com/kart-io/goagent-contrib/management"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

type flakyDelivery struct {
	mu   sync.Mutex
	ids  []string
	done bool
}

func (s *flakyDelivery) Accept(_ context.Context, d mg.Delivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ids = append(s.ids, d.ID)
	if len(s.ids) == 1 {
		return errors.New("注入首次网络失败")
	}
	s.done = true
	return nil
}
func TestNotifyFailureDoesNotRerunTask(t *testing.T) {
	f := setup(t, nil)
	sink := &flakyDelivery{}
	cfg := f.cfg
	cfg.Delivery = sink
	var e error
	f.s, e = mg.New(cfg)
	require.NoError(t, e)
	f.worker(t)
	ctx, cancel := context.WithCancel(f.ctx)
	done := make(chan error, 1)
	go func() { done <- mg.NewOutboxDispatcher(f.s).Run(ctx) }()
	t.Cleanup(func() { cancel(); require.NoError(t, <-done) })
	se := f.session(t)
	task, e := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"通知重投"`)})
	require.NoError(t, e)
	require.Eventually(t, func() bool { sink.mu.Lock(); defer sink.mu.Unlock(); return sink.done }, 6*time.Second, 20*time.Millisecond)
	sink.mu.Lock()
	ids := append([]string(nil), sink.ids...)
	sink.mu.Unlock()
	require.Len(t, ids, 2)
	require.Equal(t, ids[0], ids[1])
	run, e := f.s.GetRun(f.ctx, f.actor, task.ID)
	require.NoError(t, e)
	require.Equal(t, mg.Succeeded, run.Task.State)
	require.Len(t, run.Attempts, 1)
}
