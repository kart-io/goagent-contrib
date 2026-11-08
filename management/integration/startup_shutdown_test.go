package integration_test

import (
	"context"
	"errors"
	"github.com/kart-io/goagent/codec/json"
	"testing"
	"time"

	mg "github.com/kart-io/goagent/management"
	"github.com/stretchr/testify/require"
)

// 在真实恢复事务的读取边界等待取消，固定关闭信号与恢复扫描相撞的时序。
type startupRecoveryStore struct {
	mg.Store
	entered chan struct{}
	failure error
}

func (s startupRecoveryStore) WithinTx(ctx context.Context, fn func(mg.Tx) error) error {
	return s.Store.WithinTx(ctx, func(tx mg.Tx) error {
		return fn(startupRecoveryTx{tx, s})
	})
}

type startupRecoveryTx struct {
	mg.Tx
	gate startupRecoveryStore
}

func (tx startupRecoveryTx) List(ctx context.Context, kind string, selection mg.Selection) ([]mg.Document, error) {
	if kind == "tasks" {
		if tx.gate.failure != nil {
			return nil, tx.gate.failure
		}
		close(tx.gate.entered)
		<-ctx.Done()
	}
	return tx.Tx.List(ctx, kind, selection)
}

func TestShutdownDuringStartupRecoveryIsGraceful(t *testing.T) {
	f := setup(t, nil)
	entered := make(chan struct{})
	cfg := f.cfg
	cfg.Store = startupRecoveryStore{Store: f.store, entered: entered}
	service, err := mg.New(cfg)
	require.NoError(t, err)
	w := mg.NewWorker(service)
	ctx, cancel := context.WithCancel(f.ctx)
	done := make(chan struct{})
	var runErr error
	go func() {
		runErr = w.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("恢复扫描中的worker未响应关闭")
		}
	})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("worker未进入恢复扫描")
	}
	stop, stopCancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer stopCancel()
	require.NoError(t, w.Shutdown(stop))
	select {
	case <-done:
	case <-stop.Done():
		t.Fatal("worker关闭后Run未返回")
	}
	require.NoError(t, runErr, "启动恢复阶段的主动关闭应与主循环关闭一致")
	document, err := f.store.Get(f.ctx, "workers", "__system", "default")
	require.NoError(t, err)
	var owner mg.WorkerOwner
	require.NoError(t, json.Unmarshal(document.Data, &owner))
	require.Empty(t, owner.OwnerID, "关闭后应释放已取得的执行权")
}

func TestStartupRecoveryErrorIsNotSuppressed(t *testing.T) {
	f := setup(t, nil)
	failure := errors.New("恢复读取失败")
	cfg := f.cfg
	cfg.Store = startupRecoveryStore{Store: f.store, failure: failure}
	service, err := mg.New(cfg)
	require.NoError(t, err)
	require.ErrorIs(t, mg.NewWorker(service).Run(f.ctx), failure)
	document, err := f.store.Get(f.ctx, "workers", "__system", "default")
	require.NoError(t, err)
	var owner mg.WorkerOwner
	require.NoError(t, json.Unmarshal(document.Data, &owner))
	require.Empty(t, owner.OwnerID, "恢复失败后仍应释放执行权")
}
