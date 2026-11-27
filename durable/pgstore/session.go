package pgstore

import (
	"context"
	"fmt"

	"github.com/kart-io/goagent/core/durable/leasestore"
)

// SessionStore 实现 session.Store 与 session.Deleter，供多个实例共享会话日志；与创建它的 Store 共享连接池、租约与续约，
// 关闭 Store 即停止它的锁。语义见 leasestore.SessionStore。
type SessionStore struct {
	*leasestore.SessionStore
	root *Store
}

// NewSessionStore 校验会话日志的表结构版本。
func NewSessionStore(ctx context.Context, root *Store) (*SessionStore, error) {
	if root == nil {
		return nil, fmt.Errorf("pgstore.NewSessionStore 需要 Store")
	}
	var version int
	if err := root.cfg.Pool.QueryRow(ctx, `SELECT version FROM goagent_session_schema`).Scan(&version); err != nil {
		return nil, fmt.Errorf("读取会话日志表结构版本失败：%w", err)
	}
	if version != SessionSchemaVersion {
		return nil, fmt.Errorf("会话日志表结构版本为 %d，需要 %d", version, SessionSchemaVersion)
	}
	inner, err := leasestore.NewSessionStore(root.Store, root.backend)
	if err != nil {
		return nil, err
	}
	return &SessionStore{SessionStore: inner, root: root}, nil
}
