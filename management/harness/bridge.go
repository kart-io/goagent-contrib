// Package harness 可选地关联子工作与外部待办，不拥有顶层 Task。
// 泛型 Sink 不依赖尚未发布的 management 模块，保持两个模块可独立验证。
package harness

import (
	"context"
	"errors"
	"github.com/kart-io/goagent-harness/background"
	"github.com/kart-io/goagent-harness/effects"
)

type Sink[D any] struct {
	queue  *effects.Queue
	encode func(D) (effects.Effect, error)
}

func NewSink[D any](q *effects.Queue, encode func(D) (effects.Effect, error)) (*Sink[D], error) {
	if q == nil || encode == nil {
		return nil, errors.New("效果队列与编码器必须存在")
	}
	return &Sink[D]{q, encode}, nil
}

// Accept 只表示进入 effects 队列；目标必须按 Effect.Key 去重，不能把队列合并当永久去重。
func (s *Sink[D]) Accept(ctx context.Context, d D) error {
	effect, err := s.encode(d)
	if err != nil {
		return err
	}
	if effect.Kind == "" || effect.Key == "" {
		return errors.New("外部待办需要稳定种类与键")
	}
	return s.queue.Enqueue(ctx, effect)
}

type Child struct {
	TaskID      string            `json:"task_id"`
	ChildID     string            `json:"child_id"`
	Status      background.Status `json:"status"`
	Generation  int               `json:"generation"`
	FailureKind string            `json:"failure_kind,omitempty"`
}

func Children(ctx context.Context, m *background.Manager, taskID string) ([]Child, error) {
	if m == nil || taskID == "" {
		return nil, errors.New("子任务查询需要 Manager 与父 TaskID")
	}
	records, err := m.Records(ctx, taskID)
	if err != nil {
		return nil, err
	}
	out := make([]Child, 0, len(records))
	for _, r := range records {
		out = append(out, Child{taskID, r.ID, r.Status, r.Generation, r.FailureKind})
	}
	return out, nil
}
