package integration_test

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	mg "github.com/kart-io/goagent-contrib/management"
	"github.com/kart-io/goagent-contrib/management/adapters"
	"github.com/kart-io/goagent/workflow"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

type reviewState struct {
	Amount   int    `json:"amount"`
	Decision string `json:"decision"`
	Applied  bool   `json:"applied"`
}

func TestGraphDecisionAfterLostAck(t *testing.T) {
	for _, decision := range []string{"approve", "reject"} {
		t.Run(decision, func(t *testing.T) {
			var effects atomic.Int32
			factory := adapters.GraphFactory[reviewState]{Check: func(context.Context, mg.Version) error { return nil }, Initial: func(raw json.RawMessage) (s reviewState, e error) { e = json.Unmarshal(raw, &s); return }, BuildGraph: func(ctx context.Context, v mg.Version, r mg.Resources) (*workflow.DurableGraph[reviewState], func(context.Context) error, error) {
				g := workflow.NewGraph[reviewState]()
				if e := g.AddNode("review", func(ctx context.Context, s reviewState) (reviewState, error) {
					if s.Decision == "approve" {
						effects.Add(1)
						s.Applied = true
					}
					return s, nil
				}); e != nil {
					return nil, nil, e
				}
				if e := g.AddEdge("__start__", "review"); e != nil {
					return nil, nil, e
				}
				if e := g.AddEdge("review", "__end__"); e != nil {
					return nil, nil, e
				}
				compiled, e := g.CompileDurable(ctx, workflow.DurableConfig{Store: r.Journal, Version: v.ConfigHash, ApprovalNodes: []string{"review"}}, workflow.WithResumeHandler(func(_ context.Context, _ string, snap *workflow.GraphSnapshot[reviewState], raw json.RawMessage) (reviewState, error) {
					var answer mg.ApprovalAnswer
					if e := json.Unmarshal(raw, &answer); e != nil {
						return snap.State, e
					}
					s := snap.State
					s.Decision = answer.Decision
					return s, nil
				}))
				return compiled, nil, e
			}}
			f := setup(t, map[string]mg.Factory{"test": factory})
			f.worker(t)
			se := f.session(t)
			task, e := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`{"amount":50}`)})
			require.NoError(t, e)
			var ap mg.Approval
			require.Eventually(t, func() bool {
				p, e := f.s.ListApprovals(f.ctx, f.actor, mg.Query{TaskID: task.ID})
				if e == nil && len(p.Items) == 1 {
					ap = p.Items[0]
					return true
				}
				return false
			}, 5*time.Second, 10*time.Millisecond)
			req := mg.DecisionRequest{IdempotencyKey: uuid.NewString(), ApprovalID: ap.ID, ExpectedRevision: ap.Revision, Decision: decision}
			_, e = f.s.DecideApproval(f.ctx, f.actor, req)
			require.NoError(t, e)
			_, e = f.s.DecideApproval(f.ctx, f.actor, req)
			require.NoError(t, e)
			require.Eventually(t, func() bool { v, e := f.s.GetTask(f.ctx, f.actor, task.ID); return e == nil && v.State == mg.Succeeded }, 5*time.Second, 10*time.Millisecond)
			want := int32(0)
			if decision == "approve" {
				want = 1
			}
			require.Equal(t, want, effects.Load())
		})
	}
}
