package adapters

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	mg "github.com/kart-io/goagent-contrib/management"
	"github.com/kart-io/goagent/builder"
	"github.com/kart-io/goagent/core/budget"
	"github.com/kart-io/goagent/core/durable"
	"github.com/kart-io/goagent/core/session"
	"github.com/kart-io/goagent/tools"
)

// NativeFactory 由宿主提供固定实现版本的 Builder；适配器强制绑定管理身份与日志。
type NativeFactory struct {
	Check      func(context.Context, mg.Version) error
	BuildAgent func(context.Context, mg.Version, mg.Resources, tools.Approver) (*builder.SimpleAgentBuilder, func(context.Context) error, error)
}

func (f NativeFactory) Validate(ctx context.Context, v mg.Version) (mg.Capabilities, error) {
	if f.BuildAgent == nil || f.Check == nil {
		return mg.Capabilities{}, mg.ErrInvalid
	}
	return mg.Capabilities{Approval: true, Fork: true, Input: true}, f.Check(ctx, v)
}
func (f NativeFactory) Build(ctx context.Context, v mg.Version, r mg.Resources) (mg.Binding, error) {
	b, cleanup, err := f.BuildAgent(ctx, v, r, approver{r: r})
	binding := mg.Binding{Cleanup: cleanup}
	if err != nil {
		return binding, err
	}
	if b == nil {
		return binding, mg.ErrInvalid
	}
	agent, err := b.WithSessionID(r.Session.LogID).WithSessionLog(r.Sessions).WithDurableScope(v.AgentID + "@" + v.Version + "/" + v.ConfigHash).WithAgentID(v.AgentID).BuildContext(ctx)
	if err != nil {
		return binding, err
	}
	binding.Runtime = &native{start: agent.ExecuteStream, r: r}
	return binding, nil
}

type native struct {
	start func(context.Context, interface{}) (<-chan builder.AgentEvent, error)
	r     mg.Resources
}

func inputValue(raw json.RawMessage) (any, error) {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text, nil
	}
	var value any
	dec := json.NewDecoder(bytesReader(raw))
	dec.UseNumber()
	err := dec.Decode(&value)
	return value, err
}
func (n *native) Run(ctx context.Context, spec mg.RunSpec) (mg.RunResult, error) {
	for k, v := range spec.ResumeInputs {
		ctx = durable.WithResume(ctx, k, v)
	}
	input, err := inputValue(spec.Task.Input)
	if err != nil {
		return mg.RunResult{}, err
	}
	events, err := n.start(ctx, input)
	if err != nil {
		return mg.RunResult{}, err
	}
	var output *builder.AgentOutput
	for ev := range events {
		if ev.Error != nil {
			err = ev.Error
		}
		if ev.Output != nil {
			output = ev.Output
		}
	}
	if err != nil {
		if waits := durable.Waits(err); waits != nil {
			return mg.RunResult{Outcome: "waiting", Waits: mg.WaitPoints(waits)}, err
		}
		return mg.RunResult{}, err
	}
	if output == nil {
		return mg.RunResult{}, errors.New("生产者退出时没有确定结果")
	}
	raw, err := json.Marshal(output.Result)
	return mg.RunResult{Outcome: "completed", Output: raw}, err
}
func (n *native) Inspect(ctx context.Context, spec mg.RunSpec) (mg.RunEvidence, error) {
	events, err := n.r.Sessions.Read(ctx, n.r.Session.LogID, 0)
	if err != nil {
		return mg.RunEvidence{}, err
	}
	active := false
	completed := false
	for _, ev := range events {
		switch ev.Type {
		case "turn/start":
			var turn session.TurnStart
			if json.Unmarshal(ev.Data, &turn) != nil {
				return mg.RunEvidence{}, mg.ErrInvalid
			}
			active = turn.ExecutionKey == spec.Task.ExecutionKey
			completed = false
		case "turn/end":
			var end session.TurnEnd
			if json.Unmarshal(ev.Data, &end) != nil {
				return mg.RunEvidence{}, mg.ErrInvalid
			}
			if active && !end.Aborted {
				completed = true
			}
		}
	}
	if !completed {
		return mg.RunEvidence{State: "absent"}, nil
	}
	// 计量配置属于模型操作意图。保持同一配置，但把回放中的计量补记限制在内存副本。
	copyLog := session.NewMemoryStore()
	if _, err = copyLog.Append(ctx, n.r.Session.LogID, 0, events...); err != nil {
		return mg.RunEvidence{}, err
	}
	journal := session.DurableStore(copyLog, n.r.Session.LogID)
	controller, err := budget.NewDurable(spec.Task.Budget, journal)
	if err != nil {
		return mg.RunEvidence{}, err
	}
	ctx = budget.WithController(durable.WithJournal(ctx, journal), controller)
	result, err := n.Run(durable.WithReplayOnly(ctx), mg.RunSpec{Task: spec.Task})
	if err != nil {
		return mg.RunEvidence{}, err
	}
	return mg.RunEvidence{State: "completed", Result: result}, nil
}

type approver struct{ r mg.Resources }

func hash(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func (a approver) Approve(ctx context.Context, req tools.ApprovalRequest) (tools.ApprovalResult, error) {
	if req.Call == nil || req.Call.CallID == "" || a.r.ApprovalReader == nil {
		return tools.ApprovalResult{}, mg.ErrInvalid
	}
	intent, err := json.Marshal(struct {
		Tool, CallID, Key string
		Args              map[string]any
	}{req.Call.Tool, req.Call.CallID, req.Call.ExecutionKey, req.Call.Args})
	if err != nil {
		return tools.ApprovalResult{}, err
	}
	intentHash := hash(intent)
	info, err := json.Marshal(map[string]any{"kind": "approval", "intent_hash": intentHash, "tool": req.Call.Tool, "reason": req.Reason})
	if err != nil {
		return tools.ApprovalResult{}, err
	}
	raw, err := durable.AwaitInput(durable.AllowInterrupt(ctx), "management/approval/"+req.Call.CallID, info, func(raw json.RawMessage) error {
		var answer mg.ApprovalAnswer
		if json.Unmarshal(raw, &answer) != nil || answer.IntentHash != intentHash || (answer.Decision != "approve" && answer.Decision != "reject") {
			return mg.ErrInvalid
		}
		ap, e := a.r.ApprovalReader(ctx, answer.ApprovalID)
		if e != nil {
			return e
		}
		if ap.TaskID != a.r.Task.ID || ap.Wait.IntentHash != intentHash || ap.Decision != answer.Decision || (ap.State != "approved" && ap.State != "rejected") {
			return mg.ErrForbidden
		}
		return nil
	})
	if err != nil {
		return tools.ApprovalResult{}, err
	}
	var answer mg.ApprovalAnswer
	if err = json.Unmarshal(raw, &answer); err != nil {
		return tools.ApprovalResult{}, err
	}
	switch answer.Decision {
	case "approve":
		return tools.ApprovalResult{Outcome: tools.ApprovalAllowedOnce}, nil
	case "reject":
		return tools.ApprovalResult{Outcome: tools.ApprovalRejected, Feedback: "审批人已拒绝此操作"}, nil
	}
	return tools.ApprovalResult{}, fmt.Errorf("%w：未知审批决定", mg.ErrInvalid)
}
