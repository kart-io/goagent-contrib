package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	mg "github.com/kart-io/goagent-contrib/management"
	"github.com/kart-io/goagent-contrib/management/httpapi"
	"github.com/kart-io/goagent/core/usage"
	"github.com/kart-io/goagent/interfaces"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func TestRunDurationsAndTraceAreDerivedFromAttempts(t *testing.T) {
	f := setup(t, nil)
	traceID, err := trace.TraceIDFromHex("11111111111111111111111111111111")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("2222222222222222")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(trace.ContextWithSpanContext(f.ctx, trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled})))
	defer cancel()
	w := mg.NewWorker(f.s)
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() { cancel(); require.NoError(t, <-done) })
	se := f.session(t)
	task, err := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"运行"`)})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		current, e := f.s.GetTask(f.ctx, f.actor, task.ID)
		return e == nil && current.State == mg.Succeeded
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, w.Shutdown(f.ctx))
	run, err := f.s.GetRun(f.ctx, f.actor, task.ID)
	require.NoError(t, err)
	require.Len(t, run.Attempts, 1)
	require.Equal(t, "11111111111111111111111111111111", run.Attempts[0].TraceID)

	base := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	run.Task.CreatedAt = base
	finished := base.Add(14 * time.Second)
	run.Task.FinishedAt = &finished
	attempt := run.Attempts[0]
	attempt.CreatedAt = base.Add(2 * time.Second)
	firstEnd := base.Add(5 * time.Second)
	attempt.FinishedAt = &firstEnd
	attempt.Outcome = string(mg.WaitingInput)
	next := attempt
	next.ID = uuid.NewString()
	next.Revision = 1
	next.Number = 2
	next.CreatedAt = base.Add(9 * time.Second)
	nextEnd := base.Add(13 * time.Second)
	next.FinishedAt = &nextEnd
	next.Outcome = string(mg.Succeeded)
	require.NoError(t, f.store.WithinTx(f.ctx, func(tx mg.Tx) error {
		for _, entry := range []struct {
			kind     string
			value    any
			expected int64
		}{{"tasks", run.Task, run.Task.Revision}, {"attempts", attempt, attempt.Revision}, {"attempts", next, 0}} {
			raw, e := json.Marshal(entry.value)
			if e != nil {
				return e
			}
			var meta mg.Meta
			if e = json.Unmarshal(raw, &meta); e != nil {
				return e
			}
			if entry.expected > 0 {
				meta.Revision++
				var body map[string]any
				if e = json.Unmarshal(raw, &body); e != nil {
					return e
				}
				body["revision"] = meta.Revision
				raw, e = json.Marshal(body)
				if e != nil {
					return e
				}
			}
			d := mg.Document{Kind: entry.kind, TenantID: meta.TenantID, ID: meta.ID, Revision: meta.Revision, CreatedAt: meta.CreatedAt, Data: raw}
			if entry.expected == 0 {
				e = tx.Insert(f.ctx, d)
			} else {
				e = tx.CAS(f.ctx, d, entry.expected)
			}
			if e != nil {
				return e
			}
		}
		return nil
	}))
	run, err = f.s.GetRun(f.ctx, f.actor, task.ID)
	require.NoError(t, err)
	raw, err := json.Marshal(run)
	require.NoError(t, err)
	var public map[string]any
	require.NoError(t, json.Unmarshal(raw, &public))
	require.Equal(t, float64(2000), public["queued_millis"])
	require.Equal(t, float64(4000), public["waiting_millis"])
	require.Equal(t, float64(7000), public["execution_millis"])
	require.Equal(t, float64(14000), public["elapsed_millis"])
	require.Equal(t, "11111111111111111111111111111111", public["trace_id"])
}

func TestRunSummaryAcrossSessionsKeepsUnknownAndDoesNotCountReplayTwice(t *testing.T) {
	var effects, requests atomic.Int32
	f := setup(t, map[string]mg.Factory{"test": nativeFactory(&effects, &requests)})
	w := f.worker(t)
	first := f.session(t)
	completeApprovedTask(t, f, first.ID)
	second, err := f.s.CreateSession(f.ctx, f.actor, mg.CreateSessionRequest{IdempotencyKey: uuid.NewString(), AgentID: first.AgentID})
	require.NoError(t, err)
	require.Eventually(t, func() bool { s, e := f.s.GetSession(f.ctx, f.actor, second.ID); return e == nil && s.Status == "ready" }, 5*time.Second, 10*time.Millisecond)
	completeApprovedTask(t, f, second.ID)
	unknown := f.session(t)
	require.NoError(t, w.Shutdown(f.ctx))
	_, err = f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: unknown.ID, Input: json.RawMessage(`"未启动"`)})
	require.NoError(t, err)
	handler, err := httpapi.New(f.s, httpapi.Config{ActorFromRequest: func(*http.Request) (mg.Actor, error) { return f.actor, nil }})
	require.NoError(t, err)
	for range 2 {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest("GET", "/v1/runs/summary?limit=1", nil))
		require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
		var result struct {
			Totals struct {
				Tasks       int `json:"tasks"`
				UnknownRuns int `json:"unknown_runs"`
				KnownTokens struct {
					Total int `json:"total_tokens"`
				} `json:"known_tokens"`
				Complete bool `json:"complete"`
			} `json:"totals"`
			Agents   []json.RawMessage `json:"agents"`
			Sessions []json.RawMessage `json:"sessions"`
		}
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &result))
		require.Equal(t, 3, result.Totals.Tasks)
		require.Equal(t, 1, result.Totals.UnknownRuns)
		require.Equal(t, 20, result.Totals.KnownTokens.Total)
		require.False(t, result.Totals.Complete)
		require.Len(t, result.Agents, 2)
		require.Len(t, result.Sessions, 3)
	}
	require.Equal(t, int32(2), effects.Load())
	require.Equal(t, int32(4), requests.Load())
	denied := httptest.NewRecorder()
	handler.ServeHTTP(denied, httptest.NewRequest("GET", "/v1/runs/summary?cursor=bad", nil))
	require.Equal(t, http.StatusBadRequest, denied.Code)
}

type attributedFactory struct{ unknown bool }

func (attributedFactory) Validate(context.Context, mg.Version) (mg.Capabilities, error) {
	return mg.Capabilities{}, nil
}
func (f attributedFactory) Build(_ context.Context, _ mg.Version, r mg.Resources) (mg.Binding, error) {
	return mg.Binding{Runtime: attributedRuntime{resources: r, unknown: f.unknown}}, nil
}

type attributedRuntime struct {
	resources mg.Resources
	unknown   bool
}

func (attributedRuntime) Inspect(context.Context, mg.RunSpec) (mg.RunEvidence, error) {
	return mg.RunEvidence{State: "absent"}, nil
}
func (r attributedRuntime) Run(ctx context.Context, _ mg.RunSpec) (mg.RunResult, error) {
	root := usage.Owner{RunID: r.resources.Task.ID, AgentID: r.resources.Task.AgentID, AgentCallKey: "root"}
	child := usage.Owner{RunID: r.resources.Task.ID, AgentID: "nested-agent", AgentCallKey: "child", ParentAgentCallKey: "root"}
	for i, lineage := range [][]usage.Owner{{root}, {root, child}} {
		key := []string{"root-model", "child-model"}[i]
		if err := r.resources.Budget.ReserveAttributed(ctx, key, 0, lineage); err != nil {
			return mg.RunResult{}, err
		}
		if !r.unknown {
			amount := []int{10, 20}[i]
			if _, err := r.resources.Budget.RecordEvidence(ctx, key, lineage, usage.Evidence{KnownFields: usage.BasicFields, Tokens: interfaces.TokenUsage{PromptTokens: amount, TotalTokens: amount}}); err != nil {
				return mg.RunResult{}, err
			}
		}
	}
	return mg.RunResult{Outcome: "completed", Output: json.RawMessage(`"完成"`)}, nil
}
func TestRunSummaryUsesSelfAndPreservesUnknownEvidence(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "known", true: "unknown"}[unknown], func(t *testing.T) {
			f := setup(t, map[string]mg.Factory{"test": attributedFactory{unknown: unknown}})
			w := f.worker(t)
			se := f.session(t)
			task, err := f.s.SubmitTask(f.ctx, f.actor, mg.SubmitTaskRequest{IdempotencyKey: uuid.NewString(), SessionID: se.ID, Input: json.RawMessage(`"嵌套计量"`)})
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				current, e := f.s.GetTask(f.ctx, f.actor, task.ID)
				return e == nil && current.State == mg.Succeeded
			}, 5*time.Second, 10*time.Millisecond)
			require.NoError(t, w.Shutdown(f.ctx))
			summary, err := f.s.SummarizeRuns(f.ctx, f.actor, mg.Query{AgentID: se.AgentID})
			require.NoError(t, err)
			if unknown {
				require.Nil(t, summary.Totals.KnownTokens)
				require.Equal(t, 1, summary.Totals.UnknownRuns)
				require.False(t, summary.Totals.Complete)
			} else {
				require.Equal(t, 30, summary.Totals.KnownTokens.TotalTokens)
				require.Len(t, summary.Agents, 1)
				require.Equal(t, 10, summary.Agents[0].KnownTokens.TotalTokens, "父Agent不能累加inclusive中的子Agent消耗")
				raw, err := json.Marshal(summary)
				require.NoError(t, err)
				var public map[string]any
				require.NoError(t, json.Unmarshal(raw, &public))
				require.Len(t, public["agent_usage"], 2, "嵌套Agent的self必须独立可查")
			}
		})
	}
}
