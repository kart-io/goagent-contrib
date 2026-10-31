// Package httpagent 把 GoAgent 统一执行能力适配为 HTTP、SSE、WebSocket 与双向 Webhook。
package httpagent

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kart-io/goagent/builder"
	"github.com/kart-io/goagent/interfaces"
)

// RunRequest 是已准入的运行；ID 与业务 SessionID 独立。
type RunRequest struct {
	ID      string
	Input   any
	Source  string
	Webhook string
}
type EmitFunc func(builder.AgentEvent) error
type RunFunc func(context.Context, any, EmitFunc) (*builder.AgentOutput, error)

// Binding 显式声明能力。Streaming 不能通过 Agent 的方法集推断。
type Binding struct {
	Run       RunFunc
	Streaming bool
	Mode      string
}
type Cleanup func(context.Context) error
type Factory func(context.Context, RunRequest) (Binding, Cleanup, error)
type StreamFunc func(context.Context, any) (<-chan builder.AgentEvent, error)
type InvokeFunc func(context.Context, any) (*builder.AgentOutput, error)

// Failure 是稳定的协议错误，不将 Go error 直接序列化。
type Failure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Cause   error  `json:"-"`
}

func (e *Failure) Error() string { return e.Message }
func (e *Failure) Unwrap() error { return e.Cause }
func failure(code, message string, err error) *Failure {
	return &Failure{Code: code, Message: message, Cause: err}
}
func classify(err error) *Failure {
	var f *Failure
	if errors.As(err, &f) {
		return f
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return failure("timeout", "执行期限已到", err)
	}
	if errors.Is(err, context.Canceled) {
		return failure("cancelled", "执行已取消", err)
	}
	return failure("execution_failed", fmt.Sprint(err), err)
}

// Output 是公开结果，刻意不包含可变的 AgentState。
type Output struct {
	Result     any                    `json:"result"`
	Metadata   map[string]any         `json:"metadata,omitempty"`
	DurationMS int64                  `json:"duration_ms"`
	Timestamp  time.Time              `json:"timestamp"`
	TokenUsage *interfaces.TokenUsage `json:"token_usage,omitempty"`
}

func outputOf(out *builder.AgentOutput) *Output {
	if out == nil {
		return nil
	}
	return &Output{Result: out.Result, Metadata: out.Metadata, DurationMS: out.Duration.Milliseconds(), Timestamp: out.Timestamp, TokenUsage: out.TokenUsage}
}

// Event 的序号只用于本次连接排序，不代表持久恢复游标。
type Event struct {
	SchemaVersion int                       `json:"schema_version"`
	RunID         string                    `json:"run_id"`
	Seq           uint64                    `json:"seq"`
	Type          string                    `json:"type"`
	Sources       []builder.RetrievalSource `json:"sources,omitempty"`
	Step          string                    `json:"step,omitempty"`
	Tool          string                    `json:"tool,omitempty"`
	Delta         string                    `json:"delta,omitempty"`
	Usage         *interfaces.TokenUsage    `json:"usage,omitempty"`
	Output        *Output                   `json:"output,omitempty"`
	Error         *Failure                  `json:"error,omitempty"`
}

// RunEvent 是 Observer 与出站 Webhook 共用的不可变 JSON 快照。
type RunEvent struct {
	SchemaVersion int       `json:"schema_version"`
	EventID       string    `json:"event_id"`
	RunID         string    `json:"run_id"`
	EventType     string    `json:"event_type"`
	OccurredAt    time.Time `json:"occurred_at"`
	Data          any       `json:"data"`
}
