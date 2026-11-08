package integration_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/kart-io/goagent/codec/json"
	"github.com/kart-io/goagent/interfaces"
	"github.com/kart-io/goagent/llm"
)

type liveCall struct {
	finished      chan struct{}
	TaskID        string          `json:"task_id"`
	ResponseModel string          `json:"response_model,omitempty"`
	FinishReason  string          `json:"finish_reason,omitempty"`
	ToolCalls     []llm.ToolCall  `json:"tool_calls,omitempty"`
	Usage         *llm.Usage      `json:"usage"`
	UsageFields   uint64          `json:"usage_fields"`
	Status        string          `json:"status"`
	ErrorReason   llm.ErrorReason `json:"error_reason,omitempty"`
	MaxTokens     int             `json:"max_output_tokens"`
}

type liveJournalEntry struct {
	Index int      `json:"index"`
	Call  liveCall `json:"call"`
}

type liveLedger struct {
	mu    sync.Mutex
	calls []liveCall
	file  *os.File
}

func newLiveLedger(dir, provider string) (*liveLedger, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(filepath.Join(dir, provider+"-requests.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	return &liveLedger{file: file}, nil
}

func (l *liveLedger) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	return l.file.Close()
}

func (l *liveLedger) snapshot() []liveCall {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]liveCall(nil), l.calls...)
}

func (l *liveLedger) persistLocked(index int) error {
	if l.file == nil {
		return nil
	}
	raw, err := json.Marshal(liveJournalEntry{Index: index, Call: l.calls[index]})
	if err != nil {
		return err
	}
	if _, err = l.file.Write(append(raw, '\n')); err != nil {
		return err
	}
	return l.file.Sync()
}

func (l *liveLedger) update(index int, apply func(*liveCall) bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !apply(&l.calls[index]) {
		return nil
	}
	return l.persistLocked(index)
}

// 包装器只限额、记录及透传；异常结束时补错误终态，不生成成功回答或用量。
type liveObservedModel struct {
	llm.ToolStreamClient
	ledger  *liveLedger
	taskID  string
	timeout time.Duration
}

func (m *liveObservedModel) CompleteToolsStream(parent context.Context, req *llm.CompletionRequest, available []interfaces.Tool) (<-chan *llm.StreamChunk, error) {
	if req == nil || req.MaxTokens != 1024 {
		return nil, fmt.Errorf("真实验收必须显式限制每次输出为1024Token")
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	m.ledger.mu.Lock()
	if len(m.ledger.calls) >= 20 {
		m.ledger.mu.Unlock()
		return nil, fmt.Errorf("真实验收达到每提供方20次请求上限")
	}
	index := len(m.ledger.calls)
	done := make(chan struct{})
	m.ledger.calls = append(m.ledger.calls, liveCall{TaskID: m.taskID, Status: "started", MaxTokens: req.MaxTokens, finished: done})
	err := m.ledger.persistLocked(index)
	m.ledger.mu.Unlock()
	if err != nil {
		close(done)
		return nil, fmt.Errorf("请求前写入证据失败，未调用模型：%w", err)
	}
	timeout := m.timeout
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	failed := func(err error) error {
		persistErr := m.ledger.update(index, func(call *liveCall) bool {
			call.Status = "stream_error"
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				call.Status = "deadline_exceeded"
			case errors.Is(err, context.Canceled):
				call.Status = "canceled"
			}
			call.ErrorReason = llm.ClassifyError(err).Reason
			return true
		})
		return errors.Join(err, persistErr)
	}
	upstream, err := m.ToolStreamClient.CompleteToolsStream(ctx, req, available)
	if err != nil || upstream == nil {
		defer close(done)
		cancel()
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		persistErr := m.ledger.update(index, func(call *liveCall) bool {
			call.Status = "request_error"
			call.ErrorReason = llm.ClassifyError(err).Reason
			return true
		})
		return nil, errors.Join(err, persistErr)
	}
	out := make(chan *llm.StreamChunk, 1)
	go func() {
		defer close(done)
		defer close(out)
		defer cancel()
		keepTerminal := func(terminal *llm.StreamChunk) {
			// 失败后丢弃尚未消费的片段，为错误终态留出位置；不依赖消费者继续读流。
			select {
			case out <- terminal:
				return
			default:
			}
			select {
			case <-out:
			default:
			}
			out <- terminal
		}
		fail := func(err error) {
			keepTerminal(&llm.StreamChunk{Done: true, Error: failed(err)})
		}
		for {
			select {
			case <-ctx.Done():
				fail(ctx.Err())
				return
			case chunk, ok := <-upstream:
				if !ok {
					cause := ctx.Err()
					if cause == nil {
						cause = io.ErrUnexpectedEOF
					}
					fail(cause)
					return
				}
				if chunk == nil {
					continue
				}
				if err := m.ledger.update(index, func(call *liveCall) bool {
					changed := false
					if chunk.ResponseModel != "" && chunk.ResponseModel != call.ResponseModel {
						call.ResponseModel = chunk.ResponseModel
						changed = true
					}
					if chunk.Usage != nil {
						copyUsage := *chunk.Usage
						call.Usage = &copyUsage
						call.UsageFields = uint64(chunk.Usage.Fields())
						changed = true
					}
					if chunk.Done || chunk.Error != nil {
						changed = true
						call.Status = "completed"
						call.FinishReason = chunk.FinishReason
						call.ToolCalls = append([]llm.ToolCall(nil), chunk.ToolCalls...)
						if chunk.Error != nil {
							call.Status = "stream_error"
							call.ErrorReason = llm.ClassifyError(chunk.Error).Reason
						}
					}
					return changed
				}); err != nil {
					fail(fmt.Errorf("流式证据写入失败：%w", err))
					return
				}
				select {
				case out <- chunk:
				case <-ctx.Done():
					if chunk.Done || chunk.Error != nil {
						keepTerminal(chunk)
					} else {
						fail(ctx.Err())
					}
					return
				}
				if chunk.Done || chunk.Error != nil {
					return
				}
			}
		}
	}()
	return out, nil
}
