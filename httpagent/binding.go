package httpagent

import (
	"context"
	"errors"

	"github.com/kart-io/goagent/builder"
)

// Native 用同一事件执行路径服务所有入口；捕获终态后仍等生产者真正退出。
// 宿主须先用实际模型检查 llm.HasStreaming/HasToolStreaming，不能根据方法存在性选择本绑定。
func Native(start StreamFunc) Binding {
	return Binding{Streaming: true, Mode: "unified", Run: func(ctx context.Context, input any, emit EmitFunc) (*builder.AgentOutput, error) {
		if start == nil {
			return nil, failure("unsupported_capability", "未配置流式入口", nil)
		}
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		events, err := start(runCtx, input)
		if err != nil {
			return nil, err
		}
		if events == nil {
			return nil, failure("stream_incomplete", "执行未返回事件流", nil)
		}
		var output *builder.AgentOutput
		var resultErr error
		terminal := false
		cancelled := ctx.Done()
		for {
			select {
			case <-cancelled:
				cancel()
				cancelled = nil
				if resultErr == nil {
					resultErr = ctx.Err()
				}
				producerWaiting(ctx)
			case ev, ok := <-events:
				if !ok {
					if resultErr != nil {
						return nil, resultErr
					}
					if !terminal {
						return nil, failure("stream_incomplete", "事件流没有终态", nil)
					}
					if output == nil {
						return nil, failure("invalid_terminal", "最终事件没有结果", nil)
					}
					return output, nil
				}
				if terminal {
					if resultErr == nil {
						resultErr = failure("invalid_terminal", "终态后仍有业务事件", nil)
					}
					cancel()
					continue
				}
				if ev.Type == "done" || ev.Type == "error" || ev.Error != nil {
					terminal = true
					producerWaiting(ctx)
					if ev.Type == "error" || ev.Error != nil {
						resultErr = ev.Error
						if resultErr == nil {
							resultErr = errors.New(ev.Message)
						}
					} else {
						output = ev.Output
					}
					continue
				}
				if resultErr == nil && emit != nil {
					if err = emit(ev); err != nil {
						resultErr = err
						cancel()
						producerWaiting(ctx)
					}
				}
			}
		}
	}}
}

// Answer 显式选择普通同步回答，不承诺原生工具循环或伪造流式能力。
func Answer(invoke InvokeFunc) Binding {
	return Binding{Mode: "answer_only", Run: func(ctx context.Context, input any, _ EmitFunc) (*builder.AgentOutput, error) {
		if invoke == nil {
			return nil, failure("unsupported_capability", "未配置同步入口", nil)
		}
		return invoke(ctx, input)
	}}
}

type producerKey struct{}

// producerWaiting 仅传递生命周期进度，不作为对外业务事件。
func producerWaiting(ctx context.Context) {
	if note, ok := ctx.Value(producerKey{}).(func()); ok {
		note()
	}
}
