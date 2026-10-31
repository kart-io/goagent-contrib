package acp

import (
	"context"
	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/kart-io/goagent/builder"
	"github.com/stretchr/testify/require"
	"testing"
)

type eventRunner func(context.Context, any) (<-chan builder.AgentEvent, error)

func (f eventRunner) ExecuteStream(ctx context.Context, input any) (<-chan builder.AgentEvent, error) {
	return f(ctx, input)
}

func TestFinalAnswerContract(t *testing.T) {
	for _, tc := range []struct {
		name, delta string
		final       any
		want        string
	}{
		{"仅终态", "", "缓存答案", "缓存答案"},
		{"不重复", "你好", "你好", "你好"},
		{"补后缀", "你", "你好", "你好"},
		{"保留前言", "说明。你好", "你好", "说明。你好"},
		{"改写", "草稿", "定稿", "草稿\n\n最终答复：\n定稿"},
		{"结构化", "", map[string]any{"ok": true}, `{"ok":true}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := start(t, NewServer(func(context.Context, *Session) (Runner, error) {
				return eventRunner(func(context.Context, any) (<-chan builder.AgentEvent, error) {
					ch := make(chan builder.AgentEvent, 2)
					if tc.delta != "" {
						ch <- builder.AgentEvent{Type: "text", Delta: tc.delta}
					}
					ch <- builder.AgentEvent{Type: "done", Output: &builder.AgentOutput{Result: tc.final}}
					close(ch)
					return ch, nil
				}), nil
			}), newEditor())
			h.initialize(t, acpsdk.ClientCapabilities{})
			_, err := h.prompt(t, h.newSession(t, t.TempDir()), "问题")
			require.NoError(t, err)
			require.Equal(t, tc.want, h.editor.agentText())
		})
	}
}
