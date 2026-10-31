package acp

import (
	"context"
	"errors"
	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/kart-io/goagent/harness/env"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestExecReleaseErrorIsVisible(t *testing.T) {
	s, ed, _ := fsSession(t, termCaps)
	ed.releaseError = errors.New("终端释放失败")
	_, err := s.Executor().Run(context.Background(), env.Command{Argv: []string{"x"}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "终端释放失败")
}
func TestExecMissingExitIsNotSuccess(t *testing.T) {
	s, ed, _ := fsSession(t, termCaps)
	ed.missingExit = true
	_, err := s.Executor().Run(context.Background(), env.Command{Argv: []string{"x"}})
	require.Error(t, err)
}
func TestExecLongRunningCleanupContext(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "正常", true: "取消"}[cancelled], func(t *testing.T) {
			s, ed, _ := fsSession(t, termCaps)
			ed.waitHook = func(ctx context.Context) (response acpsdk.WaitForTerminalExitResponse, err error) {
				if ctx.Err() != nil {
					return response, ctx.Err()
				}
				delay := 5100 * time.Millisecond
				if cancelled {
					delay = 6 * time.Second
				}
				select {
				case <-time.After(delay):
					return acpsdk.WaitForTerminalExitResponse{ExitCode: acpsdk.Ptr(0)}, nil
				case <-ctx.Done():
					return response, ctx.Err()
				}
			}
			// Kill 后确认退出使用新的有效 context，不重复等待长命令。
			released := make(chan error, 1)
			ed.releaseHook = func(ctx context.Context) { released <- ctx.Err() }
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			if cancelled {
				timer := time.AfterFunc(5100*time.Millisecond, cancel)
				defer timer.Stop()
			}
			_, err := s.Executor().Run(ctx, env.Command{Argv: []string{"x"}})
			require.NoError(t, recv(t, released, "回收 context 检查"))
			if cancelled {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestTerminalCapabilitiesMustBeExplicit(t *testing.T) {
	h := start(t, NewServer(plainFactory(newModel(), nil)), newEditor())
	h.initialize(t, termCaps)
	_, err := h.client.NewSession(context.Background(), acpsdk.NewSessionRequest{Cwd: t.TempDir(), McpServers: []acpsdk.McpServer{}})
	requireCode(t, err, -32602)
}
