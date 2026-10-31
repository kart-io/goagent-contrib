package acp

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/kart-io/goagent/harness/env"
	"github.com/kart-io/goagent/interfaces"
	"github.com/kart-io/goagent/tools"
	"github.com/stretchr/testify/require"
)

var termCaps = acpsdk.ClientCapabilities{Terminal: true}

func TestClientExecutorRunsInEditorTerminal(t *testing.T) {
	s, ed, dir := fsSession(t, termCaps)
	ed.terminal = func(acpsdk.CreateTerminalRequest) (string, int) { return "ok\n", 3 }

	res, err := s.Executor().Run(context.Background(), env.Command{Argv: []string{"go", "test", "./..."}, Dir: "pkg"})
	require.NoError(t, err)
	require.Equal(t, env.Result{Output: []byte("ok\n"), ExitCode: 3}, res)
	require.Equal(t, []string{"create", "wait", "output", "release"}, ed.termCalls)
	created := ed.created[0]
	require.Equal(t, "go", created.Command)
	require.Equal(t, []string{"test", "./..."}, created.Args)
	require.Equal(t, filepath.Join(dir, "pkg"), *created.Cwd)
	require.True(t, s.Executor().Capabilities().Network)

	_, err = s.Executor().Run(context.Background(), env.Command{Dir: "../x", Argv: []string{"ls"}})
	require.ErrorIs(t, err, env.ErrOutsideWorkspace)
	_, err = s.Executor().Run(context.Background(), env.Command{})
	require.Error(t, err)
}

func TestClientExecutorKillsOnCancel(t *testing.T) {
	s, ed, _ := fsSession(t, termCaps)
	ed.hang = true
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := s.Executor().Run(ctx, env.Command{Argv: []string{"sleep", "60"}})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, []string{"create", "wait", "kill", "output", "release"}, ed.termCalls)
}

func TestExecutorFallsBackWithoutTerminal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("用 sh 验证本地执行")
	}
	s, ed, _ := fsSession(t, acpsdk.ClientCapabilities{})
	res, err := s.Executor().Run(context.Background(), env.Command{Argv: []string{"sh", "-c", "printf hi"}})
	require.NoError(t, err)
	require.Equal(t, "hi", string(res.Output))
	require.Empty(t, ed.termCalls)
}

// 工具内执行的命令以终端内容块嵌入对应的工具调用，结束时仍保留。
func TestTerminalEmbeddedInToolCall(t *testing.T) {
	ed := newEditor()
	ed.terminal = func(acpsdk.CreateTerminalRequest) (string, int) { return "PASS", 0 }
	run := func(s *Session) []interfaces.Tool {
		return []interfaces.Tool{tools.NewBaseTool("run_tests", "运行测试", `{"type":"object"}`, func(ctx context.Context, _ *interfaces.ToolInput) (*interfaces.ToolOutput, error) {
			res, err := s.Executor().Run(ctx, env.Command{Argv: []string{"go", "test"}})
			if err != nil {
				return nil, err
			}
			return &interfaces.ToolOutput{Success: true, Result: string(res.Output)}, nil
		})}
	}
	h := start(t, NewServer(toolFactory(newModel(calls("run_tests"), say("测试通过")), nil, nil, run)), ed)
	h.initialize(t, termCaps)
	_, err := h.prompt(t, h.newSession(t, t.TempDir()), "跑测试")
	require.NoError(t, err)

	var embedded bool
	for _, n := range ed.notifications() {
		if u := n.Update.ToolCallUpdate; u != nil && u.Status == nil && len(u.Content) == 1 && u.Content[0].Terminal != nil {
			embedded = embedded || u.Content[0].Terminal.TerminalId == "term-1"
		}
	}
	require.True(t, embedded, "执行中发送带终端的 tool_call_update")
	last := ed.lastUpdate("call-1")
	require.Equal(t, acpsdk.ToolCallStatusCompleted, *last.Status)
	require.Equal(t, "term-1", last.Content[0].Terminal.TerminalId)
	require.Equal(t, "PASS", last.Content[1].Content.Content.Text.Text)
}

func TestClientExecutorSignalExitCode(t *testing.T) {
	s, ed, _ := fsSession(t, termCaps)
	ed.signal = true
	res, err := s.Executor().Run(context.Background(), env.Command{Argv: []string{"x"}})
	require.NoError(t, err)
	require.Equal(t, -1, res.ExitCode)
}

func TestClientExecutorDefaultDirIsCwd(t *testing.T) {
	s, ed, dir := fsSession(t, termCaps)
	for _, d := range []string{"", "."} {
		_, err := s.Executor().Run(context.Background(), env.Command{Argv: []string{"x"}, Dir: d})
		require.NoError(t, err)
	}
	require.Len(t, ed.created, 2)
	for _, c := range ed.created {
		require.Equal(t, dir, *c.Cwd)
	}
}

// 命令运行超过清理超时后被取消，kill 与 release 仍然发出。
func TestClientExecutorCleanupOutlivesTimeout(t *testing.T) {
	old := endTurnTimeout
	endTurnTimeout = 50 * time.Millisecond
	t.Cleanup(func() { endTurnTimeout = old })
	s, ed, _ := fsSession(t, termCaps)
	ed.hang = true
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	_, err := s.Executor().Run(ctx, env.Command{Argv: []string{"sleep", "60"}})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, []string{"create", "wait", "kill", "output", "release"}, ed.termCalls)
}

func TestAddTerminalIgnoresFinishedCall(t *testing.T) {
	c := newCallTracker()
	require.True(t, c.start("a"))
	require.Equal(t, []string{"t1"}, c.addTerminal("a", "t1"))
	c.finish("a")
	require.Nil(t, c.addTerminal("a", "t2"))
	require.Empty(t, c.drain())
}
