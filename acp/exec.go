package acp

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/kart-io/goagent/harness/env"
)

// clientExecutor 在编辑器终端中执行命令，编辑器可展示输出；工具调用内执行时终端嵌入该调用。
type clientExecutor struct{ s *Session }

var _ env.Executor = (*clientExecutor)(nil)

// Capabilities 实现 env.Executor：编辑器终端在宿主机上执行，可以访问网络。
func (e *clientExecutor) Capabilities() env.Capabilities { return env.Capabilities{Network: true} }

func (e *clientExecutor) Run(ctx context.Context, cmd env.Command) (env.Result, error) {
	if len(cmd.Argv) == 0 {
		return env.Result{}, errors.New("acp: 命令为空")
	}
	dirArg := cmd.Dir
	if dirArg == "" {
		dirArg = "."
	}
	rel, err := e.s.local.Resolve(dirArg)
	if err != nil {
		return env.Result{}, err
	}
	dir := filepath.Join(e.s.cwd, filepath.FromSlash(rel))
	peer := e.s.conn.client()
	sid := acpsdk.SessionId(e.s.id)
	created, err := peer.CreateTerminal(ctx, acpsdk.CreateTerminalRequest{SessionId: sid, Command: cmd.Argv[0], Args: cmd.Argv[1:], Cwd: &dir})
	if err != nil {
		return env.Result{}, fmt.Errorf("acp: 创建终端：%w", err)
	}
	term := created.TerminalId
	defer func() {
		cleanup, cancel := cleanupCtx(ctx)
		defer cancel()
		_, _ = peer.ReleaseTerminal(cleanup, acpsdk.ReleaseTerminalRequest{SessionId: sid, TerminalId: term})
	}()

	if callID := callIDFrom(ctx); callID != "" {
		if terms := e.s.calls.addTerminal(callID, term); terms != nil {
			if err := e.s.update(ctx, acpsdk.UpdateToolCall(acpsdk.ToolCallId(callID), acpsdk.WithUpdateContent(toolContent(terms, "")))); err != nil {
				return env.Result{}, err
			}
		}
	}

	exit, err := peer.WaitForTerminalExit(ctx, acpsdk.WaitForTerminalExitRequest{SessionId: sid, TerminalId: term})
	if err != nil {
		if ctx.Err() == nil {
			return env.Result{}, fmt.Errorf("acp: 等待终端退出：%w", err)
		}
		cleanup, cancel := cleanupCtx(ctx)
		defer cancel()
		_, _ = peer.KillTerminal(cleanup, acpsdk.KillTerminalRequest{SessionId: sid, TerminalId: term})
		out, _ := peer.TerminalOutput(cleanup, acpsdk.TerminalOutputRequest{SessionId: sid, TerminalId: term})
		return env.Result{Output: []byte(out.Output)}, ctx.Err()
	}
	out, err := peer.TerminalOutput(ctx, acpsdk.TerminalOutputRequest{SessionId: sid, TerminalId: term})
	if err != nil {
		return env.Result{}, fmt.Errorf("acp: 读取终端输出：%w", err)
	}
	code := 0
	switch {
	case exit.ExitCode != nil:
		code = *exit.ExitCode
	case exit.Signal != nil:
		code = -1 // 被信号终止，没有退出码
	}
	return env.Result{Output: []byte(out.Output), ExitCode: code}, nil
}

// cleanupCtx 给结束阶段的请求一个不随调用方取消、自此刻起计时的 ctx，否则长时间运行后终端得不到 kill 与 release。
func cleanupCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), endTurnTimeout)
}
