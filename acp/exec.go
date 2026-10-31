package acp

import (
	"context"
	"errors"
	"fmt"
	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/kart-io/goagent/harness/env"
	"path/filepath"
)

type clientExecutor struct{ s *Session }

var _ env.Executor = (*clientExecutor)(nil)

func (e *clientExecutor) Capabilities() env.Capabilities { return *e.s.conn.srv.terminalCaps }

func (e *clientExecutor) Run(ctx context.Context, cmd env.Command) (result env.Result, err error) {
	if len(cmd.Argv) == 0 {
		return result, errors.New("acp: 命令为空")
	}
	dirArg := cmd.Dir
	if dirArg == "" {
		dirArg = "."
	}
	rel, err := e.s.local.Resolve(dirArg)
	if err != nil {
		return result, err
	}
	dir := filepath.Join(e.s.cwd, filepath.FromSlash(rel))
	peer := e.s.conn.client()
	sid := acpsdk.SessionId(e.s.id)
	created, err := peer.CreateTerminal(ctx, acpsdk.CreateTerminalRequest{SessionId: sid, Command: cmd.Argv[0], Args: cmd.Argv[1:], Cwd: &dir})
	if err != nil || created.TerminalId == "" {
		err = errors.Join(ctx.Err(), err, errors.New("acp: 终端创建结果未知，无法确认远端是否已启动"))
		e.s.recordFailure(err)
		return result, err
	}
	term := created.TerminalId
	uncertain := true
	defer func() {
		output, cleanupErr := e.cleanupTerminal(ctx, term, uncertain)
		if len(result.Output) == 0 {
			result.Output = output
		}
		err = errors.Join(ctx.Err(), err, cleanupErr)
		e.s.recordFailure(cleanupErr)
	}()
	if callID := callIDFrom(ctx); callID != "" {
		if terms := e.s.calls.addTerminal(callID, term); terms != nil {
			if err := e.s.update(ctx, acpsdk.UpdateToolCall(acpsdk.ToolCallId(callID), acpsdk.WithUpdateContent(toolContent(terms, "")))); err != nil {
				return result, err
			}
		}
	}
	exit, err := peer.WaitForTerminalExit(ctx, acpsdk.WaitForTerminalExitRequest{SessionId: sid, TerminalId: term})
	if err != nil {
		return result, fmt.Errorf("acp: 等待终端退出：%w", err)
	}
	switch {
	case exit.ExitCode != nil:
		result.ExitCode = *exit.ExitCode
	case exit.Signal != nil:
		result.ExitCode = -1
	default:
		return result, errors.New("acp: 终端缺少退出码和信号，执行状态未知")
	}
	uncertain = false
	output, err := peer.TerminalOutput(ctx, acpsdk.TerminalOutputRequest{SessionId: sid, TerminalId: term})
	result.Output = []byte(output.Output)
	if err != nil {
		return result, fmt.Errorf("acp: 读取终端输出：%w", err)
	}
	return result, nil
}

// 每个回收阶段在开始时单独计时，前一个阶段超时不能剥夺 release 的执行机会。
func (e *clientExecutor) cleanupTerminal(parent context.Context, term string, uncertain bool) ([]byte, error) {
	peer := e.s.conn.client()
	sid := acpsdk.SessionId(e.s.id)
	var errs []error
	var output []byte
	if uncertain {
		ctx, cancel := cleanupCtx(parent)
		_, err := peer.KillTerminal(ctx, acpsdk.KillTerminalRequest{SessionId: sid, TerminalId: term})
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("acp: 中断终端：%w", err))
		}
		ctx, cancel = cleanupCtx(parent)
		status, err := peer.WaitForTerminalExit(ctx, acpsdk.WaitForTerminalExitRequest{SessionId: sid, TerminalId: term})
		cancel()
		if err == nil && status.ExitCode == nil && status.Signal == nil {
			err = errors.New("终端未确认退出")
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("acp: 确认终端退出：%w", err))
		}
		ctx, cancel = cleanupCtx(parent)
		out, err := peer.TerminalOutput(ctx, acpsdk.TerminalOutputRequest{SessionId: sid, TerminalId: term})
		cancel()
		output = []byte(out.Output)
		if err != nil {
			errs = append(errs, fmt.Errorf("acp: 读取中断后的输出：%w", err))
		}
	}
	ctx, cancel := cleanupCtx(parent)
	_, err := peer.ReleaseTerminal(ctx, acpsdk.ReleaseTerminalRequest{SessionId: sid, TerminalId: term})
	cancel()
	if err != nil {
		errs = append(errs, fmt.Errorf("acp: 释放终端：%w", err))
	}
	return output, errors.Join(errs...)
}

func cleanupCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), endTurnTimeout)
}
