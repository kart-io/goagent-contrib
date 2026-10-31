// Package acp 让 goagent 的 Agent 以 Agent Client Protocol（ACP）Agent 身份经 stdio 运行，
// 由 Zed、JetBrains 等编辑器驱动：流式回答、工具调用通知与审批、取消、会话恢复，
// 以及把文件读写与命令执行交给编辑器。stdout 被协议占用，宿主日志须写 stderr。
package acp
