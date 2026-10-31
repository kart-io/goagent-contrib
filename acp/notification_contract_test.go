package acp

import (
	"context"
	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

type brokenWriter struct{}

func (brokenWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func TestFailedTerminalNotificationSurvivesClose(t *testing.T) {
	input, writer := io.Pipe()
	defer input.Close()
	defer writer.Close()
	ready := make(chan struct{})
	close(ready)
	c := &conn{srv: NewServer(nil), base: context.Background(), ready: ready}
	c.peer = acpsdk.NewAgentSideConnection(nil, brokenWriter{}, input)
	s, err := newSession(c, "s1", t.TempDir(), nil)
	require.NoError(t, err)
	require.True(t, s.calls.start("call-1"))
	s.fail(context.Background(), "call-1", "取消")
	require.Error(t, s.close(), "终态发送失败必须反映在关闭结果")
}
