package acp

import (
	"context"
	"errors"
	"github.com/kart-io/goagent/harness/env"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestFSUnsupportedBeforeSideEffects(t *testing.T) {
	for _, tc := range []struct {
		name   string
		data   []byte
		intent env.WriteIntent
	}{
		{"权限", []byte("hi"), env.WriteIntent{Perm: 0600}},
		{"二进制", []byte{0xff}, env.WriteIntent{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, ed, dir := fsSession(t, fsCaps)
			_, err := s.FileSystem().WriteFile(context.Background(), "missing/a", tc.data, tc.intent)
			require.ErrorIs(t, err, errors.ErrUnsupported)
			_, err = os.Stat(filepath.Join(dir, "missing"))
			require.ErrorIs(t, err, os.ErrNotExist)
			require.Empty(t, ed.writes)
		})
	}
}

func TestFSConcurrentConditionalWrite(t *testing.T) {
	seen := make(chan *Session, 2)
	ed := newEditor()
	srv := NewServer(plainFactory(newModel(), seen))
	h := start(t, srv, ed)
	h.initialize(t, fsCaps)
	dir := t.TempDir()
	h.newSession(t, dir)
	a := <-seen
	h.newSession(t, dir)
	b := <-seen
	path := filepath.Join(dir, "a")
	require.NoError(t, os.WriteFile(path, []byte("旧"), 0644))
	ed.files[path] = "旧"
	entered := make(chan struct{})
	secondRead := make(chan struct{})
	release := make(chan struct{})
	var reads atomic.Int32
	ed.readHook = func(ctx context.Context) {
		if reads.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
		} else {
			close(secondRead)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	first := make(chan error, 1)
	second := make(chan error, 1)
	intent := env.WriteIntent{IfVersion: versionOf([]byte("旧"))}
	go func() { _, err := a.FileSystem().WriteFile(ctx, "a", []byte("一"), intent); first <- err }()
	recv(t, entered, "首个版本读取")
	go func() { _, err := b.FileSystem().WriteFile(ctx, "a", []byte("二"), intent); second <- err }()
	select {
	case <-secondRead:
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	require.NoError(t, recv(t, first, "第一次写入"))
	require.ErrorIs(t, recv(t, second, "第二次版本冲突"), env.ErrStale)
}
