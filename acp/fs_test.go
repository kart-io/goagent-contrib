package acp

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/kart-io/goagent/harness/env"
	"github.com/stretchr/testify/require"
)

func fsSession(t *testing.T, caps acpsdk.ClientCapabilities) (*Session, *fakeEditor, string) {
	t.Helper()
	seen := make(chan *Session, 1)
	ed := newEditor()
	h := start(t, NewServer(plainFactory(newModel(), seen)), ed)
	h.initialize(t, caps)
	dir := t.TempDir()
	h.newSession(t, dir)
	return <-seen, ed, dir
}

var fsCaps = acpsdk.ClientCapabilities{Fs: acpsdk.FileSystemCapabilities{ReadTextFile: true, WriteTextFile: true}}

func TestClientFSReadsEditorBuffer(t *testing.T) {
	s, ed, dir := fsSession(t, fsCaps)
	ctx := context.Background()
	path := filepath.Join(dir, "a.go")
	require.NoError(t, os.WriteFile(path, []byte("磁盘"), 0o644))
	ed.files[path] = "未保存的缓冲区"

	data, version, err := s.FileSystem().ReadFile(ctx, "a.go")
	require.NoError(t, err)
	require.Equal(t, "未保存的缓冲区", string(data))
	require.Equal(t, versionOf([]byte("未保存的缓冲区")), version)

	_, _, err = s.FileSystem().ReadFile(ctx, "missing.go")
	require.ErrorIs(t, err, fs.ErrNotExist)
	require.Equal(t, []string{path}, ed.reads, "不存在的文件不转发")

	_, _, err = s.FileSystem().ReadFile(ctx, "../outside")
	require.ErrorIs(t, err, env.ErrOutsideWorkspace)
}

func TestClientFSWritesThroughEditor(t *testing.T) {
	s, ed, dir := fsSession(t, fsCaps)
	ctx := context.Background()
	path := filepath.Join(dir, "a.go")
	require.NoError(t, os.WriteFile(path, []byte("旧"), 0o644))
	ed.files[path] = "旧"

	v, err := s.FileSystem().WriteFile(ctx, "a.go", []byte("新"), env.WriteIntent{IfVersion: versionOf([]byte("旧"))})
	require.NoError(t, err)
	require.Equal(t, versionOf([]byte("新")), v)
	require.Equal(t, "新", ed.files[path])
	disk, _ := os.ReadFile(path)
	require.Equal(t, "旧", string(disk), "写入由编辑器负责，本模块不直接写盘")

	_, err = s.FileSystem().WriteFile(ctx, "a.go", []byte("再改"), env.WriteIntent{IfVersion: versionOf([]byte("旧"))})
	require.ErrorIs(t, err, env.ErrStale)
	_, err = s.FileSystem().WriteFile(ctx, "a.go", []byte("x"), env.WriteIntent{CreateIfAbsent: true})
	require.ErrorIs(t, err, env.ErrExists)

	_, err = s.FileSystem().WriteFile(ctx, "sub/b.go", []byte("新文件"), env.WriteIntent{CreateIfAbsent: true})
	require.NoError(t, err)
	require.Equal(t, "新文件", ed.files[filepath.Join(dir, "sub", "b.go")])
	require.DirExists(t, filepath.Join(dir, "sub"), "父目录由本模块创建")

	_, err = s.FileSystem().WriteFile(ctx, "nope.go", []byte("x"), env.WriteIntent{IfVersion: versionOf([]byte("旧"))})
	require.ErrorIs(t, err, env.ErrStale, "文件不存在时条件写入冲突")

	require.NoError(t, os.Mkdir(filepath.Join(dir, "d"), 0o755))
	_, err = s.FileSystem().WriteFile(ctx, "d", []byte("x"), env.WriteIntent{})
	require.ErrorContains(t, err, "是目录")
	_, _, err = s.FileSystem().ReadFile(ctx, "d")
	require.ErrorContains(t, err, "是目录")
	require.NotContains(t, ed.reads, filepath.Join(dir, "d"))
}

func TestClientFSDelegatesOtherOps(t *testing.T) {
	s, ed, dir := fsSession(t, fsCaps)
	ctx := context.Background()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.go"), []byte("x"), 0o644))
	info, err := s.FileSystem().Stat(ctx, "a.go")
	require.NoError(t, err)
	require.EqualValues(t, 1, info.Size)
	var seen []string
	require.NoError(t, s.FileSystem().Walk(ctx, ".", func(i env.Info) error { seen = append(seen, i.Path); return nil }))
	require.Contains(t, seen, "a.go")
	require.NoError(t, s.FileSystem().Remove(ctx, "a.go"))
	require.Empty(t, ed.reads)
}

func TestFSFallsBackWithoutCapability(t *testing.T) {
	s, ed, dir := fsSession(t, acpsdk.ClientCapabilities{Fs: acpsdk.FileSystemCapabilities{ReadTextFile: true}})
	ctx := context.Background()
	path := filepath.Join(dir, "a.go")
	ed.files[path] = "缓冲区"
	_, err := s.FileSystem().WriteFile(ctx, "a.go", []byte("本地写"), env.WriteIntent{})
	require.NoError(t, err)
	data, _, err := s.FileSystem().ReadFile(ctx, "a.go")
	require.NoError(t, err)
	require.Equal(t, "本地写", string(data), "只支持一侧时读写都走磁盘")
	disk, _ := os.ReadFile(path)
	require.Equal(t, "本地写", string(disk))
	require.Empty(t, ed.writes)
	require.Empty(t, ed.reads)
}
