package acp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"unicode/utf8"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/kart-io/goagent/harness/env"
)

// clientFS 把读写转发给编辑器，使 Agent 读到未保存的缓冲区、写入出现在编辑器中；
// 路径限定与 ACP 没有的操作（Stat、Walk、Remove）沿用工作区的 LocalFS。
// 同 Server 内转发写入与删除串行协调；编辑器手工改动、其他 Server 与绕过本接口的写者不参与。
// 仅在客户端同时支持读与写时使用：只转发一侧会让版本比对与磁盘、缓冲区错位。
type clientFS struct{ s *Session }

var _ env.FileSystem = (*clientFS)(nil)

func (f *clientFS) Resolve(path string) (string, error) { return f.s.local.Resolve(path) }

func (f *clientFS) Stat(ctx context.Context, path string) (env.Info, error) {
	return f.s.local.Stat(ctx, path)
}

func (f *clientFS) Remove(ctx context.Context, path string) error {
	unlock, err := f.lockWrite(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	return f.s.local.Remove(ctx, path)
}

func (f *clientFS) lockWrite(ctx context.Context) (func(), error) {
	gate := f.s.conn.srv.fileWrites
	select {
	case gate <- struct{}{}:
		return func() { <-gate }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *clientFS) Walk(ctx context.Context, path string, fn func(env.Info) error) error {
	return f.s.local.Walk(ctx, path, fn)
}

// abs 把工作区路径转为 ACP 要求的绝对路径。
func (f *clientFS) abs(path string) (string, error) {
	rel, err := f.s.local.Resolve(path)
	if err != nil {
		return "", err
	}
	return filepath.Join(f.s.cwd, filepath.FromSlash(rel)), nil
}

func (f *clientFS) ReadFile(ctx context.Context, path string) ([]byte, env.Version, error) {
	abs, err := f.abs(path)
	if err != nil {
		return nil, "", err
	}
	// ACP 没有标准的"不存在"错误码，存在性以磁盘为准。
	info, err := f.s.local.Stat(ctx, path)
	if err != nil {
		return nil, "", err
	}
	if info.IsDir() {
		return nil, "", fmt.Errorf("acp: %s 是目录", path)
	}
	resp, err := f.s.conn.client().ReadTextFile(ctx, acpsdk.ReadTextFileRequest{SessionId: acpsdk.SessionId(f.s.id), Path: abs})
	if err != nil {
		return nil, "", fmt.Errorf("acp: 经编辑器读取 %s：%w", path, err)
	}
	data := []byte(resp.Content)
	return data, versionOf(data), nil
}

func (f *clientFS) WriteFile(ctx context.Context, path string, data []byte, intent env.WriteIntent) (env.Version, error) {
	if intent.Perm != 0 || !utf8.Valid(data) {
		return "", fmt.Errorf("acp: 编辑器仅支持 UTF-8 文本及默认权限：%w", errors.ErrUnsupported)
	}
	unlock, err := f.lockWrite(ctx)
	if err != nil {
		return "", err
	}
	defer unlock()
	abs, err := f.abs(path)
	if err != nil {
		return "", err
	}
	info, err := f.s.local.Stat(ctx, path)
	switch {
	case err == nil:
		if info.IsDir() {
			return "", fmt.Errorf("acp: %s 是目录", path)
		}
		if intent.CreateIfAbsent {
			return "", env.ErrExists
		}
		if intent.IfVersion != "" {
			_, current, err := f.ReadFile(ctx, path)
			if err != nil {
				return "", err
			}
			if current != intent.IfVersion {
				return "", env.ErrStale
			}
		}
	case errors.Is(err, fs.ErrNotExist):
		if intent.IfVersion != "" {
			return "", env.ErrStale
		}
	default:
		return "", err
	}
	// ACP 未规定编辑器是否创建父目录，由这里按需创建；非默认权限已在入口拒绝。
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return "", err
	}
	if _, err := f.s.conn.client().WriteTextFile(ctx, acpsdk.WriteTextFileRequest{SessionId: acpsdk.SessionId(f.s.id), Path: abs, Content: string(data)}); err != nil {
		return "", fmt.Errorf("acp: 经编辑器写入 %s：%w", path, err)
	}
	return versionOf(data), nil
}

// versionOf 与 env.LocalFS 的版本格式一致：内容 SHA-256 的十六进制。
func versionOf(data []byte) env.Version {
	sum := sha256.Sum256(data)
	return env.Version(hex.EncodeToString(sum[:]))
}
