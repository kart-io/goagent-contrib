package acp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/kart-io/goagent/harness/env"
)

// clientFS 把读写转发给编辑器，使 Agent 读到未保存的缓冲区、写入出现在编辑器中；
// 路径限定与 ACP 没有的操作（Stat、Walk、Remove）沿用工作区的 LocalFS。
// 编辑器不参与写入协调，条件写入的检查与写入之间的修改无法发现。
type clientFS struct {
	s           *Session
	read, write bool
}

var _ env.FileSystem = (*clientFS)(nil)

func (f *clientFS) Resolve(path string) (string, error) { return f.s.local.Resolve(path) }

func (f *clientFS) Stat(ctx context.Context, path string) (env.Info, error) {
	return f.s.local.Stat(ctx, path)
}

func (f *clientFS) Remove(ctx context.Context, path string) error { return f.s.local.Remove(ctx, path) }

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
	if !f.read {
		return f.s.local.ReadFile(ctx, path)
	}
	abs, err := f.abs(path)
	if err != nil {
		return nil, "", err
	}
	// ACP 没有标准的"不存在"错误码，存在性以磁盘为准。
	if _, err := f.s.local.Stat(ctx, path); err != nil {
		return nil, "", err
	}
	resp, err := f.s.conn.client().ReadTextFile(ctx, acpsdk.ReadTextFileRequest{SessionId: acpsdk.SessionId(f.s.id), Path: abs})
	if err != nil {
		return nil, "", fmt.Errorf("acp: 经编辑器读取 %s：%w", path, err)
	}
	data := []byte(resp.Content)
	return data, versionOf(data), nil
}

func (f *clientFS) WriteFile(ctx context.Context, path string, data []byte, intent env.WriteIntent) (env.Version, error) {
	if !f.write {
		return f.s.local.WriteFile(ctx, path, data, intent)
	}
	abs, err := f.abs(path)
	if err != nil {
		return "", err
	}
	if intent.CreateIfAbsent {
		_, err := f.s.local.Stat(ctx, path)
		if err == nil {
			return "", env.ErrExists
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
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
