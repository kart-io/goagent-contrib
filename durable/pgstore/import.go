package pgstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kart-io/goagent/core/durable"
)

// ImportResult 汇总一次导入。
type ImportResult struct {
	Imported int
	// Existing 是目标库中已存在、未覆盖的快照数。
	Existing int
}

// ImportFileStore 把 durable.FileStore 目录中根命名空间的快照导入 pool 所在的库，已存在的 ID 不覆盖。
// 须在所有实例停止后离线执行：FileStore 与 pgstore 的锁互不可见。
// 作用域（dir/scopes）不导入：FileStore 只保存作用域名的摘要，名称无法恢复，依赖作用域数据的运行须在切换前结束。
func ImportFileStore(ctx context.Context, pool *pgxpool.Pool, dir string) (ImportResult, error) {
	var result ImportResult
	entries, err := os.ReadDir(dir)
	if err != nil {
		return result, err
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".json") || strings.HasPrefix(name, ".") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return result, err
		}
		var snap durable.Snapshot
		if err := json.Unmarshal(data, &snap); err != nil {
			return result, fmt.Errorf("%s 不是有效的快照：%w", name, err)
		}
		// FileStore 的文件名是存储 ID 的摘要，各调用方都把存储 ID 写在 RunID 中，据此还原并核对。
		sum := sha256.Sum256([]byte(snap.RunID))
		if snap.RunID == "" || hex.EncodeToString(sum[:])+".json" != name {
			return result, fmt.Errorf("%s 的 RunID 与文件名不符，无法还原存储 ID", name)
		}
		tag, err := pool.Exec(ctx, `INSERT INTO goagent_durable_snapshot (ns, id, data) VALUES ('', $1, $2) ON CONFLICT (ns, id) DO NOTHING`, snap.RunID, data)
		if err != nil {
			return result, err
		}
		if tag.RowsAffected() == 1 {
			result.Imported++
		} else {
			result.Existing++
		}
	}
	return result, nil
}
