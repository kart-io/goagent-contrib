// Package pgvector 是基于 Postgres pgvector 扩展的 memory.Index 实现，供多个实例共享长期记忆的向量索引。
//
// 命名空间内做精确扫描（主键前缀过滤加逐行余弦距离），不建 ANN 索引：向量维度不固定（不同嵌入模型维度不同），HNSW 无法使用；
// 长期记忆按租户或用户分命名空间，单命名空间记录数受 Retention.MaxRecords 约束（通常数千以内），精确扫描足够，
// 且排序与 memory.MemoryIndex 一致（分数在 float32 精度内）。向量较大时行数据走 TOAST，扫描成本随命名空间大小线性增长；
// 命名空间规模远超此范围时建议给数据库角色设置 statement_timeout，并改为按固定维度分表加 HNSW 索引的实现。
//
// 前置条件：
//   - 已执行 CREATE EXTENSION vector（需要较高权限，由宿主或 DBA 完成，不在 Schema 中）。
//   - Pool 指向主库：从库复制延迟会让刚删除的向量仍被召回。可与 durable/pgstore 共用同一个连接池。
//   - 表结构见 Schema，由宿主建表。
package pgvector

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgv "github.com/pgvector/pgvector-go"

	agentErrors "github.com/kart-io/goagent/errors"
	"github.com/kart-io/goagent/memory"
)

// Config 配置 Index。
type Config struct {
	// Pool 须直连主库。向量参数以 pgvector.NewVector 传入，不要求在连接池上注册 vector 类型。
	Pool *pgxpool.Pool
}

// Index 实现 memory.Index。
type Index struct {
	pool *pgxpool.Pool
}

var _ memory.Index = (*Index)(nil)

// New 检查 vector 扩展已安装且表结构版本为 SchemaVersion。
func New(ctx context.Context, cfg Config) (*Index, error) {
	if cfg.Pool == nil {
		return nil, errors.New("pgvector 需要 Pool")
	}
	var installed bool
	if err := cfg.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector')`).Scan(&installed); err != nil {
		return nil, fmt.Errorf("检查 vector 扩展失败：%w", err)
	}
	if !installed {
		return nil, errors.New("数据库未安装 vector 扩展：请由有权限的角色先执行 CREATE EXTENSION vector")
	}
	var version int
	if err := cfg.Pool.QueryRow(ctx, `SELECT version FROM goagent_memory_vectors_schema`).Scan(&version); err != nil {
		return nil, fmt.Errorf("读取 pgvector 表结构版本失败（是否已执行 pgvector.Schema）：%w", err)
	}
	if version != SchemaVersion {
		return nil, fmt.Errorf("pgvector 表结构版本为 %d，需要 %d", version, SchemaVersion)
	}
	return &Index{pool: cfg.Pool}, nil
}

// Upsert 归一化后写入；拒绝非法命名空间、零向量与非有限值。同 ID 覆盖，维度可以改变。
func (x *Index) Upsert(ctx context.Context, ns []string, id string, vector []float32) error {
	if err := validNamespace(ns); err != nil {
		return err
	}
	unit, err := normalize(vector)
	if err != nil {
		return err
	}
	_, err = x.pool.Exec(ctx, `
INSERT INTO goagent_memory_vectors (ns, id, dim, vec) VALUES ($1, $2, $3, $4)
ON CONFLICT (ns, id) DO UPDATE SET dim = EXCLUDED.dim, vec = EXCLUDED.vec`,
		ns, id, len(unit), pgv.NewVector(unit))
	if err != nil {
		return fmt.Errorf("写入向量失败：%w", err)
	}
	return nil
}

// Delete 删除精确命名空间中的一个键，键不存在不是错误。
func (x *Index) Delete(ctx context.Context, ns []string, id string) error {
	if validNamespace(ns) != nil {
		return nil
	}
	if _, err := x.pool.Exec(ctx, `DELETE FROM goagent_memory_vectors WHERE ns = $1 AND id = $2`, ns, id); err != nil {
		return fmt.Errorf("删除向量失败：%w", err)
	}
	return nil
}

// Search 只在精确命名空间内、与查询同维度的向量中按余弦相似度降序取前 k 个，分数相同按 ID 升序。
func (x *Index) Search(ctx context.Context, ns []string, vector []float32, k int) ([]memory.Hit, error) {
	if k <= 0 {
		return nil, nil
	}
	unit, err := normalize(vector)
	if err != nil {
		return nil, err
	}
	if validNamespace(ns) != nil {
		return nil, nil
	}
	rows, err := x.pool.Query(ctx, `
SELECT id, vec <=> $3 AS distance FROM goagent_memory_vectors
WHERE ns = $1 AND dim = $2 ORDER BY vec <=> $3, id LIMIT $4`,
		ns, len(unit), pgv.NewVector(unit), k)
	if err != nil {
		return nil, fmt.Errorf("检索向量失败：%w", err)
	}
	hits, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (memory.Hit, error) {
		var h memory.Hit
		var distance float64
		err := row.Scan(&h.ID, &distance)
		h.Score = 1 - distance
		return h, err
	})
	if err != nil {
		return nil, fmt.Errorf("读取检索结果失败：%w", err)
	}
	return hits, nil
}

// IDs 列出精确命名空间中的全部键。
func (x *Index) IDs(ctx context.Context, ns []string) ([]string, error) {
	if validNamespace(ns) != nil {
		return nil, nil
	}
	rows, err := x.pool.Query(ctx, `SELECT id FROM goagent_memory_vectors WHERE ns = $1`, ns)
	if err != nil {
		return nil, fmt.Errorf("列出向量键失败：%w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("读取向量键失败：%w", err)
	}
	return ids, nil
}

// Clear 删除精确命名空间中的全部键，不递归到子命名空间。
func (x *Index) Clear(ctx context.Context, ns []string) error {
	if validNamespace(ns) != nil {
		return nil
	}
	if _, err := x.pool.Exec(ctx, `DELETE FROM goagent_memory_vectors WHERE ns = $1`, ns); err != nil {
		return fmt.Errorf("清空命名空间失败：%w", err)
	}
	return nil
}

// validNamespace 与 memory.MemoryIndex 的校验一致：非空、组件不能为空白；另外 text[] 存不了 NUL。
func validNamespace(ns []string) error {
	if len(ns) == 0 || slices.ContainsFunc(ns, func(s string) bool { return strings.TrimSpace(s) == "" }) {
		return agentErrors.Newf(agentErrors.CodeInvalidInput, "长期记忆需要命名空间：用 memory.WithNamespace 绑定可信身份").WithComponent("pgvector").WithOperation("namespace")
	}
	if slices.ContainsFunc(ns, func(s string) bool { return strings.ContainsRune(s, 0) }) {
		return agentErrors.Newf(agentErrors.CodeInvalidInput, "长期记忆的命名空间组件不能包含 NUL").WithComponent("pgvector").WithOperation("namespace")
	}
	return nil
}

// normalize 与 memory.MemoryIndex 同样以 float64 计算范数（避免大幅值 float32 平方溢出），结果转回 float32 存储。
func normalize(v []float32) ([]float32, error) {
	scale := 0.0
	for _, f := range v {
		x := float64(f)
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil, agentErrors.Newf(agentErrors.CodeInvalidInput, "向量含非有限值").WithComponent("pgvector").WithOperation("normalize")
		}
		scale = max(scale, math.Abs(x))
	}
	if scale == 0 {
		return nil, agentErrors.Newf(agentErrors.CodeInvalidInput, "向量为空或全为零").WithComponent("pgvector").WithOperation("normalize")
	}
	norm := 0.0
	for _, f := range v {
		s := float64(f) / scale
		norm += s * s
	}
	norm = math.Sqrt(norm)
	out := make([]float32, len(v))
	for i, f := range v {
		out[i] = float32(float64(f) / scale / norm)
	}
	return out, nil
}
