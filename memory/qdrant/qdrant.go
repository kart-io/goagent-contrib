// Package qdrant 是基于 Qdrant 的 memory.Index 实现，供多个实例共享长期记忆的向量索引。
//
// 每个维度一个集合 <Prefix>_d<维度>（Cosine 距离），首次写入该维度时创建；同一命名空间可以共存不同维度的向量。
// 点 ID 由命名空间与键确定性派生，payload 存 ns（命名空间各组件以 NUL 连接，建 keyword 索引）与 id。
// Search 在命名空间内精确检索（exact），排序与 memory.MemoryIndex 一致：分数降序，并列按 ID 字节序升序；
// 分数为 float32 精度，与 MemoryIndex 相差在 1e-5 内。
//
// 前置条件：Client 由宿主创建并负责关闭；同一 Prefix 下的集合只由本包写入。
//
// 限制：同 ID 换维度时先写新维度再删旧维度，两步之间失败会让旧维度残留一份，下次写入或 Memory.Reindex 时清理。
package qdrant

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
	qc "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentErrors "github.com/kart-io/goagent/errors"
	"github.com/kart-io/goagent/memory"
)

// DefaultPrefix 是 Config.Prefix 为空时使用的集合名前缀。
const DefaultPrefix = "goagent_memory"

const (
	nsField = "ns"
	idField = "id"
	// tieEpsilon 用于 Search 取出与第 k 名分数并列（含 float32 舍入误差）的全部候选。
	tieEpsilon = 1e-6
)

// pointNamespace 是派生点 ID 的固定命名空间，改动会让既有数据无法按 ID 定位。
var pointNamespace = uuid.MustParse("c100f59c-29a4-4a2c-87c6-135003181c5e")

// Config 配置 Index。
type Config struct {
	Client *qc.Client
	// Prefix 为集合名前缀，默认 DefaultPrefix。
	Prefix string
}

// Index 实现 memory.Index。
type Index struct {
	client *qc.Client
	prefix string
}

var _ memory.Index = (*Index)(nil)

// New 创建 Index；集合在首次写入对应维度时创建。
func New(_ context.Context, cfg Config) (*Index, error) {
	if cfg.Client == nil {
		return nil, agentErrors.New(agentErrors.CodeInvalidInput, "qdrant 需要 Client").WithComponent("memory/qdrant")
	}
	prefix := cfg.Prefix
	if prefix == "" {
		prefix = DefaultPrefix
	}
	return &Index{client: cfg.Client, prefix: prefix}, nil
}

// Upsert 归一化后写入本维度集合，再从其他维度集合删除同一点，使同 ID 换维度时只保留新向量。
func (x *Index) Upsert(ctx context.Context, ns []string, id string, vector []float32) error {
	if err := memory.ValidateNamespace(ns); err != nil {
		return err
	}
	unit64, err := memory.NormalizeVector(vector)
	if err != nil {
		return err
	}
	unit := toFloat32(unit64)
	nsKey := namespaceKey(ns)
	pid := pointID(nsKey, id)
	target := x.collection(len(unit))
	req := &qc.UpsertPoints{
		CollectionName: target,
		Wait:           new(true),
		Points: []*qc.PointStruct{{
			Id:      pid,
			Vectors: qc.NewVectorsDense(unit),
			Payload: map[string]*qc.Value{nsField: qc.NewValueString(nsKey), idField: qc.NewValueString(id)},
		}},
	}
	_, err = x.client.Upsert(ctx, req)
	if isNotFound(err) {
		if err = x.createCollection(ctx, target, len(unit)); err != nil {
			return err
		}
		_, err = x.client.Upsert(ctx, req)
	}
	if err != nil {
		return fmt.Errorf("写入向量失败：%w", err)
	}
	names, err := x.collections(ctx)
	if err != nil {
		return err
	}
	for _, name := range names {
		if name == target {
			continue
		}
		if err := x.deletePoints(ctx, name, qc.NewPointsSelector(pid)); err != nil {
			return fmt.Errorf("删除旧维度向量失败：%w", err)
		}
	}
	return nil
}

// Delete 删除精确命名空间中的一个键，键不存在不是错误。
func (x *Index) Delete(ctx context.Context, ns []string, id string) error {
	if memory.ValidateNamespace(ns) != nil {
		return nil
	}
	return x.deleteAll(ctx, qc.NewPointsSelector(pointID(namespaceKey(ns), id)), "删除向量失败")
}

// Clear 删除精确命名空间中的全部键，不递归到子命名空间。
func (x *Index) Clear(ctx context.Context, ns []string) error {
	if memory.ValidateNamespace(ns) != nil {
		return nil
	}
	return x.deleteAll(ctx, qc.NewPointsSelectorFilter(nsFilter(namespaceKey(ns))), "清空命名空间失败")
}

// Search 只在精确命名空间内、与查询同维度的向量中按余弦相似度降序取前 k 个，分数相同按 ID 升序。
// Qdrant 对并列分数的返回顺序不确定：若第 k 名之后可能有并列，按第 k 名分数为阈值取出全部候选再在本地排序截断。
func (x *Index) Search(ctx context.Context, ns []string, vector []float32, k int) ([]memory.Hit, error) {
	if k <= 0 {
		return nil, nil
	}
	unit64, err := memory.NormalizeVector(vector)
	if err != nil {
		return nil, err
	}
	if memory.ValidateNamespace(ns) != nil {
		return nil, nil
	}
	req := &qc.QueryPoints{
		CollectionName: x.collection(len(unit64)),
		Query:          qc.NewQueryDense(toFloat32(unit64)),
		Filter:         nsFilter(namespaceKey(ns)),
		Params:         &qc.SearchParams{Exact: new(true)},
		WithPayload:    qc.NewWithPayloadInclude(idField),
		Limit:          new(uint64(k) + 1),
	}
	var points []*qc.ScoredPoint
	for {
		points, err = x.client.Query(ctx, req)
		if isNotFound(err) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("检索向量失败：%w", err)
		}
		if uint64(len(points)) < *req.Limit || points[len(points)-1].GetScore() < points[k-1].GetScore()-tieEpsilon {
			break
		}
		req.ScoreThreshold = new(points[k-1].GetScore() - tieEpsilon)
		req.Limit = new(*req.Limit * 2)
	}
	hits := make([]memory.Hit, len(points))
	for i, p := range points {
		hits[i] = memory.Hit{ID: p.GetPayload()[idField].GetStringValue(), Score: float64(p.GetScore())}
	}
	slices.SortFunc(hits, func(a, b memory.Hit) int {
		if c := cmp.Compare(b.Score, a.Score); c != 0 {
			return c
		}
		return strings.Compare(a.ID, b.ID)
	})
	return hits[:min(k, len(hits))], nil
}

// IDs 列出精确命名空间中的全部键。
func (x *Index) IDs(ctx context.Context, ns []string) ([]string, error) {
	if memory.ValidateNamespace(ns) != nil {
		return nil, nil
	}
	names, err := x.collections(ctx)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, name := range names {
		req := &qc.ScrollPoints{
			CollectionName: name,
			Filter:         nsFilter(namespaceKey(ns)),
			WithPayload:    qc.NewWithPayloadInclude(idField),
			Limit:          new(uint32(256)),
		}
		for {
			points, next, err := x.client.ScrollAndOffset(ctx, req)
			if isNotFound(err) {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("列出向量键失败：%w", err)
			}
			for _, p := range points {
				ids = append(ids, p.GetPayload()[idField].GetStringValue())
			}
			if next == nil {
				break
			}
			req.Offset = next
		}
	}
	return ids, nil
}

// deleteAll 在本前缀的全部集合中删除选中的点。
func (x *Index) deleteAll(ctx context.Context, sel *qc.PointsSelector, msg string) error {
	names, err := x.collections(ctx)
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := x.deletePoints(ctx, name, sel); err != nil {
			return fmt.Errorf("%s：%w", msg, err)
		}
	}
	return nil
}

// deletePoints 删除集合中选中的点；集合已被删除视为成功。
func (x *Index) deletePoints(ctx context.Context, name string, sel *qc.PointsSelector) error {
	_, err := x.client.Delete(ctx, &qc.DeletePoints{CollectionName: name, Wait: new(true), Points: sel})
	if isNotFound(err) {
		return nil
	}
	return err
}

// createCollection 建集合与 ns 的 keyword 索引；并发创建时对方已建好视为成功。
func (x *Index) createCollection(ctx context.Context, name string, dim int) error {
	err := x.client.CreateCollection(ctx, &qc.CreateCollection{
		CollectionName: name,
		VectorsConfig:  qc.NewVectorsConfig(&qc.VectorParams{Size: uint64(dim), Distance: qc.Distance_Cosine}),
	})
	if err != nil && !isAlreadyExists(err) {
		return fmt.Errorf("创建集合 %s 失败：%w", name, err)
	}
	_, err = x.client.CreateFieldIndex(ctx, &qc.CreateFieldIndexCollection{
		CollectionName: name,
		Wait:           new(true),
		FieldName:      nsField,
		FieldType:      new(qc.FieldType_FieldTypeKeyword),
	})
	if err != nil {
		return fmt.Errorf("为集合 %s 建 ns 索引失败：%w", name, err)
	}
	return nil
}

// collections 列出本前缀下的全部维度集合。
func (x *Index) collections(ctx context.Context) ([]string, error) {
	all, err := x.client.ListCollections(ctx)
	if err != nil {
		return nil, fmt.Errorf("列出集合失败：%w", err)
	}
	var out []string
	for _, name := range all {
		rest, ok := strings.CutPrefix(name, x.prefix+"_d")
		if !ok {
			continue
		}
		// 只认纯数字后缀，避免前缀为 a 时误把 a_d1_d2 之类别人的集合算进来。
		if _, err := strconv.ParseUint(rest, 10, 64); err == nil {
			out = append(out, name)
		}
	}
	return out, nil
}

func (x *Index) collection(dim int) string { return x.prefix + "_d" + strconv.Itoa(dim) }

func namespaceKey(ns []string) string { return strings.Join(ns, "\x00") }

// pointID 由命名空间与键派生；命名空间组件不含 NUL，拼接无歧义。
func pointID(nsKey, id string) *qc.PointId {
	return qc.NewID(uuid.NewSHA1(pointNamespace, []byte(nsKey+"\x00"+id)).String())
}

func nsFilter(nsKey string) *qc.Filter {
	return &qc.Filter{Must: []*qc.Condition{qc.NewMatchKeyword(nsField, nsKey)}}
}

func isNotFound(err error) bool { return grpcCode(err) == codes.NotFound }

func isAlreadyExists(err error) bool {
	if grpcCode(err) == codes.AlreadyExists {
		return true
	}
	// 部分版本以 InvalidArgument 加文案报告集合已存在。
	return err != nil && strings.Contains(err.Error(), "already exists")
}

func grpcCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	var s interface{ GRPCStatus() *status.Status }
	if errors.As(err, &s) {
		return s.GRPCStatus().Code()
	}
	return codes.Unknown
}

// toFloat32 把归一化后的单位向量转为 Qdrant 存储的 float32。
func toFloat32(unit []float64) []float32 {
	out := make([]float32, len(unit))
	for i, f := range unit {
		out[i] = float32(f)
	}
	return out
}
