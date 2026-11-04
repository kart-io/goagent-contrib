package retrieval

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	agentErrors "github.com/kart-io/goagent/errors"
	"github.com/kart-io/goagent/interfaces"
	"github.com/kart-io/goagent/llm"
)

// 任意 Embedder 都满足核心的单条嵌入接口，可直接交给 memory.WithEmbedder 与语义缓存。
var _ llm.EmbeddingClient = Embedder(nil)

var (
	_ interfaces.RequestRetriever = (*MemoryVectorStore)(nil)
	_ interfaces.RequestRetriever = (*VectorStoreRetriever)(nil)
)

func newTenantStore(t *testing.T) *MemoryVectorStore {
	t.Helper()
	store := NewMemoryVectorStore(MemoryVectorStoreConfig{})
	docs := []*interfaces.Document{
		{ID: "a1", PageContent: "refund policy seven days", Metadata: map[string]any{"tenant_id": "a", "level": 1}},
		{ID: "a2", PageContent: "payment arrives in three days", Metadata: map[string]any{"tenant_id": "a", "level": 2}},
		{ID: "b1", PageContent: "refund policy fifteen days", Metadata: map[string]any{"tenant_id": "b", "level": 1}},
		{ID: "n1", PageContent: "refund policy without tenant"},
	}
	require.NoError(t, store.AddDocuments(context.Background(), docs))
	return store
}

func ids(docs []*interfaces.Document) []string {
	out := make([]string, len(docs))
	for i, d := range docs {
		out[i] = d.ID
	}
	return out
}

func TestMemoryVectorStoreRetrieveFilter(t *testing.T) {
	ctx := context.Background()
	store := newTenantStore(t)

	docs, err := store.Retrieve(ctx, interfaces.RetrievalRequest{Query: "refund policy", Filter: map[string]any{"tenant_id": "a"}})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a1", "a2"}, ids(docs))
	assert.Equal(t, "a1", docs[0].ID, "与查询同词的文档排在前面")
	assert.Positive(t, docs[0].Score, "过滤后的结果仍须带相似度分数")

	docs, err = store.Retrieve(ctx, interfaces.RetrievalRequest{Query: "refund policy", Filter: map[string]any{"tenant_id": "a", "level": 2}})
	require.NoError(t, err)
	assert.Equal(t, []string{"a2"}, ids(docs), "多个键须同时满足")

	docs, err = store.Retrieve(ctx, interfaces.RetrievalRequest{Query: "refund policy", Filter: map[string]any{"tenant_id": "c"}})
	require.NoError(t, err)
	assert.Empty(t, docs)

	docs, err = store.Retrieve(ctx, interfaces.RetrievalRequest{Query: "refund policy", Filter: map[string]any{"level": int64(1)}})
	require.NoError(t, err)
	assert.Empty(t, docs, "值的类型也须一致，int 与 int64 不相等")

	docs, err = store.Retrieve(ctx, interfaces.RetrievalRequest{Query: "refund policy", TopK: 1, Filter: map[string]any{"tenant_id": "a"}})
	require.NoError(t, err)
	assert.Len(t, docs, 1, "TopK 在过滤之后生效")

	docs, err = store.Retrieve(ctx, interfaces.RetrievalRequest{Query: "refund policy"})
	require.NoError(t, err)
	assert.Len(t, docs, 4, "无过滤条件时 TopK 默认 4")
}

func TestVectorStoreRetrieverRetrieve(t *testing.T) {
	ctx := context.Background()
	t.Run("委托给支持过滤的向量库", func(t *testing.T) {
		r := NewVectorStoreRetriever(newTenantStore(t), RetrieverConfig{TopK: 1})
		docs, err := r.Retrieve(ctx, interfaces.RetrievalRequest{Query: "refund policy", Filter: map[string]any{"tenant_id": "b"}})
		require.NoError(t, err)
		assert.Equal(t, []string{"b1"}, ids(docs))
	})
	t.Run("请求未给 TopK 时用检索器配置", func(t *testing.T) {
		r := NewVectorStoreRetriever(newTenantStore(t), RetrieverConfig{TopK: 1})
		docs, err := r.Retrieve(ctx, interfaces.RetrievalRequest{Query: "refund policy", Filter: map[string]any{"tenant_id": "a"}})
		require.NoError(t, err)
		assert.Len(t, docs, 1)
	})
	t.Run("分数阈值模式在过滤后仍生效", func(t *testing.T) {
		r := NewVectorStoreRetriever(newTenantStore(t), RetrieverConfig{TopK: 4, MinScore: 2}).
			WithSearchType(SearchTypeSimilarityScoreThreshold)
		docs, err := r.Retrieve(ctx, interfaces.RetrievalRequest{Query: "refund policy", Filter: map[string]any{"tenant_id": "a"}})
		require.NoError(t, err)
		assert.Empty(t, docs)
	})
	t.Run("向量库不支持过滤时拒绝", func(t *testing.T) {
		r := NewVectorStoreRetriever(NewMockVectorStore(), DefaultRetrieverConfig())
		_, err := r.Retrieve(ctx, interfaces.RetrievalRequest{Query: "q", Filter: map[string]any{"tenant_id": "a"}})
		require.Error(t, err)
		assert.Equal(t, agentErrors.CodeInvalidInput, agentErrors.GetCode(err))
	})
	t.Run("无过滤条件时退回普通检索", func(t *testing.T) {
		r := NewVectorStoreRetriever(newTenantStore(t), RetrieverConfig{TopK: 2})
		docs, err := r.Retrieve(ctx, interfaces.RetrievalRequest{Query: "refund policy"})
		require.NoError(t, err)
		assert.Len(t, docs, 2)
	})
}
