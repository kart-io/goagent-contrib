package qdrant

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kart-io/goagent/interfaces"
)

var _ interfaces.RequestRetriever = (*QdrantVectorStore)(nil)

func TestQdrantFilter(t *testing.T) {
	filter, err := qdrantFilter(nil)
	require.NoError(t, err)
	assert.Nil(t, filter)

	filter, err = qdrantFilter(map[string]any{"tenant_id": "a", "level": 2, "count": int64(3), "enabled": true})
	require.NoError(t, err)
	assert.Equal(t, []*qdrant.Condition{
		qdrant.NewMatchInt("count", 3),
		qdrant.NewMatchBool("enabled", true),
		qdrant.NewMatchInt("level", 2),
		qdrant.NewMatchKeyword("tenant_id", "a"),
	}, filter.Must, "按键排序，保证请求稳定")

	for _, value := range []any{1.5, []string{"a"}, nil} {
		_, err = qdrantFilter(map[string]any{"k": value})
		assert.Error(t, err, "%T 无法精确匹配，必须报错而不是忽略", value)
	}
}

func TestRealQdrantRetrieveFilter(t *testing.T) {
	address := os.Getenv("GOAGENT_TEST_QDRANT_ADDR")
	if address == "" {
		t.Skip("real service endpoint not configured: GOAGENT_TEST_QDRANT_ADDR")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	collection := "goagent_filter_" + uuid.NewString()
	store, err := NewQdrantVectorStore(ctx, QdrantConfig{URL: address, CollectionName: collection, VectorSize: 4})
	require.NoError(t, err)
	defer store.Close()
	defer func() { require.NoError(t, store.client.DeleteCollection(context.Background(), collection)) }()

	docs := []*interfaces.Document{
		{ID: "a1", PageContent: "a1", Metadata: map[string]any{"tenant_id": "a", "level": int64(1)}},
		{ID: "a2", PageContent: "a2", Metadata: map[string]any{"tenant_id": "a", "level": int64(2)}},
		{ID: "b1", PageContent: "b1", Metadata: map[string]any{"tenant_id": "b", "level": int64(1)}},
	}
	vector := []float32{1, 0, 0, 0}
	require.NoError(t, store.Add(ctx, docs, [][]float32{vector, vector, vector}))

	found, err := store.Retrieve(ctx, interfaces.RetrievalRequest{Query: "q", Filter: map[string]any{"tenant_id": "a"}})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a1", "a2"}, docIDs(found))

	found, err = store.Retrieve(ctx, interfaces.RetrievalRequest{Query: "q", Filter: map[string]any{"tenant_id": "a", "level": int64(2)}})
	require.NoError(t, err)
	assert.Equal(t, []string{"a2"}, docIDs(found))

	found, err = store.Retrieve(ctx, interfaces.RetrievalRequest{Query: "q", TopK: 1, Filter: map[string]any{"tenant_id": "b"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"b1"}, docIDs(found))
}

func docIDs(docs []*interfaces.Document) []string {
	out := make([]string, len(docs))
	for i, d := range docs {
		out[i] = d.ID
	}
	return out
}
