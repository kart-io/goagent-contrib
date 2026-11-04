package cohere

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kart-io/goagent-contrib/retrieval"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEmbedder_Cohere_NoAPIKey(t *testing.T) {
	ctx := context.Background()

	// 确保环境变量未设置
	t.Setenv("COHERE_API_KEY", "")

	embedder, err := retrieval.NewEmbedder(ctx,
		retrieval.WithProvider(retrieval.EmbedderProviderCohere),
	)

	assert.Error(t, err)
	assert.Nil(t, embedder)
	assert.Contains(t, err.Error(), "API key is required")
}

func TestCohereEmbedder_Creation(t *testing.T) {
	config := CohereEmbedderConfig{
		APIKey: "test-api-key",
	}

	embedder, err := NewCohereEmbedder(config)
	require.NoError(t, err)
	require.NotNil(t, embedder)

	// 验证默认值
	assert.Equal(t, "embed-english-v3.0", embedder.model)
	assert.Equal(t, "search_document", embedder.inputType)
	assert.Equal(t, 1024, embedder.Dimensions())
}

func TestCohereEmbedder_Dimensions(t *testing.T) {
	tests := []struct {
		model      string
		dimensions int
	}{
		{"embed-english-v3.0", 1024},
		{"embed-multilingual-v3.0", 1024},
		{"embed-english-light-v3.0", 384},
		{"embed-multilingual-light-v3.0", 384},
		{"unknown-model", 1024},
	}

	for _, tt := range tests {
		t.Run(tt.model, func(t *testing.T) {
			config := CohereEmbedderConfig{
				APIKey: "test-api-key",
				Model:  tt.model,
			}
			embedder, err := NewCohereEmbedder(config)
			require.NoError(t, err)
			assert.Equal(t, tt.dimensions, embedder.Dimensions())
		})
	}
}

func TestCohereEmbedder_Embed_EmptyTexts(t *testing.T) {
	config := CohereEmbedderConfig{
		APIKey: "test-api-key",
	}
	embedder, _ := NewCohereEmbedder(config)

	result, err := embedder.EmbedBatch(context.Background(), []string{})
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestCohereEmbedder_EmbedBatch_MockServer(t *testing.T) {
	// 创建模拟服务器
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Contains(t, r.Header.Get("Authorization"), "Bearer")

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"id": "test-id",
			"texts": ["hello", "world"],
			"embeddings": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]
		}`))
	}))
	defer server.Close()

	config := CohereEmbedderConfig{
		APIKey:  "test-api-key",
		BaseURL: server.URL,
	}
	embedder, _ := NewCohereEmbedder(config)

	result, err := embedder.EmbedBatch(context.Background(), []string{"hello", "world"})
	require.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Len(t, result[0], 3)
}

func TestCohereEmbedder_EmbedSingle_MockServer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"id": "test-id",
			"texts": ["query"],
			"embeddings": [[0.1, 0.2, 0.3]]
		}`))
	}))
	defer server.Close()

	config := CohereEmbedderConfig{
		APIKey:  "test-api-key",
		BaseURL: server.URL,
	}
	embedder, _ := NewCohereEmbedder(config)

	result, err := embedder.Embed(context.Background(), "query")
	require.NoError(t, err)
	assert.Len(t, result, 3)
}

func TestCohereEmbedder_Embed_ErrorResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message": "invalid request"}`))
	}))
	defer server.Close()

	config := CohereEmbedderConfig{
		APIKey:  "test-api-key",
		BaseURL: server.URL,
	}
	embedder, _ := NewCohereEmbedder(config)

	result, err := embedder.EmbedBatch(context.Background(), []string{"hello"})
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "invalid request")
}
