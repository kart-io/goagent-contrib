package vertexai

import (
	"context"
	"testing"

	"github.com/kart-io/goagent-contrib/retrieval"
	"github.com/stretchr/testify/assert"
)

func TestNewEmbedder_VertexAI_NoProjectID(t *testing.T) {
	ctx := context.Background()

	// 确保环境变量未设置
	t.Setenv("GOOGLE_CLOUD_PROJECT", "")
	t.Setenv("GCLOUD_PROJECT", "")

	embedder, err := retrieval.NewEmbedder(ctx,
		retrieval.WithProvider(retrieval.EmbedderProviderVertexAI),
	)

	assert.Error(t, err)
	assert.Nil(t, embedder)
	assert.Contains(t, err.Error(), "project ID is required")
}

// ==================== 自定义提供商测试 ====================
