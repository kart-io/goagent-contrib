package vertexai

import (
	"context"
	"os"

	"github.com/kart-io/goagent-contrib/retrieval"
)

// 导入本包即向 retrieval.NewEmbedder 注册 retrieval.EmbedderProviderVertexAI。
func init() {
	retrieval.RegisterEmbedderProvider(retrieval.EmbedderProviderVertexAI, func(ctx context.Context, options *retrieval.EmbedderOptions) (retrieval.Embedder, error) {
		return newVertexAIEmbedderFromOptions(ctx, options)
	})
}

// newVertexAIEmbedderFromOptions 从 Options 创建 Vertex AI 嵌入器
func newVertexAIEmbedderFromOptions(ctx context.Context, options *retrieval.EmbedderOptions) (*VertexAIEmbedder, error) {
	projectID := options.ProjectID
	if projectID == "" {
		projectID = os.Getenv("GOOGLE_CLOUD_PROJECT")
		if projectID == "" {
			projectID = os.Getenv("GCLOUD_PROJECT")
		}
	}

	config := VertexAIEmbedderConfig{
		ProjectID:  projectID,
		Location:   options.Location,
		ModelID:    options.Model,
		Dimensions: options.Dimensions,
	}

	return NewVertexAIEmbedder(ctx, config)
}
