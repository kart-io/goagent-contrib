package cohere

import (
	"context"
	"os"

	"github.com/kart-io/goagent-contrib/retrieval"
)

// 导入本包即向 retrieval.NewEmbedder 注册 retrieval.EmbedderProviderCohere。
func init() {
	retrieval.RegisterEmbedderProvider(retrieval.EmbedderProviderCohere, func(ctx context.Context, options *retrieval.EmbedderOptions) (retrieval.Embedder, error) {
		return newCohereEmbedderFromOptions(options)
	})
}

// newCohereEmbedderFromOptions 从 Options 创建 Cohere 嵌入器
func newCohereEmbedderFromOptions(options *retrieval.EmbedderOptions) (retrieval.Embedder, error) {
	apiKey := options.APIKey
	if apiKey == "" {
		apiKey = os.Getenv("COHERE_API_KEY")
	}

	config := CohereEmbedderConfig{
		APIKey:     apiKey,
		Model:      options.Model,
		InputType:  options.InputType,
		Dimensions: options.Dimensions,
	}

	return NewCohereEmbedder(config)
}
