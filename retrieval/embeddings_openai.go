package retrieval

import (
	"context"
	"os"
	"strings"

	"github.com/sashabaranov/go-openai"

	agentErrors "github.com/kart-io/goagent/errors"
)

// OpenAIEmbedder 使用 OpenAI 的嵌入模型
//
// 支持 text-embedding-3-small, text-embedding-3-large 等模型
type OpenAIEmbedder struct {
	*BaseEmbedder
	client  *openai.Client
	model   openai.EmbeddingModel
	baseURL string
}

// OpenAIEmbedderConfig OpenAI 嵌入器配置
type OpenAIEmbedderConfig struct {
	// APIKey OpenAI API Key
	APIKey string
	// Model 模型名称，如 text-embedding-3-small
	Model string
	// BaseURL API 基础 URL（可选，用于兼容其他 API）
	BaseURL string
	// Dimensions 向量维度
	Dimensions int
}

// NewOpenAIEmbedder 创建 OpenAI 嵌入器
func NewOpenAIEmbedder(config OpenAIEmbedderConfig) (*OpenAIEmbedder, error) {
	if config.APIKey == "" {
		config.APIKey = os.Getenv("OPENAI_API_KEY")
	}
	if config.APIKey == "" {
		return nil, agentErrors.New(agentErrors.CodeInvalidConfig, "API key is required").
			WithComponent("openai_embedder").
			WithOperation("create")
	}

	if config.Model == "" {
		config.Model = "text-embedding-3-small"
	}

	if config.BaseURL == "" {
		config.BaseURL = "https://api.openai.com/v1"
	}

	if config.Dimensions <= 0 {
		// 根据模型设置默认维度
		switch config.Model {
		case "text-embedding-3-small":
			config.Dimensions = 1536
		case "text-embedding-3-large":
			config.Dimensions = 3072
		case "text-embedding-ada-002":
			config.Dimensions = 1536
		default:
			config.Dimensions = 1536
		}
	}

	// 创建 OpenAI 客户端
	clientConfig := openai.DefaultConfig(config.APIKey)
	clientConfig.BaseURL = strings.TrimSuffix(config.BaseURL, "/")

	// 将字符串模型转换为 openai.EmbeddingModel
	var model openai.EmbeddingModel
	switch config.Model {
	case "text-embedding-3-small":
		model = openai.SmallEmbedding3
	case "text-embedding-3-large":
		model = openai.LargeEmbedding3
	case "text-embedding-ada-002":
		model = openai.AdaEmbeddingV2
	default:
		model = openai.EmbeddingModel(config.Model)
	}

	return &OpenAIEmbedder{
		BaseEmbedder: NewBaseEmbedder(config.Dimensions),
		client:       openai.NewClientWithConfig(clientConfig),
		model:        model,
		baseURL:      strings.TrimSuffix(config.BaseURL, "/"),
	}, nil
}

// Embed 批量嵌入文本
func (e *OpenAIEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	// 调用 OpenAI 嵌入 API
	resp, err := e.client.CreateEmbeddings(ctx, openai.EmbeddingRequest{
		Input: texts,
		Model: e.model,
	})
	if err != nil {
		return nil, agentErrors.Wrap(err, agentErrors.CodeRetrievalEmbedding, "failed to create embeddings").
			WithComponent("openai_embedder").
			WithOperation("embed").
			WithContext("num_texts", len(texts))
	}

	if len(resp.Data) == 0 {
		return nil, agentErrors.New(agentErrors.CodeInternal, "no embeddings returned").
			WithComponent("openai_embedder").
			WithOperation("embed")
	}

	// 按索引排序结果（OpenAI API 可能返回乱序）
	results := make([][]float32, len(texts))
	for _, data := range resp.Data {
		if data.Index < len(results) {
			results[data.Index] = data.Embedding
		}
	}

	return results, nil
}

// EmbedQuery 嵌入单个查询文本
func (e *OpenAIEmbedder) EmbedQuery(ctx context.Context, query string) ([]float32, error) {
	vectors, err := e.Embed(ctx, []string{query})
	if err != nil {
		return nil, err
	}
	if len(vectors) == 0 {
		return nil, agentErrors.New(agentErrors.CodeInternal, "no embeddings returned").
			WithComponent("openai_embedder").
			WithOperation("embed_query")
	}
	return vectors[0], nil
}
