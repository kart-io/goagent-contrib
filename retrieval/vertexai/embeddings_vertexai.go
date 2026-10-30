package vertexai

import (
	"context"
	"fmt"
	"os"

	aiplatform "cloud.google.com/go/aiplatform/apiv1"
	"cloud.google.com/go/aiplatform/apiv1/aiplatformpb"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/kart-io/goagent-contrib/retrieval"
	agentErrors "github.com/kart-io/goagent/errors"
)

// VertexAIEmbedder 使用 Google Vertex AI 的嵌入模型
//
// 支持 text-embedding-005 等模型，提供高质量的语义嵌入
type VertexAIEmbedder struct {
	*retrieval.BaseEmbedder
	client    *aiplatform.PredictionClient
	projectID string
	location  string
	modelID   string
	endpoint  string
}

// VertexAIEmbedderConfig Vertex AI 嵌入器配置
type VertexAIEmbedderConfig struct {
	// ProjectID Google Cloud 项目 ID
	ProjectID string
	// Location 区域，如 us-central1
	Location string
	// ModelID 模型 ID，如 text-embedding-005
	ModelID string
	// Dimensions 向量维度（text-embedding-005 默认 768）
	Dimensions int
}

// NewVertexAIEmbedder 创建 Vertex AI 嵌入器
func NewVertexAIEmbedder(ctx context.Context, config VertexAIEmbedderConfig) (*VertexAIEmbedder, error) {
	// 设置默认值
	if config.ProjectID == "" {
		config.ProjectID = os.Getenv("GOOGLE_CLOUD_PROJECT")
		if config.ProjectID == "" {
			config.ProjectID = os.Getenv("GCLOUD_PROJECT")
		}
	}
	if config.ProjectID == "" {
		return nil, agentErrors.New(agentErrors.CodeInvalidConfig, "project ID is required").
			WithComponent("vertex_ai_embedder").
			WithOperation("create")
	}

	if config.Location == "" {
		config.Location = "us-central1"
	}

	if config.ModelID == "" {
		config.ModelID = "text-embedding-005"
	}

	if config.Dimensions <= 0 {
		config.Dimensions = 768
	}

	// 创建 Prediction 客户端
	endpoint := fmt.Sprintf("%s-aiplatform.googleapis.com:443", config.Location)
	client, err := aiplatform.NewPredictionClient(ctx,
		option.WithEndpoint(endpoint),
	)
	if err != nil {
		return nil, agentErrors.Wrap(err, agentErrors.CodeInternal, "failed to create Vertex AI client").
			WithComponent("vertex_ai_embedder").
			WithOperation("create")
	}

	// 构建模型端点
	modelEndpoint := fmt.Sprintf("projects/%s/locations/%s/publishers/google/models/%s",
		config.ProjectID, config.Location, config.ModelID)

	return &VertexAIEmbedder{
		BaseEmbedder: retrieval.NewBaseEmbedder(config.Dimensions),
		client:       client,
		projectID:    config.ProjectID,
		location:     config.Location,
		modelID:      config.ModelID,
		endpoint:     modelEndpoint,
	}, nil
}

// Embed 批量嵌入文本
func (e *VertexAIEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	// 构建请求实例
	instances := make([]*structpb.Value, len(texts))
	for i, text := range texts {
		instance, err := structpb.NewStruct(map[string]interface{}{
			"content":   text,
			"task_type": "RETRIEVAL_DOCUMENT",
		})
		if err != nil {
			return nil, agentErrors.Wrap(err, agentErrors.CodeInternal, "failed to create instance").
				WithComponent("vertex_ai_embedder").
				WithOperation("embed").
				WithContext("text_index", i)
		}
		instances[i] = structpb.NewStructValue(instance)
	}

	// 发送请求
	req := &aiplatformpb.PredictRequest{
		Endpoint:  e.endpoint,
		Instances: instances,
	}

	resp, err := e.client.Predict(ctx, req)
	if err != nil {
		return nil, agentErrors.Wrap(err, agentErrors.CodeRetrievalEmbedding, "failed to get embeddings from Vertex AI").
			WithComponent("vertex_ai_embedder").
			WithOperation("embed").
			WithContext("num_texts", len(texts))
	}

	// 解析响应
	vectors := make([][]float32, len(resp.Predictions))
	for i, pred := range resp.Predictions {
		// 提取嵌入向量
		predStruct := pred.GetStructValue()
		if predStruct == nil {
			return nil, agentErrors.New(agentErrors.CodeInternal, "invalid prediction response").
				WithComponent("vertex_ai_embedder").
				WithOperation("embed").
				WithContext("prediction_index", i)
		}

		embeddings := predStruct.Fields["embeddings"]
		if embeddings == nil {
			return nil, agentErrors.New(agentErrors.CodeInternal, "embeddings field not found").
				WithComponent("vertex_ai_embedder").
				WithOperation("embed").
				WithContext("prediction_index", i)
		}

		embeddingsStruct := embeddings.GetStructValue()
		if embeddingsStruct == nil {
			return nil, agentErrors.New(agentErrors.CodeInternal, "invalid embeddings structure").
				WithComponent("vertex_ai_embedder").
				WithOperation("embed").
				WithContext("prediction_index", i)
		}

		values := embeddingsStruct.Fields["values"]
		if values == nil {
			return nil, agentErrors.New(agentErrors.CodeInternal, "values field not found").
				WithComponent("vertex_ai_embedder").
				WithOperation("embed").
				WithContext("prediction_index", i)
		}

		listVal := values.GetListValue()
		if listVal == nil {
			return nil, agentErrors.New(agentErrors.CodeInternal, "invalid values list").
				WithComponent("vertex_ai_embedder").
				WithOperation("embed").
				WithContext("prediction_index", i)
		}

		vector := make([]float32, len(listVal.Values))
		for j, v := range listVal.Values {
			vector[j] = float32(v.GetNumberValue())
		}
		vectors[i] = vector
	}

	return vectors, nil
}

// EmbedQuery 嵌入单个查询文本
func (e *VertexAIEmbedder) EmbedQuery(ctx context.Context, query string) ([]float32, error) {
	// 构建请求实例（查询任务类型不同）
	instance, err := structpb.NewStruct(map[string]interface{}{
		"content":   query,
		"task_type": "RETRIEVAL_QUERY",
	})
	if err != nil {
		return nil, agentErrors.Wrap(err, agentErrors.CodeInternal, "failed to create instance").
			WithComponent("vertex_ai_embedder").
			WithOperation("embed_query")
	}

	// 发送请求
	req := &aiplatformpb.PredictRequest{
		Endpoint:  e.endpoint,
		Instances: []*structpb.Value{structpb.NewStructValue(instance)},
	}

	resp, err := e.client.Predict(ctx, req)
	if err != nil {
		return nil, agentErrors.Wrap(err, agentErrors.CodeRetrievalEmbedding, "failed to get query embedding from Vertex AI").
			WithComponent("vertex_ai_embedder").
			WithOperation("embed_query").
			WithContext("query", truncateString(query, 100))
	}

	if len(resp.Predictions) == 0 {
		return nil, agentErrors.New(agentErrors.CodeInternal, "no predictions returned").
			WithComponent("vertex_ai_embedder").
			WithOperation("embed_query")
	}

	// 解析响应
	predStruct := resp.Predictions[0].GetStructValue()
	if predStruct == nil {
		return nil, agentErrors.New(agentErrors.CodeInternal, "invalid prediction response").
			WithComponent("vertex_ai_embedder").
			WithOperation("embed_query")
	}

	embeddings := predStruct.Fields["embeddings"]
	if embeddings == nil {
		return nil, agentErrors.New(agentErrors.CodeInternal, "embeddings field not found").
			WithComponent("vertex_ai_embedder").
			WithOperation("embed_query")
	}

	embeddingsStruct := embeddings.GetStructValue()
	if embeddingsStruct == nil {
		return nil, agentErrors.New(agentErrors.CodeInternal, "invalid embeddings structure").
			WithComponent("vertex_ai_embedder").
			WithOperation("embed_query")
	}

	values := embeddingsStruct.Fields["values"]
	if values == nil {
		return nil, agentErrors.New(agentErrors.CodeInternal, "values field not found").
			WithComponent("vertex_ai_embedder").
			WithOperation("embed_query")
	}

	listVal := values.GetListValue()
	if listVal == nil {
		return nil, agentErrors.New(agentErrors.CodeInternal, "invalid values list").
			WithComponent("vertex_ai_embedder").
			WithOperation("embed_query")
	}

	vector := make([]float32, len(listVal.Values))
	for j, v := range listVal.Values {
		vector[j] = float32(v.GetNumberValue())
	}

	return vector, nil
}

// Close 关闭客户端
func (e *VertexAIEmbedder) Close() error {
	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

// truncateString 截断字符串（用于日志）
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
