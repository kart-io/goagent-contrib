package cohere

import (
	"context"

	coheregov2 "github.com/cohere-ai/cohere-go/v2"
	cohereclient "github.com/cohere-ai/cohere-go/v2/client"

	"github.com/kart-io/goagent-contrib/retrieval"
	agentErrors "github.com/kart-io/goagent/errors"
	"github.com/kart-io/goagent/interfaces"
)

// CohereReranker Cohere Rerank API 重排序器
type CohereReranker struct {
	*retrieval.BaseReranker

	// APIKey Cohere API 密钥
	APIKey string

	// Model 模型名称
	Model string

	// TopN 返回前 N 个文档
	TopN int

	// client Cohere 客户端
	client *cohereclient.Client
}

// NewCohereReranker 创建 Cohere 重排序器
//
// 参数:
//   - apiKey: Cohere API 密钥
//   - model: 模型名称（可选，默认为 "rerank-english-v2.0"）
//   - topN: 返回前 N 个文档
//
// 返回:
//   - *CohereReranker: Cohere 重排序器实例
//   - error: 错误信息
func NewCohereReranker(apiKey, model string, topN int) (*CohereReranker, error) {
	if apiKey == "" {
		return nil, agentErrors.New(agentErrors.CodeInvalidConfig, "Cohere API key is required").
			WithComponent("cohere_reranker").
			WithOperation("create")
	}

	if model == "" {
		model = "rerank-english-v2.0"
	}

	client := cohereclient.NewClient(cohereclient.WithToken(apiKey))

	return &CohereReranker{
		BaseReranker: retrieval.NewBaseReranker("cohere"),
		APIKey:       apiKey,
		Model:        model,
		TopN:         topN,
		client:       client,
	}, nil
}

// Rerank 使用 Cohere API 重新排序
//
// 参数:
//   - ctx: 上下文
//   - query: 查询字符串
//   - docs: 待重排序的文档列表
//
// 返回:
//   - []*interfaces.Document: 重排序后的文档列表
//   - error: 错误信息
func (c *CohereReranker) Rerank(ctx context.Context, query string, docs []*interfaces.Document) ([]*interfaces.Document, error) {
	if len(docs) == 0 {
		return docs, nil
	}

	// 提取文档内容并转换为 RerankRequestDocumentsItem
	documentItems := make([]*coheregov2.RerankRequestDocumentsItem, len(docs))
	for i, doc := range docs {
		documentItems[i] = &coheregov2.RerankRequestDocumentsItem{
			String: doc.PageContent,
		}
	}

	// 调用 Cohere Rerank API
	topN := c.TopN
	if topN <= 0 {
		topN = len(docs)
	}

	response, err := c.client.Rerank(ctx, &coheregov2.RerankRequest{
		Query:     query,
		Documents: documentItems,
		Model:     &c.Model,
		TopN:      &topN,
	})
	if err != nil {
		return nil, agentErrors.Wrap(err, agentErrors.CodeInternal, "Cohere rerank API call failed").
			WithComponent("cohere_reranker").
			WithOperation("rerank").
			WithContext("query", query).
			WithContext("num_docs", len(docs))
	}

	// 转换结果
	if response == nil || response.Results == nil {
		return docs, nil
	}

	rerankedDocs := make([]*interfaces.Document, 0, len(response.Results))
	for _, result := range response.Results {
		if result.Index < len(docs) {
			doc := docs[result.Index].Clone()
			doc.Score = result.RelevanceScore
			rerankedDocs = append(rerankedDocs, doc)
		}
	}

	return rerankedDocs, nil
}
