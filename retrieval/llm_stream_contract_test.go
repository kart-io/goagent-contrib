package retrieval

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/kart-io/goagent/interfaces"
	"github.com/kart-io/goagent/llm"
	"github.com/stretchr/testify/require"
)

type retrievalStreamModel struct {
	contractModel
	stream func(context.Context, *llm.CompletionRequest) (<-chan *llm.StreamChunk, error)
}

func (m *retrievalStreamModel) CompleteStream(ctx context.Context, req *llm.CompletionRequest) (<-chan *llm.StreamChunk, error) {
	return m.stream(ctx, req)
}

type retrievalModelOperation struct {
	name         string
	content      string
	checkRequest func(*testing.T, *llm.CompletionRequest)
	run          func(context.Context, llm.Client) (string, error)
	want         string
}

// 这些操作必须消费提供方的完整输出并保留请求选项；
// 提供方流失败时，必须在解析或使用不完整文本之前返回错误。
func retrievalModelOperations() []retrievalModelOperation {
	return []retrievalModelOperation{
		{
			name: "reranker", content: `{"scores":[{"index":0,"score":0.8}]}`, want: "refund",
			checkRequest: func(t *testing.T, req *llm.CompletionRequest) {
				require.Len(t, req.Messages, 2)
				require.Equal(t, "system", req.Messages[0].Role)
				require.NotEmpty(t, req.Messages[0].Content)
				require.Equal(t, "user", req.Messages[1].Role)
				var payload struct {
					Query      string
					Candidates []string
				}
				require.NoError(t, json.Unmarshal([]byte(req.Messages[1].Content), &payload))
				require.Equal(t, "退款", payload.Query)
				require.Equal(t, []string{"退款政策"}, payload.Candidates)
			},
			run: func(ctx context.Context, client llm.Client) (string, error) {
				docs, err := NewLLMRerankerWithClient(client, 1).Rerank(ctx, "退款", []*interfaces.Document{{ID: "refund", PageContent: "退款政策"}})
				if err != nil {
					return "", err
				}
				if len(docs) != 1 || docs[0].Score != 0.8 {
					return "", errors.New("missing model score")
				}
				return docs[0].ID, nil
			},
		},
		{
			name: "multi_query", content: "1. returns\n2. policy", want: "refund|returns|policy",
			checkRequest: func(t *testing.T, req *llm.CompletionRequest) {
				require.Len(t, req.Messages, 1)
				require.Equal(t, "user", req.Messages[0].Role)
				require.Contains(t, req.Messages[0].Content, "Original question: refund")
				require.Contains(t, req.Messages[0].Content, "generate 2 different versions")
				require.Equal(t, float64(0.7), req.Temperature)
				require.Equal(t, 500, req.MaxTokens)
			},
			run: func(ctx context.Context, client llm.Client) (string, error) {
				queries, err := NewMultiQueryRetriever(nil, client, 2, RetrieverConfig{}).generateQueries(ctx, "refund")
				return strings.Join(queries, "|"), err
			},
		},
		{
			name: "rag_multi_query", content: "1. returns\n2. policy\n3. extra", want: "refund|returns|policy",
			checkRequest: func(t *testing.T, req *llm.CompletionRequest) {
				require.Len(t, req.Messages, 1)
				require.Equal(t, "user", req.Messages[0].Role)
				require.Contains(t, req.Messages[0].Content, "generate 2 different variations")
				require.Contains(t, req.Messages[0].Content, "Original query: refund")
				require.Equal(t, float64(0.7), req.Temperature)
			},
			run: func(ctx context.Context, client llm.Client) (string, error) {
				queries, err := NewRAGMultiQueryRetriever(nil, 2, client).generateQueries(ctx, "refund")
				return strings.Join(queries, "|"), err
			},
		},
		{
			name: "rag_chain", content: "refund accepted", want: "refund accepted",
			checkRequest: func(t *testing.T, req *llm.CompletionRequest) {
				require.Len(t, req.Messages, 1)
				require.Equal(t, "user", req.Messages[0].Role)
				require.Contains(t, req.Messages[0].Content, "refund policy")
				require.Contains(t, req.Messages[0].Content, "Question: refund")
			},
			run: func(ctx context.Context, client llm.Client) (string, error) {
				r, err := NewRAGRetriever(RAGRetrieverConfig{VectorStore: &contractVector{}})
				if err != nil {
					return "", err
				}
				return NewRAGChain(r, client).Run(ctx, "refund")
			},
		},
	}
}

func TestRetrievalModelOperationsCollectCompletedStream(t *testing.T) {
	for _, op := range retrievalModelOperations() {
		t.Run(op.name, func(t *testing.T) {
			calls := 0
			model := &retrievalStreamModel{stream: func(_ context.Context, req *llm.CompletionRequest) (<-chan *llm.StreamChunk, error) {
				calls++
				op.checkRequest(t, req)
				out := make(chan *llm.StreamChunk, 3)
				mid := len(op.content) / 2
				out <- &llm.StreamChunk{Delta: op.content[:mid]}
				out <- &llm.StreamChunk{Delta: op.content[mid:]}
				out <- &llm.StreamChunk{Done: true, FinishReason: "stop"}
				close(out)
				return out, nil
			}}
			got, err := op.run(context.Background(), model)
			require.NoError(t, err)
			require.Equal(t, op.want, got)
			require.Equal(t, 1, calls)
		})
	}
}

func TestRetrievalModelOperationsRejectFailedStreams(t *testing.T) {
	sentinel := errors.New("provider unavailable")
	for _, op := range retrievalModelOperations() {
		for _, failure := range []string{"request", "chunk", "truncated", "cancel"} {
			t.Run(op.name+"/"+failure, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				want := sentinel
				if failure == "truncated" {
					want = io.ErrUnexpectedEOF
				}
				if failure == "cancel" {
					want = context.Canceled
				}
				model := &retrievalStreamModel{stream: func(_ context.Context, _ *llm.CompletionRequest) (<-chan *llm.StreamChunk, error) {
					if failure == "request" {
						return nil, sentinel
					}
					out := make(chan *llm.StreamChunk, 2)
					// 即使输出在语法上完整，缺少成功终止块也不能使用。
					out <- &llm.StreamChunk{Delta: op.content}
					switch failure {
					case "chunk":
						out <- &llm.StreamChunk{Done: true, Error: sentinel}
					case "cancel":
						cancel()
						return out, nil
					}
					close(out)
					return out, nil
				}}
				got, err := op.run(ctx, model)
				require.ErrorIs(t, err, want)
				require.Empty(t, got, "partial output must not become a retrieval or answer result")
			})
		}
	}
}
