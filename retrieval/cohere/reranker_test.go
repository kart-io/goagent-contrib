package cohere

import (
	"context"
	"testing"

	"github.com/kart-io/goagent/interfaces"
)

// TestCohereRerankerConfiguration tests Cohere reranker configuration
func TestCohereRerankerConfiguration(t *testing.T) {
	apiKey := "test-api-key"
	model := "rerank-model"
	topN := 3

	reranker, err := NewCohereReranker(apiKey, model, topN)
	if err != nil {
		t.Fatalf("Failed to create Cohere reranker: %v", err)
	}

	if reranker.APIKey != apiKey {
		t.Errorf("Expected APIKey %s, got %s", apiKey, reranker.APIKey)
	}

	if reranker.Model != model {
		t.Errorf("Expected Model %s, got %s", model, reranker.Model)
	}

	if reranker.TopN != topN {
		t.Errorf("Expected TopN %d, got %d", topN, reranker.TopN)
	}
}

// TestCohereRerankerEmptyDocs tests Cohere reranker with empty docs
func TestCohereRerankerEmptyDocs(t *testing.T) {
	ctx := context.Background()
	reranker, err := NewCohereReranker("key", "model", 2)
	if err != nil {
		t.Fatalf("Failed to create Cohere reranker: %v", err)
	}

	results, err := reranker.Rerank(ctx, "test", []*interfaces.Document{})
	if err != nil {
		t.Fatalf("Reranking empty docs failed: %v", err)
	}

	if len(results) != 0 {
		t.Error("Expected empty results for empty input")
	}
}
