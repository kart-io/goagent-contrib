package cohere

import (
	"testing"

	"github.com/kart-io/goagent-contrib/retrieval"
)

func TestImportRegistersProvider(t *testing.T) {
	if !retrieval.IsProviderSupported(retrieval.EmbedderProviderCohere) {
		t.Fatalf("导入 cohere 子包后应注册 %s", retrieval.EmbedderProviderCohere)
	}
}
