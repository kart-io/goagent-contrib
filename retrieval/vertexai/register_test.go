package vertexai

import (
	"testing"

	"github.com/kart-io/goagent-contrib/retrieval"
)

func TestImportRegistersProvider(t *testing.T) {
	if !retrieval.IsProviderSupported(retrieval.EmbedderProviderVertexAI) {
		t.Fatalf("导入 vertexai 子包后应注册 %s", retrieval.EmbedderProviderVertexAI)
	}
}
