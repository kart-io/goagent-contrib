package qdrant

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"

	qc "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"

	agentErrors "github.com/kart-io/goagent/errors"
	"github.com/kart-io/goagent/memory"
	"github.com/kart-io/goagent/memory/memorytest"
)

// 需要 Qdrant 的 gRPC 地址，例如 GOAGENT_QDRANT_ADDR=127.0.0.1:56344
const addrEnv = "GOAGENT_QDRANT_ADDR"

func newClient(t *testing.T) *qc.Client {
	t.Helper()
	addr := os.Getenv(addrEnv)
	if addr == "" {
		t.Skipf("未设置 %s，跳过 Qdrant 测试", addrEnv)
	}
	host, portStr, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)
	client, err := qc.NewClient(&qc.Config{Host: host, Port: port, SkipCompatibilityCheck: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// uniquePrefix 返回随机前缀，并在测试结束时删除该前缀下的全部集合。
func uniquePrefix(t *testing.T, client *qc.Client) string {
	t.Helper()
	b := make([]byte, 6)
	_, _ = crand.Read(b)
	prefix := "t" + hex.EncodeToString(b)
	t.Cleanup(func() {
		ctx := context.Background()
		names, err := client.ListCollections(ctx)
		if err != nil {
			t.Logf("列出集合失败：%v", err)
			return
		}
		for _, name := range names {
			if strings.HasPrefix(name, prefix+"_") {
				if err := client.DeleteCollection(ctx, name); err != nil {
					t.Logf("删除集合 %s 失败：%v", name, err)
				}
			}
		}
	})
	return prefix
}

func TestIndexContract(t *testing.T) {
	client := newClient(t)
	memorytest.RunIndex(t, func(t *testing.T) memory.Index {
		x, err := New(context.Background(), Config{Client: client, Prefix: uniquePrefix(t, client)})
		require.NoError(t, err)
		return x
	})
}

func TestNewRequiresClient(t *testing.T) {
	_, err := New(context.Background(), Config{})
	require.Error(t, err)
	require.True(t, agentErrors.IsCode(err, agentErrors.CodeInvalidInput), "%v", err)
}

// 第 k 名之后的并列多于一页时，阈值重查会扩大 limit 直到取全，结果仍按 ID 升序截断。
func TestTiesBeyondLimit(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	x, err := New(ctx, Config{Client: client, Prefix: uniquePrefix(t, client)})
	require.NoError(t, err)
	ns := []string{"t", "alice"}
	for i := 9; i >= 0; i-- {
		require.NoError(t, x.Upsert(ctx, ns, "m"+strconv.Itoa(i), []float32{1, 0}))
	}
	require.NoError(t, x.Upsert(ctx, ns, "far", []float32{0, 1}))
	for _, tt := range []struct {
		k    int
		want []string
	}{
		{2, []string{"m0", "m1"}},
		{10, []string{"m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9"}},
		{11, []string{"m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9", "far"}},
	} {
		t.Run(strconv.Itoa(tt.k), func(t *testing.T) {
			hits, err := x.Search(ctx, ns, []float32{1, 0}, tt.k)
			require.NoError(t, err)
			got := make([]string, len(hits))
			for i, h := range hits {
				got[i] = h.ID
			}
			require.Equal(t, tt.want, got)
		})
	}
}
