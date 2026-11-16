package redis

import (
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// 单机没有从节点：WaitReplicas=1 时 DeleteTag 返回带确认数的错误，但令牌已删除，条目已不可读（S1 不依赖 WAIT）。
func TestWaitReplicasNoReplica(t *testing.T) {
	client := testClient(t)
	if _, ok := client.(*goredis.ClusterClient); ok {
		t.Skip("Cluster 的主节点都有从节点，见 TestClusterWaitPausedReplica")
	}
	c, err := New(opCtx(t), Config{Client: client, Prefix: newPrefix(t), WaitReplicas: 1, WaitTimeout: 100 * time.Millisecond})
	require.NoError(t, err)
	require.NoError(t, c.SetTagged(opCtx(t), "k", []byte("v"), longTTL, "s"))
	requireValue(t, c, "k", "v")

	err = c.DeleteTag(opCtx(t), "s")
	require.ErrorContains(t, err, "只得到 0 个从节点确认")
	n, err := c.cfg.Client.Exists(opCtx(t), c.tokenKey("s")).Result()
	require.NoError(t, err)
	require.Zero(t, n)
	requireMiss(t, c, "k")
}
