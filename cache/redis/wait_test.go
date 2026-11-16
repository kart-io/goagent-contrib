package redis

import (
	"context"
	"os"
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

// 单机主从对：从节点暂停时第一次 DeleteTag 报错，另一个实例（新连接）重试也必须报错。
// 令牌已被删除时重试的删除若不产生复制流，新连接的复制偏移为 0，WAIT 会立即按已确认返回。
func TestWaitPausedReplicaRetry(t *testing.T) {
	addr, replica := os.Getenv(replicatedAddrEnv), os.Getenv(replicaAddrEnv)
	if addr == "" || replica == "" {
		t.Skipf("未设置 %s 与 %s，跳过", replicatedAddrEnv, replicaAddrEnv)
	}
	prefix := randomPrefix()
	open := func() *Cache {
		client := goredis.NewClient(&goredis.Options{Addr: addr})
		t.Cleanup(func() {
			deletePrefix(client, prefix)
			_ = client.Close()
		})
		c, err := New(opCtx(t), Config{Client: client, Prefix: prefix, WaitReplicas: 1, WaitTimeout: 200 * time.Millisecond})
		require.NoError(t, err)
		return c
	}
	c := open()
	require.NoError(t, c.SetTagged(opCtx(t), "k", []byte("v"), longTTL, "s"))
	require.NoError(t, c.DeleteTag(opCtx(t), "s"))

	require.NoError(t, c.SetTagged(opCtx(t), "k", []byte("v"), longTTL, "s"))
	pauseReplicas(t, []string{replica})
	requireRetryNotAcked(t, c, open)
}

// pauseReplicas 用 CLIENT PAUSE WRITE 暂停从节点处理复制流，测试结束时恢复。
func pauseReplicas(t *testing.T, addrs []string) {
	t.Helper()
	for _, addr := range addrs {
		replica := goredis.NewClient(&goredis.Options{Addr: addr})
		t.Cleanup(func() {
			_ = replica.Do(context.Background(), "client", "unpause").Err()
			_ = replica.Close()
		})
		require.NoError(t, replica.Do(opCtx(t), "client", "pause", 10000, "write").Err())
	}
}

// requireRetryNotAcked 断言从节点暂停期间 first 的 DeleteTag 报错，open 新建的实例重试同一标签也报错，条目始终不可读。
func requireRetryNotAcked(t *testing.T, first *Cache, open func() *Cache) {
	t.Helper()
	require.ErrorContains(t, first.DeleteTag(opCtx(t), "s"), "只得到 0 个从节点确认")
	requireMiss(t, first, "k")
	second := open()
	require.ErrorContains(t, second.DeleteTag(opCtx(t), "s"), "只得到 0 个从节点确认")
	requireMiss(t, second, "k")
}
