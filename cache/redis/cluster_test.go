package redis

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// clusterClient 在 GOAGENT_REDIS_CLUSTER=1 时返回 Cluster 客户端，否则跳过。
func clusterClient(t *testing.T) *goredis.ClusterClient {
	t.Helper()
	if os.Getenv(clusterEnv) != "1" {
		t.Skipf("未设置 %s=1，跳过 Cluster 测试", clusterEnv)
	}
	cluster, ok := testClient(t).(*goredis.ClusterClient)
	require.Truef(t, ok, "%s=1 但 %s 没有得到 Cluster 客户端", clusterEnv, addrEnv)
	return cluster
}

// Cluster 模式下 test-redis.sh --cluster 让整个包（含 TestConformance 的 cachetest.RunTagged 与 TestDeleteDuringWrite）
// 都跑在 Cluster 上；这里补充 Cluster 特有的断言：跨槽的 1000 个条目分布到全部主节点，DeleteTag 后各主节点上该前缀无残留。
func TestClusterConformance(t *testing.T) {
	cluster := clusterClient(t)
	prefix := newPrefix(t)
	c, err := New(opCtx(t), Config{Client: cluster, Prefix: prefix})
	require.NoError(t, err)

	const n = 1000
	for i := range n {
		require.NoError(t, c.SetTagged(opCtx(t), fmt.Sprintf("k%d", i), []byte("v"), longTTL, "s"))
	}
	var mu sync.Mutex
	perMaster := map[string]int{}
	require.NoError(t, cluster.ForEachMaster(opCtx(t), func(ctx context.Context, node *goredis.Client) error {
		var count int
		iter := node.Scan(ctx, 0, prefix+"e:*", 1000).Iterator()
		for iter.Next(ctx) {
			count++
		}
		mu.Lock()
		perMaster[node.Options().Addr] = count
		mu.Unlock()
		return iter.Err()
	}))
	require.Greater(t, len(perMaster), 1)
	var total int
	for addr, count := range perMaster {
		require.Positivef(t, count, "主节点 %s 上没有条目", addr)
		total += count
	}
	require.Equal(t, n, total)

	require.NoError(t, c.DeleteTag(opCtx(t), "s"))
	keys := scanKeys(t, cluster, prefix+"*")
	require.Empty(t, keys, strings.Join(keys, ","))
	requireMiss(t, c, "k0")
}

// WaitReplicas=1：从节点正常时 DeleteTag 成功；令牌所在主节点的从节点被 CLIENT PAUSE WRITE 暂停（不处理复制流）时，
// DeleteTag 返回错误，但令牌已删除，条目不可读。
func TestClusterWaitPausedReplica(t *testing.T) {
	cluster := clusterClient(t)
	c, err := New(opCtx(t), Config{Client: cluster, Prefix: newPrefix(t), WaitReplicas: 1})
	require.NoError(t, err)

	require.NoError(t, c.SetTagged(opCtx(t), "k", []byte("v"), longTTL, "s"))
	require.NoError(t, c.DeleteTag(opCtx(t), "s"))
	requireMiss(t, c, "k")

	require.NoError(t, c.SetTagged(opCtx(t), "k", []byte("v"), longTTL, "s"))
	master, err := cluster.MasterForKey(opCtx(t), c.tokenKey("s"))
	require.NoError(t, err)
	replicas := replicaAddrs(t, master)
	require.NotEmpty(t, replicas)
	for _, addr := range replicas {
		replica := goredis.NewClient(&goredis.Options{Addr: addr})
		t.Cleanup(func() {
			_ = replica.Do(context.Background(), "client", "unpause").Err()
			_ = replica.Close()
		})
		require.NoError(t, replica.Do(opCtx(t), "client", "pause", 2000, "write").Err())
	}

	err = c.DeleteTag(opCtx(t), "s")
	require.ErrorContains(t, err, "只得到 0 个从节点确认")
	requireMiss(t, c, "k")
}

// replicaAddrs 从主节点的 ROLE 回复中取出各从节点地址。
func replicaAddrs(t *testing.T, master *goredis.Client) []string {
	t.Helper()
	role, err := master.Do(opCtx(t), "role").Slice()
	require.NoError(t, err)
	require.Equal(t, "master", role[0])
	var addrs []string
	for _, r := range role[2].([]any) {
		fields := r.([]any)
		addrs = append(addrs, net.JoinHostPort(fields[0].(string), fields[1].(string)))
	}
	return addrs
}
