package redis

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/kart-io/goagent/cache/cachetest"
	agentErrors "github.com/kart-io/goagent/errors"
)

// 需要一个可写的 Redis：GOAGENT_REDIS_ADDR=127.0.0.1:56379（Cluster 为逗号分隔地址）。
// GOAGENT_REDIS_REPLICA_ADDR 是某个从节点的地址；GOAGENT_REDIS_CLUSTER=1 表示 GOAGENT_REDIS_ADDR 指向 Cluster。
const (
	addrEnv        = "GOAGENT_REDIS_ADDR"
	replicaAddrEnv = "GOAGENT_REDIS_REPLICA_ADDR"
	clusterEnv     = "GOAGENT_REDIS_CLUSTER"
)

func testClient(t *testing.T) goredis.UniversalClient {
	t.Helper()
	addr := os.Getenv(addrEnv)
	if addr == "" {
		t.Skipf("未设置 %s，跳过 Redis 测试", addrEnv)
	}
	c := goredis.NewUniversalClient(&goredis.UniversalOptions{Addrs: strings.Split(addr, ",")})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// newPrefix 返回一个随机前缀，测试结束时删除该前缀下的全部键。
func newPrefix(t *testing.T) string {
	t.Helper()
	prefix := "goagent:test:" + strings.ToLower(crand.Text()[:12]) + ":"
	client := testClient(t)
	t.Cleanup(func() { deletePrefix(client, prefix) })
	return prefix
}

func deletePrefix(client goredis.UniversalClient, prefix string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = forEachMaster(ctx, client, func(ctx context.Context, node *goredis.Client) error {
		iter := node.Scan(ctx, 0, prefix+"*", 1000).Iterator()
		for iter.Next(ctx) {
			node.Del(ctx, iter.Val())
		}
		return iter.Err()
	})
}

// forEachMaster 对每个主节点执行 fn：Cluster 下 DBSIZE、KEYS、SCAN、CONFIG 只作用于单个节点，凡需要全局视图的检查都经过它。
func forEachMaster(ctx context.Context, client goredis.UniversalClient, fn func(context.Context, *goredis.Client) error) error {
	if cluster, ok := client.(*goredis.ClusterClient); ok {
		return cluster.ForEachMaster(ctx, fn)
	}
	return fn(ctx, client.(*goredis.Client))
}

// scanKeys 在全部主节点上 SCAN 匹配 pattern 的键。
func scanKeys(t *testing.T, client goredis.UniversalClient, pattern string) []string {
	t.Helper()
	var mu sync.Mutex
	var keys []string
	err := forEachMaster(opCtx(t), client, func(ctx context.Context, node *goredis.Client) error {
		iter := node.Scan(ctx, 0, pattern, 1000).Iterator()
		for iter.Next(ctx) {
			mu.Lock()
			keys = append(keys, iter.Val())
			mu.Unlock()
		}
		return iter.Err()
	})
	require.NoError(t, err)
	return keys
}

// openCache 用独立客户端打开一个实例，模拟一个进程。
func openCache(t *testing.T, prefix string) *Cache {
	t.Helper()
	c, err := New(context.Background(), Config{Client: testClient(t), Prefix: prefix})
	require.NoError(t, err)
	return c
}

func TestConformance(t *testing.T) {
	var mu sync.Mutex
	prefixes := map[*testing.T]map[string]string{}
	// prefixFor 在锁内可能 Skip（Goexit），须用 defer 解锁，否则后续子测试死锁。
	prefixFor := func(t *testing.T, name string) string {
		mu.Lock()
		defer mu.Unlock()
		pool, ok := prefixes[t]
		if !ok {
			pool = map[string]string{}
			prefixes[t] = pool
			t.Cleanup(func() {
				mu.Lock()
				defer mu.Unlock()
				delete(prefixes, t)
			})
		}
		prefix, ok := pool[name]
		if !ok {
			prefix = newPrefix(t)
			pool[name] = prefix
		}
		return prefix
	}
	cachetest.RunTagged(t, func(t *testing.T, name string) cachetest.Cache {
		return openCache(t, prefixFor(t, name))
	})
}

// 会读到从节点上滞后令牌的客户端配置一律拒绝。连接从节点的用例需要 GOAGENT_REDIS_REPLICA_ADDR（test-redis.sh 会提供）。
func TestNewRejects(t *testing.T) {
	const unreachable = "127.0.0.1:1"
	cluster := func(opt goredis.ClusterOptions) func(t *testing.T) goredis.UniversalClient {
		return func(t *testing.T) goredis.UniversalClient {
			opt.Addrs = []string{unreachable}
			return goredis.NewClusterClient(&opt)
		}
	}
	for _, tc := range []struct {
		name   string
		client func(t *testing.T) goredis.UniversalClient
	}{
		{"Ring", func(t *testing.T) goredis.UniversalClient {
			return goredis.NewRing(&goredis.RingOptions{Addrs: map[string]string{"a": unreachable}})
		}},
		{"Cluster ReadOnly", cluster(goredis.ClusterOptions{ReadOnly: true})},
		{"Cluster RouteByLatency", cluster(goredis.ClusterOptions{RouteByLatency: true})},
		{"Cluster RouteRandomly", cluster(goredis.ClusterOptions{RouteRandomly: true})},
		{"哨兵 ReplicaOnly", func(t *testing.T) goredis.UniversalClient {
			return goredis.NewFailoverClusterClient(&goredis.FailoverOptions{
				MasterName: "m", SentinelAddrs: []string{unreachable}, ReplicaOnly: true,
			})
		}},
		{"连接从节点", func(t *testing.T) goredis.UniversalClient {
			addr := os.Getenv(replicaAddrEnv)
			if addr == "" {
				t.Skipf("未设置 %s，跳过", replicaAddrEnv)
			}
			return goredis.NewClient(&goredis.Options{Addr: addr})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := tc.client(t)
			t.Cleanup(func() { _ = client.Close() })
			_, err := New(opCtx(t), Config{Client: client})
			require.Truef(t, agentErrors.IsCode(err, agentErrors.CodeInvalidInput), "期望 CodeInvalidInput，得到 %v", err)
		})
	}
}

// 前缀含花括号与淘汰策略不是 noeviction 只告警、不拒绝；默认配置不告警。
func TestNewWarnings(t *testing.T) {
	for _, tc := range []struct {
		name   string
		prefix string
		policy string
		want   string
	}{
		{"无告警", "", "noeviction", ""},
		{"前缀含花括号", "{app}:", "noeviction", "花括号"},
		{"淘汰策略", "", "allkeys-lru", "noeviction"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := testClient(t)
			policyAddr := setPolicy(t, client, tc.policy)
			var buf bytes.Buffer
			old := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
			t.Cleanup(func() { slog.SetDefault(old) })

			prefix := tc.prefix
			if prefix == "" {
				prefix = newPrefix(t)
			}
			_, err := New(opCtx(t), Config{Client: client, Prefix: prefix})
			require.NoError(t, err)
			if tc.want == "" {
				require.Empty(t, buf.String())
				return
			}
			require.Contains(t, buf.String(), "level=WARN")
			require.Contains(t, buf.String(), tc.want)
			if tc.policy != "noeviction" {
				require.Contains(t, buf.String(), "addr="+policyAddr)
			}
		})
	}
}

// setPolicy 只在地址最小的一个主节点上设置 maxmemory-policy 并返回其地址，测试结束时恢复原值。
// Cluster 下只改一个主节点，用来发现只检查随机一个节点的实现。
func setPolicy(t *testing.T, client goredis.UniversalClient, policy string) string {
	t.Helper()
	var mu sync.Mutex
	var target *goredis.Client
	require.NoError(t, forEachMaster(opCtx(t), client, func(_ context.Context, node *goredis.Client) error {
		mu.Lock()
		defer mu.Unlock()
		if target == nil || node.Options().Addr < target.Options().Addr {
			target = node
		}
		return nil
	}))
	cur, err := target.ConfigGet(opCtx(t), "maxmemory-policy").Result()
	require.NoError(t, err)
	require.NoError(t, target.ConfigSet(opCtx(t), "maxmemory-policy", policy).Err())
	t.Cleanup(func() { _ = target.ConfigSet(context.Background(), "maxmemory-policy", cur["maxmemory-policy"]).Err() })
	return target.Options().Addr
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{"缺少客户端", Config{}},
		{"负的批大小", Config{Client: client, DeleteBatch: -1}},
		{"负的从节点数", Config{Client: client, WaitReplicas: -1}},
		{"负的等待时长", Config{Client: client, WaitTimeout: -time.Second}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(context.Background(), tc.cfg)
			require.Truef(t, agentErrors.IsCode(err, agentErrors.CodeInvalidInput), "期望 CodeInvalidInput，得到 %v", err)
		})
	}
}

func TestParseRedisMajor(t *testing.T) {
	for _, tc := range []struct {
		name    string
		info    string
		want    int
		wantErr bool
	}{
		{"7.4", "# Server\r\nredis_version:7.4.2\r\nredis_mode:standalone\r\n", 7, false},
		{"6.2", "# Server\r\nredis_version:6.2.14\r\n", 6, false},
		{"10.0", "redis_version:10.0.0\n", 10, false},
		{"缺少版本", "# Server\r\nredis_mode:standalone\r\n", 0, true},
		{"非数字", "redis_version:x.y\r\n", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseRedisMajor(tc.info)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestEntryRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name   string
		tags   []string
		tokens []string
		value  []byte
	}{
		{"无标签", nil, nil, []byte("v")},
		{"空值", []string{"s"}, []string{"0123456789abcdef"}, []byte{}},
		{"多标签", []string{"s", "{x}", ""}, []string{"t1", "t2\x00", "t3"}, []byte("值\x00")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, err := decodeEntry(encodeEntry(tc.tags, tc.tokens, tc.value))
			require.NoError(t, err)
			require.Equal(t, len(tc.tags), len(e.tags))
			for i := range tc.tags {
				require.Equal(t, tc.tags[i], e.tags[i])
				require.Equal(t, tc.tokens[i], e.tokens[i])
			}
			require.Equal(t, string(tc.value), string(e.value))
		})
	}
}

func TestDecodeEntryRejects(t *testing.T) {
	valid := encodeEntry([]string{"s"}, []string{"token"}, []byte("v"))
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"空", nil},
		{"未知版本", append([]byte{99}, valid[1:]...)},
		{"缺少标签数", valid[:3]},
		{"标签截断", valid[:7]},
		{"令牌截断", valid[:len(valid)-3]},
		{"标签数过大", []byte{entryVersion, 0xff, 0xff, 0xff, 0xff}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeEntry(tc.raw)
			require.Error(t, err)
		})
	}
}

func TestEscape(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"abc", "abc"},
		{"a{b}c", "a%7Bb%7Dc"},
		{"%7B", "%257B"},
		{"%", "%25"},
	} {
		require.Equal(t, tc.want, escape(tc.in), tc.in)
	}
}
