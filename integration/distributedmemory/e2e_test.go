// Package distributedmemory 用真实的 Redis 与 Postgres（pgvector）验证多实例共享长期记忆的端到端行为。
// 本模块只含测试、不发布，使 cache/redis 的使用者不必引入 pgx 与 pgstore。
package distributedmemory

import (
	"context"
	crand "crypto/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/kart-io/goagent/builder"
	"github.com/kart-io/goagent/core/session"
	"github.com/kart-io/goagent/memory"
	"github.com/kart-io/goagent/testing/mocks"
	"github.com/kart-io/goagent/tools"

	"github.com/kart-io/goagent-contrib/cache/redis"
	"github.com/kart-io/goagent-contrib/durable/pgstore"
	"github.com/kart-io/goagent-contrib/memory/pgvector"
)

// 两个变量都设置时才运行，由 scripts/test-distributed.sh 提供：
// GOAGENT_REDIS_ADDR 为 Redis 地址（Cluster 为逗号分隔地址）；
// GOAGENT_PGVECTOR_DSN 指向已执行 CREATE EXTENSION vector 的库，pgstore 与 pgvector 的表都建在其中。
const (
	redisAddrEnv = "GOAGENT_REDIS_ADDR"
	pgDSNEnv     = "GOAGENT_PGVECTOR_DSN"
)

// tagKey 是所有实例共用的标签密钥：不同则同一记忆在各实例上的标签不同，删不到其他实例写的条目。
var tagKey = []byte(strings.Repeat("k", 32))

// backend 是一个用例独占的共享后端：Postgres 中的独立 schema 与 Redis 中的独立前缀。
type backend struct {
	redisAddrs []string
	dsn        string
	schema     string
	prefix     string
}

func newBackend(t *testing.T) *backend {
	t.Helper()
	addr, dsn := os.Getenv(redisAddrEnv), os.Getenv(pgDSNEnv)
	if addr == "" || dsn == "" {
		t.Skipf("未同时设置 %s 与 %s，跳过端到端测试", redisAddrEnv, pgDSNEnv)
	}
	suffix := strings.ToLower(crand.Text()[:12])
	b := &backend{redisAddrs: strings.Split(addr, ","), dsn: dsn, schema: "t_" + suffix, prefix: "goagent:e2e:" + suffix + ":"}
	ctx := context.Background()

	admin, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA `+b.schema+` CASCADE`)
		admin.Close()
	})
	_, err = admin.Exec(ctx, `CREATE SCHEMA `+b.schema)
	require.NoError(t, err)
	pool := b.pool(t)
	for _, stmt := range strings.Split(pgstore.Schema+pgvector.Schema, ";") {
		if strings.TrimSpace(stmt) != "" {
			_, err = pool.Exec(ctx, stmt)
			require.NoError(t, err)
		}
	}

	client := b.redisClient(t)
	t.Cleanup(func() { deletePrefix(client, b.prefix) })
	return b
}

// pool 返回绑定本用例 schema 的连接池；vector 类型由扩展装在 public 中，所以 search_path 带上 public。
func (b *backend) pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(b.dsn)
	require.NoError(t, err)
	cfg.ConnConfig.RuntimeParams["search_path"] = b.schema + ",public"
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func (b *backend) redisClient(t *testing.T) goredis.UniversalClient {
	t.Helper()
	c := goredis.NewUniversalClient(&goredis.UniversalOptions{Addrs: b.redisAddrs})
	t.Cleanup(func() { _ = c.Close() })
	return c
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

// forEachMaster 对每个主节点执行 fn：Cluster 下 SCAN 只作用于单个节点。
func forEachMaster(ctx context.Context, client goredis.UniversalClient, fn func(context.Context, *goredis.Client) error) error {
	if cluster, ok := client.(*goredis.ClusterClient); ok {
		return cluster.ForEachMaster(ctx, fn)
	}
	return fn(ctx, client.(*goredis.Client))
}

// entryKeys 返回本用例前缀下的全部缓存条目键（"e:" 段见 cache/redis 的键布局）。
func (b *backend) entryKeys(t *testing.T, client goredis.UniversalClient) []string {
	t.Helper()
	var keys []string
	err := forEachMaster(context.Background(), client, func(ctx context.Context, node *goredis.Client) error {
		iter := node.Scan(ctx, 0, b.prefix+"e:*", 1000).Iterator()
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		return iter.Err()
	})
	require.NoError(t, err)
	return keys
}

// conceptEmbedder 把同一概念的不同说法映射到同一维，使"拿铁"与"咖啡"只靠向量相似、没有文本重合。
type conceptEmbedder struct{}

var concepts = [][]string{{"咖啡", "拿铁"}, {"茶"}}

func (conceptEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	v := make([]float32, len(concepts)+1)
	// 偏置维避免零向量。
	v[len(concepts)] = 0.05
	for i, words := range concepts {
		for _, w := range words {
			if strings.Contains(text, w) {
				v[i]++
			}
		}
	}
	return v, nil
}

// cachedClient 声明模型身份：ScriptedClient 没有模型名，不声明时模型调用缓存不生效。
type cachedClient struct{ *mocks.ScriptedClient }

func (cachedClient) CacheIdentity() string { return "scripted-e2e" }

// instance 模拟一个进程：自己的连接池、pgstore、pgvector 索引、Redis 客户端与缓存，只共享后端数据。
type instance struct {
	mem *memory.Memory
	mc  builder.ModelCache
}

func (b *backend) newInstance(t *testing.T, owner string) *instance {
	t.Helper()
	ctx := context.Background()
	pool := b.pool(t)
	store, err := pgstore.New(ctx, pgstore.Config{Pool: pool, Owner: owner})
	require.NoError(t, err)
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = store.Close(cctx)
	})
	index, err := pgvector.New(ctx, pgvector.Config{Pool: pool})
	require.NoError(t, err)
	rc, err := redis.New(ctx, redis.Config{Client: b.redisClient(t), Prefix: b.prefix})
	require.NoError(t, err)
	mc := builder.ModelCache{Cache: rc, TTL: time.Minute, TagKey: tagKey, Erase: builder.EraseRequired}
	mem, err := memory.New(memory.Config{Durable: store, Index: index, Embedder: conceptEmbedder{}, OnErase: mc.Eraser()})
	require.NoError(t, err)
	return &instance{mem: mem, mc: mc}
}

// ask 以新的会话日志、同一会话 ID 跑一轮：请求只取决于输入与共享的记忆，两个实例的同一问题得到同一个缓存键。
func (in *instance) ask(t *testing.T, chat *mocks.ScriptedClient, ns []string, input string) {
	t.Helper()
	a, err := builder.NewSimpleBuilder(cachedClient{chat}).
		WithSessionLog(session.NewMemoryStore()).
		WithLongTermMemory(in.mem).
		WithModelCache(in.mc).
		Build()
	require.NoError(t, err)
	ctx := tools.WithSessionID(memory.WithNamespace(context.Background(), ns...), "s1")
	_, err = a.AsRunnable().Invoke(ctx, input)
	require.NoError(t, err)
}

func newNamespace() []string {
	return []string{"tenant", "u-" + strings.ToLower(crand.Text()[:8])}
}

// 进程甲 Forget 之后，进程乙写入共享 Redis 的缓存条目被物理删除，乙再次调用回到模型。
func TestForgetErasesOtherInstanceCache(t *testing.T) {
	b := newBackend(t)
	alpha, beta := b.newInstance(t, "甲"), b.newInstance(t, "乙")
	chat := mocks.NewScriptedClient(mocks.TextReply("第1次", "你喜欢喝咖啡"), mocks.TextReply("第2次", "我不知道"))
	ns := newNamespace()
	actx := memory.WithNamespace(context.Background(), ns...)

	r, err := alpha.mem.Remember(actx, memory.Record{Kind: memory.KindFact, Content: "用户喜欢喝咖啡", Importance: 0.6})
	require.NoError(t, err)

	beta.ask(t, chat, ns, "我喜欢喝什么？")
	require.Equal(t, 1, chat.Calls(), "乙首次调用应请求模型")
	alpha.ask(t, chat, ns, "我喜欢喝什么？")
	require.Equal(t, 1, chat.Calls(), "甲的同一请求应命中乙写入的缓存")

	client := b.redisClient(t)
	// 本用例的每个条目都读过 ns，都带 ns 的记忆标签。
	entries := b.entryKeys(t, client)
	require.NotEmpty(t, entries, "Forget 前应有缓存条目")

	require.NoError(t, alpha.mem.Forget(actx, r.ID))
	require.Zero(t, alpha.mem.EraseErrors(), "提交后的立即擦除不应失败")
	n, err := client.Exists(context.Background(), entries...).Result()
	require.NoError(t, err)
	require.Zero(t, n, "Forget 返回后，带该命名空间标签的条目应已从 Redis 物理删除")

	// 记忆已删，提示词变了，即使擦除失效这里也会未命中；区分擦除是否到达的是上面的 EXISTS 断言。
	beta.ask(t, chat, ns, "我喜欢喝什么？")
	require.Equal(t, 2, chat.Calls(), "乙再次调用不应命中，回到模型")
}

// 甲写入记录并建索引，乙的召回能用甲写入 pgvector 的向量算出相关性。
func TestRecallSeesOtherInstanceVectors(t *testing.T) {
	b := newBackend(t)
	alpha, beta := b.newInstance(t, "甲"), b.newInstance(t, "乙")
	ns := newNamespace()
	ctx := memory.WithNamespace(context.Background(), ns...)

	r, err := alpha.mem.Remember(ctx, memory.Record{Kind: memory.KindFact, Content: "用户喜欢喝咖啡", Importance: 0.6})
	require.NoError(t, err)

	// "拿铁"与记录正文没有共同字符，相关性只能来自向量。
	got, err := beta.mem.Recall(ctx, memory.Query{Text: "拿铁"})
	require.NoError(t, err)
	var found *memory.Scored
	for i := range got {
		if got[i].ID == r.ID {
			found = &got[i]
		}
	}
	require.NotNil(t, found, "乙的召回应包含甲写入的记录：%v", got)
	require.Greater(t, found.Relevance, 0.9, "相关性应来自甲写入的向量")
}
