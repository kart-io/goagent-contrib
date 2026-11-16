// Package redis 是基于 Redis 的 cache.Bytes 与 cache.Tagged 实现，供多个实例共享模型调用缓存。
//
// 每个标签有一个随机令牌，条目写入时记下各标签的令牌，读取时逐个比对；DeleteTag 先删除令牌，
// 此后带旧令牌的条目一律未命中（S1），再按成员集合物理删除条目（S2，正常路径）。
// 协议不依赖多键原子操作，单机、哨兵与原生 Cluster 下都成立。
//
// 前置条件：
//   - Redis ≥ 7；不支持 Ring 客户端、代理模式集群与 KeyDB、Dragonfly 等兼容实现。
//   - 令牌的读写都到主节点：读到从节点上滞后的旧令牌会让已删除条目可读。
//
// S2 的例外：写者在登记成员后、再次登记前失败且恰有 DeleteTag 在其间移除了成员，客户端超时重试使写入迟到，
// 另一个 DeleteTag 在移除成员与删除条目之间崩溃，成员集合被 maxmemory 淘汰，过期键的回收延迟，
// 成员集合所在节点时钟向前跳变（成员被提前清理），条目所在节点时钟向后跳变（条目存活超过 TTL 而成员已被剪除），
// 以及主从切换丢失已确认的 ZREM、UNLINK 或成员登记（WaitReplicas 不覆盖这些命令）。
// 这些情形下条目始终读不到，物理删除依靠 TTL 加 Redis 的过期回收周期。
package redis

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/kart-io/goagent/cache"
	agentErrors "github.com/kart-io/goagent/errors"
)

const (
	defaultPrefix      = "goagent:cache:"
	defaultDeleteBatch = 500
	defaultWaitTimeout = time.Second
	minRedisMajor      = 7
)

// Config 是 Cache 的配置。
type Config struct {
	// Client 为单机、哨兵（Failover）或 Cluster 客户端；Ring 客户端被拒绝。
	Client goredis.UniversalClient
	// Prefix 是全部键的前缀，默认 "goagent:cache:"。
	Prefix string
	// DeleteBatch 是 DeleteTag 每批扫描的成员数，默认 500。
	DeleteBatch int
	// WaitReplicas 大于 0 时，DeleteTag 删除令牌后等待这么多从节点确认，不足则返回错误。默认 0。
	// 它缩小而不消除主从切换让已删除条目重新可读的窗口：被提升的从节点不一定是确认过的那些。
	WaitReplicas int
	// WaitTimeout 是等待从节点确认的时限，默认 1 秒。
	WaitTimeout time.Duration
}

// Cache 是 Redis 版标签缓存，可被多个 goroutine 并发使用。
type Cache struct {
	cfg Config
}

var (
	_ cache.Bytes  = (*Cache)(nil)
	_ cache.Tagged = (*Cache)(nil)
)

// New 校验配置、服务端版本与"令牌读写都到主节点"的前提后返回 Cache；不关闭 Client，由调用方负责。
// 配置错误返回 CodeInvalidInput。
func New(ctx context.Context, cfg Config) (*Cache, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	if cfg.Prefix == "" {
		cfg.Prefix = defaultPrefix
	}
	if strings.ContainsAny(cfg.Prefix, "{}") {
		slog.Warn("cache/redis 的 Prefix 含花括号，Cluster 下全部数据会落到同一个槽", "prefix", cfg.Prefix)
	}
	if cfg.DeleteBatch == 0 {
		cfg.DeleteBatch = defaultDeleteBatch
	}
	if cfg.WaitTimeout == 0 {
		cfg.WaitTimeout = defaultWaitTimeout
	}
	if err := cfg.Client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("连接 Redis 失败：%w", err)
	}
	info, err := cfg.Client.Info(ctx, "server").Result()
	if err != nil {
		return nil, fmt.Errorf("读取 Redis 版本失败：%w", err)
	}
	major, err := parseRedisMajor(info)
	if err != nil {
		return nil, err
	}
	if major < minRedisMajor {
		return nil, agentErrors.Newf(agentErrors.CodeInvalidInput, "cache/redis 需要 Redis %d 及以上，服务端主版本为 %d", minRedisMajor, major)
	}
	// Cluster 客户端只把命令路由到主节点（ReadOnly 等选项已在上面拒绝），不逐个检查。
	if _, ok := cfg.Client.(*goredis.ClusterClient); !ok {
		if err := requireMaster(ctx, cfg.Client); err != nil {
			return nil, err
		}
	}
	warnEviction(ctx, cfg.Client)
	return &Cache{cfg: cfg}, nil
}

// validateConfig 检查不需要访问服务端的配置项。
// 哨兵客户端的 RouteByLatency/RouteRandomly 在 NewFailoverClient 中直接 panic，NewFailoverClusterClient 把它们
// 与 ReplicaOnly（映射为 ReadOnly）放进 ClusterOptions，在这里拒绝；NewFailoverClient 的 ReplicaOnly 连到从节点，由 ROLE 检查拒绝。
func validateConfig(cfg Config) error {
	switch client := cfg.Client.(type) {
	case nil:
		return agentErrors.New(agentErrors.CodeInvalidInput, "cache/redis 需要 Client")
	case *goredis.Ring:
		return agentErrors.New(agentErrors.CodeInvalidInput, "cache/redis 不支持 Ring 客户端：客户端分片在主从切换与迁槽时语义不同")
	case *goredis.ClusterClient:
		opt := client.Options()
		if opt.ReadOnly || opt.RouteByLatency || opt.RouteRandomly {
			return agentErrors.New(agentErrors.CodeInvalidInput,
				"cache/redis 不支持 ReadOnly、RouteByLatency、RouteRandomly：读到从节点上滞后的令牌会让已删除条目可读")
		}
	case *goredis.Client:
	default:
		if cfg.WaitReplicas > 0 {
			return agentErrors.Newf(agentErrors.CodeInvalidInput, "cache/redis 的 WaitReplicas 需要 *redis.Client 或 *redis.ClusterClient，得到 %T", client)
		}
	}
	if cfg.DeleteBatch < 0 || cfg.WaitReplicas < 0 || cfg.WaitTimeout < 0 {
		return agentErrors.New(agentErrors.CodeInvalidInput, "cache/redis 配置无效：DeleteBatch、WaitReplicas、WaitTimeout 不能为负")
	}
	return nil
}

// requireMaster 用 ROLE 确认所连节点是主节点。
func requireMaster(ctx context.Context, client goredis.UniversalClient) error {
	role, err := client.Do(ctx, "role").Slice()
	if err != nil {
		return fmt.Errorf("执行 ROLE 失败：%w", err)
	}
	if len(role) == 0 || role[0] != "master" {
		return agentErrors.Newf(agentErrors.CodeInvalidInput, "cache/redis 须连接主节点，ROLE 返回 %v：读到从节点上滞后的令牌会让已删除条目可读", role)
	}
	return nil
}

// warnEviction 在淘汰策略不是 noeviction 时告警：标签成员集合可能被淘汰，DeleteTag 只能靠 TTL 物理删除对应条目。
// Cluster 逐个主节点检查（keyless 命令只会打到一个随机节点）；CONFIG 被禁用（托管服务常见）时静默跳过。
func warnEviction(ctx context.Context, client goredis.UniversalClient) {
	check := func(ctx context.Context, node goredis.UniversalClient, addr string) {
		cfg, err := node.ConfigGet(ctx, "maxmemory-policy").Result()
		if err != nil {
			return
		}
		if policy, ok := cfg["maxmemory-policy"]; ok && policy != "noeviction" {
			slog.Warn("cache/redis 所在 Redis 的淘汰策略不是 noeviction，标签成员集合被淘汰时 DeleteTag 只能依靠 TTL 物理删除条目",
				"addr", addr, "maxmemory-policy", policy)
		}
	}
	switch c := client.(type) {
	case *goredis.ClusterClient:
		_ = c.ForEachMaster(ctx, func(ctx context.Context, node *goredis.Client) error {
			check(ctx, node, node.Options().Addr)
			return nil
		})
	case *goredis.Client:
		check(ctx, c, c.Options().Addr)
	default:
		check(ctx, client, "")
	}
}

// parseRedisMajor 从 INFO server 的输出中取 redis_version 的主版本号。
func parseRedisMajor(info string) (int, error) {
	for line := range strings.Lines(info) {
		v, ok := strings.CutPrefix(strings.TrimSpace(line), "redis_version:")
		if !ok {
			continue
		}
		major, _, _ := strings.Cut(v, ".")
		n, err := strconv.Atoi(major)
		if err != nil {
			return 0, fmt.Errorf("无法解析 Redis 版本 %q：%w", v, err)
		}
		return n, nil
	}
	return 0, fmt.Errorf("INFO server 中没有 redis_version")
}
