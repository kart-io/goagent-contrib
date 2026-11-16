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
// 另一个 DeleteTag 在移除成员与删除条目之间崩溃，成员集合被 maxmemory 淘汰，以及过期键的回收延迟。
// 这些情形下条目始终读不到，物理删除依靠 TTL 加 Redis 的过期回收周期。
package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/kart-io/goagent/cache"
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
	// WaitReplicas 大于 0 时，DeleteTag 删除令牌后等待这么多从节点确认。默认 0。
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

// New 校验配置与服务端版本后返回 Cache；不关闭 Client，由调用方负责。
func New(ctx context.Context, cfg Config) (*Cache, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("cache/redis 需要 Client")
	}
	if _, ok := cfg.Client.(*goredis.Ring); ok {
		return nil, fmt.Errorf("cache/redis 不支持 Ring 客户端：客户端分片在主从切换与迁槽时语义不同")
	}
	if cfg.DeleteBatch < 0 || cfg.WaitReplicas < 0 || cfg.WaitTimeout < 0 {
		return nil, fmt.Errorf("cache/redis 配置无效：DeleteBatch、WaitReplicas、WaitTimeout 不能为负")
	}
	if cfg.Prefix == "" {
		cfg.Prefix = defaultPrefix
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
		return nil, fmt.Errorf("cache/redis 需要 Redis %d 及以上，服务端主版本为 %d", minRedisMajor, major)
	}
	return &Cache{cfg: cfg}, nil
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
