package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"
)

// regScript 是成员登记 reg(t, key, ttl)，只涉及 t 一个键，Cluster 下不跨槽。
// 先剪除已过期条目的成员，使集合大小以存活条目为界；ZADD GT 让并发写入取最大过期时刻，不被短 TTL 写入压低；
// PTTL 为 -1（无过期）或小于 ttl 时延长，t 不会永久存在，也不会先于成员条目过期。
var regScript = goredis.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local ttl = tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
redis.call('ZADD', KEYS[1], 'GT', now + ttl, ARGV[1])
if redis.call('PTTL', KEYS[1]) < ttl then
  redis.call('PEXPIRE', KEYS[1], ttl)
end
return 1
`)

// extendScript 在键存在且剩余时间小于 ttl 时延长到 ttl；键不存在时什么也不做，不会重建已被 DeleteTag 删除的令牌。
var extendScript = goredis.NewScript(`
local pttl = redis.call('PTTL', KEYS[1])
if pttl ~= -2 and pttl < tonumber(ARGV[1]) then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
return 1
`)

// deleteTokenScript 删除令牌并保证产生复制流：令牌已不存在（重试或并发删除）时 DEL 是空操作，不推进本连接的复制偏移，
// 随后的 WAIT 会按旧偏移立即返回；此时写入随机值再删除，脚本按效果复制，SET 与 DEL 都进入复制流。
var deleteTokenScript = goredis.NewScript(`
if redis.call('DEL', KEYS[1]) == 0 then
  redis.call('SET', KEYS[1], ARGV[1])
  redis.call('DEL', KEYS[1])
end
return 1
`)

type scriptCall struct {
	key  string
	args []any
}

// runScripts 把同一步内对多个键的同一脚本调用放进一个 pipeline。
// 先用 EVALSHA；只有返回 NOSCRIPT 的调用再用 EVAL 重发，这些调用在服务端没有执行，重发不会执行两次。
func runScripts(ctx context.Context, client goredis.UniversalClient, script *goredis.Script, calls []scriptCall) error {
	if len(calls) == 0 {
		return nil
	}
	pipe := client.Pipeline()
	cmds := make([]*goredis.Cmd, len(calls))
	for i, call := range calls {
		cmds[i] = script.EvalSha(ctx, pipe, []string{call.key}, call.args...)
	}
	_, _ = pipe.Exec(ctx)
	var retry []scriptCall
	for i, cmd := range cmds {
		if err := cmd.Err(); err != nil {
			if !goredis.HasErrorPrefix(err, "NOSCRIPT") {
				return err
			}
			retry = append(retry, calls[i])
		}
	}
	if len(retry) == 0 {
		return nil
	}
	pipe = client.Pipeline()
	for _, call := range retry {
		script.Eval(ctx, pipe, []string{call.key}, call.args...)
	}
	_, err := pipe.Exec(ctx)
	return err
}
