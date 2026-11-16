package redis

import (
	"context"
	crand "crypto/rand"
	"errors"
	"fmt"
	"slices"
	"time"

	goredis "github.com/redis/go-redis/v9"

	agentErrors "github.com/kart-io/goagent/errors"
)

// 编号步骤必须前一步完成后再发下一步，不得合进同一个 pipeline：Cluster 下 go-redis 把 pipeline 按节点拆开并发执行，
// 跨节点没有顺序，正确性论证会失效。同一步内发往多个键的同类命令可以 pipeline。

// 测试钩子，生产代码中恒为 nil。
var (
	// hookBeforeEntryWrite 在 SetTagged 第 2 步之后、第 3 步之前调用。
	hookBeforeEntryWrite func()
	// hookEntryWritten 在 SetTagged 第 3 步写入条目之后调用，返回错误时按第 3 步失败处理（模拟服务端已生效而客户端超时）。
	hookEntryWritten func() error
	// hookAfterWrite 在 SetTagged 第 4 步之后、第 5 步之前调用。
	hookAfterWrite func()
	// hookAfterTokenDelete 在 DeleteTag 删除令牌之后、扫描成员之前调用。
	hookAfterTokenDelete func()
	// hookBetweenRemoveAndUnlink 在 DeleteTag 每批 ZREM 完成之后、UNLINK 之前调用。
	hookBetweenRemoveAndUnlink func()
)

const tokenSize = 16

// GetBytes 读取条目并校验其中记录的各标签令牌；任一令牌缺失或不等即未命中，并删除该条目。
// 条目格式无法解码时同样按未命中处理并删除。
func (c *Cache) GetBytes(ctx context.Context, key string) ([]byte, error) {
	ek := c.entryKey(key)
	raw, err := c.cfg.Client.Get(ctx, ek).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, agentErrors.ErrCacheMiss
	}
	if err != nil {
		return nil, fmt.Errorf("读取缓存条目失败：%w", err)
	}
	e, err := decodeEntry(raw)
	if err != nil {
		c.discard(ctx, ek)
		return nil, agentErrors.ErrCacheMiss
	}
	ok, err := c.tokensMatch(ctx, e.tags, e.tokens)
	if err != nil {
		return nil, err
	}
	if !ok {
		// 可能误删同一时刻写入的新值，属允许的多删。
		c.discard(ctx, ek)
		return nil, agentErrors.ErrCacheMiss
	}
	return e.value, nil
}

// discard 删除已判定不可读的条目；失败不影响结果，下次读取会再次判定并删除。
func (c *Cache) discard(ctx context.Context, entryKey string) {
	_ = c.cfg.Client.Del(ctx, entryKey).Err()
}

// SetBytes 写入无标签条目。ttl 须为正，否则返回 CodeInvalidInput（Redis 不接受非正的过期时间，也不应写入永不过期的键）。
func (c *Cache) SetBytes(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	ms, err := ttlMillis(ttl)
	if err != nil {
		return err
	}
	return c.writeEntry(ctx, key, encodeEntry(nil, nil, value), ms)
}

// Delete 删除条目，不改动标签成员集合；集合中的残留成员只会让之后的 DeleteTag 多删。
func (c *Cache) Delete(ctx context.Context, key string) error {
	if err := c.cfg.Client.Del(ctx, c.entryKey(key)).Err(); err != nil {
		return fmt.Errorf("删除缓存条目失败：%w", err)
	}
	return nil
}

// SetTagged 写入条目并关联标签，五步依次执行：
//  1. 取各标签当前令牌（不存在则写入新令牌），并把已存在的令牌延长到不短于 ttl；
//  2. 登记成员；
//  3. 写条目，记下各标签令牌；
//  4. 再登记成员，与 DeleteTag 的"先移除成员、后删条目"配合；
//  5. 复查令牌，任一缺失或与第 1 步不同则删除刚写的条目。
//
// 先登记再写条目：只要条目存在，成员就已登记，写者在第 3 步前后失败都不会留下集合外的条目。
func (c *Cache) SetTagged(ctx context.Context, key string, value []byte, ttl time.Duration, tags ...string) error {
	ms, err := ttlMillis(ttl)
	if err != nil {
		return err
	}
	tags = slices.Compact(slices.Sorted(slices.Values(tags)))

	tokens, err := c.acquireTokens(ctx, tags, ms)
	if err != nil {
		return err
	}
	if err := c.register(ctx, key, tags, ms); err != nil {
		return err
	}
	if hookBeforeEntryWrite != nil {
		hookBeforeEntryWrite()
	}
	if err := c.writeEntry(ctx, key, encodeEntry(tags, tokens, value), ms); err != nil {
		return err
	}
	if hookEntryWritten != nil {
		if err := hookEntryWritten(); err != nil {
			return err
		}
	}
	if err := c.register(ctx, key, tags, ms); err != nil {
		return err
	}
	if hookAfterWrite != nil {
		hookAfterWrite()
	}
	ok, err := c.tokensMatch(ctx, tags, tokens)
	if err != nil {
		return err
	}
	if !ok {
		// 令牌在第 1 步之后被删除：自己删掉刚写的条目，减少只能靠 TTL 回收的残留。条目本就不可读，返回 nil。
		return c.Delete(ctx, key)
	}
	return nil
}

// DeleteTag 先删除标签令牌，完成即满足 S1；再分批扫描成员集合，每批先 ZREM 成员、完成后再 UNLINK 条目。
// 不在最后删除成员集合：集合清空时 Redis 自动删除它，整体删除会抹掉并发写入刚登记的新成员。
func (c *Cache) DeleteTag(ctx context.Context, tag string) error {
	if err := c.cfg.Client.Del(ctx, c.tokenKey(tag)).Err(); err != nil {
		return fmt.Errorf("删除标签令牌失败：%w", err)
	}
	if hookAfterTokenDelete != nil {
		hookAfterTokenDelete()
	}
	mk := c.memberKey(tag)
	var cursor uint64
	for {
		page, next, err := c.cfg.Client.ZScan(ctx, mk, cursor, "", int64(c.cfg.DeleteBatch)).Result()
		if err != nil {
			return fmt.Errorf("扫描标签成员失败：%w", err)
		}
		if err := c.removeBatch(ctx, mk, page); err != nil {
			return err
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}

// removeBatch 处理 ZSCAN 返回的一批"成员, 分数"交替列表。
// 条目键可能跨槽，逐键 UNLINK 放进同一个 pipeline，Cluster 客户端会按节点分组发送。
func (c *Cache) removeBatch(ctx context.Context, memberKey string, page []string) error {
	n := len(page) / 2
	if n == 0 {
		return nil
	}
	members := make([]any, n)
	for i := range n {
		members[i] = page[2*i]
	}
	if err := c.cfg.Client.ZRem(ctx, memberKey, members...).Err(); err != nil {
		return fmt.Errorf("移除标签成员失败：%w", err)
	}
	if hookBetweenRemoveAndUnlink != nil {
		hookBetweenRemoveAndUnlink()
	}
	pipe := c.cfg.Client.Pipeline()
	for i := range n {
		pipe.Unlink(ctx, c.entryKey(page[2*i]))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("删除标签条目失败：%w", err)
	}
	return nil
}

// acquireTokens 是 SetTagged 第 1 步：SET NX GET 取得各标签当前令牌；已存在的令牌在随后一轮中延长过期，
// 避免令牌先于条目过期造成无谓未命中。
func (c *Cache) acquireTokens(ctx context.Context, tags []string, ms int64) ([]string, error) {
	if len(tags) == 0 {
		return nil, nil
	}
	pipe := c.cfg.Client.Pipeline()
	fresh := make([]string, len(tags))
	cmds := make([]*goredis.Cmd, len(tags))
	for i, tag := range tags {
		fresh[i] = newToken()
		cmds[i] = pipe.Do(ctx, "set", c.tokenKey(tag), fresh[i], "nx", "get", "px", ms)
	}
	// 未命中的 SET GET 返回 nil 回复，Exec 会把它当作错误返回，因此逐条检查。
	_, _ = pipe.Exec(ctx)
	tokens := make([]string, len(tags))
	var existing []scriptCall
	for i, cmd := range cmds {
		cur, err := cmd.Text()
		switch {
		case errors.Is(err, goredis.Nil):
			tokens[i] = fresh[i]
		case err != nil:
			return nil, fmt.Errorf("取标签令牌失败：%w", err)
		default:
			tokens[i] = cur
			existing = append(existing, scriptCall{key: c.tokenKey(tags[i]), args: []any{ms}})
		}
	}
	if err := runScripts(ctx, c.cfg.Client, extendScript, existing); err != nil {
		return nil, fmt.Errorf("延长标签令牌过期时间失败：%w", err)
	}
	return tokens, nil
}

// register 对每个标签执行一次成员登记 reg(t, key, ttl)。
func (c *Cache) register(ctx context.Context, key string, tags []string, ms int64) error {
	calls := make([]scriptCall, len(tags))
	for i, tag := range tags {
		calls[i] = scriptCall{key: c.memberKey(tag), args: []any{key, ms}}
	}
	if err := runScripts(ctx, c.cfg.Client, regScript, calls); err != nil {
		return fmt.Errorf("登记标签成员失败：%w", err)
	}
	return nil
}

func (c *Cache) writeEntry(ctx context.Context, key string, payload []byte, ms int64) error {
	if err := c.cfg.Client.Do(ctx, "set", c.entryKey(key), payload, "px", ms).Err(); err != nil {
		return fmt.Errorf("写入缓存条目失败：%w", err)
	}
	return nil
}

// tokensMatch 一步 pipeline 读取各标签当前令牌，全部存在且与 want 相同时返回 true。
func (c *Cache) tokensMatch(ctx context.Context, tags, want []string) (bool, error) {
	if len(tags) == 0 {
		return true, nil
	}
	pipe := c.cfg.Client.Pipeline()
	cmds := make([]*goredis.StringCmd, len(tags))
	for i, tag := range tags {
		cmds[i] = pipe.Get(ctx, c.tokenKey(tag))
	}
	_, _ = pipe.Exec(ctx)
	match := true
	for i, cmd := range cmds {
		cur, err := cmd.Result()
		switch {
		case errors.Is(err, goredis.Nil):
			match = false
		case err != nil:
			return false, fmt.Errorf("读取标签令牌失败：%w", err)
		case cur != want[i]:
			match = false
		}
	}
	return match, nil
}

func newToken() string {
	b := make([]byte, tokenSize)
	_, _ = crand.Read(b)
	return string(b)
}

// ttlMillis 把 ttl 向上取整到毫秒；非正的 ttl 返回 CodeInvalidInput。
func ttlMillis(ttl time.Duration) (int64, error) {
	if ttl <= 0 {
		return 0, agentErrors.Newf(agentErrors.CodeInvalidInput, "缓存 TTL 须为正，得到 %v", ttl)
	}
	ms := ttl.Milliseconds()
	if ttl%time.Millisecond != 0 {
		ms++
	}
	return ms, nil
}
