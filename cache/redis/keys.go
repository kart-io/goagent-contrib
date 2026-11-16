package redis

import "strings"

// escaper 对键与标签文本做百分号编码：Cluster 把花括号内的部分当作 hash tag，不转义会把数据集中到单槽。
// % 自身也编码，使编码可逆，不同的原文不会得到同一个键。
var escaper = strings.NewReplacer("%", "%25", "{", "%7B", "}", "%7D")

func escape(s string) string { return escaper.Replace(s) }

// entryKey 是条目键（String）。
func (c *Cache) entryKey(key string) string { return c.cfg.Prefix + "e:" + escape(key) }

// tokenKey 是标签的当前令牌键（String）。
func (c *Cache) tokenKey(tag string) string { return c.cfg.Prefix + "g:" + escape(tag) }

// memberKey 是标签的成员集合（ZSET）：元素为原始缓存键，分数为该键条目的过期时刻（服务端毫秒）。
func (c *Cache) memberKey(tag string) string { return c.cfg.Prefix + "t:" + escape(tag) }
