package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	agentErrors "github.com/kart-io/goagent/errors"
)

const longTTL = time.Minute

func opCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func requireMiss(t *testing.T, c *Cache, key string) {
	t.Helper()
	v, err := c.GetBytes(opCtx(t), key)
	require.Truef(t, errors.Is(err, agentErrors.ErrCacheMiss), "GetBytes(%s) 期望未命中，得到 value=%q err=%v", key, v, err)
}

func requireValue(t *testing.T, c *Cache, key, want string) {
	t.Helper()
	v, err := c.GetBytes(opCtx(t), key)
	require.NoError(t, err)
	require.Equal(t, want, string(v))
}

// requireEntryExists 断言条目键在 Redis 中是否物理存在。
func requireEntryExists(t *testing.T, c *Cache, key string, want bool) {
	t.Helper()
	n, err := c.cfg.Client.Exists(opCtx(t), c.entryKey(key)).Result()
	require.NoError(t, err)
	require.Equalf(t, want, n == 1, "EXISTS %s", c.entryKey(key))
}

// setHook 在测试期间替换包级钩子，结束时复原。
func setHook[T any](t *testing.T, hook *T, fn T) {
	t.Helper()
	old := *hook
	*hook = fn
	t.Cleanup(func() { *hook = old })
}

// SetTagged 第 4 步之后、第 5 步之前令牌被删除：写者复查发现令牌已变，删除自己的条目。
// "令牌丢失"只删令牌键、不扫描成员，条目只能由第 5 步复查删除。
func TestDeleteDuringWrite(t *testing.T) {
	for _, tc := range []struct {
		name   string
		inject func(t *testing.T, c *Cache)
	}{
		{"插入 DeleteTag", func(t *testing.T, c *Cache) {
			require.NoError(t, c.DeleteTag(opCtx(t), "s"))
		}},
		{"令牌丢失", func(t *testing.T, c *Cache) {
			require.NoError(t, c.cfg.Client.Del(opCtx(t), c.tokenKey("s")).Err())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := openCache(t, newPrefix(t))
			setHook(t, &hookAfterWrite, func() { tc.inject(t, c) })
			require.NoError(t, c.SetTagged(opCtx(t), "k", []byte("v"), longTTL, "s"))
			requireEntryExists(t, c, "k", false)
			requireMiss(t, c, "k")
		})
	}
}

// §4.2 P2：D1 移除第一批成员后暂停；期间写入 k（与 D1 本批同键）与 k2（新键）并完成，D2 完整执行；
// 放行 D1 后条目物理不存在。另一种顺序是 D1 先返回、D2 后执行，用来发现"最后整体删除成员集合"抹掉新成员的错误。
func TestConcurrentDeleteTagsKeepsNewMember(t *testing.T) {
	for _, tc := range []struct {
		name          string
		d2BeforeD1End bool
	}{
		{"D2 在 D1 暂停期间完成", true},
		{"D1 返回后执行 D2", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefix := newPrefix(t)
			w, d1, d2 := openCache(t, prefix), openCache(t, prefix), openCache(t, prefix)
			require.NoError(t, w.SetTagged(opCtx(t), "k", []byte("v1"), longTTL, "s"))

			paused := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			setHook(t, &hookBetweenRemoveAndUnlink, func() {
				first := false
				once.Do(func() { first = true })
				if first {
					close(paused)
					<-release
				}
			})

			d1Done := make(chan error, 1)
			go func() { d1Done <- d1.DeleteTag(context.Background(), "s") }()
			select {
			case <-paused:
			case <-time.After(5 * time.Second):
				t.Fatal("D1 未到达第一批 ZREM 之后")
			}

			for _, k := range []string{"k", "k2"} {
				require.NoError(t, w.SetTagged(opCtx(t), k, []byte("v2"), longTTL, "s"))
				requireValue(t, w, k, "v2")
			}
			if tc.d2BeforeD1End {
				require.NoError(t, d2.DeleteTag(opCtx(t), "s"))
			}
			close(release)
			select {
			case err := <-d1Done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("D1 未返回")
			}
			if !tc.d2BeforeD1End {
				require.NoError(t, d2.DeleteTag(opCtx(t), "s"))
			}
			for _, k := range []string{"k", "k2"} {
				requireEntryExists(t, w, k, false)
				requireMiss(t, w, k)
			}
		})
	}
}

// 第 4 步再登记：D 删除令牌后、扫描前，W 完成第 1、2 步并暂停；D 扫描并移除 k、删除尚不存在的条目后，W 再写条目。
// W 返回时 D 尚未开始的 D' 必须物理删除条目——只有第 4 步把 k 重新登记进集合才能做到。
func TestReRegisterAfterWrite(t *testing.T) {
	prefix := newPrefix(t)
	w, d, d2 := openCache(t, prefix), openCache(t, prefix), openCache(t, prefix)

	tokenDeleted := make(chan struct{})
	resumeD := make(chan struct{})
	var once sync.Once
	setHook(t, &hookAfterTokenDelete, func() {
		first := false
		once.Do(func() { first = true })
		if first {
			close(tokenDeleted)
			<-resumeD
		}
	})
	dDone := make(chan error, 1)
	go func() { dDone <- d.DeleteTag(context.Background(), "s") }()
	select {
	case <-tokenDeleted:
	case <-time.After(5 * time.Second):
		t.Fatal("D 未到达删除令牌之后")
	}

	setHook(t, &hookBeforeEntryWrite, func() {
		close(resumeD)
		select {
		case err := <-dDone:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("D 未返回")
		}
	})
	require.NoError(t, w.SetTagged(opCtx(t), "k", []byte("v"), longTTL, "s"))
	requireValue(t, w, "k", "v")

	require.NoError(t, d2.DeleteTag(opCtx(t), "s"))
	requireEntryExists(t, w, "k", false)
	requireMiss(t, w, "k")
}

// 第 3 步写入在服务端生效但客户端报错（如超时）：第 2 步已登记成员，DeleteTag 仍能物理删除。
func TestWriteFailureLeavesNoReadable(t *testing.T) {
	c := openCache(t, newPrefix(t))
	injected := errors.New("注入的写入错误")
	setHook(t, &hookEntryWritten, func() error { return injected })
	require.ErrorIs(t, c.SetTagged(opCtx(t), "k", []byte("v"), longTTL, "s"), injected)
	requireEntryExists(t, c, "k", true)

	require.NoError(t, c.DeleteTag(opCtx(t), "s"))
	requireEntryExists(t, c, "k", false)
	requireMiss(t, c, "k")
}

// 令牌键丢失（模拟淘汰）后旧条目不可读，新令牌也不会让旧条目复活。
func TestTokenLossNoResurrection(t *testing.T) {
	c := openCache(t, newPrefix(t))
	require.NoError(t, c.SetTagged(opCtx(t), "k", []byte("v"), longTTL, "s"))
	require.NoError(t, c.SetTagged(opCtx(t), "unread", []byte("v"), longTTL, "s"))
	require.NoError(t, c.cfg.Client.Del(opCtx(t), c.tokenKey("s")).Err())

	requireMiss(t, c, "k")
	require.NoError(t, c.SetTagged(opCtx(t), "k2", []byte("v2"), longTTL, "s"))
	requireMiss(t, c, "k")
	requireMiss(t, c, "unread")
	requireValue(t, c, "k2", "v2")
}

// 成员集合剪除过期成员，大小以存活条目为界，且集合键本身带过期。
// "存活成员保持集合"用一个长 TTL 条目让集合键不随短条目一起过期，只有登记时的剪除才能让集合回落。
func TestMemberSetBounded(t *testing.T) {
	for _, tc := range []struct {
		name      string
		keepalive bool
		maxCard   int64
	}{
		{"集合整体过期", false, 1},
		{"存活成员保持集合", true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := openCache(t, newPrefix(t))
			if tc.keepalive {
				require.NoError(t, c.SetTagged(opCtx(t), "keepalive", []byte("v"), longTTL, "s"))
			}
			for i := range 1000 {
				require.NoError(t, c.SetTagged(opCtx(t), fmt.Sprintf("k%d", i), []byte("v"), 50*time.Millisecond, "s"))
			}
			time.Sleep(200 * time.Millisecond)
			require.NoError(t, c.SetTagged(opCtx(t), "last", []byte("v"), 50*time.Millisecond, "s"))

			n, err := c.cfg.Client.ZCard(opCtx(t), c.memberKey("s")).Result()
			require.NoError(t, err)
			require.LessOrEqual(t, n, tc.maxCard)
			pttl, err := c.cfg.Client.PTTL(opCtx(t), c.memberKey("s")).Result()
			require.NoError(t, err)
			require.Positive(t, pttl)
		})
	}
}

// 成员集合丢失（模拟 maxmemory 淘汰 t:）时 DeleteTag 无法物理删除，但条目仍因令牌删除而不可读（P1 不依赖扫描）。
func TestMemberSetLossStillUnreadable(t *testing.T) {
	c := openCache(t, newPrefix(t))
	require.NoError(t, c.SetTagged(opCtx(t), "k", []byte("v"), longTTL, "s"))
	require.NoError(t, c.cfg.Client.Del(opCtx(t), c.memberKey("s")).Err())
	require.NoError(t, c.DeleteTag(opCtx(t), "s"))
	requireEntryExists(t, c, "k", true)
	requireMiss(t, c, "k")
}

// 键与标签含花括号时读写删正确，落在 Redis 中的键名不含花括号；转义不会让不同的键冲突。
func TestKeyEscaping(t *testing.T) {
	prefix := newPrefix(t)
	c := openCache(t, prefix)
	require.NoError(t, c.SetTagged(opCtx(t), "a{b}c", []byte("tagged"), longTTL, "{x}"))
	require.NoError(t, c.SetBytes(opCtx(t), "%7B", []byte("pct"), longTTL))
	require.NoError(t, c.SetBytes(opCtx(t), "{", []byte("brace"), longTTL))
	requireValue(t, c, "a{b}c", "tagged")
	requireValue(t, c, "%7B", "pct")
	requireValue(t, c, "{", "brace")

	keys, err := c.cfg.Client.Keys(opCtx(t), prefix+"*").Result()
	require.NoError(t, err)
	require.NotEmpty(t, keys)
	for _, k := range keys {
		require.NotContains(t, k, "{")
		require.NotContains(t, k, "}")
	}

	require.NoError(t, c.DeleteTag(opCtx(t), "{x}"))
	requireEntryExists(t, c, "a{b}c", false)
	requireMiss(t, c, "a{b}c")
	require.NoError(t, c.Delete(opCtx(t), "{"))
	requireMiss(t, c, "{")
	requireValue(t, c, "%7B", "pct")
}

// ttl <= 0 返回 CodeInvalidInput，不写入任何键（否则会成为永不过期的键）。
func TestInvalidTTL(t *testing.T) {
	c := openCache(t, newPrefix(t))
	before, err := c.cfg.Client.DBSize(opCtx(t)).Result()
	require.NoError(t, err)
	for _, ttl := range []time.Duration{0, -time.Second} {
		err := c.SetBytes(opCtx(t), "k", []byte("v"), ttl)
		require.Truef(t, agentErrors.IsCode(err, agentErrors.CodeInvalidInput), "SetBytes ttl=%v: %v", ttl, err)
		err = c.SetTagged(opCtx(t), "k", []byte("v"), ttl, "s")
		require.Truef(t, agentErrors.IsCode(err, agentErrors.CodeInvalidInput), "SetTagged ttl=%v: %v", ttl, err)
	}
	after, err := c.cfg.Client.DBSize(opCtx(t)).Result()
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestPrefixIsolation(t *testing.T) {
	a, b := openCache(t, newPrefix(t)), openCache(t, newPrefix(t))
	require.NoError(t, a.SetTagged(opCtx(t), "k", []byte("a"), longTTL, "s"))
	require.NoError(t, b.SetTagged(opCtx(t), "k", []byte("b"), longTTL, "s"))
	require.NoError(t, a.DeleteTag(opCtx(t), "s"))
	requireMiss(t, a, "k")
	requireValue(t, b, "k", "b")
	require.NoError(t, b.Delete(opCtx(t), "k"))
	requireMiss(t, b, "k")
}

// 条目格式无法解码（未知版本或损坏）按未命中处理，并删除该条目。
func TestCorruptEntryDeleted(t *testing.T) {
	c := openCache(t, newPrefix(t))
	for name, raw := range map[string]string{"未知版本": "\x09abc", "截断": "\x01\x00\x00\x00\x02"} {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, c.cfg.Client.Set(opCtx(t), c.entryKey("k"), raw, longTTL).Err())
			requireMiss(t, c, "k")
			requireEntryExists(t, c, "k", false)
		})
	}
}

// 多个标签任一被删除，条目即不可读；成员登记与令牌对每个标签都生效。
func TestMultiTagEachDeletes(t *testing.T) {
	c := openCache(t, newPrefix(t))
	for _, tag := range []string{"a", "b", "c"} {
		require.NoError(t, c.SetTagged(opCtx(t), "k", []byte("v"), longTTL, "a", "b", "c", "b"))
		requireValue(t, c, "k", "v")
		require.NoError(t, c.DeleteTag(opCtx(t), tag))
		requireEntryExists(t, c, "k", false)
		requireMiss(t, c, "k")
	}
}

// DeleteTag 跨多批扫描时全部条目被物理删除。
func TestDeleteTagBatches(t *testing.T) {
	prefix := newPrefix(t)
	c, err := New(context.Background(), Config{Client: testClient(t), Prefix: prefix, DeleteBatch: 7})
	require.NoError(t, err)
	const n = 300
	for i := range n {
		require.NoError(t, c.SetTagged(opCtx(t), fmt.Sprintf("k%d", i), []byte("v"), longTTL, "s"))
	}
	var batches int
	setHook(t, &hookBetweenRemoveAndUnlink, func() { batches++ })
	require.NoError(t, c.DeleteTag(opCtx(t), "s"))
	require.Greater(t, batches, 1)
	keys, err := c.cfg.Client.Keys(opCtx(t), prefix+"e:*").Result()
	require.NoError(t, err)
	require.Empty(t, keys, strings.Join(keys, ","))
	exists, err := c.cfg.Client.Exists(opCtx(t), c.memberKey("s")).Result()
	require.NoError(t, err)
	require.Zero(t, exists)
}
