package redis

import (
	"encoding/binary"
	"fmt"
)

// 条目编码（所有整数为大端）：
//
//	版本号     1 字节，当前为 entryVersion
//	标签数 n   uint32
//	n 次重复： 标签长度 uint32 | 标签字节 | 令牌长度 uint32 | 令牌字节
//	值         剩余全部字节
//
// 无标签条目的 n 为 0。读取时遇到未知版本或格式错误，按未命中处理并删除条目。
const entryVersion byte = 1

type entry struct {
	tags   []string
	tokens []string
	value  []byte
}

func encodeEntry(tags, tokens []string, value []byte) []byte {
	size := 1 + 4 + len(value)
	for i := range tags {
		size += 8 + len(tags[i]) + len(tokens[i])
	}
	buf := make([]byte, 0, size)
	buf = append(buf, entryVersion)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(tags)))
	for i := range tags {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(tags[i])))
		buf = append(buf, tags[i]...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(tokens[i])))
		buf = append(buf, tokens[i]...)
	}
	return append(buf, value...)
}

func decodeEntry(raw []byte) (entry, error) {
	if len(raw) < 5 {
		return entry{}, fmt.Errorf("条目长度 %d 不足", len(raw))
	}
	if raw[0] != entryVersion {
		return entry{}, fmt.Errorf("未知的条目版本 %d", raw[0])
	}
	n := binary.BigEndian.Uint32(raw[1:5])
	rest := raw[5:]
	// 每个标签至少占 8 字节，先据此拒绝明显错误的标签数，避免按它分配内存。
	if uint64(n)*8 > uint64(len(rest)) {
		return entry{}, fmt.Errorf("条目标签数 %d 超出数据长度", n)
	}
	e := entry{tags: make([]string, n), tokens: make([]string, n)}
	var err error
	for i := range n {
		if e.tags[i], rest, err = readField(rest); err != nil {
			return entry{}, err
		}
		if e.tokens[i], rest, err = readField(rest); err != nil {
			return entry{}, err
		}
	}
	e.value = rest
	return e, nil
}

// readField 读取一个"uint32 长度 + 字节"字段。
func readField(b []byte) (string, []byte, error) {
	if len(b) < 4 {
		return "", nil, fmt.Errorf("条目字段长度截断")
	}
	n := binary.BigEndian.Uint32(b)
	b = b[4:]
	if uint64(n) > uint64(len(b)) {
		return "", nil, fmt.Errorf("条目字段截断：需要 %d 字节，剩余 %d", n, len(b))
	}
	return string(b[:n]), b[n:], nil
}
