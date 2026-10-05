package repository

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"hash/fnv"
)

// cursor 是 List 分页游标的内部表示（v1 格式）：以上一条已返回记录的
// (排序键, 主键) 作为继续读取的位置，保证排序稳定且确定。
type cursor struct {
	SortKey string `json:"s"`
	Key     string `json:"k"`
}

// encodeCursor 将游标编码为不透明字符串。
func encodeCursor(c cursor) string {
	raw, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(raw)
}

// decodeCursor 解码游标；非法或损坏的游标返回 KindInvalidCursor 错误。
func decodeCursor(s string) (cursor, error) {
	invalid := &Error{Kind: KindInvalidCursor, Op: "decodeCursor",
		Err: errors.New("malformed or expired cursor")}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return cursor{}, invalid
	}
	var c cursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return cursor{}, invalid
	}
	if c.Key == "" {
		return cursor{}, invalid
	}
	return c, nil
}

// exportCursorVersion 是导出游标的格式版本。
// 旧格式（无版本号的 v1 游标）解码后 V 为 0，会被明确拒绝。
const exportCursorVersion = 2

// exportCursor 是可续传导出游标的内部表示（v2 格式）。
// 除位置外还绑定查询指纹与校验和：来自其他筛选/排序条件的游标、
// 被篡改或结构不完整的游标都必须被明确拒绝。
type exportCursor struct {
	V int    `json:"v"` // 格式版本，必须等于 exportCursorVersion
	Q string `json:"q"` // 查询（筛选+排序）指纹
	S string `json:"s"` // 最后扫描位置的 SortKey
	K string `json:"k"` // 最后扫描位置的 Key
	C uint32 `json:"c"` // 对 Q/S/K 的校验和，用于发现篡改与截断
}

// exportCursorChecksum 计算游标内容的校验和。
// 该校验和用于发现损坏与篡改，不是密码学签名。
func exportCursorChecksum(q, s, k string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(q))
	h.Write([]byte{0})
	h.Write([]byte(s))
	h.Write([]byte{0})
	h.Write([]byte(k))
	return h.Sum32()
}

// encodeExportCursor 将查询 q 下位置 pos 的导出游标编码为不透明字符串。
func encodeExportCursor(q Query, pos ScanPosition) string {
	fp := q.fingerprint()
	c := exportCursor{
		V: exportCursorVersion,
		Q: fp,
		S: pos.SortKey,
		K: pos.Key,
		C: exportCursorChecksum(fp, pos.SortKey, pos.Key),
	}
	raw, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(raw)
}

// decodeExportCursor 解码并校验导出游标。以下情况一律返回
// KindInvalidCursor：无法解码、版本不符（含 v1 旧格式）、结构不完整
// （缺少指纹或位置）、校验和不匹配（被篡改或截断）、指纹与当前查询
// 不一致（来自其他筛选/排序条件的游标）。
func decodeExportCursor(s string, q Query) (ScanPosition, error) {
	invalid := &Error{Kind: KindInvalidCursor, Op: "decodeExportCursor",
		Err: errors.New("malformed, foreign or expired cursor")}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return ScanPosition{}, invalid
	}
	var c exportCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return ScanPosition{}, invalid
	}
	if c.V != exportCursorVersion {
		return ScanPosition{}, invalid
	}
	if c.Q == "" || c.K == "" {
		return ScanPosition{}, invalid
	}
	if c.Q != q.fingerprint() {
		return ScanPosition{}, invalid
	}
	if c.C != exportCursorChecksum(c.Q, c.S, c.K) {
		return ScanPosition{}, invalid
	}
	return ScanPosition{SortKey: c.S, Key: c.K}, nil
}
