package repository

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
)

// exportCursorVersion 是当前导出游标格式版本；旧格式（无版本号）一律拒绝。
const exportCursorVersion = 2

// exportCursor 是可续传导出游标的内部表示（v2）：
// 绑定查询指纹并带完整性校验，篡改、截断、跨查询使用都会被拒绝。
type exportCursor struct {
	V       int    `json:"v"`   // 格式版本
	FP      string `json:"fp"`  // 查询（筛选+排序）指纹
	SortKey string `json:"s"`   // 上一条已返回记录的排序键
	Key     string `json:"k"`   // 上一条已返回记录的主键
	Sum     string `json:"sum"` // 完整性校验和
}

// exportFingerprint 计算导出查询（筛选 + 排序）的指纹：
// 只有与签发时完全相同的筛选和排序条件才能继续使用该游标。
func exportFingerprint(spec ExportSpec) string {
	h := fnv.New64a()
	fmt.Fprintf(h, "export|prefix=%q|sort=sortKey,id,asc", spec.SortKeyPrefix)
	return fmt.Sprintf("%016x", h.Sum64())
}

// exportChecksum 计算游标内容的完整性校验和（防意外损坏/篡改，非安全签名）。
func exportChecksum(c exportCursor) string {
	h := fnv.New64a()
	fmt.Fprintf(h, "export-cursor|%d|%s|%q|%q", c.V, c.FP, c.SortKey, c.Key)
	return fmt.Sprintf("%016x", h.Sum64())
}

// encodeExportCursor 将导出位置编码为绑定 spec 的不透明字符串。
func encodeExportCursor(spec ExportSpec, c cursor) string {
	ec := exportCursor{
		V:       exportCursorVersion,
		FP:      exportFingerprint(spec),
		SortKey: c.SortKey,
		Key:     c.Key,
	}
	ec.Sum = exportChecksum(ec)
	raw, _ := json.Marshal(ec)
	return base64.RawURLEncoding.EncodeToString(raw)
}

// decodeExportCursor 解码并严格校验导出游标；
// 旧格式、被篡改、被截断、结构不完整或来自其他查询的游标
// 一律返回 KindInvalidCursor。
func decodeExportCursor(spec ExportSpec, s string) (cursor, error) {
	invalid := &Error{Kind: KindInvalidCursor, Op: "decodeExportCursor",
		Err: errors.New("malformed, foreign or expired export cursor")}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return cursor{}, invalid
	}
	// 严格解码：多余或缺失字段都视为结构不完整。
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var ec exportCursor
	if err := dec.Decode(&ec); err != nil {
		return cursor{}, invalid
	}
	if ec.V != exportCursorVersion || ec.FP == "" || ec.Key == "" || ec.Sum == "" {
		return cursor{}, invalid // 旧格式、未知版本或结构不完整
	}
	if ec.Sum != exportChecksum(ec) {
		return cursor{}, invalid // 内容被篡改或截断后重编码
	}
	if ec.FP != exportFingerprint(spec) {
		return cursor{}, invalid // 来自其他筛选/排序条件的查询
	}
	return cursor{SortKey: ec.SortKey, Key: ec.Key}, nil
}
