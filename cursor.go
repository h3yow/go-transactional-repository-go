package repository

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
)

// cursor 是分页游标的内部表示：以上一条已返回记录的
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

// decodeCursor 解码游标；非法、损坏或来自其他查询（如导出游标）
// 的游标返回 KindInvalidCursor 错误。
func decodeCursor(s string) (cursor, error) {
	invalid := &Error{Kind: KindInvalidCursor, Op: "decodeCursor",
		Err: errors.New("malformed or expired cursor")}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return cursor{}, invalid
	}
	// 严格解码：结构不符（包括导出游标等多字段格式）一律拒绝。
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var c cursor
	if err := dec.Decode(&c); err != nil {
		return cursor{}, invalid
	}
	if c.Key == "" {
		return cursor{}, invalid
	}
	return c, nil
}
