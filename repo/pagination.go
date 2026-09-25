package repo

import (
	"encoding/base64"
	"encoding/json"
	"strings"
)

// cursorPrefix 标识游标格式版本，使未来的格式演进可以被明确区分。
const cursorPrefix = "v1."

// cursorPayload 是游标的内部内容：上一页最后一条记录的排序位置。
// 采用键集（keyset）分页而非偏移量：翻页期间其他位置发生插入/删除时，
// 只要游标指向的位置语义不变，就不会重复或遗漏记录。
type cursorPayload struct {
	SortKey string `json:"k"`
	ID      string `json:"id"`
}

// encodeCursor 把排序位置编码为对外暴露的不透明字符串。
func encodeCursor(p cursorPayload) string {
	raw, _ := json.Marshal(p)
	return cursorPrefix + base64.RawURLEncoding.EncodeToString(raw)
}

// decodeCursor 解析游标。任何前缀不符、base64 损坏、JSON 非法或缺字段的情况
// 都归一化为 KindInvalidCursor，绝不返回猜测出来的位置。
func decodeCursor(token string) (cursorPayload, error) {
	const op = "repo.decode_cursor"
	if token == "" {
		return cursorPayload{}, nil
	}
	if !strings.HasPrefix(token, cursorPrefix) {
		return cursorPayload{}, InvalidCursor(op, "cursor has unknown or missing version prefix")
	}
	body := strings.TrimPrefix(token, cursorPrefix)
	raw, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return cursorPayload{}, InvalidCursor(op, "cursor payload is not valid base64url")
	}
	var p cursorPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return cursorPayload{}, InvalidCursor(op, "cursor payload is not valid json")
	}
	if p.ID == "" {
		return cursorPayload{}, InvalidCursor(op, "cursor payload misses record id")
	}
	return p, nil
}
