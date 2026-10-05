package repository

import (
	"context"
	"errors"
)

// Export 按 Query 的筛选条件做可续传的键集分页导出。
//
// 游标是不透明字符串，可持久化或跨进程传递：进程重启或换一个调用点，
// 只要传入同一 Query 与上次返回的 NextCursor，就能接着往下读，
// 已返回的记录不重复、应返回的记录不遗漏。
//
// 游标只在生成它的那份筛选与排序条件下有效；来自其他查询、旧格式、
// 被截断、被篡改或结构不完整的游标一律返回 KindInvalidCursor。
// 底层通过键集扫描逐块读取，数据量远大于一页时也不会一次性拉取全部记录。
func (r *Repository) Export(ctx context.Context, q Query, limit int, cursorStr string) (Page, error) {
	if limit <= 0 {
		return Page{}, &Error{Kind: KindContractViolation, Op: "Export",
			Err: errors.New("limit must be positive")}
	}
	var from ScanPosition
	if cursorStr != "" {
		pos, err := decodeExportCursor(cursorStr, q)
		if err != nil {
			return Page{}, err
		}
		from = pos
	}
	items, last, exhausted, err := r.scanPage(ctx, from, q.matches, limit)
	if err != nil {
		return Page{}, err
	}
	page := Page{Items: items}
	if !exhausted {
		page.NextCursor = encodeExportCursor(q, last)
	}
	return page, nil
}
