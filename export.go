package repository

import (
	"container/heap"
	"context"
	"errors"
	"sort"
	"strings"
)

// scanChunkSize 是每次向后端请求的记录条数上限：
// 分页实现按块循环拉取，不依赖后端一次性返回全部数据。
const scanChunkSize = 128

// ExportSpec 描述一次可续传导出的筛选与排序条件。
// 游标只在签发它的那份 Spec（筛选 + 排序）下有效。
type ExportSpec struct {
	SortKeyPrefix string // 筛选：只导出 SortKey 带该前缀的记录；空表示全部
	Limit         int    // 每页最大条数
}

// compareItems 按 (SortKey, ID) 字典序比较，排序键重复时以主键决胜。
func compareItems(a, b Item) int {
	if a.SortKey != b.SortKey {
		if a.SortKey < b.SortKey {
			return -1
		}
		return 1
	}
	if a.ID != b.ID {
		if a.ID < b.ID {
			return -1
		}
		return 1
	}
	return 0
}

// itemMaxHeap 是按 (SortKey, ID) 排序的最大堆，容量受页大小约束，
// 用于在分块扫描过程中只保留最小的一批候选记录，
// 使内存占用与数据集总规模无关。
type itemMaxHeap []Item

func (h itemMaxHeap) Len() int           { return len(h) }
func (h itemMaxHeap) Less(i, j int) bool { return compareItems(h[i], h[j]) > 0 }
func (h itemMaxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *itemMaxHeap) Push(x any)        { *h = append(*h, x.(Item)) }
func (h *itemMaxHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// Export 按 (SortKey, ID) 的稳定顺序导出一页符合条件的记录。
// cursorStr 为空表示从头开始；返回的 Page.NextCursor 是不透明字符串，
// 可跨进程、跨调用点续传，且只在相同 Spec 下有效。
// 实现按块循环扫描后端并用容量为 Limit+1 的有界堆筛选候选，
// 内存占用与数据集规模无关，不会一次性取出全部记录。
func (r *Repository) Export(ctx context.Context, spec ExportSpec, cursorStr string) (Page, error) {
	if spec.Limit <= 0 {
		return Page{}, &Error{Kind: KindContractViolation, Op: "Export",
			Err: errors.New("limit must be positive")}
	}
	var c cursor
	hasCursor := cursorStr != ""
	if hasCursor {
		var err error
		c, err = decodeExportCursor(spec, cursorStr)
		if err != nil {
			return Page{}, err
		}
	}
	h := &itemMaxHeap{}
	cap := spec.Limit + 1 // 多留一条用于判断是否还有下一页
	after := ""
	for {
		recs, err := r.store.Scan(ctx, after, scanChunkSize)
		if err != nil {
			return Page{}, normalize("Export", err)
		}
		if len(recs) == 0 {
			break
		}
		for _, rec := range recs {
			item, err := decodeRecord(rec)
			if err != nil {
				return Page{}, err
			}
			if spec.SortKeyPrefix != "" && !strings.HasPrefix(item.SortKey, spec.SortKeyPrefix) {
				continue
			}
			// 键集过滤：只保留严格位于游标之后的记录。
			if hasCursor && compareItems(item, Item{SortKey: c.SortKey, ID: c.Key}) <= 0 {
				continue
			}
			if h.Len() < cap {
				heap.Push(h, item)
			} else if compareItems(item, (*h)[0]) < 0 {
				heap.Pop(h)
				heap.Push(h, item)
			}
		}
		after = recs[len(recs)-1].Key
		// 注意：后端单次返回条数可能有上限（如 chunkedStore），
		// 不能以“返回数少于请求数”判断数据耗尽，只能以空页为准。
	}
	items := make([]Item, 0, h.Len())
	for h.Len() > 0 {
		items = append(items, heap.Pop(h).(Item))
	}
	sort.Slice(items, func(i, j int) bool { return compareItems(items[i], items[j]) < 0 })
	page := Page{}
	if len(items) > spec.Limit {
		page.Items = items[:spec.Limit]
		last := page.Items[len(page.Items)-1]
		page.NextCursor = encodeExportCursor(spec, cursor{SortKey: last.SortKey, Key: last.ID})
	} else {
		page.Items = items
	}
	return page, nil
}
