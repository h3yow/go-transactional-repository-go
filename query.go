package repository

import (
	"hash/fnv"
	"strconv"
	"strings"
)

// Query 描述一次导出的筛选与排序条件。
// 排序固定为 (SortKey, ID) 升序；筛选当前支持排序键前缀。
// 游标绑定 Query 的指纹：任何字段变化都会使旧游标失效。
type Query struct {
	// SortKeyPrefix 只导出 SortKey 以此前缀开头的记录；空串表示不过滤。
	SortKeyPrefix string
}

// fingerprint 计算查询条件的稳定指纹，写入游标以绑定筛选与排序。
// 指纹只依赖 Query 内容与固定的排序规格，跨进程、跨调用点稳定。
func (q Query) fingerprint() string {
	h := fnv.New64a()
	h.Write([]byte("sort=sk,id,asc"))
	h.Write([]byte{0})
	h.Write([]byte("prefix=" + q.SortKeyPrefix))
	return strconv.FormatUint(h.Sum64(), 16)
}

// matches 报告记录是否满足筛选条件。
func (q Query) matches(it Item) bool {
	return q.SortKeyPrefix == "" || strings.HasPrefix(it.SortKey, q.SortKeyPrefix)
}
