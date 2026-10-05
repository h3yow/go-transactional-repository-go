package repository

import "context"

// limitedStore 是一个单次读取条数受限的存储后端，
// 模拟带最大页大小的远程存储：无论调用方请求多少条，
// 单次 ScanSorted 最多返回 maxPerRead 条。
// 仓储层必须通过键集续扫翻完剩余数据，不能依赖一次性全量读取。
type limitedStore struct {
	inner      Store
	maxPerRead int
}

// NewLimitedStore 创建一个单次读取最多返回 maxPerRead 条的后端。
// maxPerRead 小于 1 时按 1 处理。
func NewLimitedStore(maxPerRead int) Store {
	if maxPerRead < 1 {
		maxPerRead = 1
	}
	return &limitedStore{inner: NewMemoryStore(), maxPerRead: maxPerRead}
}

func (l *limitedStore) Get(ctx context.Context, key string) (Record, error) {
	return l.inner.Get(ctx, key)
}

func (l *limitedStore) ScanSorted(ctx context.Context, after ScanPosition, limit int) ([]Record, error) {
	if limit > l.maxPerRead {
		limit = l.maxPerRead
	}
	return l.inner.ScanSorted(ctx, after, limit)
}

func (l *limitedStore) Commit(ctx context.Context, mutations []Mutation) error {
	return l.inner.Commit(ctx, mutations)
}
