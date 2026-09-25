package repository

import (
	"context"
	"errors"
	"sort"
	"sync"
)

// Record 是存储层的基本记录，Version 用于乐观锁。
type Record struct {
	Key     string
	Value   []byte
	Version uint64
}

// Mutation 描述一次原子提交中的单个写操作。
type Mutation struct {
	Key             string
	Value           []byte // nil 表示删除
	ExpectedVersion uint64 // 期望的当前版本；0 表示期望记录不存在（创建）
	Delete          bool
}

// Store 是可替换存储后端的契约。
// 实现必须保证：Commit 中的多个 Mutation 要么全部生效要么全部不生效；
// 版本不匹配时必须返回 KindConflict 错误且不产生任何部分写入。
type Store interface {
	Get(ctx context.Context, key string) (Record, error)
	// Scan 返回 key 严格大于 after 的最多 limit 条记录，按 key 升序。
	Scan(ctx context.Context, after string, limit int) ([]Record, error)
	Commit(ctx context.Context, mutations []Mutation) error
}

// memoryStore 是基于内存的 Store 参考实现。
type memoryStore struct {
	mu   sync.RWMutex
	data map[string]Record
}

// NewMemoryStore 创建一个空的内存存储后端。
func NewMemoryStore() Store {
	return &memoryStore{data: make(map[string]Record)}
}

func (m *memoryStore) Get(_ context.Context, key string) (Record, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rec, ok := m.data[key]
	if !ok {
		return Record{}, &Error{Kind: KindNotFound, Op: "store.Get",
			Err: errors.New("record does not exist")}
	}
	return rec, nil
}

func (m *memoryStore) Scan(_ context.Context, after string, limit int) ([]Record, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		if k > after {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if limit >= 0 && len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]Record, 0, len(keys))
	for _, k := range keys {
		out = append(out, m.data[k])
	}
	return out, nil
}

// Commit 先校验全部 Mutation 的版本前提，任一不满足即整体拒绝，
// 保证不会产生部分写入；全部满足后才统一应用。
func (m *memoryStore) Commit(_ context.Context, mutations []Mutation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, mu := range mutations {
		if mu.Key == "" {
			return &Error{Kind: KindContractViolation, Op: "store.Commit",
				Err: errors.New("mutation key must not be empty")}
		}
		cur, exists := m.data[mu.Key]
		var curVersion uint64
		if exists {
			curVersion = cur.Version
		}
		if curVersion != mu.ExpectedVersion {
			return &Error{Kind: KindConflict, Op: "store.Commit",
				Err: errors.New("version mismatch on key " + mu.Key)}
		}
	}
	for _, mu := range mutations {
		if mu.Delete {
			delete(m.data, mu.Key)
			continue
		}
		m.data[mu.Key] = Record{
			Key:     mu.Key,
			Value:   mu.Value,
			Version: mu.ExpectedVersion + 1,
		}
	}
	return nil
}
