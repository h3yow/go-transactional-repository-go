package repository

import (
	"context"
	"errors"
	"sort"
	"sync"
)

// chunkedStore 是单次 Scan 返回条数有上限的存储后端，
// 用于验证分页实现不依赖一次性取出全部数据。
// 事务与版本语义与 memoryStore 完全一致。
type chunkedStore struct {
	mu      sync.RWMutex
	data    map[string]Record
	maxScan int // 单次 Scan 最多返回的记录条数
}

// NewChunkedStore 创建一个单次读取条数上限为 maxScan 的存储后端（<1 时按 1 处理）。
func NewChunkedStore(maxScan int) Store {
	if maxScan < 1 {
		maxScan = 1
	}
	return &chunkedStore{data: make(map[string]Record), maxScan: maxScan}
}

func (c *chunkedStore) Get(_ context.Context, key string) (Record, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	rec, ok := c.data[key]
	if !ok {
		return Record{}, &Error{Kind: KindNotFound, Op: "chunked.Get",
			Err: errors.New("record does not exist")}
	}
	return rec, nil
}

// Scan 与 memoryStore 语义相同，但单次最多返回 maxScan 条，
// 即使调用方要求更多（或不限）也不会一口气交出全部数据。
func (c *chunkedStore) Scan(_ context.Context, after string, limit int) ([]Record, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]string, 0, len(c.data))
	for k := range c.data {
		if k > after {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	n := c.maxScan
	if limit >= 0 && limit < n {
		n = limit
	}
	if len(keys) > n {
		keys = keys[:n]
	}
	out := make([]Record, 0, len(keys))
	for _, k := range keys {
		out = append(out, c.data[k])
	}
	return out, nil
}

// Commit 先校验全部 Mutation 的版本前提，任一不满足即整体拒绝，
// 保证不会产生部分写入；全部满足后才统一应用。
func (c *chunkedStore) Commit(_ context.Context, mutations []Mutation) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, mu := range mutations {
		if mu.Key == "" {
			return &Error{Kind: KindContractViolation, Op: "chunked.Commit",
				Err: errors.New("mutation key must not be empty")}
		}
		cur, exists := c.data[mu.Key]
		var curVersion uint64
		if exists {
			curVersion = cur.Version
		}
		if curVersion != mu.ExpectedVersion {
			return &Error{Kind: KindConflict, Op: "chunked.Commit",
				Err: errors.New("version mismatch on key " + mu.Key)}
		}
	}
	for _, mu := range mutations {
		if mu.Delete {
			delete(c.data, mu.Key)
			continue
		}
		c.data[mu.Key] = Record{
			Key:     mu.Key,
			Value:   mu.Value,
			Version: mu.ExpectedVersion + 1,
		}
	}
	return nil
}
