package repo

import (
	"context"
	"sort"
	"sync"
)

// shardError 是分片后端“原生”的错误类型。
// 它故意与 *Error 完全不同，用来证明后端自身的错误类型不会透出给调用方：
// 仓储边界会把它们全部归一化。
type shardError struct {
	code int
	msg  string
}

const (
	shardCodeMissing = iota + 1
	shardCodeExists
	shardCodeCas
)

func (e *shardError) Error() string { return "sharded-backend: " + e.msg }

// ShardedBackend 是第二个参考后端实现：数据按 ID 哈希分散到多个分片，
// 每个分片独立加锁（没有 MemoryBackend 那样的单一全局锁）。
// 它说明只要满足 Backend 契约（含 Apply 原子性、确定排序、错误归一化），
// 内部实现可以完全不同，而对外读写与分页语义保持不变。
type ShardedBackend struct {
	shards []*shard
	n      uint32
}

type shard struct {
	mu      sync.Mutex
	records map[string]Record
}

// NewShardedBackend 创建给定分片数的分片后端。
func NewShardedBackend(shardCount int) *ShardedBackend {
	if shardCount <= 0 {
		shardCount = 16
	}
	b := &ShardedBackend{n: uint32(shardCount)}
	b.shards = make([]*shard, shardCount)
	for i := range b.shards {
		b.shards[i] = &shard{records: make(map[string]Record)}
	}
	return b
}

// fnv 是一个极小的稳定哈希，避免引入额外依赖。
func fnv(s string) uint32 {
	const offset, prime = uint32(2166136261), uint32(16777619)
	h := offset
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime
	}
	return h
}

func (b *ShardedBackend) shardFor(id string) *shard {
	return b.shards[fnv(id)%b.n]
}

// Get 读取记录，并把后端原生错误归一化为仓储错误。
func (b *ShardedBackend) Get(_ context.Context, id string) (Record, error) {
	if id == "" {
		return Record{}, ContractViolation("backend.get", "record id is empty")
	}
	sh := b.shardFor(id)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	rec, ok := sh.records[id]
	if !ok {
		return Record{}, normalizeShardErr(&shardError{code: shardCodeMissing, msg: "no such id: " + id})
	}
	return cloneRecord(rec), nil
}

// normalizeShardErr 把分片后端的原生错误码映射为统一的 Kind。
// 注意：原生 *shardError 不会被包装进返回值（既不保留为 Cause，也不复制其
// 文本），因为后端实现细节不允许透出仓储边界；调用方只能观察到 *Error 与 Kind。
func normalizeShardErr(err *shardError) error {
	switch err.code {
	case shardCodeMissing:
		return NotFound("backend.apply", "record not found")
	case shardCodeExists:
		return Conflict("backend.apply", "record already exists")
	case shardCodeCas:
		return Conflict("backend.apply", "optimistic lock check failed (version changed)")
	default:
		return newError(KindOther, "backend.apply", "backend failure", nil)
	}
}

// Apply 原子应用整组变更。
// 为保证跨分片原子性，按分片 ID 顺序加锁（避免死锁），在持有全部相关
// 分片锁的情况下完成“先全部校验、再全部落盘”。
func (b *ShardedBackend) Apply(_ context.Context, mutations []Mutation) error {
	if len(mutations) == 0 {
		return ContractViolation("backend.apply", "empty mutation batch")
	}

	// 确定涉及哪些分片，并按分片索引排序后统一加锁。
	touched := make(map[uint32]struct{})
	for _, m := range mutations {
		if m.Record.ID == "" {
			return ContractViolation("backend.apply", "mutation has empty record id")
		}
		touched[fnv(m.Record.ID)%b.n] = struct{}{}
	}
	order := make([]uint32, 0, len(touched))
	for idx := range touched {
		order = append(order, idx)
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	for _, idx := range order {
		b.shards[idx].mu.Lock()
	}
	defer func() {
		for _, idx := range order {
			b.shards[idx].mu.Unlock()
		}
	}()

	// 第一阶段：校验整组前置条件。
	seen := make(map[string]struct{}, len(mutations))
	for _, m := range mutations {
		if _, dup := seen[m.Record.ID]; dup {
			return ContractViolation("backend.apply", "multiple mutations touch the same record: "+m.Record.ID)
		}
		seen[m.Record.ID] = struct{}{}

		sh := b.shardFor(m.Record.ID)
		cur, exists := sh.records[m.Record.ID]
		switch m.Op {
		case OpCreate:
			if exists {
				return normalizeShardErr(&shardError{code: shardCodeExists, msg: "id taken: " + m.Record.ID})
			}
		case OpPut:
			if !exists {
				return normalizeShardErr(&shardError{code: shardCodeMissing, msg: "no such id: " + m.Record.ID})
			}
			if cur.Version != m.ExpectVersion {
				return normalizeShardErr(&shardError{code: shardCodeCas, msg: m.Record.ID})
			}
		case OpDelete:
			if !exists {
				return normalizeShardErr(&shardError{code: shardCodeMissing, msg: "no such id: " + m.Record.ID})
			}
			if cur.Version != m.ExpectVersion {
				return normalizeShardErr(&shardError{code: shardCodeCas, msg: m.Record.ID})
			}
		default:
			return ContractViolation("backend.apply", "unknown mutation op")
		}
	}

	// 第二阶段：全部落盘。
	for _, m := range mutations {
		sh := b.shardFor(m.Record.ID)
		switch m.Op {
		case OpCreate:
			rec := m.Record
			rec.Version = 1
			sh.records[rec.ID] = cloneRecord(rec)
		case OpPut:
			rec := m.Record
			rec.Version = m.ExpectVersion + 1
			sh.records[rec.ID] = cloneRecord(rec)
		case OpDelete:
			delete(sh.records, m.Record.ID)
		}
	}
	return nil
}

// List 聚合所有分片后按 (SortKey, ID) 排序，语义与 MemoryBackend 完全一致。
func (b *ShardedBackend) List(_ context.Context, scan Scan) ([]Record, error) {
	if scan.Limit <= 0 {
		return nil, ContractViolation("backend.list", "limit must be positive")
	}
	// 快照式读取：逐分片持锁拷贝候选记录后立即释放，避免长时间占锁。
	out := make([]Record, 0)
	for _, sh := range b.shards {
		sh.mu.Lock()
		for _, rec := range sh.records {
			if afterPosition(rec, scan.After) {
				out = append(out, cloneRecord(rec))
			}
		}
		sh.mu.Unlock()
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SortKey != out[j].SortKey {
			return out[i].SortKey < out[j].SortKey
		}
		return out[i].ID < out[j].ID
	})
	if len(out) > scan.Limit {
		out = out[:scan.Limit]
	}
	return out, nil
}
