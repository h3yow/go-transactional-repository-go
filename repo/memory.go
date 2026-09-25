package repo

import (
	"context"
	"sort"
	"sync"
)

// MemoryBackend 是基于内存 map 的参考存储后端。
// 它用单一互斥锁串行化所有写入，保证 Apply 的整组变更在同一临界区内
// 完成“先校验、后全部落盘”，从而实现原子提交与乐观锁判定。
type MemoryBackend struct {
	mu      sync.Mutex
	records map[string]Record
}

// NewMemoryBackend 创建一个空的内存后端。
func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{records: make(map[string]Record)}
}

func cloneValue(v []byte) []byte {
	if v == nil {
		return nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out
}

func cloneRecord(r Record) Record {
	r.Value = cloneValue(r.Value)
	return r
}

// Get 读取单条记录；记录不存在时返回 KindNotFound。
func (b *MemoryBackend) Get(_ context.Context, id string) (Record, error) {
	if id == "" {
		return Record{}, ContractViolation("backend.get", "record id is empty")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	rec, ok := b.records[id]
	if !ok {
		return Record{}, NotFound("backend.get", "record not found: "+id)
	}
	return cloneRecord(rec), nil
}

// validateBatch 校验整组变更的全部前置条件。
// 只有所有条件都通过时调用方才会落盘，因此失败时不会产生任何部分写入。
func (b *MemoryBackend) validateBatch(mutations []Mutation) error {
	seen := make(map[string]struct{}, len(mutations))
	for _, m := range mutations {
		id := m.Record.ID
		if id == "" {
			return ContractViolation("backend.apply", "mutation has empty record id")
		}
		if _, dup := seen[id]; dup {
			return ContractViolation("backend.apply", "multiple mutations touch the same record in one transaction: "+id)
		}
		seen[id] = struct{}{}

		cur, exists := b.records[id]
		switch m.Op {
		case OpCreate:
			if exists {
				return Conflict("backend.apply", "record already exists: "+id)
			}
		case OpPut:
			if !exists {
				return NotFound("backend.apply", "record not found for update: "+id)
			}
			if cur.Version != m.ExpectVersion {
				return Conflict("backend.apply", "version mismatch on update: "+id)
			}
		case OpDelete:
			if !exists {
				return NotFound("backend.apply", "record not found for delete: "+id)
			}
			if cur.Version != m.ExpectVersion {
				return Conflict("backend.apply", "version mismatch on delete: "+id)
			}
		default:
			return ContractViolation("backend.apply", "unknown mutation op")
		}
	}
	return nil
}

// Apply 在同一临界区内原子地应用整组变更。
func (b *MemoryBackend) Apply(_ context.Context, mutations []Mutation) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.validateBatch(mutations); err != nil {
		return err
	}
	for _, m := range mutations {
		switch m.Op {
		case OpCreate:
			rec := m.Record
			rec.Version = 1
			b.records[rec.ID] = cloneRecord(rec)
		case OpPut:
			rec := m.Record
			rec.Version = m.ExpectVersion + 1
			b.records[rec.ID] = cloneRecord(rec)
		case OpDelete:
			delete(b.records, m.Record.ID)
		}
	}
	return nil
}

// afterPosition 判断记录 r 是否严格位于位置 p 之后（按 SortKey, ID 升序）。
func afterPosition(r Record, p Position) bool {
	if p.ID == "" && p.SortKey == "" {
		return true
	}
	if r.SortKey != p.SortKey {
		return r.SortKey > p.SortKey
	}
	return r.ID > p.ID
}

// List 返回严格位于 After 之后、按 (SortKey, ID) 升序排列的记录，最多 Limit 条。
func (b *MemoryBackend) List(_ context.Context, scan Scan) ([]Record, error) {
	if scan.Limit <= 0 {
		return nil, ContractViolation("backend.list", "limit must be positive")
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	out := make([]Record, 0, scan.Limit)
	for _, rec := range b.records {
		if afterPosition(rec, scan.After) {
			out = append(out, cloneRecord(rec))
		}
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
