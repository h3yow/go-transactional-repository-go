package repository

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Intent 表示批量写入中单个条目的写入意图。
type Intent int

const (
	// IntentCreate 只允许新建：目标必须不存在。
	IntentCreate Intent = iota + 1
	// IntentUpdate 只允许更新：目标必须存在且版本匹配。
	IntentUpdate
	// IntentUpsert 不存在则新建，存在则更新。
	IntentUpsert
)

func (i Intent) String() string {
	switch i {
	case IntentCreate:
		return "create"
	case IntentUpdate:
		return "update"
	case IntentUpsert:
		return "upsert"
	default:
		return "unknown"
	}
}

// BatchEntry 是批量写入的一个条目。
type BatchEntry struct {
	ID      string
	SortKey string
	Data    []byte
	Intent  Intent
	// Version 是期望的当前版本：
	// IntentCreate 必须为 0（隐含目标不存在）；
	// IntentUpdate 必须为目标当前版本（>=1）；
	// IntentUpsert 为 0 表示不做版本校验，>0 时若目标已存在则必须匹配。
	Version uint64
}

// EntryError 描述批次中单个条目的失败原因。
type EntryError struct {
	Index int    // 在批次中的下标
	ID    string // 条目主键
	Kind  Kind   // KindNotFound / KindConflict / KindContractViolation
	Err   error  // 具体原因
}

func (e EntryError) Error() string {
	return fmt.Sprintf("entry[%d] id=%q: %s: %v", e.Index, e.ID, e.Kind, e.Err)
}

// BatchError 是批量预检/提交失败的统一错误，一次性携带全部问题条目，
// 不会只报第一条，也不会把不同原因合并成笼统错误。
type BatchError struct {
	Op      string
	Entries []EntryError
}

func (e *BatchError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %d batch entr", e.Op, len(e.Entries))
	if len(e.Entries) == 1 {
		b.WriteString("y failed")
	} else {
		b.WriteString("ies failed")
	}
	for _, ee := range e.Entries {
		b.WriteString("; ")
		b.WriteString(ee.Error())
	}
	return b.String()
}

// BatchErrors 从 err 中提取条目级错误列表；err 不是批量错误时返回 nil。
func BatchErrors(err error) []EntryError {
	var be *BatchError
	if errors.As(err, &be) {
		return be.Entries
	}
	return nil
}

// validateEntryShape 校验条目的入参本身是否合法（不依赖存储状态）。
func validateEntryShape(index int, e BatchEntry, seen map[string]int) *EntryError {
	bad := func(msg string) *EntryError {
		return &EntryError{Index: index, ID: e.ID, Kind: KindContractViolation,
			Err: errors.New(msg)}
	}
	if e.ID == "" {
		return bad("id must not be empty")
	}
	if _, dup := seen[e.ID]; dup {
		return bad("duplicate id in batch")
	}
	switch e.Intent {
	case IntentCreate:
		if e.Version != 0 {
			return bad("create intent must not carry a version")
		}
	case IntentUpdate:
		if e.Version == 0 {
			return bad("update intent requires the current version (>= 1)")
		}
	case IntentUpsert:
		// Version 任意值均合法：0 表示不做版本校验。
	default:
		return bad("unknown intent: " + fmt.Sprint(int(e.Intent)))
	}
	return nil
}

// checkBatch 对整批条目做校验，返回全部问题条目；
// 全部通过时同时返回每个条目解析后的期望版本（供原子提交使用）。
func (r *Repository) checkBatch(ctx context.Context, entries []BatchEntry) ([]EntryError, []uint64) {
	var errs []EntryError
	resolved := make([]uint64, len(entries))
	seen := make(map[string]int, len(entries))
	for i, e := range entries {
		if shapeErr := validateEntryShape(i, e, seen); shapeErr != nil {
			errs = append(errs, *shapeErr)
			continue
		}
		seen[e.ID] = i
		var exists bool
		var curVersion uint64
		rec, err := r.store.Get(ctx, e.ID)
		switch {
		case err == nil:
			exists, curVersion = true, rec.Version
		case IsKind(err, KindNotFound):
			exists = false
		default:
			// 后端错误细节不外泄，统一归为内部错误。
			errs = append(errs, EntryError{Index: i, ID: e.ID, Kind: KindInternal,
				Err: errors.New("backend error")})
			continue
		}
		conflict := func(msg string) {
			errs = append(errs, EntryError{Index: i, ID: e.ID, Kind: KindConflict,
				Err: errors.New(msg)})
		}
		switch e.Intent {
		case IntentCreate:
			if exists {
				conflict("record already exists")
			}
			resolved[i] = 0
		case IntentUpdate:
			if !exists {
				errs = append(errs, EntryError{Index: i, ID: e.ID, Kind: KindNotFound,
					Err: errors.New("record does not exist")})
				continue
			}
			if curVersion != e.Version {
				conflict(fmt.Sprintf("version mismatch: current %d, expected %d",
					curVersion, e.Version))
			}
			resolved[i] = e.Version
		case IntentUpsert:
			if !exists {
				resolved[i] = 0
				break
			}
			if e.Version > 0 && curVersion != e.Version {
				conflict(fmt.Sprintf("version mismatch: current %d, expected %d",
					curVersion, e.Version))
			}
			resolved[i] = curVersion
		}
	}
	return errs, resolved
}

// PreflightBatch 对整批条目做预检，一次性返回所有问题条目及其归类原因；
// 返回空切片表示全部通过。预检原因与 ApplyBatch 提交失败时的归类一致。
func (r *Repository) PreflightBatch(ctx context.Context, entries []BatchEntry) []EntryError {
	errs, _ := r.checkBatch(ctx, entries)
	return errs
}

// ApplyBatch 原子提交整批条目：全部生效或全部不生效。
// 任一条目不满足前提时返回 *BatchError（可用 BatchErrors 提取），
// 且不产生任何部分写入。
func (r *Repository) ApplyBatch(ctx context.Context, entries []BatchEntry) error {
	errs, resolved := r.checkBatch(ctx, entries)
	if len(errs) > 0 {
		return &BatchError{Op: "ApplyBatch", Entries: errs}
	}
	// 按主键排序构造变更，保证提交顺序确定。
	order := make([]int, len(entries))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return entries[order[a]].ID < entries[order[b]].ID })
	mutations := make([]Mutation, 0, len(entries))
	for _, i := range order {
		e := entries[i]
		mutations = append(mutations, Mutation{
			Key:             e.ID,
			Value:           encodeValue(e.SortKey, e.Data),
			ExpectedVersion: resolved[i],
		})
	}
	if err := r.store.Commit(ctx, mutations); err != nil {
		// 预检到提交之间状态可能被并发修改：重新预检，
		// 以与预检一致的条目级归类报告失败原因。
		if IsKind(err, KindConflict) {
			if errs, _ := r.checkBatch(ctx, entries); len(errs) > 0 {
				return &BatchError{Op: "ApplyBatch", Entries: errs}
			}
		}
		return normalize("ApplyBatch", err)
	}
	return nil
}
