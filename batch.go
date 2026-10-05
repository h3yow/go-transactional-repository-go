package repository

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// WriteIntent 表示批量写入中单个条目的意图。
type WriteIntent int

const (
	// IntentCreate 只允许新建：目标已存在则失败（KindConflict）。
	IntentCreate WriteIntent = iota
	// IntentUpdate 只允许更新：目标不存在则失败（KindNotFound），
	// 版本与 ExpectedVersion 不符则失败（KindConflict）。
	IntentUpdate
	// IntentUpsert 不存在就新建、存在就更新：
	// 存在时版本必须与 ExpectedVersion 一致（否则 KindConflict）。
	IntentUpsert
)

// BatchItem 是批量写入中的一个条目。
type BatchItem struct {
	ID      string
	SortKey string
	Data    []byte
	Intent  WriteIntent
	// ExpectedVersion 期望的当前版本：
	// IntentCreate 忽略（恒要求目标不存在）；IntentUpdate 必须 >= 1；
	// IntentUpsert 在目标存在时必须与当前版本一致，不存在时忽略。
	ExpectedVersion uint64
}

// ItemFailure 是预检发现的单个条目的问题，Kind 与正式提交失败时的
// 错误分类一致：KindNotFound（目标不存在）、KindConflict（版本已变
// 或新建目标已存在）、KindContractViolation（入参不合法）。
type ItemFailure struct {
	Index int    // 条目在批次中的下标
	ID    string // 条目的 ID（可能为空，对应入参不合法）
	Kind  Kind
	Err   error // *Error，含具体原因
}

// BatchError 是批量预检失败的错误，一次性列出批次内所有问题条目。
type BatchError struct {
	Failures []ItemFailure
}

func (e *BatchError) Error() string {
	return fmt.Sprintf("batch preflight: %d item(s) rejected", len(e.Failures))
}

// Preflight 对整批条目做预检，不写入任何数据，一次性返回所有问题条目
// （按下标升序）；全部通过时返回 nil。每个条目的检查彼此独立，
// 不会在第一个失败处停下。
func (r *Repository) Preflight(ctx context.Context, items []BatchItem) []ItemFailure {
	var failures []ItemFailure
	seen := make(map[string]bool, len(items))
	for i, it := range items {
		if f := r.checkItem(ctx, i, it, seen); f != nil {
			failures = append(failures, *f)
		}
	}
	return failures
}

// checkItem 校验单个条目，返回 nil 表示通过。
func (r *Repository) checkItem(ctx context.Context, index int, it BatchItem, seen map[string]bool) *ItemFailure {
	fail := func(kind Kind, msg string) *ItemFailure {
		return &ItemFailure{
			Index: index,
			ID:    it.ID,
			Kind:  kind,
			Err:   &Error{Kind: kind, Op: "batch.Preflight", Err: errors.New(msg)},
		}
	}
	if it.ID == "" {
		return fail(KindContractViolation, "id must not be empty")
	}
	if seen[it.ID] {
		return fail(KindContractViolation, "duplicate id in batch: "+it.ID)
	}
	seen[it.ID] = true

	rec, err := r.store.Get(ctx, it.ID)
	exists := true
	var curVersion uint64
	switch {
	case err == nil:
		curVersion = rec.Version
	case IsKind(err, KindNotFound):
		exists = false
	default:
		// 后端内部错误：归一化，不透出实现细节。
		return &ItemFailure{Index: index, ID: it.ID, Kind: KindInternal,
			Err: normalize("batch.Preflight", err)}
	}

	switch it.Intent {
	case IntentCreate:
		if exists {
			return fail(KindConflict, "record already exists: "+it.ID)
		}
	case IntentUpdate:
		if it.ExpectedVersion == 0 {
			return fail(KindContractViolation,
				"update intent requires expectedVersion >= 1")
		}
		if !exists {
			return fail(KindNotFound, "record does not exist: "+it.ID)
		}
		if curVersion != it.ExpectedVersion {
			return fail(KindConflict, fmt.Sprintf(
				"version mismatch on %s: expected %d, current %d",
				it.ID, it.ExpectedVersion, curVersion))
		}
	case IntentUpsert:
		if exists && curVersion != it.ExpectedVersion {
			return fail(KindConflict, fmt.Sprintf(
				"version mismatch on %s: expected %d, current %d",
				it.ID, it.ExpectedVersion, curVersion))
		}
	default:
		return fail(KindContractViolation, fmt.Sprintf("unknown write intent: %d", it.Intent))
	}
	return nil
}

// BatchWrite 按条件批量写入：先整批预检，任一条目有问题即整体拒绝
// （返回 *BatchError，列出全部问题条目，不产生任何写入）；
// 预检全部通过后一次性原子提交，整体成功或整体失败。
// 预检与提交之间目标被并发修改时，提交返回 KindConflict。
func (r *Repository) BatchWrite(ctx context.Context, items []BatchItem) error {
	if len(items) == 0 {
		return nil
	}
	if failures := r.Preflight(ctx, items); len(failures) > 0 {
		return &BatchError{Failures: failures}
	}
	// 按 ID 排序构建 Mutation，保证提交顺序确定。
	sorted := make([]BatchItem, len(items))
	copy(sorted, items)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })

	mutations := make([]Mutation, 0, len(sorted))
	for _, it := range sorted {
		m := Mutation{Key: it.ID, SortKey: it.SortKey, Value: it.Data}
		switch it.Intent {
		case IntentCreate:
			m.ExpectedVersion = 0
		case IntentUpdate:
			m.ExpectedVersion = it.ExpectedVersion
		case IntentUpsert:
			// 预检已确认：存在则版本匹配，不存在则按创建提交。
			rec, err := r.store.Get(ctx, it.ID)
			if err == nil {
				m.ExpectedVersion = rec.Version
			} else {
				m.ExpectedVersion = 0
			}
		}
		mutations = append(mutations, m)
	}
	if err := r.store.Commit(ctx, mutations); err != nil {
		return normalize("batch.Commit", err)
	}
	return nil
}
