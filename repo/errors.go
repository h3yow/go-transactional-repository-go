package repo

import (
	"errors"
	"fmt"
)

// Kind 是归一化后的错误分类，与具体存储后端无关。
// 更换存储后端不会改变这些分类，调用方应按 Kind 而非底层错误做分支处理。
type Kind int

const (
	// KindOther 未分类错误（例如后端不可用），调用方通常应重试或失败上抛。
	KindOther Kind = iota
	// KindNotFound 目标记录不存在。
	KindNotFound
	// KindConflict 乐观锁版本冲突，或创建已存在的记录；事务内任一冲突都会导致整组写入被拒绝。
	KindConflict
	// KindContractViolation 调用方违反仓储契约（空 ID、重复提交、事务结束后再使用等）。
	KindContractViolation
	// KindInvalidCursor 游标非法、损坏或已失效。
	KindInvalidCursor
)

func (k Kind) String() string {
	switch k {
	case KindNotFound:
		return "not_found"
	case KindConflict:
		return "conflict"
	case KindContractViolation:
		return "contract_violation"
	case KindInvalidCursor:
		return "invalid_cursor"
	default:
		return "other"
	}
}

// Error 是仓储层对外暴露的唯一错误形态。
// 底层后端错误永远不会直接透出：会被包装在 Cause 中且不参与判定，
// 调用方只应依赖 Kind 做判断。
type Error struct {
	Kind   Kind
	Op     string
	Detail string
	Cause  error
}

func (e *Error) Error() string {
	s := fmt.Sprintf("repo: op=%s kind=%s", e.Op, e.Kind)
	if e.Detail != "" {
		s += " " + e.Detail
	}
	if e.Cause != nil {
		s += ": " + e.Cause.Error()
	}
	return s
}

func (e *Error) Unwrap() error { return e.Cause }

func newError(kind Kind, op, detail string, cause error) *Error {
	return &Error{Kind: kind, Op: op, Detail: detail, Cause: cause}
}

// NotFound 构造未找到错误。
func NotFound(op, detail string) *Error { return newError(KindNotFound, op, detail, nil) }

// Conflict 构造冲突错误。
func Conflict(op, detail string) *Error { return newError(KindConflict, op, detail, nil) }

// ContractViolation 构造契约违例错误。
func ContractViolation(op, detail string) *Error {
	return newError(KindContractViolation, op, detail, nil)
}

// InvalidCursor 构造非法游标错误。
func InvalidCursor(op, detail string) *Error {
	return newError(KindInvalidCursor, op, detail, nil)
}

// IsKind 判断错误是否属于给定的归一化分类。
func IsKind(err error, kind Kind) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind == kind
	}
	return false
}
