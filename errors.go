// Package repository 提供可替换存储后端的本地仓储数据访问层。
package repository

import "errors"

// Kind 表示错误的分类，调用方可据此区分失败原因。
type Kind int

const (
	KindUnknown Kind = iota
	// KindNotFound 表示目标记录不存在。
	KindNotFound
	// KindConflict 表示乐观锁版本冲突或并发提交冲突。
	KindConflict
	// KindContractViolation 表示调用方违反了存储契约（如空主键）。
	KindContractViolation
	// KindInvalidCursor 表示分页游标非法或已失效。
	KindInvalidCursor
	// KindInternal 表示后端内部错误。
	KindInternal
)

// Error 是仓储层对外暴露的统一错误类型，底层错误仅通过 Unwrap 关联，
// 不会直接透出实现细节。
type Error struct {
	Kind Kind
	Op   string // 发生错误的操作名
	Err  error  // 归一化后的原因
}

func (e *Error) Error() string {
	if e.Err != nil {
		return e.Op + ": " + e.Kind.String() + ": " + e.Err.Error()
	}
	return e.Op + ": " + e.Kind.String()
}

func (k Kind) String() string {
	switch k {
	case KindNotFound:
		return "not found"
	case KindConflict:
		return "conflict"
	case KindContractViolation:
		return "contract violation"
	case KindInvalidCursor:
		return "invalid cursor"
	case KindInternal:
		return "internal error"
	default:
		return "unknown error"
	}
}

// normalize 将后端返回的错误归一化为 *Error。
// 已经是 *Error 的按原样保留操作名；未知错误归为 KindInternal，
// 避免把底层实现细节直接透出给调用方。
func normalize(op string, err error) error {
	if err == nil {
		return nil
	}
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return &Error{Kind: KindInternal, Op: op, Err: errors.New("backend error")}
}

func (e *Error) Unwrap() error { return e.Err }

// IsKind 判断 err 是否属于指定分类。
func IsKind(err error, k Kind) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind == k
	}
	return false
}
