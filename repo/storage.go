package repo

import "context"

// Record 是仓储中保存的一条记录。
// Version 由后端单调维护：创建时为 1，每次成功更新加 1，调用方不可自行指定。
type Record struct {
	ID      string
	Version uint64
	SortKey string
	Value   []byte
}

// MutationOp 表示一次事务内写入操作的类型。
type MutationOp int

const (
	// OpPut 按乐观锁条件更新已存在的记录，ExpectVersion 必须等于当前版本。
	OpPut MutationOp = iota
	// OpCreate 仅在记录不存在时创建。
	OpCreate
	// OpDelete 按乐观锁条件删除记录，ExpectVersion 必须等于当前版本。
	OpDelete
)

// Mutation 是一次原子提交中的单个写入。
type Mutation struct {
	Op            MutationOp
	Record        Record
	ExpectVersion uint64
}

// Position 是全局确定顺序中的一个位点，按 (SortKey, ID) 升序排列。
// 零值表示从头开始。
type Position struct {
	SortKey string
	ID      string
}

// Scan 描述一次后端扫描：返回严格位于 After 之后、按 (SortKey,ID) 升序排列的记录。
type Scan struct {
	After Position
	Limit int
}

// Backend 是可替换的存储后端契约。
// 任何实现都必须满足：
//   - Apply 原子地完成整组变更：先校验全部前置条件，再一次性落盘，
//     任一条件不满足时全部变更都不得生效（不允许部分写入）；
//   - List 的排序必须完全确定，SortKey 重复时以 ID 作为决胜键；
//   - 所有错误必须归一化为 *Error，不得透出后端自身的错误类型。
type Backend interface {
	Get(ctx context.Context, id string) (Record, error)
	Apply(ctx context.Context, mutations []Mutation) error
	List(ctx context.Context, scan Scan) ([]Record, error)
}
