package repo

import (
	"context"
)

const (
	// defaultPageLimit 是调用方未指定 Limit 时使用的页大小。
	defaultPageLimit = 20
	// maxPageLimit 限制单页返回条数，防止一次读取把后端拖垮。
	maxPageLimit = 100
)

// ListQuery 描述一次游标分页读取。
type ListQuery struct {
	// Limit 为页大小；<=0 时使用默认值，超过上限时截断。
	Limit int
	// Cursor 为上一页返回的 NextCursor；首次读取留空。
	Cursor string
}

// Page 是一页读取结果。
type Page struct {
	Records    []Record
	NextCursor string
	HasMore    bool
}

// Repository 是面向调用方的仓储入口。
// 它本身不关心数据存放在哪里，所有读写都委托给可替换的 Backend。
type Repository struct {
	backend Backend
}

// New 基于给定后端创建仓储。
func New(backend Backend) *Repository {
	return &Repository{backend: backend}
}

// Get 读取单条记录。
func (r *Repository) Get(ctx context.Context, id string) (Record, error) {
	if id == "" {
		return Record{}, ContractViolation("repo.get", "record id is empty")
	}
	return r.backend.Get(ctx, id)
}

// List 执行一次游标分页读取。
// 游标在仓储层解码并严格校验，后端只接收确定的排序位置；
// 因此后端的替换不会改变对外的分页语义。
func (r *Repository) List(ctx context.Context, q ListQuery) (Page, error) {
	const op = "repo.list"

	limit := q.Limit
	if limit <= 0 {
		limit = defaultPageLimit
	}
	if limit > maxPageLimit {
		limit = maxPageLimit
	}

	pos, err := decodeCursor(q.Cursor)
	if err != nil {
		return Page{}, err
	}

	// 多取一条用于判断是否还有下一页，避免依赖容易漂移的总数统计。
	rows, err := r.backend.List(ctx, Scan{
		After: Position{SortKey: pos.SortKey, ID: pos.ID},
		Limit: limit + 1,
	})
	if err != nil {
		return Page{}, err
	}

	page := Page{}
	if len(rows) > limit {
		page.HasMore = true
		rows = rows[:limit]
	}
	page.Records = rows
	if page.HasMore {
		last := rows[limit-1]
		page.NextCursor = encodeCursor(cursorPayload{SortKey: last.SortKey, ID: last.ID})
	}
	return page, nil
}

// Create 是单条创建的便捷方法，语义等价于 Begin/Create/Commit。
func (r *Repository) Create(ctx context.Context, id, sortKey string, value []byte) error {
	tx := r.Begin()
	if err := tx.Create(id, sortKey, value); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit(ctx)
}

// Delete 是单条删除的便捷方法，语义等价于 Begin/Get/Delete/Commit。
func (r *Repository) Delete(ctx context.Context, id string) error {
	tx := r.Begin()
	if err := tx.Delete(ctx, id); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit(ctx)
}

// Tx 是一次读写事务。
// 写操作先缓冲在本地，Commit 时整组交给后端原子应用：
//   - 任一写入的乐观锁条件不满足，整组被拒绝，不产生部分写入；
//   - 提交或回滚结束后该事务不可再用，继续使用属于契约违例。
//
// Tx 不是并发安全的：同一事务只应在一个 goroutine 中使用；
// 需要并发写入时请开启多个独立事务。
type Tx struct {
	repo      *Repository
	mutations []Mutation
	readVer   map[string]uint64
	done      bool
}

// Begin 开启一个新事务。
func (r *Repository) Begin() *Tx {
	return &Tx{repo: r, readVer: make(map[string]uint64)}
}

func (tx *Tx) checkLive(op string) error {
	if tx.done {
		return ContractViolation(op, "transaction has already been committed or rolled back")
	}
	return nil
}

// Get 在事务上下文中读取记录，并记录所读版本作为后续更新的乐观锁依据。
func (tx *Tx) Get(ctx context.Context, id string) (Record, error) {
	const op = "tx.get"
	if err := tx.checkLive(op); err != nil {
		return Record{}, err
	}
	if id == "" {
		return Record{}, ContractViolation(op, "record id is empty")
	}
	rec, err := tx.repo.backend.Get(ctx, id)
	if err != nil {
		return Record{}, err
	}
	tx.readVer[id] = rec.Version
	return rec, nil
}

func (tx *Tx) append(opName string, m Mutation) error {
	if err := tx.checkLive(opName); err != nil {
		return err
	}
	if m.Record.ID == "" {
		return ContractViolation(opName, "record id is empty")
	}
	// 同一事务内缓冲阶段也不允许重复触碰同一条记录：
	// 否则第二次写入的版本前提无法被有意义地表达。
	for _, existing := range tx.mutations {
		if existing.Record.ID == m.Record.ID {
			return ContractViolation(opName, "record already written in this transaction: "+m.Record.ID)
		}
	}
	tx.mutations = append(tx.mutations, m)
	return nil
}

// Create 缓冲一条创建操作：仅当记录当前不存在时提交才会成功。
func (tx *Tx) Create(id, sortKey string, value []byte) error {
	return tx.append("tx.create", Mutation{
		Op:            OpCreate,
		ExpectVersion: 0,
		Record:        Record{ID: id, SortKey: sortKey, Value: cloneValue(value)},
	})
}

// Put 缓冲一条更新操作。
// 若本事务此前 Get 过该记录，则以所读版本作为乐观锁条件；
// 否则以提交瞬间后端中的当前版本为条件（内部先读取一次）。
// 记录不存在时提交将得到 KindNotFound；版本不匹配将得到 KindConflict。
func (tx *Tx) Put(ctx context.Context, id, sortKey string, value []byte) error {
	const op = "tx.put"
	if err := tx.checkLive(op); err != nil {
		return err
	}
	expect, ok := tx.readVer[id]
	if !ok {
		cur, err := tx.repo.backend.Get(ctx, id)
		if err != nil {
			return err
		}
		expect = cur.Version
		tx.readVer[id] = expect
	}
	return tx.append(op, Mutation{
		Op:            OpPut,
		ExpectVersion: expect,
		Record:        Record{ID: id, SortKey: sortKey, Value: cloneValue(value)},
	})
}

// Delete 缓冲一条删除操作，版本条件的确定方式与 Put 相同。
func (tx *Tx) Delete(ctx context.Context, id string) error {
	const op = "tx.delete"
	if err := tx.checkLive(op); err != nil {
		return err
	}
	expect, ok := tx.readVer[id]
	if !ok {
		cur, err := tx.repo.backend.Get(ctx, id)
		if err != nil {
			return err
		}
		expect = cur.Version
		tx.readVer[id] = expect
	}
	return tx.append(op, Mutation{
		Op:            OpDelete,
		ExpectVersion: expect,
		Record:        Record{ID: id},
	})
}

// Commit 把缓冲的全部写入原子地交给后端。
// 空事务提交属于契约违例；提交后无论成败事务都结束，不可复用。
func (tx *Tx) Commit(ctx context.Context) error {
	const op = "tx.commit"
	if err := tx.checkLive(op); err != nil {
		return err
	}
	tx.done = true
	if len(tx.mutations) == 0 {
		return ContractViolation(op, "commit with no buffered writes")
	}
	err := tx.repo.backend.Apply(ctx, tx.mutations)
	// 无论成败都丢弃缓冲：成功则数据已在后端，失败则后端保证没有任何落盘。
	tx.mutations = nil
	return err
}

// Rollback 放弃事务中的全部缓冲写入。它是幂等的，重复调用不报错。
func (tx *Tx) Rollback() {
	tx.mutations = nil
	tx.done = true
}
