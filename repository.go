package repository

import (
	"context"
	"errors"
	"sort"
)

// Item 是仓储层对外的数据项。
type Item struct {
	ID      string
	SortKey string
	Data    []byte
	Version uint64
}

// Page 是一页读取结果。
type Page struct {
	Items      []Item
	NextCursor string // 为空表示没有更多数据
}

// itemFromRecord 将存储记录转换为对外数据项。
func itemFromRecord(rec Record) Item {
	return Item{ID: rec.Key, SortKey: rec.SortKey, Data: rec.Value, Version: rec.Version}
}

// Repository 是基于 Store 的仓储，提供事务、乐观锁与游标分页。
type Repository struct {
	store Store
}

// New 基于给定存储后端创建仓储。
func New(store Store) *Repository {
	return &Repository{store: store}
}

// stagedWrite 是事务内暂存的一个写操作。
type stagedWrite struct {
	mutation Mutation
	item     Item // 用于事务内 read-your-writes
}

// Tx 是一次事务的句柄，事务内的写入在提交前对其他操作不可见。
type Tx struct {
	repo   *Repository
	staged map[string]stagedWrite
}

// InTx 在事务中执行 fn：fn 返回 nil 则提交（全部生效），
// 否则回滚（无任何部分写入）。提交时的版本冲突返回 KindConflict。
func (r *Repository) InTx(ctx context.Context, fn func(tx *Tx) error) error {
	tx := &Tx{repo: r, staged: make(map[string]stagedWrite)}
	if err := fn(tx); err != nil {
		return err // 回滚：暂存的写入直接丢弃，后端无任何变化
	}
	keys := make([]string, 0, len(tx.staged))
	for k := range tx.staged {
		keys = append(keys, k)
	}
	sort.Strings(keys) // 固定提交顺序，保证行为确定
	mutations := make([]Mutation, 0, len(keys))
	for _, k := range keys {
		mutations = append(mutations, tx.staged[k].mutation)
	}
	if err := r.store.Commit(ctx, mutations); err != nil {
		return normalize("tx.Commit", err)
	}
	return nil
}

// Get 读取单条记录；不存在时返回 KindNotFound。
func (r *Repository) Get(ctx context.Context, id string) (Item, error) {
	var item Item
	err := r.InTx(ctx, func(tx *Tx) error {
		var err error
		item, err = tx.Get(ctx, id)
		return err
	})
	return item, err
}

// Get 读取单条记录；不存在时返回 KindNotFound。
// 事务内先读暂存区，保证 read-your-writes。
func (tx *Tx) Get(ctx context.Context, id string) (Item, error) {
	if id == "" {
		return Item{}, &Error{Kind: KindContractViolation, Op: "tx.Get",
			Err: errors.New("id must not be empty")}
	}
	if sw, ok := tx.staged[id]; ok {
		if sw.mutation.Delete {
			return Item{}, &Error{Kind: KindNotFound, Op: "tx.Get",
				Err: errors.New("record deleted in transaction")}
		}
		return sw.item, nil
	}
	rec, err := tx.repo.store.Get(ctx, id)
	if err != nil {
		return Item{}, normalize("tx.Get", err)
	}
	return itemFromRecord(rec), nil
}

// Put 写入记录。expectedVersion 为 0 表示创建（记录必须不存在），
// 否则必须与当前版本一致（乐观锁）。
func (tx *Tx) Put(ctx context.Context, id, sortKey string, data []byte, expectedVersion uint64) error {
	if id == "" {
		return &Error{Kind: KindContractViolation, Op: "tx.Put",
			Err: errors.New("id must not be empty")}
	}
	tx.staged[id] = stagedWrite{
		mutation: Mutation{
			Key:             id,
			SortKey:         sortKey,
			Value:           data,
			ExpectedVersion: expectedVersion,
		},
		item: Item{ID: id, SortKey: sortKey, Data: data, Version: expectedVersion + 1},
	}
	return nil
}

// Delete 删除记录，expectedVersion 语义同 Put。
func (tx *Tx) Delete(ctx context.Context, id string, expectedVersion uint64) error {
	if id == "" {
		return &Error{Kind: KindContractViolation, Op: "tx.Delete",
			Err: errors.New("id must not be empty")}
	}
	tx.staged[id] = stagedWrite{
		mutation: Mutation{Key: id, Delete: true, ExpectedVersion: expectedVersion},
	}
	return nil
}

// scanPage 从位置 from 起流式扫描，收集至多 limit 条满足 keep 的记录。
// 返回收集到的记录、最后扫描到的位置以及是否已耗尽（后端确认无更多记录）。
// 每次只向后端请求补齐当前页所需的条数，后端单次返回受限时自动续扫，
// 不会一次性把全部记录读入内存。
func (r *Repository) scanPage(ctx context.Context, from ScanPosition, keep func(Item) bool, limit int) (items []Item, last ScanPosition, exhausted bool, err error) {
	items = make([]Item, 0, limit)
	last = from
	for len(items) < limit {
		recs, err := r.store.ScanSorted(ctx, last, limit-len(items))
		if err != nil {
			return nil, last, false, normalize("scanPage", err)
		}
		if len(recs) == 0 {
			return items, last, true, nil
		}
		for _, rec := range recs {
			item := itemFromRecord(rec)
			last = ScanPosition{SortKey: item.SortKey, Key: item.ID}
			if keep == nil || keep(item) {
				items = append(items, item)
			}
		}
	}
	return items, last, false, nil
}

// List 按 (SortKey, ID) 的稳定顺序做键集分页。
// 排序键重复时以 ID 作为决胜键，顺序完全确定。
// cursor 为空表示从头开始；非法游标返回 KindInvalidCursor。
// 键集分页使得翻页期间的插入/删除不会导致已返回记录重复或遗漏。
func (r *Repository) List(ctx context.Context, limit int, cursorStr string) (Page, error) {
	if limit <= 0 {
		return Page{}, &Error{Kind: KindContractViolation, Op: "List",
			Err: errors.New("limit must be positive")}
	}
	var from ScanPosition
	if cursorStr != "" {
		c, err := decodeCursor(cursorStr)
		if err != nil {
			return Page{}, err
		}
		from = ScanPosition{SortKey: c.SortKey, Key: c.Key}
	}
	items, last, exhausted, err := r.scanPage(ctx, from, nil, limit)
	if err != nil {
		return Page{}, err
	}
	page := Page{Items: items}
	if !exhausted {
		page.NextCursor = encodeCursor(cursor{SortKey: last.SortKey, Key: last.Key})
	}
	return page, nil
}
