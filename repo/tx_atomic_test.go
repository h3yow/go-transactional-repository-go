package repo

import (
	"context"
	"testing"
)

// TestTransactionAtomicCommit 验证一次事务内多条写入全部生效，
// 包括“更新 + 删除 + 创建”的混合提交。
func TestTransactionAtomicCommit(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestRepo()
	mustCreate(t, r, "a", "k-a", "1")
	mustCreate(t, r, "b", "k-b", "1")

	tx := r.Begin()
	if err := tx.Put(ctx, "a", "k-a", []byte("2")); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if err := tx.Delete(ctx, "b"); err != nil {
		t.Fatalf("delete b: %v", err)
	}
	if err := tx.Create("c", "k-c", []byte("3")); err != nil {
		t.Fatalf("create c: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	recA, err := r.Get(ctx, "a")
	if err != nil || recA.Version != 2 || string(recA.Value) != "2" {
		t.Fatalf("a should be updated: %+v err=%v", recA, err)
	}
	if _, err := r.Get(ctx, "b"); !IsKind(err, KindNotFound) {
		t.Fatalf("b should be deleted, got err=%v", err)
	}
	recC, err := r.Get(ctx, "c")
	if err != nil || recC.Version != 1 {
		t.Fatalf("c should be created: %+v err=%v", recC, err)
	}
	t.Logf("判定依据: 混合事务提交后 a.version=2, b=notfound, c.version=1 => 全部生效")
}

// TestTransactionRollbackNoPartialWrites 验证两种“不生效”场景：
//  1. 显式 Rollback 后缓冲的写入一条都不能残留；
//  2. Commit 因组内某个写入违反前置条件而失败时，组内其他写入也不能残留。
func TestTransactionRollbackNoPartialWrites(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestRepo()
	mustCreate(t, r, "a", "k-a", "1")
	mustCreate(t, r, "b", "k-b", "1")

	// 场景 1：显式回滚。
	tx := r.Begin()
	if err := tx.Put(ctx, "a", "k-a", []byte("rolled")); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if err := tx.Delete(ctx, "b"); err != nil {
		t.Fatalf("delete b: %v", err)
	}
	tx.Rollback()

	recA, err := r.Get(ctx, "a")
	if err != nil || recA.Version != 1 || string(recA.Value) != "1" {
		t.Fatalf("after rollback a must be untouched: %+v err=%v", recA, err)
	}
	if _, err := r.Get(ctx, "b"); err != nil {
		t.Fatalf("after rollback b must still exist: %v", err)
	}
	t.Logf("判定依据: Rollback 后 a.version=%d value=%q 且 b 仍存在 => 无部分写入",
		recA.Version, string(recA.Value))

	// 场景 2：提交时组内冲突，整组被拒绝。
	// stale 事务先读取 a（版本 1）；另一个事务抢先把 a 更新到版本 2；
	// stale 同时缓冲“基于版本 1 更新 a”和“创建 d”，提交时 a 冲突，
	// 则 d 也绝不能出现。
	stale := r.Begin()
	staleRec, err := stale.Get(ctx, "a")
	if err != nil {
		t.Fatalf("stale get: %v", err)
	}
	racer := r.Begin()
	if err := racer.Put(ctx, "a", "k-a", []byte("2")); err != nil {
		t.Fatalf("racer put: %v", err)
	}
	if err := racer.Commit(ctx); err != nil {
		t.Fatalf("racer commit: %v", err)
	}
	if err := stale.Put(ctx, staleRec.ID, "k-a", []byte("stale")); err != nil {
		t.Fatalf("stale buffered put: %v", err)
	}
	if err := stale.Create("d", "k-d", []byte("x")); err != nil {
		t.Fatalf("stale buffered create: %v", err)
	}
	err = stale.Commit(ctx)
	if !IsKind(err, KindConflict) {
		t.Fatalf("stale commit: want KindConflict, got %v", err)
	}
	recA, _ = r.Get(ctx, "a")
	if recA.Version != 2 || string(recA.Value) != "2" {
		t.Fatalf("a must remain at winner value v2, got: %+v", recA)
	}
	if _, err := r.Get(ctx, "d"); !IsKind(err, KindNotFound) {
		t.Fatalf("d from rejected transaction must not exist, got err=%v", err)
	}
	t.Logf("判定依据: 冲突事务被 KindConflict 拒绝；a 保持胜者版本 2，组内新建的 d 不存在 => 无部分写入")
}

// TestPutMissingIsNotFound 验证更新不存在记录时得到的是 KindNotFound 而非冲突。
func TestPutMissingIsNotFound(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestRepo()
	tx := r.Begin()
	if err := tx.Put(ctx, "ghost", "k", []byte("v")); !IsKind(err, KindNotFound) {
		t.Fatalf("put missing: want KindNotFound, got %v", err)
	}
	tx.Rollback()
}

// TestEmptyCommitRejected 空事务提交属于契约违例。
func TestEmptyCommitRejected(t *testing.T) {
	r, _ := newTestRepo()
	if err := r.Begin().Commit(context.Background()); !IsKind(err, KindContractViolation) {
		t.Fatalf("empty commit: want KindContractViolation, got %v", err)
	}
}

// TestDoubleWriteSameRecordRejected 同一事务内重复触碰同一记录属于契约违例。
func TestDoubleWriteSameRecordRejected(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestRepo()
	mustCreate(t, r, "a", "k-a", "1")
	tx := r.Begin()
	if err := tx.Put(ctx, "a", "k-a", []byte("2")); err != nil {
		t.Fatalf("first put: %v", err)
	}
	if err := tx.Put(ctx, "a", "k-a", []byte("3")); !IsKind(err, KindContractViolation) {
		t.Fatalf("second put in same tx: want KindContractViolation, got %v", err)
	}
	tx.Rollback()
	// 回滚后数据保持原样。
	rec, _ := r.Get(ctx, "a")
	if rec.Version != 1 || string(rec.Value) != "1" {
		t.Fatalf("record must be untouched: %+v", rec)
	}
}
