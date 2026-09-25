package repo

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// TestCRUDBasic 覆盖创建、读取、更新、删除的基本路径与版本递增。
func TestCRUDBasic(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestRepo()

	if _, err := r.Get(ctx, "u1"); !IsKind(err, KindNotFound) {
		t.Fatalf("get missing: want KindNotFound, got %v", err)
	}

	if err := r.Create(ctx, "u1", "a", []byte("v1")); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := r.Create(ctx, "u1", "a", []byte("v2")); !IsKind(err, KindConflict) {
		t.Fatalf("duplicate create: want KindConflict, got %v", err)
	}

	rec, err := r.Get(ctx, "u1")
	if err != nil || rec.Version != 1 || string(rec.Value) != "v1" {
		t.Fatalf("get after create: rec=%+v err=%v", rec, err)
	}

	// 经过事务更新，版本应当 +1。
	tx := r.Begin()
	if err := tx.Put(ctx, "u1", "a", []byte("v2")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	rec, _ = r.Get(ctx, "u1")
	if rec.Version != 2 || string(rec.Value) != "v2" {
		t.Fatalf("get after update: rec=%+v", rec)
	}

	if err := r.Delete(ctx, "u1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := r.Get(ctx, "u1"); !IsKind(err, KindNotFound) {
		t.Fatalf("get after delete: want KindNotFound, got %v", err)
	}
}

// TestErrorNormalization 验证所有可观察错误都是归一化的 *Error，
// 且原因可按 Kind 区分，底层实现细节不会直接透出。
func TestErrorNormalization(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestRepo()
	mustCreate(t, r, "u1", "a", "v1")

	_, err := r.Get(ctx, "missing")
	var repoErr *Error
	if !errors.As(err, &repoErr) {
		t.Fatalf("expected *repo.Error, got %T: %v", err, err)
	}
	if !IsKind(err, KindNotFound) {
		t.Fatalf("want not_found, got %s", repoErr.Kind)
	}
	t.Logf("判定依据: 获取不存在记录 => 错误类型 *repo.Error, kind=%s, message=%q", repoErr.Kind, repoErr.Error())

	// 空 ID 是契约违例，而不是未找到。
	if _, err := r.Get(ctx, ""); !IsKind(err, KindContractViolation) {
		t.Fatalf("empty id: want KindContractViolation, got %v", err)
	}

	// 非法游标必须被明确拒绝。
	_, err = r.List(ctx, ListQuery{Cursor: "garbage-token"})
	if !IsKind(err, KindInvalidCursor) {
		t.Fatalf("garbage cursor: want KindInvalidCursor, got %v", err)
	}
	t.Logf("判定依据: 非法游标 => kind=%s, message=%q", err.(*Error).Kind, err.Error())

	// 结束后的事务不可复用。
	tx := r.Begin()
	tx.Rollback()
	if err := tx.Create("x", "k", nil); !IsKind(err, KindContractViolation) {
		t.Fatalf("write after rollback: want KindContractViolation, got %v", err)
	}
}

// TestBackendContractForAllBackends 以工厂表驱动运行，预留多后端验证。
func TestBackendContractForAllBackends(t *testing.T) {
	for name, factory := range backendFactories() {
		t.Run(name, func(t *testing.T) {
			b := factory()
			r := New(b)
			ctx := context.Background()
			if err := r.Create(ctx, "a", "k1", []byte("1")); err != nil {
				t.Fatalf("create on %s backend: %v", name, err)
			}
			rec, err := r.Get(ctx, "a")
			if err != nil || rec.Version != 1 {
				t.Fatalf("backend %s: rec=%+v err=%v", name, rec, err)
			}
			t.Logf("后端 %s 契约检查通过: create=>version=1, get=>%s", name, fmt.Sprint(sortedIDs([]Record{rec})))
		})
	}
}
