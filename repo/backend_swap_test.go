package repo

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// backendFactories 列出全部被测后端。所有跨后端语义测试都基于该表驱动，
// 更换或新增后端时只要实现相同契约，对外行为即应一致。
func backendFactories() map[string]func() Backend {
	return map[string]func() Backend{
		"memory":  func() Backend { return NewMemoryBackend() },
		"sharded": func() Backend { return NewShardedBackend(8) },
	}
}

// TestBackendSwapSameSemantics 对两种后端执行同一组读写/分页操作，
// 断言可观察结果（记录版本、全序列序、错误分类）完全一致。
func TestBackendSwapSameSemantics(t *testing.T) {
	ctx := context.Background()
	type obs struct {
		version uint64
		kind    Kind
	}
	scenario := func(r *Repository) ([]string, obs) {
		// 初始写入 5 条，含重复 SortKey。
		for _, row := range [][2]string{
			{"a2", "k1"}, {"a1", "k1"}, {"a3", "k1"},
			{"b1", "k2"}, {"c1", "k3"},
		} {
			if err := r.Create(ctx, row[0], row[1], []byte("v")); err != nil {
				t.Fatalf("create %s: %v", row[0], err)
			}
		}
		// 翻完所有页，得到全序列序。
		ids, _ := readAllPages(t, r, 2)

		// 冲突：读到 v1 后用另一事务先更新，再以旧版本提交。
		tx := r.Begin()
		rec, err := tx.Get(ctx, "b1")
		if err != nil {
			t.Fatalf("get b1: %v", err)
		}
		winner := r.Begin()
		if err := winner.Put(ctx, "b1", "k2", []byte("w")); err != nil {
			t.Fatalf("winner put: %v", err)
		}
		if err := winner.Commit(ctx); err != nil {
			t.Fatalf("winner commit: %v", err)
		}
		if err := tx.Put(ctx, rec.ID, "k2", []byte("l")); err != nil {
			t.Fatalf("loser put: %v", err)
		}
		err = tx.Commit(ctx)
		kind := KindOther
		if err != nil {
			kind = err.(*Error).Kind
		}
		final, _ := r.Get(ctx, "b1")
		return ids, obs{version: final.Version, kind: kind}
	}

	results := map[string]struct {
		ids []string
		o   obs
	}{}
	for name, factory := range backendFactories() {
		r := New(factory())
		ids, o := scenario(r)
		results[name] = struct {
			ids []string
			o   obs
		}{ids, o}
		t.Logf("后端=%s 全序列序=%v 冲突归类=%s 胜者版本=%d", name, ids, o.kind, o.version)
	}
	mem := results["memory"]
	for name, res := range results {
		if !reflect.DeepEqual(res.ids, mem.ids) {
			t.Fatalf("backend %s order=%v differs from memory=%v", name, res.ids, mem.ids)
		}
		if res.o != mem.o {
			t.Fatalf("backend %s observation=%+v differs from memory=%+v", name, res.o, mem.o)
		}
	}
}

// TestShardedErrorNormalization 专门验证分片后端的原生 *shardError
// 不会透出到仓储边界：调用方只能观察到归一化的 *Error 与 Kind。
func TestShardedErrorNormalization(t *testing.T) {
	ctx := context.Background()
	r := New(NewShardedBackend(4))

	// 未找到：底层是 shardCodeMissing，边界必须是 KindNotFound。
	_, err := r.Get(ctx, "missing")
	var re *Error
	if !errors.As(err, &re) {
		t.Fatalf("expected *repo.Error at boundary, got %T: %v", err, err)
	}
	if re.Kind != KindNotFound {
		t.Fatalf("want not_found, got %s", re.Kind)
	}
	var native *shardError
	if errors.As(err, &native) {
		t.Fatalf("native backend error type must not leak through repository boundary")
	}
	t.Logf("判定依据: 分片后端原生错误被包裹但类型不透出；边界错误=%q", err.Error())

	mustCreate(t, r, "x", "k", "v1")
	// 版本冲突：底层是 shardCodeCas，边界必须是 KindConflict。
	tx := r.Begin()
	rec, _ := tx.Get(ctx, "x")
	_ = r.Create(ctx, "y", "ky", []byte("seed")) // 确保后端确实在运转
	w := r.Begin()
	if err := w.Put(ctx, "x", "k", []byte("w")); err != nil {
		t.Fatalf("winner put: %v", err)
	}
	if err := w.Commit(ctx); err != nil {
		t.Fatalf("winner commit: %v", err)
	}
	if err := tx.Put(ctx, rec.ID, "k", []byte("l")); err != nil {
		t.Fatalf("loser put: %v", err)
	}
	if err := tx.Commit(ctx); !IsKind(err, KindConflict) {
		t.Fatalf("cas mismatch must map to KindConflict, got %v", err)
	}
	t.Logf("判定依据: 底层 CAS 失败(shardCodeCas) => 边界 kind=conflict, message=%q", err)
}
