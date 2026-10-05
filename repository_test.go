package repository

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
)

// mustCreate 创建一条记录并返回其版本（创建后版本为 1）。
func mustCreate(t *testing.T, r *Repository, id, sortKey string) uint64 {
	t.Helper()
	err := r.InTx(context.Background(), func(tx *Tx) error {
		return tx.Put(context.Background(), id, sortKey, []byte("v-"+id), 0)
	})
	if err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
	return 1
}

// TestConcurrentCommitConflict 并发修改同一记录：只有一次提交成功，
// 其余必须被归类为 KindConflict。
func TestConcurrentCommitConflict(t *testing.T) {
	r := New(NewMemoryStore())
	const writers = 8
	ver := mustCreate(t, r, "hot", "k")
	t.Logf("输入: %d 个并发写者, 均基于版本 %d 修改同一记录 hot", writers, ver)

	var wg sync.WaitGroup
	results := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results <- r.InTx(context.Background(), func(tx *Tx) error {
				return tx.Put(context.Background(), "hot", "k",
					[]byte(fmt.Sprintf("writer-%d", i)), ver)
			})
		}(i)
	}
	wg.Wait()
	close(results)

	var ok, conflicts, other int
	for err := range results {
		switch {
		case err == nil:
			ok++
		case IsKind(err, KindConflict):
			conflicts++
		default:
			other++
			t.Logf("未预期的错误类型: %v", err)
		}
	}
	t.Logf("判定依据: 成功=%d (期望1), 冲突=%d (期望%d), 其他=%d (期望0)",
		ok, conflicts, writers-1, other)
	if ok != 1 || conflicts != writers-1 || other != 0 {
		t.Fatalf("并发冲突归类错误: ok=%d conflicts=%d other=%d", ok, conflicts, other)
	}
}

// TestRollbackAtomicity 回滚不得残留任何部分写入；
// 提交期冲突同样不得产生部分写入。
func TestRollbackAtomicity(t *testing.T) {
	r := New(NewMemoryStore())
	mustCreate(t, r, "base", "k")

	// 场景1: fn 返回错误 -> 回滚，两个写入都不生效。
	errBoom := errors.New("boom")
	err := r.InTx(context.Background(), func(tx *Tx) error {
		_ = tx.Put(context.Background(), "a", "k", []byte("a"), 0)
		_ = tx.Put(context.Background(), "b", "k", []byte("b"), 0)
		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("期望回滚错误透传, 得到 %v", err)
	}
	for _, id := range []string{"a", "b"} {
		_, err := r.Get(context.Background(), id)
		t.Logf("场景1 判定: 回滚后 Get(%q) -> %v (期望 not found)", id, err)
		if !IsKind(err, KindNotFound) {
			t.Fatalf("回滚后 %q 不应存在, err=%v", id, err)
		}
	}

	// 场景2: 事务内一个写合法、一个写版本过期 -> 整体拒绝，合法写也不生效。
	stale := uint64(1)
	// 先把 base 推到版本 2，使 stale=1 过期。
	_ = r.InTx(context.Background(), func(tx *Tx) error {
		return tx.Put(context.Background(), "base", "k", []byte("v2"), 1)
	})
	err = r.InTx(context.Background(), func(tx *Tx) error {
		_ = tx.Put(context.Background(), "c", "k", []byte("c"), 0)         // 合法创建
		_ = tx.Put(context.Background(), "base", "k", []byte("v3"), stale) // 版本过期
		return nil
	})
	t.Logf("场景2 输入: 同事务含合法创建 c 与过期写 base@%d; 提交错误=%v", stale, err)
	if !IsKind(err, KindConflict) {
		t.Fatalf("期望 KindConflict, 得到 %v", err)
	}
	if _, err := r.Get(context.Background(), "c"); !IsKind(err, KindNotFound) {
		t.Fatalf("冲突提交不得残留部分写入 c, err=%v", err)
	}
	base, _ := r.Get(context.Background(), "base")
	t.Logf("场景2 判定: base 版本=%d (期望2, 未被过期写覆盖)", base.Version)
	if base.Version != 2 {
		t.Fatalf("base 版本应为 2, 得到 %d", base.Version)
	}
}

// TestPaginationWithInsertDelete 翻页期间发生插入/删除：
// 已返回的记录不得重复出现，游标之后应出现的记录不得遗漏。
func TestPaginationWithInsertDelete(t *testing.T) {
	r := New(NewMemoryStore())
	// 10 条记录，排序键故意重复（两两相同），验证决胜键稳定性。
	for i := 0; i < 10; i++ {
		mustCreate(t, r, fmt.Sprintf("id-%02d", i), fmt.Sprintf("sk-%02d", i/2))
	}
	ctx := context.Background()
	const limit = 4
	t.Logf("输入: 10 条记录, 排序键两两重复, 每页 %d 条", limit)

	page1, err := r.List(ctx, limit, "")
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if len(page1.Items) != limit || page1.NextCursor == "" {
		t.Fatalf("page1 应有 %d 条且有下一页游标, 得到 %d 条", limit, len(page1.Items))
	}
	seen := map[string]bool{}
	for _, it := range page1.Items {
		seen[it.ID] = true
	}
	t.Logf("page1=%v", idsOf(page1))

	// 翻页间隙：删除一条尚未返回的记录，插入一条排在游标之后的新记录。
	last := page1.Items[len(page1.Items)-1]
	victim, _ := r.Get(ctx, "id-07")
	_ = r.InTx(ctx, func(tx *Tx) error {
		return tx.Delete(ctx, "id-07", victim.Version)
	})
	mustCreate(t, r, "id-new", "sk-09") // 排在最后，应被后续页看到
	t.Logf("翻页间隙: 删除 id-07, 插入 id-new(sk-09), 游标=(%q,%q)",
		last.SortKey, last.ID)

	page2, err := r.List(ctx, limit, page1.NextCursor)
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	page3, err := r.List(ctx, limit, page2.NextCursor)
	if err != nil {
		t.Fatalf("page3: %v", err)
	}
	t.Logf("page2=%v page3=%v", idsOf(page2), idsOf(page3))

	for _, p := range []Page{page2, page3} {
		for _, it := range p.Items {
			if seen[it.ID] {
				t.Fatalf("记录 %s 被重复返回", it.ID)
			}
			seen[it.ID] = true
			if it.ID == "id-07" {
				t.Fatalf("已删除的 id-07 不应出现")
			}
		}
	}
	// 游标之后应出现的: id-04..id-09 中除被删的 id-07, 以及新插入的 id-new。
	want := []string{"id-04", "id-05", "id-06", "id-08", "id-09", "id-new"}
	for _, id := range want {
		if !seen[id] {
			t.Fatalf("游标之后的记录 %s 被遗漏", id)
		}
	}
	t.Logf("判定依据: 跨页无重复, 删除项未出现, 期望集合 %v 全部覆盖", want)
	if page3.NextCursor != "" {
		t.Fatalf("最后一页不应有后续游标")
	}
}

// TestStableOrderWithDuplicateSortKeys 排序键重复时顺序必须完全确定。
func TestStableOrderWithDuplicateSortKeys(t *testing.T) {
	r := New(NewMemoryStore())
	for i := 0; i < 20; i++ {
		mustCreate(t, r, fmt.Sprintf("id-%02d", i), "same")
	}
	ctx := context.Background()
	var first []string
	for round := 0; round < 5; round++ {
		var got []string
		cursor := ""
		for {
			p, err := r.List(ctx, 3, cursor)
			if err != nil {
				t.Fatalf("list: %v", err)
			}
			got = append(got, idsOf(p)...)
			if p.NextCursor == "" {
				break
			}
			cursor = p.NextCursor
		}
		if first == nil {
			first = got
			t.Logf("基准顺序: %v", first)
			continue
		}
		for i := range got {
			if got[i] != first[i] {
				t.Fatalf("第 %d 轮顺序漂移: %v != %v", round, got, first)
			}
		}
	}
	if !sort.StringsAreSorted(first) {
		t.Fatalf("排序键全相同时应按 ID 升序, 得到 %v", first)
	}
	t.Logf("判定依据: 5 轮全量翻页顺序完全一致且按 ID 升序")
}

// TestInvalidCursor 非法游标必须被明确拒绝。
func TestInvalidCursor(t *testing.T) {
	r := New(NewMemoryStore())
	mustCreate(t, r, "x", "k")
	for _, c := range []string{"!!!not-base64!!!", "aGVsbG8", "e30"} {
		_, err := r.List(context.Background(), 10, c)
		t.Logf("输入游标 %q -> %v", c, err)
		if !IsKind(err, KindInvalidCursor) {
			t.Fatalf("游标 %q 期望 KindInvalidCursor, 得到 %v", c, err)
		}
	}
}

// TestErrorClassification 未找到与契约违例必须可区分。
func TestErrorClassification(t *testing.T) {
	r := New(NewMemoryStore())
	_, err := r.Get(context.Background(), "missing")
	t.Logf("Get(missing) -> %v", err)
	if !IsKind(err, KindNotFound) {
		t.Fatalf("期望 KindNotFound, 得到 %v", err)
	}
	err = r.InTx(context.Background(), func(tx *Tx) error {
		return tx.Put(context.Background(), "", "k", nil, 0)
	})
	t.Logf("Put(空ID) -> %v", err)
	if !IsKind(err, KindContractViolation) {
		t.Fatalf("期望 KindContractViolation, 得到 %v", err)
	}
	if _, err := r.List(context.Background(), 0, ""); !IsKind(err, KindContractViolation) {
		t.Fatalf("limit=0 期望 KindContractViolation, 得到 %v", err)
	}
}

// sliceStore 是测试内的第二个存储后端（基于切片），
// 用于验证更换后端不改变对外语义。
type sliceStore struct {
	mu   sync.Mutex
	recs []Record
}

func (s *sliceStore) Get(_ context.Context, key string) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.recs {
		if r.Key == key {
			return r, nil
		}
	}
	return Record{}, &Error{Kind: KindNotFound, Op: "slice.Get", Err: errors.New("missing")}
}

func (s *sliceStore) Scan(_ context.Context, after string, limit int) ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Record
	for _, r := range s.recs {
		if r.Key > after {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	if limit >= 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *sliceStore) Commit(_ context.Context, mutations []Mutation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := map[string]int{}
	for i, r := range s.recs {
		idx[r.Key] = i
	}
	for _, m := range mutations {
		var cur uint64
		if i, ok := idx[m.Key]; ok {
			cur = s.recs[i].Version
		}
		if cur != m.ExpectedVersion {
			return &Error{Kind: KindConflict, Op: "slice.Commit", Err: errors.New("version mismatch")}
		}
	}
	for _, m := range mutations {
		if i, ok := idx[m.Key]; ok {
			if m.Delete {
				s.recs = append(s.recs[:i], s.recs[i+1:]...)
				delete(idx, m.Key)
			} else {
				s.recs[i] = Record{Key: m.Key, Value: m.Value, Version: m.ExpectedVersion + 1}
			}
			continue
		}
		if !m.Delete {
			idx[m.Key] = len(s.recs)
			s.recs = append(s.recs, Record{Key: m.Key, Value: m.Value, Version: m.ExpectedVersion + 1})
		}
	}
	return nil
}

// TestBackendSwapSemantics 同一组读写/分页场景在两个后端上结果一致。
func TestBackendSwapSemantics(t *testing.T) {
	backends := map[string]func() Store{
		"memory":     func() Store { return NewMemoryStore() },
		"slice":      func() Store { return &sliceStore{} },
		"chunked(2)": func() Store { return NewChunkedStore(2) },
	}
	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			r := New(mk())
			for i := 0; i < 6; i++ {
				mustCreate(t, r, fmt.Sprintf("id-%d", i), fmt.Sprintf("sk-%d", i%2))
			}
			ctx := context.Background()
			p, err := r.List(ctx, 4, "")
			if err != nil || len(p.Items) != 4 {
				t.Fatalf("后端 %s: 第一页异常 items=%d err=%v", name, len(p.Items), err)
			}
			p2, err := r.List(ctx, 4, p.NextCursor)
			if err != nil {
				t.Fatalf("后端 %s: 第二页 err=%v", name, err)
			}
			got := append(idsOf(p), idsOf(p2)...)
			t.Logf("后端 %s 全量顺序: %v", name, got)
			want := []string{"id-0", "id-2", "id-4", "id-1", "id-3", "id-5"}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("后端 %s 顺序不一致: got=%v want=%v", name, got, want)
				}
			}
			if _, err := r.Get(ctx, "nope"); !IsKind(err, KindNotFound) {
				t.Fatalf("后端 %s: 期望 KindNotFound, 得到 %v", name, err)
			}
			// 乐观锁冲突在两个后端上都应归类为 KindConflict。
			err = r.InTx(ctx, func(tx *Tx) error {
				return tx.Put(ctx, "id-0", "sk-0", []byte("x"), 99)
			})
			if !IsKind(err, KindConflict) {
				t.Fatalf("后端 %s: 期望 KindConflict, 得到 %v", name, err)
			}
		})
	}
}

func idsOf(p Page) []string {
	out := make([]string, 0, len(p.Items))
	for _, it := range p.Items {
		out = append(out, it.ID)
	}
	return out
}
