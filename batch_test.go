package repository

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// TestBatchPreflightClassification 同一批次中同时混入目标不存在、
// 版本过期、入参非法的条目：预检必须一次性全部列出，且每条的
// Kind 归类正确、下标可定位。
func TestBatchPreflightClassification(t *testing.T) {
	r := New(NewMemoryStore())
	ctx := context.Background()
	mustCreate(t, r, "exists", "sk")  // 版本 1
	mustCreate(t, r, "exists2", "sk") // 版本 1
	mustCreate(t, r, "exists3", "sk") // 版本 1
	mustCreate(t, r, "stale", "sk")   // 版本 1
	vStale, _ := r.Get(ctx, "stale")
	_ = r.InTx(ctx, func(tx *Tx) error { // stale 推到版本 2，使 expected=1 过期
		return tx.Put(ctx, "stale", "sk", []byte("v2"), vStale.Version)
	})

	items := []BatchItem{
		{ID: "new-ok", SortKey: "sk", Data: []byte("a"), Intent: IntentCreate},                      // 0 通过
		{ID: "exists", SortKey: "sk", Data: []byte("b"), Intent: IntentCreate},                      // 1 已存在 -> Conflict
		{ID: "ghost", SortKey: "sk", Data: []byte("c"), Intent: IntentUpdate, ExpectedVersion: 1},   // 2 不存在 -> NotFound
		{ID: "stale", SortKey: "sk", Data: []byte("d"), Intent: IntentUpdate, ExpectedVersion: 1},   // 3 版本过期 -> Conflict
		{ID: "", SortKey: "sk", Data: []byte("e"), Intent: IntentCreate},                            // 4 空 ID -> ContractViolation
		{ID: "exists2", SortKey: "sk", Data: []byte("f"), Intent: IntentUpdate, ExpectedVersion: 0}, // 5 更新缺版本 -> ContractViolation
		{ID: "new-up", SortKey: "sk", Data: []byte("g"), Intent: IntentUpsert},                      // 6 通过（不存在则新建）
		{ID: "exists3", SortKey: "sk", Data: []byte("h"), Intent: IntentUpsert, ExpectedVersion: 1}, // 7 通过（存在且版本匹配）
	}
	t.Logf("输入: %d 个条目, 混合 已存在新建/更新不存在/版本过期/空ID/缺版本/合法项", len(items))

	failures := r.Preflight(ctx, items)
	want := map[int]Kind{
		1: KindConflict,
		2: KindNotFound,
		3: KindConflict,
		4: KindContractViolation,
		5: KindContractViolation,
	}
	if len(failures) != len(want) {
		t.Fatalf("预检应报告 %d 个问题条目, 得到 %d: %+v", len(want), len(failures), failures)
	}
	for _, f := range failures {
		wk, ok := want[f.Index]
		if !ok {
			t.Fatalf("下标 %d 不应失败: %+v", f.Index, f)
		}
		t.Logf("条目[%d] id=%q -> %s (%v)", f.Index, f.ID, f.Kind, f.Err)
		if f.Kind != wk {
			t.Fatalf("条目[%d] 期望 %s, 得到 %s", f.Index, wk, f.Kind)
		}
		if !IsKind(f.Err, wk) {
			t.Fatalf("条目[%d] 的 Err 应可用 IsKind 判定为 %s", f.Index, wk)
		}
		delete(want, f.Index)
	}
	if len(want) != 0 {
		t.Fatalf("遗漏的失败条目: %v", want)
	}
	t.Logf("判定依据: 不存在/版本过期/入参非法三类原因被分别归类且一次性全部列出")
}

// TestBatchWriteAtomicity 预检失败时整批拒绝：合法条目也不得写入。
func TestBatchWriteAtomicity(t *testing.T) {
	r := New(NewMemoryStore())
	ctx := context.Background()
	mustCreate(t, r, "keep", "sk") // 版本 1

	items := []BatchItem{
		{ID: "ok-1", SortKey: "sk", Data: []byte("a"), Intent: IntentCreate},
		{ID: "ok-2", SortKey: "sk", Data: []byte("b"), Intent: IntentUpsert},
		{ID: "ghost", SortKey: "sk", Data: []byte("c"), Intent: IntentUpdate, ExpectedVersion: 1}, // 不存在
	}
	err := r.BatchWrite(ctx, items)
	t.Logf("输入: 2 个合法条目 + 1 个更新不存在目标; BatchWrite -> %v", err)

	var be *BatchError
	if !errors.As(err, &be) {
		t.Fatalf("期望 *BatchError, 得到 %T: %v", err, err)
	}
	if len(be.Failures) != 1 || be.Failures[0].Kind != KindNotFound || be.Failures[0].Index != 2 {
		t.Fatalf("失败归类错误: %+v", be.Failures)
	}
	for _, id := range []string{"ok-1", "ok-2"} {
		if _, err := r.Get(ctx, id); !IsKind(err, KindNotFound) {
			t.Fatalf("预检失败不得残留部分写入 %s, err=%v", id, err)
		}
	}
	keep, _ := r.Get(ctx, "keep")
	if keep.Version != 1 {
		t.Fatalf("既有记录不应被改动, 版本=%d", keep.Version)
	}
	t.Logf("判定依据: 整批拒绝, ok-1/ok-2 未写入, keep 版本仍为 1")
}

// TestBatchWriteSuccess 三种意图混合的合法批次：整体提交成功，
// 版本按语义推进。
func TestBatchWriteSuccess(t *testing.T) {
	r := New(NewMemoryStore())
	ctx := context.Background()
	mustCreate(t, r, "up", "sk-old") // 版本 1
	mustCreate(t, r, "upsert", "s0") // 版本 1

	items := []BatchItem{
		{ID: "created", SortKey: "s1", Data: []byte("c"), Intent: IntentCreate},
		{ID: "up", SortKey: "s2", Data: []byte("u"), Intent: IntentUpdate, ExpectedVersion: 1},
		{ID: "upsert", SortKey: "s3", Data: []byte("x"), Intent: IntentUpsert, ExpectedVersion: 1}, // 存在 -> 更新
		{ID: "upsert-new", SortKey: "s4", Data: []byte("y"), Intent: IntentUpsert},                 // 不存在 -> 新建
	}
	if err := r.BatchWrite(ctx, items); err != nil {
		t.Fatalf("合法批次应成功: %v", err)
	}
	checks := []struct {
		id, sortKey string
		version     uint64
	}{
		{"created", "s1", 1},
		{"up", "s2", 2},
		{"upsert", "s3", 2},
		{"upsert-new", "s4", 1},
	}
	for _, c := range checks {
		it, err := r.Get(ctx, c.id)
		if err != nil {
			t.Fatalf("Get(%s): %v", c.id, err)
		}
		t.Logf("%s -> sortKey=%s version=%d", c.id, it.SortKey, it.Version)
		if it.SortKey != c.sortKey || it.Version != c.version {
			t.Fatalf("%s: 期望 (%s, v%d), 得到 (%s, v%d)",
				c.id, c.sortKey, c.version, it.SortKey, it.Version)
		}
	}
	t.Logf("判定依据: 新建/更新/ upsert 三种意图均按语义生效, 版本正确")
}

// TestBatchPreflightConsistentWithCommit 预检给出的失败归类必须与
// 正式提交路径一致：同一批次、同一状态下，Preflight 与 BatchWrite
// 报告的每个条目 Kind 完全相同；预检通过后目标被并发修改时，
// 提交路径同样归一为 KindConflict。
func TestBatchPreflightConsistentWithCommit(t *testing.T) {
	r := New(NewMemoryStore())
	ctx := context.Background()
	mustCreate(t, r, "a", "sk") // 版本 1

	items := []BatchItem{
		{ID: "a", SortKey: "sk", Data: []byte("x"), Intent: IntentCreate},                           // 已存在
		{ID: "missing", SortKey: "sk", Data: []byte("y"), Intent: IntentUpdate, ExpectedVersion: 3}, // 不存在
		{ID: "a", SortKey: "sk", Data: []byte("z"), Intent: IntentUpdate, ExpectedVersion: 9},       // 版本不符
	}
	// 注意: 批次内 a 出现两次, 第 3 条会先因重复 ID 被判契约违例;
	// 为隔离版本归类, 分开两次比较。
	dupFree := items[:2]
	pf := r.Preflight(ctx, dupFree)
	err := r.BatchWrite(ctx, dupFree)
	var be *BatchError
	if !errors.As(err, &be) {
		t.Fatalf("期望 *BatchError, 得到 %v", err)
	}
	if len(pf) != len(be.Failures) {
		t.Fatalf("预检与提交失败条目数不一致: %d vs %d", len(pf), len(be.Failures))
	}
	for i := range pf {
		t.Logf("条目[%d] 预检=%s 提交=%s", pf[i].Index, pf[i].Kind, be.Failures[i].Kind)
		if pf[i].Index != be.Failures[i].Index || pf[i].Kind != be.Failures[i].Kind {
			t.Fatalf("预检与提交归类不一致: preflight=%+v commit=%+v", pf[i], be.Failures[i])
		}
	}

	// 预检通过后、提交前目标被并发修改 -> KindConflict。
	good := []BatchItem{{ID: "a", SortKey: "sk2", Data: []byte("w"), Intent: IntentUpdate, ExpectedVersion: 1}}
	if pf := r.Preflight(ctx, good); len(pf) != 0 {
		t.Fatalf("预检应通过: %+v", pf)
	}
	_ = r.InTx(ctx, func(tx *Tx) error { // 并发者把 a 推到版本 2
		return tx.Put(ctx, "a", "sk", []byte("other"), 1)
	})
	err = r.BatchWrite(ctx, good)
	t.Logf("并发修改后 BatchWrite -> %v", err)
	var be2 *BatchError
	if !errors.As(err, &be2) || len(be2.Failures) != 1 ||
		be2.Failures[0].Kind != KindConflict {
		t.Fatalf("并发修改后应得到 KindConflict 归类, 得到 %v", err)
	}
	t.Logf("判定依据: 预检与提交路径的失败 Kind 逐条一致, 并发变更归一为 KindConflict")
}

// TestBatchDuplicateID 同一批次内重复 ID 属于入参不合法。
func TestBatchDuplicateID(t *testing.T) {
	r := New(NewMemoryStore())
	items := []BatchItem{
		{ID: "dup", SortKey: "sk", Data: []byte("1"), Intent: IntentCreate},
		{ID: "dup", SortKey: "sk", Data: []byte("2"), Intent: IntentCreate},
	}
	failures := r.Preflight(context.Background(), items)
	if len(failures) != 1 || failures[0].Index != 1 || failures[0].Kind != KindContractViolation {
		t.Fatalf("重复 ID 应判 KindContractViolation: %+v", failures)
	}
	t.Logf("判定依据: 第二次出现的 dup 在下标 1 处被判 %s", failures[0].Kind)
}

// TestBatchBackendConsistency 同样的批量场景在全部后端上
// 预检归类与提交结果必须一致。
func TestBatchBackendConsistency(t *testing.T) {
	backends := map[string]func() Store{
		"memory":  func() Store { return NewMemoryStore() },
		"limited": func() Store { return NewLimitedStore(2) },
		"slice":   func() Store { return &sliceStore{} },
	}
	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			r := New(mk())
			ctx := context.Background()
			mustCreate(t, r, "e", "sk")  // 版本 1
			mustCreate(t, r, "e2", "sk") // 版本 1

			bad := []BatchItem{
				{ID: "e", SortKey: "sk", Intent: IntentCreate},
				{ID: "nope", SortKey: "sk", Intent: IntentUpdate, ExpectedVersion: 1},
				{ID: "e2", SortKey: "sk", Intent: IntentUpdate, ExpectedVersion: 7},
			}
			failures := r.Preflight(ctx, bad)
			got := make([]Kind, len(failures))
			for i, f := range failures {
				got[i] = f.Kind
			}
			want := []Kind{KindConflict, KindNotFound, KindConflict}
			t.Logf("后端 %s 预检归类: %v", name, got)
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("后端 %s 归类不一致: got=%v want=%v", name, got, want)
			}
			if err := r.BatchWrite(ctx, bad); err == nil {
				t.Fatalf("后端 %s: 非法批次应被拒绝", name)
			}

			good := []BatchItem{
				{ID: "n1", SortKey: "s1", Intent: IntentCreate},
				{ID: "e", SortKey: "s2", Intent: IntentUpdate, ExpectedVersion: 1},
				{ID: "n2", SortKey: "s3", Intent: IntentUpsert},
			}
			if err := r.BatchWrite(ctx, good); err != nil {
				t.Fatalf("后端 %s: 合法批次应成功: %v", name, err)
			}
			it, _ := r.Get(ctx, "e")
			if it.SortKey != "s2" || it.Version != 2 {
				t.Fatalf("后端 %s: 更新结果不一致: %+v", name, it)
			}
		})
	}
}
