package repository

import (
	"context"
	"fmt"
	"testing"
)

// entryKinds 提取条目错误的 (下标, 归类) 序列，便于断言。
func entryKinds(errs []EntryError) [][2]interface{} {
	out := make([][2]interface{}, 0, len(errs))
	for _, e := range errs {
		out = append(out, [2]interface{}{e.Index, e.Kind})
	}
	return out
}

// TestBatchPreflightClassification 整批预检：同一批里同时混入
// 目标不存在、版本过期、入参非法的条目，必须一次性全部列出且归类正确。
func TestBatchPreflightClassification(t *testing.T) {
	r := New(NewMemoryStore())
	mustCreate(t, r, "exist", "k")   // 版本 1
	mustCreate(t, r, "exist2", "k")  // 版本 1
	mustCreate(t, r, "stale", "k2")  // 版本 1
	mustCreate(t, r, "stale2", "k2") // 版本 1
	// 把 stale/stale2 推到版本 2，使携带版本 1 的写入过期。
	for _, id := range []string{"stale", "stale2"} {
		if err := r.InTx(context.Background(), func(tx *Tx) error {
			return tx.Put(context.Background(), id, "k2", []byte("v2"), 1)
		}); err != nil {
			t.Fatalf("bump %s: %v", id, err)
		}
	}

	entries := []BatchEntry{
		{ID: "ok-create", SortKey: "a", Intent: IntentCreate},           // 0: 合法
		{ID: "exist", SortKey: "b", Intent: IntentCreate},               // 1: 已存在 -> Conflict
		{ID: "ghost", SortKey: "c", Intent: IntentUpdate, Version: 1},   // 2: 目标不存在 -> NotFound
		{ID: "stale", SortKey: "d", Intent: IntentUpdate, Version: 1},   // 3: 版本过期 -> Conflict
		{ID: "", SortKey: "e", Intent: IntentCreate},                    // 4: 空 ID -> ContractViolation
		{ID: "bad-ver", SortKey: "f", Intent: IntentCreate, Version: 3}, // 5: 新建带版本 -> ContractViolation
		{ID: "no-ver", SortKey: "g", Intent: IntentUpdate},              // 6: 更新无版本 -> ContractViolation
		{ID: "weird", SortKey: "h", Intent: Intent(99)},                 // 7: 未知意图 -> ContractViolation
		{ID: "ok-upsert", SortKey: "i", Intent: IntentUpsert},           // 8: 合法
		{ID: "ok-upsert", SortKey: "i", Intent: IntentUpsert},           // 9: 批内重复 -> ContractViolation
		{ID: "exist2", SortKey: "j", Intent: IntentUpsert, Version: 1},  // 10: 合法（版本匹配）
		{ID: "stale2", SortKey: "l", Intent: IntentUpsert, Version: 1},  // 11: 版本过期 -> Conflict
	}
	t.Logf("输入: %d 个条目, 混合合法/已存在/不存在/版本过期/入参非法", len(entries))

	errs := r.PreflightBatch(context.Background(), entries)
	got := entryKinds(errs)
	want := [][2]interface{}{
		{1, KindConflict},
		{2, KindNotFound},
		{3, KindConflict},
		{4, KindContractViolation},
		{5, KindContractViolation},
		{6, KindContractViolation},
		{7, KindContractViolation},
		{9, KindContractViolation},
		{11, KindConflict},
	}
	t.Logf("预检归类: %v", got)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("预检归类不符:\n got=%v\nwant=%v", got, want)
	}

	// 正式提交失败时的归类必须与预检一致。
	applyErr := r.ApplyBatch(context.Background(), entries)
	batchErrs := BatchErrors(applyErr)
	if batchErrs == nil {
		t.Fatalf("ApplyBatch 应返回 *BatchError, 得到 %T: %v", applyErr, applyErr)
	}
	if fmt.Sprint(entryKinds(batchErrs)) != fmt.Sprint(want) {
		t.Fatalf("提交失败归类与预检不一致:\n preflight=%v\n commit=%v", want, entryKinds(batchErrs))
	}
	t.Logf("判定依据: 预检与提交归类一致, 共 %d 个问题条目一次性列出", len(want))

	// 原子性：整批拒绝时合法条目也不得生效。
	for _, id := range []string{"ok-create", "ok-upsert"} {
		if _, err := r.Get(context.Background(), id); !IsKind(err, KindNotFound) {
			t.Fatalf("整批拒绝后 %q 不应存在, err=%v", id, err)
		}
	}
	// 已有记录不得被改动。
	exist, _ := r.Get(context.Background(), "exist")
	stale, _ := r.Get(context.Background(), "stale")
	if exist.Version != 1 || stale.Version != 2 {
		t.Fatalf("已有记录被部分写入: exist@v%d stale@v%d", exist.Version, stale.Version)
	}
	t.Logf("判定依据: 整批拒绝后合法条目未生效, 已有记录版本未变")
}

// TestBatchIntentsSemantics 三种意图各自的通过/拒绝语义。
func TestBatchIntentsSemantics(t *testing.T) {
	r := New(NewMemoryStore())
	mustCreate(t, r, "doc", "k") // 版本 1
	ctx := context.Background()

	// Upsert 不存在则新建；Update 正常更新；Create 新建。
	err := r.ApplyBatch(ctx, []BatchEntry{
		{ID: "up-new", SortKey: "a", Intent: IntentUpsert},
		{ID: "doc", SortKey: "b", Intent: IntentUpdate, Version: 1},
		{ID: "cr-new", SortKey: "c", Intent: IntentCreate},
	})
	if err != nil {
		t.Fatalf("合法批次应成功: %v", err)
	}
	upNew, _ := r.Get(ctx, "up-new")
	doc, _ := r.Get(ctx, "doc")
	crNew, _ := r.Get(ctx, "cr-new")
	t.Logf("提交后: up-new@v%d doc@v%d doc.SortKey=%q cr-new@v%d",
		upNew.Version, doc.Version, doc.SortKey, crNew.Version)
	if upNew.Version != 1 || doc.Version != 2 || doc.SortKey != "b" || crNew.Version != 1 {
		t.Fatalf("提交结果不符: %+v %+v %+v", upNew, doc, crNew)
	}

	// Upsert 已存在且版本匹配 -> 更新；版本不匹配 -> Conflict。
	err = r.ApplyBatch(ctx, []BatchEntry{
		{ID: "doc", SortKey: "b2", Intent: IntentUpsert, Version: 2},
	})
	if err != nil {
		t.Fatalf("版本匹配的 upsert 应成功: %v", err)
	}
	err = r.ApplyBatch(ctx, []BatchEntry{
		{ID: "doc", SortKey: "b3", Intent: IntentUpsert, Version: 2}, // 当前已是 v3
	})
	if got := BatchErrors(err); len(got) != 1 || got[0].Kind != KindConflict {
		t.Fatalf("过期 upsert 应归类 KindConflict, 得到 %v", err)
	}
	doc, _ = r.Get(ctx, "doc")
	if doc.Version != 3 || doc.SortKey != "b2" {
		t.Fatalf("过期 upsert 不得生效: %+v", doc)
	}

	// Upsert 盲写（Version=0）不做版本校验。
	if err := r.ApplyBatch(ctx, []BatchEntry{
		{ID: "doc", SortKey: "b4", Intent: IntentUpsert},
	}); err != nil {
		t.Fatalf("盲写 upsert 应成功: %v", err)
	}
	doc, _ = r.Get(ctx, "doc")
	if doc.SortKey != "b4" {
		t.Fatalf("盲写 upsert 未生效: %+v", doc)
	}
	t.Logf("判定依据: create/update/upsert 及盲写 upsert 语义均符合预期")
}

// TestBatchAtomicityOnCommit 提交期失败（版本过期）同样整体拒绝：
// 同事务里的合法创建不得残留。
func TestBatchAtomicityOnCommit(t *testing.T) {
	r := New(NewMemoryStore())
	mustCreate(t, r, "base", "k") // 版本 1
	ctx := context.Background()

	err := r.ApplyBatch(ctx, []BatchEntry{
		{ID: "new-1", SortKey: "a", Intent: IntentCreate},
		{ID: "new-2", SortKey: "b", Intent: IntentCreate},
		{ID: "base", SortKey: "k", Intent: IntentUpdate, Version: 99}, // 版本过期
	})
	if got := BatchErrors(err); len(got) != 1 || got[0].Kind != KindConflict || got[0].Index != 2 {
		t.Fatalf("期望 entry[2] KindConflict, 得到 %v", err)
	}
	for _, id := range []string{"new-1", "new-2"} {
		if _, err := r.Get(ctx, id); !IsKind(err, KindNotFound) {
			t.Fatalf("提交失败后 %q 不应存在, err=%v", id, err)
		}
	}
	base, _ := r.Get(ctx, "base")
	t.Logf("判定依据: 整批拒绝后 new-1/new-2 未残留, base 版本=%d (期望1)", base.Version)
	if base.Version != 1 {
		t.Fatalf("base 版本应为 1, 得到 %d", base.Version)
	}
}

// TestBatchPreflightThenCommitRace 预检通过到正式提交之间状态被并发修改时，
// 提交失败仍以条目级归类报告（而非笼统错误），且不产生部分写入。
func TestBatchPreflightThenCommitRace(t *testing.T) {
	r := New(NewMemoryStore())
	mustCreate(t, r, "victim", "k") // 版本 1
	ctx := context.Background()
	entries := []BatchEntry{
		{ID: "fresh", SortKey: "a", Intent: IntentCreate},
		{ID: "victim", SortKey: "k2", Intent: IntentUpdate, Version: 1},
	}
	if errs := r.PreflightBatch(ctx, entries); len(errs) != 0 {
		t.Fatalf("预检应全部通过, 得到 %v", errs)
	}
	// 模拟并发修改：victim 被推到版本 2。
	if err := r.InTx(ctx, func(tx *Tx) error {
		return tx.Put(ctx, "victim", "k2", []byte("other"), 1)
	}); err != nil {
		t.Fatalf("concurrent bump: %v", err)
	}
	err := r.ApplyBatch(ctx, entries)
	got := BatchErrors(err)
	t.Logf("并发修改后提交 -> %v", err)
	if len(got) != 1 || got[0].Index != 1 || got[0].Kind != KindConflict {
		t.Fatalf("期望 entry[1] KindConflict, 得到 %v", got)
	}
	if _, err := r.Get(ctx, "fresh"); !IsKind(err, KindNotFound) {
		t.Fatalf("冲突提交不得残留部分写入 fresh, err=%v", err)
	}
	victim, _ := r.Get(ctx, "victim")
	if victim.Version != 2 {
		t.Fatalf("victim 不应被过期写覆盖, 版本=%d", victim.Version)
	}
	t.Logf("判定依据: 提交期冲突归类为条目级 KindConflict, 无部分写入")
}
