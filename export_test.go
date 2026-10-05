package repository

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// exportAll 用同一 spec 连续翻页直到没有下一页，返回全部 ID。
func exportAll(t *testing.T, r *Repository, spec ExportSpec) []string {
	t.Helper()
	var out []string
	cursor := ""
	for {
		p, err := r.Export(context.Background(), spec, cursor)
		if err != nil {
			t.Fatalf("export: %v", err)
		}
		out = append(out, idsOf(p)...)
		if p.NextCursor == "" {
			return out
		}
		cursor = p.NextCursor
	}
}

// seedExportData 写入 25 条记录：排序键两两重复，一半带前缀 "a/"，一半带 "b/"。
func seedExportData(t *testing.T, r *Repository) {
	t.Helper()
	for i := 0; i < 25; i++ {
		prefix := "a/"
		if i%2 == 1 {
			prefix = "b/"
		}
		mustCreate(t, r, fmt.Sprintf("id-%02d", i), fmt.Sprintf("%ssk-%02d", prefix, i/2))
	}
}

// TestExportResumeAcrossInstances 跨“进程”（新仓储实例、新调用点）续传：
// 不重复、不遗漏，筛选条件生效。
func TestExportResumeAcrossInstances(t *testing.T) {
	store := NewMemoryStore()
	r1 := New(store)
	seedExportData(t, r1)
	spec := ExportSpec{SortKeyPrefix: "a/", Limit: 4}
	ctx := context.Background()

	p1, err := r1.Export(ctx, spec, "")
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if len(p1.Items) != 4 || p1.NextCursor == "" {
		t.Fatalf("page1 应有 4 条且有续传游标, 得到 %d 条", len(p1.Items))
	}
	t.Logf("page1=%v cursor=%q", idsOf(p1), p1.NextCursor)

	// 模拟进程重启/换调用点：用同一个后端构造全新的仓储实例继续读。
	r2 := New(store)
	var rest []string
	cursor := p1.NextCursor
	for cursor != "" {
		p, err := r2.Export(ctx, spec, cursor)
		if err != nil {
			t.Fatalf("resume: %v", err)
		}
		rest = append(rest, idsOf(p)...)
		cursor = p.NextCursor
	}
	all := append(idsOf(p1), rest...)
	t.Logf("续传全量: %v", all)

	// 13 条 a/ 记录，不重复不遗漏。
	if len(all) != 13 {
		t.Fatalf("筛选 a/ 应有 13 条, 得到 %d: %v", len(all), all)
	}
	seen := map[string]bool{}
	for i, id := range all {
		if seen[id] {
			t.Fatalf("记录 %s 重复返回", id)
		}
		seen[id] = true
		want := fmt.Sprintf("id-%02d", i*2) // a/ 对应偶数编号
		if id != want {
			t.Fatalf("第 %d 条期望 %s, 得到 %s", i, want, id)
		}
	}
	t.Logf("判定依据: 跨实例续传 13 条 a/ 记录, 顺序确定, 无重复无遗漏")
}

// TestExportChunkedBackend 单次读取有上限的后端上，
// 大数据量翻页结果与内存后端完全一致（不依赖一次性取出全部记录）。
func TestExportChunkedBackend(t *testing.T) {
	backends := map[string]func() Store{
		"memory":     func() Store { return NewMemoryStore() },
		"chunked(3)": func() Store { return NewChunkedStore(3) },
		"chunked(1)": func() Store { return NewChunkedStore(1) },
	}
	results := map[string][]string{}
	for name, mk := range backends {
		r := New(mk())
		seedExportData(t, r)
		got := exportAll(t, r, ExportSpec{Limit: 4})
		results[name] = got
		t.Logf("后端 %s 全量顺序(%d 条): %v", name, len(got), got)
	}
	base := results["memory"]
	for name, got := range results {
		if strings.Join(got, ",") != strings.Join(base, ",") {
			t.Fatalf("后端 %s 与 memory 结果不一致:\n%v\n%v", name, got, base)
		}
	}
	if len(base) != 25 {
		t.Fatalf("应导出全部 25 条, 得到 %d", len(base))
	}
	t.Logf("判定依据: 限流后端与内存后端 25 条全量顺序完全一致")
}

// TestExportStableOrderDuplicateSortKeys 排序键全部重复时，
// 多轮导出顺序完全确定（按主键决胜）。
func TestExportStableOrderDuplicateSortKeys(t *testing.T) {
	r := New(NewChunkedStore(2)) // 限流后端 + 重复排序键
	for i := 0; i < 20; i++ {
		mustCreate(t, r, fmt.Sprintf("id-%02d", i), "same")
	}
	spec := ExportSpec{Limit: 3}
	first := exportAll(t, r, spec)
	t.Logf("基准顺序: %v", first)
	for round := 0; round < 5; round++ {
		got := exportAll(t, r, spec)
		if strings.Join(got, ",") != strings.Join(first, ",") {
			t.Fatalf("第 %d 轮顺序漂移:\n%v\n%v", round, got, first)
		}
	}
	for i := 1; i < len(first); i++ {
		if first[i] < first[i-1] {
			t.Fatalf("排序键全相同时应按主键升序, 得到 %v", first)
		}
	}
	t.Logf("判定依据: 6 轮全量导出顺序完全一致且按主键升序")
}

// TestExportCursorRejection 各类非法游标必须被明确拒绝（KindInvalidCursor）。
func TestExportCursorRejection(t *testing.T) {
	r := New(NewMemoryStore())
	seedExportData(t, r)
	ctx := context.Background()
	specA := ExportSpec{SortKeyPrefix: "a/", Limit: 4}
	specB := ExportSpec{SortKeyPrefix: "b/", Limit: 4}
	specAll := ExportSpec{Limit: 4}

	p1, err := r.Export(ctx, specA, "")
	if err != nil || p1.NextCursor == "" {
		t.Fatalf("准备游标失败: %v", err)
	}
	good := p1.NextCursor

	// 构造各类非法游标。
	raw, _ := base64.RawURLEncoding.DecodeString(good)
	tampered := base64.RawURLEncoding.EncodeToString(
		[]byte(strings.Replace(string(raw), `"a/`, `"x/`, 1))) // 篡改排序键
	truncated := good[:len(good)-4]     // 截断
	listPage, err := r.List(ctx, 4, "") // 旧格式（List v1 游标）
	if err != nil || listPage.NextCursor == "" {
		t.Fatalf("准备旧格式游标失败: %v", err)
	}
	oldFormat := listPage.NextCursor
	incomplete := base64.RawURLEncoding.EncodeToString([]byte(`{"v":2,"fp":"x"}`)) // 结构不完整
	unknownField := base64.RawURLEncoding.EncodeToString(
		[]byte(`{"v":2,"fp":"x","s":"y","k":"z","sum":"w","extra":1}`)) // 多余字段

	cases := map[string]struct {
		spec   ExportSpec
		cursor string
	}{
		"来自其他筛选条件": {specB, good},
		"来自无筛选查询":  {specAll, good},
		"篡改排序键":    {specA, tampered},
		"截断":       {specA, truncated},
		"旧格式游标":    {specA, oldFormat},
		"结构不完整":    {specA, incomplete},
		"多余字段":     {specA, unknownField},
		"非base64":  {specA, "!!!not-a-cursor!!!"},
		"非json":    {specA, base64.RawURLEncoding.EncodeToString([]byte("hello"))},
	}
	for name, tc := range cases {
		_, err := r.Export(ctx, tc.spec, tc.cursor)
		t.Logf("%s -> %v", name, err)
		if !IsKind(err, KindInvalidCursor) {
			t.Fatalf("%s: 期望 KindInvalidCursor, 得到 %v", name, err)
		}
	}
	// 合法游标在相同条件下仍然可用（对照组）。
	if _, err := r.Export(ctx, specA, good); err != nil {
		t.Fatalf("合法游标应可用: %v", err)
	}
	// 旧格式游标在 List 中仍可用，但导出游标不能被 List 接受（互不混用）。
	if _, err := r.List(ctx, 4, oldFormat); err != nil {
		t.Fatalf("旧格式游标在 List 中应可用: %v", err)
	}
	if _, err := r.List(ctx, 4, good); !IsKind(err, KindInvalidCursor) {
		t.Fatalf("导出游标不应被 List 接受, 得到 %v", err)
	}
	t.Logf("判定依据: 9 类非法游标全部归类 KindInvalidCursor, 合法游标对照可用")
}

// TestExportDuringConcurrentWrites 导出翻页期间并发写入：
// 已返回记录不重复，游标始终有效（配合 -race 检测竞争）。
func TestExportDuringConcurrentWrites(t *testing.T) {
	store := NewChunkedStore(3)
	r := New(store)
	ctx := context.Background()
	for i := 0; i < 30; i++ {
		mustCreate(t, r, fmt.Sprintf("id-%03d", i), fmt.Sprintf("sk-%03d", i))
	}
	spec := ExportSpec{Limit: 4}

	var wg sync.WaitGroup
	// 写者：持续追加排在后面的新记录。
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				id := fmt.Sprintf("w%d-%03d", w, i)
				_ = r.InTx(ctx, func(tx *Tx) error {
					return tx.Put(ctx, id, "sk-"+id, []byte("x"), 0)
				})
			}
		}(w)
	}
	// 读者：并发翻页，校验每次完整遍历无重复且顺序单调。
	errCh := make(chan error, 4)
	for rd := 0; rd < 4; rd++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			seen := map[string]bool{}
			var prev Item
			cursor := ""
			for {
				p, err := r.Export(ctx, spec, cursor)
				if err != nil {
					errCh <- fmt.Errorf("export: %w", err)
					return
				}
				for _, it := range p.Items {
					if seen[it.ID] {
						errCh <- fmt.Errorf("记录 %s 重复返回", it.ID)
						return
					}
					seen[it.ID] = true
					if prev.ID != "" && compareItems(it, prev) <= 0 {
						errCh <- fmt.Errorf("顺序非单调: %v 后接 %v", prev, it)
						return
					}
					prev = it
				}
				if p.NextCursor == "" {
					break
				}
				cursor = p.NextCursor
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("并发导出校验失败: %v", err)
	}
	t.Logf("判定依据: 4 写者 x 4 读者并发, 每次遍历无重复且顺序单调, -race 无竞争")
}

// TestBackendConsistencyBatchAndExport 批量预检/提交与续传导出在
// 三个后端上对外结果完全一致。
func TestBackendConsistencyBatchAndExport(t *testing.T) {
	backends := map[string]func() Store{
		"memory":     func() Store { return NewMemoryStore() },
		"slice":      func() Store { return &sliceStore{} },
		"chunked(2)": func() Store { return NewChunkedStore(2) },
	}
	type outcome struct {
		preflight [][2]interface{}
		applyErr  [][2]interface{}
		exported  []string
	}
	run := func(mk func() Store) outcome {
		r := New(mk())
		ctx := context.Background()
		mustCreate(t, r, "keep", "sk-1")
		entries := []BatchEntry{
			{ID: "n1", SortKey: "sk-0", Intent: IntentCreate},
			{ID: "ghost", SortKey: "sk-2", Intent: IntentUpdate, Version: 1},
			{ID: "keep", SortKey: "sk-3", Intent: IntentUpdate, Version: 7},
			{ID: "n2", SortKey: "sk-4", Intent: IntentUpsert},
		}
		var o outcome
		o.preflight = entryKinds(r.PreflightBatch(ctx, entries))
		o.applyErr = entryKinds(BatchErrors(r.ApplyBatch(ctx, entries)))
		// 修正批次后提交成功，再导出。
		entries[1] = BatchEntry{ID: "keep", SortKey: "sk-2", Intent: IntentUpdate, Version: 1}
		entries[2] = BatchEntry{ID: "n3", SortKey: "sk-3", Intent: IntentCreate}
		if err := r.ApplyBatch(ctx, entries); err != nil {
			t.Fatalf("修正后批次应成功: %v", err)
		}
		o.exported = exportAll(t, r, ExportSpec{Limit: 2})
		return o
	}
	base := run(backends["memory"])
	t.Logf("后端 memory: preflight=%v applyErr=%v exported=%v",
		base.preflight, base.applyErr, base.exported)
	for name, mk := range backends {
		if name == "memory" {
			continue
		}
		got := run(mk)
		t.Logf("后端 %s: preflight=%v applyErr=%v exported=%v",
			name, got.preflight, got.applyErr, got.exported)
		if fmt.Sprint(got) != fmt.Sprint(base) {
			t.Fatalf("后端 %s 与 memory 对外结果不一致:\n%+v\n%+v", name, got, base)
		}
	}
	wantKinds := [][2]interface{}{{1, KindNotFound}, {2, KindConflict}}
	if fmt.Sprint(base.preflight) != fmt.Sprint(wantKinds) ||
		fmt.Sprint(base.applyErr) != fmt.Sprint(wantKinds) {
		t.Fatalf("归类基准不符: preflight=%v applyErr=%v", base.preflight, base.applyErr)
	}
	t.Logf("判定依据: 三后端预检归类、提交失败归类、导出序列完全一致")
}

// flakyStore 在 Get 上返回未归一化的原始错误，验证后端错误细节不外泄。
type flakyStore struct{ Store }

func (f flakyStore) Get(ctx context.Context, key string) (Record, error) {
	return Record{}, fmt.Errorf("secret backend detail: /private/path/db.lock")
}

// TestBackendErrorNotLeaked 后端原始错误不得透给调用方。
func TestBackendErrorNotLeaked(t *testing.T) {
	r := New(flakyStore{NewMemoryStore()})
	_, err := r.Get(context.Background(), "x")
	t.Logf("Get -> %v", err)
	if !IsKind(err, KindInternal) {
		t.Fatalf("期望 KindInternal, 得到 %v", err)
	}
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "/private/") {
		t.Fatalf("后端错误细节泄漏: %v", err)
	}
	// 批量预检中的后端错误同样归类为条目级 KindInternal 且不外泄。
	errs := r.PreflightBatch(context.Background(), []BatchEntry{
		{ID: "x", SortKey: "k", Intent: IntentUpsert},
	})
	if len(errs) != 1 || errs[0].Kind != KindInternal ||
		strings.Contains(errs[0].Err.Error(), "secret") {
		t.Fatalf("预检中的后端错误应归类 KindInternal 且不外泄, 得到 %v", errs)
	}
	t.Logf("判定依据: 后端细节未出现在对外错误中")
}
