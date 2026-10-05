package repository

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
)

// collectAll 用同一查询反复翻页直到游标耗尽，返回全部 ID。
func collectAll(t *testing.T, r *Repository, q Query, limit int) []string {
	t.Helper()
	var out []string
	cursor := ""
	for {
		p, err := r.Export(context.Background(), q, limit, cursor)
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

// TestExportResumeAcrossCallSites 续传：一个调用点读出一页后，
// 换一个新的 Repository（模拟进程重启/另一个调用点）持游标继续读，
// 全程不重复、不遗漏。
func TestExportResumeAcrossCallSites(t *testing.T) {
	store := NewMemoryStore()
	r1 := New(store)
	ctx := context.Background()
	q := Query{SortKeyPrefix: "exp/"}
	// 20 条匹配记录 + 5 条不匹配记录（干扰项）。
	for i := 0; i < 20; i++ {
		mustCreate(t, r1, fmt.Sprintf("m-%02d", i), fmt.Sprintf("exp/%02d", i%4))
	}
	for i := 0; i < 5; i++ {
		mustCreate(t, r1, fmt.Sprintf("x-%02d", i), fmt.Sprintf("other/%02d", i))
	}

	// 调用点 1：只读第一页，游标作为字符串交出去（可落盘/跨进程传递）。
	p1, err := r1.Export(ctx, q, 7, "")
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	handoff := p1.NextCursor
	t.Logf("调用点1 读出 %v, 移交游标 %q...", idsOf(p1), handoff[:16])
	if handoff == "" {
		t.Fatalf("第一页应有后续游标")
	}

	// 调用点 2：全新 Repository 实例（同一后端），仅凭游标字符串续读。
	r2 := New(store)
	var rest []string
	cursor := handoff
	for {
		p, err := r2.Export(ctx, q, 7, cursor)
		if err != nil {
			t.Fatalf("resume: %v", err)
		}
		rest = append(rest, idsOf(p)...)
		if p.NextCursor == "" {
			break
		}
		cursor = p.NextCursor
	}
	all := append(idsOf(p1), rest...)
	t.Logf("续读得到 %v", rest)

	// 期望：20 条匹配记录各出现且仅出现一次，顺序确定。
	want := collectAll(t, New(store), q, 100) // 一次性读出的基准顺序
	if len(all) != 20 {
		t.Fatalf("应恰好覆盖 20 条匹配记录, 得到 %d: %v", len(all), all)
	}
	seen := map[string]int{}
	for _, id := range all {
		seen[id]++
		if seen[id] > 1 {
			t.Fatalf("记录 %s 被重复返回", id)
		}
		if strings.HasPrefix(id, "x-") {
			t.Fatalf("不匹配筛选条件的 %s 不应出现", id)
		}
	}
	for i := range want {
		if all[i] != want[i] {
			t.Fatalf("续传顺序与基准不一致: got=%v want=%v", all, want)
		}
	}
	t.Logf("判定依据: 跨调用点续读 20 条无重复无遗漏, 顺序与一次性读取一致")
}

// TestExportStreamingWithLimitedBackend 单次读取受限的后端上，
// 一页导出必须通过多次键集续扫完成，不能一次性全量拉取。
func TestExportStreamingWithLimitedBackend(t *testing.T) {
	// 统计后端的实际读取行为。
	type stats struct {
		mu       sync.Mutex
		calls    int
		maxBatch int
	}
	st := &stats{}
	base := NewLimitedStore(2) // 单次最多返回 2 条
	store := &scanStatStore{Store: base, onScan: func(n int) {
		st.mu.Lock()
		st.calls++
		if n > st.maxBatch {
			st.maxBatch = n
		}
		st.mu.Unlock()
	}}
	r := New(store)
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		mustCreate(t, r, fmt.Sprintf("id-%02d", i), fmt.Sprintf("sk-%02d", i%5))
	}

	p, err := r.Export(ctx, Query{}, 9, "")
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	t.Logf("一页导出 9 条: 后端被调用 %d 次, 单次最多返回 %d 条", st.calls, st.maxBatch)
	if len(p.Items) != 9 || p.NextCursor == "" {
		t.Fatalf("应返回 9 条并带游标, 得到 %d 条", len(p.Items))
	}
	if st.maxBatch > 2 {
		t.Fatalf("后端单次返回不应超过 2 条, 实际 %d", st.maxBatch)
	}
	if st.calls < 2 {
		t.Fatalf("受限后端上一页导出应多次续扫, 实际调用 %d 次", st.calls)
	}

	// 全量遍历：50 条记录完整覆盖，顺序为 (SortKey, ID) 升序。
	all := collectAll(t, r, Query{}, 9)
	if len(all) != 50 {
		t.Fatalf("应覆盖全部 50 条, 得到 %d", len(all))
	}
	var want []string
	for sk := 0; sk < 5; sk++ {
		for i := 0; i < 50; i++ {
			if i%5 == sk {
				want = append(want, fmt.Sprintf("id-%02d", i))
			}
		}
	}
	for i := range want {
		if all[i] != want[i] {
			t.Fatalf("顺序应为 (SortKey, ID) 升序: got=%v want=%v", all, want)
		}
	}
	t.Logf("判定依据: 单页经多次受限扫描拼成, 全量 50 条无重复无遗漏")
}

// scanStatStore 包装 Store，统计 ScanSorted 每次返回的条数。
type scanStatStore struct {
	Store
	onScan func(n int)
}

func (s *scanStatStore) ScanSorted(ctx context.Context, after ScanPosition, limit int) ([]Record, error) {
	recs, err := s.Store.ScanSorted(ctx, after, limit)
	s.onScan(len(recs))
	return recs, err
}

// TestExportDuplicateSortKeysStable 排序键大量重复时，
// 导出顺序完全确定，多轮遍历结果一致。
func TestExportDuplicateSortKeysStable(t *testing.T) {
	r := New(NewLimitedStore(3))
	for i := 0; i < 30; i++ {
		mustCreate(t, r, fmt.Sprintf("id-%02d", i), "same")
	}
	var first []string
	for round := 0; round < 4; round++ {
		got := collectAll(t, r, Query{}, 4)
		if first == nil {
			first = got
			continue
		}
		for i := range got {
			if got[i] != first[i] {
				t.Fatalf("第 %d 轮顺序漂移: %v != %v", round, got, first)
			}
		}
	}
	if !sort.StringsAreSorted(first) {
		t.Fatalf("排序键全相同时应按 ID 升序: %v", first)
	}
	t.Logf("判定依据: 4 轮遍历顺序完全一致且按 ID 升序: %v...", first[:5])
}

// TestExportCursorRejected 各类非法游标必须被明确拒绝。
func TestExportCursorRejected(t *testing.T) {
	r := New(NewMemoryStore())
	ctx := context.Background()
	q := Query{SortKeyPrefix: "a"}
	for i := 0; i < 6; i++ {
		mustCreate(t, r, fmt.Sprintf("id-%d", i), fmt.Sprintf("a/%d", i))
	}
	p, err := r.Export(ctx, q, 2, "")
	if err != nil || p.NextCursor == "" {
		t.Fatalf("构造合法游标失败: %v", err)
	}
	valid := p.NextCursor

	// 篡改：解码后改动位置字段再编码（校验和不匹配）。
	raw, _ := base64.RawURLEncoding.DecodeString(valid)
	tampered := base64.RawURLEncoding.EncodeToString(
		[]byte(strings.Replace(string(raw), `"k":"`, `"k":"Z`, 1)))
	// 截断：去掉尾部字符。
	truncated := valid[:len(valid)-3]
	// 旧格式：v1 的 List 游标。
	v1 := encodeCursor(cursor{SortKey: "a/0", Key: "id-0"})
	// 结构不完整：缺少位置字段 k。
	incomplete := base64.RawURLEncoding.EncodeToString([]byte(`{"v":2,"q":"x","s":"a/0"}`))
	// 版本不符。
	wrongVer := base64.RawURLEncoding.EncodeToString([]byte(`{"v":99,"q":"x","s":"a/0","k":"id-0","c":1}`))

	cases := map[string]string{
		"篡改":      tampered,
		"截断":      truncated,
		"旧格式":     v1,
		"结构不完整":   incomplete,
		"版本不符":    wrongVer,
		"非base64": "!!!not-a-cursor!!!",
	}
	for name, c := range cases {
		_, err := r.Export(ctx, q, 2, c)
		t.Logf("游标[%s] -> %v", name, err)
		if !IsKind(err, KindInvalidCursor) {
			t.Fatalf("游标[%s] 期望 KindInvalidCursor, 得到 %v", name, err)
		}
	}

	// 来自其他查询的游标：同一游标换一个筛选条件使用。
	_, err = r.Export(ctx, Query{SortKeyPrefix: "b"}, 2, valid)
	t.Logf("其他查询的游标 -> %v", err)
	if !IsKind(err, KindInvalidCursor) {
		t.Fatalf("来自其他查询的游标应被拒绝, 得到 %v", err)
	}
	// 同一查询下游标仍然有效（对照组）。
	if _, err := r.Export(ctx, q, 2, valid); err != nil {
		t.Fatalf("合法游标不应被拒绝: %v", err)
	}
	t.Logf("判定依据: 篡改/截断/旧格式/不完整/异查询游标均被拒, 本查询游标正常")
}

// TestExportBackendConsistency 同一导出场景在全部后端上结果一致，
// 非法游标拒绝行为一致，后端错误细节不外泄。
func TestExportBackendConsistency(t *testing.T) {
	backends := map[string]func() Store{
		"memory":  func() Store { return NewMemoryStore() },
		"limited": func() Store { return NewLimitedStore(2) },
		"slice":   func() Store { return &sliceStore{} },
	}
	q := Query{SortKeyPrefix: "p/"}
	var want []string
	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			r := New(mk())
			for i := 0; i < 15; i++ {
				mustCreate(t, r, fmt.Sprintf("id-%02d", i), fmt.Sprintf("p/%02d", i%3))
			}
			got := collectAll(t, r, q, 4)
			if want == nil {
				want = got
				t.Logf("基准顺序: %v", want)
			} else {
				for i := range got {
					if got[i] != want[i] {
						t.Fatalf("后端 %s 顺序不一致: got=%v want=%v", name, got, want)
					}
				}
			}
			// 非法游标在所有后端上同样被拒绝。
			if _, err := r.Export(context.Background(), q, 4, "garbage"); !IsKind(err, KindInvalidCursor) {
				t.Fatalf("后端 %s: 非法游标应判 KindInvalidCursor, 得到 %v", name, err)
			}
			// 批量预检归类一致（抽查）。
			mustCreate(t, r, "exist", "p/9")
			failures := r.Preflight(context.Background(), []BatchItem{
				{ID: "exist", Intent: IntentCreate},
				{ID: "ghost", Intent: IntentUpdate, ExpectedVersion: 1},
			})
			if len(failures) != 2 || failures[0].Kind != KindConflict || failures[1].Kind != KindNotFound {
				t.Fatalf("后端 %s: 预检归类不一致: %+v", name, failures)
			}
		})
	}
	t.Logf("判定依据: 三个后端导出顺序、游标拒绝、预检归类完全一致")
}

// TestConcurrentExportAndBatchWrite 并发导出与批量写入：
// 供 -race 检测数据竞争；导出不得返回错误分类之外的失败。
func TestConcurrentExportAndBatchWrite(t *testing.T) {
	r := New(NewLimitedStore(3))
	ctx := context.Background()
	for i := 0; i < 30; i++ {
		mustCreate(t, r, fmt.Sprintf("id-%02d", i), fmt.Sprintf("sk-%02d", i%6))
	}
	var wg sync.WaitGroup
	// 导出方：持续翻页。
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cursor := ""
			for {
				p, err := r.Export(ctx, Query{}, 5, cursor)
				if err != nil {
					t.Errorf("export: %v", err)
					return
				}
				if p.NextCursor == "" {
					return
				}
				cursor = p.NextCursor
			}
		}()
	}
	// 写入方：批量新增。
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			items := make([]BatchItem, 0, 5)
			for i := 0; i < 5; i++ {
				items = append(items, BatchItem{
					ID:      fmt.Sprintf("w%d-%02d", w, i),
					SortKey: fmt.Sprintf("sk-%02d", i),
					Intent:  IntentCreate,
				})
			}
			if err := r.BatchWrite(ctx, items); err != nil {
				t.Errorf("batch write: %v", err)
			}
		}(w)
	}
	wg.Wait()
	t.Logf("判定依据: 并发导出与批量写入全部完成, 无错误、无数据竞争")
}
