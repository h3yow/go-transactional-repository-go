package repo

import (
	"context"
	"encoding/base64"
	"reflect"
	"testing"
)

// seedPageData 写入给定 (id, sortKey) 集合；其中多个记录共享同一 SortKey，
// 用于验证排序键重复时排序仍完全确定（以 ID 决胜）。
func seedPageData(t *testing.T, r *Repository, rows [][2]string) {
	t.Helper()
	for _, row := range rows {
		mustCreate(t, r, row[0], row[1], row[0])
	}
}

// readAllPages 从首页开始按游标翻完，返回所有页的 ID 顺序与各页游标。
func readAllPages(t *testing.T, r *Repository, limit int) ([]string, []string) {
	t.Helper()
	ctx := context.Background()
	var got []string
	var cursors []string
	cursor := ""
	pageNum := 0
	for {
		page, err := r.List(ctx, ListQuery{Limit: limit, Cursor: cursor})
		if err != nil {
			t.Fatalf("page %d: %v", pageNum, err)
		}
		for _, rec := range page.Records {
			got = append(got, rec.ID)
		}
		cursors = append(cursors, cursor)
		if !page.HasMore {
			if page.NextCursor != "" {
				t.Fatalf("last page must not carry a next cursor")
			}
			return got, cursors
		}
		if page.NextCursor == "" {
			t.Fatalf("page %d claims HasMore but has empty cursor", pageNum)
		}
		cursor = page.NextCursor
		pageNum++
	}
}

// TestPaginationStableOrderAndDuplicateKeys 验证：
//   - 多页拼接结果与 (SortKey, ID) 的全序一致；
//   - SortKey 重复不会造成顺序漂移；
//   - 相同输入多次翻页结果完全一致。
func TestPaginationStableOrderAndDuplicateKeys(t *testing.T) {
	r, _ := newTestRepo()
	rows := [][2]string{
		{"a3", "k1"}, {"a1", "k1"}, {"a2", "k1"}, // 同键三条，必须按 ID 排
		{"b2", "k2"}, {"b1", "k2"},
		{"c1", "k3"},
	}
	seedPageData(t, r, rows)
	want := []string{"a1", "a2", "a3", "b1", "b2", "c1"}

	first, _ := readAllPages(t, r, 2)
	second, _ := readAllPages(t, r, 2)
	if !reflect.DeepEqual(first, want) {
		t.Fatalf("page order mismatch:\n got=%v\nwant=%v", first, want)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("pagination is not deterministic:\nfirst =%v\nsecond=%v", first, second)
	}
	t.Logf("判定依据: limit=2 跨 3 页拼接=%v，含重复 SortKey=k1/k2，顺序稳定且可复现", first)

	// 不同页大小必须得到相同的全序。
	got, _ := readAllPages(t, r, 5)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("limit=5 order mismatch: got=%v want=%v", got, want)
	}
}

// TestPaginationInsertDuringPaging 翻页过程中在不同排序位置插入新记录，
// 继续翻页不得重复也不得遗漏“此前已经返回过”的记录。
// 键集分页的语义保证：已翻过的位置不会再被扫到。
func TestPaginationInsertDuringPaging(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestRepo()
	// 初始：k1..k6 各一条，页大小 2 => 共 3 页。
	seedPageData(t, r, [][2]string{
		{"r1", "k1"}, {"r2", "k2"}, {"r3", "k3"},
		{"r4", "k4"}, {"r5", "k5"}, {"r6", "k6"},
	})

	p1, err := r.List(ctx, ListQuery{Limit: 2})
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if got := pageIDs(p1); !reflect.DeepEqual(got, []string{"r1", "r2"}) {
		t.Fatalf("page1=%v", got)
	}

	// 翻页期间插入：
	//   - 在游标之前（旧位置）：不会被后续扫描看到；
	//   - 在游标之后：可能在后续页出现（这是可解释的、且不会造成重复）。
	mustCreate(t, r, "old0", "k0", "old0") // 严格位于已返回位置之前
	mustCreate(t, r, "new3", "k3", "new3") // 与未读区域同键，按 ID 与 r3 决胜

	p2, err := r.List(ctx, ListQuery{Limit: 2, Cursor: p1.NextCursor})
	if err != nil {
		t.Fatalf("page2 after inserts: %v", err)
	}
	seen := map[string]bool{}
	for _, id := range pageIDs(p1) {
		seen[id] = true
	}
	for _, rec := range p2.Records {
		if seen[rec.ID] {
			t.Fatalf("record %s returned twice after concurrent insert", rec.ID)
		}
		seen[rec.ID] = true
	}
	if seen["old0"] {
		t.Fatalf("record inserted before the cursor must not appear on later pages")
	}
	t.Logf("判定依据: 游标位于 (k2,r2) 后插入 k0(旧侧) 与 k3(新侧)；page2=%v，无重复且旧侧插入不回补",
		pageIDs(p2))

	// 翻完剩余页，检查没有遗漏“初始集合中尚未返回”的记录。
	cursor := p2.NextCursor
	for cursor != "" {
		page, err := r.List(ctx, ListQuery{Limit: 2, Cursor: cursor})
		if err != nil {
			t.Fatalf("later page: %v", err)
		}
		for _, rec := range page.Records {
			if seen[rec.ID] {
				t.Fatalf("record %s returned twice", rec.ID)
			}
			seen[rec.ID] = true
		}
		cursor = page.NextCursor
	}
	for _, id := range []string{"r3", "r4", "r5", "r6"} {
		if !seen[id] {
			t.Fatalf("pre-existing record %s was skipped after insert during paging", id)
		}
	}
	t.Logf("判定依据: 初始六页记录 r1..r6 全部恰好返回一次，翻页期间插入未造成重复或遗漏")
}

// TestPaginationDeleteDuringPaging 翻页期间删除记录：
//   - 删除已返回过的记录不影响后续页（键集扫描不依赖其存在）；
//   - 删除未读记录时它自然不再出现，但其余记录不跳项。
func TestPaginationDeleteDuringPaging(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestRepo()
	seedPageData(t, r, [][2]string{
		{"r1", "k1"}, {"r2", "k2"}, {"r3", "k3"},
		{"r4", "k4"}, {"r5", "k5"}, {"r6", "k6"},
	})

	p1, err := r.List(ctx, ListQuery{Limit: 2})
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	// 删除：已返回的 r1（游标指向 r2），以及未读的 r4。
	if err := r.Delete(ctx, "r1"); err != nil {
		t.Fatalf("delete r1: %v", err)
	}
	if err := r.Delete(ctx, "r4"); err != nil {
		t.Fatalf("delete r4: %v", err)
	}

	cursor := p1.NextCursor
	seen := map[string]bool{"r1": true, "r2": true} // 首页已返回
	for {
		page, err := r.List(ctx, ListQuery{Limit: 2, Cursor: cursor})
		if err != nil {
			t.Fatalf("page after delete: %v", err)
		}
		for _, rec := range page.Records {
			if seen[rec.ID] {
				t.Fatalf("record %s returned twice after delete", rec.ID)
			}
			seen[rec.ID] = true
		}
		if !page.HasMore {
			break
		}
		cursor = page.NextCursor
	}
	for _, id := range []string{"r3", "r5", "r6"} {
		if !seen[id] {
			t.Fatalf("record %s skipped after deletes during paging", id)
		}
	}
	if seen["r4"] {
		t.Fatalf("deleted unread record r4 must not appear")
	}
	t.Logf("判定依据: 删除已返回 r1 与未读 r4 后继续翻页，返回集=%v 无重复无跳项",
		keysOf(seen))
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestInvalidCursorsRejected 各类非法/失效游标必须被明确拒绝为 KindInvalidCursor。
func TestInvalidCursorsRejected(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestRepo()
	mustCreate(t, r, "r1", "k1", "1")

	bad := []string{
		"garbage",
		"v2.something",                // 未知版本前缀
		"v1.!!!not-base64!!!",         // base64 损坏
		"v1." + b64("{\"k\":\"k1\"}"), // 缺少 id 字段
		"v1." + b64("not-json"),       // JSON 非法
	}
	for _, token := range bad {
		_, err := r.List(ctx, ListQuery{Cursor: token})
		if !IsKind(err, KindInvalidCursor) {
			t.Fatalf("cursor %q: want KindInvalidCursor, got %v", token, err)
		}
		t.Logf("判定依据: 非法游标 %q => kind=invalid_cursor (%v)", token, err)
	}
}

func b64(s string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(s))
}
