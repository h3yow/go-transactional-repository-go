package repo

import (
	"context"
	"sort"
	"testing"
)

func newTestRepo() (*Repository, Backend) {
	b := NewMemoryBackend()
	return New(b), b
}

func mustCreate(t *testing.T, r *Repository, id, sortKey, value string) {
	t.Helper()
	if err := r.Create(context.Background(), id, sortKey, []byte(value)); err != nil {
		t.Fatalf("mustCreate(%s): unexpected error: %v", id, err)
	}
}

// sortedIDs 返回按 ID 排序后的记录 ID 列表，便于做稳定断言。
func sortedIDs(recs []Record) []string {
	out := make([]string, 0, len(recs))
	for _, rec := range recs {
		out = append(out, rec.ID)
	}
	sort.Strings(out)
	return out
}

// pageIDs 提取一页中的记录 ID（保持页内顺序）。
func pageIDs(p Page) []string {
	out := make([]string, 0, len(p.Records))
	for _, rec := range p.Records {
		out = append(out, rec.ID)
	}
	return out
}
