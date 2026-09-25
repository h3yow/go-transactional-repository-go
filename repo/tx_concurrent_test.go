package repo

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// TestConcurrentOptimisticConflicts 并发对同一条记录发起“读-改-写”。
// 关键设计：所有 goroutine 先在屏障 readBarrier 前全部完成读取（此时它们
// 读到的都是同一旧版本），随后才并发提交。因此提交阶段必然发生乐观锁
// 竞争：恰好一个事务成功，其余全部得到 KindConflict，最终版本只 +1。
// 本用例设计为可在 `go test -race` 下稳定复现。
func TestConcurrentOptimisticConflicts(t *testing.T) {
	ctx := context.Background()

	for backendName, factory := range backendFactories() {
		t.Run(backendName, func(t *testing.T) {
			r := New(factory())
			if err := r.Create(ctx, "counter", "k", []byte("0")); err != nil {
				t.Fatalf("seed: %v", err)
			}

			const n = 32
			startRead := make(chan struct{})
			startCommit := make(chan struct{})

			type workerResult struct {
				idx         int
				readVersion uint64
				commitErr   error
				tx          *Tx
			}
			results := make([]workerResult, n)
			var readWG, commitWG sync.WaitGroup
			readWG.Add(n)
			commitWG.Add(n)

			for i := 0; i < n; i++ {
				go func(idx int) {
					defer commitWG.Done()
					<-startRead
					tx := r.Begin()
					rec, err := tx.Get(ctx, "counter")
					if err != nil {
						results[idx] = workerResult{idx: idx, commitErr: fmt.Errorf("get: %w", err)}
						tx.Rollback()
						readWG.Done()
						return
					}
					// 缓冲更新，此时 Put 直接使用刚读到的版本，不访问后端。
					if err := tx.Put(ctx, rec.ID, "k", []byte(fmt.Sprintf("w%d", idx))); err != nil {
						results[idx] = workerResult{idx: idx, commitErr: fmt.Errorf("put: %w", err)}
						tx.Rollback()
						readWG.Done()
						return
					}
					results[idx] = workerResult{idx: idx, readVersion: rec.Version, tx: tx}
					// 读取与缓冲阶段完成。
					readWG.Done()
					// 等待所有 worker 都基于同一旧版本完成读取与缓冲，再统一提交。
					<-startCommit
					results[idx].commitErr = results[idx].tx.Commit(ctx)
				}(i)
			}

			close(startRead)
			readWG.Wait()
			close(startCommit)
			commitWG.Wait()

			ok, conflict, other := 0, 0, 0
			for _, res := range results {
				switch {
				case res.commitErr == nil:
					ok++
				case IsKind(res.commitErr, KindConflict):
					conflict++
					t.Logf("输入: worker=%d 基于版本 v%d 的更新；判定: 提交被拒绝 kind=conflict (%v)",
						res.idx, res.readVersion, res.commitErr)
				default:
					other++
					t.Errorf("worker=%d readV=%d 返回了非预期错误类型: %v",
						res.idx, res.readVersion, res.commitErr)
				}
			}
			if ok != 1 {
				t.Fatalf("want exactly 1 winner, got %d (conflict=%d other=%d)", ok, conflict, other)
			}
			if conflict != n-1 {
				t.Fatalf("want %d conflicts, got %d", n-1, conflict)
			}
			rec, err := r.Get(ctx, "counter")
			if err != nil {
				t.Fatalf("final get: %v", err)
			}
			if rec.Version != 2 {
				t.Fatalf("version must advance exactly once, got %d (value=%q)", rec.Version, string(rec.Value))
			}
			t.Logf("判定依据: %d 个基于同一旧版本 v1 的并发事务 => 成功=%d 冲突=%d；最终版本=%d 值=%q（仅一次提交生效）",
				n, ok, conflict, rec.Version, string(rec.Value))
		})
	}
}

// TestConcurrentIndependentRecords 并发写互不相交的记录时不应互相冲突。
func TestConcurrentIndependentRecords(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestRepo()

	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			id := fmt.Sprintf("rec-%02d", idx)
			errs <- r.Create(ctx, id, fmt.Sprintf("key-%02d", idx), []byte("x"))
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("independent create failed: %v", err)
		}
	}
}
