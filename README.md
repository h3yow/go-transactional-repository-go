# 可替换后端的事务型本地仓储（Go）

`repo` 包提供一套与存储介质无关的本地数据访问层：支持事务、乐观锁、
基于游标的稳定分页，并把不同后端的错误归一化为统一分类。更换存储后端
不会改变任何对外可观察的读写与分页语义。

- 包路径：`github.com/h3y3ow/go-transactional-repository-go/repo`
- 无第三方依赖，仅使用标准库。

## 快速上手

```go
import "github.com/h3y3ow/go-transactional-repository-go/repo"

r := repo.New(repo.NewMemoryBackend()) // 也可替换为 repo.NewShardedBackend(8)

ctx := context.Background()
_ = r.Create(ctx, "user-1", "name:alice", []byte(`{"role":"admin"}`))

// 读-改-写事务
tx := r.Begin()
rec, err := tx.Get(ctx, "user-1")
if err != nil { /* ... */ }
_ = tx.Put(ctx, rec.ID, "name:alice", []byte(`{"role":"owner"}`))
if err := tx.Commit(ctx); err != nil {
    if repo.IsKind(err, repo.KindConflict) {
        // 记录已被别人改过：重试或放弃
    }
}
```

## 存储后端契约（`Backend`）

任何后端实现只需满足三个方法（见 `repo/storage.go`）：

| 方法 | 契约 |
| --- | --- |
| `Get(ctx, id)` | 返回记录；不存在时必须归一化为 `KindNotFound`。 |
| `Apply(ctx, []Mutation)` | **原子**应用整组写入：先校验全部前置条件，再一次性落盘；任一条件不满足时全部变更都不得生效，不允许部分写入。 |
| `List(ctx, Scan)` | 返回严格位于给定位置之后的记录，按 `(SortKey, ID)` 升序排列；`SortKey` 重复时以 `ID` 决胜，排序完全确定。 |

写入类型（`MutationOp`）与乐观锁条件：

| 操作 | 语义 | 失败分类 |
| --- | --- | --- |
| `OpCreate` | 仅当记录不存在时创建，初始版本为 1 | 已存在 → `KindConflict` |
| `OpPut` | 更新已存在记录，`ExpectVersion` 必须等于当前版本 | 不存在 → `KindNotFound`；版本不匹配 → `KindConflict` |
| `OpDelete` | 删除记录，版本条件同上 | 同上 |

版本号由后端单调维护（创建为 1，每次成功更新 +1），调用方不能自行指定。

仓库内提供两个参考实现，用于实证“可替换且语义不变”：

- `MemoryBackend`：单 map + 单一互斥锁，整组写入在同一临界区内“先校验后落盘”。
- `ShardedBackend`：按 ID 哈希分片、每片独立锁；`Apply` 时按分片序加锁后
  跨分片原子校验与落盘。它还自带一套与仓储错误**完全不同**的原生错误类型
  `*shardError`，用于验证原生错误类型不会透出仓储边界。

## 事务与并发语义

- `Repository.Begin()` 返回一个事务；`Create/Put/Delete` 只在本地缓冲，
  `Commit` 时整组交给后端 `Apply`。因此一个事务内的多次写入**要么全部生效，
  要么全部不生效**。
- 乐观锁：`Tx.Get` 记录所读版本，`Tx.Put/Delete` 以该版本为前置条件。
  多个事务并发修改同一条记录时，**恰好一个提交成功**；其余提交得到
  `KindConflict`，且不会留下任何部分写入。
- `Rollback` 放弃全部缓冲写入，幂等可重复调用。
- 事务一旦 `Commit`（无论成败）或 `Rollback` 即结束，继续使用属于
  `KindContractViolation`。
- 单个 `Tx` 不是并发安全的，只应在一个 goroutine 内使用；需要并发写入时
  请开启多个独立事务。
- 同一事务内不允许对同一条记录缓冲两次写入（契约违例）。

## 游标分页语义

- 采用**键集（keyset/seek）分页**，不使用偏移量。游标编码的是上一页最后一条
  记录在全局全序中的位置 `(SortKey, ID)`，内容为不透明字符串（带版本前缀
  `v1.` + base64url(JSON)）。
- 排序为 `(SortKey 升序, ID 升序)` 的全序，**即使 SortKey 重复也完全确定**，
  不会出现翻页顺序漂移。
- 翻页期间数据发生变化时的保证：
  - **插入**到游标位置之前的记录不会在后续页回补（不会重复）；插入到未读
    区域的新记录可能在后续页出现（可解释、且不会造成重复）。
  - **删除**已返回或未读记录都不会导致其它记录跳项或重复。
  - 已返回过的记录在后续页中绝不再次出现。
- 任何前缀不符、base64 损坏、JSON 非法、缺少 `id` 字段的游标都被明确拒绝为
  `KindInvalidCursor`，不会返回猜测出来的结果。
- `ListQuery.Limit` <= 0 时取默认值 20，上限 100；`Page.HasMore` 为 false 时
  `NextCursor` 必为空。

## 错误分类（归一化）

所有可观察错误都是 `*repo.Error`，通过 `repo.IsKind(err, kind)` 判定：

| Kind | 触发场景 | 调用方建议 |
| --- | --- | --- |
| `KindNotFound` | 读取/更新/删除不存在的记录 | 返回 404 或跳过 |
| `KindConflict` | 创建已存在记录；乐观锁版本不匹配 | 重新读取后重试或放弃 |
| `KindContractViolation` | 空 ID、空事务提交、事务结束后复用、同事务重复写同一记录等 | 这是调用方 bug，应修复代码 |
| `KindInvalidCursor` | 游标非法、损坏、格式不符 | 终止翻页，要求从头开始 |
| `KindOther` | 未分类的后端故障 | 重试或上抛 |

后端自身的错误类型与文案不会透出仓储边界（测试
`TestShardedErrorNormalization` 用 `errors.As` 显式断言原生类型不可见）。

## 本地验证

```bash
# 常规测试
go test ./...

# 并发竞争检测 + 多次重复（CI 推荐）
go test -race -count=5 ./...

# 查看关键输入与判定依据（测试日志会打印 worker 输入、版本、错误分类）
go test -race -v ./...

go vet ./...
```

测试覆盖：

1. 并发提交冲突：32 个基于同一旧版本的事务在“读取屏障后统一提交”，
   断言恰好 1 个成功、31 个 `KindConflict`、最终版本只 +1（两个后端各跑一遍）。
2. 事务原子性：混合“更新+删除+创建”全部生效；显式回滚无残留；
   提交期冲突时整组拒绝（连同组内新建记录也不存在）。
3. 翻页期间插入/删除：不重复、不遗漏、不跳项；重复 SortKey 排序稳定可复现。
4. 错误归一化：未找到/冲突/契约违例/非法游标分类正确，底层错误类型不透出。
5. 后端可替换：两种后端跑同一组场景，全序列序、版本推进、冲突归类完全一致。
