# go-transactional-repository-go

可替换存储后端的本地仓储数据访问层，提供事务、乐观锁与稳定的游标分页，
保证并发写入与跨页读取在异常和边界情况下仍得到一致且可解释的结果。

## 存储契约（Store）

存储后端通过 `Store` 接口替换，任何实现必须满足：

| 方法 | 契约 |
| --- | --- |
| `Get(ctx, key)` | 返回记录及其当前版本；不存在时返回 `KindNotFound`。 |
| `Scan(ctx, after, limit)` | 返回主键严格大于 `after` 的最多 `limit` 条记录，按主键升序。 |
| `Commit(ctx, mutations)` | **原子性**：全部变更要么同时生效要么全部不生效；任一 `ExpectedVersion` 与当前版本不符即整体拒绝并返回 `KindConflict`，不得残留部分写入。空主键属于契约违例（`KindContractViolation`）。 |

仓库内置参考实现 `NewMemoryStore()`（内存 + 读写锁）。更换后端只需实现上述接口，
对外可观察的读写与分页语义不变（由 `TestBackendSwapSemantics` 对两个后端做一致性验证）。

## 错误分类

所有对外错误统一为 `*repository.Error`，用 `IsKind(err, kind)` 判定，底层错误永不直接透出：

| Kind | 含义 | 典型场景 |
| --- | --- | --- |
| `KindNotFound` | 记录不存在 | `Get` 缺失的 ID |
| `KindConflict` | 乐观锁/并发提交冲突 | 基于过期版本写入 |
| `KindContractViolation` | 违反存储契约 | 空 ID、`limit <= 0` |
| `KindInvalidCursor` | 游标非法或已失效 | 无法解码或结构损坏的分页游标 |
| `KindInternal` | 后端内部错误 | 数据损坏等，细节不外泄 |

## 事务与并发语义

- `InTx(ctx, fn)`：`fn` 返回错误则回滚——写入在提交前仅暂存于事务内，
  回滚天然无任何部分写入；`fn` 返回 `nil` 时一次性原子提交全部变更。
- 乐观锁：`Put/Delete` 携带 `expectedVersion`（0 表示创建，要求记录不存在）。
  提交时由后端在校验阶段统一比对版本，任一不符即整体拒绝。
  因此同一记录的并发修改只有一次提交成功，其余得到 `KindConflict`。
- 事务内 `Get` 先读暂存区（read-your-writes），事务外不可见未提交写入。

## 分页语义

- 排序键为 `(SortKey, ID)`：排序键重复时以主键作为决胜键，顺序完全确定、无漂移。
- 采用**键集分页**：游标编码上一条已返回记录的 `(SortKey, ID)`。
  翻页期间的插入/删除不会导致已返回记录重复或遗漏——
  新插入且位于游标之前的记录不会出现，位于游标之后的记录会正常出现；
  已删除的记录不再出现。
- 游标是不透明字符串（base64 编码），非法或损坏的游标被明确拒绝
  （`KindInvalidCursor`），绝不返回任意结果。

## 本地验证

```sh
go build ./...
go test ./...            # 常规测试
go test -race -count=1 -v ./...   # 并发竞争检测 + 关键输入/判定依据日志
```

测试覆盖：并发提交冲突归类、回滚与冲突提交的原子性、
翻页期间插入/删除、重复排序键的稳定性、非法游标拒绝、
错误分类，以及双后端语义一致性。
