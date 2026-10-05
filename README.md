# go-transactional-repository-go

可替换存储后端的本地仓储数据访问层，提供事务、乐观锁、稳定的游标分页、
按条件批量写入（含整批预检）与可续传的分页导出，
保证并发写入与跨页读取在异常和边界情况下仍得到一致且可解释的结果。

## 存储契约（Store）

存储后端通过 `Store` 接口替换，任何实现必须满足：

| 方法 | 契约 |
| --- | --- |
| `Get(ctx, key)` | 返回记录及其当前版本；不存在时返回 `KindNotFound`。 |
| `ScanSorted(ctx, after, limit)` | 返回 `(SortKey, Key)` 严格大于 `after` 的最多 `limit` 条记录，按 `(SortKey, Key)` 升序。`limit` 必须为正；实现可以返回少于 `limit` 条（如单次读取受限的后端），仅当确实没有更多记录时才返回空切片。 |
| `Commit(ctx, mutations)` | **原子性**：全部变更要么同时生效要么全部不生效；任一 `ExpectedVersion` 与当前版本不符即整体拒绝并返回 `KindConflict`，不得残留部分写入。空主键属于契约违例（`KindContractViolation`）。 |

`SortKey` 是存储层记录的一等字段，排序键重复时以主键作为决胜键，
因此扫描顺序完全确定。

内置后端：

- `NewMemoryStore()`：内存 + 读写锁的参考实现。
- `NewLimitedStore(maxPerRead)`：单次 `ScanSorted` 最多返回 `maxPerRead` 条，
  模拟带最大页大小的远程存储。仓储层通过键集续扫翻页，
  不会一次性把全部记录读入内存（由 `TestExportStreamingWithLimitedBackend` 验证）。

更换后端只需实现上述接口，对外可观察的读写、批量与分页语义不变
（`TestBackendSwapSemantics`、`TestBatchBackendConsistency`、
`TestExportBackendConsistency` 对三个后端做一致性验证）。

## 错误分类

所有对外错误统一为 `*repository.Error`，用 `IsKind(err, kind)` 判定，底层错误永不直接透出：

| Kind | 含义 | 典型场景 |
| --- | --- | --- |
| `KindNotFound` | 记录不存在 | `Get` 缺失的 ID；批量更新目标不存在 |
| `KindConflict` | 乐观锁/并发提交冲突 | 基于过期版本写入；新建目标已存在 |
| `KindContractViolation` | 违反存储契约 | 空 ID、`limit <= 0`、批次内重复 ID、更新缺版本 |
| `KindInvalidCursor` | 游标非法或已失效 | 无法解码、被篡改、旧格式或来自其他查询的游标 |
| `KindInternal` | 后端内部错误 | 数据损坏等，细节不外泄 |

批量预检失败时返回 `*BatchError`（可用 `errors.As` 提取），
其 `Failures` 字段逐条列出 `ItemFailure{Index, ID, Kind, Err}`，
每个 `Err` 本身是 `*Error`，可继续用 `IsKind` 判定。

## 事务与并发语义

- `InTx(ctx, fn)`：`fn` 返回错误则回滚——写入在提交前仅暂存于事务内，
  回滚天然无任何部分写入；`fn` 返回 `nil` 时一次性原子提交全部变更。
- 乐观锁：`Put/Delete` 携带 `expectedVersion`（0 表示创建，要求记录不存在）。
  提交时由后端在校验阶段统一比对版本，任一不符即整体拒绝。
  因此同一记录的并发修改只有一次提交成功，其余得到 `KindConflict`。
- 事务内 `Get` 先读暂存区（read-your-writes），事务外不可见未提交写入。

## 批量写入与预检

`BatchWrite(ctx, items)` 支持一个批次内混合三种意图（`WriteIntent`）：

| 意图 | 语义 | 失败归类 |
| --- | --- | --- |
| `IntentCreate` | 只允许新建 | 目标已存在 → `KindConflict` |
| `IntentUpdate` | 只允许更新，须带 `ExpectedVersion >= 1` | 目标不存在 → `KindNotFound`；版本不符 → `KindConflict`；缺版本 → `KindContractViolation` |
| `IntentUpsert` | 不存在就新建、存在就更新 | 存在且版本不符 → `KindConflict` |

- **整批预检**：提交前先对整批做 `Preflight`（也可单独调用做干跑），
  一次性列出所有问题条目，不会在第一个失败处停下；
  每条失败都带独立 Kind（不存在 / 版本过期 / 入参非法互不混淆）。
- **原因一致**：预检给出的归类与正式提交路径的归类逐条一致——
  `BatchWrite` 内部即先预检再提交；预检通过后目标被并发修改时，
  提交路径同样归一为 `KindConflict`。
- **原子性**：任一条目预检失败即整批拒绝（返回 `*BatchError`，无任何写入）；
  预检全部通过后一次性原子提交，整体成功或整体失败。
- 批次内同一 ID 出现多次属于入参不合法（`KindContractViolation`）。

## 分页与续传导出

- `List(ctx, limit, cursor)`：按键集分页的基础读取（v1 游标）。
- `Export(ctx, query, limit, cursor)`：可续传的分页导出。
  `Query{SortKeyPrefix}` 描述筛选条件，排序固定为 `(SortKey, ID)` 升序。

续传语义：

- 游标是不透明字符串，可持久化或跨进程传递：进程重启或换一个调用点，
  凭同一 `Query` 与上次返回的 `NextCursor` 即可接着往下读，
  已返回的记录不重复、应返回的记录不遗漏（`NextCursor` 为空表示读完）。
- 底层按键集逐块扫描，数据量远大于一页时不依赖一次性全量读取；
  单次读取受限的后端上会自动续扫拼满一页。
- 翻页期间的插入/删除不会导致已返回记录重复或遗漏。

游标兼容范围（以下一律判 `KindInvalidCursor`，绝不当作某个位置继续返回数据）：

- 来自**其他筛选条件**的游标（游标绑定查询指纹，`Query` 任一字段变化即失效）；
- **旧格式**游标（无版本号的 v1 `List` 游标）；
- 被**截断**、无法解码或 JSON 结构**不完整**（缺版本/指纹/位置字段）的游标；
- 被**篡改**的游标（内容校验和不匹配；校验和用于发现损坏，非密码学签名）。

## 本地验证

```sh
go build ./...
go test ./...            # 常规测试
go test -race -count=1 -v ./...   # 并发竞争检测 + 关键输入/判定依据日志
```

测试覆盖：并发提交冲突归类、回滚与冲突提交的原子性、
批量预检的多原因归类与整批原子性、预检与提交归类一致性、
跨调用点续传导出、受限后端上的流式翻页、重复排序键的稳定性、
篡改/截断/旧格式/异查询游标的拒绝、错误分类，
以及三个后端（memory / limited / slice）的语义一致性。
