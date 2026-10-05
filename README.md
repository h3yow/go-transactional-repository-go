# go-transactional-repository-go

可替换存储后端的本地仓储数据访问层，提供事务、乐观锁、稳定的游标分页、
按条件批量写入（含整批预检）与可续传的分页导出，
保证并发写入与跨页读取在异常和边界情况下仍得到一致且可解释的结果。

## 存储契约（Store）

存储后端通过 `Store` 接口替换，任何实现必须满足：

| 方法 | 契约 |
| --- | --- |
| `Get(ctx, key)` | 返回记录及其当前版本；不存在时返回 `KindNotFound`。 |
| `Scan(ctx, after, limit)` | 返回主键严格大于 `after` 的最多 `limit` 条记录，按主键升序。实现可以返回得更少（单次读取有上限），调用方以空页判断数据耗尽。 |
| `Commit(ctx, mutations)` | **原子性**：全部变更要么同时生效要么全部不生效；任一 `ExpectedVersion` 与当前版本不符即整体拒绝并返回 `KindConflict`，不得残留部分写入。空主键属于契约违例（`KindContractViolation`）。 |

内置后端：

- `NewMemoryStore()`：内存 + 读写锁的参考实现。
- `NewChunkedStore(maxScan)`：单次 `Scan` 最多返回 `maxScan` 条记录的限流后端，
  用于验证分页/导出实现不依赖一次性取出全部数据。
  事务与版本语义与内存后端完全一致。

更换后端只需实现上述接口，对外可观察的读写、批量与分页语义不变
（`TestBackendSwapSemantics` 与 `TestBackendConsistencyBatchAndExport`
对三个后端做一致性验证）。

## 错误分类

所有对外错误统一为 `*repository.Error`，用 `IsKind(err, kind)` 判定，底层错误永不直接透出：

| Kind | 含义 | 典型场景 |
| --- | --- | --- |
| `KindNotFound` | 记录不存在 | `Get` 缺失的 ID；批量更新一个不存在的目标 |
| `KindConflict` | 乐观锁/并发提交冲突 | 基于过期版本写入；批量新建一个已存在的目标 |
| `KindContractViolation` | 违反存储契约 | 空 ID、`limit <= 0`、批内重复 ID、意图与版本组合非法 |
| `KindInvalidCursor` | 游标非法或已失效 | 篡改、截断、跨查询或旧格式的分页/导出游标 |
| `KindInternal` | 后端内部错误 | 数据损坏、后端故障等，细节不外泄 |

批量操作的失败以 `*BatchError` 返回（用 `BatchErrors(err)` 提取条目级列表），
每个条目独立携带上述 Kind，不会把不同原因合并成笼统错误。

## 事务与并发语义

- `InTx(ctx, fn)`：`fn` 返回错误则回滚——写入在提交前仅暂存于事务内，
  回滚天然无任何部分写入；`fn` 返回 `nil` 时一次性原子提交全部变更。
- 乐观锁：`Put/Delete` 携带 `expectedVersion`（0 表示创建，要求记录不存在）。
  提交时由后端在校验阶段统一比对版本，任一不符即整体拒绝。
  因此同一记录的并发修改只有一次提交成功，其余得到 `KindConflict`。
- 事务内 `Get` 先读暂存区（read-your-writes），事务外不可见未提交写入。

## 批量写入与预检

`ApplyBatch(ctx, entries)` 把一个批次作为整体原子提交：全部生效或全部失败。
批次内每个条目（`BatchEntry`）携带三种意图之一：

| 意图 | 语义 | 失败归类 |
| --- | --- | --- |
| `IntentCreate` | 只允许新建，目标必须不存在（`Version` 必须为 0） | 已存在 → `KindConflict` |
| `IntentUpdate` | 只允许更新，`Version` 必须等于目标当前版本（>= 1） | 不存在 → `KindNotFound`；版本不符 → `KindConflict` |
| `IntentUpsert` | 不存在则新建，存在则更新；`Version=0` 为盲写不校验版本，`Version>0` 时目标已存在则必须匹配 | 版本不符 → `KindConflict` |

入参本身不合法（空 ID、批内重复 ID、未知意图、新建携带版本、更新不带版本）
归类为 `KindContractViolation`。

- `PreflightBatch(ctx, entries)`：正式提交前的整批预检，**一次性返回全部
  问题条目**及其归类原因（`[]EntryError`，含批次下标、ID、Kind、具体原因），
  不会只报第一条。预检不修改任何数据。
- `ApplyBatch` 失败时返回的 `*BatchError` 与预检使用同一套归类逻辑，
  原因一致；预检到提交之间状态被并发修改时，提交期冲突会重新预检，
  仍以条目级归类报告。
- 任一条目不通过即整批拒绝，合法条目也不会部分生效。

## 分页语义（List）

- 排序键为 `(SortKey, ID)`：排序键重复时以主键作为决胜键，顺序完全确定、无漂移。
- 采用**键集分页**：游标编码上一条已返回记录的 `(SortKey, ID)`。
  翻页期间的插入/删除不会导致已返回记录重复或遗漏。
- 游标是不透明字符串（base64 编码），非法、损坏或来自导出的游标被明确拒绝
  （`KindInvalidCursor`），绝不返回任意结果。

## 可续传分页导出（Export）

`Export(ctx, spec, cursor)` 面向数据同步/批量导出场景：

- `ExportSpec{SortKeyPrefix, Limit}`：按 SortKey 前缀筛选，按 `(SortKey, ID)`
  稳定排序，排序键重复时以主键决胜，顺序完全确定。
- 返回的 `Page.NextCursor` 是不透明字符串，可持久化后**跨进程、跨调用点**续传；
  键集语义保证续传不重复、不遗漏（翻页期间插入到游标之后的记录会正常出现，
  已删除的记录不再出现）。
- 内存占用与数据集规模无关：实现按块循环扫描后端（兼容单次读取有上限的后端），
  用容量为 `Limit+1` 的有界堆筛选候选，不会一次性取出全部记录再切页。

### 游标兼容范围

导出游标（v2 格式）绑定签发它的查询，以下游标一律被 `KindInvalidCursor` 拒绝：

- **来自其他查询**：筛选条件（SortKey 前缀）或排序条件与签发时不同
  （查询指纹不匹配）。注意页大小 `Limit` 不属于指纹，续传时可以改变。
- **旧格式**：无版本号的游标（如 `List` 的 v1 游标）。
- **被篡改/截断**：完整性校验和不匹配或 base64/JSON 无法解码。
- **结构不完整**：缺失版本、指纹、位置或校验和字段，或携带多余字段。

反向同样成立：`List` 不接受导出游标。游标校验和用于完整性保护，不是安全签名。

## 本地验证

```sh
go build ./...
go test ./...                      # 常规测试
go test -race -count=1 -v ./...    # 并发竞争检测 + 关键输入/判定依据日志
```

测试覆盖：并发提交冲突归类、回滚与冲突提交的原子性、
批量预检的混合失败归类（不存在/版本过期/入参非法）与提交原子性、
预检与提交失败原因一致性、三种写入意图语义、
跨实例续传导出、限流后端上的大数据量翻页、重复排序键的稳定性、
九类非法游标拒绝、后端错误细节不外泄、并发导出与写入（-race），
以及三个后端（memory / slice / chunked）的对外语义一致性。
