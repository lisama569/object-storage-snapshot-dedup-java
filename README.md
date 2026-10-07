# object-storage-snapshot-dedup-java

基于本地文件系统模拟对象后端的存储服务，提供 **写入去重、引用计数、快照与原子恢复、快照生命周期管理（删除/保留策略淘汰/空间回收）、容量与回收统计、完整性校验与可区分的拒绝语义**。仅依赖本地能力，不引入外部服务或凭据，结果可在本地一次性复现。

## 架构

```
web/StorageController        REST 入口，错误码 -> HTTP 状态映射
service/ObjectStorageService 对象写/读/删，乐观并发控制（expectedVersion）
service/SnapshotService      快照创建/读取/原子恢复/删除/保留策略淘汰/统计
store/ContentStore           内容寻址存储（SHA-256），引用计数与容量限制
exception/ErrorCode          可区分的失败原因分类
```

## 去重与引用计数规则

- 内容以 **SHA-256 哈希**命名落盘（`storage.root/<hash>.blob`），内容相同的写入复用同一份数据，不重复占用容量。
- `store()` 使引用计数 +1（数据不存在时先落盘）；`release()` 使引用计数 -1，**归零时回收底层文件**。
- 覆盖写 **先存新内容、再释放旧内容**，保证写删交错时不会提前回收仍被引用的数据。
- 所有引用计数变更与文件操作在同一监视器下完成：并发写入、重复写入与删除交错时不会出现重复占用、引用丢失或提前回收。
- 落盘通过 **临时文件 + 原子移动** 完成，失败不留半写入状态；超限写入在落盘前被拒绝，不占用容量、不残留引用。

## 快照与恢复语义

- **创建快照**：复制当前索引（name -> hash/version/size），并对每个内容哈希 `addRef`。之后对当前对象的覆盖、新增、删除都不会改变快照的可读内容（快照引用的内容不会被回收）。
- **快照读取**：按快照中的元数据定位内容，读取时同样做完整性校验。
- **原子恢复**：分两阶段——
  1. **校验阶段**（只读）：对快照引用的全部内容重算哈希校验，任一失败则抛 `RESTORE_FAILED`，当前状态完全不变（无部分恢复）；
  2. **替换阶段**：一次性用快照索引替换当前索引，仅涉及引用计数转移，不会失败。

## 快照生命周期：删除、淘汰与空间回收

- **删除快照**（`DELETE /api/snapshots/{name}`）：从快照表移除并逐个 `release` 其引用的内容哈希。只有某份底层数据不再被当前对象和任何剩余快照引用（引用计数归零）时才真正回收，回收量立即体现在 `usedBytes` 上。当前对象的读写完全不受影响。
- **重复/并发删除**：删除操作在同一监视器下串行化，同一快照只能成功删除一次；其余并发删除得到 `SNAPSHOT_NOT_FOUND`，不会出现计数错乱或提前回收。
- **in-use 保护**：正在被读取或恢复的快照处于 in-use 状态。此时手动删除得到 `SNAPSHOT_IN_USE`（409），保留策略淘汰则自动跳过该快照——绝不中途抽走数据。
- **保留策略**（创建快照时可选参数）：
  - `maxSnapshots`：最多保留几份。超过上限时最旧的快照自动让位（按创建顺序，不依赖时钟相等性）；
  - `maxAgeMillis`：保留多长时间。创建时间早于 `now - maxAgeMillis` 的旧快照被淘汰；
  - 刚创建的快照与 in-use 快照永不在本次创建中被淘汰；若没有可淘汰候选，暂时超出上限并记录日志。
- **淘汰与手动删除同一回收口径**：两者都走同一条“移除快照 + 释放引用”的路径，最终存储状态一致。被淘汰的快照留下墓碑，后续读取/恢复/删除得到 `SNAPSHOT_EVICTED`（410），与“从未存在或已手动删除”的 `SNAPSHOT_NOT_FOUND`（404）可区分。同名快照重建后墓碑清除。
- **默认行为与兼容范围**：不带策略参数的 `POST /api/snapshots/{name}` 行为与之前完全一致（不限份数、不限时长，不淘汰任何快照）；旧的创建/读取/恢复接口与错误码语义不变。

## 容量与回收统计口径

- `GET /api/stats`：`usedBytes`（当前实际占用，与磁盘落盘一致）、`capacityBytes`（上限）、`dedupSavedBytes`（去重命中累计省下的字节数）、`snapshotCount`。
- `GET /api/snapshots`：每份快照的 `referencedBytes` 与 `exclusiveBytes`。

### “删除一份快照预计能回收多少”（exclusiveBytes）的统计口径

删除快照时，系统对快照索引中的**每条对象记录**逐个 `release` 其内容哈希；某份内容的全局引用计数降到 0 时，对应的 blob 文件才真正删除、容量才真正下降。预计回收量必须与这条删除路径**逐字节一致**，因此按以下规则计算：

1. **引用是按记录计数的，落盘是按内容去重的。** 同一份快照里若有多个对象内容相同（多个对象名指向同一哈希），它们共享一份 blob，但快照对该哈希持有**多条引用**（有几条记录就持有几个引用）。
2. **独占判定**：对快照引用的每个内容哈希 H，先数出“这份快照自己”对 H 的引用数 `myRefs`（快照内同内容记录条数），再取 H 的**全局**引用计数 `globalRefs`（= 当前对象的引用 + 所有快照的引用之和）。当且仅当 `myRefs == globalRefs` 时，H 的全部引用都由这份快照持有——即当前对象和任何其它快照都不再引用它——删除这份快照才会把它释放到 0 并真正回收。
   - 只要 H 还被**当前对象**或**任何其它快照**引用（哪怕只引用一次），`globalRefs > myRefs`，就不计入预计回收，删除后也确实不会回收它。
   - 典型反例（曾出现的 bug）：两个内容相同的对象去重落盘一份，打一份快照（快照内 2 条同哈希记录），随后把两个当前对象都删掉。此时全局引用计数为 2，且这 2 个引用**全部**来自这份快照。旧实现只判断“全局计数 == 1”，于是预计回收报 0；但真删快照时会 release 两次、计数归零，实际回收了一整份内容大小，造成预计与实际不符。修复后按 `myRefs(2) == globalRefs(2)` 判定为独占，预计与实际都等于这一份 blob 的大小。
3. **按 blob 大小计，不按记录条数翻倍。** 无论快照内同内容有多少条记录，`exclusiveBytes` 与 `referencedBytes` 对每个不同哈希都只计**一份 blob 的字节数**（磁盘上本来就只有一份）。
4. **referencedBytes**：快照引用的所有**不同**内容哈希的 blob 大小之和（含与他人共享的部分），同样对同内容多条记录只计一次。
5. 统计读取的是 `ContentStore` 在同一监视器下取出的引用计数一致性快照，避免并发 release 把逐哈希读数撕裂；删除/淘汰在服务层串行化，因此在操作静止后，“统计预计回收”与“删除实际释放”必然相等。删除一份快照还可能使原先与它共享的内容变成另一份快照的独占内容，后续统计会立即反映这一点。
- 一致性保证：所有引用计数与容量变更和文件操作在同一监视器下完成；并发写入、删除、创建/删除快照交错静止后，`usedBytes` 与实际磁盘占用（`ContentStore.diskUsedBytes()`）逐项相等，且每份存活快照的 `exclusiveBytes` 与其真实删除释放量相等，测试对此做了断言。

## 完整性与错误分类

读取时重算内容哈希并与元数据比对，不静默返回错误数据。失败原因通过 `ErrorCode` 区分：

| ErrorCode | 含义 | HTTP 状态 |
|---|---|---|
| `CONTENT_CORRUPTED` | 内容哈希与元数据不一致，或元数据引用的内容缺失 | 422 |
| `CONCURRENT_CONFLICT` | 乐观锁版本不匹配（`expectedVersion` != 当前版本） | 409 |
| `CAPACITY_EXCEEDED` | 写入超出 `storage.capacity-bytes` | 507 |
| `OBJECT_NOT_FOUND` | 对象不存在 | 404 |
| `SNAPSHOT_NOT_FOUND` | 快照不存在（从未存在或已手动删除） | 404 |
| `SNAPSHOT_EVICTED` | 快照已被保留策略淘汰 | 410 |
| `SNAPSHOT_IN_USE` | 快照正在被读取或恢复，暂时不能删除 | 409 |
| `RESTORE_FAILED` | 恢复校验失败，已回滚，无部分恢复结果 | 500 |

## REST 入口

```
PUT    /api/objects/{name}?expectedVersion=   写入（可带乐观锁版本）
GET    /api/objects/{name}                    读取（含完整性校验）
DELETE /api/objects/{name}                    删除
GET    /api/objects                           列表
POST   /api/snapshots/{name}?maxSnapshots=&maxAgeMillis=   创建快照（可带保留策略）
DELETE /api/snapshots/{name}                删除快照（重复删除/并发删除只成功一次）
GET    /api/snapshots                       快照列表与每份空间口径（referenced/exclusive）
GET    /api/stats                           整体容量统计（已用/上限/去重节省/快照数）
GET    /api/snapshots/{name}/objects/{obj}  读快照中的对象
POST   /api/snapshots/{name}/restore        原子恢复
```

错误响应体：`{"errorCode": "...", "message": "..."}`。

## 配置

`src/main/resources/application.properties`：

- `storage.root`：blob 落盘目录（默认 `data/content`）
- `storage.capacity-bytes`：容量上限（默认 64 MiB）

## 验证方法

```bash
mvn test
```

### 手工复现“预计回收 == 实际回收”（同内容两条记录成为唯一持有者）

启动服务（`mvn spring-boot:run`，默认端口 8080）后：

```bash
# 1) 放两个内容完全一样的对象（服务端按 SHA-256 去重，落盘一份 16 字节 blob）
printf 'identical-payload' | curl -s -X PUT --data-binary @- http://localhost:8080/api/objects/obj-a
printf 'identical-payload' | curl -s -X PUT --data-binary @- http://localhost:8080/api/objects/obj-b

# 2) 打一份快照（快照内有两条指向同一哈希的记录）
curl -s -X POST http://localhost:8080/api/snapshots/snap

# 3) 把两个当前对象都删掉（此时该 blob 的全部引用都来自这份快照）
curl -s -X DELETE http://localhost:8080/api/objects/obj-a
curl -s -X DELETE http://localhost:8080/api/objects/obj-b

# 4) 看快照统计：exclusiveBytes 应为 16（一份 blob 的大小），而不是 0
curl -s http://localhost:8080/api/snapshots
# [{"name":"snap","createdAtMillis":...,"objectCount":2,"referencedBytes":16,"exclusiveBytes":16}]
curl -s http://localhost:8080/api/stats    # 删除前 usedBytes=16

# 5) 删除快照：usedBytes 实际下降 16，与第 4 步的预计逐字节一致
curl -s -X DELETE http://localhost:8080/api/snapshots/snap
curl -s http://localhost:8080/api/stats    # 删除后 usedBytes=0
```

判定依据：第 4 步的 `exclusiveBytes` 必须等于第 5 步删除前后 `usedBytes` 的差值；共享内容（仍被当前对象或其它快照引用）不计入 `exclusiveBytes`，删除后也不会被回收。

测试一次性覆盖并本地复现以下场景，日志中打印输入、引用计数变化与判定依据：

- `ObjectStorageDedupTest`：重复写入去重与引用计数准确性；16 线程并发写同一内容（引用计数 == 线程数、仅一份数据落盘）；并发删除交错不提前回收；覆盖写释放旧内容；版本冲突拒绝且不留半写入。
- `SnapshotServiceTest`：快照与当前数据隔离（覆盖/删除/新增均不影响快照）；恢复后当前状态与快照一致；**恢复失败回退**（篡改快照引用的 blob 后恢复抛 `RESTORE_FAILED`，当前状态逐对象比对不变）。
- `IntegrityAndCapacityTest`：内容损坏与内容缺失均报 `CONTENT_CORRUPTED`；超限写入报 `CAPACITY_EXCEEDED` 且无容量/引用残留；去重命中不重复占用容量。
- `SnapshotLifecycleTest`：删除快照后当前对象不受影响且仅回收独占数据；引用计数归零才回收；删除不存在的快照报 `SNAPSHOT_NOT_FOUND`；8 线程并发删除同一快照恰好成功一次、其余报 `SNAPSHOT_NOT_FOUND` 且计数无错乱；按份数/按时长保留策略淘汰最旧快照且不误伤更近快照与当前对象；被淘汰快照读/恢复/删除均报 `SNAPSHOT_EVICTED`；淘汰与手动删除最终状态一致；统计口径（referenced/exclusive/dedupSaved）与磁盘占用逐项核对；6 线程交错并发静止后 `usedBytes == diskUsedBytes` 且对象全部可读；恢复进行中删除快照报 `SNAPSHOT_IN_USE`，恢复完整完成后删除可成功。
  - **预计回收口径专项**（本次修复重点）：
    - `duplicateRecordsInOneSnapshotBecomingSoleHolderPredictReclaimExactly`：两个同内容对象去重后打一份快照（快照内 2 条同哈希记录），再删掉两个当前对象——该快照成为唯一持有者时 `exclusiveBytes` 等于一整份 blob 大小，且删除实际释放量逐字节相等，磁盘无残留；
    - `exclusiveAccountingAcrossSharedSnapshotsAndCurrentObjects`：X 三方共享、Y 两个快照共享、Z 仅 s1 持有且快照内两条同内容记录、E 仅 s2 持有、W 仅当前对象的交叉矩阵，逐份验证预计==实际、共享内容不被提前回收、删一份后另一份的独占集会相应变化、当前对象始终可读可写；
    - `retentionEvictionReclaimsExactlyPredictedExclusiveBytes`：保留策略自动淘汰的实际释放量等于淘汰前统计的 `exclusiveBytes`，与手动删除同口径；
    - `afterConcurrentChurnEverySnapshotPredictionMatchesActualReclaim`：6 线程并发创建/删除快照并夹着对象读写，静止后逐份“现取统计→真删→核对释放量”，每份快照预计回收 == 实际回收，且 `usedBytes == diskUsedBytes`、当前对象全部可读。
- `SnapshotApiE2ETest`：通过 HTTP 端到端验证删除快照、重复删除（404 `SNAPSHOT_NOT_FOUND`）、保留策略淘汰（410 `SNAPSHOT_EVICTED` 与 404 区分）、快照列表与容量统计、淘汰回收量体现在 `usedBytes` 上。
