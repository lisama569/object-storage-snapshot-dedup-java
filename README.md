# object-storage-snapshot-dedup-java

基于本地文件系统模拟对象后端的存储服务，提供 **写入去重、引用计数、快照与原子恢复、快照生命周期管理（删除/淘汰/空间回收）、容量与回收统计、保留策略、完整性校验与可区分的拒绝语义**。仅依赖本地能力，不引入外部服务或凭据，结果可在本地一次性复现。

## 架构

```
web/StorageController        REST 入口，错误码 -> HTTP 状态映射
service/ObjectStorageService 对象写/读/删，乐观并发控制（expectedVersion）
service/SnapshotService      快照创建/读取/原子恢复/删除/淘汰/保留策略/容量统计
store/ContentStore           内容寻址存储（SHA-256），引用计数、容量限制、磁盘物理视图
model/RetentionPolicy        保留策略（最多份数 / 最长时长，均可空）
model/SpaceStats/SnapshotInfo 整体与单份快照的容量、锁定、可回收统计
model/SnapshotCreateResult/SnapshotDeleteResult 创建（含淘汰）/删除（含实际回收字节）结果
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

- **删除快照**（`DELETE /api/snapshots/{name}`）：先把快照从注册表摘除（因此重复/并发删除**恰好成功一次**，其余得到可区分的 `SNAPSHOT_NOT_FOUND`），再逐条释放该快照持有的引用。删除快照**不触碰当前对象**，当前对象照常读、写。
- **真正回收的判定**：只有当某份底层数据**不再被当前对象和任何剩余快照引用**（ContentStore 引用计数归零）时，blob 文件才被删除，回收才体现在容量上；仍被任何一方引用的数据绝不提前删除。删除结果返回 `reclaimedBytes`，即本次真正从磁盘回收的字节。
- **删除与恢复/读取交错**：快照的创建、删除、淘汰、恢复、读取、统计都在 `SnapshotService` 同一把监视器下串行，恢复与读取期间快照不可能被中途删除——读到的要么是完整内容，要么得到 `SNAPSHOT_NOT_FOUND`，不会出现读一半数据被抽走的损坏状态。
- **被删除/淘汰的快照**：不能再读取或用于回滚，统一返回 `SNAPSHOT_NOT_FOUND`（与“本来就不存在”同一可区分理由）。

## 容量与回收统计口径

`GET /api/stats`（整体）与 `GET /api/snapshots`（每一份快照明细）在同一全局一致快照下取数（统计时同时持有快照锁与对象存储锁，物理已用容量直接扫描磁盘上的 `*.blob`）：

| 指标 | 口径 |
|---|---|
| `usedBytes` | 底层 blob 实际占用，**等于磁盘上 blob 文件大小之和** |
| `logicalBytes` | 去重前逻辑字节：当前对象条目大小 + 全部快照条目大小之和 |
| `dedupSavedBytes` | 去重省下的字节 = `logicalBytes - usedBytes` |
| `snapshotLockedBytes` | 所有快照“锁定”的条目字节之和（共享部分按每份快照重复计入） |
| `snapshotReclaimableBytes` | 删除全部快照预计能回收的字节：只被快照引用、当前对象不再引用的 blob 大小之和 |
| `SnapshotInfo.lockedBytes` | 该快照全部条目的字节之和 |
| `SnapshotInfo.reclaimableBytes` | 删除**这一份**快照预计独占回收的字节：删除后引用真正归零的 blob 大小之和 |

`reclaimableBytes` 的预测与删除结果 `reclaimedBytes` 一一对应；并发写入/删除/创建/删除快照交错停止（收敛）后，报出的已用与可回收容量必然与磁盘实际一致，不会出现“统计说回收了但对象读不出来”或“统计显示没占空间、实际还占着”。

## 快照保留策略

创建快照时可通过查询参数约定保留策略（`POST /api/snapshots/{name}?maxCount=&maxAgeMillis=`，两者都可省略/为空）：

- `maxCount`：最多保留份数。超过上限时**最旧的快照自动让位淘汰**，其独占空间随同一套回收逻辑释放。
- `maxAgeMillis`：最长保留时长（毫秒）。创建时判定，年龄超过时长的最旧快照被淘汰（年龄按创建时间计）。两者可组合：先淘汰超龄快照，再按份数裁剪；**本次新建的快照保证不会被淘汰**。
- **默认行为与兼容范围**：两个参数都不传（或调用不带策略的创建接口）时为**不限制**，与历史行为完全一致——旧快照不会被淘汰。`maxCount <= 0` 或 `maxAgeMillis <= 0` 被拒绝并返回可区分的 `INVALID_RETENTION`（HTTP 400），且不创建任何快照。
- **淘汰与手动删除走同一套回收口径**（同一个内部方法、同样的引用计数转移），两种方式给存储带来的最终状态一致；被淘汰快照不可读/不可恢复，返回 `SNAPSHOT_NOT_FOUND`；淘汰不会误伤更近的快照或当前对象。

## 完整性与错误分类

读取时重算内容哈希并与元数据比对，不静默返回错误数据。失败原因通过 `ErrorCode` 区分：

| ErrorCode | 含义 | HTTP 状态 |
|---|---|---|
| `CONTENT_CORRUPTED` | 内容哈希与元数据不一致，或元数据引用的内容缺失 | 422 |
| `CONCURRENT_CONFLICT` | 乐观锁版本不匹配（`expectedVersion` != 当前版本） | 409 |
| `CAPACITY_EXCEEDED` | 写入超出 `storage.capacity-bytes` | 507 |
| `OBJECT_NOT_FOUND` | 对象不存在 | 404 |
| `SNAPSHOT_NOT_FOUND` | 快照不存在（含已被删除或保留策略淘汰） | 404 |
| `RESTORE_FAILED` | 恢复校验失败，已回滚，无部分恢复结果 | 500 |
| `INVALID_RETENTION` | 保留策略参数非法（`maxCount`/`maxAgeMillis` <= 0） | 400 |

## REST 入口

```
PUT    /api/objects/{name}?expectedVersion=   写入（可带乐观锁版本）
GET    /api/objects/{name}                    读取（含完整性校验）
DELETE /api/objects/{name}                    删除
GET    /api/objects                           列表
POST   /api/snapshots/{name}?maxCount=&maxAgeMillis=  创建快照（可选保留策略，返回淘汰明细）
GET    /api/snapshots                         快照明细列表（旧 -> 新，含锁定/预计回收字节）
GET    /api/stats                             整体已用/去重节省/快照锁定与可回收统计
DELETE /api/snapshots/{name}                  删除快照（返回 evicted 标志与 reclaimedBytes）
GET    /api/snapshots/{name}/objects/{obj}    读快照中的对象
POST   /api/snapshots/{name}/restore          原子恢复
```

创建快照响应：`{"name": "...", "evicted": [{"name": "...", "evicted": true, "reclaimedBytes": 123}]}`；
删除快照响应：`{"name": "...", "evicted": false, "reclaimedBytes": 123}`。
错误响应体：`{"errorCode": "...", "message": "..."}`。

## 配置

`src/main/resources/application.properties`：

- `storage.root`：blob 落盘目录（默认 `data/content`）
- `storage.capacity-bytes`：容量上限（默认 64 MiB）

## 验证方法

```bash
mvn test
```

测试一次性覆盖并本地复现以下场景，日志中打印输入、引用计数变化、回收字节与判定依据：

- `ObjectStorageDedupTest`：重复写入去重与引用计数准确性；16 线程并发写同一内容（引用计数 == 线程数、仅一份数据落盘）；并发删除交错不提前回收；覆盖写释放旧内容；版本冲突拒绝且不留半写入。
- `SnapshotServiceTest`：快照与当前数据隔离（覆盖/删除/新增均不影响快照）；恢复后当前状态与快照一致；**恢复失败回退**（篡改快照引用的 blob 后恢复抛 `RESTORE_FAILED`，当前状态逐对象比对不变）。
- `IntegrityAndCapacityTest`：内容损坏与内容缺失均报 `CONTENT_CORRUPTED`；超限写入报 `CAPACITY_EXCEEDED` 且无容量/引用残留；去重命中不重复占用容量。
- `SnapshotLifecycleTest`（**新增**）：删除快照仅回收独占 blob、共享数据保留且当前对象可读写；全共享快照删除回收 0；删除不存在快照 `SNAPSHOT_NOT_FOUND`；重复删除只成功一次；8 线程并发删除恰好成功一次、计数无错乱、数据可读；已删快照读/恢复均失败；恢复与删除并发交错的两种安全结局；20 轮读取/删除并发下“读到即完整、否则 `SNAPSHOT_NOT_FOUND`”。
- `SnapshotSpaceStatsTest`（**新增**）：已用容量与磁盘 blob 大小对账、预计回收 == 实际回收、去重节省口径；10 个相同条目去重节省 90%；跨快照共享 blob 仅在最后一个持有者删除后回收；明细按时间从旧到新；4 写线程 + 快照创建/删除线程 + 统计巡检线程并发交错 1.5s，收敛后统计与磁盘精确对账、预测回收 == 实际回收、全部当前对象可完整读出。
- `SnapshotRetentionTest`（**新增**）：`maxCount` 淘汰最旧快照并回收独占空间、被淘汰快照读/恢复均 `SNAPSHOT_NOT_FOUND`；淘汰不误伤更近快照与当前对象；边界恰好保留 N 份且新建快照不被淘汰；`maxAgeMillis` 超龄淘汰、未龄保留；份数与时长组合淘汰与手动删除同一回收口径；`maxCount<=0`/`maxAge<=0` 返回 `INVALID_RETENTION` 且无副作用；不带策略默认不限（兼容旧行为）。
- `SnapshotLifecycleHttpTest`（**新增，HTTP 端到端**）：经真实 HTTP 端口跑通对象写入→建快照→演进→`/stats` 预测回收→删除快照返回 `reclaimedBytes` 与预测一致→当前对象不受影响→读已删快照 404；删除不存在/重复删除的 404 区分；`maxCount=2` 经查询参数淘汰最旧快照、被淘汰快照恢复 404、`maxCount=0` 返回 400 `INVALID_RETENTION`；同名快照重复创建 409 `CONCURRENT_CONFLICT`。

### 本地手工验证（curl）

```bash
mvn spring-boot:run                      # 使用 application.properties 中的 data/content 目录

# 写对象、建快照、演进当前数据
curl -s -X PUT --data-binary 'shared'   http://localhost:8080/api/objects/a
curl -s -X PUT --data-binary 'snap-only' http://localhost:8080/api/objects/b
curl -s -X POST http://localhost:8080/api/snapshots/s1
curl -s -X PUT --data-binary 'current-b' http://localhost:8080/api/objects/b

# 查看整体与每快照的占用/预计回收
curl -s http://localhost:8080/api/stats | jq .
curl -s http://localhost:8080/api/snapshots | jq .

# 删除快照（返回实际回收字节），再确认当前对象完好、已删快照 404
curl -s -X DELETE http://localhost:8080/api/snapshots/s1 | jq .
curl -s http://localhost:8080/api/objects/a
curl -i http://localhost:8080/api/snapshots/s1/objects/b

# 保留策略：最多 2 份，创建第 3 份时最旧快照被淘汰（evicted 明细在响应中）
curl -s -X POST 'http://localhost:8080/api/snapshots/r1?maxCount=2'
curl -s -X POST 'http://localhost:8080/api/snapshots/r2?maxCount=2'
curl -s -X POST 'http://localhost:8080/api/snapshots/r3?maxCount=2' | jq .
```
