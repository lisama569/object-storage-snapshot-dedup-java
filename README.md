# object-storage-snapshot-dedup-java

基于本地文件系统模拟对象后端的存储服务，提供 **写入去重、引用计数、快照与原子恢复、完整性校验与可区分的拒绝语义**。仅依赖本地能力，不引入外部服务或凭据，结果可在本地一次性复现。

## 架构

```
web/StorageController        REST 入口，错误码 -> HTTP 状态映射
service/ObjectStorageService 对象写/读/删，乐观并发控制（expectedVersion）
service/SnapshotService      快照创建/读取/原子恢复
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

## 完整性与错误分类

读取时重算内容哈希并与元数据比对，不静默返回错误数据。失败原因通过 `ErrorCode` 区分：

| ErrorCode | 含义 | HTTP 状态 |
|---|---|---|
| `CONTENT_CORRUPTED` | 内容哈希与元数据不一致，或元数据引用的内容缺失 | 422 |
| `CONCURRENT_CONFLICT` | 乐观锁版本不匹配（`expectedVersion` != 当前版本） | 409 |
| `CAPACITY_EXCEEDED` | 写入超出 `storage.capacity-bytes` | 507 |
| `OBJECT_NOT_FOUND` | 对象不存在 | 404 |
| `SNAPSHOT_NOT_FOUND` | 快照不存在 | 404 |
| `RESTORE_FAILED` | 恢复校验失败，已回滚，无部分恢复结果 | 500 |

## REST 入口

```
PUT    /api/objects/{name}?expectedVersion=   写入（可带乐观锁版本）
GET    /api/objects/{name}                    读取（含完整性校验）
DELETE /api/objects/{name}                    删除
GET    /api/objects                           列表
POST   /api/snapshots/{name}                  创建快照
GET    /api/snapshots/{name}/objects/{obj}    读快照中的对象
POST   /api/snapshots/{name}/restore          原子恢复
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

测试一次性覆盖并本地复现以下场景，日志中打印输入、引用计数变化与判定依据：

- `ObjectStorageDedupTest`：重复写入去重与引用计数准确性；16 线程并发写同一内容（引用计数 == 线程数、仅一份数据落盘）；并发删除交错不提前回收；覆盖写释放旧内容；版本冲突拒绝且不留半写入。
- `SnapshotServiceTest`：快照与当前数据隔离（覆盖/删除/新增均不影响快照）；恢复后当前状态与快照一致；**恢复失败回退**（篡改快照引用的 blob 后恢复抛 `RESTORE_FAILED`，当前状态逐对象比对不变）。
- `IntegrityAndCapacityTest`：内容损坏与内容缺失均报 `CONTENT_CORRUPTED`；超限写入报 `CAPACITY_EXCEEDED` 且无容量/引用残留；去重命中不重复占用容量。
