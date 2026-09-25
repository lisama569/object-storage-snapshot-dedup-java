# object-storage-snapshot-dedup-java

基于本地文件系统模拟的对象存储：内容去重、引用计数、快照/恢复与完整性校验。
仅依赖本地能力，不引入外部服务或凭据，结果可在本地一次性复现。

## 架构

```
api/StorageController        REST 入口，统一错误响应 {code, message}
storage/ObjectStorageService 命名空间（key -> ObjectMeta）、快照、乐观锁、原子恢复
storage/ChunkStore           内容寻址块存储：SHA-256 去重、引用计数、容量控制、完整性校验
storage/ErrorCode            可区分的失败原因分类
config/StorageProperties     storage.root-dir（默认 data/）、storage.capacity-bytes（默认 64MB）
```

对象内容按 SHA-256 哈希落盘为 `data/chunks/<hash>`，命名空间与快照只保存元数据
（contentHash、size、version），底层数据由 `ChunkStore` 统一持有。

## 去重与引用计数规则

- 相同内容的写入只落盘一次；每个引用该内容的 key（或快照）使引用计数 +1。
- 重复写入相同内容到同一 key：引用计数不变，仅版本号 +1。
- 覆盖写入：先成功写入新内容（新引用 +1），再释放旧内容引用（-1）；
  新内容写入失败（如超限）时旧状态完全不变。
- 删除对象/快照：释放其持有的引用；引用计数归零时回收 chunk 文件并释放容量。
- 容量按**去重后的唯一内容字节数**计费；重复内容不占用新容量。
- 所有引用计数变更在全局锁内完成，并发写、重复写与删除交错时不出现
  重复占用、引用丢失或提前回收（由 `DedupConcurrencyTest` 验证）。

## 快照与恢复语义

- `createSnapshot`：复制当前命名空间元数据，并对每个被引用 chunk `retain`，
  之后对当前对象的覆盖、新增、删除都不影响快照可读内容（隔离共存）。
- `restoreSnapshot`：**先校验、后提交**。先对快照引用的全部 chunk 做完整性校验，
  任一损坏/缺失即抛 `RESTORE_FAILED` 且当前数据完全不变；校验通过后先为新命名空间
  持有引用、再释放旧引用、最后整体替换（内存操作，天然原子），不留部分恢复结果。
- 恢复不消费快照，快照可重复用于恢复；`deleteSnapshot` 释放其持有的引用。

## 完整性与错误分类

读取时重新计算内容哈希并与元数据比对，损坏时显式报错，绝不静默返回错误数据。
所有失败以可区分的错误码返回（REST 层同时映射到不同 HTTP 状态）：

| ErrorCode | 含义 | HTTP |
|---|---|---|
| `NOT_FOUND` | 对象/快照/chunk 不存在 | 404 |
| `CONTENT_CORRUPTED` | 内容哈希与元数据不一致 | 422 |
| `CAPACITY_EXCEEDED` | 新唯一内容超出容量上限，无半写入状态 | 507 |
| `CONCURRENT_CONFLICT` | 乐观锁版本冲突（expectedVersion 不符） | 409 |
| `RESTORE_FAILED` | 恢复前置校验失败，已回退 | 422 |

## REST API

```
PUT    /objects/{key}?expectedVersion=   写入（body 为原始字节，可选乐观锁版本）
GET    /objects/{key}                    读取（含完整性校验）
DELETE /objects/{key}                    删除
GET    /objects                          列出当前命名空间
POST   /snapshots/{name}                 创建快照
POST   /snapshots/{name}/restore         原子恢复
DELETE /snapshots/{name}                 删除快照
GET    /snapshots/{name}/objects/{key}   读取快照中的对象
```

## 验证方法

```bash
mvn test
```

一次运行全部 14 个测试，覆盖：

- `DedupConcurrencyTest`：16 线程并发写相同内容去重为单 chunk；交错写/删下
  引用计数与存活引用一致；重复写不增加引用、归零即回收。
- `SnapshotIsolationTest`：快照与覆盖/新增/删除隔离；恢复回到快照状态；
  篡改快照独有 chunk 后恢复抛 `RESTORE_FAILED` 且当前数据与引用计数不变。
- `IntegrityAndLimitTest`：内容损坏抛 `CONTENT_CORRUPTED`；超限抛
  `CAPACITY_EXCEEDED` 且无半写入状态；版本冲突抛 `CONCURRENT_CONFLICT`；
  五种错误码互不相同。
- `StorageApiTest`：REST 入口下快照/恢复语义与错误码映射（404/409/507/422）一致。

测试日志打印输入（`[输入]`）、引用计数变化（`[引用]`，ChunkStore 亦逐条记录
`refCount x -> y`）与判定依据（`[判定]`），可在日志中直接核对结论。
