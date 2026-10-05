package com.github.lisama569.objectstoragesnapshotdedupjava.model;

import java.util.List;

/**
 * 整体容量与回收统计（对同一全局状态的一致快照）。
 *
 * @param usedBytes              底层 blob 实际占用字节（== 磁盘上 blob 文件大小之和）
 * @param logicalBytes           去重前的逻辑字节：当前对象条目 + 全部快照条目引用大小之和
 * @param dedupSavedBytes        去重省下的字节 = logicalBytes - usedBytes
 * @param snapshotLockedBytes    所有快照锁定的字节（快照条目口径求和，共享部分会重复计入）
 * @param snapshotReclaimableBytes 删除全部快照预计能回收的字节（仅快照持有者的 blob 大小之和）
 * @param snapshots              每个快照的明细（按创建时间从旧到新）
 */
public record SpaceStats(long usedBytes, long logicalBytes, long dedupSavedBytes,
                         long snapshotLockedBytes, long snapshotReclaimableBytes,
                         List<SnapshotInfo> snapshots) {
}
