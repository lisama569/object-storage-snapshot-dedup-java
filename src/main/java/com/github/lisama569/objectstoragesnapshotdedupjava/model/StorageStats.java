package com.github.lisama569.objectstoragesnapshotdedupjava.model;

/**
 * 整体容量统计。
 *
 * usedBytes：当前实际占用（与磁盘落盘一致）；dedupSavedBytes：去重累计省下的字节数；
 * snapshotCount：现存快照数。
 */
public record StorageStats(long usedBytes, long capacityBytes, long dedupSavedBytes, int snapshotCount) {
}
