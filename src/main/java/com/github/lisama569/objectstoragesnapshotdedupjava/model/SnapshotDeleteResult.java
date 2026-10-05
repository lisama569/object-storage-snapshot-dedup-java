package com.github.lisama569.objectstoragesnapshotdedupjava.model;

/**
 * 删除/淘汰快照的结果与实际回收口径。
 *
 * @param name           被删除或淘汰的快照名
 * @param evicted        是否由保留策略自动淘汰（false 表示手动删除）
 * @param reclaimedBytes 实际从底层回收的字节数（引用归零的 blob 大小之和）
 */
public record SnapshotDeleteResult(String name, boolean evicted, long reclaimedBytes) {
}
