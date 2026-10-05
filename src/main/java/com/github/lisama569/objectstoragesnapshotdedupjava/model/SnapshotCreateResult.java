package com.github.lisama569.objectstoragesnapshotdedupjava.model;

import java.util.List;

/**
 * 创建快照的结果，包含本次因保留策略被淘汰的快照及合计回收字节。
 */
public record SnapshotCreateResult(String name, List<SnapshotDeleteResult> evicted) {

    public long evictedBytes() {
        return evicted.stream().mapToLong(SnapshotDeleteResult::reclaimedBytes).sum();
    }
}
