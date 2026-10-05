package com.github.lisama569.objectstoragesnapshotdedupjava.store;

import java.util.Map;

/**
 * ContentStore 物理状态的一致快照：已用容量与每个 blob 的物理大小。
 */
public record PhysicalView(long usedBytes, Map<String, Long> blobSizes) {
}
