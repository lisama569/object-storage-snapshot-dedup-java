package com.github.lisama569.objectstoragesnapshotdedupjava.model;

/**
 * 单个快照的生命周期与占用信息。
 *
 * @param name             快照名
 * @param createdAtMillis  创建时间（epoch millis）
 * @param objectCount      快照包含的对象条目数
 * @param lockedBytes      快照锁定的全部内容字节数（条目口径，含与其他持有者共享的部分）
 * @param reclaimableBytes 删除该快照预计能独占回收的字节数（删除后引用真正归零的 blob 大小之和）
 */
public record SnapshotInfo(String name, long createdAtMillis, int objectCount,
                           long lockedBytes, long reclaimableBytes) {
}
