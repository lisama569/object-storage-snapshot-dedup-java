package com.github.lisama569.objectstoragesnapshotdedupjava.model;

/**
 * 快照视图：名称、创建时间、对象数与空间占用口径。
 *
 * referencedBytes：快照引用的全部底层数据大小（可能与其他快照/当前对象共享）；
 * exclusiveBytes：仅被该快照引用的数据大小，即删除该快照预计能回收的空间。
 */
public record SnapshotMeta(String name, long createdAtMillis, int objectCount,
                           long referencedBytes, long exclusiveBytes) {
}
