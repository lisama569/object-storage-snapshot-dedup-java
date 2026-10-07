package com.github.lisama569.objectstoragesnapshotdedupjava.model;

/**
 * 快照视图：名称、创建时间、对象数与空间占用口径。
 *
 * referencedBytes：快照引用的去重后不同内容（blob）大小之和。同一份快照里即使多个对象
 *                  指向相同内容（多条同哈希记录），落盘只有一份 blob，这里也只计一次。
 * exclusiveBytes：删除该快照预计能回收的空间，与删除后实际释放量逐字节一致。判定规则：
 *                  对快照引用的每个内容哈希，统计这份快照“自己”持有它的引用数 myRefs
 *                  （同一快照中同内容有 N 条记录则 myRefs=N），当且仅当
 *                  myRefs == 该哈希的全局引用计数时（当前对象与任何其它快照都不再引用它，
 *                  该快照是唯一持有者），才把这一份 blob 的大小计入。独占内容同样按一份
 *                  blob 大小计算，不按记录条数翻倍。
 */
public record SnapshotMeta(String name, long createdAtMillis, int objectCount,
                           long referencedBytes, long exclusiveBytes) {
}
