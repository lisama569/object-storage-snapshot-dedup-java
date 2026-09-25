package com.github.lisama569.objectstoragesnapshotdedupjava.storage;

/**
 * 可区分的失败原因分类。
 */
public enum ErrorCode {
    /** 对象或快照不存在 */
    NOT_FOUND,
    /** 读取时内容与元数据摘要不一致 */
    CONTENT_CORRUPTED,
    /** 写入会超出容量上限 */
    CAPACITY_EXCEEDED,
    /** 乐观锁版本冲突（并发写冲突） */
    CONCURRENT_CONFLICT,
    /** 恢复过程中校验失败，已回退 */
    RESTORE_FAILED
}
