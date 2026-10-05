package com.github.lisama569.objectstoragesnapshotdedupjava.exception;

/**
 * 可区分的失败原因分类。
 */
public enum ErrorCode {
    /** 读取时内容哈希与元数据不一致 */
    CONTENT_CORRUPTED,
    /** 乐观锁版本冲突 */
    CONCURRENT_CONFLICT,
    /** 超出容量限制 */
    CAPACITY_EXCEEDED,
    /** 对象不存在 */
    OBJECT_NOT_FOUND,
    /** 快照不存在 */
    SNAPSHOT_NOT_FOUND,
    /** 恢复失败（已回滚，无部分恢复结果） */
    RESTORE_FAILED,
    /** 保留策略参数非法（份数/时长 <= 0） */
    INVALID_RETENTION
}
