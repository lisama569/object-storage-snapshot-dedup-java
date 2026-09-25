package com.github.lisama569.objectstoragesnapshotdedupjava.storage;

/**
 * 对象元数据：内容哈希、大小与乐观锁版本。
 */
public record ObjectMeta(String contentHash, long size, long version) {
}
