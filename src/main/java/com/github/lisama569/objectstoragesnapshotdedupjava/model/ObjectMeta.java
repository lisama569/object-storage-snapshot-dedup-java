package com.github.lisama569.objectstoragesnapshotdedupjava.model;

/**
 * 对象元数据：名称 -> 内容哈希 + 版本 + 大小。
 */
public record ObjectMeta(String name, String contentHash, long size, long version) {
}
