package com.github.lisama569.objectstoragesnapshotdedupjava.model;

/**
 * 快照保留策略。
 *
 * @param maxCount    最多保留份数，null 表示不限份数
 * @param maxAgeMillis 最长保留时长（毫秒），null 表示不限时长
 */
public record RetentionPolicy(Integer maxCount, Long maxAgeMillis) {

    /** 不限制（默认，兼容旧行为）。 */
    public static RetentionPolicy unlimited() {
        return new RetentionPolicy(null, null);
    }
}
