package com.github.lisama569.objectstoragesnapshotdedupjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 本地存储配置：数据根目录与容量上限（字节）。
 */
@Component
@ConfigurationProperties(prefix = "storage")
public class StorageProperties {

    /** 本地数据根目录 */
    private String rootDir = "data";

    /** 去重后唯一内容总字节数上限 */
    private long capacityBytes = 64L * 1024 * 1024;

    public String getRootDir() {
        return rootDir;
    }

    public void setRootDir(String rootDir) {
        this.rootDir = rootDir;
    }

    public long getCapacityBytes() {
        return capacityBytes;
    }

    public void setCapacityBytes(long capacityBytes) {
        this.capacityBytes = capacityBytes;
    }
}
