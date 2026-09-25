package com.github.lisama569.objectstoragesnapshotdedupjava.service;

import com.github.lisama569.objectstoragesnapshotdedupjava.exception.ErrorCode;
import com.github.lisama569.objectstoragesnapshotdedupjava.exception.StorageException;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.ObjectMeta;
import com.github.lisama569.objectstoragesnapshotdedupjava.store.ContentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 对象写入/读取/删除入口。
 *
 * 规则：
 *  - 写入按内容去重（见 ContentStore），覆盖写先存新内容再释放旧内容，避免引用丢失；
 *  - 乐观并发：put(name, content, expectedVersion) 版本不匹配时抛 CONCURRENT_CONFLICT；
 *  - 读取经 ContentStore 完整性校验，损坏抛 CONTENT_CORRUPTED，不静默返回错误数据。
 */
@Service
public class ObjectStorageService {

    private static final Logger log = LoggerFactory.getLogger(ObjectStorageService.class);

    private final ContentStore contentStore;
    private final Map<String, ObjectMeta> index = new ConcurrentHashMap<>();

    public ObjectStorageService(ContentStore contentStore) {
        this.contentStore = contentStore;
    }

    public ObjectMeta put(String name, byte[] content) {
        ObjectMeta current = index.get(name);
        long expected = current == null ? 0 : current.version();
        return put(name, content, expected);
    }

    public synchronized ObjectMeta put(String name, byte[] content, long expectedVersion) {
        ObjectMeta current = index.get(name);
        long currentVersion = current == null ? 0 : current.version();
        if (currentVersion != expectedVersion) {
            log.info("[ObjectStorage.put] name={} expectedVersion={} currentVersion={} -> CONCURRENT_CONFLICT",
                    name, expectedVersion, currentVersion);
            throw new StorageException(ErrorCode.CONCURRENT_CONFLICT,
                    "version conflict on " + name + ": expected=" + expectedVersion + " current=" + currentVersion);
        }
        // 先存新内容（去重 + 引用计数 +1），再释放旧内容，保证交错时不会提前回收。
        String hash = contentStore.store(content);
        if (current != null) {
            contentStore.release(current.contentHash());
        }
        ObjectMeta meta = new ObjectMeta(name, hash, content.length, currentVersion + 1);
        index.put(name, meta);
        log.info("[ObjectStorage.put] name={} hash={} version={} size={}", name, hash, meta.version(), meta.size());
        return meta;
    }

    public byte[] get(String name) {
        return contentStore.read(meta(name).contentHash());
    }

    public ObjectMeta meta(String name) {
        ObjectMeta meta = index.get(name);
        if (meta == null) {
            throw new StorageException(ErrorCode.OBJECT_NOT_FOUND, "object not found: " + name);
        }
        return meta;
    }

    public synchronized void delete(String name) {
        ObjectMeta meta = index.remove(name);
        if (meta == null) {
            throw new StorageException(ErrorCode.OBJECT_NOT_FOUND, "object not found: " + name);
        }
        contentStore.release(meta.contentHash());
        log.info("[ObjectStorage.delete] name={} hash={} released", name, meta.contentHash());
    }

    public List<ObjectMeta> list() {
        return new ArrayList<>(index.values());
    }

    /** 当前索引的不可变副本（快照用）。 */
    public synchronized Map<String, ObjectMeta> currentIndex() {
        return new HashMap<>(index);
    }

    /**
     * 原子地用目标索引替换当前索引（恢复快照用）。
     * 调用方需保证目标索引引用的内容均已通过完整性校验且被快照持有引用，
     * 因此本方法不会失败，不留部分恢复结果。
     */
    synchronized void applyIndex(Map<String, ObjectMeta> target) {
        for (ObjectMeta meta : target.values()) {
            contentStore.addRef(meta.contentHash());
        }
        for (ObjectMeta meta : index.values()) {
            contentStore.release(meta.contentHash());
        }
        index.clear();
        index.putAll(target);
        log.info("[ObjectStorage.applyIndex] restored {} objects", target.size());
    }
}
