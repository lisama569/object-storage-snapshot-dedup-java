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
 * 快照创建、读取与原子恢复。
 *
 * 语义：
 *  - 创建快照时复制当前索引并对每个内容哈希 addRef，之后当前对象的覆盖/新增/删除
 *    都不会改变快照可读内容（内容不会被回收）；
 *  - 恢复采用“先校验、后整体替换”：先对快照引用的全部内容做完整性校验，
 *    任一失败则抛 RESTORE_FAILED 且不改动当前状态（无部分恢复）；
 *    校验通过后一次性替换索引，替换过程只涉及引用计数转移，不会失败。
 */
@Service
public class SnapshotService {

    private static final Logger log = LoggerFactory.getLogger(SnapshotService.class);

    private final ObjectStorageService objectStorage;
    private final ContentStore contentStore;
    private final Map<String, Map<String, ObjectMeta>> snapshots = new ConcurrentHashMap<>();

    public SnapshotService(ObjectStorageService objectStorage, ContentStore contentStore) {
        this.objectStorage = objectStorage;
        this.contentStore = contentStore;
    }

    public synchronized String createSnapshot(String snapshotName) {
        if (snapshots.containsKey(snapshotName)) {
            throw new StorageException(ErrorCode.CONCURRENT_CONFLICT,
                    "snapshot already exists: " + snapshotName);
        }
        Map<String, ObjectMeta> index = objectStorage.currentIndex();
        for (ObjectMeta meta : index.values()) {
            contentStore.addRef(meta.contentHash());
        }
        snapshots.put(snapshotName, index);
        log.info("[Snapshot.create] name={} objects={}", snapshotName, index.size());
        return snapshotName;
    }

    public byte[] readFromSnapshot(String snapshotName, String objectName) {
        Map<String, ObjectMeta> index = snapshot(snapshotName);
        ObjectMeta meta = index.get(objectName);
        if (meta == null) {
            throw new StorageException(ErrorCode.OBJECT_NOT_FOUND,
                    "object not found in snapshot " + snapshotName + ": " + objectName);
        }
        return contentStore.read(meta.contentHash());
    }

    /**
     * 原子恢复：失败时不留下部分恢复的结果。
     */
    public void restore(String snapshotName) {
        Map<String, ObjectMeta> index = snapshot(snapshotName);
        // 阶段一：完整性校验（只读，不改动任何状态）。
        for (ObjectMeta meta : index.values()) {
            try {
                contentStore.read(meta.contentHash());
            } catch (StorageException e) {
                log.info("[Snapshot.restore] name={} object={} hash={} validation failed: {} -> RESTORE_FAILED (rolled back, no partial state)",
                        snapshotName, meta.name(), meta.contentHash(), e.getMessage());
                throw new StorageException(ErrorCode.RESTORE_FAILED,
                        "restore of snapshot " + snapshotName + " aborted: " + e.getMessage(), e);
            }
        }
        // 阶段二：整体替换（引用计数转移，不会失败）。
        objectStorage.applyIndex(new HashMap<>(index));
        log.info("[Snapshot.restore] name={} restored {} objects atomically", snapshotName, index.size());
    }

    public List<String> listSnapshots() {
        return new ArrayList<>(snapshots.keySet());
    }

    private Map<String, ObjectMeta> snapshot(String snapshotName) {
        Map<String, ObjectMeta> index = snapshots.get(snapshotName);
        if (index == null) {
            throw new StorageException(ErrorCode.SNAPSHOT_NOT_FOUND, "snapshot not found: " + snapshotName);
        }
        return index;
    }
}
