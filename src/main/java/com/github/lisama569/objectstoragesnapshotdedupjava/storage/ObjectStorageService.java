package com.github.lisama569.objectstoragesnapshotdedupjava.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 对象存储服务：写入/读取/删除（内容去重）、快照与恢复。
 *
 * 语义：
 * - 命名空间（key -> ObjectMeta）与快照均为内存视图，底层内容由 ChunkStore 按哈希去重持有；
 * - 写入支持乐观锁：expectedVersion 与当前版本不符时抛 CONCURRENT_CONFLICT；
 * - 快照创建时对每个被引用 chunk retain，之后当前命名空间的任何变更都不影响快照；
 * - 恢复采用“先校验、后提交”：全部 chunk 校验通过才替换命名空间，失败抛 RESTORE_FAILED 且不留部分结果。
 */
@Service
public class ObjectStorageService {

    private static final Logger log = LoggerFactory.getLogger(ObjectStorageService.class);

    private final ChunkStore chunkStore;
    private final Map<String, ObjectMeta> current = new LinkedHashMap<>();
    private final Map<String, Map<String, ObjectMeta>> snapshots = new LinkedHashMap<>();

    public ObjectStorageService(ChunkStore chunkStore) {
        this.chunkStore = chunkStore;
    }

    /** 写入对象；expectedVersion 为 null 表示不校验版本，否则版本不符抛 CONCURRENT_CONFLICT */
    public synchronized ObjectMeta putObject(String key, byte[] content, Long expectedVersion) {
        ObjectMeta old = current.get(key);
        long oldVersion = old == null ? 0 : old.version();
        if (expectedVersion != null && expectedVersion != oldVersion) {
            log.info("[put] CONCURRENT_CONFLICT key={} expectedVersion={} actualVersion={}",
                    key, expectedVersion, oldVersion);
            throw new StorageException(ErrorCode.CONCURRENT_CONFLICT,
                    "version conflict on key " + key + ": expected " + expectedVersion + " but actual " + oldVersion);
        }
        String newHash = ChunkStore.sha256(content);
        if (old != null && old.contentHash().equals(newHash)) {
            // 内容未变化：只推进版本，引用计数不变
            ObjectMeta meta = new ObjectMeta(newHash, content.length, oldVersion + 1);
            current.put(key, meta);
            log.info("[put] key={} unchanged content hash={} version {} -> {}", key, newHash, oldVersion, meta.version());
            return meta;
        }
        // 先成功写入新内容（可能抛 CAPACITY_EXCEEDED，此时旧状态完全不变），再释放旧引用
        chunkStore.put(content);
        if (old != null) {
            chunkStore.release(old.contentHash());
        }
        ObjectMeta meta = new ObjectMeta(newHash, content.length, oldVersion + 1);
        current.put(key, meta);
        log.info("[put] key={} hash={} size={} version {} -> {} refCount={}",
                key, newHash, content.length, oldVersion, meta.version(), chunkStore.refCount(newHash));
        return meta;
    }

    /** 读取对象，内容损坏抛 CONTENT_CORRUPTED，不存在抛 NOT_FOUND */
    public synchronized byte[] getObject(String key) {
        ObjectMeta meta = current.get(key);
        if (meta == null) {
            throw new StorageException(ErrorCode.NOT_FOUND, "object not found: " + key);
        }
        return chunkStore.get(meta.contentHash());
    }

    /** 删除对象并释放引用 */
    public synchronized void deleteObject(String key) {
        ObjectMeta meta = current.remove(key);
        if (meta == null) {
            throw new StorageException(ErrorCode.NOT_FOUND, "object not found: " + key);
        }
        chunkStore.release(meta.contentHash());
        log.info("[delete] key={} hash={} refCount now {}", key, meta.contentHash(), chunkStore.refCount(meta.contentHash()));
    }

    /** 创建快照，与当前数据隔离共存；同名快照被替换 */
    public synchronized void createSnapshot(String snapshotName) {
        Map<String, ObjectMeta> old = snapshots.remove(snapshotName);
        if (old != null) {
            old.values().forEach(m -> chunkStore.release(m.contentHash()));
        }
        Map<String, ObjectMeta> snap = new LinkedHashMap<>(current);
        snap.values().forEach(m -> chunkStore.retain(m.contentHash()));
        snapshots.put(snapshotName, snap);
        log.info("[snapshot] created name={} objects={}", snapshotName, snap.size());
    }

    /** 原子恢复：先校验全部内容完整，再一次性提交；失败抛 RESTORE_FAILED 且当前数据不变 */
    public synchronized void restoreSnapshot(String snapshotName) {
        Map<String, ObjectMeta> snap = snapshots.get(snapshotName);
        if (snap == null) {
            throw new StorageException(ErrorCode.NOT_FOUND, "snapshot not found: " + snapshotName);
        }
        // 校验阶段：任何 chunk 缺失或损坏都拒绝恢复，不产生任何变更
        for (Map.Entry<String, ObjectMeta> e : snap.entrySet()) {
            if (!chunkStore.verify(e.getValue().contentHash())) {
                log.info("[restore] RESTORE_FAILED snapshot={} key={} hash={} corrupted or missing",
                        snapshotName, e.getKey(), e.getValue().contentHash());
                throw new StorageException(ErrorCode.RESTORE_FAILED,
                        "restore failed: chunk " + e.getValue().contentHash()
                                + " for key " + e.getKey() + " is corrupted or missing; current data untouched");
            }
        }
        // 提交阶段：先为新命名空间持有引用，再释放旧引用，最后整体替换（内存操作，天然原子）
        snap.values().forEach(m -> chunkStore.retain(m.contentHash()));
        current.values().forEach(m -> chunkStore.release(m.contentHash()));
        current.clear();
        current.putAll(snap);
        log.info("[restore] snapshot={} restored, objects={}", snapshotName, current.size());
    }

    /** 删除快照并释放其持有的引用 */
    public synchronized void deleteSnapshot(String snapshotName) {
        Map<String, ObjectMeta> snap = snapshots.remove(snapshotName);
        if (snap == null) {
            throw new StorageException(ErrorCode.NOT_FOUND, "snapshot not found: " + snapshotName);
        }
        snap.values().forEach(m -> chunkStore.release(m.contentHash()));
        log.info("[snapshot] deleted name={}", snapshotName);
    }

    /** 读取快照中的对象 */
    public synchronized byte[] getSnapshotObject(String snapshotName, String key) {
        Map<String, ObjectMeta> snap = snapshots.get(snapshotName);
        if (snap == null) {
            throw new StorageException(ErrorCode.NOT_FOUND, "snapshot not found: " + snapshotName);
        }
        ObjectMeta meta = snap.get(key);
        if (meta == null) {
            throw new StorageException(ErrorCode.NOT_FOUND,
                    "object not found in snapshot " + snapshotName + ": " + key);
        }
        return chunkStore.get(meta.contentHash());
    }

    /** 当前命名空间视图（测试用） */
    public synchronized Map<String, ObjectMeta> listObjects() {
        return Collections.unmodifiableMap(new HashMap<>(current));
    }

    /** 快照列表（测试用） */
    public synchronized List<String> listSnapshots() {
        return Collections.unmodifiableList(new ArrayList<>(snapshots.keySet()));
    }
}
