package com.github.lisama569.objectstoragesnapshotdedupjava.service;

import com.github.lisama569.objectstoragesnapshotdedupjava.exception.ErrorCode;
import com.github.lisama569.objectstoragesnapshotdedupjava.exception.StorageException;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.ObjectMeta;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.SnapshotMeta;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.StorageStats;
import com.github.lisama569.objectstoragesnapshotdedupjava.store.ContentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 快照创建、读取、原子恢复与生命周期管理（删除、保留策略淘汰、空间回收、统计）。
 *
 * 语义：
 *  - 创建快照时复制当前索引并对每个内容哈希 addRef，之后当前对象的覆盖/新增/删除
 *    都不会改变快照可读内容（内容不会被回收）；
 *  - 恢复采用“先校验、后整体替换”：先对快照引用的全部内容做完整性校验，
 *    任一失败则抛 RESTORE_FAILED 且不改动当前状态（无部分恢复）；
 *    校验通过后一次性替换索引，替换过程只涉及引用计数转移，不会失败；
 *  - 删除快照（手动 deleteSnapshot 或保留策略淘汰）走同一套回收口径：
 *    从快照表移除后逐个 release 引用的内容哈希，仅当某份数据不再被当前对象
 *    和任何剩余快照引用时（引用计数归零）才真正回收并体现在容量上；
 *  - 正在被读取或恢复的快照标记为 in-use：手动删除抛 SNAPSHOT_IN_USE，
 *    保留策略淘汰自动跳过它，绝不中途抽走数据；
 *  - 被淘汰的快照留下墓碑：后续读取/恢复/删除抛 SNAPSHOT_EVICTED，
 *    与“从未存在或已手动删除”的 SNAPSHOT_NOT_FOUND 可区分。
 */
@Service
public class SnapshotService {

    private static final Logger log = LoggerFactory.getLogger(SnapshotService.class);

    /** 内部快照条目：seq 单调递增用于“最旧”排序，避免依赖时钟相等性。 */
    private record SnapshotEntry(long seq, long createdAtMillis, Map<String, ObjectMeta> index) {
    }

    private final ObjectStorageService objectStorage;
    private final ContentStore contentStore;
    private final Map<String, SnapshotEntry> snapshots = new ConcurrentHashMap<>();
    private final Map<String, Integer> inUseCounts = new HashMap<>();
    private final Set<String> evictedNames = new HashSet<>();
    private long nextSeq;

    public SnapshotService(ObjectStorageService objectStorage, ContentStore contentStore) {
        this.objectStorage = objectStorage;
        this.contentStore = contentStore;
    }

    public String createSnapshot(String snapshotName) {
        return createSnapshot(snapshotName, null, null);
    }

    /**
     * 创建快照并按保留策略淘汰旧快照。
     *
     * @param maxSnapshots  最多保留几份（null 表示不限）；超过时最旧的让位，
     *                      刚创建的快照与 in-use 快照不在淘汰候选内
     * @param maxAgeMillis  保留多长时间（null 表示不限）；创建时间早于此窗口的旧快照被淘汰
     */
    public synchronized String createSnapshot(String snapshotName, Integer maxSnapshots, Long maxAgeMillis) {
        if (snapshots.containsKey(snapshotName)) {
            throw new StorageException(ErrorCode.CONCURRENT_CONFLICT,
                    "snapshot already exists: " + snapshotName);
        }
        Map<String, ObjectMeta> index = objectStorage.currentIndex();
        for (ObjectMeta meta : index.values()) {
            contentStore.addRef(meta.contentHash());
        }
        long now = System.currentTimeMillis();
        snapshots.put(snapshotName, new SnapshotEntry(nextSeq++, now, index));
        evictedNames.remove(snapshotName); // 同名重建后不再是“被淘汰”
        log.info("[Snapshot.create] name={} objects={} maxSnapshots={} maxAgeMillis={}",
                snapshotName, index.size(), maxSnapshots, maxAgeMillis);
        enforceRetention(snapshotName, maxSnapshots, maxAgeMillis, now);
        return snapshotName;
    }

    /**
     * 保留策略：先按保留时长、再按保留份数淘汰最旧快照。
     * 刚创建的快照与 in-use 快照绝不淘汰；淘汰与手动删除共用 reclaimLocked 回收口径。
     */
    private void enforceRetention(String justCreated, Integer maxSnapshots, Long maxAgeMillis, long now) {
        if (maxAgeMillis != null) {
            long cutoff = now - maxAgeMillis;
            for (Map.Entry<String, SnapshotEntry> e : entriesOldestFirst()) {
                String name = e.getKey();
                if (name.equals(justCreated)) {
                    continue;
                }
                if (e.getValue().createdAtMillis() < cutoff && !isInUse(name)) {
                    log.info("[Snapshot.evict] name={} reason=age createdAt={} cutoff={}", name, e.getValue().createdAtMillis(), cutoff);
                    reclaimLocked(name, true);
                }
            }
        }
        if (maxSnapshots != null) {
            while (snapshots.size() > maxSnapshots) {
                String victim = entriesOldestFirst().stream()
                        .map(Map.Entry::getKey)
                        .filter(n -> !n.equals(justCreated) && !isInUse(n))
                        .findFirst()
                        .orElse(null);
                if (victim == null) {
                    log.info("[Snapshot.evict] skipped: no evictable candidate (count={} limit={}, rest are in-use or just-created)",
                            snapshots.size(), maxSnapshots);
                    break;
                }
                log.info("[Snapshot.evict] name={} reason=count count={} limit={}", victim, snapshots.size(), maxSnapshots);
                reclaimLocked(victim, true);
            }
        }
    }

    public byte[] readFromSnapshot(String snapshotName, String objectName) {
        Map<String, ObjectMeta> index = acquire(snapshotName);
        try {
            ObjectMeta meta = index.get(objectName);
            if (meta == null) {
                throw new StorageException(ErrorCode.OBJECT_NOT_FOUND,
                        "object not found in snapshot " + snapshotName + ": " + objectName);
            }
            return contentStore.read(meta.contentHash());
        } finally {
            release(snapshotName);
        }
    }

    /**
     * 原子恢复：失败时不留下部分恢复的结果。
     * 恢复期间快照处于 in-use，删除/淘汰不会抽走其数据。
     */
    public void restore(String snapshotName) {
        Map<String, ObjectMeta> index = acquire(snapshotName);
        try {
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
        } finally {
            release(snapshotName);
        }
    }

    /**
     * 删除快照：仅当底层数据不再被当前对象和任何剩余快照引用时才真正回收。
     * 重复/并发删除只有第一次成功，其余得到 SNAPSHOT_NOT_FOUND；
     * 删除已被淘汰的快照得到 SNAPSHOT_EVICTED；删除 in-use 快照得到 SNAPSHOT_IN_USE。
     */
    public synchronized void deleteSnapshot(String snapshotName) {
        SnapshotEntry entry = snapshots.get(snapshotName);
        if (entry == null) {
            throw notFoundOrEvicted(snapshotName, "delete");
        }
        if (isInUse(snapshotName)) {
            log.info("[Snapshot.delete] name={} in-use -> SNAPSHOT_IN_USE", snapshotName);
            throw new StorageException(ErrorCode.SNAPSHOT_IN_USE,
                    "snapshot is being read or restored, cannot delete now: " + snapshotName);
        }
        reclaimLocked(snapshotName, false);
    }

    /**
     * 统一回收口径：从快照表移除并逐个释放引用（引用计数归零时 ContentStore 才真正回收）。
     *
     * @param evicted true 表示保留策略淘汰（留墓碑），false 表示手动删除
     */
    private void reclaimLocked(String name, boolean evicted) {
        SnapshotEntry entry = snapshots.remove(name);
        if (entry == null) {
            return;
        }
        for (ObjectMeta meta : entry.index().values()) {
            contentStore.release(meta.contentHash());
        }
        if (evicted) {
            evictedNames.add(name);
        }
        log.info("[Snapshot.{}] name={} objects={} released, usedBytes={}",
                evicted ? "evict" : "delete", name, entry.index().size(), contentStore.usedBytes());
    }

    public List<String> listSnapshots() {
        return new ArrayList<>(snapshots.keySet());
    }

    /** 快照列表及每份的空间占用口径（referenced / exclusive）。 */
    public synchronized List<SnapshotMeta> snapshotStats() {
        List<SnapshotMeta> result = new ArrayList<>();
        for (Map.Entry<String, SnapshotEntry> e : snapshots.entrySet()) {
            SnapshotEntry entry = e.getValue();
            long referenced = 0;
            long exclusive = 0;
            Set<String> seen = new HashSet<>();
            for (ObjectMeta meta : entry.index().values()) {
                if (seen.add(meta.contentHash())) {
                    long size = contentStore.sizeOf(meta.contentHash());
                    referenced += size;
                    if (contentStore.refCount(meta.contentHash()) == 1) {
                        exclusive += size; // 仅该快照引用，删除即可回收
                    }
                }
            }
            result.add(new SnapshotMeta(e.getKey(), entry.createdAtMillis(), entry.index().size(),
                    referenced, exclusive));
        }
        result.sort(Comparator.comparing(SnapshotMeta::name));
        return result;
    }

    /** 整体容量统计：已用、上限、去重节省、快照数。 */
    public synchronized StorageStats storageStats() {
        return new StorageStats(contentStore.usedBytes(), contentStore.capacityBytes(),
                contentStore.dedupSavedBytes(), snapshots.size());
    }

    /** 登记 in-use 并返回快照索引；不存在/已淘汰时抛可区分错误。 */
    private synchronized Map<String, ObjectMeta> acquire(String snapshotName) {
        SnapshotEntry entry = snapshots.get(snapshotName);
        if (entry == null) {
            throw notFoundOrEvicted(snapshotName, "access");
        }
        inUseCounts.merge(snapshotName, 1, Integer::sum);
        log.info("[Snapshot.acquire] name={} inUse -> {}", snapshotName, inUseCounts.get(snapshotName));
        return entry.index();
    }

    private synchronized void release(String snapshotName) {
        int remaining = inUseCounts.merge(snapshotName, -1, Integer::sum);
        if (remaining == 0) {
            inUseCounts.remove(snapshotName);
        }
        log.info("[Snapshot.release] name={} inUse -> {}", snapshotName, remaining);
    }

    private boolean isInUse(String name) {
        return inUseCounts.getOrDefault(name, 0) > 0;
    }

    private StorageException notFoundOrEvicted(String snapshotName, String op) {
        if (evictedNames.contains(snapshotName)) {
            log.info("[Snapshot.{}] name={} was evicted -> SNAPSHOT_EVICTED", op, snapshotName);
            return new StorageException(ErrorCode.SNAPSHOT_EVICTED,
                    "snapshot was evicted by retention policy: " + snapshotName);
        }
        log.info("[Snapshot.{}] name={} not found -> SNAPSHOT_NOT_FOUND", op, snapshotName);
        return new StorageException(ErrorCode.SNAPSHOT_NOT_FOUND, "snapshot not found: " + snapshotName);
    }

    private List<Map.Entry<String, SnapshotEntry>> entriesOldestFirst() {
        return snapshots.entrySet().stream()
                .sorted(Comparator.comparingLong(e -> e.getValue().seq()))
                .toList();
    }
}
