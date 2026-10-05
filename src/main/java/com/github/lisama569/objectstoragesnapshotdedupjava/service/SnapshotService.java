package com.github.lisama569.objectstoragesnapshotdedupjava.service;

import com.github.lisama569.objectstoragesnapshotdedupjava.exception.ErrorCode;
import com.github.lisama569.objectstoragesnapshotdedupjava.exception.StorageException;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.ObjectMeta;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.RetentionPolicy;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.SnapshotCreateResult;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.SnapshotDeleteResult;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.SnapshotInfo;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.SpaceStats;
import com.github.lisama569.objectstoragesnapshotdedupjava.store.ContentStore;
import com.github.lisama569.objectstoragesnapshotdedupjava.store.PhysicalView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 快照生命周期管理：创建（含保留策略自动淘汰）、读取、原子恢复、删除与空间回收、容量统计。
 *
 * 并发与安全口径：
 *  - 所有生命周期操作（创建/删除/淘汰/恢复/读取/统计）都在 SnapshotService 同一把监视器下串行，
 *    因此删除与恢复、删除与读取不可能互相穿插：快照不会在恢复或读取途中被抽走数据；
 *    重复或并发删除只有一个成功，其余得到 SNAPSHOT_NOT_FOUND。
 *  - 统计时额外持有 ObjectStorageService 监视器，阻塞并发 put/delete，使“持有者计数 + 磁盘物理视图”
 *    对应同一个全局状态，保证报出来的已用/可回收与磁盘实际一致。
 *  - 手动删除与策略淘汰走同一套 {@link #removeSnapshot} 回收口径，最终存储状态一致。
 *  - 删除快照只释放快照自己持有的引用；当前对象与其他快照仍引用的数据不会被提前回收
 *    （ContentStore 引用计数归零才删底层 blob）。
 */
@Service
public class SnapshotService {

    private static final Logger log = LoggerFactory.getLogger(SnapshotService.class);

    private final ObjectStorageService objectStorage;
    private final ContentStore contentStore;
    private final Map<String, SnapshotEntry> snapshots = new LinkedHashMap<>();
    private final java.util.function.LongSupplier clockMillis;

    @org.springframework.beans.factory.annotation.Autowired
    public SnapshotService(ObjectStorageService objectStorage, ContentStore contentStore) {
        this(objectStorage, contentStore, System::currentTimeMillis);
    }

    /** 测试用：可注入时钟（保留时长判定需要可控时间）。 */
    public SnapshotService(ObjectStorageService objectStorage, ContentStore contentStore,
                           java.util.function.LongSupplier clockMillis) {
        this.objectStorage = objectStorage;
        this.contentStore = contentStore;
        this.clockMillis = clockMillis;
    }

    /** 快照内部条目：不可变索引 + 创建时间。 */
    private record SnapshotEntry(Map<String, ObjectMeta> index, long createdAtMillis) {
    }

    public SnapshotCreateResult createSnapshot(String snapshotName) {
        return createSnapshot(snapshotName, RetentionPolicy.unlimited());
    }

    /**
     * 创建快照并应用保留策略：超过 maxCount 时最旧的快照让位，超过 maxAgeMillis 的快照淘汰。
     * 新建的快照保证不会被本次淘汰。
     */
    public synchronized SnapshotCreateResult createSnapshot(String snapshotName, RetentionPolicy policy) {
        if (policy == null) {
            policy = RetentionPolicy.unlimited();
        }
        if (policy.maxCount() != null && policy.maxCount() <= 0) {
            throw new StorageException(ErrorCode.INVALID_RETENTION,
                    "maxCount must be positive: " + policy.maxCount());
        }
        if (policy.maxAgeMillis() != null && policy.maxAgeMillis() <= 0) {
            throw new StorageException(ErrorCode.INVALID_RETENTION,
                    "maxAgeMillis must be positive: " + policy.maxAgeMillis());
        }
        if (snapshots.containsKey(snapshotName)) {
            throw new StorageException(ErrorCode.CONCURRENT_CONFLICT,
                    "snapshot already exists: " + snapshotName);
        }

        long now = clockMillis.getAsLong();
        Map<String, ObjectMeta> index = objectStorage.currentIndex();
        for (ObjectMeta meta : index.values()) {
            contentStore.addRef(meta.contentHash());
        }
        snapshots.put(snapshotName, new SnapshotEntry(index, now));
        log.info("[Snapshot.create] name={} objects={} policy(maxCount={}, maxAgeMillis={})",
                snapshotName, index.size(), policy.maxCount(), policy.maxAgeMillis());

        List<SnapshotDeleteResult> evicted = applyRetention(policy, now);
        return new SnapshotCreateResult(snapshotName, evicted);
    }

    /**
     * 先淘汰超过保留时长的快照（最旧的先），再按份数从最旧开始裁剪，直到不超过 maxCount。
     * 与手动删除共用 {@link #removeSnapshot}，回收口径完全一致。
     */
    private List<SnapshotDeleteResult> applyRetention(RetentionPolicy policy, long now) {
        List<SnapshotDeleteResult> evicted = new ArrayList<>();

        if (policy.maxAgeMillis() != null) {
            Iterator<Map.Entry<String, SnapshotEntry>> it = snapshots.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, SnapshotEntry> e = it.next();
                long age = now - e.getValue().createdAtMillis();
                if (age > policy.maxAgeMillis()) {
                    String name = e.getKey();
                    it.remove();
                    evicted.add(removeSnapshot(name, e.getValue(), true));
                }
            }
        }

        if (policy.maxCount() != null) {
            Iterator<Map.Entry<String, SnapshotEntry>> it = snapshots.entrySet().iterator();
            while (snapshots.size() > policy.maxCount() && it.hasNext()) {
                Map.Entry<String, SnapshotEntry> e = it.next();
                String name = e.getKey();
                it.remove();
                evicted.add(removeSnapshot(name, e.getValue(), true));
            }
        }
        return evicted;
    }

    /**
     * 删除快照：只有这一次删除成功（map 中先摘除），随后释放该快照持有的全部引用；
     * 仅当某份数据不再被当前对象和任何剩余快照引用时，底层 blob 才被真正回收。
     */
    public synchronized SnapshotDeleteResult deleteSnapshot(String snapshotName) {
        SnapshotEntry entry = snapshots.get(snapshotName);
        if (entry == null) {
            log.info("[Snapshot.delete] name={} -> SNAPSHOT_NOT_FOUND", snapshotName);
            throw new StorageException(ErrorCode.SNAPSHOT_NOT_FOUND, "snapshot not found: " + snapshotName);
        }
        // 先从注册表摘除：并发/重复删除的其余调用必然拿到 SNAPSHOT_NOT_FOUND。
        snapshots.remove(snapshotName);
        SnapshotDeleteResult result = removeSnapshot(snapshotName, entry, false);
        log.info("[Snapshot.delete] name={} deleted, reclaimedBytes={}", snapshotName, result.reclaimedBytes());
        return result;
    }

    /**
     * 统一回收口径：释放快照中每个对象条目对内容的一次引用（与创建时按条目 addRef 对应）。
     * ContentStore 仅在最后一个引用释放时删除 blob 并返回其大小，因此 reclaimedBytes
     * 恰好等于本次真正从磁盘回收的字节；仍被当前对象或其他快照引用的数据不动。
     */
    private SnapshotDeleteResult removeSnapshot(String name, SnapshotEntry entry, boolean evicted) {
        long reclaimed = 0;
        for (ObjectMeta meta : entry.index().values()) {
            reclaimed += contentStore.release(meta.contentHash());
        }
        log.info("[Snapshot.{}] name={} released {} refs, reclaimedBytes={}",
                evicted ? "evict" : "delete", name, entry.index().size(), reclaimed);
        return new SnapshotDeleteResult(name, evicted, reclaimed);
    }

    public synchronized byte[] readFromSnapshot(String snapshotName, String objectName) {
        Map<String, ObjectMeta> index = snapshot(snapshotName);
        ObjectMeta meta = index.get(objectName);
        if (meta == null) {
            throw new StorageException(ErrorCode.OBJECT_NOT_FOUND,
                    "object not found in snapshot " + snapshotName + ": " + objectName);
        }
        // 读取在快照监视器内完成，删除/淘汰必须等待读完，不会中途抽走底层数据。
        return contentStore.read(meta.contentHash());
    }

    /**
     * 原子恢复：失败时不留下部分恢复的结果。恢复在快照监视器内进行，
     * 因此被恢复的快照不可能在恢复途中被删除或淘汰。
     */
    public synchronized void restore(String snapshotName) {
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

    public synchronized List<String> listSnapshots() {
        return new ArrayList<>(snapshots.keySet());
    }

    public synchronized List<SnapshotInfo> listSnapshotDetails() {
        return gather().infos();
    }

    /**
     * 整体容量与回收统计。持有快照监视器 + 对象存储监视器获取一致状态，
     * 物理已用容量直接来自磁盘扫描，可回收按“引用是否仅归该快照所有”计算。
     */
    public synchronized SpaceStats stats() {
        HolderView view = gather();

        long snapshotLocked = 0;
        long onlySnapshotBytes = 0;
        for (Map.Entry<String, Integer> e : view.snapshotCount().entrySet()) {
            String hash = e.getKey();
            if (view.currentCount().getOrDefault(hash, 0) == 0) {
                onlySnapshotBytes += view.sizes().getOrDefault(hash, 0L);
            }
        }
        for (SnapshotInfo info : view.infos()) {
            snapshotLocked += info.lockedBytes();
        }

        long logical = view.currentLogicalBytes() + snapshotLocked;
        long used = view.physical().usedBytes();
        return new SpaceStats(used, logical, Math.max(0, logical - used),
                snapshotLocked, onlySnapshotBytes, view.infos());
    }

    /**
     * 在“快照监视器 + 对象存储监视器”下采集一致视图：
     * 阻塞快照生命周期操作与并发 put/delete/applyIndex，使持有者计数与磁盘物理视图对应同一时刻。
     */
    private HolderView gather() {
        synchronized (objectStorage) {
            PhysicalView physical = contentStore.physicalView();
            Map<String, ObjectMeta> current = objectStorage.currentIndex();

            Map<String, Integer> currentCount = new HashMap<>();
            long currentLogical = 0;
            for (ObjectMeta meta : current.values()) {
                currentCount.merge(meta.contentHash(), 1, Integer::sum);
                currentLogical += meta.size();
            }

            Map<String, Integer> snapshotCount = new HashMap<>();
            for (SnapshotEntry entry : snapshots.values()) {
                for (ObjectMeta meta : entry.index().values()) {
                    snapshotCount.merge(meta.contentHash(), 1, Integer::sum);
                }
            }

            List<SnapshotInfo> infos = new ArrayList<>();
            for (Map.Entry<String, SnapshotEntry> e : snapshots.entrySet()) {
                Map<String, ObjectMeta> index = e.getValue().index();
                Map<String, Integer> inThis = new HashMap<>();
                long locked = 0;
                for (ObjectMeta meta : index.values()) {
                    inThis.merge(meta.contentHash(), 1, Integer::sum);
                    locked += meta.size();
                }
                long reclaimable = 0;
                for (Map.Entry<String, Integer> hc : inThis.entrySet()) {
                    String hash = hc.getKey();
                    boolean onlyHolder = currentCount.getOrDefault(hash, 0) == 0
                            && snapshotCount.getOrDefault(hash, 0).equals(hc.getValue());
                    if (onlyHolder) {
                        reclaimable += physical.blobSizes().getOrDefault(hash, 0L);
                    }
                }
                infos.add(new SnapshotInfo(e.getKey(), e.getValue().createdAtMillis(),
                        index.size(), locked, reclaimable));
            }
            return new HolderView(physical, currentCount, snapshotCount, currentLogical, infos);
        }
    }

    private record HolderView(PhysicalView physical, Map<String, Integer> currentCount,
                              Map<String, Integer> snapshotCount, long currentLogicalBytes,
                              List<SnapshotInfo> infos) {
        Map<String, Long> sizes() {
            return physical.blobSizes();
        }
    }

    private Map<String, ObjectMeta> snapshot(String snapshotName) {
        SnapshotEntry entry = snapshots.get(snapshotName);
        if (entry == null) {
            throw new StorageException(ErrorCode.SNAPSHOT_NOT_FOUND, "snapshot not found: " + snapshotName);
        }
        return entry.index();
    }
}
