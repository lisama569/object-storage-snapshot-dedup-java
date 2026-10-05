package com.github.lisama569.objectstoragesnapshotdedupjava.tests;

import com.github.lisama569.objectstoragesnapshotdedupjava.model.SnapshotInfo;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.SpaceStats;
import com.github.lisama569.objectstoragesnapshotdedupjava.service.ObjectStorageService;
import com.github.lisama569.objectstoragesnapshotdedupjava.service.SnapshotService;
import com.github.lisama569.objectstoragesnapshotdedupjava.store.ContentStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 容量与回收统计口径：已用容量、去重节省、快照锁定/可回收，以及并发交错后统计与磁盘对账。
 */
class SnapshotSpaceStatsTest {

    private static final Logger log = LoggerFactory.getLogger(SnapshotSpaceStatsTest.class);

    @TempDir
    Path tempDir;

    ContentStore contentStore;
    ObjectStorageService storage;
    SnapshotService snapshots;

    @BeforeEach
    void setUp() {
        contentStore = new ContentStore(tempDir.toString(), 50_000_000);
        storage = new ObjectStorageService(contentStore);
        snapshots = new SnapshotService(storage, contentStore);
    }

    private long diskBlobBytes() throws Exception {
        long total = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(tempDir, "*.blob")) {
            for (Path p : stream) {
                total += Files.size(p);
            }
        }
        return total;
    }

    @Test
    void statsReconcileWithDiskAndPredictedReclaimMatchesActual() throws Exception {
        byte[] shared = "shared-content".getBytes(StandardCharsets.UTF_8);
        byte[] exclusive = "exclusive-to-snapshot".getBytes(StandardCharsets.UTF_8);
        storage.put("a", shared);
        storage.put("b", exclusive);
        snapshots.createSnapshot("snap");
        // 演进：b 覆盖，快照中的旧 b 成为独占
        storage.put("b", "b-new".getBytes(StandardCharsets.UTF_8));
        storage.put("c", "c-content".getBytes(StandardCharsets.UTF_8));

        SpaceStats stats = snapshots.stats();
        log.info("[test] stats before delete: used={} logical={} dedupSaved={} snapLocked={} snapReclaimable={}",
                stats.usedBytes(), stats.logicalBytes(), stats.dedupSavedBytes(),
                stats.snapshotLockedBytes(), stats.snapshotReclaimableBytes());

        assertEquals(diskBlobBytes(), stats.usedBytes(), "已用容量必须等于磁盘上 blob 大小之和");
        assertEquals(stats.usedBytes(), contentStore.usedBytes());
        assertEquals(shared.length + exclusive.length, stats.snapshots().get(0).lockedBytes(),
                "快照锁定口径=快照条目大小之和");
        assertEquals(exclusive.length, stats.snapshots().get(0).reclaimableBytes(),
                "预计可回收=仅该快照持有的 blob 大小");
        assertEquals(exclusive.length, stats.snapshotReclaimableBytes());
        assertTrue(stats.dedupSavedBytes() >= 0, "去重节省不得为负");
        assertEquals(stats.logicalBytes() - stats.usedBytes(), stats.dedupSavedBytes(),
                "去重节省 = 逻辑字节 - 物理字节");

        // 预计回收 == 实际容量变化
        long predicted = stats.snapshots().get(0).reclaimableBytes();
        var result = snapshots.deleteSnapshot("snap");
        assertEquals(predicted, result.reclaimedBytes(), "预计回收必须等于实际回收");
        assertEquals(diskBlobBytes(), contentStore.usedBytes());

        SpaceStats after = snapshots.stats();
        assertEquals(0, after.snapshots().size());
        assertEquals(0, after.snapshotLockedBytes());
        assertEquals(0, after.snapshotReclaimableBytes());
        assertArrayEquals(shared, storage.get("a"), "回收后当前对象仍可读（不能出现统计说回收但读不出）");
        log.info("[test] 判定通过: 统计==磁盘, 预计回收={}==实际回收={}, 对象可读", predicted, result.reclaimedBytes());
    }

    @Test
    void dedupSavingsAreReportedWhenIdenticalContentIsWrittenManyTimes() throws Exception {
        byte[] payload = new byte[100];
        for (int i = 0; i < 10; i++) {
            storage.put("obj-" + i, payload);
        }
        SpaceStats stats = snapshots.stats();
        assertEquals(100, stats.usedBytes(), "10 份相同内容只落一份物理 blob");
        assertEquals(1000, stats.logicalBytes(), "逻辑口径按 10 个条目计");
        assertEquals(900, stats.dedupSavedBytes(), "去重节省=1000-100=900");
        assertEquals(100, diskBlobBytes());
        log.info("[test] 判定通过: 10 个相同条目 used=100 logical=1000 dedupSaved=900");
    }

    @Test
    void sharedBlobsAcrossSnapshotsAreReclaimedOnlyAfterLastHolderDeleted() {
        byte[] data = "held-by-two-snapshots".getBytes(StandardCharsets.UTF_8);
        storage.put("a", data);
        snapshots.createSnapshot("old");
        snapshots.createSnapshot("new");
        // 当前对象删除后，数据仍被两个快照持有
        storage.delete("a");
        String hash = ContentStore.sha256Hex(data);

        SpaceStats both = snapshots.stats();
        assertEquals(0, both.snapshots().get(0).reclaimableBytes(),
                "old 仍与 new 共享，删 old 预计回收 0");
        assertEquals(0, both.snapshots().get(1).reclaimableBytes(),
                "两个快照共同持有时，单独删 new 也不回收（old 仍持有）");

        assertEquals(0, snapshots.deleteSnapshot("old").reclaimedBytes(), "删除共享方不得回收");
        assertTrue(contentStore.exists(hash), "仍有快照引用，blob 必须保留");
        assertEquals(data.length, snapshots.listSnapshotDetails().get(0).reclaimableBytes(),
                "old 删除后 new 成为唯一持有者，预计回收全部");
        assertEquals(data.length, snapshots.deleteSnapshot("new").reclaimedBytes(), "最后一个持有者删除才回收");
        assertFalse(contentStore.exists(hash));
        log.info("[test] 判定通过: 共享 blob 仅在最后一个快照持有者删除后回收");
    }

    @Test
    void snapshotDetailsAreOrderedOldestFirstWithCorrectSizes() {
        storage.put("a", "aaa".getBytes(StandardCharsets.UTF_8));
        snapshots.createSnapshot("s1");
        storage.put("b", "bbbb".getBytes(StandardCharsets.UTF_8));
        snapshots.createSnapshot("s2");

        List<SnapshotInfo> infos = snapshots.listSnapshotDetails();
        assertEquals(List.of("s1", "s2"), infos.stream().map(SnapshotInfo::name).toList(),
                "明细必须按创建时间从旧到新");
        assertTrue(infos.get(0).createdAtMillis() <= infos.get(1).createdAtMillis());
        log.info("[test] snapshot details: {}", infos);
    }

    @Test
    void concurrentWritesSnapshotCreateDeleteStatsAlwaysReconcileAtQuiescence() throws Exception {
        // 业务：写入/覆盖/删除对象、创建/删除快照、读取统计 交错进行；
        // 收敛后报出来的已用/可回收必须与磁盘对账，且所有当前对象都能完整读出。
        int writerThreads = 4;
        int durationMillis = 1500;
        ExecutorService pool = Executors.newFixedThreadPool(writerThreads + 2);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicBoolean failed = new AtomicBoolean(false);
        AtomicInteger created = new AtomicInteger();

        // 写线程：对各自名字段做 put/覆盖/删除
        for (int t = 0; t < writerThreads; t++) {
            int tid = t;
            pool.submit(() -> {
                int i = 0;
                while (running.get()) {
                    try {
                        String name = "w" + tid + "-o" + (i % 6);
                        byte[] payload = ("payload-" + tid + "-" + (i % 4)).getBytes(StandardCharsets.UTF_8);
                        storage.put(name, payload);
                        if (i % 5 == 0) {
                            try {
                                storage.delete(name);
                            } catch (com.github.lisama569.objectstoragesnapshotdedupjava.exception.StorageException ignored) {
                            }
                        }
                    } catch (Exception e) {
                        log.error("writer error", e);
                        failed.set(true);
                    }
                    i++;
                }
            });
        }
        // 快照创建/删除线程
        pool.submit(() -> {
            int i = 0;
            List<String> live = new ArrayList<>();
            while (running.get()) {
                try {
                    String name = "snap-" + i++;
                    snapshots.createSnapshot(name);
                    live.add(name);
                    created.incrementAndGet();
                    if (live.size() > 4) {
                        String victim = live.remove(0);
                        try {
                            snapshots.deleteSnapshot(victim);
                        } catch (com.github.lisama569.objectstoragesnapshotdedupjava.exception.StorageException ignored) {
                            live.remove(victim);
                        }
                    }
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    return;
                } catch (Exception e) {
                    log.error("snapshot thread error", e);
                    failed.set(true);
                }
            }
        });
        // 统计巡检线程：运行期间持续校验单次统计自身口径非负（统计本身是一致快照）；
        // “已用/可回收与磁盘精确对账”的强校验放在所有操作停止后的收敛点。
        pool.submit(() -> {
            while (running.get()) {
                try {
                    SpaceStats s = snapshots.stats();
                    if (s.dedupSavedBytes() < 0 || s.snapshotReclaimableBytes() < 0
                            || s.usedBytes() < 0 || s.logicalBytes() < s.usedBytes()) {
                        log.error("inconsistent stats observed: {}", s);
                        failed.set(true);
                    }
                } catch (Exception e) {
                    log.error("stats thread error", e);
                    failed.set(true);
                }
            }
        });

        Thread.sleep(durationMillis);
        running.set(false);
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        assertFalse(failed.get(), "并发交错过程中不得出现异常或负口径");
        log.info("[test] concurrent phase done, snapshots created={}", created.get());

        // 收敛点一：统计的已用容量 == 磁盘 blob 大小之和
        SpaceStats atQuiescence = snapshots.stats();
        assertEquals(diskBlobBytes(), atQuiescence.usedBytes(), "已用容量必须与磁盘实际对上");
        assertEquals(diskBlobBytes(), contentStore.usedBytes());
        assertTrue(atQuiescence.snapshotReclaimableBytes() <= atQuiescence.usedBytes());

        // 收敛点二：所有当前对象必须读得出完整内容（不允许“回收统计对了但对象读不出来”）
        for (var meta : storage.list()) {
            byte[] got = storage.get(meta.name());
            assertEquals(meta.contentHash(), ContentStore.sha256Hex(got),
                    "当前对象内容必须与哈希一致: " + meta.name());
        }

        // 收敛点三：此刻预计删除全部快照可回收的量 == 顺序删完后的实际容量差
        long predictedAll = atQuiescence.snapshotReclaimableBytes();
        long usedBefore = contentStore.usedBytes();
        long actualReclaimed = 0;
        for (String name : snapshots.listSnapshots()) {
            try {
                actualReclaimed += snapshots.deleteSnapshot(name).reclaimedBytes();
            } catch (com.github.lisama569.objectstoragesnapshotdedupjava.exception.StorageException ignored) {
                // 其他线程已停，不会有并发删除；忽略不应发生
            }
        }
        assertEquals(predictedAll, actualReclaimed, "预计可回收必须等于实际回收");
        assertEquals(usedBefore - predictedAll, contentStore.usedBytes());
        assertEquals(diskBlobBytes(), contentStore.usedBytes(), "删完快照后统计仍须与磁盘对上");

        // 收敛点四：删除全部快照后当前对象仍全部可读
        for (var meta : storage.list()) {
            byte[] got = storage.get(meta.name());
            assertEquals(meta.contentHash(), ContentStore.sha256Hex(got));
        }
        log.info("[test] 判定通过: 并发交错后 used={} 对账磁盘, 预测回收={}==实际={}, 对象全部可读",
                atQuiescence.usedBytes(), predictedAll, actualReclaimed);
    }
}
