package com.github.lisama569.objectstoragesnapshotdedupjava.tests;

import com.github.lisama569.objectstoragesnapshotdedupjava.exception.ErrorCode;
import com.github.lisama569.objectstoragesnapshotdedupjava.exception.StorageException;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.SnapshotMeta;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.StorageStats;
import com.github.lisama569.objectstoragesnapshotdedupjava.service.ObjectStorageService;
import com.github.lisama569.objectstoragesnapshotdedupjava.service.SnapshotService;
import com.github.lisama569.objectstoragesnapshotdedupjava.store.ContentStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 快照生命周期：删除、重复/并发删除、引用回收、保留策略淘汰、容量与回收统计口径。
 */
class SnapshotLifecycleTest {

    private static final Logger log = LoggerFactory.getLogger(SnapshotLifecycleTest.class);

    @TempDir
    Path tempDir;

    ContentStore contentStore;
    ObjectStorageService storage;
    SnapshotService snapshots;

    @BeforeEach
    void setUp() {
        contentStore = new ContentStore(tempDir.toString(), 1_000_000);
        storage = new ObjectStorageService(contentStore);
        snapshots = new SnapshotService(storage, contentStore);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void deleteSnapshotKeepsCurrentObjectsAndReclaimsOnlyUnreferencedData() {
        storage.put("shared", bytes("shared-content"));          // 10 bytes? -> "shared-content" = 14
        storage.put("transient", bytes("transient-content"));
        snapshots.createSnapshot("snap");
        long usedWithSnapshot = contentStore.usedBytes();
        log.info("[test] input: 2 objects + snapshot snap, usedBytes={}", usedWithSnapshot);

        // 当前对象继续演进：覆盖 shared、删除 transient 之外的读写不受影响
        storage.put("shared", bytes("shared-v2-content"));

        snapshots.deleteSnapshot("snap");
        log.info("[test] deleted snap, usedBytes={} diskUsed={}", contentStore.usedBytes(), contentStore.diskUsedBytes());

        // 判定依据：当前对象照常可读可写
        assertArrayEquals(bytes("shared-v2-content"), storage.get("shared"));
        assertArrayEquals(bytes("transient-content"), storage.get("transient"));
        storage.put("after", bytes("after-delete"));
        assertArrayEquals(bytes("after-delete"), storage.get("after"));

        // 判定依据：只有 snap 独占的 shared-content 被回收（当前仍引用的不回收）
        long expected = (long) "shared-v2-content".length() + "transient-content".length()
                + "after-delete".length();
        assertEquals(expected, contentStore.usedBytes(), "仅快照独占数据被回收，容量须体现");
        assertEquals(expected, contentStore.diskUsedBytes(), "内存统计须与磁盘实际占用一致");
        assertFalse(contentStore.exists(ContentStore.sha256Hex(bytes("shared-content"))),
                "不再被引用的旧内容须真正回收");
        log.info("[test] current objects intact, exclusive data reclaimed (判定依据: usedBytes == 当前对象引用的数据量 == 磁盘占用)");
    }

    @Test
    void reclaimHappensOnlyWhenLastReferenceDrops() {
        storage.put("obj", bytes("only-copy"));
        snapshots.createSnapshot("s1");
        snapshots.createSnapshot("s2");
        String hash = storage.meta("obj").contentHash();
        assertEquals(3, contentStore.refCount(hash), "当前对象 + 2 份快照 = 3 个引用");

        storage.delete("obj");
        assertTrue(contentStore.exists(hash), "快照仍引用，数据不得回收");
        snapshots.deleteSnapshot("s1");
        assertTrue(contentStore.exists(hash), "s2 仍引用，数据不得回收");
        assertEquals(1, contentStore.refCount(hash));

        snapshots.deleteSnapshot("s2");
        assertFalse(contentStore.exists(hash), "最后一个引用释放后才真正回收");
        assertEquals(0, contentStore.usedBytes());
        assertEquals(0, contentStore.diskUsedBytes());
        log.info("[test] refcount 3->2->1->0, blob reclaimed only at 0 (判定依据: 引用计数归零才回收)");
    }

    @Test
    void deletingMissingSnapshotIsDistinguishable() {
        StorageException e = assertThrows(StorageException.class, () -> snapshots.deleteSnapshot("nope"));
        assertEquals(ErrorCode.SNAPSHOT_NOT_FOUND, e.getErrorCode(), "删除不存在的快照须可区分");
        log.info("[test] errorCode={} message={} (判定依据: 快照不存在)", e.getErrorCode(), e.getMessage());
    }

    @Test
    void concurrentDeletesSucceedExactlyOnceAndKeepRefcountsConsistent() throws Exception {
        storage.put("keep", bytes("keep-content"));
        snapshots.createSnapshot("hot");
        String hash = storage.meta("keep").contentHash();
        int threads = 8;
        log.info("[test] input: {} threads concurrently delete snapshot 'hot'", threads);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger successes = new AtomicInteger();
        Map<ErrorCode, AtomicInteger> failures = new ConcurrentHashMap<>();
        List<Future<?>> futures = new java.util.ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    snapshots.deleteSnapshot("hot");
                    successes.incrementAndGet();
                } catch (StorageException e) {
                    failures.computeIfAbsent(e.getErrorCode(), k -> new AtomicInteger()).incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        pool.shutdown();

        assertEquals(1, successes.get(), "并发删除只能成功一次");
        assertEquals(threads - 1, failures.getOrDefault(ErrorCode.SNAPSHOT_NOT_FOUND, new AtomicInteger()).get(),
                "其余删除须得到 SNAPSHOT_NOT_FOUND");
        assertEquals(1, contentStore.refCount(hash), "引用计数不得错乱：只剩当前对象的引用");
        assertArrayEquals(bytes("keep-content"), storage.get("keep"), "仍被引用的数据不得被提前删除");
        assertEquals(contentStore.usedBytes(), contentStore.diskUsedBytes(), "统计与磁盘须一致");
        log.info("[test] successes=1 failures(SNAPSHOT_NOT_FOUND)={} refCount={} (判定依据: 恰好一次成功，计数无错乱)",
                threads - 1, contentStore.refCount(hash));
    }

    @Test
    void countBasedRetentionEvictsOldestAndReclaimsExclusiveSpace() {
        storage.put("doc", bytes("v1-content"));
        snapshots.createSnapshot("s1");
        storage.put("doc", bytes("v2-content"));
        snapshots.createSnapshot("s2");
        log.info("[test] input: s1={v1}, s2={v2}, now create s3 with maxSnapshots=2");

        storage.put("doc", bytes("v3-content"));
        snapshots.createSnapshot("s3", 2, null);

        assertEquals(List.of("s2", "s3"), snapshots.listSnapshots().stream().sorted().toList(),
                "超过上限时最旧的 s1 须被淘汰");
        StorageException e = assertThrows(StorageException.class,
                () -> snapshots.readFromSnapshot("s1", "doc"));
        assertEquals(ErrorCode.SNAPSHOT_EVICTED, e.getErrorCode(), "被淘汰快照的读取须与不存在可区分");
        StorageException e2 = assertThrows(StorageException.class, () -> snapshots.restore("s1"));
        assertEquals(ErrorCode.SNAPSHOT_EVICTED, e2.getErrorCode(), "被淘汰快照的恢复须与不存在可区分");
        StorageException e3 = assertThrows(StorageException.class, () -> snapshots.deleteSnapshot("s1"));
        assertEquals(ErrorCode.SNAPSHOT_EVICTED, e3.getErrorCode(), "重复删除被淘汰快照须报已淘汰");
        log.info("[test] evicted s1: read/restore/delete -> SNAPSHOT_EVICTED (判定依据: 与 SNAPSHOT_NOT_FOUND 区分)");

        // 更近的快照与当前对象不受影响
        assertArrayEquals(bytes("v2-content"), snapshots.readFromSnapshot("s2", "doc"));
        assertArrayEquals(bytes("v3-content"), snapshots.readFromSnapshot("s3", "doc"));
        assertArrayEquals(bytes("v3-content"), storage.get("doc"));

        // s1 独占的 v1-content 已回收并体现在容量上
        assertFalse(contentStore.exists(ContentStore.sha256Hex(bytes("v1-content"))),
                "被淘汰快照独占的数据须回收");
        long expected = (long) "v2-content".length() + "v3-content".length();
        assertEquals(expected, contentStore.usedBytes());
        assertEquals(expected, contentStore.diskUsedBytes());
        log.info("[test] s2/s3/current intact, v1 reclaimed, usedBytes={} (判定依据: 淘汰只回收独占数据)", expected);
    }

    @Test
    void ageBasedRetentionEvictsOnlyExpiredSnapshots() throws InterruptedException {
        storage.put("doc", bytes("old-content"));
        snapshots.createSnapshot("old");
        Thread.sleep(50); // 让 old 的创建时间明确落在保留窗口之外
        storage.put("doc", bytes("new-content"));
        log.info("[test] input: snapshot 'old' created 50ms ago, create 'fresh' with maxAgeMillis=10");

        snapshots.createSnapshot("fresh", null, 10L);

        StorageException e = assertThrows(StorageException.class,
                () -> snapshots.readFromSnapshot("old", "doc"));
        assertEquals(ErrorCode.SNAPSHOT_EVICTED, e.getErrorCode(), "超出保留时长的旧快照须被淘汰");
        assertArrayEquals(bytes("new-content"), snapshots.readFromSnapshot("fresh", "doc"),
                "刚创建的快照永不在本次创建中被淘汰");
        assertArrayEquals(bytes("new-content"), storage.get("doc"), "当前对象不得被误伤");
        assertFalse(contentStore.exists(ContentStore.sha256Hex(bytes("old-content"))),
                "过期快照独占的数据须回收");
        log.info("[test] age-based eviction ok (判定依据: createdAt < now-maxAge 才淘汰，新建豁免)");
    }

    @Test
    void evictionAndManualDeleteUseSameReclaimAccounting() {
        // 场景 A：手动删除；场景 B：保留策略淘汰。相同初始状态，最终存储状态须一致。
        ContentStore storeB = new ContentStore(tempDir.resolve("b").toString(), 1_000_000);
        ObjectStorageService storageB = new ObjectStorageService(storeB);
        SnapshotService snapshotsB = new SnapshotService(storageB, storeB);

        storage.put("doc", bytes("payload-v1"));
        snapshots.createSnapshot("victim");
        storage.put("doc", bytes("payload-v2"));
        snapshots.createSnapshot("keeper");

        storageB.put("doc", bytes("payload-v1"));
        snapshotsB.createSnapshot("victim");
        storageB.put("doc", bytes("payload-v2"));
        snapshotsB.createSnapshot("keeper");
        log.info("[test] input: two identical stores; A deletes 'victim' manually, B evicts it via retention");

        snapshots.deleteSnapshot("victim");                                  // A：手动删除
        snapshotsB.createSnapshot("trigger", 2, null);                       // B：淘汰 victim（最旧）

        assertEquals(contentStore.usedBytes(), storeB.usedBytes(),
                "手动删除与淘汰须走同一回收口径，最终占用一致");
        assertEquals(contentStore.diskUsedBytes(), storeB.diskUsedBytes());
        assertEquals(contentStore.usedBytes(), contentStore.diskUsedBytes());
        assertEquals(storeB.usedBytes(), storeB.diskUsedBytes());
        assertArrayEquals(storage.get("doc"), storageB.get("doc"));
        assertArrayEquals(snapshots.readFromSnapshot("keeper", "doc"),
                snapshotsB.readFromSnapshot("keeper", "doc"));
        log.info("[test] A.used={} == B.used={} (判定依据: 两种回收路径最终状态一致)",
                contentStore.usedBytes(), storeB.usedBytes());
    }

    @Test
    void snapshotAndStorageStatisticsMatchReality() {
        storage.put("a", bytes("aaaa-content"));
        storage.put("b", bytes("aaaa-content")); // 去重命中
        snapshots.createSnapshot("s1");
        storage.put("a", bytes("a-v2-content!"));
        snapshots.createSnapshot("s2");
        storage.put("a", bytes("a-v3-content!")); // s2 的 v2 从此仅被 s2 引用
        log.info("[test] input: a/b share content, s1 before overwrite, s2 after, then a overwritten to v3");

        List<SnapshotMeta> metas = snapshots.snapshotStats();
        SnapshotMeta s1 = metas.stream().filter(m -> m.name().equals("s1")).findFirst().orElseThrow();
        SnapshotMeta s2 = metas.stream().filter(m -> m.name().equals("s2")).findFirst().orElseThrow();
        log.info("[test] s1 referenced={} exclusive={}; s2 referenced={} exclusive={}",
                s1.referencedBytes(), s1.exclusiveBytes(), s2.referencedBytes(), s2.exclusiveBytes());

        assertEquals("aaaa-content".length(), s1.referencedBytes(), "s1 引用一份去重后的数据");
        assertEquals(0, s1.exclusiveBytes(), "s1 的数据仍被当前对象 b 引用，删除 s1 回收 0");
        assertEquals((long) "aaaa-content".length() + "a-v2-content!".length(), s2.referencedBytes(),
                "s2 引用两份不同内容");
        assertEquals("a-v2-content!".length(), s2.exclusiveBytes(), "s2 独占 a 的新版本");

        StorageStats stats = snapshots.storageStats();
        long expectedUsed = (long) "aaaa-content".length() + "a-v2-content!".length()
                + "a-v3-content!".length();
        assertEquals(expectedUsed, stats.usedBytes());
        assertEquals("aaaa-content".length(), stats.dedupSavedBytes(), "b 的重复写入须计入去重节省");
        assertEquals(2, stats.snapshotCount());
        assertEquals(stats.usedBytes(), contentStore.diskUsedBytes(), "已用容量须与磁盘实际占用一致");
        log.info("[test] stats: used={} dedupSaved={} snapshots={} (判定依据: 与磁盘占用、引用关系逐项核对)",
                stats.usedBytes(), stats.dedupSavedBytes(), stats.snapshotCount());

        // 删除 s2 预计回收 exclusiveBytes，验证预计值 == 实际回收
        long before = contentStore.usedBytes();
        snapshots.deleteSnapshot("s2");
        assertEquals(s2.exclusiveBytes(), before - contentStore.usedBytes(),
                "删除快照实际回收量须等于统计的 exclusiveBytes");
        log.info("[test] reclaimed {} == predicted exclusiveBytes (判定依据: 预计回收 == 实际回收)",
                before - contentStore.usedBytes());
    }

    @Test
    void statsStayConsistentAfterInterleavedConcurrentChurn() throws Exception {
        int threads = 6;
        int ops = 40;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new java.util.ArrayList<>();
        log.info("[test] input: {} threads x {} interleaved put/delete/snapshot ops", threads, ops);
        for (int t = 0; t < threads; t++) {
            final int id = t;
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    java.util.Random rnd = new java.util.Random(id);
                    for (int i = 0; i < ops; i++) {
                        String obj = "obj-" + (i % 5);
                        String snap = "snap-" + id + "-" + (i % 3);
                        switch (rnd.nextInt(4)) {
                            case 0 -> {
                                try {
                                    storage.put(obj, bytes("content-" + id + "-" + i));
                                } catch (StorageException ignored) {
                                }
                            }
                            case 1 -> {
                                try {
                                    storage.delete(obj);
                                } catch (StorageException ignored) {
                                }
                            }
                            case 2 -> {
                                try {
                                    snapshots.createSnapshot(snap);
                                } catch (StorageException ignored) {
                                }
                            }
                            default -> {
                                try {
                                    snapshots.deleteSnapshot(snap);
                                } catch (StorageException ignored) {
                                }
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        // 判定依据：并发交错静止后，统计口径与磁盘、索引三者一致
        assertEquals(contentStore.usedBytes(), contentStore.diskUsedBytes(),
                "已用容量须与实际落盘一致");
        for (var meta : storage.list()) {
            assertDoesNotThrow(() -> storage.get(meta.name()),
                    "统计说在用的对象必须读得出来: " + meta.name());
        }
        assertEquals(snapshots.listSnapshots().size(), snapshots.storageStats().snapshotCount(),
                "快照列表与统计中的快照数须一致");
        long sumExclusive = snapshots.snapshotStats().stream().mapToLong(SnapshotMeta::exclusiveBytes).sum();
        assertTrue(sumExclusive <= contentStore.usedBytes(), "各快照独占空间之和不得超过总占用");
        log.info("[test] after churn: used={} disk={} snapshots={} objects={} (判定依据: 统计==磁盘，对象全部可读)",
                contentStore.usedBytes(), contentStore.diskUsedBytes(),
                snapshots.listSnapshots().size(), storage.list().size());
    }

    @Test
    void deletingSnapshotWhileRestoreInFlightIsRejectedSafely() throws Exception {
        storage.put("doc", bytes("restore-payload"));
        snapshots.createSnapshot("busy");
        String hash = storage.meta("doc").contentHash();
        storage.put("doc", bytes("other-payload"));

        CountDownLatch readStarted = new CountDownLatch(1);
        CountDownLatch allowRead = new CountDownLatch(1);
        ContentStore blockingStore = new ContentStore(tempDir.resolve("blocking").toString(), 1_000_000) {
            @Override
            public byte[] read(String contentHash) {
                if (contentHash.equals(hash)) {
                    readStarted.countDown();
                    try {
                        allowRead.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return super.read(contentHash);
            }
        };
        ObjectStorageService storage2 = new ObjectStorageService(blockingStore);
        SnapshotService snapshots2 = new SnapshotService(storage2, blockingStore);
        storage2.put("doc", bytes("restore-payload"));
        snapshots2.createSnapshot("busy");
        storage2.put("doc", bytes("other-payload"));
        log.info("[test] input: restore of 'busy' blocked mid-validation, then concurrent delete");

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> restoreFuture = pool.submit(() -> snapshots2.restore("busy"));
        assertTrue(readStarted.await(10, TimeUnit.SECONDS), "恢复须已进入校验读取阶段");

        StorageException e = assertThrows(StorageException.class, () -> snapshots2.deleteSnapshot("busy"));
        assertEquals(ErrorCode.SNAPSHOT_IN_USE, e.getErrorCode(),
                "删除正在被恢复的快照须得到可区分的 SNAPSHOT_IN_USE");
        log.info("[test] delete during restore -> {} (判定依据: in-use 保护)", e.getErrorCode());

        allowRead.countDown();
        restoreFuture.get(10, TimeUnit.SECONDS);
        pool.shutdown();

        // 恢复完成后数据完好，且此时删除可以成功
        assertArrayEquals(bytes("restore-payload"), storage2.get("doc"), "恢复须完整完成，数据未被抽走");
        snapshots2.deleteSnapshot("busy");
        assertArrayEquals(bytes("restore-payload"), storage2.get("doc"), "删除快照后当前对象不受影响");
        log.info("[test] restore completed intact; delete succeeds after restore finished");
    }
}
