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
    void duplicateRecordsInOneSnapshotBecomingSoleHolderPredictReclaimExactly() {
        // 用户稳定复现场景：两个内容完全一样的对象（走内容去重），打一份快照，再把两个当前对象都删掉。
        byte[] payload = bytes("identical-payload");
        storage.put("obj-a", payload);
        storage.put("obj-b", payload); // 去重命中：落盘一份，当前对象持有 2 个引用
        String hash = ContentStore.sha256Hex(payload);
        snapshots.createSnapshot("snap"); // 快照内 2 条记录指向同一哈希，再持有 2 个引用
        assertEquals(4, contentStore.refCount(hash), "2 个当前对象 + 快照内 2 条记录 = 4 个引用");
        assertEquals(payload.length, contentStore.usedBytes(), "去重后落盘只占一份内容大小");

        // 删除两个当前对象：此时这份快照是该内容的唯一持有者（独占 2 条记录，但 blob 只有一份）
        storage.delete("obj-a");
        storage.delete("obj-b");
        assertEquals(2, contentStore.refCount(hash), "两个引用都来自同一份快照");
        assertTrue(contentStore.exists(hash), "快照仍持有，数据绝不能提前回收");

        SnapshotMeta meta = snapshots.snapshotStats().stream()
                .filter(m -> m.name().equals("snap")).findFirst().orElseThrow();
        log.info("[test] input: 2 identical objects + 1 snapshot, both current objects deleted; "
                        + "snap referenced={} exclusive={} usedBytes={}",
                meta.referencedBytes(), meta.exclusiveBytes(), contentStore.usedBytes());
        assertEquals(payload.length, meta.referencedBytes(), "referenced 按去重后的不同内容各计一份");
        assertEquals(payload.length, meta.exclusiveBytes(),
                "快照是唯一持有者（独占 2 条记录但只占一份 blob），删除预计回收一整份内容大小");

        long before = contentStore.usedBytes();
        snapshots.deleteSnapshot("snap");
        long actual = before - contentStore.usedBytes();
        log.info("[test] predicted exclusive={} bytes, actually reclaimed={} bytes, usedBytes now={} (判定依据: 预计 == 实际，逐字节一致)",
                meta.exclusiveBytes(), actual, contentStore.usedBytes());
        assertEquals(meta.exclusiveBytes(), actual, "预计回收量必须与删除后实际释放量逐字节一致");
        assertEquals(0, contentStore.usedBytes(), "唯一持有者删除后内容真正回收");
        assertEquals(0, contentStore.diskUsedBytes(), "磁盘上不得残留“显示没占其实还占着”的 blob");
        assertFalse(contentStore.exists(hash));
    }

    @Test
    void exclusiveAccountingAcrossSharedSnapshotsAndCurrentObjects() {
        // 交叉共享矩阵（去重后每份内容落盘一份 blob）：
        //   X = 当前对象 + s1 + s2 共同引用
        //   Y = 仅 s1 + s2 两个快照引用（当前对象版本已演进）
        //   Z = 仅 s1 持有，且 s1 内有两条同内容记录（z1、z2）
        //   E = 仅 s2 持有
        //   W = 仅当前对象持有，从不出现在任何快照里
        byte[] X = bytes("content-X-shared-by-all");
        byte[] Y = bytes("content-Y-two-snapshots");
        byte[] Z = bytes("content-Z-sole-s1-duplicate");
        byte[] E = bytes("content-E-sole-s2-only");
        byte[] W = bytes("content-W-current-only");

        storage.put("x", X);
        storage.put("y", Y);
        storage.put("z1", Z);
        storage.put("z2", Z); // 与 z1 同内容，去重：s1 内将有 2 条指向 Z 的记录
        snapshots.createSnapshot("s1"); // s1 = {x:X, y:Y, z1:Z, z2:Z}

        // 当前状态演进：删掉 y/z；之后 y 以相同内容写回供 s2 共享
        storage.delete("y");
        storage.delete("z1");
        storage.delete("z2");
        storage.put("y", Y);
        storage.put("e", E);
        snapshots.createSnapshot("s2"); // s2 = {x:X, y:Y, e:E}
        storage.delete("e");            // E 变成仅 s2 持有
        storage.put("y", bytes("content-C-current-y-new")); // 当前 y 与快照版本 Y 分离
        storage.put("w", W);            // W 仅当前持有，不进任何快照
        log.info("[test] matrix: X=current+s1+s2, Y=s1+s2, Z=s1-only(2 dup records), E=s2-only, W=current-only");

        Map<String, SnapshotMeta> stats = new java.util.HashMap<>();
        for (SnapshotMeta m : snapshots.snapshotStats()) {
            stats.put(m.name(), m);
        }
        SnapshotMeta s1 = stats.get("s1");
        SnapshotMeta s2 = stats.get("s2");

        long s1Referenced = (long) X.length + Y.length + Z.length; // Z 两条记录但只占一份 blob
        long s1Exclusive = Z.length; // X 被当前和 s2 引用，Y 被 s2 引用，只有 Z 的全部引用都在 s1
        log.info("[test] s1 referenced={} exclusive={} (判定依据: myRefs(Z)=2 == globalRefs(Z)=2)",
                s1.referencedBytes(), s1.exclusiveBytes());
        assertEquals(s1Referenced, s1.referencedBytes());
        assertEquals(s1Exclusive, s1.exclusiveBytes());

        long s2Referenced = (long) X.length + Y.length + E.length;
        long s2Exclusive = E.length; // X 三方共享，Y 与 s1 共享，只有 E 为 s2 独占
        log.info("[test] s2 referenced={} exclusive={} (判定依据: 仅 E 的全部引用都在 s2)",
                s2.referencedBytes(), s2.exclusiveBytes());
        assertEquals(s2Referenced, s2.referencedBytes());
        assertEquals(s2Exclusive, s2.exclusiveBytes());

        // 删 s1：预计/实际只回收 Z（一份 blob）；X、Y 仍被 s2 或当前对象引用
        long before1 = contentStore.usedBytes();
        snapshots.deleteSnapshot("s1");
        long reclaimed1 = before1 - contentStore.usedBytes();
        log.info("[test] delete s1: predicted={} actual={} (判定依据: 逐字节一致，X/Y 保留)", s1Exclusive, reclaimed1);
        assertEquals(s1Exclusive, reclaimed1);
        assertTrue(contentStore.exists(ContentStore.sha256Hex(Y)), "s2 仍引用 Y，不得回收");
        assertFalse(contentStore.exists(ContentStore.sha256Hex(Z)), "s1 独占的 Z 必须回收");

        // s1 删除后：E 仍为 s2 独占；而 Y 原由 s1+s2 共享，s1 一删，Y 也变成 s2 独占
        SnapshotMeta s2After = snapshots.snapshotStats().stream()
                .filter(m -> m.name().equals("s2")).findFirst().orElseThrow();
        assertEquals((long) Y.length + E.length, s2After.exclusiveBytes(),
                "s1 删除后 Y 也转为 s2 独占，删除 s2 将回收 Y+E");
        assertArrayEquals(X, storage.get("x"));
        assertArrayEquals(W, storage.get("w"));
        assertArrayEquals(bytes("content-C-current-y-new"), storage.get("y"));

        // 删 s2：回收 Y（s1 已删、当前 y 已换版本）和 E 共两份；X 仍被当前对象引用
        long before2 = contentStore.usedBytes();
        snapshots.deleteSnapshot("s2");
        long reclaimed2 = before2 - contentStore.usedBytes();
        long expected2 = s2After.exclusiveBytes(); // 以删前统计的预计独占量为准
        log.info("[test] delete s2: predicted={} actual={} (判定依据: Y+E 归零回收，X 仍被当前引用)",
                expected2, reclaimed2);
        assertEquals(expected2, reclaimed2);
        assertTrue(contentStore.exists(ContentStore.sha256Hex(X)), "当前对象仍引用 X，不得回收");
        assertArrayEquals(X, storage.get("x"), "快照全部删除后当前对象照常可读");
        storage.put("post", bytes("post-all-snapshots"));
        assertArrayEquals(bytes("post-all-snapshots"), storage.get("post"), "快照删完后当前对象照常可写");
        assertEquals(contentStore.usedBytes(), contentStore.diskUsedBytes(), "内存统计与磁盘占用一致");
    }

    @Test
    void retentionEvictionReclaimsExactlyPredictedExclusiveBytes() throws InterruptedException {
        // 保留策略淘汰与手动删除对同一份数据给出同样的最终状态，且淘汰释放量 == 淘汰前预计独占量
        byte[] sole = bytes("evicted-sole-content");
        byte[] shared = bytes("kept-shared-content");
        storage.put("a", sole);
        storage.put("b", shared);
        snapshots.createSnapshot("old");
        storage.delete("a"); // sole 变成仅 old 持有（且只有一条记录）
        Thread.sleep(20);
        storage.put("c", bytes("fresh-only"));
        long predictedExclusive = sole.length;
        SnapshotMeta before = snapshots.snapshotStats().stream()
                .filter(m -> m.name().equals("old")).findFirst().orElseThrow();
        assertEquals(predictedExclusive, before.exclusiveBytes(),
                "淘汰前预计 old 独占 sole，删除/淘汰将回收一整份");
        log.info("[test] input: old sole-holds 'sole'; create fresh with maxAgeMillis=10 -> evict old");

        long usedBefore = contentStore.usedBytes();
        snapshots.createSnapshot("fresh", null, 10L); // old 超龄被淘汰
        long reclaimed = usedBefore - contentStore.usedBytes();
        log.info("[test] eviction reclaimed {} == predicted {} (判定依据: 自动淘汰释放量 == 预计独占量)",
                reclaimed, predictedExclusive);
        assertEquals(predictedExclusive, reclaimed, "自动淘汰的实际回收量必须等于预计独占量");
        assertFalse(contentStore.exists(ContentStore.sha256Hex(sole)), "独占内容随淘汰真正回收");
        assertTrue(contentStore.exists(ContentStore.sha256Hex(shared)), "共享内容保留");
        assertArrayEquals(shared, storage.get("b"), "当前对象不受淘汰影响，照常可读");
        assertEquals(contentStore.usedBytes(), contentStore.diskUsedBytes());
    }

    @Test
    void afterConcurrentChurnEverySnapshotPredictionMatchesActualReclaim() throws Exception {
        // 并发创建/删除快照 + 对象读写交错，静止后：
        //  1) usedBytes == 磁盘实际占用；
        //  2) 对每份存活快照，其统计的 exclusiveBytes 必须等于“此刻真删它”释放的字节数；
        //     为不破坏共享状态，用独立的克隆场景逐个验证（删除会改变后续快照的独占集）。
        int threads = 6;
        int ops = 30;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new java.util.ArrayList<>();
        log.info("[test] input: {} threads x {} interleaved put/delete/snapshot-create/delete, then verify predictions",
                threads, ops);
        for (int t = 0; t < threads; t++) {
            final int id = t;
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    java.util.Random rnd = new java.util.Random(id * 31L + 7);
                    for (int i = 0; i < ops; i++) {
                        String obj = "obj-" + (i % 4);
                        String snap = "snap-" + id + "-" + (i % 2);
                        switch (rnd.nextInt(4)) {
                            case 0 -> {
                                try {
                                    // 刻意制造跨对象同内容（去重）与不同内容
                                    byte[] payload = bytes("shared-body-" + (i % 3));
                                    storage.put(obj, payload);
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

        assertEquals(contentStore.usedBytes(), contentStore.diskUsedBytes(),
                "并发静止后已用容量须与落盘一致");

        // 静止后逐份删除：每删一份前都读取它“此刻”的预计独占量，
        // 再与删除实际释放量逐字节核对（前一份被删后，共享内容可能转为下一份独占，统计必须同步反映）。
        List<String> names = snapshots.listSnapshots();
        log.info("[test] churn settled: {} snapshots alive, verifying each prediction against real delete",
                names.size());
        for (String name : names) {
            SnapshotMeta current = snapshots.snapshotStats().stream()
                    .filter(m -> m.name().equals(name)).findFirst().orElseThrow();
            long predicted = current.exclusiveBytes();
            long usedBefore = contentStore.usedBytes();
            snapshots.deleteSnapshot(name);
            long actual = usedBefore - contentStore.usedBytes();
            log.info("[test] snapshot={} referenced={} predictedExclusive={} actualReclaimed={} (判定依据: 每删一份前现取统计，预计==实际)",
                    name, current.referencedBytes(), predicted, actual);
            assertEquals(predicted, actual,
                    "并发交错后快照 " + name + " 的预计回收量必须等于实际释放量");
        }

        // 全部快照删完后：当前对象必须照常可读，且磁盘无残留
        for (var m : storage.list()) {
            assertDoesNotThrow(() -> storage.get(m.name()), "所有当前对象必须可读: " + m.name());
        }
        assertEquals(contentStore.usedBytes(), contentStore.diskUsedBytes(),
                "不得出现说回收了却还占着磁盘的 blob");
        log.info("[test] all snapshots deleted; remaining objects={} all readable, used==disk={}",
                storage.list().size(), contentStore.usedBytes());
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
