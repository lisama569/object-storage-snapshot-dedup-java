package com.github.lisama569.objectstoragesnapshotdedupjava.tests;

import com.github.lisama569.objectstoragesnapshotdedupjava.model.SnapshotMeta;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * “删除一份快照预计能回收多少（exclusiveBytes）”与“删完实际释放多少”逐字节一致的回归测试。
 *
 * 背景缺陷：旧口径用“引用计数 == 1”判定独占，同一份快照里同内容有多条记录时
 * 该快照对它持有多条引用（计数 > 1），被误判为“非独占、预计回收 0”，
 * 但删除时逐条释放引用仍会把数据回收到零，导致预计值与实际回收对不上。
 * 正确口径：只要当前对象和其它快照都不引用该内容（本快照是唯一持有者），
 * 无论本快照内有几条记录，都计入预计回收。
 */
class SnapshotExclusiveReclaimTest {

    private static final Logger log = LoggerFactory.getLogger(SnapshotExclusiveReclaimTest.class);

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

    private SnapshotMeta statOf(String name) {
        return snapshots.snapshotStats().stream()
                .filter(m -> m.name().equals(name)).findFirst().orElseThrow();
    }

    /** 删除指定快照，并断言“实际回收 == 删除前统计的 exclusiveBytes”。 */
    private void deleteAndVerifyReclaim(String name) {
        SnapshotMeta meta = statOf(name);
        long before = contentStore.usedBytes();
        snapshots.deleteSnapshot(name);
        long reclaimed = before - contentStore.usedBytes();
        assertEquals(meta.exclusiveBytes(), reclaimed,
                "删除 " + name + " 实际回收量须逐字节等于预计的 exclusiveBytes");
        assertEquals(contentStore.usedBytes(), contentStore.diskUsedBytes(),
                "删除后已用容量须与实际落盘一致");
        log.info("[test] delete {} predicted={} actual={} usedBytes={} (判定依据: 预计回收 == 实际回收 == 磁盘差值)",
                name, meta.exclusiveBytes(), reclaimed, contentStore.usedBytes());
    }

    @Test
    void duplicateContentRecordsInOneSnapshotAreCountedAsExclusive() {
        // 用户复现场景：两个内容完全相同的对象（去重到同一份数据），打快照后删掉两个当前对象。
        storage.put("o1", bytes("dup-payload-0123456789"));
        storage.put("o2", bytes("dup-payload-0123456789")); // 去重命中，同一内容两条记录
        snapshots.createSnapshot("snap");
        storage.delete("o1");
        storage.delete("o2");
        int size = "dup-payload-0123456789".length();
        log.info("[test] input: 2 same-content objects + snapshot, then both current objects deleted; contentSize={}", size);

        SnapshotMeta meta = statOf("snap");
        log.info("[test] snap objectCount={} referenced={} exclusive={}",
                meta.objectCount(), meta.referencedBytes(), meta.exclusiveBytes());
        assertEquals(2, meta.objectCount(), "快照内有两条同内容记录");
        assertEquals(size, meta.referencedBytes(), "同内容只算一份底层数据");
        assertEquals(size, meta.exclusiveBytes(),
                "本快照是该内容唯一持有者（虽有两条记录），删除预计回收一整份，不得为 0");

        deleteAndVerifyReclaim("snap");
        assertEquals(0, contentStore.usedBytes(), "唯一持有者删除后数据须完全回收");
        assertEquals(0, contentStore.diskUsedBytes());
        assertFalse(contentStore.exists(ContentStore.sha256Hex(bytes("dup-payload-0123456789"))));
    }

    @Test
    void contentStillReferencedByCurrentObjectIsNotExclusive() {
        storage.put("keep", bytes("shared-with-current"));
        storage.put("drop", bytes("shared-with-current")); // 同内容第二条记录
        snapshots.createSnapshot("snap");
        storage.delete("drop"); // 当前对象 keep 仍引用该内容
        int size = "shared-with-current".length();
        log.info("[test] input: snapshot holds 2 records of same content, current object 'keep' still references it");

        SnapshotMeta meta = statOf("snap");
        assertEquals(0, meta.exclusiveBytes(), "内容仍被当前对象引用，预计回收须为 0");
        assertEquals(size, meta.referencedBytes());

        deleteAndVerifyReclaim("snap"); // 预计 0，实际也须为 0
        assertEquals(size, contentStore.usedBytes(), "共享内容不得被回收");
        assertArrayEquals(bytes("shared-with-current"), storage.get("keep"),
                "当前对象必须照常可读，不得提前回收");
        storage.put("keep", bytes("rewritten")); // 当前对象照常可写
        assertArrayEquals(bytes("rewritten"), storage.get("keep"));
        log.info("[test] current object readable/writable after snapshot delete (判定依据: 共享内容零回收)");
    }

    @Test
    void contentSharedWithOtherSnapshotIsNotExclusiveUntilLastHolderDrops() {
        storage.put("a", bytes("cross-snapshot-shared"));
        storage.put("b", bytes("cross-snapshot-shared")); // 同内容两条记录
        snapshots.createSnapshot("s1");
        snapshots.createSnapshot("s2");
        storage.delete("a");
        storage.delete("b");
        int size = "cross-snapshot-shared".length();
        log.info("[test] input: same content (2 records) held by s1 and s2, current objects deleted");

        assertEquals(0, statOf("s1").exclusiveBytes(), "s2 也持有，s1 不得计入独占");
        assertEquals(0, statOf("s2").exclusiveBytes(), "s1 也持有，s2 不得计入独占");

        deleteAndVerifyReclaim("s1"); // 预计 0 实际 0
        assertTrue(contentStore.exists(ContentStore.sha256Hex(bytes("cross-snapshot-shared"))),
                "s2 仍持有，数据不得回收");
        assertArrayEquals(bytes("cross-snapshot-shared"),
                snapshots.readFromSnapshot("s2", "a"), "s2 必须照常可读");

        assertEquals(size, statOf("s2").exclusiveBytes(),
                "s1 删除后 s2 成为唯一持有者，预计回收须更新为整份大小");
        deleteAndVerifyReclaim("s2"); // 预计 size 实际 size
        assertEquals(0, contentStore.usedBytes());
        assertEquals(0, contentStore.diskUsedBytes());
        log.info("[test] last-holder delete reclaimed {} (判定依据: 仅最后一个持有者删除才回收)", size);
    }

    @Test
    void mixedExclusiveAndSharedContentInOneSnapshot() {
        String a = "AAAA-exclusive-dup";   // 快照内两条记录，且仅该快照持有
        String b = "BB-shared-with-current"; // 与当前对象共享
        String c = "CCC-shared-with-s2";     // 与另一份快照共享
        storage.put("dup1", bytes(a));
        storage.put("dup2", bytes(a));
        storage.put("cur", bytes(b));
        storage.put("shr", bytes(c));
        snapshots.createSnapshot("s1");
        storage.delete("dup1");
        storage.delete("dup2");          // A 此后仅被 s1 持有（两条记录）
        snapshots.createSnapshot("s2");  // s2 含 cur、shr
        storage.delete("shr");           // C 此后被 s1、s2 共同持有
        log.info("[test] input: s1={{A,A,B,C}}, s2={{B,C}}, current={{B}}; sizes A={} B={} C={}",
                a.length(), b.length(), c.length());

        SnapshotMeta s1 = statOf("s1");
        assertEquals((long) a.length() + b.length() + c.length(), s1.referencedBytes(),
                "s1 引用三份不同内容（A 去重后只算一份）");
        assertEquals(a.length(), s1.exclusiveBytes(),
                "只有 A 是 s1 独占：B 被当前对象引用、C 被 s2 引用，均不得计入");

        deleteAndVerifyReclaim("s1"); // 预计 A 实际 A
        assertArrayEquals(bytes(b), storage.get("cur"), "当前对象不得受影响");
        assertArrayEquals(bytes(c), snapshots.readFromSnapshot("s2", "shr"), "s2 不得受影响");
        assertFalse(contentStore.exists(ContentStore.sha256Hex(bytes(a))), "A 须被回收");
        assertTrue(contentStore.exists(ContentStore.sha256Hex(bytes(c))), "C 仍被 s2 持有不得回收");

        assertEquals(c.length(), statOf("s2").exclusiveBytes(),
                "s1 删除后 C 归 s2 独占，B 仍被当前对象引用");
        deleteAndVerifyReclaim("s2"); // 预计 C 实际 C
        assertEquals(b.length(), contentStore.usedBytes(), "只剩当前对象引用的 B");
        assertEquals(b.length(), contentStore.diskUsedBytes());
        assertArrayEquals(bytes(b), storage.get("cur"));
        log.info("[test] mixed scenario settled: used={} (判定依据: 独占/共享逐项核对，最终只剩当前对象数据)",
                contentStore.usedBytes());
    }

    @Test
    void evictionAndManualDeleteGiveSameFinalStateForDuplicateRecords() {
        // 场景 A：手动删除；场景 B：保留策略淘汰。同内容多条记录 + 唯一持有者，最终状态须一致。
        ContentStore storeB = new ContentStore(tempDir.resolve("b").toString(), 1_000_000);
        ObjectStorageService storageB = new ObjectStorageService(storeB);
        SnapshotService snapshotsB = new SnapshotService(storageB, storeB);

        storage.put("x1", bytes("same-payload"));
        storage.put("x2", bytes("same-payload"));
        snapshots.createSnapshot("victim");
        storage.delete("x1");
        storage.delete("x2");

        storageB.put("x1", bytes("same-payload"));
        storageB.put("x2", bytes("same-payload"));
        snapshotsB.createSnapshot("victim");
        storageB.delete("x1");
        storageB.delete("x2");
        log.info("[test] input: two identical stores, victim snapshot holds 2 records of same content and is sole holder");

        long predictedA = statOf("victim").exclusiveBytes();
        long predictedB = snapshotsB.snapshotStats().stream()
                .filter(m -> m.name().equals("victim")).findFirst().orElseThrow().exclusiveBytes();
        assertEquals(predictedA, predictedB, "两条路径的预计回收口径须一致");
        assertEquals("same-payload".length(), predictedA);

        long beforeA = contentStore.usedBytes();
        snapshots.deleteSnapshot("victim");              // A：手动删除
        long beforeB = storeB.usedBytes();
        snapshotsB.createSnapshot("trigger", 1, null);   // B：按份数淘汰 victim
        log.info("[test] A manual delete reclaimed={}; B retention evict reclaimed={}",
                beforeA - contentStore.usedBytes(), beforeB - storeB.usedBytes());

        assertEquals(contentStore.usedBytes(), storeB.usedBytes(),
                "手动删除与自动淘汰对同一份数据须给出同样的最终状态");
        assertEquals(0, contentStore.usedBytes());
        assertEquals(0, storeB.usedBytes());
        assertEquals(0, contentStore.diskUsedBytes());
        assertEquals(0, storeB.diskUsedBytes());
        log.info("[test] final states identical (判定依据: 删除与淘汰最终占用都为 0 且与磁盘一致)");
    }

    @Test
    void predictedEqualsActualForEveryDeleteAfterConcurrentChurn() throws Exception {
        int threads = 6;
        int ops = 40;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new java.util.ArrayList<>();
        log.info("[test] input: {} threads x {} interleaved put/delete/snapshot-create/snapshot-delete ops", threads, ops);
        for (int t = 0; t < threads; t++) {
            final int id = t;
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    java.util.Random rnd = new java.util.Random(id);
                    for (int i = 0; i < ops; i++) {
                        String obj = "obj-" + (i % 4);
                        String snap = "snap-" + id + "-" + (i % 3);
                        switch (rnd.nextInt(4)) {
                            case 0 -> {
                                try {
                                    // 有意让不同对象写同内容，制造快照内/跨快照的同内容多条记录
                                    storage.put(obj, bytes("payload-" + (i % 7)));
                                } catch (Exception ignored) {
                                }
                            }
                            case 1 -> {
                                try {
                                    storage.delete(obj);
                                } catch (Exception ignored) {
                                }
                            }
                            case 2 -> {
                                try {
                                    snapshots.createSnapshot(snap);
                                } catch (Exception ignored) {
                                }
                            }
                            default -> {
                                try {
                                    snapshots.deleteSnapshot(snap);
                                } catch (Exception ignored) {
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

        // 静止后逐份删除：每一刀的实际回收都必须等于删除前统计的预计回收
        List<String> names = snapshots.listSnapshots();
        log.info("[test] churn settled with {} snapshots; deleting one by one and checking predicted==actual each time",
                names.size());
        for (String name : names) {
            deleteAndVerifyReclaim(name);
        }

        // 全部快照删完后：占用 == 当前对象引用的去重后数据量 == 磁盘实际占用，且对象全部可读
        Set<String> liveHashes = new HashSet<>();
        long expected = 0;
        for (var meta : storage.list()) {
            if (liveHashes.add(meta.contentHash())) {
                expected += contentStore.sizeOf(meta.contentHash());
            }
        }
        assertEquals(expected, contentStore.usedBytes(),
                "快照清空后不得残留无主数据（说回收了就必须真回收）");
        assertEquals(expected, contentStore.diskUsedBytes(),
                "不得显示没占空间其实还占着（内存统计 == 磁盘占用）");
        for (var meta : storage.list()) {
            assertDoesNotThrow(() -> storage.get(meta.name()),
                    "当前对象必须照常可读: " + meta.name());
        }
        storage.put("post-churn", bytes("writable-after-churn"));
        assertArrayEquals(bytes("writable-after-churn"), storage.get("post-churn"));
        log.info("[test] after full teardown: used={} expected={} objects={} (判定依据: 占用==当前对象去重数据量==磁盘，对象可读写)",
                contentStore.usedBytes(), expected, storage.list().size());
    }
}
