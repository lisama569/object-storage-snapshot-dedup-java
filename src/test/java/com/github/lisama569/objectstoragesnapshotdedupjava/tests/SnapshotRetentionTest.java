package com.github.lisama569.objectstoragesnapshotdedupjava.tests;

import com.github.lisama569.objectstoragesnapshotdedupjava.exception.ErrorCode;
import com.github.lisama569.objectstoragesnapshotdedupjava.exception.StorageException;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.RetentionPolicy;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.SnapshotCreateResult;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.SnapshotDeleteResult;
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
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 保留策略：按份数/时长自动淘汰最旧快照、边界行为、淘汰与手动删除同一回收口径、
 * 被淘汰快照不可读/恢复、不误伤更近快照与当前对象。
 */
class SnapshotRetentionTest {

    private static final Logger log = LoggerFactory.getLogger(SnapshotRetentionTest.class);

    @TempDir
    Path tempDir;

    ContentStore contentStore;
    ObjectStorageService storage;
    AtomicLong clock;
    SnapshotService snapshots;

    @BeforeEach
    void setUp() {
        contentStore = new ContentStore(tempDir.toString(), 50_000_000);
        storage = new ObjectStorageService(contentStore);
        clock = new AtomicLong(1_000_000L);
        snapshots = new SnapshotService(storage, contentStore, clock::get);
    }

    @Test
    void maxCountEvictsOldestSnapshotsAndReclaimsExclusiveSpace() {
        byte[] v1 = "version-1".getBytes(StandardCharsets.UTF_8);
        storage.put("a", v1);
        SnapshotCreateResult r1 = snapshots.createSnapshot("s1", new RetentionPolicy(3, null));
        assertEquals(0, r1.evicted().size());
        clock.incrementAndGet();

        storage.put("a", "version-2".getBytes(StandardCharsets.UTF_8));
        snapshots.createSnapshot("s2", new RetentionPolicy(3, null));
        clock.incrementAndGet();
        storage.put("a", "version-3".getBytes(StandardCharsets.UTF_8));
        snapshots.createSnapshot("s3", new RetentionPolicy(3, null));
        clock.incrementAndGet();
        log.info("[test] created s1,s2,s3 with maxCount=3; now creating s4 should evict oldest s1");

        String s1ExclusiveHash = ContentStore.sha256Hex(v1);
        assertTrue(contentStore.exists(s1ExclusiveHash), "前置：s1 独占的 v1 blob 仍在");

        storage.put("a", "version-4".getBytes(StandardCharsets.UTF_8));
        SnapshotCreateResult r4 = snapshots.createSnapshot("s4", new RetentionPolicy(3, null));

        assertEquals(1, r4.evicted().size(), "超出上限必须恰好淘汰一份");
        SnapshotDeleteResult eviction = r4.evicted().get(0);
        assertEquals("s1", eviction.name(), "必须淘汰最旧的快照");
        assertTrue(eviction.evicted(), "淘汰必须标记 evicted=true");
        assertEquals(v1.length, eviction.reclaimedBytes(), "被淘汰快照独占空间必须随之回收");
        assertFalse(contentStore.exists(s1ExclusiveHash), "淘汰后独占 blob 必须真正从磁盘回收");

        assertEquals(java.util.List.of("s2", "s3", "s4"), snapshots.listSnapshots(),
                "保留最新的 3 份");
        // 被淘汰快照不可读、不可恢复，失败理由与手动删除一致（SNAPSHOT_NOT_FOUND）
        StorageException readEx = assertThrows(StorageException.class,
                () -> snapshots.readFromSnapshot("s1", "a"));
        assertEquals(ErrorCode.SNAPSHOT_NOT_FOUND, readEx.getErrorCode());
        StorageException restoreEx = assertThrows(StorageException.class,
                () -> snapshots.restore("s1"));
        assertEquals(ErrorCode.SNAPSHOT_NOT_FOUND, restoreEx.getErrorCode(),
                "被淘汰快照不能再用于回滚，理由须与不存在/手动删除一致");
        log.info("[test] 判定通过: s1 淘汰回收={}B, 读/恢复均 SNAPSHOT_NOT_FOUND", v1.length);
    }

    @Test
    void evictionDoesNotHarmNewerSnapshotsOrCurrentObjects() {
        byte[] common = "common-content".getBytes(StandardCharsets.UTF_8);
        storage.put("a", common);
        snapshots.createSnapshot("old", new RetentionPolicy(1, null));
        clock.incrementAndGet();
        snapshots.createSnapshot("new", new RetentionPolicy(1, null));

        // old 被淘汰，但其内容当前对象与 new 仍引用 -> 不回收
        String hash = ContentStore.sha256Hex(common);
        assertTrue(contentStore.exists(hash), "共享数据不得被淘汰误伤");
        assertArrayEquals(common, snapshots.readFromSnapshot("new", "a"), "更近快照仍可读");
        assertArrayEquals(common, storage.get("a"), "当前对象不受淘汰影响");
        log.info("[test] 判定通过: 淘汰 old 后共享 blob 保留，new 与当前对象完好");
    }

    @Test
    void maxCountBoundaryKeepExactlyNAndNeverEvictTheJustCreatedOne() {
        for (int i = 1; i <= 5; i++) {
            storage.put("a", ("v" + i).getBytes(StandardCharsets.UTF_8));
            SnapshotCreateResult r = snapshots.createSnapshot("s" + i, new RetentionPolicy(3, null));
            clock.incrementAndGet();
            // 每次创建后存量不得超过上限，且刚创建的快照必须还在
            assertEquals(Math.min(i, 3), snapshots.listSnapshots().size());
            assertTrue(snapshots.listSnapshots().contains("s" + i), "新快照不得被本次策略淘汰");
        }
        assertEquals(java.util.List.of("s3", "s4", "s5"), snapshots.listSnapshots());
        log.info("[test] 判定通过: 边界 maxCount=3 下始终恰好保留最新 3 份");
    }

    @Test
    void maxAgeEvictsExpiredSnapshotsAndReclaimsSpace() {
        storage.put("a", "old-version".getBytes(StandardCharsets.UTF_8));
        snapshots.createSnapshot("ancient", new RetentionPolicy(null, 100L));
        clock.addAndGet(50);
        storage.put("a", "recent-version".getBytes(StandardCharsets.UTF_8));
        snapshots.createSnapshot("fresh", new RetentionPolicy(null, 100L));
        log.info("[test] ancient age=50 fresh age=0, maxAge=100; advance clock by 60");

        clock.addAndGet(60); // ancient=110 过期, fresh=60 未过期
        storage.put("a", "now-version".getBytes(StandardCharsets.UTF_8));
        SnapshotCreateResult r = snapshots.createSnapshot("now", new RetentionPolicy(null, 100L));

        assertEquals(1, r.evicted().size());
        assertEquals("ancient", r.evicted().get(0).name(), "必须淘汰超过保留时长的最旧快照");
        assertEquals(java.util.List.of("fresh", "now"), snapshots.listSnapshots(),
                "未过期的快照必须保留");
        assertThrows(StorageException.class, () -> snapshots.readFromSnapshot("ancient", "a"));
        assertArrayEquals("recent-version".getBytes(StandardCharsets.UTF_8),
                snapshots.readFromSnapshot("fresh", "a"), "未过期快照仍可读取");
        log.info("[test] 判定通过: 超龄 ancient 被淘汰，fresh 保留可读");
    }

    @Test
    void countAndAgeCombineWithSameReclaimAccountingAsManualDelete() {
        // 同一回收口径：先准备 4 份各含独占内容的快照，再用 (maxCount=2, maxAge=100) 创建第 5 份。
        // 预期：最旧的 3 份被淘汰（age 与 count 共同作用），淘汰回收字节之和 == 手动逐个删除同样快照的回收量。
        byte[][] contents = new byte[5][];
        for (int i = 0; i < 4; i++) {
            contents[i] = ("unique-content-" + i).getBytes(StandardCharsets.UTF_8);
            storage.put("a", contents[i]);
            snapshots.createSnapshot("s" + i, RetentionPolicy.unlimited());
            clock.incrementAndGet();
        }
        long expectedExclusive = 0;
        for (int i = 0; i < 3; i++) {
            expectedExclusive += contents[i].length;
        }

        contents[4] = "unique-content-4".getBytes(StandardCharsets.UTF_8);
        storage.put("a", contents[4]);
        long beforeUsed = contentStore.usedBytes(); // 紧接淘汰前测量（含当前 v4 blob）
        SnapshotCreateResult r = snapshots.createSnapshot("s4", new RetentionPolicy(2, 100L));

        assertEquals(3, r.evicted().size(), "s0/s1/s2 均超龄且超出 maxCount=2，共淘汰 3 份");
        assertEquals(java.util.List.of("s3", "s4"), snapshots.listSnapshots());
        assertEquals(expectedExclusive, r.evictedBytes(), "淘汰回收口径必须与手动删除一致");
        assertEquals(beforeUsed - expectedExclusive, contentStore.usedBytes(),
                "容量下降必须等于淘汰回收量");
        assertEquals(expectedExclusive,
                manualDeleteReclaimReference(),
                "口径对照：被淘汰快照独占字节之和（本用例即前 3 份独占内容之和）");
        log.info("[test] 判定通过: 淘汰 {} 份, 合计回收 {}B 与手动删除口径一致", 3, expectedExclusive);
    }

    /** 参考值：前 3 份快照独占内容字节之和（它们内容互不相同且已不在当前对象中）。 */
    private long manualDeleteReclaimReference() {
        long sum = 0;
        for (int i = 0; i < 3; i++) {
            sum += ("unique-content-" + i).getBytes(StandardCharsets.UTF_8).length;
        }
        return sum;
    }

    @Test
    void invalidPolicyIsRejectedAndCreatesNothing() {
        StorageException e1 = assertThrows(StorageException.class,
                () -> snapshots.createSnapshot("bad", new RetentionPolicy(0, null)));
        assertEquals(ErrorCode.INVALID_RETENTION, e1.getErrorCode(), "maxCount=0 须可区分拒绝");
        StorageException e2 = assertThrows(StorageException.class,
                () -> snapshots.createSnapshot("bad", new RetentionPolicy(null, -5L)));
        assertEquals(ErrorCode.INVALID_RETENTION, e2.getErrorCode(), "maxAge<=0 须可区分拒绝");
        assertEquals(0, snapshots.listSnapshots().size(), "非法策略不得创建任何快照");
        log.info("[test] 判定通过: maxCount=0 / maxAge<0 均 INVALID_RETENTION 且无副作用");
    }

    @Test
    void defaultCreateWithoutPolicyKeepsAllSnapshotsForCompatibility() {
        for (int i = 0; i < 5; i++) {
            storage.put("a", ("v" + i).getBytes(StandardCharsets.UTF_8));
            SnapshotCreateResult r = snapshots.createSnapshot("s" + i); // 不带策略，默认不限
            clock.incrementAndGet();
            assertEquals(0, r.evicted().size(), "默认行为必须与旧版本兼容：不淘汰");
        }
        assertEquals(5, snapshots.listSnapshots().size(), "默认不限制保留份数");
        log.info("[test] 判定通过: 无策略创建默认不限保留（兼容旧行为）");
    }
}
