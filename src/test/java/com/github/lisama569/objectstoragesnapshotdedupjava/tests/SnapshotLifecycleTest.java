package com.github.lisama569.objectstoragesnapshotdedupjava.tests;

import com.github.lisama569.objectstoragesnapshotdedupjava.exception.ErrorCode;
import com.github.lisama569.objectstoragesnapshotdedupjava.exception.StorageException;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.ObjectMeta;
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
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 快照生命周期：手动删除、重复/并发删除、引用回收、删除与恢复/读取交错的安全性。
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
        contentStore = new ContentStore(tempDir.toString(), 10_000_000);
        storage = new ObjectStorageService(contentStore);
        snapshots = new SnapshotService(storage, contentStore);
    }

    @Test
    void deleteSnapshotReclaimsOnlyExclusiveBlobsAndKeepsCurrentObjects() {
        // 业务：快照独占的数据应回收；与当前对象共享的数据不能回收
        byte[] shared = "still-current".getBytes(StandardCharsets.UTF_8);
        byte[] exclusive = "only-in-snapshot".getBytes(StandardCharsets.UTF_8);
        storage.put("a", shared);
        storage.put("b", exclusive);
        snapshots.createSnapshot("snap");
        log.info("[test] input: current a,b -> snapshot snap; then mutate: b overwritten, c added");

        // 快照后演进：b 覆盖（快照独占旧内容），c 新增
        storage.put("b", "new-b".getBytes(StandardCharsets.UTF_8));
        storage.put("c", "new-c".getBytes(StandardCharsets.UTF_8));

        String exclusiveHash = ContentStore.sha256Hex(exclusive);
        String sharedHash = storage.meta("a").contentHash();
        long before = contentStore.usedBytes();
        assertTrue(contentStore.exists(exclusiveHash), "前置：快照仍独占引用，blob 必须还在");

        SnapshotDeleteResult result = snapshots.deleteSnapshot("snap");
        log.info("[test] delete result: name={} evicted={} reclaimedBytes={}",
                result.name(), result.evicted(), result.reclaimedBytes());

        assertFalse(result.evicted(), "手动删除必须标记 evicted=false");
        assertFalse(contentStore.exists(exclusiveHash), "快照独占数据必须回收并体现到容量");
        assertEquals(exclusive.length, result.reclaimedBytes(),
                "回收字节必须恰好等于独占 blob 的物理大小");
        assertEquals(before - exclusive.length, contentStore.usedBytes(),
                "已用容量必须按回收字节下降");
        assertTrue(contentStore.exists(sharedHash), "当前对象仍引用的数据不得被误删");
        assertArrayEquals(shared, storage.get("a"), "删除快照后当前对象照常可读");
        assertArrayEquals("new-b".getBytes(StandardCharsets.UTF_8), storage.get("b"));
        assertArrayEquals("new-c".getBytes(StandardCharsets.UTF_8), storage.get("c"));
        storage.put("d", "write-after-delete".getBytes(StandardCharsets.UTF_8));
        assertArrayEquals("write-after-delete".getBytes(StandardCharsets.UTF_8), storage.get("d"),
                "删除快照后当前对象照样能写");
        log.info("[test] 判定通过: 独占回收={}B 共享保留 当前读写正常", exclusive.length);
    }

    @Test
    void deleteSharedSnapshotReclaimsNothing() {
        // 业务：快照引用的每份数据当前对象也都还在 -> 删除快照回收 0
        byte[] data = "shared-by-all".getBytes(StandardCharsets.UTF_8);
        storage.put("a", data);
        snapshots.createSnapshot("snap");
        long before = contentStore.usedBytes();

        SnapshotDeleteResult result = snapshots.deleteSnapshot("snap");
        assertEquals(0, result.reclaimedBytes(), "全部共享时预计/实际回收都应为 0");
        assertEquals(before, contentStore.usedBytes(), "容量不应变化");
        assertArrayEquals(data, storage.get("a"));
        log.info("[test] 判定通过: 全共享快照 reclaimedBytes=0, usedBytes={} 不变", before);
    }

    @Test
    void deleteMissingSnapshotIsDistinguishable() {
        StorageException e = assertThrows(StorageException.class,
                () -> snapshots.deleteSnapshot("ghost"));
        assertEquals(ErrorCode.SNAPSHOT_NOT_FOUND, e.getErrorCode(),
                "删除不存在的快照须与其他失败区分");
        log.info("[test] input: delete absent snapshot ghost -> errorCode={} message={}",
                e.getErrorCode(), e.getMessage());
    }

    @Test
    void duplicateDeleteSucceedsExactlyOnce() {
        storage.put("a", "payload-a".getBytes(StandardCharsets.UTF_8));
        snapshots.createSnapshot("snap");

        SnapshotDeleteResult first = snapshots.deleteSnapshot("snap");
        assertTrue(first.reclaimedBytes() >= 0);
        StorageException second = assertThrows(StorageException.class,
                () -> snapshots.deleteSnapshot("snap"));
        assertEquals(ErrorCode.SNAPSHOT_NOT_FOUND, second.getErrorCode(),
                "重复删除必须以可区分理由失败，不能再释放一次引用");
        log.info("[test] 判定通过: 第一次删除成功, 第二次 errorCode={}（引用计数无错乱）",
                second.getErrorCode());
        assertArrayEquals("payload-a".getBytes(StandardCharsets.UTF_8), storage.get("a"),
                "重复删除不得影响当前对象");
    }

    @Test
    void concurrentDeleteSucceedsExactlyOnce() throws Exception {
        // 业务：N 个线程并发删除同一快照，恰好 1 个成功，其余 SNAPSHOT_NOT_FOUND，无计数错乱
        storage.put("a", "concurrent-delete-payload".getBytes(StandardCharsets.UTF_8));
        snapshots.createSnapshot("snap");
        String hash = storage.meta("a").contentHash();

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);
        AtomicInteger success = new AtomicInteger();
        AtomicInteger notFound = new AtomicInteger();
        AtomicInteger other = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    barrier.await();
                    snapshots.deleteSnapshot("snap");
                    success.incrementAndGet();
                } catch (StorageException e) {
                    if (e.getErrorCode() == ErrorCode.SNAPSHOT_NOT_FOUND) {
                        notFound.incrementAndGet();
                    } else {
                        other.incrementAndGet();
                    }
                } catch (InterruptedException | BrokenBarrierException ex) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);

        log.info("[test] concurrent delete results: success={} notFound={} other={}",
                success.get(), notFound.get(), other.get());
        assertEquals(1, success.get(), "并发删除必须恰好成功一次");
        assertEquals(threads - 1, notFound.get(), "其余并发删除必须是可区分的 SNAPSHOT_NOT_FOUND");
        assertEquals(0, other.get(), "不得出现其他异常");
        assertEquals(1, contentStore.refCount(hash), "当前对象的唯一引用必须保留，计数不得错乱");
        assertTrue(contentStore.exists(hash), "当前对象仍读得出，数据未被提前删除");
        assertArrayEquals("concurrent-delete-payload".getBytes(StandardCharsets.UTF_8), storage.get("a"));
        log.info("[test] 判定通过: 唯一成功 + 引用计数=1 + 数据可读（无损坏状态）");
    }

    @Test
    void deletedSnapshotCannotBeReadOrRestored() {
        storage.put("a", "v1".getBytes(StandardCharsets.UTF_8));
        snapshots.createSnapshot("snap");
        storage.put("a", "v2".getBytes(StandardCharsets.UTF_8));
        snapshots.deleteSnapshot("snap");

        StorageException readEx = assertThrows(StorageException.class,
                () -> snapshots.readFromSnapshot("snap", "a"));
        assertEquals(ErrorCode.SNAPSHOT_NOT_FOUND, readEx.getErrorCode());
        StorageException restoreEx = assertThrows(StorageException.class,
                () -> snapshots.restore("snap"));
        assertEquals(ErrorCode.SNAPSHOT_NOT_FOUND, restoreEx.getErrorCode());
        assertArrayEquals("v2".getBytes(StandardCharsets.UTF_8), storage.get("a"),
                "删除快照不得改变当前状态");
        log.info("[test] 判定通过: 已删快照读取/恢复均 SNAPSHOT_NOT_FOUND，当前状态不变");
    }

    @Test
    void restoreAndDeleteOfSameSnapshotInterleaveSafely() throws Exception {
        // 业务：恢复与删除并发作用于同一快照。串行化下要么先恢复成功（随后删除成功且当前不变），
        // 要么先删除成功（恢复得到 SNAPSHOT_NOT_FOUND 且当前保持 v2）。两种结局都必须安全。
        storage.put("a", "v1".getBytes(StandardCharsets.UTF_8));
        snapshots.createSnapshot("snap");
        storage.put("a", "v2".getBytes(StandardCharsets.UTF_8));

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CyclicBarrier barrier = new CyclicBarrier(2);
        Future<?> restore = pool.submit(() -> {
            try {
                barrier.await();
                snapshots.restore("snap");
            } catch (StorageException e) {
                assertEquals(ErrorCode.SNAPSHOT_NOT_FOUND, e.getErrorCode());
            } catch (Exception ignored) {
            }
        });
        Future<?> delete = pool.submit(() -> {
            try {
                barrier.await();
                snapshots.deleteSnapshot("snap");
            } catch (StorageException e) {
                assertEquals(ErrorCode.SNAPSHOT_NOT_FOUND, e.getErrorCode());
            } catch (Exception ignored) {
            }
        });
        restore.get();
        delete.get();
        pool.shutdown();

        // 无论顺序如何，当前对象必须可读且状态合法：恢复成功过则为 v1（随后删除快照不影响当前）；
        // 若先删除则为 v2。数据绝不能中途被抽走。
        byte[] current = storage.get("a");
        String text = new String(current, StandardCharsets.UTF_8);
        assertTrue(text.equals("v1") || text.equals("v2"), "当前状态必须是合法结局之一: " + text);
        log.info("[test] 判定通过: restore/delete 交错后当前 a={}，无中途抽数据", text);
    }

    @Test
    void concurrentReadAndDeleteNeverReturnsUnreadableState() throws Exception {
        // 业务：读取快照与删除快照并发，读到的线程必须拿到完整内容；被删除挡住的读得到 SNAPSHOT_NOT_FOUND。
        int rounds = 20;
        ExecutorService pool = Executors.newFixedThreadPool(2);
        for (int round = 0; round < rounds; round++) {
            byte[] payload = ("snapshot-payload-round-" + round).getBytes(StandardCharsets.UTF_8);
            ObjectMeta meta = storage.put("a", payload);
            String name = "snap-" + round;
            snapshots.createSnapshot(name);

            CyclicBarrier barrier = new CyclicBarrier(2);
            Future<?> reader = pool.submit(() -> {
                try {
                    barrier.await();
                    byte[] got = snapshots.readFromSnapshot(name, "a");
                    assertArrayEquals(payload, got, "一旦读到就必须是完整数据，不能读到损坏/缺失");
                } catch (StorageException e) {
                    assertEquals(ErrorCode.SNAPSHOT_NOT_FOUND, e.getErrorCode(),
                            "读被删除挡住时只能是 SNAPSHOT_NOT_FOUND");
                } catch (Exception ignored) {
                }
            });
            Future<?> deleter = pool.submit(() -> {
                try {
                    barrier.await();
                    snapshots.deleteSnapshot(name);
                } catch (StorageException e) {
                    assertEquals(ErrorCode.SNAPSHOT_NOT_FOUND, e.getErrorCode());
                } catch (Exception ignored) {
                }
            });
            reader.get();
            deleter.get();
            // 当前对象始终完好
            assertArrayEquals(payload, storage.get("a"), "当前对象不得受快照删除影响 (round=" + round + ")");
        }
        pool.shutdown();
        log.info("[test] 判定通过: {} 轮读/删并发，读要么完整要么 SNAPSHOT_NOT_FOUND，当前始终完好", rounds);
    }
}
