package com.github.lisama569.objectstoragesnapshotdedupjava.tests;

import com.github.lisama569.objectstoragesnapshotdedupjava.exception.ErrorCode;
import com.github.lisama569.objectstoragesnapshotdedupjava.exception.StorageException;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.ObjectMeta;
import com.github.lisama569.objectstoragesnapshotdedupjava.service.ObjectStorageService;
import com.github.lisama569.objectstoragesnapshotdedupjava.store.ContentStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 去重与引用计数：并发写入、重复写入、删除交错。
 */
class ObjectStorageDedupTest {

    private static final Logger log = LoggerFactory.getLogger(ObjectStorageDedupTest.class);

    @TempDir
    Path tempDir;

    ContentStore contentStore;
    ObjectStorageService storage;

    @BeforeEach
    void setUp() {
        contentStore = new ContentStore(tempDir.toString(), 1_000_000);
        storage = new ObjectStorageService(contentStore);
    }

    @Test
    void duplicateWritesShareSingleBlobAndRefCountIsAccurate() {
        byte[] content = "hello-dedup".getBytes(StandardCharsets.UTF_8);
        log.info("[test] input: two objects with identical content '{}'", new String(content));

        ObjectMeta a = storage.put("a", content);
        ObjectMeta b = storage.put("b", content);

        assertEquals(a.contentHash(), b.contentHash(), "相同内容必须复用同一哈希");
        assertEquals(2, contentStore.refCount(a.contentHash()), "两次写入后引用计数应为 2");
        log.info("[test] refCount after 2 puts = {} (判定依据: 内容相同复用同一份数据)", 2);

        storage.delete("a");
        assertEquals(1, contentStore.refCount(a.contentHash()));
        assertTrue(contentStore.exists(a.contentHash()), "仍有引用时不得提前回收");
        assertArrayEquals(content, storage.get("b"), "删除 a 后 b 仍可读");

        storage.delete("b");
        assertEquals(0, contentStore.refCount(a.contentHash()));
        assertFalse(contentStore.exists(a.contentHash()), "引用归零后应回收底层数据");
        log.info("[test] refCount 2->1->0, blob reclaimed: 判定通过");
    }

    @Test
    void concurrentWritesOfSameContentDeduplicateWithoutLostRefs() throws Exception {
        byte[] content = "concurrent-payload".getBytes(StandardCharsets.UTF_8);
        int threads = 16;
        log.info("[test] input: {} threads writing identical content concurrently", threads);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<ObjectMeta>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            String name = "obj-" + i;
            futures.add(pool.submit(() -> {
                start.await();
                return storage.put(name, content);
            }));
        }
        start.countDown();
        String hash = null;
        for (Future<ObjectMeta> f : futures) {
            ObjectMeta m = f.get();
            if (hash == null) hash = m.contentHash();
            assertEquals(hash, m.contentHash(), "所有并发写入必须得到同一哈希");
        }
        pool.shutdown();

        assertEquals(threads, contentStore.refCount(hash),
                "并发写入后引用计数必须等于写入次数，不得丢失引用");
        long blobCount = countBlobs(hash);
        assertEquals(1, blobCount, "相同内容只能落盘一份，不得重复占用");
        log.info("[test] refCount={} blobFiles={} (判定依据: 引用计数==线程数 且 仅一份数据)", threads, blobCount);

        // 并发删除一半，剩余仍可读，计数准确
        ExecutorService delPool = Executors.newFixedThreadPool(threads / 2);
        CountDownLatch delStart = new CountDownLatch(1);
        List<Future<?>> delFutures = new ArrayList<>();
        for (int i = 0; i < threads / 2; i++) {
            String name = "obj-" + i;
            delFutures.add(delPool.submit(() -> {
                delStart.await();
                storage.delete(name);
                return null;
            }));
        }
        delStart.countDown();
        for (Future<?> f : delFutures) f.get();
        delPool.shutdown();

        assertEquals(threads / 2, contentStore.refCount(hash));
        assertTrue(contentStore.exists(hash), "仍有引用时不得提前回收");
        assertArrayEquals(content, storage.get("obj-" + (threads - 1)));
        log.info("[test] after deleting half: refCount={} (判定依据: 计数随删除递减且数据仍可读)",
                contentStore.refCount(hash));
    }

    @Test
    void overwriteReleasesOldContentAndKeepsNew() {
        byte[] v1 = "version-1".getBytes(StandardCharsets.UTF_8);
        byte[] v2 = "version-2".getBytes(StandardCharsets.UTF_8);
        log.info("[test] input: overwrite same name with v1 then v2");

        ObjectMeta m1 = storage.put("doc", v1);
        ObjectMeta m2 = storage.put("doc", v2);

        assertEquals(2, m2.version());
        assertEquals(0, contentStore.refCount(m1.contentHash()), "覆盖后旧内容引用应归零");
        assertFalse(contentStore.exists(m1.contentHash()), "旧内容应被回收");
        assertEquals(1, contentStore.refCount(m2.contentHash()));
        assertArrayEquals(v2, storage.get("doc"));
        log.info("[test] old hash reclaimed, new hash refCount=1: 判定通过");
    }

    @Test
    void staleVersionWriteIsRejectedWithDistinguishableCode() {
        storage.put("doc", "v1".getBytes(StandardCharsets.UTF_8));
        log.info("[test] input: put with expectedVersion=0 while current version=1");

        StorageException e = assertThrows(StorageException.class,
                () -> storage.put("doc", "v2".getBytes(StandardCharsets.UTF_8), 0));
        assertEquals(ErrorCode.CONCURRENT_CONFLICT, e.getErrorCode(), "版本冲突须可区分");
        assertArrayEquals("v1".getBytes(StandardCharsets.UTF_8), storage.get("doc"),
                "冲突写入不得留下半写入状态");
        log.info("[test] errorCode={} (判定依据: 乐观锁版本不匹配)", e.getErrorCode());
    }

    private long countBlobs(String hash) throws Exception {
        try (var stream = java.nio.file.Files.list(tempDir)) {
            return stream.filter(p -> p.getFileName().toString().equals(hash + ".blob")).count();
        }
    }
}
