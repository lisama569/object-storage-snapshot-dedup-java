package com.github.lisama569.objectstoragesnapshotdedupjava.tests;

import com.github.lisama569.objectstoragesnapshotdedupjava.config.StorageProperties;
import com.github.lisama569.objectstoragesnapshotdedupjava.storage.ChunkStore;
import com.github.lisama569.objectstoragesnapshotdedupjava.storage.ObjectMeta;
import com.github.lisama569.objectstoragesnapshotdedupjava.storage.ObjectStorageService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发写入去重：相同内容只保留一份数据，引用计数在并发写、重复写与删除交错时保持准确。
 */
class DedupConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(DedupConcurrencyTest.class);

    @TempDir
    Path tempDir;

    private ChunkStore chunkStore;
    private ObjectStorageService service;

    @BeforeEach
    void setUp() {
        StorageProperties props = new StorageProperties();
        props.setRootDir(tempDir.toString());
        props.setCapacityBytes(1024 * 1024);
        chunkStore = new ChunkStore(props);
        service = new ObjectStorageService(chunkStore);
    }

    @Test
    void concurrentWritesOfSameContentDeduplicateToSingleChunk() throws Exception {
        byte[] content = "shared-payload".getBytes(StandardCharsets.UTF_8);
        String expectedHash = ChunkStore.sha256(content);
        int threads = 16;
        log.info("[输入] {} 个线程并发写入相同内容到不同 key, content='{}'", threads, new String(content));

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Callable<ObjectMeta>> tasks = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            String key = "obj-" + i;
            tasks.add(() -> service.putObject(key, content, null));
        }
        List<Future<ObjectMeta>> results = pool.invokeAll(tasks);
        pool.shutdown();
        for (Future<ObjectMeta> f : results) {
            assertEquals(expectedHash, f.get().contentHash(), "所有写入应映射到同一内容哈希");
        }

        int refCount = chunkStore.refCount(expectedHash);
        log.info("[判定] 去重后 refCount={} (期望 {}), usedBytes={} (期望 {})",
                refCount, threads, chunkStore.usedBytes(), content.length);
        assertEquals(threads, refCount, "每个 key 持有一个引用，不得重复占用或丢失");
        assertEquals(content.length, chunkStore.usedBytes(), "相同内容只占用一份容量");
        assertEquals(1, chunkStore.getClass() != null
                ? java.nio.file.Files.list(tempDir.resolve("chunks")).count() : -1, "磁盘上只应有一个 chunk 文件");
    }

    @Test
    void interleavedDuplicateWritesAndDeletesKeepRefCountAccurate() throws Exception {
        byte[] contentA = "alpha".getBytes(StandardCharsets.UTF_8);
        byte[] contentB = "beta".getBytes(StandardCharsets.UTF_8);
        String hashA = ChunkStore.sha256(contentA);
        String hashB = ChunkStore.sha256(contentB);
        log.info("[输入] 交错写入/删除: k1=A, k2=A, k3=B, 重复写 k1=A, 覆盖 k2=B, 删除 k1/k3");

        service.putObject("k1", contentA, null);
        service.putObject("k2", contentA, null);
        service.putObject("k3", contentB, null);
        log.info("[引用] hashA refCount={} (期望 2), hashB refCount={} (期望 1)",
                chunkStore.refCount(hashA), chunkStore.refCount(hashB));
        assertEquals(2, chunkStore.refCount(hashA));
        assertEquals(1, chunkStore.refCount(hashB));

        // 重复写相同内容：引用计数不变，仅版本推进
        ObjectMeta meta = service.putObject("k1", contentA, null);
        log.info("[判定] 重复写 k1 相同内容后 hashA refCount={} (期望 2), version={}",
                chunkStore.refCount(hashA), meta.version());
        assertEquals(2, chunkStore.refCount(hashA), "重复写相同内容不得增加引用");
        assertEquals(2, meta.version());

        // 覆盖 k2: A -> B，A 引用 -1，B 引用 +1
        service.putObject("k2", contentB, null);
        log.info("[判定] 覆盖 k2=B 后 hashA refCount={} (期望 1), hashB refCount={} (期望 2)",
                chunkStore.refCount(hashA), chunkStore.refCount(hashB));
        assertEquals(1, chunkStore.refCount(hashA));
        assertEquals(2, chunkStore.refCount(hashB));

        // 删除 k1(A) 与 k3(B)：A 归零被回收，B 仍被 k2 持有
        service.deleteObject("k1");
        service.deleteObject("k3");
        log.info("[判定] 删除 k1,k3 后 hashA refCount={} (期望 0, 已回收), hashB refCount={} (期望 1), usedBytes={} (期望 {})",
                chunkStore.refCount(hashA), chunkStore.refCount(hashB), chunkStore.usedBytes(), contentB.length);
        assertEquals(0, chunkStore.refCount(hashA), "引用归零必须回收");
        assertEquals(1, chunkStore.refCount(hashB), "仍被引用的内容不得提前回收");
        assertEquals(contentB.length, chunkStore.usedBytes());
        assertArrayEquals(contentB, service.getObject("k2"));
    }

    @Test
    void concurrentDeletesAndRewritesNeverLoseLiveContent() throws Exception {
        byte[] content = "hot-content".getBytes(StandardCharsets.UTF_8);
        String hash = ChunkStore.sha256(content);
        int rounds = 50;
        log.info("[输入] {} 轮并发: 一个线程反复删除 k, 另一个线程反复重写 k", rounds);

        service.putObject("k", content, null);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<?> writer = pool.submit(() -> {
            for (int i = 0; i < rounds; i++) {
                try {
                    service.putObject("k", content, null);
                } catch (Exception ignored) {
                }
            }
        });
        Future<?> deleter = pool.submit(() -> {
            for (int i = 0; i < rounds; i++) {
                try {
                    service.deleteObject("k");
                } catch (Exception ignored) {
                }
            }
        });
        writer.get();
        deleter.get();
        pool.shutdown();

        // 交错结束后做一次确定的写入，验证存储仍处于一致状态
        service.putObject("final", content, null);
        int refCount = chunkStore.refCount(hash);
        Map<String, ObjectMeta> objects = service.listObjects();
        long liveRefs = objects.values().stream().filter(m -> m.contentHash().equals(hash)).count();
        log.info("[判定] 交错结束后 refCount={} 与存活引用数={} 必须一致, usedBytes={}",
                refCount, liveRefs, chunkStore.usedBytes());
        assertEquals(liveRefs, refCount, "引用计数必须与存活引用一致：无丢失、无泄漏");
        assertTrue(chunkStore.verify(hash), "存活内容必须完整可读");
        assertArrayEquals(content, service.getObject("final"));
    }
}
