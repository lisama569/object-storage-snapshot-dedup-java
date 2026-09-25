package com.github.lisama569.objectstoragesnapshotdedupjava.tests;

import com.github.lisama569.objectstoragesnapshotdedupjava.config.StorageProperties;
import com.github.lisama569.objectstoragesnapshotdedupjava.storage.ChunkStore;
import com.github.lisama569.objectstoragesnapshotdedupjava.storage.ErrorCode;
import com.github.lisama569.objectstoragesnapshotdedupjava.storage.ObjectStorageService;
import com.github.lisama569.objectstoragesnapshotdedupjava.storage.StorageException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 快照隔离与恢复：快照与当前数据隔离共存；恢复具备原子性，失败回退不留部分结果。
 */
class SnapshotIsolationTest {

    private static final Logger log = LoggerFactory.getLogger(SnapshotIsolationTest.class);

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

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void snapshotIsIsolatedFromOverwriteAddAndDelete() {
        log.info("[输入] 写入 a=1, b=2 后创建快照 snap1，随后覆盖 a=1'、删除 b、新增 c=3");
        service.putObject("a", bytes("1"), null);
        service.putObject("b", bytes("2"), null);
        service.createSnapshot("snap1");
        String hashA = ChunkStore.sha256(bytes("1"));
        String hashB = ChunkStore.sha256(bytes("2"));
        log.info("[引用] 快照后 hashA refCount={} (期望 2: 当前+快照), hashB refCount={} (期望 2)",
                chunkStore.refCount(hashA), chunkStore.refCount(hashB));
        assertEquals(2, chunkStore.refCount(hashA));
        assertEquals(2, chunkStore.refCount(hashB));

        // 覆盖、删除、新增：均不得影响快照
        service.putObject("a", bytes("1'"), null);
        service.deleteObject("b");
        service.putObject("c", bytes("3"), null);
        log.info("[引用] 当前删除 b 后 hashB refCount={} (期望 1: 仅快照持有，不得提前回收)",
                chunkStore.refCount(hashB));
        assertEquals(1, chunkStore.refCount(hashB), "快照持有的引用不得被当前删除回收");

        assertArrayEquals(bytes("1"), service.getSnapshotObject("snap1", "a"), "快照中的 a 必须保持旧内容");
        assertArrayEquals(bytes("2"), service.getSnapshotObject("snap1", "b"), "快照中的 b 必须仍然可读");
        StorageException e = assertThrows(StorageException.class,
                () -> service.getSnapshotObject("snap1", "c"));
        log.info("[判定] 读取快照中不存在的 c -> code={} (期望 NOT_FOUND)", e.getCode());
        assertEquals(ErrorCode.NOT_FOUND, e.getCode());
    }

    @Test
    void restoreReturnsCurrentNamespaceToSnapshotState() {
        log.info("[输入] a=1, b=2 建快照后覆盖 a、删除 b、新增 c，然后恢复");
        service.putObject("a", bytes("1"), null);
        service.putObject("b", bytes("2"), null);
        service.createSnapshot("snap1");
        service.putObject("a", bytes("1'"), null);
        service.deleteObject("b");
        service.putObject("c", bytes("3"), null);

        service.restoreSnapshot("snap1");
        log.info("[判定] 恢复后当前命名空间 keys={} (期望仅 a,b)", service.listObjects().keySet());
        assertArrayEquals(bytes("1"), service.getObject("a"), "恢复后 a 必须回到快照内容");
        assertArrayEquals(bytes("2"), service.getObject("b"), "恢复后 b 必须重新可读");
        StorageException e = assertThrows(StorageException.class, () -> service.getObject("c"));
        assertEquals(ErrorCode.NOT_FOUND, e.getCode(), "恢复后快照之外的新增对象必须消失");
        // 恢复后快照本身仍然保留，可再次恢复
        service.putObject("a", bytes("changed-again"), null);
        service.restoreSnapshot("snap1");
        assertArrayEquals(bytes("1"), service.getObject("a"));
    }

    @Test
    void failedRestoreRollsBackWithoutPartialResults() throws Exception {
        log.info("[输入] 写入 only=old 后建快照，删除当前对象使 chunk 仅被快照持有，再损坏该 chunk 文件");
        service.putObject("only", bytes("old"), null);
        service.createSnapshot("snap1");
        String hash = ChunkStore.sha256(bytes("old"));
        service.deleteObject("only");
        log.info("[引用] 当前删除后 hash refCount={} (期望 1: 仅快照持有)", chunkStore.refCount(hash));
        assertEquals(1, chunkStore.refCount(hash));

        // 写入一个恢复前存在的当前对象，用于验证回退后当前数据不变
        service.putObject("keep", bytes("keep-me"), null);

        // 损坏快照唯一持有的 chunk 文件
        Path chunkFile = tempDir.resolve("chunks").resolve(hash);
        Files.write(chunkFile, bytes("tampered!"));
        log.info("[输入] 已篡改 chunk 文件 {}, 触发恢复", chunkFile.getFileName());

        StorageException e = assertThrows(StorageException.class, () -> service.restoreSnapshot("snap1"));
        log.info("[判定] 恢复失败 code={} (期望 RESTORE_FAILED), message={}", e.getCode(), e.getMessage());
        assertEquals(ErrorCode.RESTORE_FAILED, e.getCode());

        // 回退验证：当前命名空间与引用计数完全未变
        assertTrue(service.listObjects().containsKey("keep"), "失败恢复不得影响现存对象");
        assertEquals(1, service.listObjects().size(), "失败恢复不得留下部分恢复的对象");
        assertArrayEquals(bytes("keep-me"), service.getObject("keep"));
        log.info("[判定] 回退后 refCount(hash)={} (期望 1), 当前对象数={} (期望 1)",
                chunkStore.refCount(hash), service.listObjects().size());
        assertEquals(1, chunkStore.refCount(hash), "失败恢复不得改变引用计数");
    }
}
