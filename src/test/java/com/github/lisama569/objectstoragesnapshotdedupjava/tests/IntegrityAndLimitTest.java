package com.github.lisama569.objectstoragesnapshotdedupjava.tests;

import com.github.lisama569.objectstoragesnapshotdedupjava.config.StorageProperties;
import com.github.lisama569.objectstoragesnapshotdedupjava.storage.ChunkStore;
import com.github.lisama569.objectstoragesnapshotdedupjava.storage.ErrorCode;
import com.github.lisama569.objectstoragesnapshotdedupjava.storage.ObjectMeta;
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
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 完整性与拒绝语义：内容损坏、容量超限、并发冲突、对象缺失分别给出可区分的失败原因，
 * 且失败不得留下半写入状态。
 */
class IntegrityAndLimitTest {

    private static final Logger log = LoggerFactory.getLogger(IntegrityAndLimitTest.class);

    @TempDir
    Path tempDir;

    private StorageProperties props;
    private ChunkStore chunkStore;
    private ObjectStorageService service;

    @BeforeEach
    void setUp() {
        props = new StorageProperties();
        props.setRootDir(tempDir.toString());
        props.setCapacityBytes(64); // 小容量便于触发超限
        chunkStore = new ChunkStore(props);
        service = new ObjectStorageService(chunkStore);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void corruptedContentIsRejectedWithDistinctCode() throws Exception {
        service.putObject("doc", bytes("integrity-me"), null);
        String hash = ChunkStore.sha256(bytes("integrity-me"));
        Files.write(tempDir.resolve("chunks").resolve(hash), bytes("tampered!!!"));
        log.info("[输入] 篡改对象 doc 的 chunk 文件后读取");

        StorageException e = assertThrows(StorageException.class, () -> service.getObject("doc"));
        log.info("[判定] 读取损坏内容 -> code={} (期望 CONTENT_CORRUPTED), message={}", e.getCode(), e.getMessage());
        assertEquals(ErrorCode.CONTENT_CORRUPTED, e.getCode(), "损坏必须显式报错，不得静默返回错误数据");
    }

    @Test
    void capacityExceededLeavesNoPartialState() {
        byte[] first = new byte[40];
        byte[] second = new byte[40]; // 40+40 > 64，第二笔必被拒绝
        java.util.Arrays.fill(second, (byte) 1); // 与 first 内容不同，避免被去重
        log.info("[输入] 容量 64 字节, 先写 40 字节, 再写另一份 40 字节唯一内容");

        service.putObject("first", first, null);
        long usedAfterFirst = chunkStore.usedBytes();
        log.info("[引用] 第一笔后 usedBytes={} (期望 40)", usedAfterFirst);
        assertEquals(40, usedAfterFirst);

        StorageException e = assertThrows(StorageException.class,
                () -> service.putObject("second", second, null));
        log.info("[判定] 超限写入 -> code={} (期望 CAPACITY_EXCEEDED), message={}", e.getCode(), e.getMessage());
        assertEquals(ErrorCode.CAPACITY_EXCEEDED, e.getCode());

        log.info("[判定] 失败后 usedBytes={} (期望仍 40), 对象数={} (期望 1), 无半写入状态",
                chunkStore.usedBytes(), service.listObjects().size());
        assertEquals(usedAfterFirst, chunkStore.usedBytes(), "超限失败不得占用容量");
        assertEquals(1, service.listObjects().size(), "超限失败不得留下对象元数据");
        assertThrows(StorageException.class, () -> service.getObject("second"));

        // 去重内容不受容量限制：重复写入已有内容必须成功
        ObjectMeta meta = service.putObject("first-copy", first, null);
        log.info("[判定] 重复内容写入成功 hash={} (去重不占用新容量), usedBytes={}",
                meta.contentHash(), chunkStore.usedBytes());
        assertEquals(usedAfterFirst, chunkStore.usedBytes());
    }

    @Test
    void versionConflictIsDistinctFromOtherFailures() {
        ObjectMeta meta = service.putObject("k", bytes("v1"), null);
        log.info("[输入] 写入 k=v1 (version={}), 随后以错误 expectedVersion=99 覆盖", meta.version());

        StorageException e = assertThrows(StorageException.class,
                () -> service.putObject("k", bytes("v2"), 99L));
        log.info("[判定] 版本冲突 -> code={} (期望 CONCURRENT_CONFLICT), message={}", e.getCode(), e.getMessage());
        assertEquals(ErrorCode.CONCURRENT_CONFLICT, e.getCode());
        assertArrayEquals(bytes("v1"), service.getObject("k"), "冲突失败不得改变现有内容");

        // 正确版本可写入
        ObjectMeta updated = service.putObject("k", bytes("v2"), meta.version());
        log.info("[判定] 正确版本写入成功 version {} -> {}", meta.version(), updated.version());
        assertEquals(meta.version() + 1, updated.version());
        assertArrayEquals(bytes("v2"), service.getObject("k"));
    }

    @Test
    void missingObjectAndSnapshotReportNotFound() {
        StorageException e1 = assertThrows(StorageException.class, () -> service.getObject("nope"));
        StorageException e2 = assertThrows(StorageException.class, () -> service.deleteObject("nope"));
        StorageException e3 = assertThrows(StorageException.class, () -> service.restoreSnapshot("nope"));
        log.info("[判定] 缺失对象/快照 -> codes={}, {}, {} (均期望 NOT_FOUND)", e1.getCode(), e2.getCode(), e3.getCode());
        assertEquals(ErrorCode.NOT_FOUND, e1.getCode());
        assertEquals(ErrorCode.NOT_FOUND, e2.getCode());
        assertEquals(ErrorCode.NOT_FOUND, e3.getCode());
    }

    @Test
    void allFailureCodesAreDistinguishable() throws Exception {
        props.setCapacityBytes(1024);
        Set<ErrorCode> observed = new HashSet<>();

        service.putObject("x", bytes("data"), null);
        // NOT_FOUND
        observed.add(assertThrows(StorageException.class, () -> service.getObject("missing")).getCode());
        // CONCURRENT_CONFLICT
        observed.add(assertThrows(StorageException.class,
                () -> service.putObject("x", bytes("y"), 42L)).getCode());
        // CAPACITY_EXCEEDED
        observed.add(assertThrows(StorageException.class,
                () -> service.putObject("big", new byte[2048], null)).getCode());
        // CONTENT_CORRUPTED
        String hash = ChunkStore.sha256(bytes("data"));
        Files.write(tempDir.resolve("chunks").resolve(hash), bytes("XXXX"));
        observed.add(assertThrows(StorageException.class, () -> service.getObject("x")).getCode());
        // RESTORE_FAILED
        service.createSnapshot("s1");
        observed.add(assertThrows(StorageException.class, () -> service.restoreSnapshot("s1")).getCode());

        log.info("[判定] 收集到的错误码={} (期望覆盖全部 5 种且互不相同)", observed);
        assertEquals(Set.of(ErrorCode.NOT_FOUND, ErrorCode.CONCURRENT_CONFLICT,
                ErrorCode.CAPACITY_EXCEEDED, ErrorCode.CONTENT_CORRUPTED,
                ErrorCode.RESTORE_FAILED), observed);
        assertEquals(5, observed.size(), "五种失败原因必须可区分");
    }
}
