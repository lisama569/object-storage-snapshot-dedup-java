package com.github.lisama569.objectstoragesnapshotdedupjava.tests;

import com.github.lisama569.objectstoragesnapshotdedupjava.exception.ErrorCode;
import com.github.lisama569.objectstoragesnapshotdedupjava.exception.StorageException;
import com.github.lisama569.objectstoragesnapshotdedupjava.service.ObjectStorageService;
import com.github.lisama569.objectstoragesnapshotdedupjava.store.ContentStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 完整性校验与拒绝语义：内容损坏、容量超限、失败不留半写入状态。
 */
class IntegrityAndCapacityTest {

    private static final Logger log = LoggerFactory.getLogger(IntegrityAndCapacityTest.class);

    @TempDir
    Path tempDir;

    @Test
    void corruptedContentIsRejectedWithDistinguishableCode() throws IOException {
        ContentStore contentStore = new ContentStore(tempDir.toString(), 1_000_000);
        ObjectStorageService storage = new ObjectStorageService(contentStore);

        storage.put("doc", "original".getBytes(StandardCharsets.UTF_8));
        String hash = storage.meta("doc").contentHash();
        log.info("[test] input: corrupt blob file of hash={}", hash);

        // 模拟后端数据损坏
        Files.write(contentStore.contentPath(hash), "tampered-bytes".getBytes(StandardCharsets.UTF_8));

        StorageException e = assertThrows(StorageException.class, () -> storage.get("doc"));
        assertEquals(ErrorCode.CONTENT_CORRUPTED, e.getErrorCode(), "内容损坏须可区分，不得静默返回错误数据");
        log.info("[test] errorCode={} message={} (判定依据: 重算哈希与元数据不一致)", e.getErrorCode(), e.getMessage());
    }

    @Test
    void missingBlobIsReportedAsCorruptionNotWrongData() throws IOException {
        ContentStore contentStore = new ContentStore(tempDir.toString(), 1_000_000);
        ObjectStorageService storage = new ObjectStorageService(contentStore);

        storage.put("doc", "payload".getBytes(StandardCharsets.UTF_8));
        String hash = storage.meta("doc").contentHash();
        Files.delete(contentStore.contentPath(hash));
        log.info("[test] input: deleted blob file of hash={} to simulate backend loss", hash);

        StorageException e = assertThrows(StorageException.class, () -> storage.get("doc"));
        assertEquals(ErrorCode.CONTENT_CORRUPTED, e.getErrorCode());
        log.info("[test] errorCode={} (判定依据: 元数据引用的内容缺失)", e.getErrorCode());
    }

    @Test
    void capacityExceededIsRejectedAndLeavesNoPartialWrite() {
        ContentStore contentStore = new ContentStore(tempDir.toString(), 100);
        ObjectStorageService storage = new ObjectStorageService(contentStore);

        byte[] first = new byte[80];
        byte[] second = new byte[80];
        java.util.Arrays.fill(second, (byte) 1); // 与 first 内容不同，避免去重命中
        storage.put("big1", first);
        log.info("[test] input: capacity=100, used=80, now write another 80 bytes");

        StorageException e = assertThrows(StorageException.class, () -> storage.put("big2", second));
        assertEquals(ErrorCode.CAPACITY_EXCEEDED, e.getErrorCode(), "超限须可区分");
        log.info("[test] errorCode={} message={}", e.getErrorCode(), e.getMessage());

        // 不留半写入状态：big2 不存在、无临时文件残留、已用容量未变
        StorageException notFound = assertThrows(StorageException.class, () -> storage.meta("big2"));
        assertEquals(ErrorCode.OBJECT_NOT_FOUND, notFound.getErrorCode());
        assertEquals(80, contentStore.usedBytes(), "超限写入不得占用容量");
        assertEquals(0, contentStore.refCount(ContentStore.sha256Hex(second)),
                "超限写入不得残留引用计数");
        assertArrayEquals(first, storage.get("big1"), "已有对象不得受影响");
        log.info("[test] no partial state: usedBytes=80, big2 absent (判定依据: 容量与索引均未变化)");
    }

    @Test
    void dedupWriteDoesNotConsumeExtraCapacity() {
        ContentStore contentStore = new ContentStore(tempDir.toString(), 100);
        ObjectStorageService storage = new ObjectStorageService(contentStore);

        byte[] content = new byte[90];
        storage.put("a", content);
        storage.put("b", content); // 去重命中，不占用新容量
        log.info("[test] input: capacity=100, two identical 90-byte writes");

        assertEquals(90, contentStore.usedBytes(), "去重命中不得重复占用容量");
        assertEquals(2, contentStore.refCount(ContentStore.sha256Hex(content)));
        log.info("[test] usedBytes=90 refCount=2 (判定依据: 内容寻址去重)");
    }
}
