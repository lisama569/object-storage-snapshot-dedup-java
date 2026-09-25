package com.github.lisama569.objectstoragesnapshotdedupjava.tests;

import com.github.lisama569.objectstoragesnapshotdedupjava.exception.ErrorCode;
import com.github.lisama569.objectstoragesnapshotdedupjava.exception.StorageException;
import com.github.lisama569.objectstoragesnapshotdedupjava.service.ObjectStorageService;
import com.github.lisama569.objectstoragesnapshotdedupjava.service.SnapshotService;
import com.github.lisama569.objectstoragesnapshotdedupjava.store.ContentStore;
import org.junit.jupiter.api.BeforeEach;
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
 * 快照隔离与原子恢复（含失败回退）。
 */
class SnapshotServiceTest {

    private static final Logger log = LoggerFactory.getLogger(SnapshotServiceTest.class);

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

    @Test
    void snapshotIsIsolatedFromLaterOverwriteAddAndDelete() {
        storage.put("keep", "v1".getBytes(StandardCharsets.UTF_8));
        storage.put("gone", "to-be-deleted".getBytes(StandardCharsets.UTF_8));
        snapshots.createSnapshot("snap1");
        log.info("[test] snapshot snap1 created with keep=v1, gone=to-be-deleted");

        // 快照后：覆盖、删除、新增
        storage.put("keep", "v2".getBytes(StandardCharsets.UTF_8));
        storage.delete("gone");
        storage.put("fresh", "new".getBytes(StandardCharsets.UTF_8));

        assertArrayEquals("v1".getBytes(StandardCharsets.UTF_8),
                snapshots.readFromSnapshot("snap1", "keep"), "快照内容不得被覆盖写改变");
        assertArrayEquals("to-be-deleted".getBytes(StandardCharsets.UTF_8),
                snapshots.readFromSnapshot("snap1", "gone"), "快照内容不得被删除改变");
        StorageException e = assertThrows(StorageException.class,
                () -> snapshots.readFromSnapshot("snap1", "fresh"));
        assertEquals(ErrorCode.OBJECT_NOT_FOUND, e.getErrorCode(), "快照后新增的对象不得出现在快照中");
        log.info("[test] snapshot isolation verified (判定依据: 覆盖/删除/新增均不影响快照可读内容)");
    }

    @Test
    void restoreReturnsToSnapshotStateAtomically() {
        storage.put("a", "a1".getBytes(StandardCharsets.UTF_8));
        storage.put("b", "b1".getBytes(StandardCharsets.UTF_8));
        snapshots.createSnapshot("snap");

        storage.put("a", "a2".getBytes(StandardCharsets.UTF_8));
        storage.delete("b");
        storage.put("c", "c1".getBytes(StandardCharsets.UTF_8));
        log.info("[test] mutated current state, now restoring snap");

        snapshots.restore("snap");

        assertArrayEquals("a1".getBytes(StandardCharsets.UTF_8), storage.get("a"));
        assertArrayEquals("b1".getBytes(StandardCharsets.UTF_8), storage.get("b"));
        StorageException e = assertThrows(StorageException.class, () -> storage.get("c"));
        assertEquals(ErrorCode.OBJECT_NOT_FOUND, e.getErrorCode(), "恢复后快照外对象不得残留");
        assertEquals(2, storage.list().size());
        log.info("[test] restore ok: current state == snapshot state (判定依据: 逐对象比对)");
    }

    @Test
    void failedRestoreLeavesNoPartialState() throws IOException {
        storage.put("x", "x-content".getBytes(StandardCharsets.UTF_8));
        storage.put("y", "y-content".getBytes(StandardCharsets.UTF_8));
        snapshots.createSnapshot("snap");
        String xHash = storage.meta("x").contentHash();

        // 当前状态演进
        storage.put("x", "x-v2".getBytes(StandardCharsets.UTF_8));
        storage.put("z", "z-content".getBytes(StandardCharsets.UTF_8));

        // 模拟快照引用的内容损坏（直接篡改后端文件）
        Files.write(contentStore.contentPath(xHash), "tampered".getBytes(StandardCharsets.UTF_8));
        log.info("[test] corrupted blob of hash={} to force restore failure", xHash);

        StorageException e = assertThrows(StorageException.class, () -> snapshots.restore("snap"));
        assertEquals(ErrorCode.RESTORE_FAILED, e.getErrorCode(), "恢复失败原因须可区分");
        log.info("[test] errorCode={} message={}", e.getErrorCode(), e.getMessage());

        // 回退验证：当前状态必须保持恢复前原样，无部分恢复
        assertArrayEquals("x-v2".getBytes(StandardCharsets.UTF_8), storage.get("x"),
                "恢复失败后当前对象不得被改动");
        assertArrayEquals("z-content".getBytes(StandardCharsets.UTF_8), storage.get("z"),
                "恢复失败后新增对象不得被删除");
        assertEquals(3, storage.list().size(), "恢复失败后对象集合不得变化（x、y、z 保持原样）");
        log.info("[test] rollback verified: no partial restore (判定依据: 恢复前后当前状态完全一致)");
    }

    @Test
    void missingSnapshotIsDistinguishable() {
        StorageException e = assertThrows(StorageException.class, () -> snapshots.restore("nope"));
        assertEquals(ErrorCode.SNAPSHOT_NOT_FOUND, e.getErrorCode());
        log.info("[test] errorCode={} (判定依据: 快照不存在)", e.getErrorCode());
    }
}
