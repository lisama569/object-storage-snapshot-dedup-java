package com.github.lisama569.objectstoragesnapshotdedupjava.web;

import com.github.lisama569.objectstoragesnapshotdedupjava.exception.ErrorCode;
import com.github.lisama569.objectstoragesnapshotdedupjava.exception.StorageException;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.ObjectMeta;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.RetentionPolicy;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.SnapshotCreateResult;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.SnapshotDeleteResult;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.SnapshotInfo;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.SpaceStats;
import com.github.lisama569.objectstoragesnapshotdedupjava.service.ObjectStorageService;
import com.github.lisama569.objectstoragesnapshotdedupjava.service.SnapshotService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST 入口：对象读写删、快照生命周期（创建/删除/列表/恢复）、容量统计。
 * 错误码映射为可区分的 HTTP 状态。
 */
@RestController
@RequestMapping("/api")
public class StorageController {

    private final ObjectStorageService objectStorage;
    private final SnapshotService snapshotService;

    public StorageController(ObjectStorageService objectStorage, SnapshotService snapshotService) {
        this.objectStorage = objectStorage;
        this.snapshotService = snapshotService;
    }

    @PutMapping("/objects/{name}")
    public ObjectMeta put(@PathVariable String name,
                          @RequestBody byte[] content,
                          @RequestParam(required = false) Long expectedVersion) {
        return expectedVersion == null
                ? objectStorage.put(name, content)
                : objectStorage.put(name, content, expectedVersion);
    }

    @GetMapping("/objects/{name}")
    public byte[] get(@PathVariable String name) {
        return objectStorage.get(name);
    }

    @DeleteMapping("/objects/{name}")
    public void delete(@PathVariable String name) {
        objectStorage.delete(name);
    }

    @GetMapping("/objects")
    public List<ObjectMeta> list() {
        return objectStorage.list();
    }

    @PostMapping("/snapshots/{name}")
    public SnapshotCreateResult createSnapshot(@PathVariable String name,
                                               @RequestParam(required = false) Integer maxCount,
                                               @RequestParam(required = false) Long maxAgeMillis) {
        return snapshotService.createSnapshot(name, new RetentionPolicy(maxCount, maxAgeMillis));
    }

    @DeleteMapping("/snapshots/{name}")
    public SnapshotDeleteResult deleteSnapshot(@PathVariable String name) {
        return snapshotService.deleteSnapshot(name);
    }

    @GetMapping("/snapshots")
    public List<SnapshotInfo> listSnapshots() {
        return snapshotService.listSnapshotDetails();
    }

    @GetMapping("/stats")
    public SpaceStats stats() {
        return snapshotService.stats();
    }

    @GetMapping("/snapshots/{name}/objects/{object}")
    public byte[] readFromSnapshot(@PathVariable String name, @PathVariable String object) {
        return snapshotService.readFromSnapshot(name, object);
    }

    @PostMapping("/snapshots/{name}/restore")
    public void restore(@PathVariable String name) {
        snapshotService.restore(name);
    }

    @ExceptionHandler(StorageException.class)
    public ResponseEntity<Map<String, String>> handle(StorageException e) {
        HttpStatus status = switch (e.getErrorCode()) {
            case CONTENT_CORRUPTED -> HttpStatus.UNPROCESSABLE_ENTITY;
            case CONCURRENT_CONFLICT -> HttpStatus.CONFLICT;
            case CAPACITY_EXCEEDED -> HttpStatus.INSUFFICIENT_STORAGE;
            case OBJECT_NOT_FOUND, SNAPSHOT_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case RESTORE_FAILED -> HttpStatus.INTERNAL_SERVER_ERROR;
            case INVALID_RETENTION -> HttpStatus.BAD_REQUEST;
        };
        return ResponseEntity.status(status)
                .body(Map.of("errorCode", e.getErrorCode().name(), "message", e.getMessage()));
    }
}
