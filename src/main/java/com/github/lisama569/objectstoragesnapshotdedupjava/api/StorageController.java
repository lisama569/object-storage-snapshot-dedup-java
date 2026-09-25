package com.github.lisama569.objectstoragesnapshotdedupjava.api;

import com.github.lisama569.objectstoragesnapshotdedupjava.storage.ErrorCode;
import com.github.lisama569.objectstoragesnapshotdedupjava.storage.ObjectMeta;
import com.github.lisama569.objectstoragesnapshotdedupjava.storage.ObjectStorageService;
import com.github.lisama569.objectstoragesnapshotdedupjava.storage.StorageException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 对象存储 REST 入口：对象读写删、快照与恢复。
 * 所有失败均以 {code, message} 形式返回，错误码可区分。
 */
@RestController
public class StorageController {

    private final ObjectStorageService storageService;

    public StorageController(ObjectStorageService storageService) {
        this.storageService = storageService;
    }

    /** 写入对象；可选 expectedVersion 做乐观锁校验 */
    @PutMapping("/objects/{key}")
    public ObjectMeta putObject(@PathVariable String key,
                                @RequestBody byte[] content,
                                @RequestParam(required = false) Long expectedVersion) {
        return storageService.putObject(key, content, expectedVersion);
    }

    @GetMapping("/objects/{key}")
    public byte[] getObject(@PathVariable String key) {
        return storageService.getObject(key);
    }

    @DeleteMapping("/objects/{key}")
    public Map<String, String> deleteObject(@PathVariable String key) {
        storageService.deleteObject(key);
        return Map.of("status", "deleted", "key", key);
    }

    @GetMapping("/objects")
    public Map<String, ObjectMeta> listObjects() {
        return storageService.listObjects();
    }

    @PostMapping("/snapshots/{name}")
    public Map<String, String> createSnapshot(@PathVariable String name) {
        storageService.createSnapshot(name);
        return Map.of("status", "created", "snapshot", name);
    }

    @PostMapping("/snapshots/{name}/restore")
    public Map<String, String> restoreSnapshot(@PathVariable String name) {
        storageService.restoreSnapshot(name);
        return Map.of("status", "restored", "snapshot", name);
    }

    @DeleteMapping("/snapshots/{name}")
    public Map<String, String> deleteSnapshot(@PathVariable String name) {
        storageService.deleteSnapshot(name);
        return Map.of("status", "deleted", "snapshot", name);
    }

    @GetMapping("/snapshots")
    public List<String> listSnapshots() {
        return storageService.listSnapshots();
    }

    @GetMapping("/snapshots/{name}/objects/{key}")
    public byte[] getSnapshotObject(@PathVariable String name, @PathVariable String key) {
        return storageService.getSnapshotObject(name, key);
    }

    /** 统一异常映射：不同错误码对应不同 HTTP 状态，原因可区分 */
    @ExceptionHandler(StorageException.class)
    public ResponseEntity<ErrorResponse> handleStorageException(StorageException e) {
        HttpStatus status = switch (e.getCode()) {
            case NOT_FOUND -> HttpStatus.NOT_FOUND;
            case CONTENT_CORRUPTED -> HttpStatus.UNPROCESSABLE_ENTITY;
            case CAPACITY_EXCEEDED -> HttpStatus.INSUFFICIENT_STORAGE;
            case CONCURRENT_CONFLICT -> HttpStatus.CONFLICT;
            case RESTORE_FAILED -> HttpStatus.UNPROCESSABLE_ENTITY;
        };
        return ResponseEntity.status(status).body(new ErrorResponse(e.getCode(), e.getMessage()));
    }
}
