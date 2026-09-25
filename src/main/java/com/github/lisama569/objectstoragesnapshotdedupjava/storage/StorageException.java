package com.github.lisama569.objectstoragesnapshotdedupjava.storage;

/**
 * 携带可区分错误码的存储异常。
 */
public class StorageException extends RuntimeException {

    private final ErrorCode code;

    public StorageException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public ErrorCode getCode() {
        return code;
    }
}
