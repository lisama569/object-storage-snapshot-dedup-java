package com.github.lisama569.objectstoragesnapshotdedupjava.exception;

/**
 * 存储层统一异常，携带可区分的错误码。
 */
public class StorageException extends RuntimeException {

    private final ErrorCode errorCode;

    public StorageException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public StorageException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }
}
