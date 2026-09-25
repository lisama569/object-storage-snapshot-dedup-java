package com.github.lisama569.objectstoragesnapshotdedupjava.api;

import com.github.lisama569.objectstoragesnapshotdedupjava.storage.ErrorCode;

/**
 * 统一错误响应：携带可区分的错误码与说明。
 */
public record ErrorResponse(ErrorCode code, String message) {
}
