package com.github.lisama569.objectstoragesnapshotdedupjava.storage;

import com.github.lisama569.objectstoragesnapshotdedupjava.config.StorageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内容寻址块存储：按内容 SHA-256 去重，维护引用计数与容量。
 *
 * 规则：
 * - 相同内容只落盘一次，put 会为调用方持有一个引用；
 * - 引用计数归零时回收底层文件并释放容量；
 * - 读取时重新计算哈希校验完整性，损坏抛 CONTENT_CORRUPTED；
 * - 新唯一内容写入前检查容量，超限抛 CAPACITY_EXCEEDED 且不产生半写入状态。
 */
@Component
public class ChunkStore {

    private static final Logger log = LoggerFactory.getLogger(ChunkStore.class);

    private final StorageProperties properties;
    private final Map<String, Integer> refCounts = new ConcurrentHashMap<>();
    private final Map<String, Long> sizes = new ConcurrentHashMap<>();
    private volatile long usedBytes;
    private Path chunkDir;

    public ChunkStore(StorageProperties properties) {
        this.properties = properties;
    }

    private synchronized Path chunkDir() {
        if (chunkDir == null) {
            chunkDir = Path.of(properties.getRootDir(), "chunks");
            try {
                Files.createDirectories(chunkDir);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return chunkDir;
    }

    /**
     * 写入内容（按哈希去重），返回内容哈希，并为调用方持有一个引用。
     * 超容量抛 CAPACITY_EXCEEDED，且不留半写入状态。
     */
    public synchronized String put(byte[] content) {
        String hash = sha256(content);
        if (refCounts.containsKey(hash)) {
            int before = refCounts.get(hash);
            refCounts.put(hash, before + 1);
            log.info("[ChunkStore.put] dedup hit hash={} refCount {} -> {}", hash, before, before + 1);
            return hash;
        }
        if (usedBytes + content.length > properties.getCapacityBytes()) {
            log.info("[ChunkStore.put] CAPACITY_EXCEEDED hash={} need={} used={} capacity={}",
                    hash, content.length, usedBytes, properties.getCapacityBytes());
            throw new StorageException(ErrorCode.CAPACITY_EXCEEDED,
                    "capacity exceeded: need " + content.length + " bytes, used "
                            + usedBytes + "/" + properties.getCapacityBytes());
        }
        // 先写临时文件再原子移动，避免半写入状态
        Path tmp;
        try {
            tmp = Files.createTempFile(chunkDir(), "tmp-", ".part");
            Files.write(tmp, content);
            Files.move(tmp, chunkDir().resolve(hash), StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        refCounts.put(hash, 1);
        sizes.put(hash, (long) content.length);
        usedBytes += content.length;
        log.info("[ChunkStore.put] new chunk hash={} size={} refCount 0 -> 1 usedBytes={}",
                hash, content.length, usedBytes);
        return hash;
    }

    /**
     * 读取内容并校验完整性，损坏抛 CONTENT_CORRUPTED，不存在抛 NOT_FOUND。
     */
    public byte[] get(String contentHash) {
        Path file = chunkDir().resolve(contentHash);
        byte[] content;
        try {
            content = Files.readAllBytes(file);
        } catch (IOException e) {
            throw new StorageException(ErrorCode.NOT_FOUND, "chunk not found: " + contentHash);
        }
        String actual = sha256(content);
        if (!actual.equals(contentHash)) {
            log.info("[ChunkStore.get] CONTENT_CORRUPTED expected={} actual={}", contentHash, actual);
            throw new StorageException(ErrorCode.CONTENT_CORRUPTED,
                    "content corrupted: expected hash " + contentHash + " but actual " + actual);
        }
        return content;
    }

    /** 增加引用计数（快照持有引用时使用） */
    public synchronized void retain(String contentHash) {
        Integer before = refCounts.get(contentHash);
        if (before == null) {
            throw new StorageException(ErrorCode.NOT_FOUND, "chunk not found: " + contentHash);
        }
        refCounts.put(contentHash, before + 1);
        log.info("[ChunkStore.retain] hash={} refCount {} -> {}", contentHash, before, before + 1);
    }

    /** 减少引用计数，归零时回收底层数据并释放容量 */
    public synchronized void release(String contentHash) {
        Integer before = refCounts.get(contentHash);
        if (before == null) {
            throw new StorageException(ErrorCode.NOT_FOUND, "chunk not found: " + contentHash);
        }
        if (before == 1) {
            refCounts.remove(contentHash);
            Long size = sizes.remove(contentHash);
            usedBytes -= size == null ? 0 : size;
            try {
                Files.deleteIfExists(chunkDir().resolve(contentHash));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            log.info("[ChunkStore.release] hash={} refCount 1 -> 0, chunk reclaimed, usedBytes={}",
                    contentHash, usedBytes);
        } else {
            refCounts.put(contentHash, before - 1);
            log.info("[ChunkStore.release] hash={} refCount {} -> {}", contentHash, before, before - 1);
        }
    }

    /** 当前引用计数（测试与日志用） */
    public int refCount(String contentHash) {
        return refCounts.getOrDefault(contentHash, 0);
    }

    /** 当前已用容量（唯一内容字节数） */
    public long usedBytes() {
        return usedBytes;
    }

    /** 校验指定内容是否完整存在（恢复前置校验用），不抛异常 */
    public boolean verify(String contentHash) {
        Path file = chunkDir().resolve(contentHash);
        if (!Files.exists(file)) {
            return false;
        }
        try {
            return sha256(Files.readAllBytes(file)).equals(contentHash);
        } catch (IOException e) {
            return false;
        }
    }

    /** 计算内容 SHA-256（测试与日志判定依据） */
    public static String sha256(byte[] content) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
