package com.github.lisama569.objectstoragesnapshotdedupjava.store;

import com.github.lisama569.objectstoragesnapshotdedupjava.exception.ErrorCode;
import com.github.lisama569.objectstoragesnapshotdedupjava.exception.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于本地文件系统的内容寻址存储（模拟对象后端）。
 *
 * 去重与引用计数规则：
 *  - 内容以 SHA-256 哈希命名落盘，内容相同的写入复用同一份数据；
 *  - store() 使引用计数 +1（数据不存在时先落盘），release() 使引用计数 -1，归零时回收底层文件；
 *  - 所有引用计数与文件操作在同一监视器下完成，保证并发写/删交错时不出现
 *    重复占用、引用丢失或提前回收。
 */
@Component
public class ContentStore {

    private static final Logger log = LoggerFactory.getLogger(ContentStore.class);

    private final Path rootDir;
    private final long maxBytes;
    private final Map<String, Integer> refCounts = new ConcurrentHashMap<>();
    private long usedBytes;

    public ContentStore(@Value("${storage.root:data/content}") String rootDir,
                        @Value("${storage.capacity-bytes:67108864}") long maxBytes) {
        this.rootDir = Paths.get(rootDir);
        this.maxBytes = maxBytes;
        try {
            Files.createDirectories(this.rootDir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static String sha256Hex(byte[] content) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(content);
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * 写入内容并按内容去重；返回内容哈希。引用计数 +1。
     * 通过临时文件 + 原子移动避免半写入状态。
     */
    public synchronized String store(byte[] content) {
        String hash = sha256Hex(content);
        Path target = contentPath(hash);
        if (!Files.exists(target)) {
            if (usedBytes + content.length > maxBytes) {
                log.info("[ContentStore.store] hash={} size={} used={} max={} -> CAPACITY_EXCEEDED",
                        hash, content.length, usedBytes, maxBytes);
                throw new StorageException(ErrorCode.CAPACITY_EXCEEDED,
                        "capacity exceeded: used=" + usedBytes + " need=" + content.length + " max=" + maxBytes);
            }
            Path tmp;
            try {
                tmp = Files.createTempFile(rootDir, "tmp-", ".part");
                Files.write(tmp, content);
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                throw new UncheckedIOException("failed to store content " + hash, e);
            }
            usedBytes += content.length;
            log.info("[ContentStore.store] hash={} stored new blob size={} used={}", hash, content.length, usedBytes);
        } else {
            log.info("[ContentStore.store] hash={} dedup hit, reuse existing blob", hash);
        }
        int rc = refCounts.merge(hash, 1, Integer::sum);
        log.info("[ContentStore.store] hash={} refCount -> {}", hash, rc);
        return hash;
    }

    /**
     * 读取内容并校验完整性（重算哈希比对），损坏时抛出 CONTENT_CORRUPTED。
     */
    public byte[] read(String contentHash) {
        Path path = contentPath(contentHash);
        if (!Files.exists(path)) {
            throw new StorageException(ErrorCode.CONTENT_CORRUPTED,
                    "content missing for hash " + contentHash);
        }
        byte[] data;
        try {
            data = Files.readAllBytes(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        String actual = sha256Hex(data);
        if (!actual.equals(contentHash)) {
            log.info("[ContentStore.read] hash={} actual={} -> CONTENT_CORRUPTED", contentHash, actual);
            throw new StorageException(ErrorCode.CONTENT_CORRUPTED,
                    "content corrupted: expected=" + contentHash + " actual=" + actual);
        }
        return data;
    }

    /** 增加引用计数（快照引用）。 */
    public synchronized void addRef(String contentHash) {
        if (!Files.exists(contentPath(contentHash))) {
            throw new StorageException(ErrorCode.CONTENT_CORRUPTED,
                    "cannot addRef, content missing for hash " + contentHash);
        }
        int rc = refCounts.merge(contentHash, 1, Integer::sum);
        log.info("[ContentStore.addRef] hash={} refCount -> {}", contentHash, rc);
    }

    /** 减少引用计数；归零时回收底层数据。返回本次实际回收的字节数（未归零则为 0）。 */
    public synchronized long release(String contentHash) {
        Integer rc = refCounts.get(contentHash);
        if (rc == null || rc <= 0) {
            throw new StorageException(ErrorCode.CONTENT_CORRUPTED,
                    "refCount underflow for hash " + contentHash);
        }
        if (rc == 1) {
            refCounts.remove(contentHash);
            Path path = contentPath(contentHash);
            try {
                long size = Files.exists(path) ? Files.size(path) : 0;
                Files.deleteIfExists(path);
                usedBytes -= size;
                log.info("[ContentStore.release] hash={} refCount -> 0, blob reclaimed, used={}", contentHash, usedBytes);
                return size;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            refCounts.put(contentHash, rc - 1);
            log.info("[ContentStore.release] hash={} refCount -> {}", contentHash, rc - 1);
            return 0;
        }
    }

    public synchronized int refCount(String contentHash) {
        return refCounts.getOrDefault(contentHash, 0);
    }

    public boolean exists(String contentHash) {
        return Files.exists(contentPath(contentHash));
    }

    /** 内容文件路径（测试用于模拟损坏）。 */
    public Path contentPath(String contentHash) {
        return rootDir.resolve(contentHash + ".blob");
    }

    public synchronized long usedBytes() {
        return usedBytes;
    }

    /**
     * 物理状态的一致视图：已用容量与每个现存 blob 的物理大小。
     * 与引用计数变更在同一监视器下获取，统计口径以磁盘上真实落盘的数据为准
     * （遍历 {@code *.blob}），保证“统计 == 磁盘实际”。
     */
    public synchronized PhysicalView physicalView() {
        Map<String, Long> sizes = new HashMap<>();
        long total = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(rootDir, "*.blob")) {
            for (Path p : stream) {
                long size = Files.size(p);
                String fileName = p.getFileName().toString();
                String hash = fileName.substring(0, fileName.length() - ".blob".length());
                sizes.put(hash, size);
                total += size;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        // 以磁盘真实总量为准（正常情况下与 usedBytes 一致）。
        return new PhysicalView(total, sizes);
    }
}
