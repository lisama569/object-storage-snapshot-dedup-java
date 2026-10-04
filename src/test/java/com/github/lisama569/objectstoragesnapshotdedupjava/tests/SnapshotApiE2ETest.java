package com.github.lisama569.objectstoragesnapshotdedupjava.tests;

import com.github.lisama569.objectstoragesnapshotdedupjava.model.SnapshotMeta;
import com.github.lisama569.objectstoragesnapshotdedupjava.model.StorageStats;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 通过 HTTP 外部接口端到端验证快照生命周期：删除、统计、保留策略淘汰，
 * 以及失败理由（errorCode + HTTP 状态）的可区分性。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SnapshotApiE2ETest {

    private static final Logger log = LoggerFactory.getLogger(SnapshotApiE2ETest.class);

    private static final Path ROOT = createTempRoot();

    private static Path createTempRoot() {
        try {
            return Files.createTempDirectory("snapshot-e2e-");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("storage.root", ROOT::toString);
        registry.add("storage.capacity-bytes", () -> "1048576");
    }

    @Autowired
    TestRestTemplate http;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private ResponseEntity<Map> putObject(String name, String content) {
        return http.exchange("/api/objects/" + name, HttpMethod.PUT,
                new HttpEntity<>(bytes(content)), Map.class);
    }

    @Test
    void deleteSnapshotAndStatsOverHttp() {
        // 准备：两个对象共享内容（触发去重），再打快照
        assertEquals(HttpStatus.OK, putObject("e2e-a", "e2e-shared-content").getStatusCode());
        assertEquals(HttpStatus.OK, putObject("e2e-b", "e2e-shared-content").getStatusCode());
        assertEquals(HttpStatus.OK, http.postForEntity("/api/snapshots/e2e-s1", null, String.class).getStatusCode());
        log.info("[e2e] input: objects e2e-a/e2e-b (same content), snapshot e2e-s1");

        // 快照列表与空间口径
        ResponseEntity<List<SnapshotMeta>> listResp = http.exchange("/api/snapshots", HttpMethod.GET,
                null, new ParameterizedTypeReference<>() {
                });
        assertEquals(HttpStatus.OK, listResp.getStatusCode());
        SnapshotMeta s1 = listResp.getBody().stream().filter(m -> m.name().equals("e2e-s1")).findFirst().orElseThrow();
        assertEquals("e2e-shared-content".length(), s1.referencedBytes(), "快照引用一份去重后的数据");
        assertEquals(0, s1.exclusiveBytes(), "数据仍被当前对象引用，删除快照预计回收 0");
        log.info("[e2e] GET /api/snapshots -> s1 referenced={} exclusive={} (判定依据: 引用计数口径)",
                s1.referencedBytes(), s1.exclusiveBytes());

        // 整体统计：去重节省须体现
        ResponseEntity<StorageStats> statsResp = http.getForEntity("/api/stats", StorageStats.class);
        assertEquals(HttpStatus.OK, statsResp.getStatusCode());
        assertEquals("e2e-shared-content".length(), statsResp.getBody().dedupSavedBytes(),
                "e2e-b 的重复写入须计入去重节省");
        assertEquals("e2e-shared-content".length(), statsResp.getBody().usedBytes());
        log.info("[e2e] GET /api/stats -> used={} dedupSaved={} snapshots={}",
                statsResp.getBody().usedBytes(), statsResp.getBody().dedupSavedBytes(),
                statsResp.getBody().snapshotCount());

        // 删除快照：第一次成功，当前对象不受影响
        assertEquals(HttpStatus.OK, http.exchange("/api/snapshots/e2e-s1", HttpMethod.DELETE,
                null, Void.class).getStatusCode());
        ResponseEntity<byte[]> objResp = http.getForEntity("/api/objects/e2e-a", byte[].class);
        assertEquals(HttpStatus.OK, objResp.getStatusCode());
        assertArrayEquals(bytes("e2e-shared-content"), objResp.getBody(), "删除快照后当前对象照常可读");

        // 重复删除：404 + SNAPSHOT_NOT_FOUND（与其他失败可区分）
        ResponseEntity<Map> err = http.exchange("/api/snapshots/e2e-s1", HttpMethod.DELETE, null, Map.class);
        assertEquals(HttpStatus.NOT_FOUND, err.getStatusCode());
        assertEquals("SNAPSHOT_NOT_FOUND", err.getBody().get("errorCode"));
        log.info("[e2e] re-delete -> {} {} (判定依据: 重复删除可区分)", err.getStatusCode(), err.getBody().get("errorCode"));

        // 删除从不存在的快照：同样的 404 但区别于其他错误码
        ResponseEntity<Map> err2 = http.exchange("/api/snapshots/e2e-never", HttpMethod.DELETE, null, Map.class);
        assertEquals(HttpStatus.NOT_FOUND, err2.getStatusCode());
        assertEquals("SNAPSHOT_NOT_FOUND", err2.getBody().get("errorCode"));
        log.info("[e2e] delete missing -> {} {}", err2.getStatusCode(), err2.getBody().get("errorCode"));
    }

    @Test
    void retentionEvictionOverHttp() {
        assertEquals(HttpStatus.OK, putObject("e2e-doc", "v1-content").getStatusCode());
        assertEquals(HttpStatus.OK, http.postForEntity("/api/snapshots/e2e-t1", null, String.class).getStatusCode());
        assertEquals(HttpStatus.OK, putObject("e2e-doc", "v2-content").getStatusCode());
        assertEquals(HttpStatus.OK, http.postForEntity("/api/snapshots/e2e-t2", null, String.class).getStatusCode());
        assertEquals(HttpStatus.OK, putObject("e2e-doc", "v3-content").getStatusCode());
        long usedBeforeEvict = http.getForEntity("/api/stats", StorageStats.class).getBody().usedBytes();
        log.info("[e2e] input: t1={v1} t2={v2}, used={} before creating t3 with maxSnapshots=2", usedBeforeEvict);

        // 创建 t3 并限制最多 2 份：最旧的 t1 应被淘汰
        assertEquals(HttpStatus.OK, http.postForEntity("/api/snapshots/e2e-t3?maxSnapshots=2",
                null, String.class).getStatusCode());

        ResponseEntity<List<SnapshotMeta>> listResp = http.exchange("/api/snapshots", HttpMethod.GET,
                null, new ParameterizedTypeReference<>() {
                });
        List<String> names = listResp.getBody().stream().map(SnapshotMeta::name).toList();
        assertFalse(names.contains("e2e-t1"), "最旧的 t1 须被淘汰");
        assertTrue(names.contains("e2e-t2") && names.contains("e2e-t3"), "更近的快照须保留");
        log.info("[e2e] GET /api/snapshots -> {} (判定依据: 最旧让位)", names);

        // 被淘汰的 t1：读取/恢复/删除均 410 SNAPSHOT_EVICTED，与 404 可区分
        ResponseEntity<Map> readErr = http.getForEntity("/api/snapshots/e2e-t1/objects/e2e-doc", Map.class);
        assertEquals(HttpStatus.GONE, readErr.getStatusCode());
        assertEquals("SNAPSHOT_EVICTED", readErr.getBody().get("errorCode"));
        ResponseEntity<Map> restoreErr = http.postForEntity("/api/snapshots/e2e-t1/restore", null, Map.class);
        assertEquals(HttpStatus.GONE, restoreErr.getStatusCode());
        assertEquals("SNAPSHOT_EVICTED", restoreErr.getBody().get("errorCode"));
        ResponseEntity<Map> delErr = http.exchange("/api/snapshots/e2e-t1", HttpMethod.DELETE, null, Map.class);
        assertEquals(HttpStatus.GONE, delErr.getStatusCode());
        assertEquals("SNAPSHOT_EVICTED", delErr.getBody().get("errorCode"));
        log.info("[e2e] evicted t1: read/restore/delete -> 410 SNAPSHOT_EVICTED");

        // 从不存在的快照：404 SNAPSHOT_NOT_FOUND，与被淘汰明确区分
        ResponseEntity<Map> notFound = http.getForEntity("/api/snapshots/e2e-ghost/objects/e2e-doc", Map.class);
        assertEquals(HttpStatus.NOT_FOUND, notFound.getStatusCode());
        assertEquals("SNAPSHOT_NOT_FOUND", notFound.getBody().get("errorCode"));
        log.info("[e2e] missing snapshot -> 404 SNAPSHOT_NOT_FOUND (判定依据: 与 410 区分)");

        // 幸存快照与当前对象不受影响
        ResponseEntity<byte[]> t2Doc = http.getForEntity("/api/snapshots/e2e-t2/objects/e2e-doc", byte[].class);
        assertEquals(HttpStatus.OK, t2Doc.getStatusCode());
        assertArrayEquals(bytes("v2-content"), t2Doc.getBody());
        ResponseEntity<byte[]> current = http.getForEntity("/api/objects/e2e-doc", byte[].class);
        assertArrayEquals(bytes("v3-content"), current.getBody(), "当前对象不得被误伤");

        // 淘汰回收体现在容量上：占用下降量 == t1 独占的 v1-content 大小
        ResponseEntity<StorageStats> stats = http.getForEntity("/api/stats", StorageStats.class);
        long reclaimed = usedBeforeEvict - stats.getBody().usedBytes();
        assertEquals("v1-content".length(), reclaimed,
                "被淘汰快照独占的空间须回收并体现在容量上");
        log.info("[e2e] GET /api/stats -> used={} reclaimed={} (判定依据: 回收量 == t1 独占的 v1-content)",
                stats.getBody().usedBytes(), reclaimed);
    }
}
