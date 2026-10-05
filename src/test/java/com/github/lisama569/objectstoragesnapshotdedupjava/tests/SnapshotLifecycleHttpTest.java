package com.github.lisama569.objectstoragesnapshotdedupjava.tests;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
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
 * 通过外部 HTTP 接口端到端验证快照生命周期、空间回收、统计与保留策略，以及失败理由的区分。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.MethodName.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SnapshotLifecycleHttpTest {

    private static final Logger log = LoggerFactory.getLogger(SnapshotLifecycleHttpTest.class);

    static Path tempDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        try {
            tempDir = Files.createTempDirectory("snapshot-http-test");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        registry.add("storage.root", () -> tempDir.toString());
    }

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate http;

    private String base;

    @BeforeAll
    void init() {
        base = "http://localhost:" + port + "/api";
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> body(ResponseEntity<String> resp) throws Exception {
        return new com.fasterxml.jackson.databind.ObjectMapper()
                .readValue(resp.getBody(), Map.class);
    }

    @Test
    void a01_objectsSnapshotsAndStatsEndToEnd() throws Exception {
        // 写两个对象（a 共享、b 将被快照独占）
        http.put(base + "/objects/a", "shared".getBytes(StandardCharsets.UTF_8));
        http.put(base + "/objects/b", "snap-only".getBytes(StandardCharsets.UTF_8));
        log.info("[http-test] PUT a=shared,b=snap-only");

        // 创建快照
        ResponseEntity<String> created = http.postForEntity(base + "/snapshots/s1", null, String.class);
        assertEquals(HttpStatus.OK, created.getStatusCode());
        Map<String, Object> c = body(created);
        assertEquals("s1", c.get("name"));
        log.info("[http-test] POST /snapshots/s1 -> {}", c);

        // 演进：覆盖 b（快照中的旧 b 变独占）
        http.put(base + "/objects/b", "current-b".getBytes(StandardCharsets.UTF_8));

        // 统计
        ResponseEntity<String> statsResp = http.getForEntity(base + "/stats", String.class);
        Map<String, Object> stats = body(statsResp);
        List<Map<String, Object>> snaps = (List<Map<String, Object>>) stats.get("snapshots");
        assertEquals(1, snaps.size());
        int reclaimable = ((Number) snaps.get(0).get("reclaimableBytes")).intValue();
        assertEquals("snap-only".getBytes(StandardCharsets.UTF_8).length, reclaimable,
                "HTTP 统计：删除 s1 预计回收独占的 snap-only blob");
        log.info("[http-test] GET /stats -> used={} dedupSaved={} snapReclaimable={} snapshots={}",
                stats.get("usedBytes"), stats.get("dedupSavedBytes"),
                stats.get("snapshotReclaimableBytes"), snaps);

        // 快照列表
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> infos = http.getForObject(base + "/snapshots", List.class);
        assertEquals(1, infos.size());
        assertEquals("s1", infos.get(0).get("name"));

        // 快照读取（独占内容仍可读）
        byte[] inSnap = http.getForObject(base + "/snapshots/s1/objects/b", byte[].class);
        assertArrayEquals("snap-only".getBytes(StandardCharsets.UTF_8), inSnap);
        log.info("[http-test] snapshot read b='{}' (旧内容受快照保护)", new String(inSnap));

        // 删除快照：返回实际回收字节
        ResponseEntity<String> del = http.exchange(base + "/snapshots/s1", HttpMethod.DELETE, null, String.class);
        assertEquals(HttpStatus.OK, del.getStatusCode());
        Map<String, Object> delBody = body(del);
        assertEquals(false, delBody.get("evicted"));
        assertEquals(reclaimable, ((Number) delBody.get("reclaimedBytes")).intValue(),
                "HTTP 删除返回的实际回收必须与之前预测一致");
        log.info("[http-test] DELETE s1 -> {}", delBody);

        // 当前对象不受影响：b 是新内容，a 仍在
        assertArrayEquals("shared".getBytes(StandardCharsets.UTF_8),
                http.getForObject(base + "/objects/a", byte[].class));
        assertArrayEquals("current-b".getBytes(StandardCharsets.UTF_8),
                http.getForObject(base + "/objects/b", byte[].class));

        // 删除后再读快照 -> 404 SNAPSHOT_NOT_FOUND
        ResponseEntity<String> missing = http.getForEntity(base + "/snapshots/s1/objects/b", String.class);
        assertEquals(HttpStatus.NOT_FOUND, missing.getStatusCode());
        assertEquals("SNAPSHOT_NOT_FOUND", body(missing).get("errorCode"));
        log.info("[http-test] 读已删快照 -> 404 {}", body(missing).get("errorCode"));
    }

    @Test
    void a02_deleteMissingAndDuplicateDeleteAreDistinguishable() throws Exception {
        http.put(base + "/objects/x", "x-payload".getBytes(StandardCharsets.UTF_8));
        http.postForEntity(base + "/snapshots/s2", null, String.class);

        // 删除不存在的快照 -> 404 SNAPSHOT_NOT_FOUND
        ResponseEntity<String> ghost = http.exchange(base + "/snapshots/ghost",
                HttpMethod.DELETE, null, String.class);
        assertEquals(HttpStatus.NOT_FOUND, ghost.getStatusCode());
        assertEquals("SNAPSHOT_NOT_FOUND", body(ghost).get("errorCode"));
        log.info("[http-test] 删除不存在快照 -> 404 {} : {}",
                body(ghost).get("errorCode"), body(ghost).get("message"));

        // 第一次删除成功
        ResponseEntity<String> first = http.exchange(base + "/snapshots/s2",
                HttpMethod.DELETE, null, String.class);
        assertEquals(HttpStatus.OK, first.getStatusCode());
        // 重复删除 -> 404，与“本来就不存在”同一可区分理由，且不会二次回收
        ResponseEntity<String> second = http.exchange(base + "/snapshots/s2",
                HttpMethod.DELETE, null, String.class);
        assertEquals(HttpStatus.NOT_FOUND, second.getStatusCode());
        assertEquals("SNAPSHOT_NOT_FOUND", body(second).get("errorCode"));
        log.info("[http-test] 重复删除 -> 404 {}（仅成功一次）", body(second).get("errorCode"));
    }

    @Test
    void a03_retentionPolicyEvictsOldestViaHttp() throws Exception {
        http.put(base + "/objects/r", "rv1".getBytes(StandardCharsets.UTF_8));
        http.postForEntity(base + "/snapshots/r1?maxCount=2", null, String.class);
        http.put(base + "/objects/r", "rv2".getBytes(StandardCharsets.UTF_8));
        http.postForEntity(base + "/snapshots/r2?maxCount=2", null, String.class);
        http.put(base + "/objects/r", "rv3".getBytes(StandardCharsets.UTF_8));
        ResponseEntity<String> third = http.postForEntity(base + "/snapshots/r3?maxCount=2",
                null, String.class);
        assertEquals(HttpStatus.OK, third.getStatusCode());
        Map<String, Object> createBody = body(third);
        List<Map<String, Object>> evicted = (List<Map<String, Object>>) createBody.get("evicted");
        assertEquals(1, evicted.size(), "maxCount=2 创建第 3 份必须淘汰最旧的 r1");
        assertEquals("r1", evicted.get(0).get("name"));
        assertEquals(true, evicted.get(0).get("evicted"));
        assertTrue(((Number) evicted.get(0).get("reclaimedBytes")).intValue() > 0,
                "被淘汰快照独占的 rv1 必须回收");
        log.info("[http-test] 创建 r3(maxCount=2) 淘汰 -> {}", evicted);

        // 被淘汰快照不能恢复 -> 404 SNAPSHOT_NOT_FOUND
        ResponseEntity<String> restore = http.postForEntity(base + "/snapshots/r1/restore",
                null, String.class);
        assertEquals(HttpStatus.NOT_FOUND, restore.getStatusCode());
        assertEquals("SNAPSHOT_NOT_FOUND", body(restore).get("errorCode"));
        log.info("[http-test] 恢复被淘汰快照 r1 -> 404 {}", body(restore).get("errorCode"));

        // 更近快照仍可恢复，当前对象不受影响
        assertArrayEquals("rv3".getBytes(StandardCharsets.UTF_8),
                http.getForObject(base + "/objects/r", byte[].class));

        // 非法策略 -> 400 INVALID_RETENTION
        ResponseEntity<String> bad = http.postForEntity(base + "/snapshots/bad?maxCount=0",
                null, String.class);
        assertEquals(HttpStatus.BAD_REQUEST, bad.getStatusCode());
        assertEquals("INVALID_RETENTION", body(bad).get("errorCode"));
        log.info("[http-test] maxCount=0 -> 400 {}", body(bad).get("errorCode"));
    }

    @Test
    void a04_createDuplicateSnapshotIsConflict() throws Exception {
        http.put(base + "/objects/y", "y1".getBytes(StandardCharsets.UTF_8));
        http.postForEntity(base + "/snapshots/dup", null, String.class);
        ResponseEntity<String> again = http.postForEntity(base + "/snapshots/dup", null, String.class);
        assertEquals(HttpStatus.CONFLICT, again.getStatusCode());
        assertEquals("CONCURRENT_CONFLICT", body(again).get("errorCode"));
        log.info("[http-test] 同名快照重复创建 -> 409 {}", body(again).get("errorCode"));
        // 清理，避免影响其他类的磁盘统计
        http.exchange(base + "/snapshots/dup", HttpMethod.DELETE, null, String.class);
        http.exchange(base + "/objects/y", HttpMethod.DELETE, null, String.class);
        http.exchange(base + "/objects/x", HttpMethod.DELETE, null, String.class);
        http.exchange(base + "/objects/a", HttpMethod.DELETE, null, String.class);
        http.exchange(base + "/objects/b", HttpMethod.DELETE, null, String.class);
        http.exchange(base + "/objects/r", HttpMethod.DELETE, null, String.class);
    }
}
