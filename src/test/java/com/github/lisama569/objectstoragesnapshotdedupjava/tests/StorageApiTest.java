package com.github.lisama569.objectstoragesnapshotdedupjava.tests;

import com.github.lisama569.objectstoragesnapshotdedupjava.storage.ErrorCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * REST 入口测试：验证各入口下错误码可区分、快照/恢复语义与服务层一致。
 */
@SpringBootTest
@AutoConfigureMockMvc
class StorageApiTest {

    private static final Logger log = LoggerFactory.getLogger(StorageApiTest.class);

    @TempDir
    static Path tempDir;

    @DynamicPropertySource
    static void storageProps(DynamicPropertyRegistry registry) {
        registry.add("storage.root-dir", () -> tempDir.toString());
        registry.add("storage.capacity-bytes", () -> "64");
    }

    @Autowired
    MockMvc mvc;

    @Test
    void objectLifecycleAndSnapshotOverHttp() throws Exception {
        log.info("[输入] HTTP: PUT a=hello, 建快照 s1, 覆盖 a=world, 恢复快照 s1");
        mvc.perform(put("/objects/a").content("hello")).andExpect(status().isOk())
                .andExpect(jsonPath("$.version").value(1));
        mvc.perform(post("/snapshots/s1")).andExpect(status().isOk());
        mvc.perform(put("/objects/a").content("world")).andExpect(status().isOk());

        // 快照内容隔离
        MvcResult snap = mvc.perform(get("/snapshots/s1/objects/a")).andExpect(status().isOk()).andReturn();
        assertEquals("hello", snap.getResponse().getContentAsString(StandardCharsets.UTF_8));

        // 恢复后当前内容回到快照
        mvc.perform(post("/snapshots/s1/restore")).andExpect(status().isOk());
        MvcResult cur = mvc.perform(get("/objects/a")).andExpect(status().isOk()).andReturn();
        assertEquals("hello", cur.getResponse().getContentAsString(StandardCharsets.UTF_8));
        log.info("[判定] 恢复快照后 GET /objects/a = '{}' (期望 'hello')",
                cur.getResponse().getContentAsString(StandardCharsets.UTF_8));
    }

    @Test
    void errorCodesAreDistinctOverHttp() throws Exception {
        // NOT_FOUND -> 404
        mvc.perform(get("/objects/ghost")).andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value(ErrorCode.NOT_FOUND.name()));
        // CONCURRENT_CONFLICT -> 409
        mvc.perform(put("/objects/k").content("v1")).andExpect(status().isOk());
        mvc.perform(put("/objects/k").param("expectedVersion", "99").content("v2"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value(ErrorCode.CONCURRENT_CONFLICT.name()));
        // CAPACITY_EXCEEDED -> 507（容量 64 字节）
        byte[] big = new byte[128];
        mvc.perform(put("/objects/big").content(big))
                .andExpect(status().isInsufficientStorage())
                .andExpect(jsonPath("$.code").value(ErrorCode.CAPACITY_EXCEEDED.name()));
        log.info("[判定] HTTP 层错误码: NOT_FOUND=404, CONCURRENT_CONFLICT=409, CAPACITY_EXCEEDED=507, 均可区分");
        mvc.perform(delete("/objects/k")).andExpect(status().isOk());
    }
}
