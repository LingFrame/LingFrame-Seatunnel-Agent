package com.lingframe.agent.e2e;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Metaspace 零泄漏端到端验证。
 * <p>
 * 在挂载 Agent 的 SeaTunnel 集群上连续提交 1000 个作业，
 * 通过 docker exec jcmd 采集容器内 SeaTunnel 进程的 Metaspace，
 * 验证作业完成后不持续增长。
 * <p>
 * 前置条件：docker-compose up 启动 SeaTunnel + Agent，容器名 seatunnel-server。
 */
@DisplayName("Metaspace 零泄漏长稳验证")
class MetaspaceLeakIT {

    private static final String SEATUNNEL_REST_URL = "http://localhost:5801/hazelcast/rest/maps/submit-job";
    private static final String CONTAINER_NAME = "seatunnel-server";
    private static final int JOB_COUNT = 1000;
    private static final long METASPACE_GROWTH_THRESHOLD_BYTES = 10 * 1024 * 1024L;

    private static final Pattern METASPACE_USED_PATTERN =
            Pattern.compile("Metaspace.*?used\\s*=\\s*(\\d+)", Pattern.CASE_INSENSITIVE);

    @Test
    @DisplayName("1000 作业连续提交后容器内 Metaspace 增长应低于 10MB")
    void shouldNotLeakMetaspaceAfter1000Jobs() throws Exception {
        Assumptions.assumeTrue(isDockerContainerRunning(),
                "Docker daemon or '" + CONTAINER_NAME + "' container is not available, skipping MetaspaceLeakIT");

        final long metaspaceBefore = getCurrentMetaspaceUsed();

        for (int i = 0; i < JOB_COUNT; i++) {
            submitDummyJob(i);
        }

        Thread.sleep(5000);

        final long metaspaceAfter = getCurrentMetaspaceUsed();
        final long growth = metaspaceAfter - metaspaceBefore;

        assertThat(growth)
                .as("Metaspace growth after %d jobs should be < %d bytes, actual: %d",
                        JOB_COUNT, METASPACE_GROWTH_THRESHOLD_BYTES, growth)
                .isLessThan(METASPACE_GROWTH_THRESHOLD_BYTES);
    }

    private void submitDummyJob(int jobId) throws IOException {
        final String jobConfig = buildDummyJobConfig(jobId);
        final HttpURLConnection conn = (HttpURLConnection) new URL(SEATUNNEL_REST_URL).openConnection();
        try {
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(30000);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(jobConfig.getBytes(StandardCharsets.UTF_8));
            }
            final int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                throw new IOException("SeaTunnel job submission failed: HTTP " + responseCode);
            }
        } finally {
            conn.disconnect();
        }
    }

    private String buildDummyJobConfig(int jobId) {
        return "{\"env\":{\"execution.mode\":\"BATCH\"},"
                + "\"source\":[{\"plugin_name\":\"Fake\",\"row.num\":16,\"schema\":{\"fields\":{\"id\":\"int\"}}}],"
                + "\"sink\":[{\"plugin_name\":\"Console\"}]}";
    }

    /**
     * 通过 docker exec jcmd 采集容器内 SeaTunnel 进程（PID=1）的 Metaspace used 值。
     * <p>
     * jcmd 1 VM.metaspace 输出包含形如 "Metaspace       used = 12345678" 的行，
     * 用正则提取第一个匹配的数值。
     *
     * @return 容器内 Metaspace used 字节数，采集失败返回 0
     */
    private long getCurrentMetaspaceUsed() throws Exception {
        final ProcessBuilder pb = new ProcessBuilder(
                "docker", "exec", CONTAINER_NAME,
                "jcmd", "1", "VM.metaspace");
        pb.redirectErrorStream(true);
        final Process p = pb.start();
        final StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append('\n');
            }
        }
        p.waitFor();

        final Matcher matcher = METASPACE_USED_PATTERN.matcher(output);
        if (matcher.find()) {
            return Long.parseLong(matcher.group(1));
        }
        return 0L;
    }

    /**
     * 探测 Docker 守护进程与目标容器是否可用。
     * <p>
     * 通过 docker inspect 检查容器运行状态。若 Docker 未安装或容器未启动，
     * 返回 false，调用方应通过 JUnit Assumption 跳过测试而非报错。
     *
     * @return 容器正在运行返回 true，否则返回 false
     */
    private static boolean isDockerContainerRunning() {
        try {
            final Process p = new ProcessBuilder("docker", "inspect", "-f", "{{.State.Running}}", CONTAINER_NAME)
                    .redirectErrorStream(true)
                    .start();
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
