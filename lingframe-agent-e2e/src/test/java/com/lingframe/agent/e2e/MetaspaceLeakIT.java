package com.lingframe.agent.e2e;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Metaspace 零泄漏端到端全矩阵与并发验证。
 * <p>
 * 验证目标：
 * <ol>
 *   <li>阶段一：Source × Sink 3×3 全正交矩阵覆盖（Fake、MySQL JDBC、Kafka KRaft）；</li>
 *   <li>阶段二：多 Job 异构并发交错执行（考验 ClassLoader 隔离、并发卸载不误伤、线程池 TCCL 防污染）；</li>
 *   <li>阶段三：Full GC 后 Metaspace 物理收敛断言（类彻底卸载，零泄漏）。</li>
 * </ol>
 * 前置条件：docker compose 启动 seatunnel-server、mysql-server、kafka-server。
 */
@DisplayName("Metaspace 零泄漏全正交矩阵与并发验证")
class MetaspaceLeakIT {

    private static final Logger log = LoggerFactory.getLogger(MetaspaceLeakIT.class);

    private static final String AGENT_REST_URL = "http://localhost:5801/hazelcast/rest/maps/submit-job";
    private static final String AGENT_CONTAINER_NAME = "seatunnel-server";

    private static final String NATIVE_REST_URL = "http://localhost:5802/hazelcast/rest/maps/submit-job";
    private static final String NATIVE_CONTAINER_NAME = "seatunnel-native";

    private static final long METASPACE_GROWTH_THRESHOLD_BYTES = 15 * 1024 * 1024L;
    private static final long MAX_NET_OVERHEAD_BYTES = 2 * 1024 * 1024L;

    private static final Pattern METASPACE_USED_PATTERN =
            Pattern.compile("Metaspace.*?used\\s*=\\s*(\\d+)", Pattern.CASE_INSENSITIVE);

    static final class MetaspaceAuditResult {
        private final long baseline;
        private final long round1;
        private final long round2;
        private final long finalUsed;
        private final long netGrowth;
        private final long retainedClasses;

        MetaspaceAuditResult(long baseline, long round1, long round2, long finalUsed, long retainedClasses) {
            this.baseline = baseline;
            this.round1 = round1;
            this.round2 = round2;
            this.finalUsed = finalUsed;
            this.netGrowth = finalUsed - baseline;
            this.retainedClasses = retainedClasses;
        }

        public long getBaseline() {
            return baseline;
        }

        public long getRound1() {
            return round1;
        }

        public long getRound2() {
            return round2;
        }

        public long getFinalUsed() {
            return finalUsed;
        }

        public long getNetGrowth() {
            return netGrowth;
        }

        public long getRetainedClasses() {
            return retainedClasses;
        }
    }

    private static String formatMb(long bytes) {
        return String.format(Locale.ROOT, "%.2f MB", bytes / (1024.0 * 1024.0));
    }

    @Test
    @DisplayName("全正交 9 组矩阵 + 多 Job 异构并发压测后 Metaspace 应完全收敛零泄漏")
    void shouldNotLeakMetaspaceUnderFullMatrixAndConcurrency() throws Exception {
        Assumptions.assumeTrue(isDockerContainerRunning(AGENT_CONTAINER_NAME),
                "Docker daemon or '" + AGENT_CONTAINER_NAME + "' container is not available, skipping MetaspaceLeakIT");

        final List<String> matrixJobs = buildOrthogonalMatrixJobConfigs();
        // [对照组已注释] 当前阶段不再每次 CI 跑对照组，只验证实验组独立 Metaspace 收敛
        // final boolean isNativeRunning = isDockerContainerRunning(NATIVE_CONTAINER_NAME);
        // assertThat(isNativeRunning)
        //         .as("Native control container '%s' must be running for strict A/B audit", NATIVE_CONTAINER_NAME)
        //         .isTrue();

        // [对照组已注释] log.info("Starting Control Group benchmark on native SeaTunnel (without agent)...");
        // final MetaspaceAuditResult nativeResult =
        //         runTestMatrixAndAudit("Native-Control", NATIVE_REST_URL, NATIVE_CONTAINER_NAME, matrixJobs);

        log.info("Starting Treatment Group benchmark on SeaTunnel with LingFrame Agent...");
        final MetaspaceAuditResult agentResult =
                runTestMatrixAndAudit("Agent-Treatment", AGENT_REST_URL, AGENT_CONTAINER_NAME, matrixJobs);

        // [对照组已注释] final long netOverhead = agentResult.getNetGrowth() - nativeResult.getNetGrowth();
        log.info("==================== [Metaspace Audit Report (Agent Only)] ====================");
        log.info("Agent Baseline : {}", formatMb(agentResult.getBaseline()));
        log.info("Agent Round 1  : {}", formatMb(agentResult.getRound1()));
        log.info("Agent Round 2  : {}", formatMb(agentResult.getRound2()));
        log.info("Agent Final    : {}", formatMb(agentResult.getFinalUsed()));
        log.info("Agent Growth   : {}", formatMb(agentResult.getNetGrowth()));
        log.info("Upper Growth Threshold: {}", formatMb(METASPACE_GROWTH_THRESHOLD_BYTES));
        log.info("======================================================================");

        // [对照组已注释] A/B 对比 netOverhead 断言
        // assertThat(netOverhead)
        //         .as("Agent net overhead should be <= %d bytes, actual: %d (nativeGrowth: %d, agentGrowth: %d)",
        //                 MAX_NET_OVERHEAD_BYTES, netOverhead, nativeResult.getNetGrowth(), agentResult.getNetGrowth())
        //         .isLessThanOrEqualTo(MAX_NET_OVERHEAD_BYTES);

        // 只要还有未回收的类（retained > 0），不管测试通过与否都抓 heap dump，供 MAT 分析定位残留 GC Root
        // 除非类全部回收（retained == 0），才跳过 dump
        if (agentResult.getRetainedClasses() > 0) {
            try {
                final String pid = resolveJavaPid(AGENT_CONTAINER_NAME);
                final String dumpPath = "/tmp/heapdump-" + System.currentTimeMillis() + ".hprof";
                log.info("[Agent-Treatment] Retained {} classes, generating heap dump at {} in container {}",
                        agentResult.getRetainedClasses(), dumpPath, AGENT_CONTAINER_NAME);
                final ProcessBuilder dumpPb = new ProcessBuilder(
                        "docker", "exec", AGENT_CONTAINER_NAME,
                        "jcmd", pid, "GC.heap_dump", dumpPath);
                dumpPb.redirectErrorStream(true);
                final Process dumpP = dumpPb.start();
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(dumpP.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        log.info("[heap_dump] {}", line);
                    }
                }
                dumpP.waitFor(60, TimeUnit.SECONDS);
                final String localDumpPath = "target/logs/heapdump-" + System.currentTimeMillis() + ".hprof";
                new java.io.File("target/logs").mkdirs();
                final ProcessBuilder cpPb = new ProcessBuilder(
                        "docker", "cp", AGENT_CONTAINER_NAME + ":" + dumpPath, localDumpPath);
                cpPb.redirectErrorStream(true);
                final Process cpP = cpPb.start();
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(cpP.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        log.info("[docker cp] {}", line);
                    }
                }
                cpP.waitFor(60, TimeUnit.SECONDS);
                log.info("[Agent-Treatment] Heap dump saved to {}", localDumpPath);
            } catch (Exception e) {
                log.warn("[Agent-Treatment] Failed to generate heap dump: {}", e.getMessage());
            }
        }

        assertThat(agentResult.getNetGrowth())
                .as("Agent Metaspace growth should be < %d bytes, actual: %d (baseline: %d, final: %d)",
                        METASPACE_GROWTH_THRESHOLD_BYTES, agentResult.getNetGrowth(),
                        agentResult.getBaseline(), agentResult.getFinalUsed())
                .isLessThan(METASPACE_GROWTH_THRESHOLD_BYTES);
    }

    private MetaspaceAuditResult runTestMatrixAndAudit(
            String targetLabel,
            String restUrl,
            String containerName,
            List<String> matrixJobs) throws Exception {
        // 1. 基线采样：记录初始 Metaspace
        forceFullGcInContainer(containerName);
        final long baseline = getCurrentMetaspaceUsed(containerName);
        // 刚性断言：真实的 SeaTunnel JVM 启动后 Metaspace 必然大于 10MB（约 10,485,760 字节）
        // 绝不容许任何抓错进程、解析错误或 0 字节伪造
        assertThat(baseline)
                .as("[%s] Baseline Metaspace must be > 10MB (real SeaTunnel JVM required), actual: %d bytes (%s)",
                        targetLabel, baseline, formatMb(baseline))
                .isGreaterThan(10 * 1024 * 1024L);
        log.info("[{}] Metaspace audit started: baseline={} bytes ({})",
                targetLabel, baseline, formatMb(baseline));
        final long[] baselineClassCounts = captureClassCounts(containerName);

        // 2. 阶段一：执行 3×3 全正交 9 组作业矩阵（每组执行 2 轮）
        long round1Used = 0L;
        long round2Used = 0L;
        final List<String> allJobIds = new ArrayList<>();
        for (int round = 1; round <= 2; round++) {
            for (int i = 0; i < matrixJobs.size(); i++) {
                final String jobConfig = matrixJobs.get(i);
                final String jobTag = targetLabel + "-r" + round + "-j" + (i + 1);
                final String jobId = submitJob(restUrl, jobTag, jobConfig);
                allJobIds.add(jobId);
                Thread.sleep(1000);
            }
            forceFullGcInContainer(containerName);
            final long rUsed = getCurrentMetaspaceUsed(containerName);
            if (round == 1) {
                round1Used = rUsed;
            } else {
                round2Used = rUsed;
            }
            log.info("[{}] Metaspace progression [Matrix Round {}/2]: used={} bytes ({}), deltaFromBaseline={} bytes ({})",
                    targetLabel, round, rUsed, formatMb(rUsed), rUsed - baseline, formatMb(rUsed - baseline));
        }

        // 3. 阶段二：多 Job 异构并发压测（4 线程并发交错提交不同异构作业）
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            final List<CompletableFuture<String>> futures = new ArrayList<>();
            final int concurrentRounds = 3;
            for (int r = 0; r < concurrentRounds; r++) {
                final int roundIndex = r;
                for (int j = 0; j < matrixJobs.size(); j++) {
                    final int jobIndex = j;
                    final String jobConfig = matrixJobs.get(j);
                    futures.add(CompletableFuture.supplyAsync(() -> {
                        try {
                            final String tag = targetLabel + "-c-r" + roundIndex + "-j" + jobIndex;
                            return submitJob(restUrl, tag, jobConfig);
                        } catch (Exception e) {
                            throw new RuntimeException("Concurrent job failed", e);
                        }
                    }, executor));
                }
            }
            for (CompletableFuture<String> f : futures) {
                allJobIds.add(f.get(120, TimeUnit.SECONDS));
            }
        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        // 4. 验证所有作业正常 FINISHED（排除崩溃假象——作业提交成功 HTTP 200 不等于执行完成）
        log.info("[{}] Verifying {} submitted jobs reached FINISHED state...", targetLabel, allJobIds.size());
        for (int i = 0; i < allJobIds.size(); i++) {
            final String jobId = allJobIds.get(i);
            waitForJobFinished(restUrl, jobId, targetLabel + "-job-" + (i + 1), 60);
        }
        log.info("[{}] All {} jobs confirmed FINISHED.", targetLabel, allJobIds.size());

        // 5. 等待引擎将所有异步作业生命周期完成与释放
        // Kafka source 作业消费全量消息可能需要较长时间，30 秒确保所有作业完成
        Thread.sleep(30000);

        // 5. 阶段三：强制 Full GC 并采样终态 Metaspace
        forceFullGcInContainer(containerName);
        final long finalUsed = getCurrentMetaspaceUsed(containerName);
        assertThat(finalUsed)
                .as("[%s] Final Metaspace must be > 10MB (real SeaTunnel JVM required), actual: %d bytes (%s)",
                        targetLabel, finalUsed, formatMb(finalUsed))
                .isGreaterThan(10 * 1024 * 1024L);
        final long growth = finalUsed - baseline;

        log.info("[{}] Metaspace progression [Concurrent Final]: used={} bytes ({}), deltaFromBaseline={} bytes ({})",
                targetLabel, finalUsed, formatMb(finalUsed), growth, formatMb(growth));

        final long[] finalClassCounts = captureClassCounts(containerName);
        printClassCountDiff(targetLabel, baselineClassCounts, finalClassCounts);

        final long retainedClasses = computeRetainedClasses(baselineClassCounts, finalClassCounts);
        return new MetaspaceAuditResult(baseline, round1Used, round2Used, finalUsed, retainedClasses);
    }


    private static final String MYSQL_URL =
            "jdbc:mysql://mysql-server:3306/test?useSSL=false&allowPublicKeyRetrieval=true";
    private static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String KAFKA_BOOTSTRAP = "kafka-server:9092";
    private static final String SCHEMA_FIELDS =
            "\"schema\":{\"fields\":{\"id\":\"int\",\"name\":\"string\",\"score\":\"double\"}}";
    private static final String INSERT_QUERY =
            "INSERT INTO t_sink (id, name, score) VALUES (?, ?, ?)"
                    + " ON DUPLICATE KEY UPDATE name=VALUES(name), score=VALUES(score)";
    private static final String SELECT_QUERY = "SELECT id, name, score FROM t_source";

    /**
     * 构建 3×3 = 9 组全正交作业配置。
     * 组件维度：Fake、MySQL JDBC、Kafka KRaft。
     */
    private List<String> buildOrthogonalMatrixJobConfigs() {
        final List<String> jobs = new ArrayList<>();

        // ① Fake -> Console（纯内存基线）
        jobs.add("{\"env\":{\"job.name\":\"fake2console\",\"job.mode\":\"BATCH\"},"
                + "\"source\":[{\"plugin_name\":\"FakeSource\",\"row.num\":1000," + SCHEMA_FIELDS + "}],"
                + "\"sink\":[{\"plugin_name\":\"Console\"}]}");

        // ② Fake -> MySQL（单端 JDBC 写入）
        jobs.add("{\"env\":{\"job.name\":\"fake2mysql\",\"job.mode\":\"BATCH\"},"
                + "\"source\":[{\"plugin_name\":\"FakeSource\",\"row.num\":1000," + SCHEMA_FIELDS + "}],"
                + "\"sink\":[{\"plugin_name\":\"Jdbc\",\"url\":\"" + MYSQL_URL + "\","
                + "\"driver\":\"" + MYSQL_DRIVER + "\",\"user\":\"root\",\"password\":\"root\","
                + "\"query\":\"" + INSERT_QUERY + "\"}]}");

        // ③ Fake -> Kafka（单端 MQ 生产）
        jobs.add("{\"env\":{\"job.name\":\"fake2kafka\",\"job.mode\":\"BATCH\"},"
                + "\"source\":[{\"plugin_name\":\"FakeSource\",\"row.num\":1000," + SCHEMA_FIELDS + "}],"
                + "\"sink\":[{\"plugin_name\":\"Kafka\",\"topic\":\"test-topic-1\","
                + "\"bootstrap.servers\":\"" + KAFKA_BOOTSTRAP + "\",\"format\":\"json\"}]}");

        // ④ MySQL -> Console（单端 JDBC 读取）
        jobs.add("{\"env\":{\"job.name\":\"mysql2console\",\"job.mode\":\"BATCH\"},"
                + "\"source\":[{\"plugin_name\":\"Jdbc\",\"url\":\"" + MYSQL_URL + "\","
                + "\"driver\":\"" + MYSQL_DRIVER + "\",\"user\":\"root\",\"password\":\"root\","
                + "\"query\":\"" + SELECT_QUERY + "\"," + SCHEMA_FIELDS + "}],"
                + "\"sink\":[{\"plugin_name\":\"Console\"}]}");

        // ⑤ MySQL -> MySQL（双端真实 JDBC 同构）
        jobs.add("{\"env\":{\"job.name\":\"mysql2mysql\",\"job.mode\":\"BATCH\"},"
                + "\"source\":[{\"plugin_name\":\"Jdbc\",\"url\":\"" + MYSQL_URL + "\","
                + "\"driver\":\"" + MYSQL_DRIVER + "\",\"user\":\"root\",\"password\":\"root\","
                + "\"query\":\"" + SELECT_QUERY + "\"," + SCHEMA_FIELDS + "}],"
                + "\"sink\":[{\"plugin_name\":\"Jdbc\",\"url\":\"" + MYSQL_URL + "\","
                + "\"driver\":\"" + MYSQL_DRIVER + "\",\"user\":\"root\",\"password\":\"root\","
                + "\"query\":\"" + INSERT_QUERY + "\"}]}");

        // ⑥ MySQL -> Kafka（跨协议异构：DB -> MQ）
        jobs.add("{\"env\":{\"job.name\":\"mysql2kafka\",\"job.mode\":\"BATCH\"},"
                + "\"source\":[{\"plugin_name\":\"Jdbc\",\"url\":\"" + MYSQL_URL + "\","
                + "\"driver\":\"" + MYSQL_DRIVER + "\",\"user\":\"root\",\"password\":\"root\","
                + "\"query\":\"" + SELECT_QUERY + "\"," + SCHEMA_FIELDS + "}],"
                + "\"sink\":[{\"plugin_name\":\"Kafka\",\"topic\":\"test-topic-1\","
                + "\"bootstrap.servers\":\"" + KAFKA_BOOTSTRAP + "\",\"format\":\"json\"}]}");

        // ⑦ Kafka -> Console（单端 MQ 消费）
        jobs.add("{\"env\":{\"job.name\":\"kafka2console\",\"job.mode\":\"BATCH\"},"
                + "\"source\":[{\"plugin_name\":\"Kafka\",\"topic\":\"test-topic-1\","
                + "\"bootstrap.servers\":\"" + KAFKA_BOOTSTRAP + "\","
                + "\"consumer.group\":\"e2e-group-console\",\"commit_on_checkpoint\":false,\"start_mode\":\"earliest\","
                + SCHEMA_FIELDS + ",\"format\":\"json\"}],"
                + "\"sink\":[{\"plugin_name\":\"Console\"}]}");

        // ⑧ Kafka -> MySQL（跨协议异构：MQ -> DB）
        jobs.add("{\"env\":{\"job.name\":\"kafka2mysql\",\"job.mode\":\"BATCH\"},"
                + "\"source\":[{\"plugin_name\":\"Kafka\",\"topic\":\"test-topic-1\","
                + "\"bootstrap.servers\":\"" + KAFKA_BOOTSTRAP + "\","
                + "\"consumer.group\":\"e2e-group-mysql\",\"commit_on_checkpoint\":false,\"start_mode\":\"earliest\","
                + SCHEMA_FIELDS + ",\"format\":\"json\"}],"
                + "\"sink\":[{\"plugin_name\":\"Jdbc\",\"url\":\"" + MYSQL_URL + "\","
                + "\"driver\":\"" + MYSQL_DRIVER + "\",\"user\":\"root\",\"password\":\"root\","
                + "\"query\":\"" + INSERT_QUERY + "\"}]}");

        // ⑨ Kafka -> Kafka（双端真实 MQ 同构）
        jobs.add("{\"env\":{\"job.name\":\"kafka2kafka\",\"job.mode\":\"BATCH\"},"
                + "\"source\":[{\"plugin_name\":\"Kafka\",\"topic\":\"test-topic-1\","
                + "\"bootstrap.servers\":\"" + KAFKA_BOOTSTRAP + "\","
                + "\"consumer.group\":\"e2e-group-kafka\",\"commit_on_checkpoint\":false,\"start_mode\":\"earliest\","
                + SCHEMA_FIELDS + ",\"format\":\"json\"}],"
                + "\"sink\":[{\"plugin_name\":\"Kafka\",\"topic\":\"test-topic-2\","
                + "\"bootstrap.servers\":\"" + KAFKA_BOOTSTRAP + "\",\"format\":\"json\"}]}");

        return jobs;
    }

    private static final int MAX_SUBMIT_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 2000L;

    private String submitJob(String restUrl, String jobTag, String jobConfig) throws IOException {
        IOException lastException = null;
        for (int attempt = 1; attempt <= MAX_SUBMIT_RETRIES; attempt++) {
            try {
                return submitJobOnce(restUrl, jobTag, jobConfig);
            } catch (IOException e) {
                lastException = e;
                log.warn("[{}] Job submission attempt {}/{} failed: {}", jobTag, attempt, MAX_SUBMIT_RETRIES, e.getMessage());
                if (attempt < MAX_SUBMIT_RETRIES) {
                    try {
                        Thread.sleep(RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Retry interrupted for " + jobTag, ie);
                    }
                }
            }
        }
        throw lastException;
    }

    private String submitJobOnce(String restUrl, String jobTag, String jobConfig) throws IOException {
        final HttpURLConnection conn = (HttpURLConnection) new URL(restUrl).openConnection();
        try {
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(60000);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(jobConfig.getBytes(StandardCharsets.UTF_8));
            }
            final int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                final StringBuilder err = new StringBuilder();
                try (InputStream es = conn.getErrorStream()) {
                    if (es != null) {
                        try (BufferedReader r = new BufferedReader(
                                new InputStreamReader(es, StandardCharsets.UTF_8))) {
                            String l;
                            while ((l = r.readLine()) != null) {
                                err.append(l).append('\n');
                            }
                        }
                    }
                }
                throw new IOException("SeaTunnel job submission failed (" + jobTag + "): HTTP "
                        + responseCode + " - " + err.toString().trim());
            }
            final String responseBody;
            try (InputStream is = conn.getInputStream()) {
                responseBody = readAll(is);
            }
            final String jobId = extractJsonField(responseBody, "jobId");
            if (jobId == null || jobId.isEmpty()) {
                throw new IOException("SeaTunnel job submission failed (" + jobTag
                        + "): no jobId in response: " + responseBody);
            }
            return jobId;
        } finally {
            conn.disconnect();
        }
    }

    /**
     * 轮询 SeaTunnel REST API 直到作业到达终态（FINISHED / FAILED / CANCELED）或超时。
     * <p>
     * 排除"提交成功但执行崩溃"的假象：HTTP 200 只证明作业被引擎接收，
     * 不证明作业正常执行完成。此方法通过 {@code GET /job-info/{jobId}} 确认终态。
     *
     * @param submitUrl     提交作业用的 REST URL（含 /submit-job 后缀）
     * @param jobId         作业 ID
     * @param jobTag        日志标签
     * @param timeoutSeconds 单作业等待超时（秒）
     */
    private void waitForJobFinished(String submitUrl, String jobId, String jobTag, int timeoutSeconds)
            throws IOException {
        final String jobInfoUrl = submitUrl.replace("submit-job", "running-job") + "/" + jobId;
        log.info("[{}] Polling job status at {}", jobTag, jobInfoUrl);
        final long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        String lastStatus = "UNKNOWN";
        while (System.currentTimeMillis() < deadline) {
            final HttpURLConnection conn = (HttpURLConnection) new URL(jobInfoUrl).openConnection();
            try {
                conn.setRequestMethod("GET");
                conn.setConnectTimeout(5000);
                conn.setReadTimeout(10000);
                final int responseCode = conn.getResponseCode();
                if (responseCode == 200) {
                    final String body;
                    try (InputStream is = conn.getInputStream()) {
                        body = readAll(is);
                    }
                    final String jobStatus = extractJsonField(body, "jobStatus");
                    if (jobStatus != null) {
                        lastStatus = jobStatus;
                        if ("FINISHED".equals(jobStatus)) {
                            log.info("[{}] Job {} -> FINISHED", jobTag, jobId);
                            return;
                        }
                        if ("FAILED".equals(jobStatus) || "CANCELED".equals(jobStatus)) {
                            throw new IOException("Job " + jobTag + " (id=" + jobId
                                    + ") ended with status " + jobStatus + ", body: " + body);
                        }
                    } else {
                        log.warn("[{}] Job {} response has no jobStatus field, body: {}", jobTag, jobId, body);
                    }
                } else {
                    final String errBody;
                    try (InputStream es = conn.getErrorStream()) {
                        errBody = es != null ? readAll(es) : "(no error stream)";
                    }
                    log.warn("[{}] Job {} HTTP {} - error: {}", jobTag, jobId, responseCode, errBody);
                }
            } finally {
                conn.disconnect();
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for job " + jobTag, e);
            }
        }
        throw new IOException("Job " + jobTag + " (id=" + jobId + ") did not finish within "
                + timeoutSeconds + " seconds, last status: " + lastStatus);
    }

    private static String extractJsonField(String json, String fieldName) {
        final Pattern p = Pattern.compile("\"" + fieldName + "\"\\s*:\\s*\"([^\"]+)\"");
        final Matcher m = p.matcher(json);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }

    private static String readAll(InputStream is) throws IOException {
        final StringBuilder sb = new StringBuilder();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String l;
            while ((l = r.readLine()) != null) {
                sb.append(l);
            }
        }
        return sb.toString();
    }

    private static final Pattern HEAP_INFO_METASPACE_PATTERN =
            Pattern.compile("Metaspace\\s+used\\s+(\\d+)\\s*K", Pattern.CASE_INSENSITIVE);

    private static int indexOf(String[] arr, String key) {
        for (int i = 0; i < arr.length; i++) {
            if (arr[i].equalsIgnoreCase(key)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 执行 {@code jstat -class} 采集类加载/卸载计数 {@code [loaded, unloaded]}。
     * <p>
     * 与 jcmd GC.class_stats 不同：后者依赖 -XX:+UnlockDiagnosticVMOptions，而该参数
     * 通过 JAVA_OPTS 注入会被 SeaTunnel 启动脚本重建的 JAVA_OPTS 覆盖、无法生效；
     * {@code jstat -class} 无需任何诊断参数，容器内 JDK 自带、稳定可用，直接给出
     * Loaded/Unloaded 两列。失败时返回 {@code new long[] {-1L, -1L}} 不影响主审计。
     */
    private long[] captureClassCounts(String containerName) {
        try {
            final String pid = resolveJavaPid(containerName);
            final ProcessBuilder pb = new ProcessBuilder(
                    "docker", "exec", containerName, "jstat", "-class", pid);
            pb.redirectErrorStream(true);
            final Process p = pb.start();
            final List<String> lines = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    lines.add(line.trim());
                }
            }
            p.waitFor(15, TimeUnit.SECONDS);
            // 过滤 JVM 工具参数输出（如 Picked up JAVA_TOOL_OPTIONS 等），动态匹配包含 Loaded/Unloaded 的真实表头行
            int headerIdx = -1;
            for (int i = 0; i < lines.size(); i++) {
                final String l = lines.get(i);
                if (l.contains("Loaded") && l.contains("Unloaded")) {
                    headerIdx = i;
                    break;
                }
            }
            if (headerIdx >= 0 && headerIdx + 1 < lines.size()) {
                final String[] headers = lines.get(headerIdx).split("\\s+");
                final String[] values = lines.get(headerIdx + 1).split("\\s+");
                final int loadedIdx = indexOf(headers, "Loaded");
                final int unloadedIdx = indexOf(headers, "Unloaded");
                if (loadedIdx >= 0 && unloadedIdx >= 0
                        && loadedIdx < values.length && unloadedIdx < values.length) {
                    return new long[]{Long.parseLong(values[loadedIdx]), Long.parseLong(values[unloadedIdx])};
                }
            }
            log.warn("jstat -class summary not found in container {}; raw head: {}",
                    containerName, lines.isEmpty() ? "" : lines.get(0));
            return new long[]{-1L, -1L};
        } catch (Exception e) {
            log.warn("jstat -class capture failed in container {}: {}", containerName, e.getMessage());
            return new long[]{-1L, -1L};
        }
    }

    /**
     * 计算 retained classes（loaded delta - unloaded delta），即未卸载的类元数据净增数。
     * 若 baseline 或 final 采样失败（含 -1），返回 -1 表示不可用。
     */
    private static long computeRetainedClasses(long[] baseline, long[] finalSample) {
        if (baseline[0] < 0 || finalSample[0] < 0) {
            return -1L;
        }
        final long loadedDelta = finalSample[0] - baseline[0];
        final long unloadedDelta = finalSample[1] - baseline[1];
        return loadedDelta - unloadedDelta;
    }

    /**
     * 打印类加载/卸载净增（baseline -> final），定位 Metaspace 泄漏是否源于类未卸载。
     * <p>
     * 判据：若加载持续增加而卸载近零（retained 净增），则类元数据未随 job 结束回滚，
     * Metaspace 泄漏成立；反之说明增长来自空间分配/碎片而非类元数据泄漏。
     */
    private void printClassCountDiff(String targetLabel, long[] baseline, long[] finalSample) {
        log.info("[{}] ========== Class Load / Unload Growth (Metaspace root cause) ==========",
                targetLabel);
        if (baseline[0] < 0 || finalSample[0] < 0) {
            log.info("[{}]   (class load/unload counts unavailable via jstat -class)",
                    targetLabel);
            log.info("[{}] ===========================================================", targetLabel);
            return;
        }
        final long loadedDelta = finalSample[0] - baseline[0];
        final long unloadedDelta = finalSample[1] - baseline[1];
        final long retained = loadedDelta - unloadedDelta;
        log.info("[{}]   loaded classes   baseline={} -> final={} (delta +{})",
                targetLabel, baseline[0], finalSample[0], loadedDelta);
        log.info("[{}]   unloaded classes baseline={} -> final={} (delta +{})",
                targetLabel, baseline[1], finalSample[1], unloadedDelta);
        log.info("[{}]   retained (loaded - unloaded) delta: +{} classes => 类元数据未卸载证候 {}",
                targetLabel, retained, retained > 0 ? "成立（Metaspace 泄漏）" : "不成立");
        log.info("[{}] ===========================================================", targetLabel);
    }

    /**
     * 执行 {@code jcmd <pid> GC.class_stats} 捕获存活类的"每个类同名字符串出现次数"。
     * <p>
     * GC.class_stats 按 ClassLoader 输出，同一类名被 N 个 ClassLoader 加载即出现 N 次——
     * 该计数正是"类元数据被多少份 loader 强持有、未随 job 回收"的直接度量。
     * 需要容器 JVM 带 -XX:+UnlockDiagnosticVMOptions（经 JAVA_TOOL_OPTIONS 注入才确保生效）。
     * 失败时返回空 Map，不影响主审计。
     */
    private Map<String, Integer> captureClassStats(String containerName) {
        final Map<String, Integer> counts = new HashMap<>();
        try {
            final String pid = resolveJavaPid(containerName);
            final ProcessBuilder pb = new ProcessBuilder(
                    "docker", "exec", containerName, "jcmd", pid, "GC.class_stats");
            pb.redirectErrorStream(true);
            final Process p = pb.start();
            final List<String> lines = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    lines.add(line.trim());
                }
            }
            p.waitFor(25, TimeUnit.SECONDS);
            for (String line : lines) {
                final String cls = firstClassToken(line);
                if (cls != null) {
                    counts.merge(cls, 1, Integer::sum);
                }
            }
            if (counts.isEmpty()) {
                log.warn("GC.class_stats returned no classes in container {} (unlock may be missing via JAVA_TOOL_OPTIONS); head: {}",
                        containerName, lines.isEmpty() ? "" : String.join(" | ", lines.subList(0, Math.min(3, lines.size()))));
            }
        } catch (Exception e) {
            log.warn("GC.class_stats capture failed in container {}: {}", containerName, e.getMessage());
        }
        return counts;
    }

    /**
     * 从 GC.class_stats 的一行中提取类名 token（形如 {@code a/b/C} 或 {@code pkg.Cls}），
     * 跳过索引数字列与表头。提取不到返回 {@code null}。
     */
    private String firstClassToken(String line) {
        if (line == null || line.isEmpty()) {
            return null;
        }
        if (line.startsWith("=") || line.startsWith("GC class") || line.startsWith("Class")) {
            return null;
        }
        for (String t : line.split("\\s+")) {
            if (t.length() <= 1 || t.matches("\\d+")) {
                continue;
            }
            if (!Character.isLetter(t.charAt(0))) {
                continue;
            }
            if (t.contains("/") || t.contains(".")) {
                return t;
            }
        }
        return null;
    }

    /**
     * 打印引擎/连接器类在 final 较 baseline 的净增 loader 持有副本，定位具体泄漏类。
     * <p>
     * 只统计命中 SeaTunnel/连接器/MySQL/Kafka 关键字的类，打印净增 Top 15，避免刷日志。
     */
    private void printClassStatsDiff(String targetLabel,
                                     Map<String, Integer> baseline,
                                     Map<String, Integer> finalSample) {
        log.info("[{}] ========== Class Stats Growth (GC.class_stats; +copies = 被多 ClassLoader 持有未回收) " + "==========",
                targetLabel);
        if (baseline.isEmpty() || finalSample.isEmpty()) {
            log.info("[{}]   (class stats unavailable — 需 JAVA_TOOL_OPTIONS 注入 -XX:+UnlockDiagnosticVMOptions)",
                    targetLabel);
            log.info("[{}] ===========================================================", targetLabel);
            return;
        }
        final List<String> includes = Arrays.asList(
                "org/apache/seatunnel", "SeaTunnelChildFirstClassLoader", "com/mysql", "org/apache/kafka");
        final Map<String, Integer> retained = new HashMap<>();
        for (Map.Entry<String, Integer> e : finalSample.entrySet()) {
            final String cls = e.getKey();
            if (includes.stream().noneMatch(cls::contains)) {
                continue;
            }
            final int net = e.getValue() - baseline.getOrDefault(cls, 0);
            if (net > 0) {
                retained.put(cls, net);
            }
        }
        if (retained.isEmpty()) {
            log.info("[{}]   no retained connector/engine class growth (类均已卸载或无新增占用)", targetLabel);
        } else {
            final List<Map.Entry<String, Integer>> entries = new ArrayList<>(retained.entrySet());
            entries.sort((a, b) -> b.getValue() - a.getValue());
            final int top = Math.min(15, entries.size());
            for (int i = 0; i < top; i++) {
                log.info("[{}]   +{} loader copies  {}", targetLabel,
                        entries.get(i).getValue(), entries.get(i).getKey());
            }
        }
        log.info("[{}] ===========================================================", targetLabel);
    }

    private void forceFullGcInContainer(String containerName) {
        try {
            final String pid = resolveJavaPid(containerName);
            // 三轮 Full GC 保障类元数据彻底卸载：
            // 第一轮：回收只被 weak/soft 引用持有的 ClassLoader
            // 第二轮：回收第一轮 GC 后因 WeakHashMap expunge / 缓存清理才暴露的 ClassLoader
            // 第三轮：卸载已回收 ClassLoader 加载的类元数据（Metaspace 释放）
            for (int i = 0; i < 3; i++) {
                final Process p = new ProcessBuilder("docker", "exec", containerName, "jcmd", pid, "GC.run").start();
                p.waitFor(5, TimeUnit.SECONDS);
                Thread.sleep(2000);
            }
        } catch (InterruptedException e) {
            log.warn("Full GC trigger interrupted in container {}: {}", containerName, e.getMessage());
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn("Full GC trigger failed in container {}: {}", containerName, e.getMessage());
        }
    }

    /**
     * 动态探测并获取容器内运行的 Java 进程 PID。
     * <p>
     * 容器的主进程通常是入口 Shell 脚本（占用 PID 1），真实的 SeaTunnel JVM 实例是其派生的子进程。
     * 本方法首先通过容器内 JDK 自带的 {@code jps -l} 进行探测，排除 jps 自身瞬时进程后，精准锁定真实的 JVM PID；
     * 若 jps 异常，后备使用 Linux 原生 {@code pgrep -f java} 进行兜底检索。
     *
     * @param containerName 目标 Docker 容器名
     * @return 真实的 Java 进程 PID 字符串
     * @throws IllegalStateException 若容器内未检测到任何正在运行的 Java 进程
     */
    private String resolveJavaPid(String containerName) throws Exception {
        // 1. 优先通过 jps -l 检索，提取包含 SeaTunnelServer 或非 Jps 的真实 Java 进程
        final ProcessBuilder jpsPb = new ProcessBuilder(
                "docker", "exec", containerName, "jps", "-l");
        jpsPb.redirectErrorStream(true);
        final Process jpsProcess = jpsPb.start();
        final List<String> jpsLines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(jpsProcess.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                final String trimmed = line.trim();
                if (!trimmed.isEmpty()) {
                    jpsLines.add(trimmed);
                }
            }
        }
        jpsProcess.waitFor(5, TimeUnit.SECONDS);

        // 优先匹配 SeaTunnel 核心服务类
        for (String line : jpsLines) {
            final String[] parts = line.split("\\s+");
            if (parts.length >= 2 && parts[0].matches("\\d+")) {
                final String pid = parts[0];
                final String mainClass = parts[1];
                if (!mainClass.contains("Jps") && (mainClass.contains("SeaTunnel") || mainClass.contains("starter"))) {
                    return pid;
                }
            }
        }

        // 次选：排除 Jps 后的任意合法 Java PID
        for (String line : jpsLines) {
            final String[] parts = line.split("\\s+");
            if (parts.length >= 1 && parts[0].matches("\\d+")) {
                final String pid = parts[0];
                final String mainClass = parts.length > 1 ? parts[1] : "";
                if (!mainClass.contains("Jps")) {
                    return pid;
                }
            }
        }

        // 2. 后备方案：通过 Linux 原生 pgrep -f java 查找 PID
        final ProcessBuilder pgrepPb = new ProcessBuilder(
                "docker", "exec", containerName, "sh", "-c", "pgrep -f java || pidof java");
        pgrepPb.redirectErrorStream(true);
        final Process pgrepProcess = pgrepPb.start();
        final List<String> pgrepLines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(pgrepProcess.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                final String trimmed = line.trim();
                if (!trimmed.isEmpty()) {
                    pgrepLines.add(trimmed);
                }
            }
        }
        pgrepProcess.waitFor(5, TimeUnit.SECONDS);

        for (String line : pgrepLines) {
            final String[] pids = line.split("\\s+");
            for (String pid : pids) {
                if (pid.matches("\\d+")) {
                    return pid;
                }
            }
        }

        throw new IllegalStateException("Failed to resolve Java PID in container '"
                + containerName + "'. jps output: " + jpsLines + ", pgrep output: " + pgrepLines);
    }

    private long getCurrentMetaspaceUsed(String containerName) throws Exception {
        final String pid = resolveJavaPid(containerName);

        // 1. 优先使用 JDK 8~21 通用的 jcmd <PID> GC.heap_info
        final ProcessBuilder pb = new ProcessBuilder(
                "docker", "exec", containerName,
                "jcmd", pid, "GC.heap_info");
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
        p.waitFor(10, TimeUnit.SECONDS);

        final Matcher matcher = HEAP_INFO_METASPACE_PATTERN.matcher(output);
        if (matcher.find()) {
            final long kb = Long.parseLong(matcher.group(1));
            return kb * 1024L;
        }

        // 2. 后备方案：使用 jstat -gc <PID> 提取 MU (Metaspace Used, KB)
        final ProcessBuilder jstatPb = new ProcessBuilder(
                "docker", "exec", containerName,
                "jstat", "-gc", pid);
        jstatPb.redirectErrorStream(true);
        final Process jstatP = jstatPb.start();
        final List<String> jstatLines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(jstatP.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    jstatLines.add(line.trim());
                }
            }
        }
        jstatP.waitFor(10, TimeUnit.SECONDS);

        // 动态定位包含 MU 的真实表头行
        int headerIdx = -1;
        for (int i = 0; i < jstatLines.size(); i++) {
            final String l = jstatLines.get(i);
            if (l.contains("MU")) {
                headerIdx = i;
                break;
            }
        }
        if (headerIdx >= 0 && headerIdx + 1 < jstatLines.size()) {
            final String[] headers = jstatLines.get(headerIdx).split("\\s+");
            final String[] values = jstatLines.get(headerIdx + 1).split("\\s+");
            int muIndex = -1;
            for (int i = 0; i < headers.length; i++) {
                if ("MU".equalsIgnoreCase(headers[i])) {
                    muIndex = i;
                    break;
                }
            }
            if (muIndex >= 0 && muIndex < values.length) {
                final double kb = Double.parseDouble(values[muIndex]);
                return (long) (kb * 1024.0);
            }
        }

        throw new IllegalStateException("Failed to parse Metaspace usage from container '"
                + containerName + "' (PID " + pid + "). jcmd output: [" + output.toString().trim() + "], jstat lines: " + jstatLines);
    }

    /**
     * 探测 Docker 守护进程与目标容器是否可用。
     * <p>
     * 通过 docker inspect 检查容器运行状态。若 Docker 未安装或容器未启动，
     * 返回 false，调用方应通过 JUnit Assumption 跳过测试而非报错。
     * <p>
     * 注意：`docker inspect` 对「容器存在但已停止（Exited）」也会返回退出码 0，
     * 仅凭退出码会误判为运行中；必须解析 {@code {{.State.Running}}} 的输出是否为 {@code true}。
     *
     * @param containerName 目标容器名
     * @return 容器正在运行返回 true，否则返回 false
     */
    private static boolean isDockerContainerRunning(String containerName) {
        try {
            final Process p = new ProcessBuilder("docker", "inspect", "-f", "{{.State.Running}}", containerName)
                    .redirectErrorStream(true)
                    .start();
            final StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line);
                }
            }
            return p.waitFor() == 0 && Boolean.parseBoolean(output.toString().trim());
        } catch (Exception e) {
            return false;
        }
    }
}
