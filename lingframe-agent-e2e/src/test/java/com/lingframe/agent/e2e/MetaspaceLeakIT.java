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
import java.util.List;
import java.util.Locale;
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

        MetaspaceAuditResult(long baseline, long round1, long round2, long finalUsed) {
            this.baseline = baseline;
            this.round1 = round1;
            this.round2 = round2;
            this.finalUsed = finalUsed;
            this.netGrowth = finalUsed - baseline;
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
        final boolean isNativeRunning = isDockerContainerRunning(NATIVE_CONTAINER_NAME);
        assertThat(isNativeRunning)
                .as("Native control container '%s' must be running for strict A/B audit", NATIVE_CONTAINER_NAME)
                .isTrue();

        log.info("Starting Control Group benchmark on native SeaTunnel (without agent)...");
        final MetaspaceAuditResult nativeResult =
                runTestMatrixAndAudit("Native-Control", NATIVE_REST_URL, NATIVE_CONTAINER_NAME, matrixJobs);

        log.info("Starting Treatment Group benchmark on SeaTunnel with LingFrame Agent...");
        final MetaspaceAuditResult agentResult =
                runTestMatrixAndAudit("Agent-Treatment", AGENT_REST_URL, AGENT_CONTAINER_NAME, matrixJobs);

        final long netOverhead = agentResult.getNetGrowth() - nativeResult.getNetGrowth();
        log.info("==================== [Metaspace A/B Audit Report] ====================");
        log.info("Native Baseline: {} | Agent Baseline: {} | Baseline Delta: {}",
                formatMb(nativeResult.getBaseline()), formatMb(agentResult.getBaseline()),
                formatMb(agentResult.getBaseline() - nativeResult.getBaseline()));
        log.info("Native Round 1 : {} | Agent Round 1 : {} | Round 1 Delta: {}",
                formatMb(nativeResult.getRound1()), formatMb(agentResult.getRound1()),
                formatMb(agentResult.getRound1() - nativeResult.getRound1()));
        log.info("Native Round 2 : {} | Agent Round 2 : {} | Round 2 Delta: {}",
                formatMb(nativeResult.getRound2()), formatMb(agentResult.getRound2()),
                formatMb(agentResult.getRound2() - nativeResult.getRound2()));
        log.info("Native Final   : {} | Agent Final   : {} | Final Delta: {}",
                formatMb(nativeResult.getFinalUsed()), formatMb(agentResult.getFinalUsed()),
                formatMb(agentResult.getFinalUsed() - nativeResult.getFinalUsed()));
        log.info("Native Growth  : {} | Agent Growth  : {} | Net Overhead: {}",
                formatMb(nativeResult.getNetGrowth()), formatMb(agentResult.getNetGrowth()),
                formatMb(netOverhead));
        log.info("Upper Growth Threshold: {} | Max Net Overhead Limit: {}",
                formatMb(METASPACE_GROWTH_THRESHOLD_BYTES), formatMb(MAX_NET_OVERHEAD_BYTES));
        log.info("======================================================================");

        assertThat(netOverhead)
                .as("Agent net overhead should be <= %d bytes, actual: %d (nativeGrowth: %d, agentGrowth: %d)",
                        MAX_NET_OVERHEAD_BYTES, netOverhead, nativeResult.getNetGrowth(), agentResult.getNetGrowth())
                .isLessThanOrEqualTo(MAX_NET_OVERHEAD_BYTES);

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

        // 2. 阶段一：执行 3×3 全正交 9 组作业矩阵（每组执行 2 轮）
        long round1Used = 0L;
        long round2Used = 0L;
        for (int round = 1; round <= 2; round++) {
            for (int i = 0; i < matrixJobs.size(); i++) {
                final String jobConfig = matrixJobs.get(i);
                submitJob(restUrl, targetLabel + "-r" + round + "-j" + (i + 1), jobConfig);
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
            final List<CompletableFuture<Void>> futures = new ArrayList<>();
            final int concurrentRounds = 3;
            for (int r = 0; r < concurrentRounds; r++) {
                final int roundIndex = r;
                for (int j = 0; j < matrixJobs.size(); j++) {
                    final int jobIndex = j;
                    final String jobConfig = matrixJobs.get(j);
                    futures.add(CompletableFuture.runAsync(() -> {
                        try {
                            submitJob(restUrl, targetLabel + "-c-r" + roundIndex + "-j" + jobIndex, jobConfig);
                        } catch (Exception e) {
                            throw new RuntimeException("Concurrent job failed", e);
                        }
                    }, executor));
                }
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(60, TimeUnit.SECONDS);
        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        // 4. 等待引擎将所有异步作业生命周期完成与释放
        Thread.sleep(10000);

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

        return new MetaspaceAuditResult(baseline, round1Used, round2Used, finalUsed);
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

    private void submitJob(String restUrl, String jobTag, String jobConfig) throws IOException {
        final HttpURLConnection conn = (HttpURLConnection) new URL(restUrl).openConnection();
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
        } finally {
            conn.disconnect();
        }
    }

    private static final Pattern HEAP_INFO_METASPACE_PATTERN =
            Pattern.compile("Metaspace\\s+used\\s+(\\d+)\\s*K", Pattern.CASE_INSENSITIVE);

    private void forceFullGcInContainer(String containerName) {
        try {
            final String pid = resolveJavaPid(containerName);
            final Process p = new ProcessBuilder("docker", "exec", containerName, "jcmd", pid, "GC.run").start();
            p.waitFor(5, TimeUnit.SECONDS);
            Thread.sleep(2000);
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

        if (jstatLines.size() >= 2) {
            final String[] headers = jstatLines.get(0).split("\\s+");
            final String[] values = jstatLines.get(1).split("\\s+");
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
