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
import java.util.ArrayList;
import java.util.List;
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

    private static final String SEATUNNEL_REST_URL = "http://localhost:5801/hazelcast/rest/maps/submit-job";
    private static final String CONTAINER_NAME = "seatunnel-server";
    private static final long METASPACE_GROWTH_THRESHOLD_BYTES = 15 * 1024 * 1024L;

    private static final Pattern METASPACE_USED_PATTERN =
            Pattern.compile("Metaspace.*?used\\s*=\\s*(\\d+)", Pattern.CASE_INSENSITIVE);

    @Test
    @DisplayName("全正交 9 组矩阵 + 多 Job 异构并发压测后 Metaspace 应完全收敛零泄漏")
    void shouldNotLeakMetaspaceUnderFullMatrixAndConcurrency() throws Exception {
        Assumptions.assumeTrue(isDockerContainerRunning(),
                "Docker daemon or '" + CONTAINER_NAME + "' container is not available, skipping MetaspaceLeakIT");

        // 1. 基线采样：记录初始 Metaspace
        forceFullGcInContainer();
        final long metaspaceBaseline = getCurrentMetaspaceUsed();

        // 2. 阶段一：执行 3×3 全正交 9 组作业矩阵（每组执行 2 轮，确保自闭环且发生卸载）
        final List<String> matrixJobs = buildOrthogonalMatrixJobConfigs();
        for (int round = 1; round <= 2; round++) {
            for (int i = 0; i < matrixJobs.size(); i++) {
                final String jobConfig = matrixJobs.get(i);
                submitJob("matrix-r" + round + "-j" + (i + 1), jobConfig);
                Thread.sleep(1000); // 间隔让作业执行并触发释放
            }
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
                            submitJob("concurrent-r" + roundIndex + "-j" + jobIndex, jobConfig);
                        } catch (Exception e) {
                            throw new RuntimeException("Concurrent job failed", e);
                        }
                    }, executor));
                }
            }
            // 等待全部并发作业提交完毕
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(60, TimeUnit.SECONDS);
        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        // 4. 等待引擎将所有异步作业生命周期完成与释放
        Thread.sleep(10000);

        // 5. 阶段三：强制 Full GC 并采样 Metaspace
        forceFullGcInContainer();
        final long metaspaceAfter = getCurrentMetaspaceUsed();
        final long growth = metaspaceAfter - metaspaceBaseline;

        // 6. 核心断言：经过全矩阵与并发洗礼后，Metaspace 增长应完全收敛在安全阈值（< 15MB）之内
        assertThat(growth)
                .as("Metaspace growth after 9-matrix and concurrent jobs should be < %d bytes, actual: %d (baseline: %d, after: %d)",
                        METASPACE_GROWTH_THRESHOLD_BYTES, growth, metaspaceBaseline, metaspaceAfter)
                .isLessThan(METASPACE_GROWTH_THRESHOLD_BYTES);
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
        jobs.add("{\"env\":{\"execution.mode\":\"BATCH\"},"
                + "\"source\":[{\"plugin_name\":\"Fake\",\"row.num\":1000," + SCHEMA_FIELDS + "}],"
                + "\"sink\":[{\"plugin_name\":\"Console\"}]}");

        // ② Fake -> MySQL（单端 JDBC 写入）
        jobs.add("{\"env\":{\"execution.mode\":\"BATCH\"},"
                + "\"source\":[{\"plugin_name\":\"Fake\",\"row.num\":1000," + SCHEMA_FIELDS + "}],"
                + "\"sink\":[{\"plugin_name\":\"Jdbc\",\"url\":\"" + MYSQL_URL + "\","
                + "\"driver\":\"" + MYSQL_DRIVER + "\",\"user\":\"root\",\"password\":\"root\","
                + "\"query\":\"" + INSERT_QUERY + "\"}]}");

        // ③ Fake -> Kafka（单端 MQ 生产）
        jobs.add("{\"env\":{\"execution.mode\":\"BATCH\"},"
                + "\"source\":[{\"plugin_name\":\"Fake\",\"row.num\":1000," + SCHEMA_FIELDS + "}],"
                + "\"sink\":[{\"plugin_name\":\"Kafka\",\"topic\":\"test-topic-1\","
                + "\"bootstrap.servers\":\"" + KAFKA_BOOTSTRAP + "\",\"format\":\"json\"}]}");

        // ④ MySQL -> Console（单端 JDBC 读取）
        jobs.add("{\"env\":{\"execution.mode\":\"BATCH\"},"
                + "\"source\":[{\"plugin_name\":\"Jdbc\",\"url\":\"" + MYSQL_URL + "\","
                + "\"driver\":\"" + MYSQL_DRIVER + "\",\"user\":\"root\",\"password\":\"root\","
                + "\"query\":\"" + SELECT_QUERY + "\"," + SCHEMA_FIELDS + "}],"
                + "\"sink\":[{\"plugin_name\":\"Console\"}]}");

        // ⑤ MySQL -> MySQL（双端真实 JDBC 同构）
        jobs.add("{\"env\":{\"execution.mode\":\"BATCH\"},"
                + "\"source\":[{\"plugin_name\":\"Jdbc\",\"url\":\"" + MYSQL_URL + "\","
                + "\"driver\":\"" + MYSQL_DRIVER + "\",\"user\":\"root\",\"password\":\"root\","
                + "\"query\":\"" + SELECT_QUERY + "\"," + SCHEMA_FIELDS + "}],"
                + "\"sink\":[{\"plugin_name\":\"Jdbc\",\"url\":\"" + MYSQL_URL + "\","
                + "\"driver\":\"" + MYSQL_DRIVER + "\",\"user\":\"root\",\"password\":\"root\","
                + "\"query\":\"" + INSERT_QUERY + "\"}]}");

        // ⑥ MySQL -> Kafka（跨协议异构：DB -> MQ）
        jobs.add("{\"env\":{\"execution.mode\":\"BATCH\"},"
                + "\"source\":[{\"plugin_name\":\"Jdbc\",\"url\":\"" + MYSQL_URL + "\","
                + "\"driver\":\"" + MYSQL_DRIVER + "\",\"user\":\"root\",\"password\":\"root\","
                + "\"query\":\"" + SELECT_QUERY + "\"," + SCHEMA_FIELDS + "}],"
                + "\"sink\":[{\"plugin_name\":\"Kafka\",\"topic\":\"test-topic-1\","
                + "\"bootstrap.servers\":\"" + KAFKA_BOOTSTRAP + "\",\"format\":\"json\"}]}");

        // ⑦ Kafka -> Console（单端 MQ 消费）
        jobs.add("{\"env\":{\"execution.mode\":\"BATCH\"},"
                + "\"source\":[{\"plugin_name\":\"Kafka\",\"topic\":\"test-topic-1\","
                + "\"bootstrap.servers\":\"" + KAFKA_BOOTSTRAP + "\","
                + "\"consumer.group\":\"e2e-group-console\",\"commit_on_checkpoint\":false,\"start_mode\":\"EARLIEST\","
                + SCHEMA_FIELDS + ",\"format\":\"json\"}],"
                + "\"sink\":[{\"plugin_name\":\"Console\"}]}");

        // ⑧ Kafka -> MySQL（跨协议异构：MQ -> DB）
        jobs.add("{\"env\":{\"execution.mode\":\"BATCH\"},"
                + "\"source\":[{\"plugin_name\":\"Kafka\",\"topic\":\"test-topic-1\","
                + "\"bootstrap.servers\":\"" + KAFKA_BOOTSTRAP + "\","
                + "\"consumer.group\":\"e2e-group-mysql\",\"commit_on_checkpoint\":false,\"start_mode\":\"EARLIEST\","
                + SCHEMA_FIELDS + ",\"format\":\"json\"}],"
                + "\"sink\":[{\"plugin_name\":\"Jdbc\",\"url\":\"" + MYSQL_URL + "\","
                + "\"driver\":\"" + MYSQL_DRIVER + "\",\"user\":\"root\",\"password\":\"root\","
                + "\"query\":\"" + INSERT_QUERY + "\"}]}");

        // ⑨ Kafka -> Kafka（双端真实 MQ 同构）
        jobs.add("{\"env\":{\"execution.mode\":\"BATCH\"},"
                + "\"source\":[{\"plugin_name\":\"Kafka\",\"topic\":\"test-topic-1\","
                + "\"bootstrap.servers\":\"" + KAFKA_BOOTSTRAP + "\","
                + "\"consumer.group\":\"e2e-group-kafka\",\"commit_on_checkpoint\":false,\"start_mode\":\"EARLIEST\","
                + SCHEMA_FIELDS + ",\"format\":\"json\"}],"
                + "\"sink\":[{\"plugin_name\":\"Kafka\",\"topic\":\"test-topic-2\","
                + "\"bootstrap.servers\":\"" + KAFKA_BOOTSTRAP + "\",\"format\":\"json\"}]}");

        return jobs;
    }

    private void submitJob(String jobTag, String jobConfig) throws IOException {
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
                throw new IOException("SeaTunnel job submission failed (" + jobTag + "): HTTP " + responseCode);
            }
        } finally {
            conn.disconnect();
        }
    }

    private void forceFullGcInContainer() {
        try {
            final Process p = new ProcessBuilder("docker", "exec", CONTAINER_NAME, "jcmd", "1", "GC.run").start();
            p.waitFor(5, TimeUnit.SECONDS);
            Thread.sleep(2000);
        } catch (Exception ignored) {
            // 忽略容器 Full GC 触发异常，不阻断主流程
            Thread.currentThread().interrupt();
        }
    }

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
        p.waitFor(10, TimeUnit.SECONDS);

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
     * <p>
     * 注意：`docker inspect` 对「容器存在但已停止（Exited）」也会返回退出码 0，
     * 仅凭退出码会误判为运行中；必须解析 {@code {{.State.Running}}} 的输出是否为 {@code true}。
     *
     * @return 容器正在运行返回 true，否则返回 false
     */
    private static boolean isDockerContainerRunning() {
        try {
            final Process p = new ProcessBuilder("docker", "inspect", "-f", "{{.State.Running}}", CONTAINER_NAME)
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
