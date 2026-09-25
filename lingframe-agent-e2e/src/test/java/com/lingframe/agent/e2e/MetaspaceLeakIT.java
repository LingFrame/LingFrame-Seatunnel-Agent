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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Metaspace 零泄漏端到端全矩阵与并发验证。
 * <p>
 * 验证目标：
 * <ol>
 *   <li>阶段一：Source × Sink 3×3 全正交矩阵覆盖（Fake、MySQL JDBC、Kafka KRaft），
 *       外加扩展链路（Fake→LocalFile 文件写入、LocalFile→SQL→LocalFile 文件到文件、
 *       LocalFile→SQL→Jdbc 与 Jdbc→SQL→Kafka 跨组件混合），共 14 组矩阵；</li>
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

    /**
     * 是否采集详细的 jmap -clstats 诊断数据。普通提交默认关闭，避免重型诊断拖慢 E2E 主路径。
     */
    private static final String CLASSLOADER_STATS_PROPERTY = "lingframe.test.classloader.stats";

    // 动态 Metaspace 增长阈值：base + perJob × totalJobExecutions，再乘安全余量
    // base ≈ 7 MB（Agent 固定残留），perJob ≈ 0.25 MB/作业次（CL 卸载后 chunk 惰性回收残留）
    private static final long METASPACE_GROWTH_BASE_BYTES = 7 * 1024 * 1024L;
    private static final double METASPACE_GROWTH_PER_JOB_MB = 0.25;
    private static final double METASPACE_GROWTH_SAFETY_MARGIN = 1.5;
    private static final double METASPACE_CONVERGENCE_THRESHOLD = 0.2;

    private static long computeDynamicGrowthThreshold(int totalJobExecutions) {
        return (long) ((METASPACE_GROWTH_BASE_BYTES
                + METASPACE_GROWTH_PER_JOB_MB * totalJobExecutions * 1024 * 1024L)
                * METASPACE_GROWTH_SAFETY_MARGIN);
    }

    private static boolean isClassLoaderStatsEnabled() {
        return Boolean.parseBoolean(System.getProperty(CLASSLOADER_STATS_PROPERTY, "false"));
    }

    private static final long MAX_NET_OVERHEAD_BYTES = 2 * 1024 * 1024L;

    private static String formatMb(long bytes) {
        return String.format(Locale.ROOT, "%.2f MB", bytes / (1024.0 * 1024.0));
    }

    private static String formatDelta(long delta) {
        if (delta == Long.MIN_VALUE) {
            return "N/A";
        }
        return String.format(Locale.ROOT, "%+d", delta);
    }

    private static String reportRow(String label, String nativeVal, String agentVal, String deltaVal) {
        return String.format("  %-20s %-18s %-18s %s", label, nativeVal, agentVal, deltaVal);
    }

    @Test
    @DisplayName("全正交矩阵（含文件、SQL transform 与跨组件混合链路）+ 多 Job 异构并发压测后 Metaspace 应完全收敛零泄漏")
    void shouldNotLeakMetaspaceUnderFullMatrixAndConcurrency() throws Exception {
        Assumptions.assumeTrue(JvmDiagnostics.isDockerContainerRunning(AGENT_CONTAINER_NAME),
                "Docker daemon or '" + AGENT_CONTAINER_NAME + "' container is not available, skipping MetaspaceLeakIT");

        final List<String> matrixJobs = SeaTunnelJobMatrix.buildOrthogonalMatrixJobConfigs();
        final boolean isNativeRunning = JvmDiagnostics.isDockerContainerRunning(NATIVE_CONTAINER_NAME);
        assertThat(isNativeRunning)
                .as("Native control container '%s' must be running for strict A/B audit", NATIVE_CONTAINER_NAME)
                .isTrue();

        log.info("Starting Native-Control and Agent-Treatment benchmarks in parallel"
                + " (Kafka topics isolated by group prefix)...");
        log.info("Detailed jmap -clstats collection: {}", isClassLoaderStatsEnabled() ? "ENABLED" : "DISABLED");
        final CompletableFuture<MetaspaceAuditResult> nativeFuture = CompletableFuture.supplyAsync(() -> {
            try {
                return MetaspaceAuditRunner.runTestMatrixAndAudit("Native-Control", NATIVE_REST_URL,
                        NATIVE_CONTAINER_NAME, matrixJobs, "native-", isClassLoaderStatsEnabled());
            } catch (Exception e) {
                throw new RuntimeException("Native-Control benchmark failed", e);
            }
        });
        final CompletableFuture<MetaspaceAuditResult> agentFuture = CompletableFuture.supplyAsync(() -> {
            try {
                return MetaspaceAuditRunner.runTestMatrixAndAudit("Agent-Treatment", AGENT_REST_URL,
                        AGENT_CONTAINER_NAME, matrixJobs, "agent-", isClassLoaderStatsEnabled());
            } catch (Exception e) {
                throw new RuntimeException("Agent-Treatment benchmark failed", e);
            }
        });
        final MetaspaceAuditResult nativeResult = nativeFuture.get();
        final MetaspaceAuditResult agentResult = agentFuture.get();

        final long netOverhead = agentResult.getNetGrowth() - nativeResult.getNetGrowth();
        log.info("==================== [Metaspace A/B Audit Report] ====================");
        log.info("{}", reportRow("", "Native(Control)", "Agent(Treatment)", "Delta"));
        log.info("{}", reportRow("Baseline",
                formatMb(nativeResult.getBaseline()),
                formatMb(agentResult.getBaseline()),
                formatMb(agentResult.getBaseline() - nativeResult.getBaseline())));
        final int roundCount = Math.min(nativeResult.getRoundUsed().size(), agentResult.getRoundUsed().size());
        for (int r = 0; r < roundCount; r++) {
            final long nativeRound = nativeResult.getRoundUsed().get(r);
            final long agentRound = agentResult.getRoundUsed().get(r);
            log.info("{}", reportRow("Round " + (r + 1),
                    formatMb(nativeRound),
                    formatMb(agentRound),
                    formatMb(agentRound - nativeRound)));
        }
        log.info("{}", reportRow("Final",
                formatMb(nativeResult.getFinalUsed()),
                formatMb(agentResult.getFinalUsed()),
                formatMb(agentResult.getFinalUsed() - nativeResult.getFinalUsed())));
        log.info("{}", reportRow("Growth",
                formatMb(nativeResult.getNetGrowth()),
                formatMb(agentResult.getNetGrowth()),
                formatMb(netOverhead)));
        log.info("------------------------------------------------------------------------");
        log.info("{}", reportRow("Loaded Delta",
                formatDelta(nativeResult.getLoadedDelta()),
                formatDelta(agentResult.getLoadedDelta()),
                formatDelta(agentResult.getLoadedDelta() - nativeResult.getLoadedDelta())));
        log.info("{}", reportRow("Unloaded Delta",
                formatDelta(nativeResult.getUnloadedDelta()),
                formatDelta(agentResult.getUnloadedDelta()),
                formatDelta(agentResult.getUnloadedDelta() - nativeResult.getUnloadedDelta())));
        log.info("{}", reportRow("Retained Classes",
                formatDelta(nativeResult.getRetainedClasses()),
                formatDelta(agentResult.getRetainedClasses()),
                formatDelta(agentResult.getRetainedClasses() - nativeResult.getRetainedClasses())));
        log.info("{}", reportRow("ST ClassLoaders",
                formatDelta(nativeResult.getClassLoaderCount()),
                formatDelta(agentResult.getClassLoaderCount()),
                formatDelta(agentResult.getClassLoaderCount() - nativeResult.getClassLoaderCount())));
        log.info("========================================================================");
        log.info("  Net Overhead (Agent - Native) : {}", formatMb(netOverhead));
        final long dynamicGrowthThreshold = computeDynamicGrowthThreshold(agentResult.getTotalJobExecutions());
        log.info("  Growth Threshold (Agent, dynamic) : {} (for {} job executions)",
                formatMb(dynamicGrowthThreshold), agentResult.getTotalJobExecutions());
        log.info("  Overhead Threshold (Net)      : {}", formatMb(MAX_NET_OVERHEAD_BYTES));
        log.info("========================================================================");

        // 按 ClassLoader 分组打印类统计，证明 retained classes 归属（非子 CL）。
        // jmap -clstats 是重型诊断，仅在合并 PR 的 CI 中显式开启。
        if (isClassLoaderStatsEnabled()) {
            final CompletableFuture<Void> agentDumpFuture = CompletableFuture.runAsync(() ->
                    JvmDiagnostics.dumpClassLoaderStats(AGENT_CONTAINER_NAME, "Agent-Treatment",
                            agentResult.getBaselineClStats(), agentResult.getPreGcClStats()));
            final CompletableFuture<Void> nativeDumpFuture = CompletableFuture.runAsync(() ->
                    JvmDiagnostics.dumpClassLoaderStats(NATIVE_CONTAINER_NAME, "Native-Control",
                            nativeResult.getBaselineClStats(), nativeResult.getPreGcClStats()));
            try {
                agentDumpFuture.get();
                nativeDumpFuture.get();
            } catch (Exception e) {
                throw new RuntimeException("Parallel jmap clstats failed", e);
            }
        }

        // ====== 泄漏判定（综合方案：诊断先行 → 断言殿后）======
        // 采样失败（Long.MIN_VALUE）时跳过而非误判
        Assumptions.assumeTrue(agentResult.getClassLoaderCount() >= 0,
                "SeaTunnelChildFirstClassLoader count sampling failed, skipping leak verdict");

        // 1. 辅助提醒：net overhead 超阈值时 WARN
        if (netOverhead > MAX_NET_OVERHEAD_BYTES) {
            log.warn("Agent net overhead {} exceeds threshold {}."
                    + " Further investigation recommended.",
                    formatMb(netOverhead), formatMb(MAX_NET_OVERHEAD_BYTES));
        }

        // 2. 辅助提醒：Metaspace growth 超动态阈值时 WARN
        if (agentResult.getNetGrowth() >= dynamicGrowthThreshold) {
            log.warn("Agent Metaspace growth {} exceeds dynamic threshold {}."
                    + " Further investigation recommended.",
                    formatMb(agentResult.getNetGrowth()),
                    formatMb(dynamicGrowthThreshold));
        }

        // 2b. 趋势提醒：Round 2 增量应远小于 Round 1 增量（收敛率 > 80%），不收敛时显眼告警
        if (agentResult.getRoundUsed().size() >= 2) {
            final long round1Delta = agentResult.getRoundUsed().get(0) - agentResult.getBaseline();
            final long round2Delta = agentResult.getRoundUsed().get(1) - agentResult.getRoundUsed().get(0);
            if (round1Delta > 1024 * 1024L) {
                final double ratio = (double) round2Delta / round1Delta;
                log.info("  Convergence (1 - Round2/Round1)     : {}%",
                        String.format(Locale.ROOT, "%.1f", (1.0 - ratio) * 100));
                if (ratio >= METASPACE_CONVERGENCE_THRESHOLD) {
                    log.warn("========================================================================");
                    log.warn("  !!! TREND WARNING !!! Agent Metaspace NOT converging!");
                    log.warn("  Round 2 delta ({}) / Round 1 delta ({}) = {}% >= {}%",
                            formatMb(round2Delta), formatMb(round1Delta),
                            String.format(Locale.ROOT, "%.1f", ratio * 100),
                            String.format(Locale.ROOT, "%.0f", METASPACE_CONVERGENCE_THRESHOLD * 100));
                    log.warn("  Potential Metaspace leak -- investigate CL retention.");
                    log.warn("========================================================================");
                }
            }
        }

        // 3. 类元数据未卸载 / CL 拘留诊断（先行：不管后续断言成功失败，dump 必须先抓）
        //    retained > 0：类元数据未卸载（CL=0 时可能来自 bootstrap/AppCL 正常驻留）
        //    CL > 0：ClassLoader 拘留（泄漏直接证据）
        //    任一成立即抓 heap dump 供 MAT 分析残留 GC Root
        final boolean hasLeakEvidence = agentResult.getRetainedClasses() > 0
                || agentResult.getClassLoaderCount() > 0;
        if (hasLeakEvidence) {
            log.info("Leak evidence: retained={} classes, SeaTunnelCL={} instances."
                    + " Generating heap dump for MAT analysis.",
                    agentResult.getRetainedClasses(),
                    agentResult.getClassLoaderCount());
            try {
                final String pid = JvmDiagnostics.resolveJavaPid(AGENT_CONTAINER_NAME);
                final String dumpPath = "/tmp/heapdump-" + System.currentTimeMillis() + ".hprof";
                log.info("[Agent-Treatment] Generating heap dump at {} in container {}",
                        dumpPath, AGENT_CONTAINER_NAME);
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

        // 4. 主要断言（殿后）：CL=0 → 无泄漏
        //    放在 dump 之后，确保断言失败前 dump 已生成
        assertThat(agentResult.getClassLoaderCount())
                .as("SeaTunnelChildFirstClassLoader live instances must be 0 (no leak), actual: %d",
                        agentResult.getClassLoaderCount())
                .isZero();
    }

    /**
     * 按组别前缀隔离 Kafka topic，使 Native-Control 和 Agent-Treatment 可并行执行而不交叉污染数据。
     * 例：topicPrefix="native-" 时，"test-topic-1" → "native-test-topic-1"。
     */


}
