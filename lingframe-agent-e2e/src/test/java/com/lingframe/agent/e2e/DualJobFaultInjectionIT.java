package com.lingframe.agent.e2e;

import com.lingframe.agent.adapter.GovernanceRejectException;
import com.lingframe.agent.adapter.JobIdExtractor;
import com.lingframe.agent.adapter.JobLingRegistry;
import com.lingframe.agent.adapter.SeaTunnelAdapter;
import com.lingframe.agent.bridge.LingFrameAgentBridge;
import com.lingframe.agent.config.AgentConfig;
import com.lingframe.agent.pipeline.AgentGovernanceRuntime;
import com.lingframe.agent.pipeline.AgentPipelineFactory;
import com.lingframe.core.ling.LingRuntimeConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 双作业故障注入 e2e 前置验收。
 * <p>
 * 与 JobIsolationIT 的差异：本用例聚焦「真实下游故障注入 → 熔断器对目标作业真实打开」的
 * 完整闭环，而非治理拒绝。装配真实微内核链（AgentPipelineFactory + VirtualLingManager +
 * JobLingRegistry + SeaTunnelAdapter），用伪任务宿主（携带 {@code jobID}）驱动作业 A / B 两个
 * 独立作业，覆盖三条验收语义：
 * <ol>
 *   <li>场景1（fail-closed=true）：注入下游故障到作业 A → A 熔断打开硬拒绝（fail-closed 硬拒），
 *       作业 B 全程 100% 放行且零失败（故障计数隔离）；</li>
 *   <li>场景2（恢复闭环）：作业 A 熔断打开后复位其健康指标（模拟下游恢复 + 运维复位）→
 *       A 重新放行，作业 B 全程不受影响；</li>
 *   <li>场景3（fail-closed=false 安全基线）：A 熔断打开但软退避放行（不抛异常、不 sleep），
 *       不产生新拒绝语义；作业 B 100% 放行。</li>
 * </ol>
 * 本 IT 不依赖 Docker / 真实 SeaTunnel，可在常规构建中确定性执行。
 */
@DisplayName("双作业故障注入验收")
class DualJobFaultInjectionIT {

    /** 下游可用性失败（喂给熔断器 onError 的典型信号，走分类器类型契约命中）。 */
    private static final IOException DOWNSTREAM_FAILURE =
            new IOException("connection refused");

    private static final String JOB_A = "7";
    private static final String JOB_B = "8";
    private static final String LING_A = "seatunnel-job-7";
    private static final String LING_B = "seatunnel-job-8";

    @TempDir
    Path tempDir;

    private AgentGovernanceRuntime runtime;
    private SeaTunnelAdapter adapter;

    /** 伪任务宿主：携带作业身份字段（镜像 AbstractTask 的 jobID 契约；跨包用 public 确保可反射读取）。 */
    static final class FakeTask {
        public final long jobID;

        FakeTask(long jobID) {
            this.jobID = jobID;
        }
    }

    @BeforeEach
    void setUp() {
        LingFrameAgentBridge.registerContract(null);
    }

    @AfterEach
    void tearDown() {
        LingFrameAgentBridge.registerContract(null);
    }

    /** per-job 双作业装配（threshold=10 / window=5 / minCalls=1，限流放宽排除干扰）。 */
    private void wire(boolean failClosed) throws IOException {
        final StringBuilder yaml = new StringBuilder()
                .append("governance:\n")
                .append("  enabled: true\n")
                .append("  task-execution-advice-enabled: true\n")
                .append("  per-job-governance-enabled: true\n")
                .append("  per-job-max-tracked-jobs: 1024\n")
                .append("  per-job-idle-ttl-ms: 1800000\n")
                .append("  per-job-reap-interval-ms: 300000\n")
                .append("  resilience:\n")
                .append("    circuit-breaker-enabled: true\n")
                .append("    rate-limiter-enabled: true\n")
                .append("    fail-closed: ").append(Boolean.toString(failClosed)).append('\n')
                .append("    circuit-breaker-failure-rate-threshold: 10\n")
                .append("    circuit-breaker-sliding-window-size: 5\n")
                .append("    circuit-breaker-minimum-number-of-calls: 1\n")
                .append("    rate-limit-per-second: 100000\n");
        final Path cfg = tempDir.resolve("lingframe-governance-dual-job.yaml");
        Files.write(cfg, yaml.toString().getBytes(StandardCharsets.UTF_8));
        final AgentConfig config = AgentConfig.load(cfg.toString());
        runtime = AgentPipelineFactory.create(config);
        adapter = new SeaTunnelAdapter(
                config,
                runtime.getPipelineEngine(),
                runtime.getUnloadCoordinator(),
                runtime.getConfigCenter(),
                runtime.getEventBus(),
                runtime.getMetricsCollector());
        final JobIdExtractor extractor = new JobIdExtractor(true);
        final JobLingRegistry registry = new JobLingRegistry(
                runtime.getVirtualLingManager(),
                runtime.getPipelineEngine(),
                runtime.getMetricsCollector(),
                LingRuntimeConfig.builder().rateLimitPerSecond(100000)
                        .circuitBreakerFailureRateThreshold(10)
                        .circuitBreakerSlidingWindowSize(5)
                        .circuitBreakerMinimumNumberOfCalls(1)
                        .circuitBreakerWaitDurationInOpenStateMs(100L)
                        .bulkheadMaxConcurrent(10).build(),
                1024, 1_800_000L, 300_000L);
        adapter.setJobLevelGovernance(extractor, registry);
        LingFrameAgentBridge.registerContract(adapter);
    }

    /** 连续喂下游故障直至作业熔断打开（fail-closed=true 时由硬拒绝异常触达），返回是否打开。 */
    private boolean driveUntilCircuitOpen(FakeTask task) {
        final int maxDrives = 200;
        for (int i = 0; i < maxDrives; i++) {
            try {
                adapter.beforeTaskCall(task);
                adapter.afterTaskCall(task, DOWNSTREAM_FAILURE);
            } catch (GovernanceRejectException e) {
                return true; // fail-closed：CIRCUIT_OPEN 硬拒绝由此抵达
            }
        }
        return false;
    }

    private long failedRequests(String lingId) {
        return runtime.getMetricsCollector().get(lingId).snapshot().getFailedRequests();
    }

    @Nested
    @DisplayName("场景1：故障注入 → A 熔断硬拒绝，B 隔离")
    class FaultInjectionIsolation {

        @Test
        @DisplayName("作业 A 注入下游故障后熔断硬拒绝，作业 B 100% 放行且零失败")
        void shouldRejectFaultyJobAndKeepSiblingUnharmed() throws IOException {
            wire(true);

            final FakeTask taskA = new FakeTask(Long.parseLong(JOB_A));
            final FakeTask taskB = new FakeTask(Long.parseLong(JOB_B));

            // 故障注入：连续喂下游故障直至 A 熔断打开（fail-closed 硬拒绝触达）
            assertThat(driveUntilCircuitOpen(taskA))
                    .as("作业 A 应在持续下游故障注入后熔断打开")
                    .isTrue();

            // A 熔断打开期间：beforeTaskCall(A) 被硬拒绝
            assertThatThrownBy(() -> adapter.beforeTaskCall(taskA))
                    .as("作业 A 熔断打开应硬拒绝（fail-closed）")
                    .isInstanceOf(GovernanceRejectException.class);

            // 故障计数隔离：A 失败计入 A 自己的作业灵元
            assertThat(runtime.getMetricsCollector().get(LING_A)).isNotNull();
            assertThat(failedRequests(LING_A)).as("A 的失败应计入作业 A 灵元").isGreaterThan(0);

            // B 100% 放行 + 零失败（不受 A 故障风暴影响）
            assertThatCode(() -> {
                adapter.beforeTaskCall(taskB);
                adapter.afterTaskCall(taskB, null);
            }).as("作业 B 在作业 A 熔断打开期间应 100% 放行").doesNotThrowAnyException();
            assertThat(runtime.getMetricsCollector().get(LING_B)).isNotNull();
            assertThat(failedRequests(LING_B)).as("作业 B 不应记录任何失败").isZero();
        }
    }

    @Nested
    @DisplayName("场景2：恢复闭环")
    class RecoveryClosedLoop {

        @Test
        @DisplayName("作业 A 熔断打开后经自愈探测恢复（OPEN→HALF_OPEN→CLOSED），A 重新放行，B 全程不受影响")
        void shouldRecoverJobAfterSelfHealing() throws IOException {
            wire(true);

            final FakeTask taskA = new FakeTask(Long.parseLong(JOB_A));
            final FakeTask taskB = new FakeTask(Long.parseLong(JOB_B));

            // 故障注入直至 A 熔断打开
            assertThat(driveUntilCircuitOpen(taskA)).isTrue();
            assertThatThrownBy(() -> adapter.beforeTaskCall(taskA))
                    .as("熔断打开后 A 应被硬拒绝")
                    .isInstanceOf(GovernanceRejectException.class);

            // 自愈闭环：OPEN 等待窗口（wait-duration=0，下一毫秒即到期）后转 HALF_OPEN，放行试探请求；
            // 首轮可能仍处于 OPEN 窗口被拒，重试直至自愈放行
            int halfOpenPasses = 0;
            int rejectedDuringOpenWindow = 0;
            final long deadline = System.currentTimeMillis() + 2_000L;
            while (System.currentTimeMillis() < deadline && halfOpenPasses == 0) {
                try {
                    adapter.beforeTaskCall(taskA);
                    adapter.afterTaskCall(taskA, null); // 试探请求成功
                    halfOpenPasses = 1;
                } catch (GovernanceRejectException e) {
                    // 仍在 OPEN 窗口，等待自愈（累计被拒次数供诊断）
                    rejectedDuringOpenWindow++;
                }
            }
            assertThat(halfOpenPasses)
                    .as("等待窗口到期后熔断器应 HALF_OPEN 放行试探请求（OPEN 窗口累计被拒 %d 次）", rejectedDuringOpenWindow)
                    .isEqualTo(1);

            // 补齐 HALF_OPEN 成功配额（灵核默认 10）→ 熔断器转 CLOSED
            for (int i = 0; i < 9; i++) {
                adapter.beforeTaskCall(taskA);
                adapter.afterTaskCall(taskA, null);
            }

            // A 恢复：CLOSED 后批次正常放行
            assertThatCode(() -> {
                adapter.beforeTaskCall(taskA);
                adapter.afterTaskCall(taskA, null);
            }).as("自愈后作业 A 应重新放行").doesNotThrowAnyException();

            // B 全程不受影响
            assertThatCode(() -> {
                adapter.beforeTaskCall(taskB);
                adapter.afterTaskCall(taskB, null);
            }).as("作业 B 全程应 100% 放行").doesNotThrowAnyException();
            assertThat(failedRequests(LING_B)).as("作业 B 不应记录任何失败").isZero();
        }
    }

    @Nested
    @DisplayName("场景3：fail-closed=false 安全基线")
    class SoftPathBaseline {

        @Test
        @DisplayName("fail-closed=false 时 A 熔断打开但软退避放行（无新增拒绝语义），B 100% 放行")
        void shouldSoftPassThroughWhenFailOpen() throws IOException {
            wire(false);

            final FakeTask taskA = new FakeTask(Long.parseLong(JOB_A));
            final FakeTask taskB = new FakeTask(Long.parseLong(JOB_B));

            // 故障注入：连续喂下游故障，窗口失败率拉满（熔断必开），但软路径不抛拒绝
            assertThatCode(() -> {
                for (int i = 0; i < 20; i++) {
                    adapter.beforeTaskCall(taskA);
                    adapter.afterTaskCall(taskA, DOWNSTREAM_FAILURE);
                }
            }).as("fail-closed=false 时熔断打开应软退避放行，不抛拒绝异常")
                    .doesNotThrowAnyException();

            // A 的失败确实被记录（熔断器对真实业务结果敏感）
            assertThat(runtime.getMetricsCollector().get(LING_A)).isNotNull();
            assertThat(failedRequests(LING_A)).as("A 的失败应计入作业 A 灵元").isGreaterThan(0);

            // B 100% 放行 + 零失败（隔离不受软退避影响）
            assertThatCode(() -> {
                adapter.beforeTaskCall(taskB);
                adapter.afterTaskCall(taskB, null);
            }).as("作业 B 在 A 故障期间应 100% 放行").doesNotThrowAnyException();
            assertThat(runtime.getMetricsCollector().get(LING_B)).isNotNull();
            assertThat(failedRequests(LING_B)).as("作业 B 不应记录任何失败").isZero();
        }
    }
}
