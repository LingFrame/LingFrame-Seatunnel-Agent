package com.lingframe.agent.e2e;

import com.lingframe.agent.adapter.GovernanceRejectException;
import com.lingframe.agent.adapter.JobIdExtractor;
import com.lingframe.agent.adapter.JobLingRegistry;
import com.lingframe.agent.adapter.SeaTunnelAdapter;
import com.lingframe.agent.bridge.LingFrameAgentBridge;
import com.lingframe.agent.config.AgentConfig;
import com.lingframe.agent.config.HazelcastConfigCenter;
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
 * 作业级治理验收：作业故障注入隔离。
 * <p>
 * 进程内装配真实微内核链（AgentPipelineFactory + VirtualLingManager + JobLingRegistry +
 * SeaTunnelAdapter），用伪任务宿主（携带 {@code jobID}）驱动两个独立作业，
 * 覆盖三条核心验收语义：
 * <ol>
 *   <li>#1：作业 A 熔断打开期间，作业 B 批次 100% 放行（隔离断言）；</li>
 *   <li>#3：task 无 job 字段（映射未知版本改名）→ 降级共享灵元，不猜测；</li>
 *   <li>#4：per-job-governance-enabled=false → 逐字节回到引擎级共享灵元。</li>
 * </ol>
 * 本 IT 不依赖 Docker / 真实 SeaTunnel，可在常规构建中确定性执行。
 */
@DisplayName("作业级隔离故障注入验收")
class JobIsolationIT {

    /** 下游可用性失败（喂给熔断器 onError 的典型信号）。 */
    private static final IOException DOWNSTREAM_FAILURE =
            new IOException("connection refused");

    private static final String JOB_A = "7";
    private static final String JOB_B = "8";

    @TempDir
    Path tempDir;

    private AgentGovernanceRuntime runtime;
    private SeaTunnelAdapter adapter;
    private JobLingRegistry registry;
    private AgentConfig config;

    /** 伪任务宿主：携带作业身份字段（镜像 AbstractTask 的 jobID 契约；跨包用 public 确保可反射读取）。 */
    static final class FakeTask {
        public final long jobID;

        FakeTask(long jobID) {
            this.jobID = jobID;
        }
    }

    /** 无作业身份的宿主（镜像未知版本 jobID 改名后的形态）。 */
    static final class NoJobIdTask {
    }

    @BeforeEach
    void setUp() {
        LingFrameAgentBridge.registerContract(null);
    }

    @AfterEach
    void tearDown() {
        LingFrameAgentBridge.registerContract(null);
    }

    /** 构造显式写入临时文件的 yaml 配置并加载为 AgentConfig。 */
    private AgentConfig loadConfig(boolean perJob, boolean failClosed, int cbThreshold, int cbWindow) throws IOException {
        final StringBuilder yaml = new StringBuilder()
                .append("governance:\n")
                .append("  enabled: true\n")
                .append("  task-execution-advice-enabled: true\n")
                .append("  per-job-governance-enabled: ").append(Boolean.toString(perJob)).append('\n')
                .append("  per-job-max-tracked-jobs: 1024\n")
                .append("  per-job-idle-ttl-ms: 1800000\n")
                .append("  per-job-reap-interval-ms: 300000\n")
                .append("  resilience:\n")
                .append("    circuit-breaker-enabled: true\n")
                .append("    rate-limiter-enabled: true\n")
                .append("    fail-closed: ").append(Boolean.toString(failClosed)).append('\n')
                .append("    circuit-breaker-failure-rate-threshold: ").append(cbThreshold).append('\n')
                .append("    circuit-breaker-sliding-window-size: ").append(cbWindow).append('\n')
                .append("    circuit-breaker-minimum-number-of-calls: 1\n")
                .append("    rate-limit-per-second: 100000\n");
        final Path cfg = tempDir.resolve("lingframe-governance-it.yaml");
        Files.write(cfg, yaml.toString().getBytes(StandardCharsets.UTF_8));
        return AgentConfig.load(cfg.toString());
    }

    private AgentConfig perJobConfig(int cbThreshold, int cbWindow) throws IOException {
        return loadConfig(true, true, cbThreshold, cbWindow);
    }

    private AgentConfig sharedConfig(int cbThreshold, int cbWindow) throws IOException {
        return loadConfig(false, true, cbThreshold, cbWindow);
    }

    /** premain 同款装配：runtime + adapter + jobLevelGovernance；返回完整运行时。 */
    private void wirePerJob(String aJob, String bJob) throws IOException {
        config = perJobConfig(10, 5);
        runtime = AgentPipelineFactory.create(config);
        adapter = new SeaTunnelAdapter(
                config,
                runtime.getPipelineEngine(),
                runtime.getUnloadCoordinator(),
                runtime.getConfigCenter(),
                runtime.getEventBus(),
                runtime.getMetricsCollector());
        final JobIdExtractor extractor = new JobIdExtractor(true);
        registry = new JobLingRegistry(
                runtime.getVirtualLingManager(),
                runtime.getPipelineEngine(),
                runtime.getMetricsCollector(),
                LingRuntimeConfig.builder().rateLimitPerSecond(100000)
                        .circuitBreakerFailureRateThreshold(10)
                        .circuitBreakerSlidingWindowSize(5)
                        .circuitBreakerMinimumNumberOfCalls(1)
                        .bulkheadMaxConcurrent(10).build(),
                1024, 1_800_000L, 300_000L);
        adapter.setJobLevelGovernance(extractor, registry);
        LingFrameAgentBridge.registerContract(adapter);
    }

    private void wireShared() throws IOException {
        config = sharedConfig(10, 5);
        runtime = AgentPipelineFactory.create(config);
        adapter = new SeaTunnelAdapter(
                config,
                runtime.getPipelineEngine(),
                runtime.getUnloadCoordinator(),
                runtime.getConfigCenter(),
                runtime.getEventBus(),
                runtime.getMetricsCollector());
        // 不注入 extractor / registry → 强制回退共享灵元（per-job 关闭，与引擎级治理一致）
        LingFrameAgentBridge.registerContract(adapter);
    }

    @Nested
    @DisplayName("#1 熔断打开期间作业隔离")
    class CircuitOpenIsolation {

        @Test
        @DisplayName("作业 A 熔断打开时硬拒绝，作业 B 批次 100% 放行（核心断言）")
        void shouldIsolateCircuitOpenToTargetJobOnly() throws IOException {
            wirePerJob(JOB_A, JOB_B);

            final FakeTask taskA = new FakeTask(Long.parseLong(JOB_A));
            final FakeTask taskB = new FakeTask(Long.parseLong(JOB_B));

            // 故障风暴：连续喂下游可用性失败 + 伴随 beforeTaskCall，直至 A 熔断打开
            final int maxDrives = 100;
            int driven = 0;
            boolean opened = false;
            for (int i = 0; i < maxDrives && !opened; i++) {
                try {
                    adapter.beforeTaskCall(taskA);
                    adapter.afterTaskCall(taskA, DOWNSTREAM_FAILURE);
                    driven++;
                } catch (GovernanceRejectException e) {
                    opened = true; // fail-closed 下 CIRCUIT_OPEN 硬拒绝由此抵达
                }
            }

            assertThat(opened)
                    .as("作业 A 熔断应在持续失败后打开（driven=%d）", driven)
                    .isTrue();

            // A 熔断打开期间，再次 beforeTaskCall(A) 应被硬拒绝
            assertThatThrownBy(() -> adapter.beforeTaskCall(taskA))
                    .as("作业 A 处于熔断打开应被硬拒绝")
                    .isInstanceOf(GovernanceRejectException.class);

            // B 应 100% 放行（不受 A 影响）：before+after 全程不抛异常
            assertThatCode(() -> {
                adapter.beforeTaskCall(taskB);
                adapter.afterTaskCall(taskB, null);
            }).as("作业 B 在作业 A 熔断打开期间应 100% 放行")
                    .doesNotThrowAnyException();

            // 隔离证据：B 落到了自己的作业灵元而非 A 的
            assertThat(runtime.getVirtualLingManager().hasRuntime("seatunnel-job-7")).isTrue();
            assertThat(runtime.getVirtualLingManager().hasRuntime("seatunnel-job-8")).isTrue();
            assertThat(runtime.getMetricsCollector().get("seatunnel-job-8")).isNotNull();
        }
    }

    @Nested
    @DisplayName("#3 未知版本降级")
    class UnknownVersionDegradation {

        @Test
        @DisplayName("task 无 job 字段（等价 unknown version 改名）应降级共享灵元，不猜测")
        void shouldDegradeToSharedLingWhenJobIdFieldAbsent() throws IOException {
            wirePerJob(JOB_A, JOB_B);

            final NoJobIdTask task = new NoJobIdTask();
            adapter.beforeTaskCall(task);
            adapter.afterTaskCall(task, null);

            // 未创建任何 seatunnel-job-* 灵元，全部落到共享灵元 seatunnel
            final boolean hasAnyJobLing = runtime.getLingRepository().getAllRuntimes().stream()
                    .anyMatch(rt -> rt.getLingId() != null
                            && rt.getLingId().startsWith(HazelcastConfigCenter.JOB_LING_PREFIX));
            assertThat(hasAnyJobLing).as("无 job 字段的 task 不应建立作业灵元").isFalse();
            assertThat(runtime.getMetricsCollector().get("seatunnel")).isNotNull();
        }
    }

    @Nested
    @DisplayName("#4 per-job 关闭兼容")
    class PerJobDisabled {

        @Test
        @DisplayName("per-job-governance-enabled=false 时带 jobID 的 task 也应走共享灵元")
        void shouldUseSharedLingWhenPerJobDisabled() throws IOException {
            wireShared();

            final FakeTask task = new FakeTask(Long.parseLong(JOB_A));
            adapter.beforeTaskCall(task);
            adapter.afterTaskCall(task, null);

            assertThat(runtime.getMetricsCollector().get("seatunnel")).isNotNull();
            assertThat(runtime.getVirtualLingManager().hasRuntime("seatunnel-job-7")).isFalse();
        }
    }
}