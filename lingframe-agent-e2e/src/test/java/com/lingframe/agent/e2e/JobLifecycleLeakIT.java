package com.lingframe.agent.e2e;

import com.lingframe.agent.adapter.JobIdExtractor;
import com.lingframe.agent.adapter.JobLingRegistry;
import com.lingframe.agent.adapter.SeaTunnelAdapter;
import com.lingframe.agent.bridge.LingFrameAgentBridge;
import com.lingframe.agent.config.AgentConfig;
import com.lingframe.agent.config.HazelcastConfigCenter;
import com.lingframe.agent.pipeline.AgentGovernanceRuntime;
import com.lingframe.agent.pipeline.AgentPipelineFactory;
import com.lingframe.core.ling.LingRuntime;
import com.lingframe.core.ling.LingRuntimeConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 作业级治理验收：千级作业生命周期零残留。
 * <p>
 * 进程内装配真实微内核链（与 {@link JobIsolationIT} 同款），以极小 TTL / Reaper 节流
 * 驱动 1000 个作业的「注册 → 空闲过期 → 机会式回收」完整生命周期，
 * 断言回收链（unregister → evictLingResources → metrics.remove）后
 * LingRepository / MetricsCollector / JobLingRegistry 对 {@code seatunnel-job-*}
 * 零残留，共享灵元 {@code seatunnel} 完好。
 * <p>
 * 不依赖 Docker / 真实 SeaTunnel，可在常规构建中确定性执行；总耗时约 3s
 * （两轮 TTL 等待，每轮 1.5s）+ 注册开销。
 */
@DisplayName("千级作业生命周期零残留验收")
class JobLifecycleLeakIT {

    /** 千级作业规模（对应 per-job-max-tracked-jobs 默认 1024 之内的压测规模）。 */
    private static final int JOB_COUNT = 1000;
    /** 空闲回收 TTL / Reaper 节流均取 JobLingRegistry 最小钳制值 1s，压缩测试等待。 */
    private static final long TINY_TTL_MS = 1_000L;
    private static final long TINY_REAP_INTERVAL_MS = 1_000L;
    /** 空闲超 TTL 后的额外等待，确保所有作业进入「可回收」状态。 */
    private static final long IDLE_WAIT_MS = 1_500L;

    @TempDir
    Path tempDir;

    private AgentGovernanceRuntime runtime;
    private SeaTunnelAdapter adapter;
    private JobLingRegistry registry;

    /** 伪任务宿主：携带作业身份字段（镜像 AbstractTask 的 jobID 契约）。 */
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

    private void wireLeakScenario() throws IOException {
        final StringBuilder yaml = new StringBuilder()
                .append("governance:\n")
                .append("  enabled: true\n")
                .append("  task-execution-advice-enabled: true\n")
                .append("  per-job-governance-enabled: true\n")
                .append("  per-job-max-tracked-jobs: 1024\n")
                .append("  per-job-idle-ttl-ms: ").append(TINY_TTL_MS).append('\n')
                .append("  per-job-reap-interval-ms: ").append(TINY_REAP_INTERVAL_MS).append('\n')
                .append("  resilience:\n")
                .append("    circuit-breaker-enabled: true\n")
                .append("    rate-limiter-enabled: true\n")
                .append("    fail-closed: false\n")
                .append("    circuit-breaker-failure-rate-threshold: 50\n")
                .append("    circuit-breaker-sliding-window-size: 20\n")
                .append("    circuit-breaker-minimum-number-of-calls: 10\n")
                .append("    rate-limit-per-second: 100000\n");
        final Path cfg = tempDir.resolve("lingframe-leak-it.yaml");
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
        registry = new JobLingRegistry(
                runtime.getVirtualLingManager(),
                runtime.getPipelineEngine(),
                runtime.getMetricsCollector(),
                LingRuntimeConfig.builder().rateLimitPerSecond(100000)
                        .circuitBreakerFailureRateThreshold(50)
                        .circuitBreakerSlidingWindowSize(20)
                        .circuitBreakerMinimumNumberOfCalls(10)
                        .bulkheadMaxConcurrent(10).build(),
                1024, TINY_TTL_MS, TINY_REAP_INTERVAL_MS);
        adapter.setJobLevelGovernance(extractor, registry);
        LingFrameAgentBridge.registerContract(adapter);
    }

    @Test
    @DisplayName("1000 作业注册→过期→回收后，灵元/指标/记账零残留，共享灵元完好")
    void shouldRecycleAllJobLingsWithZeroResidue() throws Exception {
        wireLeakScenario();

        // 阶段 1：注册 1000 个作业（各驱动一次 beforeTaskCall + afterTaskCall）
        for (int i = 0; i < JOB_COUNT; i++) {
            final FakeTask task = new FakeTask(i);
            adapter.beforeTaskCall(task);
            adapter.afterTaskCall(task, null);
        }

        assertThat(registry.trackedCount())
                .as("1000 个作业应全部被跟踪")
                .isEqualTo(JOB_COUNT);
        assertThat(runtime.getVirtualLingManager().hasRuntime("seatunnel-job-0")).isTrue();
        assertThat(runtime.getVirtualLingManager().hasRuntime("seatunnel-job-" + (JOB_COUNT - 1))).isTrue();
        assertThat(runtime.getMetricsCollector().get("seatunnel-job-0")).isNotNull();
        // 共享灵元始终在位
        assertThat(runtime.getVirtualLingManager().hasRuntime("seatunnel")).isTrue();

        // 阶段 2：全部空闲超 TTL，用新作业驱动机会式 Reaper
        Thread.sleep(IDLE_WAIT_MS);
        final FakeTask driver1 = new FakeTask(JOB_COUNT);
        adapter.beforeTaskCall(driver1);
        adapter.afterTaskCall(driver1, null);

        assertThat(registry.trackedCount())
                .as("过期作业应全部被回收，仅剩本次驱动作业")
                .isEqualTo(1);
        assertOnlyActiveJobLingResidue(JOB_COUNT);

        // 阶段 3：驱动作业也空闲过期，再触发一轮回收
        Thread.sleep(IDLE_WAIT_MS);
        final FakeTask driver2 = new FakeTask(JOB_COUNT + 1);
        adapter.beforeTaskCall(driver2);
        adapter.afterTaskCall(driver2, null);

        assertThat(registry.trackedCount())
                .as("第二轮回收后仅剩当前驱动作业")
                .isEqualTo(1);
        assertOnlyActiveJobLingResidue(JOB_COUNT + 1);

        // 最终：共享灵元完好（在位且未被回收链误伤）。测试全程走作业路径，共享灵元无调用
        // 故无健康指标——指标是调用侧产物，其「完好」以 LingRepository 在位为准。
        assertThat(runtime.getVirtualLingManager().getRuntime("seatunnel"))
                .as("共享灵元应始终在位且未被回收链误伤")
                .isNotNull();
        assertThat(runtime.getLingRepository().hasRuntime("seatunnel")).isTrue();
    }

    /**
     * 断言过期作业零残留：LingRepository 与 MetricsCollector 中所有 {@code seatunnel-job-*}
     * 条目必须恰好等于当前活跃驱动作业（其余 1000 个过期作业已被完整回收）。
     * <p>
     * 口径说明：活跃驱动作业刚注册，其灵元/指标理应存在，故不能断言「绝无任何作业灵元」，
     * 而是断言「除活跃作业外零残留」——这才能真实守住「回收链完整」的验收红线。
     */
    private void assertOnlyActiveJobLingResidue(long activeJobId) {
        final String expectedLingId = HazelcastConfigCenter.JOB_LING_PREFIX + activeJobId;

        final List<String> jobLingIds = runtime.getLingRepository().getAllRuntimes().stream()
                .map(LingRuntime::getLingId)
                .filter(id -> id != null && id.startsWith(HazelcastConfigCenter.JOB_LING_PREFIX))
                .collect(Collectors.toList());
        assertThat(jobLingIds)
                .as("过期作业灵元应全部回收，LingRepository 仅保留活跃作业 %s", expectedLingId)
                .containsExactly(expectedLingId);

        final Set<String> jobMetricIds = runtime.getMetricsCollector().getAllMetrics().keySet().stream()
                .filter(id -> id.startsWith(HazelcastConfigCenter.JOB_LING_PREFIX))
                .collect(Collectors.toSet());
        assertThat(jobMetricIds)
                .as("过期作业健康指标应全部回收，MetricsCollector 仅保留活跃作业 %s", expectedLingId)
                .containsExactly(expectedLingId);
    }
}
