package com.lingframe.agent.e2e;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.lingframe.agent.adapter.GovernanceRejectException;
import com.lingframe.agent.adapter.JobIdExtractor;
import com.lingframe.agent.adapter.JobLingRegistry;
import com.lingframe.agent.adapter.SeaTunnelAdapter;
import com.lingframe.agent.bridge.LingFrameAgentBridge;
import com.lingframe.agent.config.AgentConfig;
import com.lingframe.agent.config.HazelcastConfigCenter;
import com.lingframe.agent.pipeline.AgentGovernanceRuntime;
import com.lingframe.agent.pipeline.AgentPipelineFactory;
import com.lingframe.core.ling.LingRepository;
import com.lingframe.core.ling.LingRuntime;
import com.lingframe.core.ling.LingRuntimeConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 作业级配置刷新验收：真实 Hazelcast 配置中心 + 真实微内核链。
 * <p>
 * 与 {@code HazelcastConfigCenterTest}（mock IMap 单测）不同，本 IT 启动真实嵌入式
 * Hazelcast 实例，验证生产时序与参数真正落地：
 * <ol>
 *   <li>Agent premain 先于 Hazelcast 启动 → 首次拦截 tryInit 自动发现已启动实例；</li>
 *   <li>作业级 key {@code job.{jobId}.{bareKey}} 热刷仅刷新目标作业灵元，且新参数被
 *       治理器真实消费（熔断/限流行为随之变化），不污染其他作业与共享灵元；</li>
 *   <li>全局裸 key 变更广播到共享灵元与全部作业灵元，作业级 key 覆盖优先（缺省逐级回退）。</li>
 * </ol>
 * <p>
 * 配置 key 直接使用字面量（core 中 {@code HazelcastConfigCenter.KEY_*} 为包私有，跨包不可见）。
 */
@DisplayName("作业级配置刷新验收（真实 Hazelcast）")
class JobConfigRefreshIT {

    /** 下游可用性失败（喂给熔断器 onError 的典型信号）。 */
    private static final IOException DOWNSTREAM_FAILURE =
            new IOException("connection refused");

    private static final String JOB_A = "7";
    private static final String JOB_B = "8";

    /** 配置 IMap 的 key（与 core HazelcastConfigCenter.KEY_* 保持一致）。 */
    private static final String KEY_RATE_LIMIT = "rate-limit-per-second";
    private static final String KEY_CB_FAILURE_RATE = "circuit-breaker-failure-rate-threshold";

    /** 伪任务宿主：携带作业身份字段（镜像 AbstractTask 的 jobID 契约）。 */
    static final class FakeTask {
        public final long jobID;

        FakeTask(long jobID) {
            this.jobID = jobID;
        }
    }

    private static HazelcastInstance hz;
    private static IMap<String, String> configMap;

    @TempDir
    Path tempDir;

    private AgentGovernanceRuntime runtime;
    private SeaTunnelAdapter adapter;
    private HazelcastConfigCenter configCenter;
    private LingRepository lingRepository;
    private AgentConfig config;

    @BeforeAll
    static void startHazelcast() {
        // 模拟 SeaTunnel 内部 Hazelcast 实例：Agent premain 在此实例创建之前已执行
        hz = Hazelcast.newHazelcastInstance();
        configMap = hz.getMap(HazelcastConfigCenter.CONFIG_IMAP_NAME);
    }

    @AfterAll
    static void stopHazelcast() {
        if (hz != null) {
            hz.shutdown();
        }
    }

    @BeforeEach
    void setUp() {
        LingFrameAgentBridge.registerContract(null);
        // 清空上一用例写入的配置，避免 IMap 残留交叉污染
        configMap.clear();
    }

    @AfterEach
    void tearDown() {
        LingFrameAgentBridge.registerContract(null);
    }

    /** 构造显式写入临时文件的 yaml 配置并加载为 AgentConfig。 */
    private AgentConfig loadConfig(boolean perJob, int cbThreshold, int cbWindow) throws IOException {
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
                .append("    fail-closed: true\n")
                .append("    circuit-breaker-failure-rate-threshold: ").append(cbThreshold).append('\n')
                .append("    circuit-breaker-sliding-window-size: ").append(cbWindow).append('\n')
                .append("    circuit-breaker-minimum-number-of-calls: 1\n")
                .append("    rate-limit-per-second: 100000\n");
        final Path cfg = tempDir.resolve("lingframe-config-refresh-it.yaml");
        Files.write(cfg, yaml.toString().getBytes(StandardCharsets.UTF_8));
        return AgentConfig.load(cfg.toString());
    }

    /** premain 同款装配：runtime + adapter + jobLevelGovernance。 */
    private void wirePerJob() throws IOException {
        config = loadConfig(true, 10, 5);
        runtime = AgentPipelineFactory.create(config);
        configCenter = runtime.getConfigCenter();
        lingRepository = runtime.getLingRepository();
        adapter = new SeaTunnelAdapter(
                config,
                runtime.getPipelineEngine(),
                runtime.getUnloadCoordinator(),
                configCenter,
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
                        .bulkheadMaxConcurrent(10).build(),
                1024, 1_800_000L, 300_000L);
        adapter.setJobLevelGovernance(extractor, registry);
        LingFrameAgentBridge.registerContract(adapter);
    }

    /** 注册作业灵元：驱动一次完整批次（before + after 成功）。 */
    private void driveOnce(long jobId) {
        final FakeTask task = new FakeTask(jobId);
        adapter.beforeTaskCall(task);
        adapter.afterTaskCall(task, null);
    }

    private LingRuntime ling(String id) {
        final LingRuntime rt = lingRepository.getRuntime(id);
        assertThat(rt).as("灵元 %s 应已注册", id).isNotNull();
        return rt;
    }

    /** 自旋等待：Hazelcast 事件监听器在内部线程异步回调，需轮询配置值收敛。 */
    private void awaitUntil(BooleanSupplier condition, String message) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20L);
        }
        throw new AssertionError("awaitUntil 超时: " + message);
    }

    @Nested
    @DisplayName("真实实例自动发现与初始配置生效")
    class AutoDiscoveryAndInitialConfig {

        @Test
        @DisplayName("首次拦截应自动发现已启动的 Hazelcast 实例并应用初始全局配置")
        void shouldAutoDiscoverInstanceAndApplyInitialConfig() throws Exception {
            configMap.put(KEY_RATE_LIMIT, "600");

            wirePerJob();

            // 首次拦截：ensureConfigCenterInit → tryInit → getAllHazelcastInstances 自动发现
            driveOnce(Long.parseLong(JOB_A));

            assertThat(configCenter.isInitialized()).as("配置中心应自动发现真实实例并初始化成功").isTrue();
            assertThat(ling("seatunnel").getConfig().getRateLimitPerSecond())
                    .as("applyInitialConfig 应将预写全局限流刷入共享灵元").isEqualTo(600);
        }
    }

    @Nested
    @DisplayName("作业级 key 热刷新")
    class JobLevelHotRefresh {

        @Test
        @DisplayName("作业级限流热刷仅影响目标作业，且业务生效（限流器重建）")
        void shouldHotRefreshJobLevelRateLimitOnlyForTargetJob() throws Exception {
            wirePerJob();
            driveOnce(Long.parseLong(JOB_A));
            driveOnce(Long.parseLong(JOB_B));

            // 运维写入作业级限流：job.7 → 2/s；job.8 与共享灵元不写
            configMap.put("job.7." + KEY_RATE_LIMIT, "2");

            awaitUntil(() -> ling("seatunnel-job-7").getConfig().getRateLimitPerSecond() == 2,
                    "job-7 限流应热刷为 2/s");

            assertThat(ling("seatunnel-job-8").getConfig().getRateLimitPerSecond())
                    .as("job-8 不应受 job.7 作业级 key 影响").isEqualTo(100000);
            assertThat(ling("seatunnel").getConfig().getRateLimitPerSecond())
                    .as("共享灵元不应受 job.7 作业级 key 影响").isEqualTo(100000);
        }

        @Test
        @DisplayName("作业级熔断阈值热刷应被熔断器真实消费：job-7 高阈值不熔断，job-8 低阈值熔断打开")
        void shouldHotRefreshJobLevelCircuitBreakerThresholdAndTakeEffect() throws Exception {
            wirePerJob();
            driveOnce(Long.parseLong(JOB_A));
            driveOnce(Long.parseLong(JOB_B));

            // 运维把 job-7 熔断阈值提到 100（永不打开）；job-8 保持模板阈值 10
            configMap.put("job.7." + KEY_CB_FAILURE_RATE, "100");
            awaitUntil(() -> ling("seatunnel-job-7").getConfig().getCircuitBreakerFailureRateThreshold() == 100,
                    "job-7 熔断阈值应热刷为 100");

            assertThat(ling("seatunnel-job-8").getConfig().getCircuitBreakerFailureRateThreshold())
                    .as("job-8 不应受 job.7 作业级 key 影响").isEqualTo(10);

            // job-7：混合喂 4 成功 + 1 失败（任何 5 调用窗口失败率恒 ≤ 20%）。
            // 若熔断器已消费新阈值 100 → 永不打开；若仍按旧阈值 10 → 20% > 10% 必打开并抛异常。
            for (int i = 0; i < 30; i++) {
                for (int k = 0; k < 4; k++) {
                    adapter.beforeTaskCall(new FakeTask(Long.parseLong(JOB_A)));
                    adapter.afterTaskCall(new FakeTask(Long.parseLong(JOB_A)), null);
                }
                adapter.beforeTaskCall(new FakeTask(Long.parseLong(JOB_A)));
                adapter.afterTaskCall(new FakeTask(Long.parseLong(JOB_A)), DOWNSTREAM_FAILURE);
            }
            assertThat(runtime.getVirtualLingManager().hasRuntime("seatunnel-job-7"))
                    .as("job-7 阈值 100 下混合失败（20%）不应熔断，隔离断言").isTrue();

            // job-8：全失败 → 按阈值 10 熔断打开 → fail-closed 硬拒绝
            int driven = 0;
            boolean opened = false;
            for (int i = 0; i < 100 && !opened; i++) {
                try {
                    adapter.beforeTaskCall(new FakeTask(Long.parseLong(JOB_B)));
                    adapter.afterTaskCall(new FakeTask(Long.parseLong(JOB_B)), DOWNSTREAM_FAILURE);
                    driven++;
                } catch (GovernanceRejectException e) {
                    opened = true;
                }
            }
            assertThat(opened).as("job-8 阈值 10 全失败应熔断打开（driven=%d）", driven).isTrue();
            assertThatThrownBy(() -> adapter.beforeTaskCall(new FakeTask(Long.parseLong(JOB_B))))
                    .as("job-8 熔断打开期间应被硬拒绝")
                    .isInstanceOf(GovernanceRejectException.class);
        }
    }

    @Nested
    @DisplayName("全局广播与作业级覆盖优先")
    class GlobalBroadcastAndOverridePriority {

        @Test
        @DisplayName("全局 key 变更广播到全部灵元，作业级 key 覆盖优先")
        void shouldBroadcastGlobalKeyWithJobLevelOverridePriority() throws Exception {
            wirePerJob();
            driveOnce(Long.parseLong(JOB_A));
            driveOnce(Long.parseLong(JOB_B));

            // 先写作业级覆盖 job.7 → 100
            configMap.put("job.7." + KEY_CB_FAILURE_RATE, "100");
            awaitUntil(() -> ling("seatunnel-job-7").getConfig().getCircuitBreakerFailureRateThreshold() == 100,
                    "job-7 作业级阈值应先热刷为 100");

            // 再广播全局阈值 50 → job-8 / 共享灵元应变 50，job-7 保持 100（覆盖优先）
            configMap.put(KEY_CB_FAILURE_RATE, "50");
            awaitUntil(() -> ling("seatunnel-job-8").getConfig().getCircuitBreakerFailureRateThreshold() == 50,
                    "全局阈值广播应使 job-8 回退为 50");

            assertThat(ling("seatunnel").getConfig().getCircuitBreakerFailureRateThreshold())
                    .as("共享灵元应随全局广播变为 50").isEqualTo(50);
            assertThat(ling("seatunnel-job-7").getConfig().getCircuitBreakerFailureRateThreshold())
                    .as("job-7 应保持作业级覆盖值 100").isEqualTo(100);
        }
    }
}
