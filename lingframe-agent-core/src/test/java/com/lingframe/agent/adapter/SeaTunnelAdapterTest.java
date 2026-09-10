package com.lingframe.agent.adapter;

import com.lingframe.agent.bridge.ReleasedClassLoaderRegistry;
import com.lingframe.agent.config.AgentConfig;
import com.lingframe.agent.config.HazelcastConfigCenter;
import com.lingframe.agent.config.TestAgentConfigs;
import com.lingframe.agent.pipeline.AgentGovernanceRuntime;
import com.lingframe.agent.pipeline.AgentPipelineFactory;
import com.lingframe.api.exception.LingInvocationException;
import com.lingframe.api.event.LingEventListener;
import com.lingframe.core.event.EventBus;
import com.lingframe.core.event.monitor.MonitoringEvents;
import com.lingframe.core.ling.LingUnloadCoordinator;
import com.lingframe.core.metrics.LingHealthMetrics;
import com.lingframe.core.pipeline.InvocationPipelineEngine;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@DisplayName("SeaTunnelAdapter 适配器测试")
class SeaTunnelAdapterTest {

    private SeaTunnelAdapterTest() {
    }

    @Nested
    @DisplayName("物理释放委托")
    class PhysicalRelease {

        @Test
        @DisplayName("null ClassLoader 应安全跳过")
        void shouldSkipNullClassLoader() {
            final SeaTunnelAdapter adapter = createAdapter(true);
            adapter.onPhysicalRelease(null);
        }

        @Test
        @DisplayName("非 null ClassLoader 应注册到 ReleasedClassLoaderRegistry 并委托给 unloadCoordinator")
        void shouldDelegateToUnloadCoordinatorAndRegistry() {
            final LingUnloadCoordinator mockCoordinator = mock(LingUnloadCoordinator.class);
            final AgentConfig config = TestAgentConfigs.create(true, true, true, false, false, false);
            final InvocationPipelineEngine pipeline = new InvocationPipelineEngine(null);
            final SeaTunnelAdapter adapter = new SeaTunnelAdapter(config, pipeline, mockCoordinator, null, null, null);

            final ClassLoader loader = new ClassLoader() { };
            adapter.onPhysicalRelease(loader);

            assertThat(ReleasedClassLoaderRegistry.isReleased(loader)).isTrue();
            verify(mockCoordinator).onFailureCleanup(loader);
        }

        @Test
        @DisplayName("非 null ClassLoader 应正确注册到 ReleasedClassLoaderRegistry")
        void shouldRegisterReleasedClassLoader() throws Exception {
            final AgentConfig config = TestAgentConfigs.create(true, true, true, false, false, false);
            final SeaTunnelAdapter adapter = new SeaTunnelAdapter(config, null, null, null, null, null);

            final URLClassLoader urlClassLoader = new URLClassLoader(new URL[0], getClass().getClassLoader());
            adapter.onPhysicalRelease(urlClassLoader);
            assertThat(ReleasedClassLoaderRegistry.isReleased(urlClassLoader)).isTrue();
        }

        @Test
        @DisplayName("物理释放应同步将活动线程残留的 TCCL 重置为 SystemClassLoader")
        void shouldResetThreadContextClassLoaderOnPhysicalRelease() throws Exception {
            final AgentConfig config = TestAgentConfigs.create(true, true, true, false, false, false);
            final SeaTunnelAdapter adapter = new SeaTunnelAdapter(config, null, null, null, null, null);

            final ClassLoader loader = new ClassLoader() { };
            final CountDownLatch threadStarted = new CountDownLatch(1);
            final CountDownLatch threadEnd = new CountDownLatch(1);

            final Thread workerThread = new Thread(() -> {
                Thread.currentThread().setContextClassLoader(loader);
                threadStarted.countDown();
                try {
                    threadEnd.await(3, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "seatunnel-mock-tccl-worker");

            workerThread.start();
            try {
                assertThat(threadStarted.await(2, TimeUnit.SECONDS)).isTrue();
                assertThat(workerThread.getContextClassLoader()).isSameAs(loader);

                adapter.onPhysicalRelease(loader);

                assertThat(workerThread.getContextClassLoader()).isSameAs(ClassLoader.getSystemClassLoader());
            } finally {
                threadEnd.countDown();
                workerThread.join(2000);
            }
        }
    }

    @Nested
    @DisplayName("Jars Key 转换算法对齐")
    class JarKeyConversion {

        @Test
        @DisplayName("空集合应返回空字符串")
        void shouldReturnEmptyStringForEmptyCollection() {
            final SeaTunnelAdapter adapter = createAdapter(true);
            final String key = adapter.convertJarsToKey(Collections.emptyList());
            assertThat(key).isEmpty();
        }

        @Test
        @DisplayName("应与 SeaTunnel 官方 sorted + reduce(a + b) 算法输出严格一致")
        void shouldMatchSeaTunnelOfficialAlgorithm() throws MalformedURLException {
            final SeaTunnelAdapter adapter = createAdapter(true);
            final List<URL> jars = Arrays.asList(
                    new URL("file:/b.jar"),
                    new URL("file:/a.jar"),
                    new URL("file:/c.jar")
            );
            final String key = adapter.convertJarsToKey(jars);
            final String expected = jars.stream()
                    .map(URL::toString)
                    .sorted()
                    .reduce((a, b) -> a + b)
                    .orElse("");
            assertThat(key).isEqualTo(expected);
            assertThat(key).isEqualTo("file:/a.jarfile:/b.jarfile:/c.jar");
        }

        @Test
        @DisplayName("相同 URL 不同顺序应生成相同 Key（排序稳定性）")
        void shouldGenerateSameKeyForDifferentOrder() throws MalformedURLException {
            final SeaTunnelAdapter adapter = createAdapter(true);
            final List<URL> jars1 = Arrays.asList(
                    new URL("file:/b.jar"),
                    new URL("file:/a.jar")
            );
            final List<URL> jars2 = Arrays.asList(
                    new URL("file:/a.jar"),
                    new URL("file:/b.jar")
            );
            assertThat(adapter.convertJarsToKey(jars1))
                    .isEqualTo(adapter.convertJarsToKey(jars2));
        }
    }

    @Nested
    @DisplayName("beforeTaskCall Pipeline 前置治理")
    class BeforeTaskCallGovernance {

        @Test
        @DisplayName("Pipeline engine 为 null 时应安全跳过不抛异常")
        void shouldSkipWhenPipelineEngineNull() {
            final SeaTunnelAdapter adapter = createAdapterWithNullPipeline(true);
            adapter.beforeTaskCall();
            adapter.afterTaskCall(null);
        }

        @Test
        @DisplayName("治理启用时应正常完成前置治理")
        void shouldCompletePreGovernance() {
            final SeaTunnelAdapter adapter = createAdapter(true);
            adapter.beforeTaskCall();
            adapter.afterTaskCall(null);
        }

        @Test
        @DisplayName("治理禁用时应正常完成")
        void shouldCompleteWhenGovernanceDisabled() {
            final SeaTunnelAdapter adapter = createAdapter(false);
            adapter.beforeTaskCall();
            adapter.afterTaskCall(null);
        }
    }

    @Nested
    @DisplayName("afterTaskCall 结果回灌")
    class AfterTaskCallGovernance {

        @Test
        @DisplayName("传入 null 应正常完成（成功路径回灌 LingHealthMetrics）")
        void shouldNotThrowWhenErrorIsNull() {
            final SeaTunnelAdapter adapter = createAdapter(true);
            adapter.beforeTaskCall();
            adapter.afterTaskCall(null);
        }

        @Test
        @DisplayName("传入异常应正常完成（失败路径回灌 LingHealthMetrics）")
        void shouldNotThrowWhenErrorIsNotNull() {
            final SeaTunnelAdapter adapter = createAdapter(true);
            final RuntimeException error = new RuntimeException("batch failure");
            adapter.beforeTaskCall();
            adapter.afterTaskCall(error);
        }

        @Test
        @DisplayName("连续多批次 beforeTaskCall + afterTaskCall 应正常完成")
        void shouldNotThrowOnConsecutiveBatches() {
            final SeaTunnelAdapter adapter = createAdapter(true);
            for (int i = 0; i < 5; i++) {
                adapter.beforeTaskCall();
                adapter.afterTaskCall(null);
            }
        }

        @Test
        @DisplayName("超时异常应正确识别 isTimeout 标记")
        void shouldIdentifyTimeoutError() {
            final SeaTunnelAdapter adapter = createAdapter(true);
            final RuntimeException timeoutError = new RuntimeException("operation timed out after 30s");
            adapter.beforeTaskCall();
            adapter.afterTaskCall(timeoutError);
        }

        @Test
        @DisplayName("Pipeline engine 为 null 时 afterTaskCall 应安全清理 ThreadLocal")
        void shouldCleanThreadLocalWhenPipelineNull() {
            final SeaTunnelAdapter adapter = createAdapterWithNullPipeline(true);
            adapter.beforeTaskCall();
            adapter.afterTaskCall(null);
            adapter.beforeTaskCall();
            adapter.afterTaskCall(new RuntimeException("test"));
        }
    }

    @Nested
    @DisplayName("真实微内核治理穿透测试")
    class RealMicrokernelGovernance {

        @Test
        @DisplayName("真实微内核成功与可用性失败调用应准确回灌真实指标且无上下文泄漏")
        void shouldCollectRealMetricsOnSuccessAndFailure() {
            final AgentConfig config = TestAgentConfigs.create(
                    true,
                    true,
                    true,
                    false,
                    false,
                    true,
                    true,
                    100,
                    50,
                    100,
                    30_000
            );
            final AgentGovernanceRuntime runtime = AgentPipelineFactory.create(config);
            final SeaTunnelAdapter adapter = new SeaTunnelAdapter(
                    config,
                    runtime.getPipelineEngine(),
                    runtime.getUnloadCoordinator(),
                    runtime.getConfigCenter(),
                    runtime.getEventBus(),
                    runtime.getMetricsCollector()
            );

            for (int i = 0; i < 5; i++) {
                adapter.beforeTaskCall();
                adapter.afterTaskCall(null);
            }

            for (int i = 0; i < 2; i++) {
                adapter.beforeTaskCall();
                // 下游可用性失败（IOException 连接类）——仍计入熔断失败率
                adapter.afterTaskCall(new IOException("sink connection refused"));
            }

            final LingHealthMetrics metrics = runtime.getMetricsCollector().getOrCreate("seatunnel");
            assertThat(metrics).isNotNull();
            // 物理事实验证：
            // 1. beforeTaskCall 穿透真实微内核流水线，内建 TrafficMetricsFilter 记录 7 次准入成功
            // 2. afterTaskCall 回灌真实任务执行结果，记录 5 次业务成功 + 2 次可用性失败
            // 3. 两者在 LingHealthMetrics 统一累加，验证真实流水线与回灌通道全部生效且无上下文泄漏
            assertThat(metrics.getTotalRequests().sum()).isEqualTo(14L);
            assertThat(metrics.getSuccessRequests().sum()).isEqualTo(12L);
            assertThat(metrics.getFailedRequests().sum()).isEqualTo(2L);
        }

        @Test
        @DisplayName("触发真实限流时应安全退避且无上下文泄漏")
        void shouldTriggerRateLimitingWhenThresholdExceeded() {
            final AgentConfig config = TestAgentConfigs.create(
                    true,
                    true,
                    true,
                    false,
                    false,
                    true,
                    true,
                    1,
                    50,
                    100,
                    30_000
            );
            final AgentGovernanceRuntime runtime = AgentPipelineFactory.create(config);
            final SeaTunnelAdapter adapter = new SeaTunnelAdapter(
                    config,
                    runtime.getPipelineEngine(),
                    runtime.getUnloadCoordinator(),
                    runtime.getConfigCenter(),
                    runtime.getEventBus(),
                    runtime.getMetricsCollector()
            );

            adapter.beforeTaskCall();
            adapter.afterTaskCall(null);

            final long startNs = System.nanoTime();
            adapter.beforeTaskCall();
            final long durationMs = (System.nanoTime() - startNs) / 1_000_000;
            assertThat(durationMs).isGreaterThanOrEqualTo(90L);
            adapter.afterTaskCall(null);
        }

        @Test
        @DisplayName("未调用 beforeTaskCall 直接调用 afterTaskCall 应安全兜底且耗时不会纳秒溢出")
        void shouldHandleAfterTaskCallSafelyWithoutBeforeTaskCall() {
            final AgentConfig config = TestAgentConfigs.create(true, true, true, false, false, false);
            final AgentGovernanceRuntime runtime = AgentPipelineFactory.create(config);
            final SeaTunnelAdapter adapter = new SeaTunnelAdapter(
                    config,
                    runtime.getPipelineEngine(),
                    runtime.getUnloadCoordinator(),
                    runtime.getConfigCenter(),
                    runtime.getEventBus(),
                    runtime.getMetricsCollector()
            );

            adapter.afterTaskCall(null);

            final LingHealthMetrics metrics = runtime.getMetricsCollector().getOrCreate("seatunnel");
            assertThat(metrics.getTotalRequests().sum()).isEqualTo(1L);
            assertThat(metrics.getSuccessRequests().sum()).isEqualTo(1L);
            assertThat(metrics.getMaxLatencyMs().get()).isEqualTo(0L);
        }
    }

    @Nested
    @DisplayName("配置中心延迟初始化重试")
    class ConfigCenterLazyInit {

        @Test
        @DisplayName("初始化失败后应保留重试能力，且重试按间隔节流")
        void shouldRetryConfigCenterInitWithThrottle() throws Exception {
            final AgentConfig config = TestAgentConfigs.create(true, true, true, false, false, false);
            final HazelcastConfigCenter configCenter = new HazelcastConfigCenter(null);
            final SeaTunnelAdapter adapter = new SeaTunnelAdapter(config, null, null, configCenter, null, null);
            final Field retryAtField = SeaTunnelAdapter.class.getDeclaredField("nextConfigCenterRetryAt");
            retryAtField.setAccessible(true);

            // 首次调用：立即尝试初始化。测试环境无 Hazelcast 实例，tryInit 必然失败，
            // 但必须已推后下次重试时间——原实现用布尔标记一次性封死，本断言即回归防线
            adapter.beforeTaskCall();
            final AtomicLong retryAt = (AtomicLong) retryAtField.get(adapter);
            final long firstRetryAt = retryAt.get();
            assertThat(firstRetryAt).isGreaterThan(System.currentTimeMillis());
            assertThat(configCenter.isInitialized()).isFalse();

            // 节流窗口内再次调用不应重复探测（重试时间保持不变）
            adapter.beforeTaskCall();
            assertThat(retryAt.get()).isEqualTo(firstRetryAt);

            // 模拟重试间隔已过，应再次尝试初始化并重新推后重试时间
            retryAt.set(0L);
            adapter.beforeTaskCall();
            assertThat(retryAt.get()).isGreaterThan(System.currentTimeMillis());
        }

        @Test
        @DisplayName("配置中心为 null 时不应抛异常")
        void shouldSkipWhenConfigCenterNull() {
            final SeaTunnelAdapter adapter = createAdapterWithNullPipeline(true);
            adapter.beforeTaskCall();
            adapter.afterTaskCall(null);
        }
    }

    @Nested
    @DisplayName("熔断硬拒绝（fail-closed）")
    class FailClosedCircuitBreaker {

        @Test
        @DisplayName("fail-closed=true 且 CIRCUIT_OPEN 应抛出 GovernanceRejectException 真正拒绝批次")
        void shouldThrowOnCircuitOpenWhenFailClosed() {
            final AgentConfig config = TestAgentConfigs.create(
                    true, true, true, false, false, false, true, 100, 50, 20, 3000, true);
            final InvocationPipelineEngine pipeline = mock(InvocationPipelineEngine.class);
            doThrow(new LingInvocationException("seatunnel:seatunnel", LingInvocationException.ErrorKind.CIRCUIT_OPEN))
                    .when(pipeline).invoke(any());
            final SeaTunnelAdapter adapter = new SeaTunnelAdapter(
                    config, pipeline, null, null, null, null);

            assertThatThrownBy(adapter::beforeTaskCall)
                    .isInstanceOf(GovernanceRejectException.class)
                    .hasCauseInstanceOf(LingInvocationException.class);
        }

        @Test
        @DisplayName("fail-closed=false（默认）时 CIRCUIT_OPEN 软退避应直接放行、不 sleep（熔断自愈交半开探针）")
        void shouldPassthroughWhenFailClosedFalse() {
            final AgentConfig config = TestAgentConfigs.create(
                    true, true, true, false, false, false, true, 100, 50, 20, 3000, false);
            final InvocationPipelineEngine pipeline = mock(InvocationPipelineEngine.class);
            doThrow(new LingInvocationException("seatunnel:seatunnel", LingInvocationException.ErrorKind.CIRCUIT_OPEN))
                    .when(pipeline).invoke(any());
            final SeaTunnelAdapter adapter = new SeaTunnelAdapter(
                    config, pipeline, null, null, null, null);

            final long startNs = System.nanoTime();
            adapter.beforeTaskCall();
            final long durationMs = (System.nanoTime() - startNs) / 1_000_000;
            // 软退避不再 sleep：避免逐批次 100ms 串行阻塞 Worker
            assertThat(durationMs).isLessThan(50L);
        }

        @Test
        @DisplayName("fail-closed=true 但 RATE_LIMITED 仍应软退避（不硬拒绝，避免数据丢失）")
        void shouldBackOffOnRateLimitedEvenWhenFailClosed() {
            final AgentConfig config = TestAgentConfigs.create(
                    true, true, true, false, false, false, true, 10, 50, 20, 3000, true);
            final InvocationPipelineEngine pipeline = mock(InvocationPipelineEngine.class);
            doThrow(new LingInvocationException("seatunnel:seatunnel", LingInvocationException.ErrorKind.RATE_LIMITED))
                    .when(pipeline).invoke(any());
            final SeaTunnelAdapter adapter = new SeaTunnelAdapter(
                    config, pipeline, null, null, null, null);

            final long startNs = System.nanoTime();
            adapter.beforeTaskCall();
            final long durationMs = (System.nanoTime() - startNs) / 1_000_000;
            // rateLimit=10 → 令牌间隔 1000/10=100ms（含 ±20% 抖动），应在此区间，而非固定 100ms 或硬拒绝
            assertThat(durationMs).isBetween(60L, 500L);
        }
    }

    @Nested
    @DisplayName("失败率误计修复：仅下游可用性异常计入熔断失败率")
    class FailureRateMiscount {

        @Test
        @DisplayName("普通业务异常（数据/Transform 错误）不应计入熔断失败率")
        void shouldExcludeBusinessExceptionFromBreakerMetrics() {
            final AgentConfig config = TestAgentConfigs.create(
                    true, true, true, false, false, true, true, 100, 50, 100, 30_000);
            final AgentGovernanceRuntime runtime = AgentPipelineFactory.create(config);
            final SeaTunnelAdapter adapter = new SeaTunnelAdapter(
                    config, runtime.getPipelineEngine(), runtime.getUnloadCoordinator(),
                    runtime.getConfigCenter(),
                    runtime.getEventBus(), runtime.getMetricsCollector());

            for (int i = 0; i < 5; i++) {
                adapter.beforeTaskCall();
                adapter.afterTaskCall(null);
            }
            for (int i = 0; i < 2; i++) {
                adapter.beforeTaskCall();
                adapter.afterTaskCall(new RuntimeException("data transform NPE at row 42"));
            }

            final LingHealthMetrics metrics = runtime.getMetricsCollector().getOrCreate("seatunnel");
            // 2 次业务异常被排除：失败计数保持 0，不会虚高失败率触发 DEGRADED 后每批次 +100ms 自我放大
            assertThat(metrics.getFailedRequests().sum()).isEqualTo(0L);
            assertThat(metrics.getSuccessRequests().sum()).isEqualTo(12L);
        }

        @Test
        @DisplayName("下游可用性异常（IOException 连接类）应计入熔断失败率")
        void shouldCountAvailabilityExceptionAsBreakerFailure() {
            final AgentConfig config = TestAgentConfigs.create(
                    true, true, true, false, false, true, true, 100, 50, 100, 30_000);
            final AgentGovernanceRuntime runtime = AgentPipelineFactory.create(config);
            final SeaTunnelAdapter adapter = new SeaTunnelAdapter(
                    config, runtime.getPipelineEngine(), runtime.getUnloadCoordinator(),
                    runtime.getConfigCenter(),
                    runtime.getEventBus(), runtime.getMetricsCollector());

            for (int i = 0; i < 3; i++) {
                adapter.beforeTaskCall();
                adapter.afterTaskCall(null);
            }
            for (int i = 0; i < 2; i++) {
                adapter.beforeTaskCall();
                adapter.afterTaskCall(new IOException("sink connection refused by mysql:3306"));
            }

            final LingHealthMetrics metrics = runtime.getMetricsCollector().getOrCreate("seatunnel");
            assertThat(metrics.getFailedRequests().sum()).isEqualTo(2L);
            assertThat(metrics.getSuccessRequests().sum()).isEqualTo(8L);
        }

        @Test
        @DisplayName("超时异常（消息含 timed out，含 wrapper 链）应计入熔断失败率")
        void shouldCountTimeoutWrappedAsBreakerFailure() {
            final AgentConfig config = TestAgentConfigs.create(
                    true, true, true, false, false, true, true, 100, 50, 100, 30_000);
            final AgentGovernanceRuntime runtime = AgentPipelineFactory.create(config);
            final SeaTunnelAdapter adapter = new SeaTunnelAdapter(
                    config, runtime.getPipelineEngine(), runtime.getUnloadCoordinator(),
                    runtime.getConfigCenter(),
                    runtime.getEventBus(), runtime.getMetricsCollector());

            adapter.beforeTaskCall();
            adapter.afterTaskCall(new RuntimeException("batch write failed",
                    new java.net.SocketTimeoutException("Read timed out")));

            final LingHealthMetrics metrics = runtime.getMetricsCollector().getOrCreate("seatunnel");
            assertThat(metrics.getFailedRequests().sum()).isEqualTo(1L);
            // SocketTimeoutException 属 IOException：是超时失败（isTimeout=true 计入 timeoutRequests）
            assertThat(metrics.getTimeoutRequests().sum()).isEqualTo(1L);
        }
    }

    @Nested
    @DisplayName("熔断态手动复位：resetHealthMetrics 清空健康指标")
    class CircuitBreakerReset {

        @Test
        @DisplayName("记录可用性失败后调用 resetHealthMetrics 应清空失败计数（解除 DEGRADED）")
        void shouldResetHealthMetricsAfterFailures() {
            final AgentConfig config = TestAgentConfigs.create(
                    true, true, true, false, false, true, true, 100, 50, 100, 30_000);
            final AgentGovernanceRuntime runtime = AgentPipelineFactory.create(config);
            final SeaTunnelAdapter adapter = new SeaTunnelAdapter(
                    config, runtime.getPipelineEngine(), runtime.getUnloadCoordinator(),
                    runtime.getConfigCenter(),
                    runtime.getEventBus(), runtime.getMetricsCollector());

            for (int i = 0; i < 3; i++) {
                adapter.beforeTaskCall();
                adapter.afterTaskCall(new IOException("sink unreachable"));
            }

            final LingHealthMetrics metrics = runtime.getMetricsCollector().getOrCreate("seatunnel");
            assertThat(metrics.getFailedRequests().sum()).isEqualTo(3L);

            adapter.resetHealthMetrics();

            assertThat(metrics.getFailedRequests().sum()).isEqualTo(0L);
            assertThat(metrics.getTotalRequests().sum()).isEqualTo(0L);
        }

        @Test
        @DisplayName("metricsCollector 为 null 时 resetHealthMetrics 应安全跳过")
        void shouldSkipResetWhenMetricsCollectorNull() {
            final SeaTunnelAdapter adapter = createAdapter(true);
            adapter.resetHealthMetrics();
        }
    }

    @Nested
    @DisplayName("Trace 失败日志窗口限频")
    class TraceErrorThrottling {

        @Test
        @DisplayName("熔断风暴下同签名 ERROR 事件在同一窗口内仅放行 1 条，其余抑制")
        void shouldThrottleRepeatedErrorTrace() {
            final AgentConfig config = TestAgentConfigs.create(true, true, true, false, false, false);
            final EventBus eventBus = mock(EventBus.class);
            final Map<Class<?>, LingEventListener<?>> subs = captureSubscriptions(eventBus);
            final SeaTunnelAdapter adapter = new SeaTunnelAdapter(config, null, null, null, eventBus, null);

            adapter.beforeTaskCall(); // 触发订阅（traceLogLevel=INFO 时应注册监听器）

            final LingEventListener<MonitoringEvents.TraceLogEvent> listener =
                    (LingEventListener<MonitoringEvents.TraceLogEvent>) subs.get(MonitoringEvents.TraceLogEvent.class);
            assertThat(listener).isNotNull();

            final int bursts = 10;
            // 仅耗时（ms）不同，归一化后为同一签名 → 窗口内应只放行 1 条
            for (int i = 0; i < bursts; i++) {
                listener.onEvent(new MonitoringEvents.TraceLogEvent(
                        "trace-" + i, "seatunnel-job-7",
                        "taskCall (" + i + "ms) - LingInvocationException", "ERROR", 0));
            }

            assertThat(adapter.getTraceErrorSuppressedCount()).isEqualTo(bursts - 1);
        }

        @Test
        @DisplayName("成功轨迹（IN/OUT）不触发失败限频抑制")
        void shouldNotThrottleNonErrorTrace() {
            final AgentConfig config = TestAgentConfigs.create(true, true, true, false, false, false);
            final EventBus eventBus = mock(EventBus.class);
            final Map<Class<?>, LingEventListener<?>> subs = captureSubscriptions(eventBus);
            final SeaTunnelAdapter adapter = new SeaTunnelAdapter(config, null, null, null, eventBus, null);

            adapter.beforeTaskCall();

            final LingEventListener<MonitoringEvents.TraceLogEvent> listener =
                    (LingEventListener<MonitoringEvents.TraceLogEvent>) subs.get(MonitoringEvents.TraceLogEvent.class);
            for (int i = 0; i < 5; i++) {
                listener.onEvent(new MonitoringEvents.TraceLogEvent(
                        "trace-" + i, "seatunnel-job-7", "taskCall", "IN", 0));
            }

            assertThat(adapter.getTraceErrorSuppressedCount()).isZero();
        }
    }

    @Nested
    @DisplayName("失败审计日志窗口限频")
    class AuditFailThrottling {

        @Test
        @DisplayName("熔断风暴下同签名失败审计在同一窗口内仅放行 1 条，其余抑制")
        void shouldThrottleRepeatedFailAudit() {
            final AgentConfig config = TestAgentConfigs.create(true, true, true, false, false, false);
            final EventBus eventBus = mock(EventBus.class);
            final Map<Class<?>, LingEventListener<?>> subs = captureSubscriptions(eventBus);
            final SeaTunnelAdapter adapter = new SeaTunnelAdapter(config, null, null, null, eventBus, null);

            adapter.beforeTaskCall(); // 触发订阅

            final LingEventListener<MonitoringEvents.AuditLogEvent> listener =
                    (LingEventListener<MonitoringEvents.AuditLogEvent>) subs.get(MonitoringEvents.AuditLogEvent.class);
            assertThat(listener).isNotNull();

            final int bursts = 10;
            // 同 lingId/action/resource 的同签名失败审计，窗口内应只放行 1 条
            for (int i = 0; i < bursts; i++) {
                listener.onEvent(new MonitoringEvents.AuditLogEvent(
                        "trace-" + i, "seatunnel-job-7", "beforeTaskCall", "source-1", false, 12L));
            }

            assertThat(adapter.getAuditFailSuppressedCount()).isEqualTo(bursts - 1);
        }

        @Test
        @DisplayName("成功审计不触发失败限频抑制，保持采样率降频")
        void shouldNotThrottleSuccessAudit() {
            final AgentConfig config = TestAgentConfigs.create(true, true, true, false, false, false);
            final EventBus eventBus = mock(EventBus.class);
            final Map<Class<?>, LingEventListener<?>> subs = captureSubscriptions(eventBus);
            final SeaTunnelAdapter adapter = new SeaTunnelAdapter(config, null, null, null, eventBus, null);

            adapter.beforeTaskCall();

            final LingEventListener<MonitoringEvents.AuditLogEvent> listener =
                    (LingEventListener<MonitoringEvents.AuditLogEvent>) subs.get(MonitoringEvents.AuditLogEvent.class);
            for (int i = 0; i < 5; i++) {
                listener.onEvent(new MonitoringEvents.AuditLogEvent(
                        "trace-" + i, "seatunnel-job-7", "beforeTaskCall", "source-1", true, 8L));
            }

            assertThat(adapter.getAuditFailSuppressedCount()).isZero();
        }
    }

    /** 令 mock EventBus 记录已注册的订阅监听器，供测试直接驱动事件。 */
    private Map<Class<?>, LingEventListener<?>> captureSubscriptions(EventBus eventBus) {
        final Map<Class<?>, LingEventListener<?>> subs = new HashMap<>();
        doAnswer(invocation -> {
            subs.put(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(eventBus).subscribeGlobal(any(), any());
        return subs;
    }

    private SeaTunnelAdapter createAdapter(boolean enabled) {
        final AgentConfig config = TestAgentConfigs.create(enabled, true, true, false, false, false);
        final InvocationPipelineEngine pipeline = new InvocationPipelineEngine(null);
        return new SeaTunnelAdapter(config, pipeline, null, null, null, null);
    }

    private SeaTunnelAdapter createAdapterWithNullPipeline(boolean enabled) {
        final AgentConfig config = TestAgentConfigs.create(enabled, true, true, false, false, false);
        return new SeaTunnelAdapter(config, null, null, null, null, null);
    }

}
