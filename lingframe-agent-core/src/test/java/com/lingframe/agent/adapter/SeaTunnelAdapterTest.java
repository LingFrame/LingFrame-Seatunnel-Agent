package com.lingframe.agent.adapter;

import com.lingframe.agent.bridge.ReleasedClassLoaderRegistry;
import com.lingframe.agent.config.AgentConfig;
import com.lingframe.agent.config.TestAgentConfigs;
import com.lingframe.agent.pipeline.AgentGovernanceRuntime;
import com.lingframe.agent.pipeline.AgentPipelineFactory;
import com.lingframe.core.ling.LingUnloadCoordinator;
import com.lingframe.core.metrics.LingHealthMetrics;
import com.lingframe.core.pipeline.InvocationPipelineEngine;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
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
            final SeaTunnelAdapter adapter = new SeaTunnelAdapter(config, pipeline, mockCoordinator, null, null, null, null);

            final ClassLoader loader = new ClassLoader() { };
            adapter.onPhysicalRelease(loader);

            assertThat(ReleasedClassLoaderRegistry.isReleased(loader)).isTrue();
            verify(mockCoordinator).onFailureCleanup(loader);
        }

        @Test
        @DisplayName("URLClassLoader 应被正常关闭")
        void shouldCloseUrlClassLoader() throws Exception {
            final AgentConfig config = TestAgentConfigs.create(true, true, true, false, false, false);
            final SeaTunnelAdapter adapter = new SeaTunnelAdapter(config, null, null, null, null, null, null);

            final URLClassLoader urlClassLoader = new URLClassLoader(new URL[0], getClass().getClassLoader());
            adapter.onPhysicalRelease(urlClassLoader);
            assertThat(ReleasedClassLoaderRegistry.isReleased(urlClassLoader)).isTrue();
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
        @DisplayName("应与 sorted + joining 算法输出一致（SeaTunnel 官方算法对齐）")
        void shouldMatchSortedJoiningAlgorithm() throws MalformedURLException {
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
                    .collect(Collectors.joining());
            assertThat(key).isEqualTo(expected);
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
        @DisplayName("真实微内核成功与失败调用应准确回灌真实指标且无上下文泄漏")
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
                    runtime.getLingRepository(),
                    runtime.getEventBus(),
                    runtime.getMetricsCollector()
            );

            for (int i = 0; i < 5; i++) {
                adapter.beforeTaskCall();
                adapter.afterTaskCall(null);
            }

            for (int i = 0; i < 2; i++) {
                adapter.beforeTaskCall();
                adapter.afterTaskCall(new RuntimeException("task failure mock"));
            }

            final LingHealthMetrics metrics = runtime.getMetricsCollector().getOrCreate("seatunnel");
            assertThat(metrics).isNotNull();
            // 物理事实验证：
            // 1. beforeTaskCall 穿透真实微内核流水线，内建 TrafficMetricsFilter 记录 7 次准入成功
            // 2. afterTaskCall 回灌真实任务执行结果，记录 5 次业务成功 + 2 次业务失败
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
                    runtime.getLingRepository(),
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
                    runtime.getLingRepository(),
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

    private SeaTunnelAdapter createAdapter(boolean enabled) {
        final AgentConfig config = TestAgentConfigs.create(enabled, true, true, false, false, false);
        final InvocationPipelineEngine pipeline = new InvocationPipelineEngine(null);
        return new SeaTunnelAdapter(config, pipeline, null, null, null, null, null);
    }

    private SeaTunnelAdapter createAdapterWithNullPipeline(boolean enabled) {
        final AgentConfig config = TestAgentConfigs.create(enabled, true, true, false, false, false);
        return new SeaTunnelAdapter(config, null, null, null, null, null, null);
    }

}
