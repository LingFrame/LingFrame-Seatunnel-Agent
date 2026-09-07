package com.lingframe.agent.observability;

import com.lingframe.agent.config.AgentConfig;
import com.lingframe.core.event.EventBus;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * LingFrameAgentObservability MBean 单元测试。
 * <p>
 * 覆盖：advice 状态暴露、EventBus 计数实时查询、adapter 计时 null 安全、config 摘要。
 */
public class LingFrameAgentObservabilityTest {

    @Test
    public void exposesAdviceStatusFromSnapshot() {
        final Map<String, String> status = new HashMap<>();
        status.put("AbstractTask", "INSTALLED");
        status.put("DefaultClassLoaderService", "ABSENT");
        status.put("TcclGuard", "INSTALLED");
        final AgentConfig cfg = AgentConfig.load(null);
        final LingFrameAgentObservability mbean =
                new LingFrameAgentObservability(cfg, status, new EventBus(), null);

        assertTrue(mbean.isTaskExecutionAdviceInstalled());
        assertFalse(mbean.isClassLoaderReleaseAdviceInstalled());
        assertTrue(mbean.isTcclGuardAdviceInstalled());
        assertEquals("ABSENT", mbean.getClassloaderServiceClassStatus());
    }

    @Test
    public void adapterNullReturnsZeroForTiming() {
        final AgentConfig cfg = AgentConfig.load(null);
        final LingFrameAgentObservability mbean =
                new LingFrameAgentObservability(cfg, new HashMap<>(), new EventBus(), null);
        assertEquals(0L, mbean.getHookCallCount());
        assertEquals(0L, mbean.getHookLatencyAvgNanos());
        assertEquals(0L, mbean.getHookLatencyMinNanos());
    }

    @Test
    public void nullSafeWhenGovernanceDisabled() {
        final LingFrameAgentObservability mbean =
                new LingFrameAgentObservability(null, new HashMap<>(), null, null);
        assertEquals(-1, mbean.getEventBusQueueSize());
        assertEquals(-1L, mbean.getEventBusDroppedCount());
        assertEquals(-1L, mbean.getEventBusSubmittedCount());
        assertEquals("UNKNOWN", mbean.getAbstractTaskClassStatus());
        assertEquals("config=null", mbean.getConfigSummary());
    }

    @Test
    public void configSummaryReflectsSettings() {
        final AgentConfig cfg = AgentConfig.load(null);
        final LingFrameAgentObservability mbean =
                new LingFrameAgentObservability(cfg, new HashMap<>(), null, null);
        final String summary = mbean.getConfigSummary();
        assertTrue(summary.contains("traceLevel=INFO"));
        assertTrue(summary.contains("timing=false"));
    }

    @Test
    public void eventBusCountersReadable() {
        final EventBus bus = new EventBus();
        final AgentConfig cfg = AgentConfig.load(null);
        final LingFrameAgentObservability mbean =
                new LingFrameAgentObservability(cfg, new HashMap<>(), bus, null);
        // 空 EventBus：队列 0、丢弃 0、提交 0
        assertEquals(0, mbean.getEventBusQueueSize());
        assertEquals(0L, mbean.getEventBusDroppedCount());
        assertEquals(0L, mbean.getEventBusSubmittedCount());
    }

    @Test
    public void resetCircuitBreakerNoOpWhenAdapterNull() {
        // 无 adapter（治理关闭）时 JMX 复位入口应安全空操作，不抛异常
        final AgentConfig cfg = AgentConfig.load(null);
        final LingFrameAgentObservability mbean =
                new LingFrameAgentObservability(cfg, new HashMap<>(), null, null);
        mbean.resetCircuitBreaker();
    }
}
