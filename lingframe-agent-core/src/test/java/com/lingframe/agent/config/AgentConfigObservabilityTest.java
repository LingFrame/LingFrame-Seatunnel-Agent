package com.lingframe.agent.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * AgentConfig 可观测性字段（trace/audit 级别、采样率、计时开关）单元测试。
 * <p>
 * 覆盖：构造默认值、OFF 级别判定、采样率下界保护。
 */
public class AgentConfigObservabilityTest {

    @Test
    public void defaultsHaveObservabilityInfoAndOff() {
        final AgentConfig c = new AgentConfig(true, true, true, false, false, false, false, 100, 50, 20, 3000, false);
        assertEquals("INFO", c.getTraceLogLevel());
        assertEquals("INFO", c.getAuditLogLevel());
        assertEquals(1, c.getLogSampleRate());
        assertFalse(c.isTimingEnabled());
        assertTrue(c.isTraceLogEnabled());
        assertTrue(c.isAuditLogEnabled());
    }

    @Test
    public void offLevelDisablesLog() {
        final AgentConfig c = new AgentConfig(true, true, true, false, false, false, false,
                100, 50, 20, 10, 3000, false, false, 1024, 1_800_000L, 300_000L, "OFF", "OFF", 1, false);
        assertFalse(c.isTraceLogEnabled());
        assertFalse(c.isAuditLogEnabled());
    }

    @Test
    public void sampleRateLowerBoundedToOne() {
        // 0 或负数应被保护为 1（全打），避免取模零除
        final AgentConfig c = new AgentConfig(true, true, true, false, false, false, false,
                100, 50, 20, 10, 3000, false, false, 1024, 1_800_000L, 300_000L, "INFO", "INFO", 0, false);
        assertEquals(1, c.getLogSampleRate());
    }

    @Test
    public void timingEnabledFlagPropagated() {
        final AgentConfig c = new AgentConfig(true, true, true, false, false, false, false,
                100, 50, 20, 10, 3000, false, false, 1024, 1_800_000L, 300_000L, "INFO", "WARN", 10, true);
        assertTrue(c.isTimingEnabled());
        assertEquals("WARN", c.getAuditLogLevel());
        assertEquals(10, c.getLogSampleRate());
    }

    @Test
    public void effectiveTaskAdviceEnabledWhenResilienceOn() {
        // 显式开启熔断/限流（opt-in）但未单独开 task-execution-advice-enabled → 有效切点必须为 true，
        // 否则 before/afterTaskCall 永不调用、弹性治理"伪开启"（开了特性却静默失效）
        final AgentConfig c = new AgentConfig(true, true, true, false, false, false, false,
                100, 50, 20, 3000, false);
        assertFalse(c.isTaskExecutionAdviceEnabled());
        assertTrue(c.isEffectiveTaskExecutionAdviceEnabled());
    }

    @Test
    public void effectiveTaskAdviceEnabledWhenExplicitAdviceOn() {
        // 显式开启批次切点（即使各治理特性关闭）→ 有效切点为 true
        final AgentConfig c = new AgentConfig(true, false, false, false, false, false, true,
                100, 50, 20, 3000, false);
        assertTrue(c.isTaskExecutionAdviceEnabled());
        assertTrue(c.isEffectiveTaskExecutionAdviceEnabled());
    }

    @Test
    public void effectiveTaskAdviceDisabledWhenAllFeaturesOff() {
        // 无任何治理特性且批次切点未开 → 有效切点为 false（纯 ClassLoader 清理模式）
        final AgentConfig c = new AgentConfig(true, false, false, false, false, false, false,
                100, 50, 20, 3000, false);
        assertFalse(c.isEffectiveTaskExecutionAdviceEnabled());
    }
}
