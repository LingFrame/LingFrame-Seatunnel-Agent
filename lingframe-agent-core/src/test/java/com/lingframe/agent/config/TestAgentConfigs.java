package com.lingframe.agent.config;

import java.util.List;

/**
 * 单元测试专用的 AgentConfig 构造工具。
 * <p>
 * 仅置于 src/test/java 下，彻底杜绝在生产代码中暴露测试专用逻辑。
 */
public final class TestAgentConfigs {

    private TestAgentConfigs() {
    }

    /**
     * 创建测试用配置实例。
     *
     * @param governanceEnabled 治理开关
     * @param circuitBreakerEnabled 熔断开关
     * @param rateLimiterEnabled 限流开关
     * @param grayRoutingEnabled 灰度路由开关
     * @param permissionEnabled 权限审计开关
     * @param devMode 开发模式
     * @return 测试用 AgentConfig
     */
    public static AgentConfig create(boolean governanceEnabled, boolean circuitBreakerEnabled,
                                     boolean rateLimiterEnabled, boolean grayRoutingEnabled,
                                     boolean permissionEnabled, boolean devMode) {
        return create(governanceEnabled, circuitBreakerEnabled, rateLimiterEnabled,
                grayRoutingEnabled, permissionEnabled, devMode, false, 100, 50, 20, 3000);
    }

    /**
     * 创建支持自定义弹性数值与切面开关的测试用配置实例。
     *
     * @param governanceEnabled 治理开关
     * @param circuitBreakerEnabled 熔断开关
     * @param rateLimiterEnabled 限流开关
     * @param grayRoutingEnabled 灰度路由开关
     * @param permissionEnabled 权限审计开关
     * @param devMode 开发模式
     * @param taskExecutionAdviceEnabled 任务切面拦截开关
     * @param rateLimitPerSecond 限流每秒速率
     * @param circuitBreakerFailureRateThreshold 熔断失败率阈值
     * @param circuitBreakerSlidingWindowSize 熔断滑动窗口大小
     * @param defaultTimeoutMs 默认超时毫秒数
     * @return 测试用 AgentConfig
     */
    public static AgentConfig create(boolean governanceEnabled, boolean circuitBreakerEnabled,
                                     boolean rateLimiterEnabled, boolean grayRoutingEnabled,
                                     boolean permissionEnabled, boolean devMode,
                                     boolean taskExecutionAdviceEnabled,
                                     int rateLimitPerSecond, int circuitBreakerFailureRateThreshold,
                                     int circuitBreakerSlidingWindowSize, int defaultTimeoutMs) {
        return create(governanceEnabled, circuitBreakerEnabled, rateLimiterEnabled,
                grayRoutingEnabled, permissionEnabled, devMode, taskExecutionAdviceEnabled,
                rateLimitPerSecond, circuitBreakerFailureRateThreshold,
                circuitBreakerSlidingWindowSize, defaultTimeoutMs, false);
    }

    /**
     * 创建支持熔断硬拒绝开关的测试用配置实例。
     *
     * @param failClosed 熔断硬拒绝开关（true 时 CIRCUIT_OPEN / BULKHEAD_FULL 真正拒绝批次）
     * @see AgentConfig#isFailClosed()
     */
    public static AgentConfig create(boolean governanceEnabled, boolean circuitBreakerEnabled,
                                     boolean rateLimiterEnabled, boolean grayRoutingEnabled,
                                     boolean permissionEnabled, boolean devMode,
                                     boolean taskExecutionAdviceEnabled,
                                     int rateLimitPerSecond, int circuitBreakerFailureRateThreshold,
                                     int circuitBreakerSlidingWindowSize, int defaultTimeoutMs,
                                     boolean failClosed) {
        return new AgentConfig(governanceEnabled, circuitBreakerEnabled, rateLimiterEnabled,
                grayRoutingEnabled, permissionEnabled, devMode, taskExecutionAdviceEnabled,
                rateLimitPerSecond, circuitBreakerFailureRateThreshold,
                circuitBreakerSlidingWindowSize, defaultTimeoutMs, failClosed);
    }

    /**
     * 创建支持熔断失败判定分类器配置的测试用配置实例。
     *
     * @param classifierEnabled 分类器开关（false 回退既有启发式）
     * @param downstreamPatterns 下游可用性失败显式 patterns（显式配置层·正向包含）
     * @param businessPatterns 业务异常显式排除 patterns（显式配置层·反向排除，优先级最高）
     */
    public static AgentConfig create(boolean governanceEnabled, boolean circuitBreakerEnabled,
                                     boolean rateLimiterEnabled, boolean grayRoutingEnabled,
                                     boolean permissionEnabled, boolean devMode,
                                     boolean taskExecutionAdviceEnabled,
                                     int rateLimitPerSecond, int circuitBreakerFailureRateThreshold,
                                     int circuitBreakerSlidingWindowSize,
                                     int circuitBreakerMinimumNumberOfCalls,
                                     int defaultTimeoutMs,
                                     boolean failClosed,
                                     boolean classifierEnabled,
                                     List<String> downstreamPatterns, List<String> businessPatterns) {
        return new AgentConfig(governanceEnabled, circuitBreakerEnabled, rateLimiterEnabled,
                grayRoutingEnabled, permissionEnabled, devMode, taskExecutionAdviceEnabled,
                rateLimitPerSecond, circuitBreakerFailureRateThreshold,
                circuitBreakerSlidingWindowSize, circuitBreakerMinimumNumberOfCalls,
                defaultTimeoutMs, failClosed,
                false, 1024, 1_800_000L, 300_000L,
                "INFO", "INFO", 1, false,
                classifierEnabled, downstreamPatterns, businessPatterns);
    }
}
