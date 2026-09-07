package com.lingframe.agent.observability;

/**
 * LingFrame SeaTunnel Agent JMX 可观测性 MBean。
 * <p>
 * 通过 {@code jcmd <pid> ManagementAgent.local} 或 jconsole 连接 JVM 后，
 * 在 {@code com.lingframe.agent:type=Observability} 下可读取：
 * <ul>
 *   <li>各 advice 安装状态与目标类存在性（防上游 SeaTunnel 重构后静默失效）</li>
 *   <li>EventBus 异步队列积压 / 丢弃 / 提交数（治理事件反压监控）</li>
 *   <li>per-call 钩子耗时统计（timing-enabled=true 时，min/avg/max/count）</li>
 *   <li>Agent 配置摘要</li>
 * </ul>
 * <p>
 * 所有 getter 返回基本类型 / String，JMX 自动序列化，无第三方依赖。
 */
public interface LingFrameAgentObservabilityMBean {

    /* ==================== Advice 安装状态 ==================== */

    boolean isTaskExecutionAdviceInstalled();

    boolean isClassLoaderReleaseAdviceInstalled();

    boolean isTcclGuardAdviceInstalled();

    /** AbstractTask 目标类状态：PRESENT（存在，已织入候选）/ ABSENT（缺失，上游重构？静默不生效） */
    String getAbstractTaskClassStatus();

    /** DefaultClassLoaderService 目标类状态 */
    String getClassloaderServiceClassStatus();

    /* ==================== per-call 钩子耗时统计 ==================== */

    boolean isTimingEnabled();

    long getHookCallCount();

    long getHookLatencyAvgNanos();

    long getHookLatencyMinNanos();

    long getHookLatencyMaxNanos();

    /* ==================== EventBus 异步分发监控 ==================== */

    int getEventBusQueueSize();

    long getEventBusDroppedCount();

    long getEventBusSubmittedCount();

    /* ==================== 配置摘要 ==================== */

    String getConfigSummary();

    /* ==================== 运维操作 ==================== */

    /**
     * 手动复位熔断态：清空虚拟灵元健康指标失败计数，解除 RuntimeStatus.DEGRADED。
     * <p>
     * 熔断本会在 circuit-breaker-wait-duration 后自动 OPEN → HALF_OPEN 恢复，
     * 本方法用于真实下游已恢复时即时复位，无需等待恢复窗口 / 重启。
     * JMX 调用：{@code resetCircuitBreaker()}。
     */
    void resetCircuitBreaker();
}
