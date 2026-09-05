package com.lingframe.agent.adapter;

import com.lingframe.agent.bridge.LingGovernanceContract;
import com.lingframe.agent.bridge.ReleasedClassLoaderRegistry;
import com.lingframe.agent.config.AgentConfig;
import com.lingframe.agent.config.HazelcastConfigCenter;
import com.lingframe.api.event.LingEventListener;
import com.lingframe.api.exception.LingInvocationException;
import com.lingframe.api.security.AccessType;
import com.lingframe.core.event.EventBus;
import com.lingframe.core.event.monitor.MonitoringEvents;
import com.lingframe.core.ling.LingRepository;
import com.lingframe.core.ling.LingUnloadCoordinator;
import com.lingframe.core.metrics.LingHealthMetrics;
import com.lingframe.core.metrics.MetricsCollector;
import com.lingframe.core.pipeline.InvocationContext;
import com.lingframe.core.pipeline.InvocationExecutionMode;
import com.lingframe.core.pipeline.InvocationPipelineEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * SeaTunnel 上下文到 LingFrame 治理微内核的适配器。
 * <p>
 * 实现 {@link LingGovernanceContract}，被注册到 {@link com.lingframe.agent.bridge.LingFrameAgentBridge}。
 * 将 SeaTunnel 的 AbstractTask.call() 批次调度事件适配为 LingFrame 治理流水线调用。
 * <p>
 * 治理模型：
 * <ul>
 *   <li>{@code beforeTaskCall}：构造 {@link InvocationContext}（GOVERN_ONLY 模式），调
 *       {@link InvocationPipelineEngine#invoke} 走完整 12 个 Filter 链做前置治理
 *       （限流、熔断前置检查、权限、状态守卫、审计、Trace 事件）。Pipeline 通过则 SeaTunnel
 *       正常执行 call()，Pipeline 拒绝则根据 ErrorKind 退避/告警。</li>
 *   <li>{@code afterTaskCall}：回灌真实业务结果到 {@link LingHealthMetrics}，触发
 *       LingHealthMetrics → RuntimeStatus.DEGRADED → MacroStateGuardFilter 拒绝的熔断路径。
 *       仿照 {@code LingWebGovernanceFilter.recordWebMetrics} 的回灌模式。</li>
 * </ul>
 * <p>
 * 不自造弹性与清理组件——Pipeline 内的 ResilienceGovernanceFilter 已持有限流器/熔断器；
 * 物理卸载清理统一委托给灵核底座的 {@link LingUnloadCoordinator}（含 ThreadReference/JdbcDriver 等全量 JVM 钩子）。
 */
public final class SeaTunnelAdapter implements LingGovernanceContract {

    private static final Logger log = LoggerFactory.getLogger(SeaTunnelAdapter.class);

    /** 虚拟灵元 ID——SeaTunnel 治理切点的统一灵元标识 */
    private static final String VIRTUAL_LING_ID = "seatunnel";

    /** 虚拟灵元服务 FQSID */
    private static final String VIRTUAL_LING_FQSID = "seatunnel:seatunnel";

    /** 调用方灵元 ID */
    private static final String CALLER_LING_ID = "seatunnel-agent";

    private final AgentConfig config;
    private final InvocationPipelineEngine pipelineEngine;
    private final LingUnloadCoordinator unloadCoordinator;
    private final HazelcastConfigCenter configCenter;
    private final LingRepository lingRepository;
    private final EventBus eventBus;
    private final MetricsCollector metricsCollector;
    /** 跨方法传递 InvocationContext，beforeTaskCall 设置，afterTaskCall 回收 */
    private final ThreadLocal<InvocationContext> pipelineContext = new ThreadLocal<>();
    /** 批次调用开始时间，用于计算耗时回灌 LingHealthMetrics */
    private final ThreadLocal<Long> callStartTime = ThreadLocal.withInitial(() -> 0L);
    private volatile boolean configCenterInitAttempted;
    private volatile boolean eventSubscribed;

    public SeaTunnelAdapter(AgentConfig config,
                           InvocationPipelineEngine pipelineEngine,
                           LingUnloadCoordinator unloadCoordinator,
                           HazelcastConfigCenter configCenter,
                           LingRepository lingRepository,
                           EventBus eventBus,
                           MetricsCollector metricsCollector) {
        this.config = config;
        this.pipelineEngine = pipelineEngine;
        this.unloadCoordinator = unloadCoordinator;
        this.configCenter = configCenter;
        this.lingRepository = lingRepository;
        this.eventBus = eventBus;
        this.metricsCollector = metricsCollector;
    }

    @Override
    public boolean isGovernanceEnabled() {
        return config.isGovernanceEnabled();
    }

    @Override
    public void onPhysicalRelease(ClassLoader classLoader) {
        if (classLoader == null) {
            return;
        }
        log.info("Triggering physical release for ClassLoader {}", classLoader.getClass().getName());
        ReleasedClassLoaderRegistry.register(classLoader);
        if (unloadCoordinator != null) {
            unloadCoordinator.onFailureCleanup(classLoader);
            log.debug("Unload coordinator completed JVM-level cleanup");
        }
        closeUrlClassLoaderIfPossible(classLoader);
    }

    @Override
    public String convertJarsToKey(Collection<URL> jars) {
        return jars.stream().map(URL::toString).sorted().collect(Collectors.joining());
    }

    @Override
    public void beforeTaskCall() {
        ensureConfigCenterInit();
        subscribePipelineEventsIfNeeded();
        callStartTime.set(System.nanoTime());

        if (pipelineEngine == null) {
            log.debug("Pipeline engine unavailable, skipping governance pre-check");
            return;
        }

        InvocationContext ctx = null;
        try {
            ctx = InvocationContext.obtain();
            ctx.setCallerLingId(CALLER_LING_ID);
            ctx.setTargetLingId(VIRTUAL_LING_ID);
            ctx.setServiceFQSID(VIRTUAL_LING_FQSID);
            ctx.setMethodName("call");
            ctx.setResourceType("SEATUNNEL_TASK");
            ctx.setResourceId(VIRTUAL_LING_ID + "#call");
            ctx.setOperation("taskCall");
            ctx.governance().setAccessType(AccessType.EXECUTE);
            ctx.governance().setShouldAudit(true);
            ctx.governance().setAuditAction(VIRTUAL_LING_ID + "#call");
            ctx.execution().setMode(InvocationExecutionMode.GOVERN_ONLY);

            pipelineEngine.invoke(ctx);
            pipelineContext.set(ctx);
        } catch (LingInvocationException e) {
            handleGovernanceRejection(e);
            if (ctx != null) {
                ctx.recycle();
            }
        } catch (Throwable t) {
            log.warn("Pipeline pre-governance failed, allowing passthrough: {}", t.getMessage());
            if (ctx != null) {
                ctx.recycle();
            }
        }
    }

    /**
     * 处理 Pipeline 治理拒绝。
     * <p>
     * 根据 ErrorKind 决定退避策略：
     * <ul>
     *   <li>RATE_LIMITED / CIRCUIT_OPEN / BULKHEAD_FULL：治理拒绝，sleep 100ms 退避不抛异常
     *       （避免 SeaTunnel Worker 捕获 Throwable 后触发 Failover 风暴）</li>
     *   <li>STATE_REJECTED / ROUTE_FAILURE：灵元不可用，告警</li>
     *   <li>SECURITY_REJECTED：权限不足，告警</li>
     *   <li>其他：真实故障，告警</li>
     * </ul>
     */
    private void handleGovernanceRejection(LingInvocationException e) {
        switch (e.getKind()) {
            case RATE_LIMITED:
            case CIRCUIT_OPEN:
            case BULKHEAD_FULL:
                log.warn("Governance rejected [{}], backing off 100ms to avoid Failover cascade: {}",
                        e.getKind(), e.getMessage());
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                break;
            case STATE_REJECTED:
            case ROUTE_FAILURE:
                log.warn("Governance rejected [{}]: ling unavailable: {}", e.getKind(), e.getMessage());
                break;
            case SECURITY_REJECTED:
                log.warn("Governance rejected [{}]: permission denied: {}", e.getKind(), e.getMessage());
                break;
            default:
                log.warn("Governance rejected [{}]: {}", e.getKind(), e.getMessage());
                break;
        }
    }

    @Override
    public void afterTaskCall(Throwable error) {
        try {
            final Long startTime = callStartTime.get();
            final long costMs = (startTime != null && startTime > 0L)
                    ? (System.nanoTime() - startTime) / 1_000_000
                    : 0L;
            recordTaskMetrics(error, costMs);
        } finally {
            final InvocationContext ctx = pipelineContext.get();
            if (ctx != null) {
                ctx.recycle();
            }
            pipelineContext.remove();
            callStartTime.remove();
        }
    }

    /**
     * 回灌真实业务结果到 LingHealthMetrics。
     * <p>
     * 仿照 {@code LingWebGovernanceFilter.recordWebMetrics} 的回灌模式：
     * 成功调 {@link LingHealthMetrics#recordSuccess}，失败调 {@link LingHealthMetrics#recordFailure}。
     * LingHealthMetrics 失败率过高时触发 RuntimeStatus.DEGRADED，
     * 下次 beforeTaskCall 时 MacroStateGuardFilter 拒绝请求——这是 GOVERN_ONLY 模式的熔断路径。
     */
    private void recordTaskMetrics(Throwable error, long costMs) {
        if (metricsCollector == null) {
            return;
        }
        try {
            final LingHealthMetrics metrics = metricsCollector.getOrCreate(VIRTUAL_LING_ID);
            if (error == null) {
                metrics.recordSuccess(costMs);
            } else {
                final boolean isTimeout = isTimeoutError(error);
                metrics.recordFailure(costMs, isTimeout);
            }
        } catch (Exception e) {
            log.debug("Failed to record task metrics: {}", e.getMessage());
        }
    }

    private boolean isTimeoutError(Throwable error) {
        if (error == null) {
            return false;
        }
        final String message = error.getMessage();
        if (message != null) {
            final String lower = message.toLowerCase(Locale.ROOT);
            if (lower.contains("timeout") || lower.contains("timed out")) {
                return true;
            }
        }
        return isTimeoutError(error.getCause());
    }

    /**
     * 订阅 Pipeline 的 Trace/Event 监控事件。
     * <p>
     * 全局订阅（不绑定 lingId），因为 SeaTunnel Agent 是框架级组件。
     * 事件转发到日志，供运维排障和 Dashboard 消费。
     */
    private void subscribePipelineEventsIfNeeded() {
        if (eventSubscribed || eventBus == null) {
            return;
        }
        eventSubscribed = true;
        try {
            eventBus.subscribeGlobal(MonitoringEvents.TraceLogEvent.class,
                    (LingEventListener<MonitoringEvents.TraceLogEvent>) event ->
                            log.info("[Trace] traceId={}, lingId={}, action={}, type={}, depth={}",
                                    event.getTraceId(), event.getLingId(), event.getAction(),
                                    event.getType(), event.getDepth()));
            eventBus.subscribeGlobal(MonitoringEvents.AuditLogEvent.class,
                    (LingEventListener<MonitoringEvents.AuditLogEvent>) event ->
                            log.info("[Audit] traceId={}, lingId={}, action={}, resource={}, success={}",
                                    event.getTraceId(), event.getLingId(), event.getAction(),
                                    event.getResource(), event.isSuccess()));
            eventBus.subscribeGlobal(MonitoringEvents.CircuitBreakerStateEvent.class,
                    (LingEventListener<MonitoringEvents.CircuitBreakerStateEvent>) event ->
                            log.warn("[CircuitBreaker] resourceId={}, {} -> {}, failureRate={}%",
                                    event.getResourceId(), event.getOldState(),
                                    event.getNewState(), event.getFailureRate()));
            log.info("Pipeline event subscriptions registered: TraceLog, AuditLog, CircuitBreakerState");
        } catch (Exception e) {
            log.warn("Failed to subscribe pipeline events: {}", e.getMessage());
        }
    }

    /**
     * 延迟初始化 Hazelcast 配置中心。
     * <p>
     * Agent premain 在 SeaTunnel 启动前执行，Hazelcast 实例可能尚未创建。
     * beforeTaskCall 是 SeaTunnel 运行期第一次与 Agent 交互的入口，
     * 此时 Hazelcast 实例一定已存在，尝试初始化配置中心。
     */
    private void ensureConfigCenterInit() {
        if (configCenterInitAttempted || configCenter == null) {
            return;
        }
        configCenterInitAttempted = true;
        if (!configCenter.isInitialized()) {
            configCenter.tryInit();
        }
    }


    private void closeUrlClassLoaderIfPossible(ClassLoader classLoader) {
        if (classLoader instanceof URLClassLoader) {
            try {
                ((URLClassLoader) classLoader).close();
                log.debug("Closed URLClassLoader");
            } catch (IOException e) {
                log.warn("Failed to close URLClassLoader: {}", e.getMessage());
            }
        }
    }
}
