package com.lingframe.agent.adapter;

import com.lingframe.agent.bridge.LingGovernanceContract;
import com.lingframe.agent.bridge.ReleasedClassLoaderRegistry;
import com.lingframe.agent.cleaner.EngineClassLoaderCleaner;
import com.lingframe.agent.hook.EngineSafeThreadReferenceUnloadHook;
import com.lingframe.agent.config.AgentConfig;
import com.lingframe.agent.config.HazelcastConfigCenter;
import com.lingframe.api.event.LingEventListener;
import com.lingframe.api.exception.LingInvocationException;
import com.lingframe.api.exception.LingInvocationException.ErrorKind;
import com.lingframe.api.security.AccessType;
import com.lingframe.core.event.EventBus;
import com.lingframe.core.event.monitor.MonitoringEvents;
import com.lingframe.core.ling.LingUnloadCoordinator;
import com.lingframe.core.metrics.MetricsCollector;
import com.lingframe.core.pipeline.InvocationContext;
import com.lingframe.core.pipeline.InvocationExecutionMode;
import com.lingframe.core.pipeline.InvocationPipelineEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.regex.Pattern;

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

    /** 调用方灵元 ID */
    private static final String CALLER_LING_ID = "seatunnel-agent";

    /** Hazelcast 配置中心初始化失败后的重试间隔（毫秒）——实例就绪前避免每个批次都探测一次 */
    private static final long CONFIG_CENTER_RETRY_INTERVAL_MS = 5_000L;

    /** Trace 失败 action 中的耗时片段（如 {@code taskCall (5ms)}），归一化后用于失败签名去重 */
    private static final Pattern TRACE_COST_CLEANER = Pattern.compile("\\(\\d+\\s*ms\\)");

    private final AgentConfig config;
    private final InvocationPipelineEngine pipelineEngine;
    private final LingUnloadCoordinator unloadCoordinator;
    private final HazelcastConfigCenter configCenter;
    private final EventBus eventBus;
    private final MetricsCollector metricsCollector;
    /** 熔断硬拒绝开关：true 时 CIRCUIT_OPEN / BULKHEAD_FULL 真正拒绝批次（触发引擎 Failover），false（默认）软退避放行。 */
    private final boolean failClosed;
    /** 熔断失败判定契约化分类器：裁决异常是否代表下游可用性失败、可否喂 onError。 */
    private final FailureClassifier failureClassifier;
    /** 跨方法传递 InvocationContext，beforeTaskCall 设置，afterTaskCall 回收 */
    private final ThreadLocal<InvocationContext> pipelineContext = new ThreadLocal<>();
    /** 批次调用开始时间，用于计算耗时回灌 LingHealthMetrics */
    private final ThreadLocal<Long> callStartTime = ThreadLocal.withInitial(() -> 0L);
    /** 治理拒绝退避控制器（令牌间隔 + 抖动 + 每线程每秒退避预算） */
    private final BackoffController backoffController = new BackoffController();
    /** 作业 ID 解析器（注入；null 或返回 NO_JOB 时回退共享灵元 seatunnel）。 */
    private volatile JobIdExtractor jobIdExtractor;
    /** 作业级虚拟灵元注册表（注入；null 时退化为共享灵元，与引擎级治理行为一致）。 */
    private volatile JobLingRegistry jobLingRegistry;
    /** 本次批次解析出的目标灵元 ID（beforeTaskCall 设置，afterTaskCall 读取用于回灌同一作业灵元）。 */
    private final ThreadLocal<String> currentLingId = new ThreadLocal<>();
    /** Hazelcast 配置中心下一次允许重试初始化时间戳（毫秒），用于失败重试节流 */
    private volatile long nextConfigCenterRetryAt;
    private volatile boolean eventSubscribed;

    /* ==================== 可观测性：per-call 钩子耗时埋点（timing-enabled=true 时启用，默认零损耗） ==================== */
    private final AtomicLong hookCallCount = new AtomicLong();
    private final AtomicLong hookLatencySumNanos = new AtomicLong();
    /** 耗时 min/max 用 LongAccumulator 无锁累加，替代 synchronized（timing 高频路径避免锁竞争） */
    private final LongAccumulator hookLatencyMinNanos = new LongAccumulator(Long::min, Long.MAX_VALUE);
    private final LongAccumulator hookLatencyMaxNanos = new LongAccumulator(Long::max, Long.MIN_VALUE);
    /** beforeTaskCall 钩子耗时（ThreadLocal，afterTaskCall 合并累加到全局统计） */
    private final ThreadLocal<Long> beforeCostNanos = new ThreadLocal<>();
    /** Trace/Audit 事件日志采样计数器（按 log-sample-rate 抽样，降低高 QPS 日志压力） */
    private final AtomicLong traceLogCounter = new AtomicLong();
    private final AtomicLong auditLogCounter = new AtomicLong();
    /** Trace 失败事件窗口限频器：熔断风暴等高重复失败仍逐条落 INFO 会引爆日志，仅失败轨迹接入 */
    private final TraceLogThrottle traceErrorThrottle = new TraceLogThrottle();
    /** 失败审计事件窗口限频器：失败审计与 Trace 失败同根（熔断拒绝等），按签名收敛抑制同类重复，成功审计仍走采样率 */
    private final TraceLogThrottle auditFailThrottle = new TraceLogThrottle();
    /** 治理拒绝告警窗口限频器：熔断/限流风暴下避免 Worker 调度循环产生海量告警日志刷屏 */
    private final TraceLogThrottle rejectionThrottle = new TraceLogThrottle();

    public SeaTunnelAdapter(AgentConfig config,
                           InvocationPipelineEngine pipelineEngine,
                           LingUnloadCoordinator unloadCoordinator,
                           HazelcastConfigCenter configCenter,
                           EventBus eventBus,
                           MetricsCollector metricsCollector) {
        this.config = config;
        this.pipelineEngine = pipelineEngine;
        this.unloadCoordinator = unloadCoordinator;
        this.configCenter = configCenter;
        this.eventBus = eventBus;
        this.metricsCollector = metricsCollector;
        this.failClosed = config != null && config.isFailClosed();
        this.failureClassifier = new FailureClassifier(
                config != null && config.isClassifierEnabled(),
                config != null ? config.getDownstreamReadableFailuresPatterns() : Collections.emptyList(),
                config != null ? config.getBusinessExceptionsPatterns() : Collections.emptyList(),
                config != null ? config.getLogSampleRate() : 1);
    }

    /**
     * 注入作业级治理组件。可为 null——此时适配器退化为共享灵元 seatunnel，
     * 与引擎级治理行为完全一致，保证作业级治理未启用时零副作用。
     */
    public void setJobLevelGovernance(JobIdExtractor extractor, JobLingRegistry registry) {
        this.jobIdExtractor = extractor;
        this.jobLingRegistry = registry;
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
        EngineSafeThreadReferenceUnloadHook.resetThreadContextClassLoaders(CALLER_LING_ID, classLoader);
        EngineClassLoaderCleaner.cleanStaticCaches(classLoader);
        EngineClassLoaderCleaner.closeClassLoaderSafely(classLoader);
        if (unloadCoordinator != null) {
            unloadCoordinator.onFailureCleanup(classLoader);
            log.debug("Unload coordinator completed JVM-level cleanup");
        }
        EngineClassLoaderCleaner.cleanFinished();
    }

    @Override
    public String convertJarsToKey(Collection<URL> jars) {
        // 与 SeaTunnel 官方 DefaultClassLoaderService.buildClassLoaderKey 保持严格一致
        // 使用 sorted() + Collectors.joining() 拼接，确保在 releaseClassLoader 切面拦截时精确命中 key
        if (jars == null || jars.isEmpty()) {
            return "";
        }
        return jars.stream().map(URL::toString).sorted().collect(Collectors.joining());
    }

    /** 兼容便捷入口：无 task 上下文时按共享灵元处理。 */
    public void beforeTaskCall() {
        beforeTaskCall(null);
    }

    @Override
    public void beforeTaskCall(Object task) {
        final long hookT0 = config.isTimingEnabled() ? System.nanoTime() : 0L;
        try {
            ensureConfigCenterInit();
            subscribePipelineEventsIfNeeded();
            callStartTime.set(System.nanoTime());
            // 解析本次批次的目标灵元（作业级或共享）；供 invoke 与 afterTaskCall 回灌一致使用
            final String targetLingId = resolveTargetLingId(task);
            currentLingId.set(targetLingId);

            if (pipelineEngine == null) {
                log.debug("Pipeline engine unavailable, skipping governance pre-check");
                return;
            }

            InvocationContext ctx = null;
            try {
                ctx = InvocationContext.obtain();
                ctx.setCallerLingId(CALLER_LING_ID);
                ctx.setTargetLingId(targetLingId);
                ctx.setServiceFQSID(targetLingId + ":" + targetLingId);
                ctx.setMethodName("call");
                ctx.setResourceType("SEATUNNEL_TASK");
                ctx.setResourceId(targetLingId + "#call");
                ctx.setOperation("taskCall");
                ctx.governance().setAccessType(AccessType.EXECUTE);
                ctx.governance().setShouldAudit(true);
                ctx.governance().setAuditAction(targetLingId + "#call");
                ctx.execution().setMode(InvocationExecutionMode.GOVERN_ONLY);

                pipelineEngine.invoke(ctx);
                pipelineContext.set(ctx);
            } catch (LingInvocationException e) {
                if (failClosed && isHardReject(e.getKind())) {
                    // 熔断名副其实：电路打开 / 舱壁打满时硬拒绝，使 call() 携带异常逃逸 → 引擎 Failover，
                    // 而非 fail-open 软退避放行。fail-closed 默认关闭以保证向后安全。
                    if (ctx != null) {
                        ctx.recycle();
                    }
                    currentLingId.remove();
                    throw new GovernanceRejectException(e);
                }
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
        } finally {
            if (config.isTimingEnabled()) {
                beforeCostNanos.set(System.nanoTime() - hookT0);
            }
        }
    }

    /**
     * 解析本次批次的目标灵元 ID（作业级治理）。
     * <p>
     * 优先级：作业级支持且 jobId 可解析 → {@code seatunnel-job-{jobId}}；
     * 否则（未启用 / 提取失败 / 超限回退 / extractor 未注入）→ 共享灵元 {@link #VIRTUAL_LING_ID}。
     * 作业在注册表建立跟踪并 touch；超限回退打采样 WARN 由注册表负责。
     */
    private String resolveTargetLingId(Object task) {
        final JobIdExtractor extractor = jobIdExtractor;
        final JobLingRegistry registry = jobLingRegistry;
        if (extractor == null || registry == null || !extractor.isJobLevelSupported()) {
            // 正常降级路径（未装配/未启用），高频短路——debug 级别避免生产刷屏
            log.debug("shared fallback: task={} extractor={} registry={} supported={}",
                    task == null ? "null" : task.getClass().getName(), extractor, registry,
                    extractor != null && extractor.isJobLevelSupported());
            return VIRTUAL_LING_ID;
        }
        final long jobId = extractor.extract(task);
        if (jobId == JobIdExtractor.NO_JOB) {
            // 提取失败（版本不支持等）；extractor 侧已 warn 首因，此处不重复告警
            log.debug("NO_JOB for {}", task == null ? "null" : task.getClass().getName());
            return VIRTUAL_LING_ID;
        }
        final String lingId = registry.resolveLingId(jobId, System.currentTimeMillis());
        if (lingId == null) {
            // 硬上限回退：注册表已采样告警，回到共享灵元
            return VIRTUAL_LING_ID;
        }
        registry.touch(jobId);
        return lingId;
    }

    /**
     * 处理 Pipeline 治理拒绝。
     * <p>
     * 根据 ErrorKind 决定退避策略：
     * <ul>
     *   <li>RATE_LIMITED：平滑限流（backpressure），等待一个令牌桶填充间隔（{@code 1000/rateLimit} ms，
     *       含 ±20% 抖动，受单线程每秒退避预算约束）。等待下一令牌是有意义的——配额耗尽按节奏等即可，
     *       而非固定 sleep 100ms 过度降速（rateLimit=1000/s 时令牌间隔仅 1ms，sleep 100ms 慢 100 倍）。</li>
     *   <li>CIRCUIT_OPEN / BULKHEAD_FULL：软退避下日志 + 放行（不 sleep、不抛异常），
     *       避免逐批次 100ms 串行阻塞 Worker、checkpoint 超时；熔断自愈由灵核半开探针承担。</li>
     *   <li>STATE_REJECTED / ROUTE_FAILURE：灵元不可用，告警</li>
     *   <li>SECURITY_REJECTED：权限不足，告警</li>
     *   <li>其他：真实故障，告警</li>
     * </ul>
     */
    private void handleGovernanceRejection(LingInvocationException e) {
        final ErrorKind kind = e.getKind();
        final String throttleKey = kind != null ? kind.name() : "UNKNOWN";
        final boolean allowLog = rejectionThrottle.tryAcquire(throttleKey, System.currentTimeMillis());
        if (kind == null) {
            if (allowLog) {
                log.warn("Governance rejected [UNKNOWN]: {}", e.getMessage());
            }
            return;
        }
        switch (kind) {
            case RATE_LIMITED:
                if (allowLog) {
                    log.warn("Governance rejected [{}], backing off to next token: {}", e.getKind(), e.getMessage());
                }
                backoffController.backoffRatelimited(config.getRateLimitPerSecond());
                break;
            case CIRCUIT_OPEN:
            case BULKHEAD_FULL:
                if (allowLog) {
                    log.warn("Governance rejected [{}], passthrough without backoff (soft path): {}",
                            e.getKind(), e.getMessage());
                }
                break;
            case STATE_REJECTED:
            case ROUTE_FAILURE:
                if (allowLog) {
                    log.warn("Governance rejected [{}]: ling unavailable: {}", e.getKind(), e.getMessage());
                }
                break;
            case SECURITY_REJECTED:
                if (allowLog) {
                    log.warn("Governance rejected [{}]: permission denied: {}", e.getKind(), e.getMessage());
                }
                break;
            default:
                if (allowLog) {
                    log.warn("Governance rejected [{}]: {}", e.getKind(), e.getMessage());
                }
                break;
        }
    }

    /**
     * 判断治理拒绝种类是否属于「硬拒绝」（应真正丢弃批次而非软退避）。
     * <p>
     * {@link ErrorKind#CIRCUIT_OPEN}（下游持续失败，电路已打开）与
     * {@link ErrorKind#BULKHEAD_FULL}（并发资源耗尽）属不可恢复的健康/资源状态，
     * fail-closed 时应硬拒绝，使任务快速失败交由引擎 Failover。
     * {@link ErrorKind#RATE_LIMITED} 是平滑限流（backpressure），无论 fail-closed 与否都软退避，
     * 因为丢弃限流批次等同数据丢失，不符合批次调度语义。
     */
    private boolean isHardReject(ErrorKind kind) {
        return kind == ErrorKind.CIRCUIT_OPEN || kind == ErrorKind.BULKHEAD_FULL;
    }

    @Override
    public void afterTaskCall(Object task, Throwable error) {
        afterTaskCall(error);
    }

    /** 兼容便捷入口：等价于 with-task 版本（task 仅用于作业级灵元解析，回灌已由 currentLingId 记录）。 */
    public void afterTaskCall(Throwable error) {
        final long hookT0 = config.isTimingEnabled() ? System.nanoTime() : 0L;
        try {
            final Long startTime = callStartTime.get();
            final long durationNanos = (startTime != null && startTime > 0L)
                    ? (System.nanoTime() - startTime)
                    : 0L;
            final long costMs = durationNanos / 1_000_000;
            final String lingId = currentLingId.get();
            recordTaskMetrics(lingId, error, costMs, durationNanos);
        } finally {
            final InvocationContext ctx = pipelineContext.get();
            if (ctx != null) {
                ctx.recycle();
            }
            pipelineContext.remove();
            callStartTime.remove();
            currentLingId.remove();
            if (config.isTimingEnabled()) {
                final long afterCost = System.nanoTime() - hookT0;
                final Long beforeCost = beforeCostNanos.get();
                beforeCostNanos.remove();
                recordHookLatency(afterCost + (beforeCost != null ? beforeCost : 0L));
            }
        }
    }

    /**
     * 回灌真实业务结果（双喂）。
     * <p>
     * 1) {@link LingHealthMetrics}：健康度 / DEGRADED 可见性（观测），保持既有回灌模式；
     * 2) {@link InvocationPipelineEngine#reportOutcome}：喂进虚拟灵元的熔断器执行统计
     *    （onSuccess / onError），让熔断器对真实业务结果敏感——GOVERN_ONLY 下
     *    TerminalInvokerFilter 恒 return null、内部失败回灌失效，此出口补上该链路缺口。
     * <p>
     * 熔断失败判定沿用 {@link #isDownstreamAvailabilityFailure}：仅下游可用性异常喂 onError，
     * 普通业务异常不计入熔断失败率，避免正常业务错误误触发熔断。
     */
    private void recordTaskMetrics(String lingId, Throwable error, long costMs, long durationNanos) {
        if (metricsCollector == null && pipelineEngine == null) {
            return;
        }
        final String effectiveLingId = lingId != null ? lingId : VIRTUAL_LING_ID;
        try {
            if (error == null) {
                if (metricsCollector != null) {
                    metricsCollector.getOrCreate(effectiveLingId).recordSuccess(costMs);
                }
                reportOutcome(effectiveLingId, true, durationNanos, null);
                return;
            }
            // 修复：仅下游可用性异常计入熔断失败率。普通业务异常（Transform / 数据校验 NPE /
            // IllegalArgumentException 等）不代表下游不可用，不计入——否则会虚高虚拟灵元失败率，
            // 触发 RuntimeStatus.DEGRADED 后每批次 +100ms 软退避自我放大延迟。业务失败由 SeaTunnel
            // 自身的 Failover / 重试处理，agent 熔断只关心「下游是否可用」。
            if (isDownstreamAvailabilityFailure(error)) {
                final boolean isTimeout = isTimeoutError(error);
                if (metricsCollector != null) {
                    metricsCollector.getOrCreate(effectiveLingId).recordFailure(costMs, isTimeout);
                }
                reportOutcome(effectiveLingId, false, durationNanos, error);
            } else {
                log.debug("Business exception excluded from circuit-breaker failure metrics: {}",
                        error.toString());
            }
        } catch (Exception e) {
            log.debug("Failed to record task metrics", e);
        }
    }

    /**
     * 回灌成败到目标灵元熔断器（双喂的熔断执行侧）。
     * 上报内部异常绝不外抛——可观测路径 fail-open，不影响 afterTaskCall 主流程。
     */
    private void reportOutcome(String lingId, boolean success, long durationNanos, Throwable error) {
        if (pipelineEngine == null) {
            return;
        }
        try {
            pipelineEngine.reportOutcome(
                    lingId != null ? lingId : VIRTUAL_LING_ID, success, durationNanos, error);
        } catch (Exception e) {
            log.debug("Failed to report outcome to circuit breaker", e);
        }
    }

    /**
     * 判断异常是否代表「下游可用性失败」，应计入虚拟灵元熔断失败率。
     * <p>
     * 统一委托 {@link FailureClassifier} 两层契约化分类（类型契约 + 显式 patterns +
     * 兜底启发式），分类结果与命中层级可采样打点、可追溯；{@code classifier-enabled=false}
     * 时分类器跳过显式配置层直接回退既有启发式，行为与既有实现逐例一致（回滚开关）。
     */
    private boolean isDownstreamAvailabilityFailure(Throwable error) {
        return failureClassifier.isDownstreamAvailabilityFailure(error);
    }

    /**
     * 重置虚拟灵元健康指标（清空失败计数，解除 DEGRADED），供运维经 JMX 手动解除熔断态。
     * <p>
     * 熔断本会在 {@code circuit-breaker-wait-duration} 后自动 OPEN → HALF_OPEN 恢复，
     * 本方法提供即时复位入口，真实下游恢复后无需再等自动恢复窗口。
     */
    public void resetHealthMetrics() {
        if (metricsCollector == null) {
            return;
        }
        try {
            metricsCollector.getOrCreate(VIRTUAL_LING_ID).reset();
            log.info("Virtual ling [{}] health metrics reset (circuit breaker cleared)", VIRTUAL_LING_ID);
        } catch (Exception e) {
            log.warn("Failed to reset health metrics: {}", e.getMessage());
        }
    }

    private boolean isTimeoutError(Throwable error) {
        Throwable t = error;
        while (t != null) {
            // 类型判定优先：TimeoutException 家族任意消息均计超时（契约优先于文本猜测）
            if (t instanceof TimeoutException) {
                return true;
            }
            final String message = t.getMessage();
            if (message != null) {
                final String lower = message.toLowerCase(Locale.ROOT);
                if (lower.contains("timeout") || lower.contains("timed out")) {
                    return true;
                }
            }
            t = t.getCause();
        }
        return false;
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
                    (LingEventListener<MonitoringEvents.TraceLogEvent>) event -> {
                        if (!config.isTraceLogEnabled()) {
                            return;
                        }
                        // 失败类 Trace 走「按签名 + 时间窗口」限频：熔断 OPEN 等故障形态下同一资源
                        // 会在极短时间同毫秒内高频重复产生同类 ERROR 事件，逐条落 INFO 会引爆日志量
                        //（数十 MB / 十数万行）。归一化 action 里的耗时片段后按 (lingId,type,action)
                        // 在窗口内限频，其余抑制并累计计数；成功轨迹（IN/OUT）仍按全局采样。
                        if (isTraceError(event)) {
                            if (!traceErrorThrottle.tryAcquire(traceErrorKey(event), System.currentTimeMillis())) {
                                return;
                            }
                        } else if (traceLogCounter.getAndIncrement() % config.getLogSampleRate() != 0) {
                            return;
                        }
                        // 已禁用 Trace 打印：全矩阵 E2E 下 Trace 即便限频仍持续产生日志，
                        // 属运维审计类噪音，改以限频计数（suppressedCount）观测，后续可恢复打印。
                    });
            eventBus.subscribeGlobal(MonitoringEvents.AuditLogEvent.class,
                    (LingEventListener<MonitoringEvents.AuditLogEvent>) event -> {
                        if (!config.isAuditLogEnabled()) {
                            return;
                        }
                        // 失败审计（熔断拒绝等）与 Trace 失败同根，按签名+时间窗口限频抑制同类重复；
                        // 成功审计保持全局采样率降频。
                        if (!event.isSuccess()) {
                            if (!auditFailThrottle.tryAcquire(auditFailKey(event), System.currentTimeMillis())) {
                                return;
                            }
                        } else if (auditLogCounter.getAndIncrement() % config.getLogSampleRate() != 0) {
                            return;
                        }
                        // 已禁用 Audit 打印：与 Trace 同理审计类日志在全矩阵下污染日志，
                        // 改以限频计数（suppressedCount）观测，后续可恢复打印。
                    });
            eventBus.subscribeGlobal(MonitoringEvents.CircuitBreakerStateEvent.class,
                    (LingEventListener<MonitoringEvents.CircuitBreakerStateEvent>) event ->
                            log.warn("[CircuitBreaker] resourceId={}, {} -> {}, failureRate={}%",
                                    event.getResourceId(), event.getOldState(),
                                    event.getNewState(), event.getFailureRate()));
            log.info("Pipeline event subscriptions registered: TraceLog, AuditLog, CircuitBreakerState");
        } catch (Exception e) {
            log.warn("Failed to subscribe pipeline events", e);
        }
    }

    /** 是否为失败类 Trace 事件（熔断拒绝等 ERROR 轨迹，需要限频抑制重复打印）。 */
    private boolean isTraceError(MonitoringEvents.TraceLogEvent event) {
        return "ERROR".equalsIgnoreCase(event.getType());
    }

    /**
     * 归一化失败 Trace 的限频签名。
     * 归一化 action 中的耗时片段（如 {@code taskCall (5ms)} -> {@code taskCall}），
     * 使同签名不同耗时的重复失败命中同一窗口限频，而非各自计成新签名。
     */
    private String traceErrorKey(MonitoringEvents.TraceLogEvent event) {
        final String action = event.getAction() == null
                ? "" : TRACE_COST_CLEANER.matcher(event.getAction()).replaceAll("");
        return event.getLingId() + "|" + event.getType() + "|" + action;
    }

    /**
     * 归一化失败审计的限频签名。
     * 熔断拒绝等失败审计中异常信息（failureReason）多态易变，仅取稳定维度（lingId, action, resource）
     * 收敛同类失败，使同一资源被熔断拒绝时命中同一窗口限频，而非每条计成新签名。
     */
    private String auditFailKey(MonitoringEvents.AuditLogEvent event) {
        return event.getLingId() + "|" + event.getAction() + "|" + event.getResource();
    }

    /**
     * 延迟初始化 Hazelcast 配置中心，失败后按间隔重试直到成功。
     * <p>
     * 时序约束：Agent premain 在 SeaTunnel 启动之前执行，此时 Hazelcast 实例尚未创建，
     * {@code AgentPipelineFactory} 中的首次 tryInit 必然失败。beforeTaskCall 是 SeaTunnel
     * 运行期与 Agent 交互的入口，但集群模式下 Hazelcast 成员发现需要时间，
     * 首个批次到达时实例仍可能未就绪——因此初始化**失败后必须保留重试能力**：
     * 原实现用 {@code configCenterInitAttempted} 一次性标记，首次失败即永久放弃，
     * 会导致分布式动态配置在集群启动期静默失效。
     * <p>
     * 节流而非每次批次都探测：重试按 {@link #CONFIG_CENTER_RETRY_INTERVAL_MS} 间隔进行，
     * 避免 Hazelcast 就绪前高频调用 {@code Hazelcast.getAllHazelcastInstances()}。
     * 初始化成功后 {@code configCenter.isInitialized()} 恒为 true，本方法退化为一次布尔判断。
     * <p>
     * 并发安全：多线程可能同时进入 tryInit，但后者为 synchronized 且内部有 initialized 短路，
     * 不会重复注册监听器。
     */
    private void ensureConfigCenterInit() {
        if (configCenter == null || configCenter.isInitialized()) {
            return;
        }
        final long now = System.currentTimeMillis();
        if (now < nextConfigCenterRetryAt) {
            return;
        }
        nextConfigCenterRetryAt = now + CONFIG_CENTER_RETRY_INTERVAL_MS;
        try {
            configCenter.tryInit();
        } catch (Throwable t) {
            // 钩子 fail-open 铁律：配置中心探测失败绝不向业务 call() 传播异常——
            // 否则每次探测失败都会让被织入的 AbstractTask.call() 抛异常，触发 Failover 风暴。
            log.warn("Hazelcast config center init attempt failed, will retry in {}ms: {}",
                    CONFIG_CENTER_RETRY_INTERVAL_MS, t.getMessage());
        }
    }


    /* ==================== 可观测性 getter（供 JMX MBean 读取） ==================== */

    private void recordHookLatency(long nanos) {
        hookCallCount.incrementAndGet();
        hookLatencySumNanos.addAndGet(nanos);
        hookLatencyMinNanos.accumulate(nanos);
        hookLatencyMaxNanos.accumulate(nanos);
    }

    public long getHookCallCount() {
        return hookCallCount.get();
    }

    /** 被 Trace 失败限频器抑制（未打印）的失败事件累计计数，供可观测与测试断言。 */
    public long getTraceErrorSuppressedCount() {
        return traceErrorThrottle.suppressedCount();
    }

    /** 返回已被窗口限频抑制的失败审计条数（供运维/测试观测）。 */
    public long getAuditFailSuppressedCount() {
        return auditFailThrottle.suppressedCount();
    }

    public long getHookLatencyAvgNanos() {
        final long c = hookCallCount.get();
        return c > 0 ? hookLatencySumNanos.get() / c : 0L;
    }

    public long getHookLatencyMinNanos() {
        final long m = hookLatencyMinNanos.get();
        return m == Long.MAX_VALUE ? 0L : m;
    }

    public long getHookLatencyMaxNanos() {
        return hookLatencyMaxNanos.get();
    }
}