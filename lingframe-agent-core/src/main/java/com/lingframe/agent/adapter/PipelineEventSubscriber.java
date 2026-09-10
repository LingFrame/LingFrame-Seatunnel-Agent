package com.lingframe.agent.adapter;

import com.lingframe.agent.config.AgentConfig;
import com.lingframe.api.event.LingEventListener;
import com.lingframe.core.event.EventBus;
import com.lingframe.core.event.monitor.MonitoringEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Pipeline Trace/Audit/CircuitBreaker 事件订阅器。
 * <p>
 * 从 {@code SeaTunnelAdapter} 提取，职责单一化：全局订阅监控事件并按采样率/限频策略落日志。
 * 事件订阅是幂等的（{@code subscribed} 标志保证仅执行一次）。
 */
final class PipelineEventSubscriber {

    private static final Logger log = LoggerFactory.getLogger(PipelineEventSubscriber.class);

    /** Trace 失败 action 中的耗时片段（如 {@code taskCall (5ms)}），归一化后用于失败签名去重 */
    private static final Pattern TRACE_COST_CLEANER = Pattern.compile("\\(\\d+\\s*ms\\)");

    private volatile boolean subscribed;
    private final AtomicLong traceLogCounter = new AtomicLong();
    private final AtomicLong auditLogCounter = new AtomicLong();
    /** Trace 失败事件窗口限频器：熔断风暴等高重复失败仍逐条落 INFO 会引爆日志，仅失败轨迹接入 */
    private final TraceLogThrottle traceErrorThrottle = new TraceLogThrottle();
    /** 失败审计事件窗口限频器：失败审计与 Trace 失败同根（熔断拒绝等），按签名收敛抑制同类重复 */
    private final TraceLogThrottle auditFailThrottle = new TraceLogThrottle();

    void subscribeIfNeeded(EventBus eventBus, AgentConfig config) {
        if (subscribed || eventBus == null) {
            return;
        }
        subscribed = true;
        try {
            eventBus.subscribeGlobal(MonitoringEvents.TraceLogEvent.class,
                    (LingEventListener<MonitoringEvents.TraceLogEvent>) event -> {
                        if (!config.isTraceLogEnabled()) {
                            return;
                        }
                        // 失败类 Trace 走「按签名 + 时间窗口」限频：熔断 OPEN 等故障形态下同一资源
                        // 会在极短时间同毫秒内高频重复产生同类 ERROR 事件，逐条落 INFO 会引爆日志量
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

    /** 被 Trace 失败限频器抑制（未打印）的失败事件累计计数，供可观测与测试断言。 */
    long getTraceErrorSuppressedCount() {
        return traceErrorThrottle.suppressedCount();
    }

    /** 返回已被窗口限频抑制的失败审计条数（供运维/测试观测）。 */
    long getAuditFailSuppressedCount() {
        return auditFailThrottle.suppressedCount();
    }
}