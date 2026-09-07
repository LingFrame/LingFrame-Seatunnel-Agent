package com.lingframe.agent.adapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * 熔断失败判定契约化分类器（两层）。
 * <p>
 * 「异常是否代表下游不可用」是知识问题，可靠来源只有契约，最不可靠的是对可读文本的猜测
 * （locale / 文案演进漏判、数据内含关键字误判）。分类器只保留两个可靠来源：
 * <ul>
 *   <li><b>类型契约层（默认启用）</b>：JDK 异常家族 instanceof 白名单——{@link IOException}
 *       全子类天然覆盖 ConnectException / SocketException / UnknownHostException /
 *       SocketTimeoutException；{@link SQLException} 覆盖 JDBC 通信异常；{@link TimeoutException}。
 *       沿 cause 链遍历，任一节点命中即判下游故障。</li>
 *   <li><b>显式配置层（opt-in）</b>：
 *       {@code downstream-readable-failures-patterns}（正则，match FQCN 或 message，判为下游故障）与
 *       {@code business-exceptions-patterns}（反向排除，优先级最高——命中即业务异常，不计入熔断失败率），
 *       覆盖自研 connector / 内部中间件，运维免发版接入。</li>
 * </ul>
 * 兜底保留消息关键字启发式，保证默认配置（无 patterns）下分类结果与既有实现逐例一致
 * （黄金样本回归）；{@code classifier-enabled=false} 时跳过显式配置层直接回退既有启发式（回滚开关）。
 * <p>
 * 决策可观测：分类结果 + 命中层级按采样率入 DEBUG 日志，熔断误判可追溯。
 */
public final class FailureClassifier {

    private static final Logger log = LoggerFactory.getLogger(FailureClassifier.class);

    /** 分类命中层级（决策可观测：日志 / 计数可追溯）。 */
    public enum HitLevel {
        /** 类型契约层：JDK 异常家族（IOException / SQLException / TimeoutException，含 cause 链） */
        TYPE_CONTRACT,
        /** 显式配置层·正向包含：downstream-readable-failures-patterns 命中（FQCN 或 message 正则） */
        PATTERN_DOWNSTREAM,
        /** 兜底：消息关键字启发式（默认配置下与既有实现逐例一致） */
        HEURISTIC,
        /** 显式配置层·反向排除：business-exceptions-patterns 命中（业务异常，不计入熔断失败率） */
        BUSINESS_EXCLUDED,
        /** 未命中：非下游可用性失败 */
        NONE
    }

    private final boolean enabled;
    private final List<Pattern> downstreamPatterns;
    private final List<Pattern> businessPatterns;
    private final int logSampleRate;
    /** 判定为下游故障的分类次数（采样打点计数器）。 */
    private final AtomicLong classificationCounter = new AtomicLong();

    public FailureClassifier(boolean enabled,
                             List<String> downstreamReadableFailuresPatterns,
                             List<String> businessExceptionsPatterns,
                             int logSampleRate) {
        this.enabled = enabled;
        this.downstreamPatterns = compile(downstreamReadableFailuresPatterns);
        this.businessPatterns = compile(businessExceptionsPatterns);
        this.logSampleRate = Math.max(1, logSampleRate);
    }

    private static List<Pattern> compile(List<String> patterns) {
        if (patterns == null || patterns.isEmpty()) {
            return Collections.emptyList();
        }
        final List<Pattern> compiled = new ArrayList<>(patterns.size());
        for (String p : patterns) {
            if (p == null || p.trim().isEmpty()) {
                continue;
            }
            try {
                compiled.add(Pattern.compile(p));
            } catch (PatternSyntaxException e) {
                // 非法正则不阻断分类：跳过并告警，运维可即时修正
                log.warn("Invalid failure classifier pattern '{}' skipped: {}", p, e.getMessage());
            }
        }
        return compiled;
    }

    /**
     * 分类入口：异常是否代表「下游可用性失败」（应计入熔断失败率）。
     * <p>
     * 业务异常（数据解析 / 校验等）经 {@code business-exceptions-patterns} 显式排除，
     * 不计入熔断失败率——避免正常业务错误误触发熔断（契约化收敛语义）。
     */
    public boolean isDownstreamAvailabilityFailure(Throwable error) {
        final HitLevel hit = classify(error);
        if (hit == HitLevel.NONE || hit == HitLevel.BUSINESS_EXCLUDED) {
            return false;
        }
        // 采样打点：分类结果 + 命中层级，熔断误判可追溯（复用 log-sample-rate）
        final long n = classificationCounter.incrementAndGet();
        if (n % logSampleRate == 0) {
            log.debug("Failure classified as downstream-unavailable [{}]: {}",
                    hit, error != null ? error.toString() : "null");
        }
        return true;
    }

    /**
     * 分类决策（单次 cause 链遍历）：命中层级可观测。
     * <p>
     * 判定顺序：显式配置层业务排除（优先级最高）→ 类型契约层 → 显式配置层正向包含 →
     * 兜底启发式。cause 链任一节点命中即定级。
     */
    public HitLevel classify(Throwable error) {
        if (error == null) {
            return HitLevel.NONE;
        }
        Throwable t = error;
        while (t != null) {
            // 显式配置层·反向排除：业务异常 pattern，优先级最高——任一 cause 命中即整链判为业务失败
            if (enabled && matchesAny(businessPatterns, t)) {
                return HitLevel.BUSINESS_EXCLUDED;
            }
            // 类型契约层：JDK 异常家族（cause 链任一命中即计入）
            if (t instanceof IOException || t instanceof SQLException || t instanceof TimeoutException) {
                return HitLevel.TYPE_CONTRACT;
            }
            // 显式配置层·正向包含：运维配置的 downstream pattern（FQCN 或 message）
            if (enabled && matchesAny(downstreamPatterns, t)) {
                return HitLevel.PATTERN_DOWNSTREAM;
            }
            // 兜底启发式：保证默认配置下与既有实现逐例一致（黄金样本回归）
            if (heuristicMatch(t)) {
                return HitLevel.HEURISTIC;
            }
            t = t.getCause();
        }
        return HitLevel.NONE;
    }

    private static boolean matchesAny(List<Pattern> patterns, Throwable t) {
        if (patterns.isEmpty()) {
            return false;
        }
        final String fqcn = t.getClass().getName();
        final String msg = t.getMessage();
        for (Pattern p : patterns) {
            if (p.matcher(fqcn).find() || (msg != null && p.matcher(msg).find())) {
                return true;
            }
        }
        return false;
    }

    /** 兜底启发式：类名 / 消息关键字猜测（默认配置下保留既有判定能力，供回滚与黄金样本一致）。 */
    private static boolean heuristicMatch(Throwable t) {
        final String name = t.getClass().getName();
        if (name != null && (name.contains("ConnectException")
                || name.contains("SocketException")
                || name.contains("TimeoutException")
                || name.contains("Unreachable")
                || name.contains("ConnectionClosed")
                || name.contains("BrokenPipe"))) {
            return true;
        }
        final String msg = t.getMessage();
        if (msg != null) {
            final String lower = msg.toLowerCase(Locale.ROOT);
            if (lower.contains("connection refused")
                    || lower.contains("connection reset")
                    || lower.contains("connection timed out")
                    || lower.contains("no route to host")
                    || lower.contains("unknown host")
                    || lower.contains("broken pipe")
                    || lower.contains("timed out")
                    || lower.contains("unreachable")) {
                return true;
            }
        }
        return false;
    }
}
