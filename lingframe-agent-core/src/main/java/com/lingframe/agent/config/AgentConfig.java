package com.lingframe.agent.config;

import org.yaml.snakeyaml.Yaml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Agent 治理配置加载。
 * <p>
 * 配置发现优先级：Agent 参数显式指定 > 引擎配置目录 > 内嵌默认值。
 * <p>
 * 可观测性与运行时调参字段（trace-level / audit-level / log-sample-rate / timing-enabled）
 * 支持运行期系统属性覆盖（{@code -Dlingframe.agent.*}），优先级最高，
 * 便于生产环境不重启改配置 / 临时压测打开计时埋点。
 */
public final class AgentConfig {

    private static final Logger log = LoggerFactory.getLogger(AgentConfig.class);

    private static final String DEFAULT_CONFIG_PATH = "config/lingframe-governance.yaml";
    private static final String ENV_SEATUNNEL_HOME = "SEATUNNEL_HOME";

    /** 系统属性前缀，用于运行期覆盖可观测性配置。 */
    private static final String SYS_PROP_PREFIX = "lingframe.agent.";

    /** 熔断器窗口内最小调用数默认值（与灵核 LingRuntimeConfig 默认一致）。 */
    private static final int MINIMUM_CALLS_DEFAULT = 10;

    private final boolean governanceEnabled;
    private final boolean circuitBreakerEnabled;
    private final boolean rateLimiterEnabled;
    private final boolean grayRoutingEnabled;
    private final boolean permissionEnabled;
    private final boolean devMode;
    private final boolean taskExecutionAdviceEnabled;
    private final int rateLimitPerSecond;
    private final int circuitBreakerFailureRateThreshold;
    private final int circuitBreakerSlidingWindowSize;
    /** 熔断器窗口内最小调用数：调用数达到该值后才按失败率判定熔断（默认 10）。 */
    private final int circuitBreakerMinimumNumberOfCalls;
    private final int defaultTimeoutMs;
    /** 熔断硬拒绝开关：true 时 CIRCUIT_OPEN / BULKHEAD_FULL 真正拒绝批次（触发引擎 Failover），
     *  false（默认）时软退避放行。仅作显式 opt-in，避免反复失败批次触发 Failover 雪崩。 */
    private final boolean failClosed;

    /** 熔断失败判定分类器开关：true（默认）启用两层契约化分类（类型契约 + 显式 patterns），
     *  false 时回退既有启发式判定（回滚开关）。 */
    private final boolean classifierEnabled;
    /** 下游可用性失败显式 patterns（显式配置层·正向包含，正则 match FQCN 或 message，判为下游故障，默认空）。 */
    private final List<String> downstreamReadableFailuresPatterns;
    /** 业务异常显式排除 patterns（显式配置层·反向排除，正则 match FQCN 或 message，优先级最高，默认空）。 */
    private final List<String> businessExceptionsPatterns;

    /** 作业级治理开关：默认 true 将治理域收敛到作业隔离单元；显式 false 回引擎级共享灵元。 */
    private final boolean perJobGovernanceEnabled;
    /** 作业级灵元硬上限：超限新作业回退共享灵元 + WARN 采样（默认 1024）。 */
    private final int perJobMaxTrackedJobs;
    /** 作业灵元空闲回收 TTL（毫秒，默认 30 分钟）。 */
    private final long perJobIdleTtlMs;
    /** 作业灵元 Reaper 节流周期（毫秒，默认 5 分钟）。 */
    private final long perJobReapIntervalMs;

    /** Trace 事件日志级别：OFF / WARN / INFO（默认 INFO）。OFF 时 listener 直接 return。 */
    private final String traceLogLevel;
    /** Audit 事件日志级别：OFF / WARN / INFO（默认 INFO）。 */
    private final String auditLogLevel;
    /** 日志采样率：每 N 次事件打 1 次（默认 1 = 全打），降低高 QPS 场景日志压力。 */
    private final int logSampleRate;
    /** per-call 钩子耗时埋点开关（默认 false，零损耗；需要时 -Dlingframe.agent.timing-enabled=true）。 */
    private final boolean timingEnabled;

    AgentConfig(boolean governanceEnabled, boolean circuitBreakerEnabled,
                boolean rateLimiterEnabled, boolean grayRoutingEnabled,
                boolean permissionEnabled, boolean devMode,
                boolean taskExecutionAdviceEnabled,
                int rateLimitPerSecond, int circuitBreakerFailureRateThreshold,
                int circuitBreakerSlidingWindowSize, int defaultTimeoutMs,
                boolean failClosed) {
        this(governanceEnabled, circuitBreakerEnabled, rateLimiterEnabled, grayRoutingEnabled,
                permissionEnabled, devMode, taskExecutionAdviceEnabled, rateLimitPerSecond,
                circuitBreakerFailureRateThreshold, circuitBreakerSlidingWindowSize,
                MINIMUM_CALLS_DEFAULT, defaultTimeoutMs,
                failClosed,
                true, 1024, 1_800_000L, 300_000L,
                "INFO", "INFO", 1, false,
                true, Collections.emptyList(), Collections.emptyList());
    }

    AgentConfig(boolean governanceEnabled, boolean circuitBreakerEnabled,
                boolean rateLimiterEnabled, boolean grayRoutingEnabled,
                boolean permissionEnabled, boolean devMode,
                boolean taskExecutionAdviceEnabled,
                int rateLimitPerSecond, int circuitBreakerFailureRateThreshold,
                int circuitBreakerSlidingWindowSize,
                int circuitBreakerMinimumNumberOfCalls,
                int defaultTimeoutMs,
                boolean failClosed,
                boolean perJobGovernanceEnabled, int perJobMaxTrackedJobs,
                long perJobIdleTtlMs, long perJobReapIntervalMs,
                String traceLogLevel, String auditLogLevel, int logSampleRate, boolean timingEnabled) {
        this(governanceEnabled, circuitBreakerEnabled, rateLimiterEnabled, grayRoutingEnabled,
                permissionEnabled, devMode, taskExecutionAdviceEnabled, rateLimitPerSecond,
                circuitBreakerFailureRateThreshold, circuitBreakerSlidingWindowSize,
                circuitBreakerMinimumNumberOfCalls, defaultTimeoutMs,
                failClosed,
                perJobGovernanceEnabled, perJobMaxTrackedJobs,
                perJobIdleTtlMs, perJobReapIntervalMs,
                traceLogLevel, auditLogLevel, logSampleRate, timingEnabled,
                true, Collections.emptyList(), Collections.emptyList());
    }

    AgentConfig(boolean governanceEnabled, boolean circuitBreakerEnabled,
                boolean rateLimiterEnabled, boolean grayRoutingEnabled,
                boolean permissionEnabled, boolean devMode,
                boolean taskExecutionAdviceEnabled,
                int rateLimitPerSecond, int circuitBreakerFailureRateThreshold,
                int circuitBreakerSlidingWindowSize,
                int circuitBreakerMinimumNumberOfCalls,
                int defaultTimeoutMs,
                boolean failClosed,
                boolean perJobGovernanceEnabled, int perJobMaxTrackedJobs,
                long perJobIdleTtlMs, long perJobReapIntervalMs,
                String traceLogLevel, String auditLogLevel, int logSampleRate, boolean timingEnabled,
                boolean classifierEnabled,
                List<String> downstreamReadableFailuresPatterns,
                List<String> businessExceptionsPatterns) {
        this.governanceEnabled = governanceEnabled;
        this.circuitBreakerEnabled = circuitBreakerEnabled;
        this.rateLimiterEnabled = rateLimiterEnabled;
        this.grayRoutingEnabled = grayRoutingEnabled;
        this.permissionEnabled = permissionEnabled;
        this.devMode = devMode;
        this.taskExecutionAdviceEnabled = taskExecutionAdviceEnabled;
        this.rateLimitPerSecond = rateLimitPerSecond;
        this.circuitBreakerFailureRateThreshold = circuitBreakerFailureRateThreshold;
        this.circuitBreakerSlidingWindowSize = circuitBreakerSlidingWindowSize;
        this.circuitBreakerMinimumNumberOfCalls = circuitBreakerMinimumNumberOfCalls;
        this.defaultTimeoutMs = defaultTimeoutMs;
        this.failClosed = failClosed;
        this.classifierEnabled = classifierEnabled;
        this.downstreamReadableFailuresPatterns =
                immutableCopy(downstreamReadableFailuresPatterns);
        this.businessExceptionsPatterns = immutableCopy(businessExceptionsPatterns);
        this.perJobGovernanceEnabled = perJobGovernanceEnabled;
        this.perJobMaxTrackedJobs = Math.max(1, perJobMaxTrackedJobs);
        this.perJobIdleTtlMs = perJobIdleTtlMs;
        this.perJobReapIntervalMs = perJobReapIntervalMs;
        this.traceLogLevel = traceLogLevel;
        this.auditLogLevel = auditLogLevel;
        this.logSampleRate = Math.max(1, logSampleRate);
        this.timingEnabled = timingEnabled;
    }

    private static List<String> immutableCopy(List<String> patterns) {
        return patterns == null || patterns.isEmpty()
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(patterns));
    }

    public static AgentConfig load(String agentArgs) {
        final String configPath = resolveConfigPath(agentArgs);
        if (configPath == null) {
            log.info("No governance config found, using defaults (governance enabled)");
            return defaults();
        }
        try (InputStream is = new FileInputStream(configPath)) {
            final Yaml yaml = new Yaml();
            final Map<String, Object> root = yaml.load(is);
            return fromMap(root != null ? root : Collections.emptyMap());
        } catch (IOException | RuntimeException e) {
            log.warn("Failed to load governance config from {}, using defaults. Error: {}",
                    configPath, e.getMessage());
            return defaults();
        }
    }

    private static String resolveConfigPath(String agentArgs) {
        if (agentArgs != null && !agentArgs.trim().isEmpty()) {
            final Path p = Paths.get(agentArgs.trim());
            if (Files.exists(p)) {
                return p.toString();
            }
        }
        final String seatunnelHome = System.getenv(ENV_SEATUNNEL_HOME);
        if (seatunnelHome != null) {
            final Path p = Paths.get(seatunnelHome, DEFAULT_CONFIG_PATH);
            if (Files.exists(p)) {
                return p.toString();
            }
        }
        final Path p = Paths.get(DEFAULT_CONFIG_PATH);
        if (Files.exists(p)) {
            return p.toString();
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static AgentConfig fromMap(Map<String, Object> root) {
        final Map<String, Object> governance = (Map<String, Object>) root.getOrDefault("governance", Collections.emptyMap());
        final Map<String, Object> resilience = (Map<String, Object>) governance.getOrDefault("resilience", Collections.emptyMap());
        final Map<String, Object> routing = (Map<String, Object>) governance.getOrDefault("routing", Collections.emptyMap());
        final Map<String, Object> security = (Map<String, Object>) governance.getOrDefault("security", Collections.emptyMap());
        final Map<String, Object> observability = (Map<String, Object>) governance.getOrDefault("observability", Collections.emptyMap());

        // 可观测性字段：yaml 默认值 → 系统属性覆盖（优先级最高，便于生产运维调参）
        final String traceLevel = sysProp("trace-level",
                String.valueOf(observability.getOrDefault("trace-level", "INFO")));
        final String auditLevel = sysProp("audit-level",
                String.valueOf(observability.getOrDefault("audit-level", "INFO")));
        final int sampleRate = toInt(sysProp("log-sample-rate",
                observability.getOrDefault("log-sample-rate", 1)));
        final boolean timing = toBoolean(sysProp("timing-enabled",
                observability.getOrDefault("timing-enabled", false)));

        return new AgentConfig(
                toBoolean(governance.getOrDefault("enabled", true)),
                toBoolean(resilience.getOrDefault("circuit-breaker-enabled", false)),
                toBoolean(resilience.getOrDefault("rate-limiter-enabled", false)),
                toBoolean(routing.getOrDefault("gray-routing-enabled", false)),
                toBoolean(security.getOrDefault("permission-enabled", false)),
                toBoolean(governance.getOrDefault("dev-mode", false)),
                toBoolean(governance.getOrDefault("task-execution-advice-enabled", false)),
                toInt(resilience.getOrDefault("rate-limit-per-second", 100)),
                toInt(resilience.getOrDefault("circuit-breaker-failure-rate-threshold", 50)),
                toInt(resilience.getOrDefault("circuit-breaker-sliding-window-size", 20)),
                toInt(resilience.getOrDefault("circuit-breaker-minimum-number-of-calls", MINIMUM_CALLS_DEFAULT)),
                toInt(resilience.getOrDefault("default-timeout-ms", 3000)),
                toBoolean(resilience.getOrDefault("fail-closed", false)),
                toBoolean(governance.getOrDefault("per-job-governance-enabled", true)),
                toInt(governance.getOrDefault("per-job-max-tracked-jobs", 1024)),
                toLong(governance.getOrDefault("per-job-idle-ttl-ms", 1_800_000L)),
                toLong(governance.getOrDefault("per-job-reap-interval-ms", 300_000L)),
                traceLevel, auditLevel, sampleRate, timing,
                toBoolean(resilience.getOrDefault("classifier-enabled", true)),
                toStringList(resilience.getOrDefault("downstream-readable-failures-patterns", Collections.emptyList())),
                toStringList(resilience.getOrDefault("business-exceptions-patterns", Collections.emptyList()))
        );
    }

    /** 解析 patterns 配置：YAML list（推荐）或单字符串均支持，缺失返回空列表。 */
    @SuppressWarnings("unchecked")
    private static List<String> toStringList(Object value) {
        if (value instanceof List) {
            final List<String> result = new ArrayList<>();
            for (Object o : (List<?>) value) {
                if (o != null) {
                    result.add(String.valueOf(o));
                }
            }
            return result;
        }
        if (value == null) {
            return Collections.emptyList();
        }
        return Collections.singletonList(String.valueOf(value));
    }

    /** 读取系统属性 {@code lingframe.agent.<name>}，未设置时返回 yaml 默认值。 */
    private static String sysProp(String name, Object yamlDefault) {
        final String v = System.getProperty(SYS_PROP_PREFIX + name);
        return v != null ? v : String.valueOf(yamlDefault);
    }

    private static boolean toBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Boolean.valueOf(String.valueOf(value));
    }

    private static int toInt(Object value) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return Integer.parseInt(String.valueOf(value));
    }

    private static long toLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }

    /**
     * 内嵌默认配置：**默认不开启批次级治理**（仅 ClassLoader 深度清理 + TCCL 防御，零治理损耗）。
     * <p>
     * 熔断/限流/灰度/权限与批次切点全部默认 {@code false}——需要弹性治理的部署须在 YAML 中
     * 显式开启（任一治理特性开启后，批次切点经 {@link #isEffectiveTaskExecutionAdviceEnabled()}
     * 自动联动织入，无需再单独开切点）。
     */
    private static AgentConfig defaults() {
        return new AgentConfig(true, false, false, false, false, false, false, 100, 50, 20, 3000, false);
    }

    public boolean isGovernanceEnabled() {
        return governanceEnabled;
    }

    public boolean isCircuitBreakerEnabled() {
        return circuitBreakerEnabled;
    }

    public boolean isRateLimiterEnabled() {
        return rateLimiterEnabled;
    }

    public boolean isGrayRoutingEnabled() {
        return grayRoutingEnabled;
    }

    public boolean isPermissionEnabled() {
        return permissionEnabled;
    }

    public boolean isDevMode() {
        return devMode;
    }

    public boolean isTaskExecutionAdviceEnabled() {
        return taskExecutionAdviceEnabled;
    }

    /**
     * 批次切点（TaskExecutionAdvice）实际是否织入（有效开关）。
     * <p>
     * 熔断 / 限流 / 灰度 / 权限均为**显式 opt-in**（默认 {@code false}，默认配置 = 纯 ClassLoader 清理，
     * 不织入批次切点）。当用户**显式开启**任一治理特性时，批次切点必须随之织入——
     * 否则 {@code beforeTaskCall}/{@code afterTaskCall} 永不调用、该特性静默失效
     * （「伪开启」：YAML 写了 circuit-breaker-enabled: true，却因批次切点未织入而零弹性）。
     * 故有效开关 = 显式 {@code task-execution-advice-enabled} 或任一治理特性启用。
     * <p>
     * premain 的织入判定与可观测性摘要均应使用本方法而非 {@link #isTaskExecutionAdviceEnabled()}，
     * 以保证「显式开启了治理特性就真的生效」；默认全关时本方法返回 {@code false}（不织入）。
     */
    public boolean isEffectiveTaskExecutionAdviceEnabled() {
        if (!governanceEnabled) {
            return false;
        }
        return taskExecutionAdviceEnabled
                || circuitBreakerEnabled
                || rateLimiterEnabled
                || grayRoutingEnabled
                || permissionEnabled;
    }

    public boolean isFailClosed() {
        return failClosed;
    }

    public boolean isClassifierEnabled() {
        return classifierEnabled;
    }

    public List<String> getDownstreamReadableFailuresPatterns() {
        return Collections.unmodifiableList(downstreamReadableFailuresPatterns);
    }

    public List<String> getBusinessExceptionsPatterns() {
        return Collections.unmodifiableList(businessExceptionsPatterns);
    }

    public boolean isPerJobGovernanceEnabled() {
        return perJobGovernanceEnabled;
    }

    public int getPerJobMaxTrackedJobs() {
        return perJobMaxTrackedJobs;
    }

    public long getPerJobIdleTtlMs() {
        return perJobIdleTtlMs;
    }

    public long getPerJobReapIntervalMs() {
        return perJobReapIntervalMs;
    }

    public int getRateLimitPerSecond() {
        return rateLimitPerSecond;
    }

    public int getCircuitBreakerFailureRateThreshold() {
        return circuitBreakerFailureRateThreshold;
    }

    public int getCircuitBreakerSlidingWindowSize() {
        return circuitBreakerSlidingWindowSize;
    }

    public int getCircuitBreakerMinimumNumberOfCalls() {
        return circuitBreakerMinimumNumberOfCalls;
    }

    public int getDefaultTimeoutMs() {
        return defaultTimeoutMs;
    }

    public String getTraceLogLevel() {
        return traceLogLevel;
    }

    public String getAuditLogLevel() {
        return auditLogLevel;
    }

    public int getLogSampleRate() {
        return logSampleRate;
    }

    public boolean isTimingEnabled() {
        return timingEnabled;
    }

    /** Trace 日志是否启用（级别非 OFF）。 */
    public boolean isTraceLogEnabled() {
        return !"OFF".equalsIgnoreCase(traceLogLevel);
    }

    /** Audit 日志是否启用（级别非 OFF）。 */
    public boolean isAuditLogEnabled() {
        return !"OFF".equalsIgnoreCase(auditLogLevel);
    }
}
