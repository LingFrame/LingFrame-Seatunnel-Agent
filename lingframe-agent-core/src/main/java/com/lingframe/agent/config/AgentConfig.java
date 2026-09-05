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
import java.util.Collections;
import java.util.Map;

/**
 * Agent 治理配置加载。
 * <p>
 * 配置发现优先级：Agent 参数显式指定 > 引擎配置目录 > 内嵌默认值。
 */
public final class AgentConfig {

    private static final Logger log = LoggerFactory.getLogger(AgentConfig.class);

    private static final String DEFAULT_CONFIG_PATH = "config/lingframe-governance.yaml";
    private static final String ENV_SEATUNNEL_HOME = "SEATUNNEL_HOME";

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
    private final int defaultTimeoutMs;

    AgentConfig(boolean governanceEnabled, boolean circuitBreakerEnabled,
                boolean rateLimiterEnabled, boolean grayRoutingEnabled,
                boolean permissionEnabled, boolean devMode,
                boolean taskExecutionAdviceEnabled,
                int rateLimitPerSecond, int circuitBreakerFailureRateThreshold,
                int circuitBreakerSlidingWindowSize, int defaultTimeoutMs) {
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
        this.defaultTimeoutMs = defaultTimeoutMs;
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

        return new AgentConfig(
                toBoolean(governance.getOrDefault("enabled", true)),
                toBoolean(resilience.getOrDefault("circuit-breaker-enabled", true)),
                toBoolean(resilience.getOrDefault("rate-limiter-enabled", true)),
                toBoolean(routing.getOrDefault("gray-routing-enabled", false)),
                toBoolean(security.getOrDefault("permission-enabled", false)),
                toBoolean(governance.getOrDefault("dev-mode", false)),
                toBoolean(governance.getOrDefault("task-execution-advice-enabled", false)),
                toInt(resilience.getOrDefault("rate-limit-per-second", 100)),
                toInt(resilience.getOrDefault("circuit-breaker-failure-rate-threshold", 50)),
                toInt(resilience.getOrDefault("circuit-breaker-sliding-window-size", 20)),
                toInt(resilience.getOrDefault("default-timeout-ms", 3000))
        );
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

    private static AgentConfig defaults() {
        return new AgentConfig(true, true, true, false, false, false, false, 100, 50, 20, 3000);
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

    public int getRateLimitPerSecond() {
        return rateLimitPerSecond;
    }

    public int getCircuitBreakerFailureRateThreshold() {
        return circuitBreakerFailureRateThreshold;
    }

    public int getCircuitBreakerSlidingWindowSize() {
        return circuitBreakerSlidingWindowSize;
    }

    public int getDefaultTimeoutMs() {
        return defaultTimeoutMs;
    }
}
