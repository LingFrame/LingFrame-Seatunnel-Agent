package com.lingframe.agent.config;


import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.lingframe.core.ling.LingRepository;
import com.lingframe.core.ling.LingRuntime;
import com.lingframe.core.ling.LingRuntimeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * Hazelcast IMap 分布式配置中心。
 * <p>
 * 通过 SeaTunnel 内部 Hazelcast 实例创建 IMap "lingframe-governance-config"，
 * 实现集群级治理配置同步。配置变更通过 EntryListener 实时推送，
 * 自动更新虚拟灵元的 LingRuntimeConfig，使 ResilienceGovernanceFilter
 * 下次请求读到新参数。
 * <p>
 * 时序：Hazelcast 实例在 SeaTunnel 启动时创建，Agent premain 在其之前执行。
 * 因此 tryInit 可能首次失败（实例不存在），需在 SeaTunnelAdapter 首次拦截时重试。
 * <p>
 * 注意：worker 节点是 lite member，不存储 IMap 数据，getMap 返回远程代理。
 * 配置写入应由 master 节点发起，worker 节点通过监听器接收推送。
 */
public final class HazelcastConfigCenter {

    private static final Logger log = LoggerFactory.getLogger(HazelcastConfigCenter.class);

    public static final String CONFIG_IMAP_NAME = "lingframe-governance-config";
    private static final String VIRTUAL_LING_ID = "seatunnel";

    static final String KEY_RATE_LIMIT = "rate-limit-per-second";
    static final String KEY_CB_FAILURE_RATE = "circuit-breaker-failure-rate-threshold";
    static final String KEY_CB_SLIDING_WINDOW = "circuit-breaker-sliding-window-size";
    static final String KEY_DEFAULT_TIMEOUT = "default-timeout-ms";

    private final LingRepository lingRepository;
    private HazelcastInstance hazelcastInstance;
    private volatile IMap<String, String> configMap;
    private volatile List<UUID> listenerIds;
    private volatile boolean initialized;

    public HazelcastConfigCenter(LingRepository lingRepository) {
        this(lingRepository, null);
    }

    public HazelcastConfigCenter(LingRepository lingRepository, HazelcastInstance hazelcastInstance) {
        this.lingRepository = lingRepository;
        this.hazelcastInstance = hazelcastInstance;
    }

    /**
     * 尝试初始化配置中心。
     * <p>
     * 若构造时未指定 Hazelcast 实例，则通过 Hazelcast.getAllHazelcastInstances() 自动获取 SeaTunnel 内部实例。
     * 如果实例不存在（Agent 在 SeaTunnel 启动前执行），返回 false，可稍后重试。
     * <p>
     * synchronized 防止并发首次调用重复注册监听器：SeaTunnel 任务部署是多线程的，
     * wrapTaskGroup 可能被多线程同时触发，无锁 check-then-act 会造成监听器重复注册
     * 且 listenerId 被覆盖后旧监听器无法移除。调用频率极低（仅首次），锁开销可忽略。
     *
     * @return 初始化成功返回 true，Hazelcast 实例不存在返回 false
     */
    public synchronized boolean tryInit() {
        if (initialized) {
            return true;
        }

        try {
            if (this.hazelcastInstance == null) {
                final Collection<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();
                if (instances == null || instances.isEmpty()) {
                    log.debug("No Hazelcast instance available yet, config center init deferred");
                    return false;
                }
                this.hazelcastInstance = instances.iterator().next();
            }

            configMap = hazelcastInstance.getMap(CONFIG_IMAP_NAME);

            listenerIds = registerConfigListeners(configMap);

            applyInitialConfig();

            initialized = true;
            log.info("Hazelcast config center initialized on instance [{}], IMap [{}], listeners [{}]",
                    hazelcastInstance.getName(), CONFIG_IMAP_NAME, listenerIds.size());
            return true;
        } catch (Throwable t) {
            log.warn("Failed to init Hazelcast config center: {}", t.getMessage());
            return false;
        }
    }

    /**
     * 从 IMap 读取当前配置并应用到虚拟灵元。
     */
    private void applyInitialConfig() {
        if (lingRepository == null || configMap == null) {
            return;
        }
        final LingRuntime runtime = lingRepository.getRuntime(VIRTUAL_LING_ID);
        if (runtime == null) {
            return;
        }
        final LingRuntimeConfig newConfig = buildConfigFromMap(runtime.getConfig());
        runtime.updateConfig(newConfig);
        log.info("Applied initial config from IMap: rateLimit={}/s, cbFailureRate={}%, slidingWindow={}",
                newConfig.getRateLimitPerSecond(),
                newConfig.getCircuitBreakerFailureRateThreshold(),
                newConfig.getCircuitBreakerSlidingWindowSize());
    }

    /**
     * 从 IMap 构建 LingRuntimeConfig，缺失的 key 回退到现有 config 默认值。
     */
    private LingRuntimeConfig buildConfigFromMap(LingRuntimeConfig fallback) {
        final LingRuntimeConfig.LingRuntimeConfigBuilder builder = LingRuntimeConfig.builder()
                .maxHistorySnapshots(fallback.getMaxHistorySnapshots())
                .bulkheadMaxConcurrent(fallback.getBulkheadMaxConcurrent());

        builder.rateLimitPerSecond(parseInt(
                configMap.get(KEY_RATE_LIMIT), fallback.getRateLimitPerSecond()));
        builder.circuitBreakerFailureRateThreshold(parseInt(
                configMap.get(KEY_CB_FAILURE_RATE), fallback.getCircuitBreakerFailureRateThreshold()));
        builder.circuitBreakerSlidingWindowSize(parseInt(
                configMap.get(KEY_CB_SLIDING_WINDOW), fallback.getCircuitBreakerSlidingWindowSize()));
        builder.defaultTimeoutMs(parseInt(
                configMap.get(KEY_DEFAULT_TIMEOUT), fallback.getDefaultTimeoutMs()));

        return builder.build();
    }

    private static int parseInt(String value, int fallback) {
        if (value == null || value.trim().isEmpty()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /**
     * 关闭配置中心，移除监听器。
     */
    public synchronized void stop() {
        if (configMap != null && listenerIds != null) {
            for (UUID id : listenerIds) {
                try {
                    configMap.removeEntryListener(id);
                } catch (Exception e) {
                    log.debug("Failed to remove entry listener {}: {}", id, e.getMessage());
                }
            }
        }
        initialized = false;
        log.info("Hazelcast config center stopped");
    }

    public boolean isInitialized() {
        return initialized;
    }

    /**
     * 注册配置变更监听器（函数式接口，每种事件单独注册）。
     * <p>
     * 五种 Entry 事件全部注册并统一回调 refreshVirtualLingConfig：
     * added/updated/removed/evicted/expired。
     * 修改已有配置 key 的值触发 EntryUpdated 事件——这是运维改配置最常见路径，
     * 若只监听 added 会导致热更新失效。
     * <p>
     * 每个 addEntryListener 调用返回独立 UUID，全部收集以便 stop() 逐一移除，
     * 防止孤儿监听器泄漏。
     */
    private List<UUID> registerConfigListeners(IMap<String, String> map) {
        final List<UUID> ids = new ArrayList<>();
        ids.add(map.addEntryListener((EntryAddedListener<String, String>) event -> {
            log.info("Config entry added: {} = {}", event.getKey(), event.getValue());
            refreshVirtualLingConfig();
        }, true));
        ids.add(map.addEntryListener((EntryUpdatedListener<String, String>) event -> {
            log.info("Config entry updated: {} = {}", event.getKey(), event.getValue());
            refreshVirtualLingConfig();
        }, true));
        ids.add(map.addEntryListener((EntryRemovedListener<String, String>) event -> {
            log.info("Config entry removed: {}", event.getKey());
            refreshVirtualLingConfig();
        }, true));
        ids.add(map.addEntryListener((EntryEvictedListener<String, String>) event -> {
            log.info("Config entry evicted: {}", event.getKey());
            refreshVirtualLingConfig();
        }, true));
        ids.add(map.addEntryListener((EntryExpiredListener<String, String>) event -> {
            log.info("Config entry expired: {}", event.getKey());
            refreshVirtualLingConfig();
        }, true));
        return ids;
    }

    /**
     * 配置变更时更新虚拟灵元的 LingRuntimeConfig。
     */
    private void refreshVirtualLingConfig() {
        if (lingRepository == null || configMap == null) {
            return;
        }
        final LingRuntime runtime = lingRepository.getRuntime(VIRTUAL_LING_ID);
        if (runtime == null) {
            log.debug("Virtual ling [{}] not registered, skip config refresh", VIRTUAL_LING_ID);
            return;
        }
        try {
            final LingRuntimeConfig newConfig = buildConfigFromMap(runtime.getConfig());
            runtime.updateConfig(newConfig);
            log.info("Virtual ling [{}] config refreshed: rateLimit={}/s, cbFailureRate={}%, slidingWindow={}",
                    VIRTUAL_LING_ID,
                    newConfig.getRateLimitPerSecond(),
                    newConfig.getCircuitBreakerFailureRateThreshold(),
                    newConfig.getCircuitBreakerSlidingWindowSize());
        } catch (Exception e) {
            log.warn("Failed to refresh virtual ling config: {}", e.getMessage());
        }
    }
}
