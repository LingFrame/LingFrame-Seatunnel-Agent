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

    /** 作业级灵元 ID 前缀：`seatunnel-job-{jobId}`。 */
    public static final String JOB_LING_PREFIX = "seatunnel-job-";
    /** 作业级配置 key 前缀：`job.{jobId}.{bareKey}`（缺省回退全局裸 key）。 */
    private static final String JOB_KEY_PREFIX = "job.";

    static final String KEY_RATE_LIMIT = "rate-limit-per-second";
    static final String KEY_CB_FAILURE_RATE = "circuit-breaker-failure-rate-threshold";
    static final String KEY_CB_SLIDING_WINDOW = "circuit-breaker-sliding-window-size";
    static final String KEY_DEFAULT_TIMEOUT = "default-timeout-ms";
    static final String KEY_BULKHEAD_MAX_CONCURRENT = "bulkhead-max-concurrent";

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
            log.warn("Failed to init Hazelcast config center", t);
            return false;
        }
    }

    /**
     * 从 IMap 读取当前配置并应用到虚拟灵元。
     * <p>
     * 初始化时不仅刷共享灵元，同时把全局裸 key 应用到全部已注册作业灵元
     * （作业级 key 覆盖，缺失回退全局，见 {@link #buildConfigFromMap}）。
     */
    private void applyInitialConfig() {
        if (lingRepository == null || configMap == null) {
            return;
        }
        refreshLing(VIRTUAL_LING_ID, null);
        refreshAllJobLings();
    }

    /**
     * 刷新单个灵元配置。globalJobId 为空表示共享灵元（仅读全局裸 key）；
     * 非空表示作业灵元（先读 `job.{jobId}.{bareKey}` 覆盖，缺省回退全局裸 key）。
     */
    private void refreshLing(String lingId, String globalJobId) {
        if (lingRepository == null || configMap == null) {
            return;
        }
        final LingRuntime runtime = lingRepository.getRuntime(lingId);
        if (runtime == null) {
            log.debug("Ling [{}] not registered, skip config refresh", lingId);
            return;
        }
        try {
            final LingRuntimeConfig newConfig = buildConfigFromMap(runtime.getConfig(), globalJobId);
            runtime.updateConfig(newConfig);
            log.info("Ling [{}] config refreshed: rateLimit={}/s, cbFailureRate={}%, slidingWindow={}, bulkheadMaxConcurrent={}",
                    lingId,
                    newConfig.getRateLimitPerSecond(),
                    newConfig.getCircuitBreakerFailureRateThreshold(),
                    newConfig.getCircuitBreakerSlidingWindowSize(),
                    newConfig.getBulkheadMaxConcurrent());
        } catch (Exception e) {
            log.warn("Failed to refresh ling [{}] config", lingId, e);
        }
    }

    /** 刷新全部已注册作业灵元（`seatunnel-job-*`），把全局裸 key 变更广播到各作业。 */
    private void refreshAllJobLings() {
        final Collection<LingRuntime> runtimes = lingRepository.getAllRuntimes();
        if (runtimes == null) {
            return;
        }
        for (LingRuntime rt : runtimes) {
            final String id = rt.getLingId();
            if (id != null && id.startsWith(JOB_LING_PREFIX)) {
                refreshLing(id, id.substring(JOB_LING_PREFIX.length()));
            }
        }
    }

    /**
     * 从 IMap 构建 LingRuntimeConfig，缺失的 key 回退到打包时的默认值。
     * <p>
     * globalJobId 非空时，每个 key 先读作业级 {@code job.{jobId}.{bareKey}}，
     * 缺省再回退全局裸 key/{@code fallback}——作业级配置缺省逐级回退全局，符合 opt-in 语义。
     */
    private LingRuntimeConfig buildConfigFromMap(LingRuntimeConfig fallback, String globalJobId) {
        final LingRuntimeConfig.LingRuntimeConfigBuilder builder = LingRuntimeConfig.builder()
                .maxHistorySnapshots(fallback.getMaxHistorySnapshots())
                // bulkhead-max-concurrent 纳入 IMap 热刷 key 集合（缺省回退现有值）。
                .bulkheadMaxConcurrent(parseInt(
                        cfgKey(configMap, globalJobId, KEY_BULKHEAD_MAX_CONCURRENT), fallback.getBulkheadMaxConcurrent()));

        builder.rateLimitPerSecond(parseInt(
                cfgKey(configMap, globalJobId, KEY_RATE_LIMIT), fallback.getRateLimitPerSecond()));
        builder.circuitBreakerFailureRateThreshold(parseInt(
                cfgKey(configMap, globalJobId, KEY_CB_FAILURE_RATE), fallback.getCircuitBreakerFailureRateThreshold()));
        builder.circuitBreakerSlidingWindowSize(parseInt(
                cfgKey(configMap, globalJobId, KEY_CB_SLIDING_WINDOW), fallback.getCircuitBreakerSlidingWindowSize()));
        builder.defaultTimeoutMs(parseInt(
                cfgKey(configMap, globalJobId, KEY_DEFAULT_TIMEOUT), fallback.getDefaultTimeoutMs()));

        return builder.build();
    }

    /**
     * 取 IMap 中某个治理 key 的值（作业级优先，全局回退）。
     *
     * @param map          配置 IMap
     * @param globalJobId  非空表示作业级 key `job.{jobId}.{bareKey}` 优先；空表示仅读全局裸 key
     * @param bareKey      全局裸 key（如 {@link #KEY_RATE_LIMIT}）
     * @return 优先取值；两级均缺失返回 null，由调用方回退 fallback
     */
    private static String cfgKey(IMap<String, String> map, String globalJobId, String bareKey) {
        if (globalJobId != null) {
            final String jobLevelValue = map.get(JOB_KEY_PREFIX + globalJobId + "." + bareKey);
            if (jobLevelValue != null) {
                return jobLevelValue;
            }
        }
        return map.get(bareKey);
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
            refreshConfig(event.getKey());
        }, true));
        ids.add(map.addEntryListener((EntryUpdatedListener<String, String>) event -> {
            log.info("Config entry updated: {} = {}", event.getKey(), event.getValue());
            refreshConfig(event.getKey());
        }, true));
        ids.add(map.addEntryListener((EntryRemovedListener<String, String>) event -> {
            log.info("Config entry removed: {}", event.getKey());
            refreshConfig(event.getKey());
        }, true));
        ids.add(map.addEntryListener((EntryEvictedListener<String, String>) event -> {
            log.info("Config entry evicted: {}", event.getKey());
            refreshConfig(event.getKey());
        }, true));
        ids.add(map.addEntryListener((EntryExpiredListener<String, String>) event -> {
            log.info("Config entry expired: {}", event.getKey());
            refreshConfig(event.getKey());
        }, true));
        return ids;
    }

    /**
     * 配置变更时按 key 类型路由刷新：
     * <ul>
     *   <li>{@code job.{jobId}.{bareKey}} 作业级 key → 仅刷新该作业灵元（作业级覆盖全局）；</li>
     *   <li>其余（全局裸 key / 未知）→ 刷新共享灵元 + 全部已注册作业灵元（全局广播）。</li>
     * </ul>
     */
    private void refreshConfig(String changedKey) {
        if (lingRepository == null || configMap == null) {
            return;
        }
        if (changedKey != null && changedKey.startsWith(JOB_KEY_PREFIX)) {
            final String jobId = parseJobIdFromKey(changedKey);
            if (jobId != null) {
                refreshLing(JOB_LING_PREFIX + jobId, jobId);
                return;
            }
        }
        refreshLing(VIRTUAL_LING_ID, null);
        refreshAllJobLings();
    }

    /**
     * 从作业级 key `job.{jobId}.{bareKey}` 解析 jobId；格式异常返回 null。
     */
    private static String parseJobIdFromKey(String key) {
        final int dot = key.indexOf('.', JOB_KEY_PREFIX.length());
        if (dot < 0) {
            return null;
        }
        final String jobId = key.substring(JOB_KEY_PREFIX.length(), dot);
        return jobId.isEmpty() ? null : jobId;
    }
}
