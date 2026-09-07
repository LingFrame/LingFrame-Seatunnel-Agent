package com.lingframe.agent.observability;

import com.lingframe.agent.adapter.SeaTunnelAdapter;
import com.lingframe.agent.config.AgentConfig;
import com.lingframe.core.event.EventBus;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link LingFrameAgentObservabilityMBean} 实现。
 * <p>
 * 在 premain 末尾注册到平台 MBeanServer（{@code com.lingframe.agent:type=Observability}）。
 * 持有 EventBus / SeaTunnelAdapter 的动态引用，getter 调用时实时查询（非快照），
 * 保证运维读取的是当前值；advice 安装状态是 premain 期确定的快照（不变）。
 */
public class LingFrameAgentObservability implements LingFrameAgentObservabilityMBean {

    /** advice 状态：target 类名 → INSTALLED / ABSENT / SKIPPED / DISABLED */
    private final Map<String, String> adviceStatus;
    private final AgentConfig config;
    private final EventBus eventBus;
    private final SeaTunnelAdapter adapter;

    /**
     * @param adapter SeaTunnelAdapter 实例（governance 关时可为 null）。
     *                <b>签名刻意用 {@code Object} 而非 {@code SeaTunnelAdapter}</b>：
     *                premain 类加载验证期会强解析 invokespecial 构造签名中的参数类型，
     *                若直接写 {@code SeaTunnelAdapter} 会触发其 implements 的
     *                {@code LingGovernanceContract} 从 AppClassLoader 提前加载
     *                （早于 appendToBootstrap），形成 Bootstrap/App 双副本，导致
     *                registerContract 的 instanceof 校验失败（同类回归，见 CHANGELOG bridge 双副本）。
     *                MBean 类加载在运行期（appendToBootstrap 之后），双亲委派命中 Bootstrap 单一副本，安全。
     */
    public LingFrameAgentObservability(AgentConfig config,
                                      Map<String, String> adviceStatus,
                                      EventBus eventBus,
                                      Object adapter) {
        this.config = config;
        // advice 状态为 premain 期确定的快照，防御性拷贝 + 不可变包装，防外部继续变更
        this.adviceStatus = Collections.unmodifiableMap(new HashMap<>(adviceStatus));
        this.eventBus = eventBus;
        this.adapter = adapter instanceof SeaTunnelAdapter ? (SeaTunnelAdapter) adapter : null;
    }

    @Override
    public boolean isTaskExecutionAdviceInstalled() {
        return "INSTALLED".equals(adviceStatus.get("AbstractTask"));
    }

    @Override
    public boolean isClassLoaderReleaseAdviceInstalled() {
        return "INSTALLED".equals(adviceStatus.get("DefaultClassLoaderService"));
    }

    @Override
    public boolean isTcclGuardAdviceInstalled() {
        return "INSTALLED".equals(adviceStatus.get("TcclGuard"));
    }

    @Override
    public String getAbstractTaskClassStatus() {
        return statusOrEmpty("AbstractTask");
    }

    @Override
    public String getClassloaderServiceClassStatus() {
        return statusOrEmpty("DefaultClassLoaderService");
    }

    private String statusOrEmpty(String key) {
        final String s = adviceStatus.get(key);
        return s != null ? s : "UNKNOWN";
    }

    @Override
    public boolean isTimingEnabled() {
        return config != null && config.isTimingEnabled();
    }

    @Override
    public long getHookCallCount() {
        return adapter != null ? adapter.getHookCallCount() : 0L;
    }

    @Override
    public long getHookLatencyAvgNanos() {
        return adapter != null ? adapter.getHookLatencyAvgNanos() : 0L;
    }

    @Override
    public long getHookLatencyMinNanos() {
        return adapter != null ? adapter.getHookLatencyMinNanos() : 0L;
    }

    @Override
    public long getHookLatencyMaxNanos() {
        return adapter != null ? adapter.getHookLatencyMaxNanos() : 0L;
    }

    @Override
    public int getEventBusQueueSize() {
        try {
            return eventBus != null ? eventBus.getQueueSize() : -1;
        } catch (Throwable t) {
            return -1;
        }
    }

    @Override
    public long getEventBusDroppedCount() {
        try {
            return eventBus != null ? eventBus.getDroppedAsyncEvents() : -1L;
        } catch (Throwable t) {
            return -1L;
        }
    }

    @Override
    public long getEventBusSubmittedCount() {
        try {
            return eventBus != null ? eventBus.getSubmittedAsyncEvents() : -1L;
        } catch (Throwable t) {
            return -1L;
        }
    }

    @Override
    public String getConfigSummary() {
        if (config == null) {
            return "config=null";
        }
        return new StringBuilder(256)
                .append("governance=").append(config.isGovernanceEnabled())
                .append(", taskAdvice(eff)=").append(config.isEffectiveTaskExecutionAdviceEnabled())
                .append(", devMode=").append(config.isDevMode())
                .append(", traceLevel=").append(config.getTraceLogLevel())
                .append(", auditLevel=").append(config.getAuditLogLevel())
                .append(", sampleRate=").append(config.getLogSampleRate())
                .append(", timing=").append(config.isTimingEnabled())
                .append(", classifier=").append(config.isClassifierEnabled())
                .toString();
    }

    @Override
    public void resetCircuitBreaker() {
        if (adapter == null) {
            return;
        }
        adapter.resetHealthMetrics();
    }
}
