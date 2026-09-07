package com.lingframe.agent.pipeline;

import com.lingframe.agent.config.HazelcastConfigCenter;
import com.lingframe.core.event.EventBus;
import com.lingframe.core.ling.LingRepository;
import com.lingframe.core.ling.LingUnloadCoordinator;
import com.lingframe.core.ling.VirtualLingManager;
import com.lingframe.core.metrics.MetricsCollector;
import com.lingframe.core.pipeline.InvocationPipelineEngine;

/**
 * Agent 治理运行时聚合体，持有流水线引擎与卸载协调器。
 * <p>
 * 由 {@link AgentPipelineFactory} 编程式装配后返回，供 Agent 入口分发到各组件：
 * <ul>
 *   <li>{@link InvocationPipelineEngine} 传入 {@code SeaTunnelAdapter} 供治理代理调用</li>
 *   <li>{@link LingUnloadCoordinator} 传入 {@code SeaTunnelAdapter} 供物理释放委托</li>
 *   <li>{@link LingRepository} 供运行期查询虚拟灵元状态</li>
 *   <li>{@link HazelcastConfigCenter} 供分布式配置中心延迟初始化与配置同步</li>
 *   <li>{@link EventBus} 供 {@code SeaTunnelAdapter} 订阅 Trace/Event 监控事件</li>
 *   <li>{@link MetricsCollector} 供 {@code SeaTunnelAdapter} 回灌真实业务结果到 LingHealthMetrics</li>
 * </ul>
 */
public final class AgentGovernanceRuntime {

    private final InvocationPipelineEngine pipelineEngine;
    private final LingUnloadCoordinator unloadCoordinator;
    private final LingRepository lingRepository;
    private final HazelcastConfigCenter configCenter;
    private final EventBus eventBus;
    private final MetricsCollector metricsCollector;
    /** 虚拟灵元注册入口（作业级治理复用：共享灵元 + 作业灵元统一经此注册/注销；resilience 关闭时为 null）。 */
    private final VirtualLingManager virtualLingManager;

    public AgentGovernanceRuntime(InvocationPipelineEngine pipelineEngine,
                                  LingUnloadCoordinator unloadCoordinator,
                                  LingRepository lingRepository,
                                  HazelcastConfigCenter configCenter,
                                  EventBus eventBus,
                                  MetricsCollector metricsCollector,
                                  VirtualLingManager virtualLingManager) {
        this.pipelineEngine = pipelineEngine;
        this.unloadCoordinator = unloadCoordinator;
        this.lingRepository = lingRepository;
        this.configCenter = configCenter;
        this.eventBus = eventBus;
        this.metricsCollector = metricsCollector;
        this.virtualLingManager = virtualLingManager;
    }

    public InvocationPipelineEngine getPipelineEngine() {
        return pipelineEngine;
    }

    public LingUnloadCoordinator getUnloadCoordinator() {
        return unloadCoordinator;
    }

    public LingRepository getLingRepository() {
        return lingRepository;
    }

    public HazelcastConfigCenter getConfigCenter() {
        return configCenter;
    }

    public EventBus getEventBus() {
        return eventBus;
    }

    public MetricsCollector getMetricsCollector() {
        return metricsCollector;
    }

    public VirtualLingManager getVirtualLingManager() {
        return virtualLingManager;
    }
}
