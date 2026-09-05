package com.lingframe.agent.pipeline;

import com.lingframe.agent.config.AgentConfig;
import com.lingframe.agent.config.HazelcastConfigCenter;
import com.lingframe.core.config.LingFrameConfig;
import com.lingframe.core.event.EventBus;
import com.lingframe.core.fsm.RuntimeCoordinator;
import com.lingframe.core.governance.LocalGovernanceRegistry;
import com.lingframe.core.invoker.FastLingServiceInvoker;
import com.lingframe.core.ling.DefaultLingRepository;
import com.lingframe.core.ling.DefaultLingResourceManager;
import com.lingframe.core.ling.DefaultLingServiceRegistry;
import com.lingframe.core.ling.InvokableMethodCache;
import com.lingframe.core.ling.LingRepository;
import com.lingframe.core.ling.LingRuntimeConfig;
import com.lingframe.core.ling.LingServiceRegistry;
import com.lingframe.core.ling.LingUnloadCoordinator;
import com.lingframe.core.ling.VirtualLingManager;
import com.lingframe.core.metrics.GovernanceMetricsCollector;
import com.lingframe.core.metrics.MetricsCollector;
import com.lingframe.core.pipeline.FilterRegistry;
import com.lingframe.core.pipeline.FilterRegistryConfig;
import com.lingframe.core.pipeline.InvocationPipelineEngine;
import com.lingframe.core.resource.DebuggerCaptureUnloadHook;
import com.lingframe.core.resource.DefaultLeakDetector;
import com.lingframe.core.resource.JdbcDriverUnloadHook;
import com.lingframe.core.resource.JvmShutdownHookUnloadHook;
import com.lingframe.core.resource.LoggingFrameworkUnloadHook;
import com.lingframe.core.resource.RmiTargetUnloadHook;
import com.lingframe.core.resource.ThreadReferenceUnloadHook;
import com.lingframe.core.routing.LabelMatchRouter;
import com.lingframe.core.security.DefaultPermissionService;
import com.lingframe.core.spi.LeakDetector;
import com.lingframe.core.spi.LingServiceInvoker;
import com.lingframe.core.spi.LingUnloadHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Agent 治理流水线工厂。
 * <p>
 * 编程式装配 LingFrame 治理微内核，零 Spring 依赖。
 * 参考 {@code NativeLingFrame.start()} 的装配方式，但省略灵元加载、热重载、
 * SharedApiManager 冻结等重型设施——Agent 只需要治理流水线本身，不需要灵元生命周期管理。
 * <p>
 * 装配的组件：
 * <ul>
 *   <li>强依赖：{@link InvokableMethodCache}、{@link DefaultPermissionService}</li>
 *   <li>关键可选依赖：{@link EventBus}、{@link LocalGovernanceRegistry}、
 *       {@link RuntimeCoordinator}、{@link MetricsCollector}、
 *       {@link GovernanceMetricsCollector}、{@link DefaultLingRepository}、
 *       {@link DefaultLingServiceRegistry}、{@link LabelMatchRouter}、
 *       {@link FastLingServiceInvoker}</li>
 * </ul>
 * 这使 12 个内建 Filter 全部装配，治理链完整可用。
 * <p>
 * GOVERN_ONLY 模式下的治理生效范围：
 * <ul>
 *   <li>已生效：指标收集（TrafficMetricsFilter）、事件发布（EventBus）、审计追踪、
 *       熔断限流（ResilienceGovernanceFilter——通过注册虚拟灵元激活）</li>
 *   <li>退化放行：路由（ContractProviderRoutingFilter/InstanceRoutingFilter）、
 *       状态守卫（MacroStateGuardFilter——虚拟灵元注册为 ACTIVE 后放行）</li>
 *   <li>权限审计（PermissionGovernanceFilter）：devMode=true 时未声明权限放行，
 *       devMode=false 时零信任拒绝</li>
 * </ul>
 */
public final class AgentPipelineFactory {

    private static final Logger log = LoggerFactory.getLogger(AgentPipelineFactory.class);

    private AgentPipelineFactory() {
    }

    /**
     * 创建治理运行时，包含流水线引擎与卸载协调器。
     *
     * @param agentConfig Agent 配置，提供 devMode 等参数
     * @return 已装配完整 Filter 链的治理运行时
     */
    public static AgentGovernanceRuntime create(AgentConfig agentConfig) {
        final boolean devMode = agentConfig != null && agentConfig.isDevMode();
        final boolean permissionEnabled = agentConfig == null || agentConfig.isPermissionEnabled();
        final boolean resilienceEnabled = agentConfig == null
                || agentConfig.isCircuitBreakerEnabled()
                || agentConfig.isRateLimiterEnabled();
        final boolean grayRoutingEnabled = agentConfig != null && agentConfig.isGrayRoutingEnabled();

        // 权限关闭时强制 devMode=true，使 PermissionGovernanceFilter 未声明权限即放行
        final boolean effectiveDevMode = devMode || !permissionEnabled;
        log.info("Assembling LingFrame governance pipeline (programmatic, zero-Spring, devMode={}, " +
                "permissionEnabled={}, resilienceEnabled={}, grayRoutingEnabled={})",
                effectiveDevMode, permissionEnabled, resilienceEnabled, grayRoutingEnabled);

        final LingFrameConfig config = LingFrameConfig.builder()
                .devMode(effectiveDevMode)
                .build();

        final EventBus eventBus = new EventBus();
        final RuntimeCoordinator runtimeCoordinator = new RuntimeCoordinator(eventBus);
        runtimeCoordinator.start();
        final LocalGovernanceRegistry governanceRegistry = resilienceEnabled
                ? new LocalGovernanceRegistry(eventBus)
                : null;

        final DefaultPermissionService permissionService =
                new DefaultPermissionService(eventBus, config);

        final LingRepository lingRepository = new DefaultLingRepository();
        final LingServiceRegistry lingServiceRegistry = new DefaultLingServiceRegistry();

        registerVirtualLing(lingRepository, runtimeCoordinator, eventBus, agentConfig, resilienceEnabled);

        final InvokableMethodCache methodCache = new InvokableMethodCache();
        final LingServiceInvoker invoker = new FastLingServiceInvoker();

        final MetricsCollector metricsCollector = new MetricsCollector(lingRepository);
        final GovernanceMetricsCollector governanceMetricsCollector = new GovernanceMetricsCollector();

        final LabelMatchRouter trafficRouter = grayRoutingEnabled ? new LabelMatchRouter() : null;

        final FilterRegistry filterRegistry = new FilterRegistry(FilterRegistryConfig.builder()
                .methodCache(methodCache)
                .permissionService(permissionService)
                .serviceInvoker(invoker)
                .lingRepository(lingRepository)
                .trafficRouter(trafficRouter)
                .eventBus(eventBus)
                .serviceRegistry(lingServiceRegistry)
                .metricsCollector(metricsCollector)
                .runtimeCoordinator(runtimeCoordinator)
                .governanceMetricsCollector(governanceMetricsCollector)
                .lingFrameInfo(config)
                .governanceRegistry(governanceRegistry)
                .build());

        final InvocationPipelineEngine pipelineEngine = new InvocationPipelineEngine(filterRegistry);

        final DefaultLingResourceManager resourceManager =
                new DefaultLingResourceManager(lingRepository, eventBus, methodCache);
        final LeakDetector leakDetector = new DefaultLeakDetector(eventBus, config);
        final List<LingUnloadHook> jvmHooks = Arrays.asList(
                new JdbcDriverUnloadHook(),
                new ThreadReferenceUnloadHook(),
                new JvmShutdownHookUnloadHook(),
                new RmiTargetUnloadHook(),
                new LoggingFrameworkUnloadHook(),
                new DebuggerCaptureUnloadHook());
        final LingUnloadCoordinator unloadCoordinator = new LingUnloadCoordinator(
                pipelineEngine,
                Collections.emptyList(),
                jvmHooks,
                resourceManager,
                leakDetector);

        log.info("LingFrame governance pipeline assembled: 12 filters registered, unload coordinator with {} JVM hooks",
                jvmHooks.size());

        final HazelcastConfigCenter configCenter = new HazelcastConfigCenter(lingRepository);
        configCenter.tryInit();

        return new AgentGovernanceRuntime(pipelineEngine, unloadCoordinator, lingRepository, configCenter, eventBus, metricsCollector);
    }

    /**
     * 虚拟灵元 ID——SeaTunnel 治理切点的统一灵元标识。
     */
    private static final String VIRTUAL_LING_ID = "seatunnel";

    /**
     * 注册虚拟灵元到 LingRepository，使 ResilienceGovernanceFilter 的限流/熔断生效。
     * <p>
     * 架构规范升级：通过灵核一等公民领域服务 {@link VirtualLingManager} 统一注册并激活虚拟灵元，
     * 彻底消灭对手动状态机时序（register → 构造 → 仓储登记 → ACTIVE）的外部耦合，
     * 保持与灵珑底座以及 Spring Boot 环境中 VirtualLingManager 治理契约的 100% 一致。
     */
    private static void registerVirtualLing(LingRepository lingRepository,
                                            RuntimeCoordinator runtimeCoordinator,
                                            EventBus eventBus,
                                            AgentConfig agentConfig,
                                            boolean resilienceEnabled) {
        if (!resilienceEnabled) {
            log.info("Resilience disabled, skipping virtual ling registration");
            return;
        }

        try {
            final LingRuntimeConfig.LingRuntimeConfigBuilder configBuilder = LingRuntimeConfig.builder()
                    .maxHistorySnapshots(1)
                    .bulkheadMaxConcurrent(10);

            if (agentConfig != null) {
                configBuilder
                        .rateLimitPerSecond(agentConfig.getRateLimitPerSecond())
                        .circuitBreakerFailureRateThreshold(agentConfig.getCircuitBreakerFailureRateThreshold())
                        .circuitBreakerSlidingWindowSize(agentConfig.getCircuitBreakerSlidingWindowSize())
                        .defaultTimeoutMs(agentConfig.getDefaultTimeoutMs());
            }

            final LingRuntimeConfig runtimeConfig = configBuilder.build();
            final VirtualLingManager virtualLingManager = new VirtualLingManager(lingRepository, runtimeCoordinator, eventBus);
            virtualLingManager.register(VIRTUAL_LING_ID, runtimeConfig);

            log.info("Virtual ling [{}] registered via VirtualLingManager, rateLimit={}/s, " +
                            "circuitBreakerFailureRate={}%, slidingWindow={}",
                    VIRTUAL_LING_ID,
                    runtimeConfig.getRateLimitPerSecond(),
                    runtimeConfig.getCircuitBreakerFailureRateThreshold(),
                    runtimeConfig.getCircuitBreakerSlidingWindowSize());
        } catch (Exception e) {
            log.warn("Failed to register virtual ling [{}], resilience filters will degrade to passthrough: {}",
                    VIRTUAL_LING_ID, e.getMessage());
        }
    }
}
