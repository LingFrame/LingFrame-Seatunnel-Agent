package com.lingframe.agent;

import com.lingframe.agent.adapter.JobIdExtractor;
import com.lingframe.agent.adapter.JobLingRegistry;
import com.lingframe.agent.adapter.SeaTunnelAdapter;
import com.lingframe.agent.bridge.LingFrameAgentBridge;
import com.lingframe.agent.config.AgentConfig;
import com.lingframe.agent.advice.ClassLoaderReleaseAdvice;
import com.lingframe.agent.advice.ClassLoaderServiceCacheAdvice;
import com.lingframe.agent.advice.ClassLoaderServiceGetAdvice;
import com.lingframe.agent.advice.JobMasterCleanJobAdvice;
import com.lingframe.agent.advice.TaskExecutionAdvice;
import com.lingframe.agent.advice.TaskExecutionServiceCacheAdvice;
import com.lingframe.agent.advice.TcclGuardAdvice;
import com.lingframe.agent.cleaner.EngineClassLoaderCleaner;
import com.lingframe.agent.pipeline.AgentGovernanceRuntime;
import com.lingframe.agent.pipeline.AgentPipelineFactory;
import com.lingframe.agent.observability.LingFrameAgentObservability;
import com.lingframe.core.ling.LingRuntimeConfig;
import net.bytebuddy.agent.builder.AgentBuilder;

import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.utility.JavaModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.instrument.Instrumentation;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.management.ObjectName;

/**
 * 治理激活运行器（独立于 {@link LingFrameAgentPremain} 的委托目标）。
 * <p>
 * 双副本根因（JMH governed fork 实测复现）决定了它必须独立成类、且只允许被
 * {@code premain()} 以反射方式加载：
 * <ol>
 *   <li>{@code -javaagent} 规范会把 Agent Fat-Jar 自动追加进 system classpath，
 *       core/adapter/bridge 类全部对 AppClassLoader 可见。</li>
 *   <li>JVM 反射调用 {@code premain()} 前必须 link 入口类——若入口类字节码直接引用
 *       {@code SeaTunnelAdapter}（如 {@code new SeaTunnelAdapter}），link 的类型检查
 *       就会在 {@code appendToBootstrap} 尚未执行时把 {@code LingGovernanceContract}
 *       从 AppClassLoader 抢载成 App 副本，与运行期注入的 Bootstrap 副本形成双份，
 *       {@code registerContract} 的 instanceof 失配，治理整体 fail-open。</li>
 *   <li>把激活逻辑下沉到本类后，入口类字节码仅含 JDK 类与字符串常量，link 期不再
 *       解析任何 core/bridge 符号；本类由 premain 运行时（Bridge 契约已注入
 *       Bootstrap）反射加载，其内部符号经双亲委派统一解析到 Bootstrap 单副本。</li>
 * </ol>
 * 本类只承载 {@code activate} 及其辅助方法，不暴露任何公开构造（工具类语义）。
 */
public final class LingFrameAgentActivationRunner {

    private static final Logger log = LoggerFactory.getLogger(LingFrameAgentActivationRunner.class);

    /** ClassLoader 清理调度器引用——保存以便 Agent 卸载时主动 shutdown */
    private static ScheduledExecutorService cleanerScheduler;

    private static final String CLASSLOADER_SERVICE =
            "org.apache.seatunnel.engine.core.classloader.DefaultClassLoaderService";
    private static final String ABSTRACT_TASK_TYPE =
            "org.apache.seatunnel.engine.server.task.AbstractTask";
    /** 引擎执行服务类型——捕获其实例，清理已完成作业上下文中强持有的 ClassLoader 引用 */
    private static final String TASK_EXECUTION_SERVICE_TYPE =
            "org.apache.seatunnel.engine.server.TaskExecutionService";
    /** 引擎 Master 作业服务类型——拦截 cleanJob，在 Coordinator 终态强制排空作业 ClassLoader 缓存 */
    private static final String JOB_MASTER_TYPE =
            "org.apache.seatunnel.engine.server.master.JobMaster";

    /**
     * 治理微内核实际装配入口。被 {@link LingFrameAgentPremain#premain} 反射调用，
     * 仅允许在本方法内部引用 core/adapter/bridge 类（此时 Bridge 契约已在 Bootstrap）。
     *
     * @param agentArgs Agent 参数（配置文件路径等）
     * @param inst      JVM Instrumentation 实例
     */
    public static void activate(String agentArgs, Instrumentation inst) {
        log.info("LingFrame SeaTunnel Agent starting...");

        final AgentConfig config = AgentConfig.load(agentArgs);

        // 引擎 ClassLoader 清理必须在治理门控之前安装：Metaspace 泄漏治理在任何模式均生效
        installEngineClassLoaderCleanup(inst);

        final boolean governanceEnabled = config.isGovernanceEnabled();

        // Bridge 契约注入已由 premain() 完成（见 LingFrameAgentPremain.premain），
        // 此处 LingGovernanceContract 经双亲委派统一由 Bootstrap 加载，单一副本。

        // 初始化治理微内核与适配器（即使业务治理关闭，卸载协调器与底座钩子依然注册，确保物理释放正常生效）
        final AgentGovernanceRuntime governanceRuntime = initGovernanceRuntime(config);
        final SeaTunnelAdapter adapter = new SeaTunnelAdapter(
                config,
                governanceRuntime != null ? governanceRuntime.getPipelineEngine() : null,
                governanceRuntime != null ? governanceRuntime.getUnloadCoordinator() : null,
                governanceRuntime != null ? governanceRuntime.getConfigCenter() : null,
                governanceRuntime != null ? governanceRuntime.getEventBus() : null,
                governanceRuntime != null ? governanceRuntime.getMetricsCollector() : null);

        final Map<String, String> adviceStatus = new HashMap<>();

        if (governanceEnabled) {
            // 作业级治理装配：运行时指纹门控 + JobLingRegistry（隔离单元从引擎收敛到作业）
            wireJobLevelGovernance(config, adapter, governanceRuntime, adviceStatus);
        }

        // 注册治理契约到 Bridge（提供 ClassLoader 清理与物理释放能力）
        LingFrameAgentBridge.registerContract(adapter);
        // 织入 ByteBuddy 拦截器（DefaultClassLoaderService 与 TcclGuard 在任何模式均织入；AbstractTask 由 isEffectiveTaskExecutionAdviceEnabled 门控）
        installByteBuddyAdvice(inst, config, adviceStatus);
        // 注册可观测性 MBean（JMX），运维可 jcmd/jconsole 直接读 advice 状态/EventBus/计时
        registerObservabilityMBean(config, adviceStatus, governanceRuntime, adapter);

        if (!governanceEnabled) {
            log.info("Governance is disabled by config, Agent runs in PASS_THROUGH / ClassLoader-cleanup-only mode");
        } else {
            log.info("LingFrame SeaTunnel Agent activated successfully");
        }
    }

    private static AgentGovernanceRuntime initGovernanceRuntime(AgentConfig config) {
        try {
            return AgentPipelineFactory.create(config);
        } catch (Exception e) {
            log.warn("Failed to initialize governance pipeline, falling back to TRACE_ONLY mode", e);
            return null;
        }
    }

    /**
     * 作业级治理装配（指纹门控 + 能力位传递）。
     * <p>
     * {@code per-job-governance-enabled} 默认开启；装配与否取决于运行期
     * 版本指纹校验（{@link #checkJobLevelFieldPresent()}）与治理运行时可用性——任一前提
     * 不满足即降级共享灵元 {@code seatunnel}（引擎级隔离）。
     * <p>
     * 诚实降级原则：在未知 SeaTunnel 版本上，「降级引擎级」比「猜字段名后
     * 拿错 jobId、挂在错误隔离单元上」更安全。故版本指纹缺失时宁可放弃作业级隔离。
     *
     * @param adviceStatus MBean 可观测状态（暴露 {@code JobIdExtractor} 能力位：INSTALLED /
     *                     UNSUPPORTED_VERSION / NO_RUNTIME / DISABLED）
     */
    private static void wireJobLevelGovernance(AgentConfig config, SeaTunnelAdapter adapter,
                                               AgentGovernanceRuntime runtime,
                                               Map<String, String> adviceStatus) {
        if (config == null || !config.isPerJobGovernanceEnabled()) {
            adviceStatus.put("JobIdExtractor", "DISABLED");
            return;
        }
        if (!checkJobLevelFieldPresent()) {
            adviceStatus.put("JobIdExtractor", "UNSUPPORTED_VERSION");
            log.error("Job-level governance DISABLED — AbstractTask.jobID field not present. "
                    + "Degrading to shared ling 'seatunnel' (engine-level isolation). "
                    + "SeaTunnel version may have renamed/restructured the jobID field.");
            return;
        }
        if (runtime == null || runtime.getVirtualLingManager() == null) {
            adviceStatus.put("JobIdExtractor", "NO_RUNTIME");
            log.warn("Job-level governance DISABLED — governance runtime / virtual ling manager unavailable, "
                    + "degrading to shared ling 'seatunnel'.");
            return;
        }
        final JobIdExtractor extractor = new JobIdExtractor(true);
        final JobLingRegistry registry = new JobLingRegistry(
                runtime.getVirtualLingManager(),
                runtime.getPipelineEngine(),
                runtime.getMetricsCollector(),
                buildJobLingTemplate(config),
                config.getPerJobMaxTrackedJobs(),
                config.getPerJobIdleTtlMs(),
                config.getPerJobReapIntervalMs());
        adapter.setJobLevelGovernance(extractor, registry);
        adviceStatus.put("JobIdExtractor", "INSTALLED");
        log.info("Job-level governance ENABLED — failure isolation unit converged to job granularity "
                + "(maxTrackedJobs={}, idleTtlMs={}ms, reapIntervalMs={}ms)",
                config.getPerJobMaxTrackedJobs(), config.getPerJobIdleTtlMs(), config.getPerJobReapIntervalMs());
    }

    /**
     * 从 {@code AbstractTask} 构建作业灵元治理配置模板，各作业独立实例。
     * <p>
     * 复用 AgentConfig 的限流/熔断/超时参数，与共享灵元的治理强度一致，仅治理域不同。
     * minimum-calls 按 {@link AgentPipelineFactory} 同款规则钳制到 (0, windowSize]，
     * 避免灵核构造器校验越界抛异常导致作业熔断器静默失效。
     */
    private static LingRuntimeConfig buildJobLingTemplate(AgentConfig config) {
        final int windowSize = config.getCircuitBreakerSlidingWindowSize();
        final int minimumCalls = Math.max(1,
                Math.min(config.getCircuitBreakerMinimumNumberOfCalls(), windowSize));
        return LingRuntimeConfig.builder()
                .maxHistorySnapshots(1)
                .bulkheadMaxConcurrent(10)
                .rateLimitPerSecond(config.getRateLimitPerSecond())
                .circuitBreakerFailureRateThreshold(config.getCircuitBreakerFailureRateThreshold())
                .circuitBreakerSlidingWindowSize(windowSize)
                .circuitBreakerMinimumNumberOfCalls(minimumCalls)
                .defaultTimeoutMs(config.getDefaultTimeoutMs())
                .build();
    }

    /**
     * 运行时指纹门控：校验 {@code AbstractTask} 是否声明 `protected final long jobID` 字段。
     * <p>
     * 与既有 {@link #validateClassLoaderServiceFields()} 同构——SeaTunnel 演进重命名字段后
     * ByteBuddy 织入会静默跳过，作业级治理若不门控会拿错 jobId 挂在错误隔离单元上。
     * 指纹缺失时返回 false，由调用方降级共享灵元并暴露 MBean 能力位，绝不猜测字段名。
     *
     * @return true 表示字段存在，作业级治理可安全启用
     */
    private static boolean checkJobLevelFieldPresent() {
        try {
            final Class<?> clazz = Class.forName(ABSTRACT_TASK_TYPE);
            for (Field f : clazz.getDeclaredFields()) {
                if (("jobID".equals(f.getName()) || "jobId".equals(f.getName()))
                        && (f.getType() == long.class || f.getType() == Long.class)) {
                    return true;
                }
            }
            return false;
        } catch (ClassNotFoundException e) {
            log.warn("AbstractTask not found on classpath, job-level governance disabled: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 检测 SeaTunnel 目标类是否包含 Advice 依赖的私有字段。
     * <p>
     * {@code @Advice.FieldValue} 在织入期硬编码字段名，若 SeaTunnel 版本升级重命名字段，
     * ByteBuddy 会静默跳过织入——ClassLoader 泄漏不会立即表现，而是以 Metaspace OOM 形式爆发。
     * 此方法在织入前通过反射检测字段存在性，缺失时打印 ERROR 日志预警。
     *
     * @return true 表示字段全部存在，可安全织入 ClassLoader 拦截器
     */
    private static boolean validateClassLoaderServiceFields() {
        try {
            final Class<?> clazz = Class.forName(CLASSLOADER_SERVICE);
            boolean hasCacheMode = false;
            boolean hasClassLoaderCache = false;
            for (Field f : clazz.getDeclaredFields()) {
                if ("cacheMode".equals(f.getName())) {
                    hasCacheMode = true;
                }
                if ("classLoaderCache".equals(f.getName())) {
                    hasClassLoaderCache = true;
                }
            }
            if (!hasCacheMode || !hasClassLoaderCache) {
                log.error("DefaultClassLoaderService field validation FAILED — " +
                        "cacheMode={}, classLoaderCache={}. These fields are required by ClassLoaderReleaseAdvice. " +
                        "ClassLoader leak interception will be SKIPPED. " +
                        "SeaTunnel version may have renamed or restructured these fields.",
                        hasCacheMode, hasClassLoaderCache);
                return false;
            }
            log.debug("DefaultClassLoaderService field validation passed: cacheMode and classLoaderCache present");
            return true;
        } catch (ClassNotFoundException e) {
            log.warn("DefaultClassLoaderService not found on classpath, skipping ClassLoader advice: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 引擎 ClassLoader 清理织入（独立于治理门控，Metaspace 泄漏治理在任何模式均生效）。
     * <p>
     * 织入 {@code TaskExecutionService#getExecutionContext} 捕获引擎实例，并由后台守护线程
     * 周期调用 {@link EngineClassLoaderCleaner#cleanFinished()}，释放已完成作业上下文中强持有的
     * ClassLoader/jars/taskGroup 引用。全程反射、失败降级，绝不破坏引擎运行。
     */
    private static void installEngineClassLoaderCleanup(Instrumentation inst) {
        try {
            final AgentBuilder builder = new AgentBuilder.Default()
                    .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
                    .with(AgentBuilder.TypeStrategy.Default.REDEFINE)
                    .with(new AgentBuilder.Listener.Adapter() {
                        @Override
                        public void onError(String typeName, ClassLoader classLoader, JavaModule module,
                                            boolean loaded, Throwable throwable) {
                            log.error("ByteBuddy weaving FAILED for type [{}] (engine ClassLoader cleanup "
                                    + "INACTIVE): {}", typeName, throwable.getMessage(), throwable);
                        }
                    })
                    .ignore(ElementMatchers.nameStartsWith("net.bytebuddy.")
                            .or(ElementMatchers.nameStartsWith("com.lingframe.agent."))
                            .or(ElementMatchers.isSynthetic()))
                    .type(ElementMatchers.named(TASK_EXECUTION_SERVICE_TYPE))
                    .transform(new AgentBuilder.Transformer.ForAdvice()
                            .include(TaskExecutionServiceCacheAdvice.class.getClassLoader())
                            .advice(ElementMatchers.isConstructor()
                                            .or(ElementMatchers.named("start"))
                                            .or(ElementMatchers.named("getExecutionContext")),
                                    TaskExecutionServiceCacheAdvice.class.getName()));
            builder.installOn(inst);
            cleanerScheduler =
                    Executors.newSingleThreadScheduledExecutor(r -> {
                        final Thread t = new Thread(r, "ling-engine-classloader-cleaner");
                        t.setDaemon(true);
                        return t;
                    });
            cleanerScheduler.scheduleWithFixedDelay(EngineClassLoaderCleaner::cleanFinished, 1, 1, TimeUnit.SECONDS);
            log.info("EngineClassLoaderCleanup ENABLED — capturing TaskExecutionService, releasing finished "
                    + "job ClassLoader refs (interval=1s)");
        } catch (Throwable t) {
            log.warn("EngineClassLoaderCleanup setup FAILED (Metaspace governance degraded)", t);
        }
    }

    private static void installByteBuddyAdvice(Instrumentation inst, AgentConfig config,
                                               Map<String, String> adviceStatus) {
        // AgentBuilder 是不可变 fluent API：type()/transform()/ignore() 都返回新实例，
        // 返回值必须逐级赋回，否则配置被丢弃、installOn 安装的是空白 builder。
        // 实证（ByteBuddy 1.14.12）：默认 ignoreMatcher 对 Bootstrap 加载的
        // java.lang.Thread 返回 true（静默忽略）。ignore() 是替换语义——
        // 必须显式替换默认 matcher，同时保留 net.bytebuddy（防自织入递归）/
        // com.lingframe.agent（防 Agent 自身类被织入）/ synthetic 的忽略，
        // java.lang.Thread 才能被织入。
        AgentBuilder builder = new AgentBuilder.Default()
                .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
                .with(AgentBuilder.TypeStrategy.Default.REDEFINE)
                // 织入错误必须走 SLF4J 统一日志体系：StreamWriting.toSystemOut() 会让错误
                // 裸写标准输出流，绕过应用日志框架（生产排查不可见、污染 JMH/stdout 采集）。
                // 语义与 withErrorsOnly() 等价——只监听 onError，成功事件不刷屏。
                .with(new AgentBuilder.Listener.Adapter() {
                    @Override
                    public void onError(String typeName, ClassLoader classLoader, JavaModule module,
                                        boolean loaded, Throwable throwable) {
                        log.error("ByteBuddy weaving FAILED for type [{}] (loader={}, loaded={}) — "
                                + "governance/cleanup advice on this type is INACTIVE: {}",
                                typeName, classLoader, loaded, throwable.getMessage(), throwable);
                    }
                })
                .ignore(ElementMatchers.nameStartsWith("net.bytebuddy.")
                        .or(ElementMatchers.nameStartsWith("com.lingframe.agent."))
                        .or(ElementMatchers.isSynthetic()));

        if (config.isEffectiveTaskExecutionAdviceEnabled()) {
            log.warn("TaskExecutionAdvice ENABLED — intercepting AbstractTask.call() for batch-level governance. " +
                    "This intercepts the Worker scheduling loop at millisecond granularity. " +
                    "Potential risks: (1) throughput degradation in high-QPS scenarios, " +
                    "(2) checkpoint timeout if backpressure is too aggressive. " +
                    "Monitor these metrics after enabling.");
            final boolean targetPresent = checkTargetClassPresent(ABSTRACT_TASK_TYPE, "AbstractTask");
            builder = builder
                    .type(ElementMatchers.hasSuperType(ElementMatchers.named(ABSTRACT_TASK_TYPE))
                            .and(ElementMatchers.not(ElementMatchers.isAbstract())))
                    .transform(new AgentBuilder.Transformer.ForAdvice()
                            .include(TaskExecutionAdvice.class.getClassLoader())
                            .advice(ElementMatchers.named("call"),
                                    TaskExecutionAdvice.class.getName()));
            adviceStatus.put("AbstractTask", targetPresent ? "INSTALLED" : "ABSENT");
            log.info("TaskExecutionAdvice installed for AbstractTask.call() interception (targetClass={})",
                    targetPresent ? "PRESENT" : "ABSENT");
        } else {
            adviceStatus.put("AbstractTask", "DISABLED");
            log.info("TaskExecutionAdvice DISABLED — no governance feature (circuit-breaker/rate-limiter/" +
                    "gray-routing/permission) nor task-execution-advice-enabled is enabled, " +
                    "Agent runs in ClassLoader-cleanup-only mode. " +
                    "To enable batch-level governance, enable a resilience feature or set " +
                    "governance.task-execution-advice-enabled: true in lingframe-governance.yaml");
        }

        if (validateClassLoaderServiceFields()) {
            adviceStatus.put("DefaultClassLoaderService", "INSTALLED");
            builder = builder
                    .type(ElementMatchers.named(CLASSLOADER_SERVICE))
                    .transform(new AgentBuilder.Transformer.ForAdvice()
                            .include(ClassLoaderReleaseAdvice.class.getClassLoader())
                            .advice(ElementMatchers.isConstructor(),
                                    ClassLoaderServiceCacheAdvice.class.getName())
                            .advice(ElementMatchers.named("releaseClassLoader")
                                    .and(ElementMatchers.takesArgument(0, ElementMatchers.named("long"))),
                                    ClassLoaderReleaseAdvice.class.getName())
                            .advice(ElementMatchers.named("getClassLoader")
                                    .and(ElementMatchers.takesArgument(0, ElementMatchers.named("long"))),
                                    ClassLoaderServiceGetAdvice.class.getName()));
            log.info("ClassLoaderReleaseAdvice, ClassLoaderServiceGetAdvice & ClassLoaderServiceCacheAdvice "
                    + "installed for DefaultClassLoaderService");
        } else {
            adviceStatus.put("DefaultClassLoaderService", "SKIPPED");
            log.warn("ClassLoaderReleaseAdvice SKIPPED — target fields missing, ClassLoader leak detection disabled");
        }

        builder = builder
                .type(ElementMatchers.named(JOB_MASTER_TYPE))
                .transform(new AgentBuilder.Transformer.ForAdvice()
                        .include(JobMasterCleanJobAdvice.class.getClassLoader())
                        .advice(ElementMatchers.named("cleanJob")
                                        .or(ElementMatchers.named("run")),
                                JobMasterCleanJobAdvice.class.getName()));
        adviceStatus.put("JobMaster", "INSTALLED");
        log.info("JobMasterCleanJobAdvice installed for cleanJob & run interception (Coordinator ClassLoader evict)");

        builder = installTcclGuardAdvice(builder, adviceStatus);
        builder.installOn(inst);
    }

    /**
     * 启动期目标类存在性校验（防上游 SeaTunnel 重构后静默失效）。
     * <p>
     * ByteBuddy 用类名字符串匹配织入，若上游重构类名，ByteBuddy 静默不织入且不报错——
     * agent 表面正常但治理/清理实际未生效。此方法在 premain 期对目标类做 {@code Class.forName}
     * 探测，缺失时打 ERROR 日志明确告警，便于运维核查 advice 状态（MBean 同步暴露）。
     *
     * @return true 目标类存在于宿主 classpath
     */
    private static boolean checkTargetClassPresent(String className, String shortName) {
        try {
            Class.forName(className, false, ClassLoader.getSystemClassLoader());
            return true;
        } catch (ClassNotFoundException e) {
            log.error("Target class [{}] not found on classpath — advice for [{}] will be SILENTLY INACTIVE. " +
                    "SeaTunnel version may have restructured/renamed this class. " +
                    "Check MBean com.lingframe.agent:type=Observability for advice status.", className, shortName);
            return false;
        }
    }

    /**
     * 注册可观测性 MBean 到平台 MBeanServer。
     * <p>
     * 注册失败不阻断 agent 启动（warn 降级），符合 premain fail-open 原则。
     */
    private static void registerObservabilityMBean(AgentConfig config,
                                                   Map<String, String> adviceStatus,
                                                   AgentGovernanceRuntime runtime,
                                                   Object adapter) {
        try {
            final LingFrameAgentObservability mbean = new LingFrameAgentObservability(
                    config, adviceStatus,
                    runtime != null ? runtime.getEventBus() : null,
                    adapter);
            final ObjectName name = new ObjectName("com.lingframe.agent:type=Observability");
            ManagementFactory.getPlatformMBeanServer().registerMBean(mbean, name);
            log.info("LingFrame Agent observability MBean registered: {}", name);
        } catch (Throwable t) {
            log.warn("Failed to register observability MBean (agent continues without JMX exposure)", t);
        }
    }

    /**
     * 织入 TCCL 拘留防御切面。
     * <p>
     * 拦截 {@code Thread.setContextClassLoader}，在设置前检查目标 ClassLoader
     * 是否已释放，如果是则替换为 parent。防止线程池复用时残留已释放 ClassLoader 为 TCCL。
     * <p>
     * Thread 是 JDK 核心类，retransform 由 {@link AgentBuilder#installOn} 的
     * RETRANSFORMATION 策略在安装期批量执行——此处的 try-catch 只能捕获注册阶段
     * 异常；retransform 阶段的失败由 AgentBuilder Listener 记录（withErrorsOnly），
     * 失败时 TCCL 防御自然退化为仅 onPhysicalRelease 时复位。
     *
     * @param builder 当前 builder
     * @return 携带 TCCL 切面配置的新 builder（不可变 API，必须接收返回值）
     */
    private static AgentBuilder installTcclGuardAdvice(AgentBuilder builder,
                                                       Map<String, String> adviceStatus) {
        try {
            final AgentBuilder withGuard = builder
                    .type(ElementMatchers.named("java.lang.Thread"))
                    .transform(new AgentBuilder.Transformer.ForAdvice()
                            .include(TcclGuardAdvice.class.getClassLoader())
                            .advice(ElementMatchers.named("setContextClassLoader")
                                    .and(ElementMatchers.takesArgument(0, ClassLoader.class)),
                                    TcclGuardAdvice.class.getName()));
            adviceStatus.put("TcclGuard", "INSTALLED");
            return withGuard;
        } catch (Exception e) {
            adviceStatus.put("TcclGuard", "FAILED");
            log.warn("TcclGuardAdvice registration FAILED, TCCL guard degraded to onPhysicalRelease-only mode",
                    e);
            return builder;
        }
    }

    private LingFrameAgentActivationRunner() {
    }
}
