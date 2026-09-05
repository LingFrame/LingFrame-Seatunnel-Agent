package com.lingframe.agent;

import com.lingframe.agent.adapter.SeaTunnelAdapter;
import com.lingframe.agent.bridge.LingFrameAgentBridge;
import com.lingframe.agent.config.AgentConfig;
import com.lingframe.agent.advice.ClassLoaderReleaseAdvice;
import com.lingframe.agent.advice.TaskExecutionAdvice;
import com.lingframe.agent.advice.TcclGuardAdvice;
import com.lingframe.agent.pipeline.AgentGovernanceRuntime;
import com.lingframe.agent.pipeline.AgentPipelineFactory;
import net.bytebuddy.agent.builder.AgentBuilder;

import net.bytebuddy.matcher.ElementMatchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.net.URL;
import java.security.CodeSource;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;

/**
 * Java Agent premain 入口。
 * <p>
 * 启动流程：
 * <ol>
 *   <li>加载治理配置（Agent 参数 > SEATUNNEL_HOME/config > 默认值）</li>
 *   <li>初始化 LingFrame 治理微内核（Pipeline + Filter 链）</li>
 *   <li>将微内核契约注册到 Bootstrap Bridge</li>
 *   <li>通过 ByteBuddy AgentBuilder 织入 Advice 拦截器</li>
 * </ol>
 * <p>
 * 防御性降级：若目标类签名不匹配，ByteBuddy 自动跳过织入，Bridge 降级为 no-op。
 */
public final class LingFrameAgentPremain {

    private static final Logger log = LoggerFactory.getLogger(LingFrameAgentPremain.class);

    private static final String TASK_EXEC_SERVICE =
            "org.apache.seatunnel.engine.server.TaskExecutionService";
    private static final String CLASSLOADER_SERVICE =
            "org.apache.seatunnel.engine.core.classloader.DefaultClassLoaderService";
    private static final String ABSTRACT_TASK_TYPE =
            "org.apache.seatunnel.engine.server.task.AbstractTask";

    /** Bridge 契约类在 Fat-Jar 中的包路径前缀，只提取这些类注入 Bootstrap。 */
    private static final String BRIDGE_PACKAGE_PREFIX = "com/lingframe/agent/bridge/";

    public static void premain(String agentArgs, Instrumentation inst) {
        log.info("LingFrame SeaTunnel Agent starting...");

        final AgentConfig config = AgentConfig.load(agentArgs);
        if (!config.isGovernanceEnabled()) {
            log.info("Governance is disabled by config, Agent runs in PASS_THROUGH mode");
            return;
        }

        // 1. 先将 Bridge JAR 注入 Bootstrap ClassLoader——必须在创建 SeaTunnelAdapter 之前，
        //    否则 LingGovernanceContract 会被 AppClassLoader 先加载，与 Bootstrap 版本不一致，
        //    导致 registerContract 时类型不匹配（两份不同的类）
        appendToBootstrap(inst);

        // 2. 初始化治理微内核与适配器（此时 LingGovernanceContract 通过双亲委派由 Bootstrap 加载）
        final AgentGovernanceRuntime governanceRuntime = initGovernanceRuntime(config);
        final SeaTunnelAdapter adapter = new SeaTunnelAdapter(
                config,
                governanceRuntime != null ? governanceRuntime.getPipelineEngine() : null,
                governanceRuntime != null ? governanceRuntime.getUnloadCoordinator() : null,
                governanceRuntime != null ? governanceRuntime.getConfigCenter() : null,
                governanceRuntime != null ? governanceRuntime.getLingRepository() : null,
                governanceRuntime != null ? governanceRuntime.getEventBus() : null,
                governanceRuntime != null ? governanceRuntime.getMetricsCollector() : null);

        // 3. 注册治理契约到 Bridge
        LingFrameAgentBridge.registerContract(adapter);
        // 4. 织入 ByteBuddy 拦截器（含目标字段存在性检测）
        installByteBuddyAdvice(inst, config);
        log.info("LingFrame SeaTunnel Agent activated successfully");
    }

    private static AgentGovernanceRuntime initGovernanceRuntime(AgentConfig config) {
        try {
            return AgentPipelineFactory.create(config);
        } catch (Exception e) {
            log.warn("Failed to initialize governance pipeline, falling back to TRACE_ONLY mode: {}", e.getMessage());
            return null;
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

    private static void installByteBuddyAdvice(Instrumentation inst, AgentConfig config) {
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
                .with(AgentBuilder.Listener.StreamWriting.toSystemOut().withErrorsOnly())
                .ignore(ElementMatchers.nameStartsWith("net.bytebuddy.")
                        .or(ElementMatchers.nameStartsWith("com.lingframe.agent."))
                        .or(ElementMatchers.isSynthetic()));

        if (config.isTaskExecutionAdviceEnabled()) {
            log.warn("TaskExecutionAdvice ENABLED — intercepting AbstractTask.call() for batch-level governance. " +
                    "This intercepts the Worker scheduling loop at millisecond granularity. " +
                    "Potential risks: (1) throughput degradation in high-QPS scenarios, " +
                    "(2) checkpoint timeout if backpressure is too aggressive. " +
                    "Monitor these metrics after enabling.");
            builder = builder
                    .type(ElementMatchers.hasSuperType(ElementMatchers.named(ABSTRACT_TASK_TYPE))
                            .and(ElementMatchers.not(ElementMatchers.isAbstract())))
                    .transform(new AgentBuilder.Transformer.ForAdvice()
                            .include(TaskExecutionAdvice.class.getClassLoader())
                            .advice(ElementMatchers.named("call"),
                                    TaskExecutionAdvice.class.getName()));
            log.info("TaskExecutionAdvice installed for AbstractTask.call() interception");
        } else {
            log.info("TaskExecutionAdvice DISABLED (default) — Agent runs in ClassLoader-cleanup-only mode. " +
                    "To enable batch-level governance, set governance.task-execution-advice-enabled: true " +
                    "in lingframe-governance.yaml");
        }

        if (validateClassLoaderServiceFields()) {
            builder = builder
                    .type(ElementMatchers.named(CLASSLOADER_SERVICE))
                    .transform(new AgentBuilder.Transformer.ForAdvice()
                            .include(ClassLoaderReleaseAdvice.class.getClassLoader())
                            .advice(ElementMatchers.named("releaseClassLoader")
                                    .and(ElementMatchers.takesArgument(0, ElementMatchers.named("long"))),
                                    ClassLoaderReleaseAdvice.class.getName()));
            log.info("ClassLoaderReleaseAdvice installed for releaseClassLoader interception");
        } else {
            log.warn("ClassLoaderReleaseAdvice SKIPPED — target fields missing, ClassLoader leak detection disabled");
        }

        builder = installTcclGuardAdvice(builder);
        builder.installOn(inst);
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
    private static AgentBuilder installTcclGuardAdvice(AgentBuilder builder) {
        try {
            return builder
                    .type(ElementMatchers.named("java.lang.Thread"))
                    .transform(new AgentBuilder.Transformer.ForAdvice()
                            .include(TcclGuardAdvice.class.getClassLoader())
                            .advice(ElementMatchers.named("setContextClassLoader")
                                    .and(ElementMatchers.takesArgument(0, ClassLoader.class)),
                                    TcclGuardAdvice.class.getName()));
        } catch (Exception e) {
            log.warn("TcclGuardAdvice registration FAILED, TCCL guard degraded to onPhysicalRelease-only mode: {}",
                    e.getMessage());
            return builder;
        }
    }

    /**
     * 从 Agent Fat-Jar 中提取极薄 Bridge 契约类（&lt;20KB），打包为临时 JAR 注入 Bootstrap ClassLoader。
     * <p>
     * 三层拓扑要求 Bootstrap 只包含 Bridge 契约（3 个类），而非整个 Fat-Jar（含 ByteBuddy、
     * LingFrame Core、SLF4J 等依赖）。这样避免污染 JVM 顶层命名空间、防止与目标应用同名类冲突、
     * 且 Bridge 类虽不可卸载但体积极小可忽略。
     * <p>
     * 提取范围：Fat-Jar 中 {@code com/lingframe/agent/bridge/} 路径下的所有 .class 文件。
     *
     * @param inst JVM Instrumentation 实例
     */
    private static void appendToBootstrap(Instrumentation inst) {
        try {
            final CodeSource codeSource = LingFrameAgentPremain.class.getProtectionDomain().getCodeSource();
            if (codeSource == null) {
                log.warn("Cannot determine Agent JAR location, skipping Bootstrap ClassLoader injection");
                return;
            }
            final URL jarUrl = codeSource.getLocation();
            if (jarUrl == null || !"file".equals(jarUrl.getProtocol())) {
                log.warn("Agent JAR URL is not a file URL, skipping Bootstrap injection: {}", jarUrl);
                return;
            }
            final File fatJar = new File(jarUrl.toURI());
            if (!fatJar.isFile()) {
                log.warn("Agent JAR is not a regular file, skipping Bootstrap injection: {}", fatJar);
                return;
            }
            final File bridgeJar = extractBridgeJar(fatJar);
            if (bridgeJar == null) {
                log.warn("No Bridge classes extracted from {}, skipping Bootstrap injection", fatJar.getAbsolutePath());
                return;
            }
            inst.appendToBootstrapClassLoaderSearch(new JarFile(bridgeJar));
            log.info("Bridge JAR ({} bytes) appended to Bootstrap ClassLoader search: {}",
                    bridgeJar.length(), bridgeJar.getAbsolutePath());
        } catch (Exception e) {
            log.warn("Failed to append Bridge JAR to Bootstrap ClassLoader: {}", e.getMessage());
        }
    }

    /**
     * 从 Fat-Jar 中提取 Bridge 契约类到临时 JAR 文件。
     *
     * @param fatJar Agent Fat-JAR
     * @return 包含 Bridge 类的临时 JAR，若无 Bridge 类则返回 null
     */
    private static File extractBridgeJar(File fatJar) throws Exception {
        final File tempJar = File.createTempFile("lingframe-agent-bridge-", ".jar");
        boolean hasBridgeClass = false;
        try (JarFile jarFile = new JarFile(fatJar);
             JarOutputStream jos = new JarOutputStream(new FileOutputStream(tempJar))) {
            final Enumeration<JarEntry> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                final JarEntry entry = entries.nextElement();
                final String name = entry.getName();
                if (!name.startsWith(BRIDGE_PACKAGE_PREFIX) || !name.endsWith(".class")) {
                    continue;
                }
                jos.putNextEntry(new JarEntry(name));
                try (InputStream is = jarFile.getInputStream(entry)) {
                    final byte[] buffer = new byte[4096];
                    int len;
                    while ((len = is.read(buffer)) != -1) {
                        jos.write(buffer, 0, len);
                    }
                }
                jos.closeEntry();
                hasBridgeClass = true;
                log.debug("Extracted Bridge class from Fat-Jar: {}", name);
            }
        }
        if (!hasBridgeClass) {
            if (!tempJar.delete()) {
                log.debug("Failed to delete empty temp JAR: {}", tempJar.getAbsolutePath());
            }
            return null;
        }
        tempJar.deleteOnExit();
        return tempJar;
    }

    private LingFrameAgentPremain() {
    }
}
