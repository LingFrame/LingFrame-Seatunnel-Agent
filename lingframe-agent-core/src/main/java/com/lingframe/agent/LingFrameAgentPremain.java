package com.lingframe.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.lang.instrument.Instrumentation;
import java.net.URL;
import java.security.CodeSource;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;

/**
 * Java Agent premain 入口（纯引导类）。
 * <p>
 * 职责边界（双副本根因决定的瘦身结构，见 {@link LingFrameAgentActivationRunner}）：
 * <ol>
 *   <li>把极薄 Bridge 契约（{@code com/lingframe/agent/bridge/} 包）注入 Bootstrap ClassLoader；</li>
 *   <li>以反射方式委托 {@link LingFrameAgentActivationRunner#activate} 完成治理装配。</li>
 * </ol>
 * 本类字节码<b>不得直接引用</b> core/adapter/bridge 任何类型——JVM 反射调用 premain 前
 * 需 link 本类，link 的类型检查若解析到 {@code SeaTunnelAdapter} 等符号，会在 Bridge
 * 契约注入 Bootstrap 之前就由 AppClassLoader 抢载成 App 副本，运行期与 Bootstrap 副本
 * 双份并存导致 {@code registerContract} instanceof 失配、治理整体 fail-open（JMH governed
 * fork 实测复现）。反射委托把这类强解析全部延迟到 Bridge 注入之后。
 * <p>
 * 防御性降级：若目标类签名不匹配，ByteBuddy 自动跳过织入，Bridge 降级为 no-op。
 */
public final class LingFrameAgentPremain {

    private static final Logger log = LoggerFactory.getLogger(LingFrameAgentPremain.class);

    /** 治理激活运行器类名（仅字符串常量，避免字节码强引用触发 link 期抢载）。 */
    private static final String ACTIVATION_RUNNER_CLASS =
            "com.lingframe.agent.LingFrameAgentActivationRunner";

    /** Bridge 契约类在 Fat-Jar 中的包路径前缀，只提取这些类注入 Bootstrap。 */
    private static final String BRIDGE_PACKAGE_PREFIX = "com/lingframe/agent/bridge/";

    public static void premain(String agentArgs, Instrumentation inst) {
        // 顺序铁律：必须先注入 Bootstrap Bridge 契约，再加载任何 core/adapter 类。
        // 若颠倒（或直接强引用激活类），link 期就会把 LingGovernanceContract 抢载成
        // App 副本，与 Bootstrap 副本双份并存，registerContract instanceof 失配，
        // 治理 fail-open（JMH governed fork 实测复现，详见 LingFrameAgentActivationRunner）。
        try {
            appendToBootstrap(inst);
            final Class<?> runnerClass = Class.forName(ACTIVATION_RUNNER_CLASS, true,
                    ClassLoader.getSystemClassLoader());
            runnerClass.getMethod("activate", String.class, Instrumentation.class)
                    .invoke(null, agentArgs, inst);
        } catch (Throwable t) {
            // 生产级 fail-open 铁律：Agent 启动的任何未预期异常都不得拖垮宿主 JVM。
            // 记录完整 ERROR 后以「零织入/零治理」继续（宿主引擎正常运行，功能降级，
            // 而非 premain 抛异常导致 JVM 启动失败 FATAL）。
            log.error("LingFrame SeaTunnel Agent activation FAILED — continuing WITHOUT agent instrumentation. "
                    + "Governance/ClassLoader-cleanup disabled for this JVM. Cause: {}",
                    t.getMessage(), t);
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
            log.warn("Failed to append Bridge JAR to Bootstrap ClassLoader", e);
        }
    }

    /**
     * 从 Fat-Jar 中提取 Bridge 契约类到临时 JAR 文件。
     *
     * @param fatJar Agent Fat-JAR
     * @return 包含 Bridge 类的临时 JAR，若无 Bridge 类则返回 null
     */
    private static File extractBridgeJar(File fatJar) throws Exception {
        cleanupStaleTempJars();
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

    /** 清理上次 JVM crash 残留的临时 Bridge JAR（deleteOnExit 仅在正常退出时生效）。 */
    private static void cleanupStaleTempJars() {
        try {
            final File tempDir = new File(System.getProperty("java.io.tmpdir"));
            final File[] stale = tempDir.listFiles((dir, name) ->
                    name.startsWith("lingframe-agent-bridge-") && name.endsWith(".jar"));
            if (stale != null) {
                for (File f : stale) {
                    if (f.delete()) {
                        log.debug("Cleaned up stale temp JAR: {}", f.getAbsolutePath());
                    }
                }
            }
        } catch (Throwable t) {
            // best-effort，不阻塞 Agent 启动
        }
    }

    private LingFrameAgentPremain() {
    }
}
