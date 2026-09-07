package com.lingframe.agent.e2e;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Agent 分发产物（Fat-Jar）本地可跑的打包守卫。
 * <p>
 * 本测试不依赖 Docker / SeaTunnel 集群，本地 {@code mvn test} 即可执行。-javaagent 能否成功
 * 挂载，首先取决于 jar 的 Manifest 与内容是否正确；这里守住三类致命回归：
 * <ol>
 *   <li>Manifest 缺失 Premain-Class / Agent-Class / Can-* 标志 → JVM 拒绝挂载 agent，整组件静默失效；</li>
 *   <li>bridge 契约类未打进 jar → premain 注入 Bootstrap 时 ClassNotFound；</li>
 *   <li>ByteBuddy / SnakeYAML 未正确 Relocate（仍含 {@code net/bytebuddy} 等原包）→ 与 SeaTunnel 自带版本冲突。</li>
 * </ol>
 * 若 dist 模块尚未构建（jar 不存在），优雅跳过而非失败，便于仅编译的场景下运行。
 */
@DisplayName("Agent Fat-Jar 打包守卫（本地可跑，无需集群）")
class AgentJarManifestTest {

    private static final String EXPECTED_PREMAIN = "com.lingframe.agent.LingFrameAgentPremain";

    private File resolveAgentJar() {
        final String userDir = System.getProperty("user.dir");
        return new File(userDir, "../lingframe-agent-dist/target/lingframe-seatunnel-agent.jar");
    }

    @Test
    @DisplayName("Manifest 必须声明正确的 Premain-Class / Agent-Class 与 Can-* 重转换标志")
    void manifestShouldDeclareValidAgentEntrypoints() throws IOException {
        final File jar = resolveAgentJar();
        Assumptions.assumeTrue(jar.isFile(),
                "lingframe-agent-dist 未构建，跳过打包守卫（先执行 mvn package -pl lingframe-agent-dist）");

        try (JarFile jarFile = new JarFile(jar)) {
            final Manifest manifest = jarFile.getManifest();
            assertThat(manifest).as("jar 必须包含 MANIFEST.MF").isNotNull();

            final Attributes main = manifest.getMainAttributes();
            assertThat(main.getValue("Premain-Class"))
                    .as("Premain-Class 决定 -javaagent 挂载入口").isEqualTo(EXPECTED_PREMAIN);
            assertThat(main.getValue("Agent-Class"))
                    .as("Agent-Class 支持运行时 attach").isEqualTo(EXPECTED_PREMAIN);
            assertThat(main.getValue("Can-Redefine-Classes"))
                    .as("需允许类重定义").isEqualToIgnoringCase("true");
            assertThat(main.getValue("Can-Retransform-Classes"))
                    .as("需允许类重转换").isEqualToIgnoringCase("true");
            assertThat(main.getValue("Can-Set-Native-Method-Prefix"))
                    .as("需允许设置 native 方法前缀").isEqualToIgnoringCase("true");
        }
    }

    @Test
    @DisplayName("bridge 契约类必须被打进 jar（Bootstrap 注入依赖）")
    void bridgeClassesMustBePresentInJar() throws IOException {
        final File jar = resolveAgentJar();
        Assumptions.assumeTrue(jar.isFile(),
                "lingframe-agent-dist 未构建，跳过打包守卫（先执行 mvn package -pl lingframe-agent-dist）");

        try (JarFile jarFile = new JarFile(jar)) {
            assertThat(jarFile.getEntry("com/lingframe/agent/bridge/LingFrameAgentBridge.class"))
                    .as("LingFrameAgentBridge 须随 Bootstrap 注入").isNotNull();
            assertThat(jarFile.getEntry("com/lingframe/agent/bridge/LingGovernanceContract.class"))
                    .as("LingGovernanceContract 须随 Bootstrap 注入").isNotNull();
            assertThat(jarFile.getEntry("com/lingframe/agent/bridge/ReleasedClassLoaderRegistry.class"))
                    .as("ReleasedClassLoaderRegistry 须随 Bootstrap 注入").isNotNull();
        }
    }

    @Test
    @DisplayName("ByteBuddy / SnakeYAML 必须已 Relocate，避免与 SeaTunnel 冲突")
    void thirdPartyClassesMustBeRelocated() throws IOException {
        final File jar = resolveAgentJar();
        Assumptions.assumeTrue(jar.isFile(),
                "lingframe-agent-dist 未构建，跳过打包守卫（先执行 mvn package -pl lingframe-agent-dist）");

        boolean foundRelocatedByteBuddy = false;
        boolean foundUnrelocatedByteBuddy = false;
        boolean foundUnrelocatedSnakeyaml = false;

        try (JarFile jarFile = new JarFile(jar)) {
            final Enumeration<?> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                final String name = entries.nextElement().toString();
                if (name.startsWith("com/lingframe/agent/shaded/bytebuddy/")) {
                    foundRelocatedByteBuddy = true;
                } else if (name.startsWith("net/bytebuddy/")) {
                    foundUnrelocatedByteBuddy = true;
                } else if (name.startsWith("org/yaml/snakeyaml/")) {
                    foundUnrelocatedSnakeyaml = true;
                }
            }
        }

        assertThat(foundRelocatedByteBuddy)
                .as("ByteBuddy 应已 Relocate 到 com.lingframe.agent.shaded.bytebuddy").isTrue();
        assertThat(foundUnrelocatedByteBuddy)
                .as("net.bytebuddy 仍原位说明 Relocate 失效，会与 SeaTunnel 冲突").isFalse();
        assertThat(foundUnrelocatedSnakeyaml)
                .as("org.yaml.snakeyaml 仍原位说明 Relocate 失效，会与 SeaTunnel 冲突").isFalse();
    }

    @Test
    @DisplayName("premain 字节码不得直接引用 core/bridge 类型（双副本 fail-open 回归守卫）")
    void premainClassMustNotReferenceContractType() throws IOException {
        final File jar = resolveAgentJar();
        Assumptions.assumeTrue(jar.isFile(),
                "lingframe-agent-dist 未构建，跳过打包守卫（先执行 mvn package -pl lingframe-agent-dist）");

        final String premainText;
        final String runnerText;
        try (JarFile jarFile = new JarFile(jar)) {
            premainText = readClassText(jarFile, "com/lingframe/agent/LingFrameAgentPremain.class");
            runnerText = readClassText(jarFile, "com/lingframe/agent/LingFrameAgentActivationRunner.class");
        }

        // 根因（JMH governed fork 实测）：-javaagent 会把 Agent Fat-Jar 追加进 system classpath，
        // JVM 反射调用 premain() 前须 link 入口类，若入口类字节码直接引用 SeaTunnelAdapter，
        // link 的类型检查就在 appendToBootstrap 尚未执行时把 LingGovernanceContract 从
        // AppClassLoader 抢载成 App 副本，运行期与 Bootstrap 副本双份并存，
        // registerContract 的 instanceof 失配 → 治理整体 fail-open。
        // 修复：激活逻辑整体下沉到 LingFrameAgentActivationRunner，premain 仅保留
        // Bootstrap 注入 + 反射委托（类名走字符串常量），link 期不再解析任何 core/bridge 符号。
        // 此处断言「premain 字节码不含契约接口与适配器类名」即锁死该回归路径。
        assertThat(premainText)
                .as("premain 字节码不得含 LingGovernanceContract 常量池/调试属性引用——"
                        + "否则 App 侧抢载路径复活，双副本失配回归")
                .doesNotContain("LingGovernanceContract");
        assertThat(premainText)
                .as("premain 字节码不得直接引用 SeaTunnelAdapter——link 期抢载契约接口的源头")
                .doesNotContain("SeaTunnelAdapter");
        // 反向守卫：激活委托与契约注册必须仍在（防止「删掉注册/激活」式的假修复）
        assertThat(premainText)
                .as("premain 必须保留到 ActivationRunner 的反射委托（纵深防御）")
                .contains("LingFrameAgentActivationRunner");
        assertThat(premainText)
                .as("premain 必须保留 activate 反射方法名委托")
                .contains("activate");
        assertThat(runnerText)
                .as("runner 必须保留 registerContract 调用点（契约注册不能被静默删除）")
                .contains("registerContract");
    }

    /**
     * 从 Fat-Jar 读取指定类的原始字节，转为 ISO-8859-1 文本用于字节级断言。
     */
    private String readClassText(JarFile jarFile, String entryName) throws IOException {
        final JarEntry entry = jarFile.getJarEntry(entryName);
        assertThat(entry).as("Fat-Jar 必须包含 %s", entryName).isNotNull();
        try (InputStream is = jarFile.getInputStream(entry);
             ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            final byte[] buffer = new byte[4096];
            int len;
            while ((len = is.read(buffer)) != -1) {
                bos.write(buffer, 0, len);
            }
            return new String(bos.toByteArray(), StandardCharsets.ISO_8859_1);
        }
    }
}
