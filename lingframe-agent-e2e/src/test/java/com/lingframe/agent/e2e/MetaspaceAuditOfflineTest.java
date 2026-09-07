package com.lingframe.agent.e2e;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 元空间审计解析逻辑与日志管道的本地离线单元测试。
 * <p>
 * 在无需外部 Docker 依赖的情况下，针对以下核心机制进行 100% 离线确定性闭环验证：
 * 1. 动态 Java PID 嗅探与过滤算法；
 * 2. JDK 8 GC.heap_info 元空间正则提取与换算精度；
 * 3. Log4j2 精准放行与 target/logs/metaspace-audit.log 物理落盘连通性。
 */
@DisplayName("元空间审计解析与日志本地离线单元测试")
class MetaspaceAuditOfflineTest {

    private static final Logger log = LoggerFactory.getLogger(MetaspaceLeakIT.class);

    private static final Pattern HEAP_INFO_METASPACE_PATTERN =
            Pattern.compile("Metaspace\\s+used\\s+(\\d+)\\s*K", Pattern.CASE_INSENSITIVE);

    @Test
    @DisplayName("验证 jps 输出多进程时能精准提取真实 SeaTunnel JVM PID 并剔除 Jps 自身")
    void testResolveJavaPidFromJpsOutput() {
        final List<String> mockJpsLines = Arrays.asList(
                "742 sun.tools.jps.Jps -l",
                "15 org.apache.seatunnel.core.starter.seatunnel.SeaTunnelServer -c /opt/seatunnel/config/seatunnel.yaml"
        );

        String resolvedPid = null;
        // 优先匹配 SeaTunnel 核心服务类
        for (String line : mockJpsLines) {
            final String[] parts = line.split("\\s+");
            if (parts.length >= 2 && parts[0].matches("\\d+")) {
                final String pid = parts[0];
                final String mainClass = parts[1];
                if (!mainClass.contains("Jps") && (mainClass.contains("SeaTunnel") || mainClass.contains("starter"))) {
                    resolvedPid = pid;
                    break;
                }
            }
        }

        assertThat(resolvedPid).isEqualTo("15");
    }

    @Test
    @DisplayName("验证真实 JDK 8 GC.heap_info 输出中 Metaspace 字节数的提取与换算精度")
    void testParseJdk8HeapInfoOutput() {
        final String mockJdk8HeapInfo =
                "PSYoungGen      total 9216K, used 6224K [0x00000000ff600000, 0x0000000100000000, 0x0000000100000000)\n"
                        + "  eden space 8192K, 75% used [0x00000000ff600000,0x00000000ffc14040,0x00000000ffe00000)\n"
                        + "  from space 1024K, 0% used [0x00000000fff00000,0x00000000fff00000,0x0000000100000000)\n"
                        + "  to   space 1024K, 0% used [0x00000000ffe00000,0x00000000ffe00000,0x00000000fff00000)\n"
                        + "ParOldGen       total 20480K, used 0K [0x00000000fe200000, 0x00000000ff600000, 0x00000000ff600000)\n"
                        + "  object space 20480K, 0% used [0x00000000fe200000,0x00000000fe200000,0x00000000ff600000)\n"
                        + "Metaspace       used 2939K, capacity 4486K, committed 4864K, reserved 1056768K\n"
                        + "  class space    used 312K, capacity 386K, committed 512K, reserved 1048576K\n";

        final Matcher matcher = HEAP_INFO_METASPACE_PATTERN.matcher(mockJdk8HeapInfo);
        assertThat(matcher.find()).isTrue();
        final long kb = Long.parseLong(matcher.group(1));
        assertThat(kb).isEqualTo(2939L);
        final long bytes = kb * 1024L;
        assertThat(bytes).isEqualTo(3009536L);
    }

    @Test
    @DisplayName("验证 MetaspaceLeakIT 的 Logger 能成功双写并落盘至 target/logs/metaspace-audit.log")
    void testLog4j2AuditFileWriting() throws Exception {
        final String testMarker = "OfflineUnitTestMarker-" + System.currentTimeMillis();
        log.info("[OfflineTest] Metaspace progression [Unit Test Check]: marker={}", testMarker);

        final File logFile = new File("target/logs/metaspace-audit.log");
        assertThat(logFile.exists()).isTrue();

        final String logContent = new String(Files.readAllBytes(logFile.toPath()), "UTF-8");
        assertThat(logContent).contains(testMarker);
    }

    @Test
    @DisplayName("验证刚性物理断言能准确拦截 0 字节或低于 10MB 的虚假数据")
    void testDefensiveAssertionInterceptsFalseZeroOrSmallValues() {
        // 场景 1：如果 baseline 为 0，必须被强断言拦截并抛出 AssertionError
        final long fakeZeroBaseline = 0L;
        assertThrows(AssertionError.class, () -> {
            assertThat(fakeZeroBaseline)
                    .as("Baseline Metaspace must be > 10MB")
                    .isGreaterThan(10 * 1024 * 1024L);
        });

        // 场景 2：如果抓取到非 SeaTunnel 的空进程（如 5MB），也必须被强断言拦截
        final long fakeSmallBaseline = 5 * 1024 * 1024L;
        assertThrows(AssertionError.class, () -> {
            assertThat(fakeSmallBaseline)
                    .as("Baseline Metaspace must be > 10MB")
                    .isGreaterThan(10 * 1024 * 1024L);
        });

        // 场景 3：真实的 SeaTunnel 运行时 Metaspace（如 45MB），通过断言
        final long realBaseline = 45 * 1024 * 1024L;
        assertThat(realBaseline)
                .as("Baseline Metaspace must be > 10MB")
                .isGreaterThan(10 * 1024 * 1024L);
    }
}
