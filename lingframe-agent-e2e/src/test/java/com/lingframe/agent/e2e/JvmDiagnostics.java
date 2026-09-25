package com.lingframe.agent.e2e;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/** JVM and Docker diagnostics used by the Metaspace E2E audit. */
final class JvmDiagnostics {
    private static final Logger log = LoggerFactory.getLogger(JvmDiagnostics.class);

    private JvmDiagnostics() {
    }

    static boolean isDockerContainerRunning(String containerName) {
        try {
            final Process p = new ProcessBuilder("docker", "inspect", "-f", "{{.State.Running}}", containerName)
                    .redirectErrorStream(true)
                    .start();
            final StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line);
                }
            }
            return p.waitFor() == 0 && Boolean.parseBoolean(output.toString().trim());
        } catch (Exception e) {
            return false;
        }
    }

    static ClassLoaderStatsResult captureClassLoaderStats(String containerName, String label) {
        try {
            final String pid = resolveJavaPid(containerName);
            final ProcessBuilder pb = new ProcessBuilder(
                    "docker", "exec", containerName, "jmap", "-clstats", pid);
            final Process p = pb.start();
            // 分离 stdout 和 stderr：jmap -clstats 的 stderr（如 "liveness analysis
            // may be inaccurate"）会交错插入 stdout 数据行，导致 parseLong 失败、
            // bootstrap 行被跳过。用 daemon 线程排空 stderr 防止管道死锁，只解析 stdout。
            final List<String> stderrLines = new ArrayList<>();
            final Thread stderrDrain = new Thread(() -> {
                try (BufferedReader errReader = new BufferedReader(
                        new InputStreamReader(p.getErrorStream(), StandardCharsets.UTF_8))) {
                    String errLine;
                    while ((errLine = errReader.readLine()) != null) {
                        stderrLines.add(errLine);
                    }
                } catch (IOException ioe) {
                    log.debug("[{}] jmap stderr drain interrupted: {}", label, ioe.getMessage());
                }
            }, "jmap-clstats-stderr-drain");
            stderrDrain.setDaemon(true);
            stderrDrain.start();
            final List<String> lines = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    lines.add(line);
                }
            }
            final boolean exited = p.waitFor(30, TimeUnit.SECONDS);
            final int exitCode = exited ? p.exitValue() : -1;
            stderrDrain.join(2000);

            final ClassLoaderStatsResult r = ClassLoaderStatsParser.parse(lines);

            // suspicious 条件：parsedLines=0 或 bootstrap=0 表示解析完全失败；
            // otherClasses>100000 表示数据异常；otherDead>100 表示大量死 CL 可疑。
            // suspicious 条件：parsedLines=0 或 bootstrap=0 表示解析完全失败；
            // otherClasses>100000 表示数据异常。
            // otherDead 不作为条件：大量 dead DelegatingClassLoader 在两组都属正常。
            if (r.parsedLines == 0 || r.bootstrapClasses == 0
                    || r.otherClasses > 100000L) {
                log.warn("[{}] jmap -clstats suspicious result (parsedLines={}, exitCode={}, "
                        + "rawLines={}, bootstrap={}, other={}, otherDead={}). Raw output (first 30):",
                        label, r.parsedLines, exitCode, lines.size(),
                        r.bootstrapClasses, r.otherClasses, r.otherDead);
                for (int i = 0; i < Math.min(lines.size(), 30); i++) {
                    log.warn("[{}]   [{}] {}", label, i, lines.get(i));
                }
                if (!stderrLines.isEmpty()) {
                    log.warn("[{}]   stderr (first 10):", label);
                    for (int i = 0; i < Math.min(stderrLines.size(), 10); i++) {
                        log.warn("[{}]     [{}] {}", label, i, stderrLines.get(i));
                    }
                }
            }
            return r;
        } catch (Exception e) {
            log.warn("[{}] Failed to capture ClassLoader stats in container {}: {}",
                    label, containerName, e.getMessage());
            return new ClassLoaderStatsResult();
        }
    }

    static void dumpClassLoaderStats(String containerName, String label,
                                      ClassLoaderStatsResult baseline,
                                      ClassLoaderStatsResult preGc) {
        final ClassLoaderStatsResult r = captureClassLoaderStats(containerName, label);

        log.info("[{}] ========== ClassLoader Stats (jmap -clstats) ==========", label);
        log.info("[{}]   <bootstrap>                {} classes  live", label, r.bootstrapClasses);
        log.info("[{}]   AppClassLoader             {} classes  live", label, r.appClasses);
        log.info("[{}]   SeaTunnelChildFirstCL      {} classes  (alive={}, dead={})",
                label, r.subClTotalClasses, r.subClAlive, r.subClDead);
        log.info("[{}]   other CLs                  {} classes  (alive={}, dead={})",
                label, r.otherClasses, r.otherAlive, r.otherDead);
        final long retainedFromSystem = r.bootstrapClasses + r.appClasses;
        log.info("[{}]   retained from bootstrap+AppCL: {} (sub CL classes: {})",
                label, retainedFromSystem, r.subClTotalClasses);

        if (baseline != null) {
            final long baselineTotal = baseline.bootstrapClasses + baseline.appClasses
                    + baseline.subClTotalClasses + baseline.otherClasses;
            log.info("[{}]   ----- baseline -----", label);
            log.info("[{}]   total: {} (boot {} + AppCL {} + STCL {} (alive={},dead={}) + other {} (alive={},dead={}))",
                    label, baselineTotal, baseline.bootstrapClasses, baseline.appClasses,
                    baseline.subClTotalClasses, baseline.subClAlive, baseline.subClDead,
                    baseline.otherClasses, baseline.otherAlive, baseline.otherDead);
        }
        if (preGc != null) {
            final long preGcTotal = preGc.bootstrapClasses + preGc.appClasses
                    + preGc.subClTotalClasses + preGc.otherClasses;
            log.info("[{}]   ----- pre-GC -----", label);
            log.info("[{}]   total: {} (boot {} + AppCL {} + STCL {} (alive={},dead={}) + other {} (alive={},dead={}))",
                    label, preGcTotal, preGc.bootstrapClasses, preGc.appClasses,
                    preGc.subClTotalClasses, preGc.subClAlive, preGc.subClDead,
                    preGc.otherClasses, preGc.otherAlive, preGc.otherDead);
        }
        {
            final long postGcTotal = r.bootstrapClasses + r.appClasses
                    + r.subClTotalClasses + r.otherClasses;
            log.info("[{}]   ----- post-GC -----", label);
            log.info("[{}]   total: {} (boot {} + AppCL {} + STCL {} (alive={},dead={}) + other {} (alive={},dead={}))",
                    label, postGcTotal, r.bootstrapClasses, r.appClasses,
                    r.subClTotalClasses, r.subClAlive, r.subClDead,
                    r.otherClasses, r.otherAlive, r.otherDead);
        }
        log.info("[{}] ===========================================================", label);
    }

    static final Pattern HEAP_INFO_METASPACE_PATTERN =
            Pattern.compile("Metaspace\\s+used\\s+(\\d+)\\s*K", Pattern.CASE_INSENSITIVE);

    static int indexOf(String[] arr, String key) {
        for (int i = 0; i < arr.length; i++) {
            if (arr[i].equalsIgnoreCase(key)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 执行 {@code jstat -class} 采集类加载/卸载计数 {@code [loaded, unloaded]}。
     * <p>
     * 与 jcmd GC.class_stats 不同：后者依赖 -XX:+UnlockDiagnosticVMOptions，而该参数
     * 通过 JAVA_OPTS 注入会被 SeaTunnel 启动脚本重建的 JAVA_OPTS 覆盖、无法生效；
     * {@code jstat -class} 无需任何诊断参数，容器内 JDK 自带、稳定可用，直接给出
     * Loaded/Unloaded 两列。失败时返回 {@code new long[] {-1L, -1L}} 不影响主审计。
     */
    static long[] captureClassCounts(String containerName) {
        try {
            final String pid = resolveJavaPid(containerName);
            final ProcessBuilder pb = new ProcessBuilder(
                    "docker", "exec", containerName, "jstat", "-class", pid);
            pb.redirectErrorStream(true);
            final Process p = pb.start();
            final List<String> lines = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    lines.add(line.trim());
                }
            }
            p.waitFor(15, TimeUnit.SECONDS);
            // 过滤 JVM 工具参数输出（如 Picked up JAVA_TOOL_OPTIONS 等），动态匹配包含 Loaded/Unloaded 的真实表头行
            int headerIdx = -1;
            for (int i = 0; i < lines.size(); i++) {
                final String l = lines.get(i);
                if (l.contains("Loaded") && l.contains("Unloaded")) {
                    headerIdx = i;
                    break;
                }
            }
            if (headerIdx >= 0 && headerIdx + 1 < lines.size()) {
                final String[] headers = lines.get(headerIdx).split("\\s+");
                final String[] values = lines.get(headerIdx + 1).split("\\s+");
                final int loadedIdx = indexOf(headers, "Loaded");
                final int unloadedIdx = indexOf(headers, "Unloaded");
                if (loadedIdx >= 0 && unloadedIdx >= 0
                        && loadedIdx < values.length && unloadedIdx < values.length) {
                    return new long[]{Long.parseLong(values[loadedIdx]), Long.parseLong(values[unloadedIdx])};
                }
            }
            log.warn("jstat -class summary not found in container {}; raw head: {}",
                    containerName, lines.isEmpty() ? "" : lines.get(0));
            return new long[]{-1L, -1L};
        } catch (Exception e) {
            log.warn("jstat -class capture failed in container {}: {}", containerName, e.getMessage());
            return new long[]{-1L, -1L};
        }
    }

    /**
     * 打印类加载/卸载净增（baseline -> final），结合 CL 拘留计数判定泄漏证候。
     * <p>
     * 判据（综合方案）：CL=0 时类元数据未卸载属 bootstrap/AppCL 正常驻留，非泄漏；
     * CL>0 且 retained>0 时 CL 拘留 + 类未卸载 = Metaspace 泄漏成立。
     */
    static void printClassCountDiff(String targetLabel, long[] baseline, long[] finalSample,
                                     long classLoaderCount) {
        log.info("[{}] ========== Class Load / Unload Growth (Metaspace root cause) ==========",
                targetLabel);
        if (baseline[0] < 0 || finalSample[0] < 0) {
            log.info("[{}]   (class load/unload counts unavailable via jstat -class)",
                    targetLabel);
            log.info("[{}] ===========================================================", targetLabel);
            return;
        }
        final long loadedDelta = finalSample[0] - baseline[0];
        final long unloadedDelta = finalSample[1] - baseline[1];
        final long retained = loadedDelta - unloadedDelta;
        log.info("[{}]   loaded classes   baseline={} -> final={} (delta +{})",
                targetLabel, baseline[0], finalSample[0], loadedDelta);
        log.info("[{}]   unloaded classes baseline={} -> final={} (delta +{})",
                targetLabel, baseline[1], finalSample[1], unloadedDelta);
        final String verdict;
        if (classLoaderCount < 0) {
            verdict = "CL 采样失败，无法判定";
        } else if (retained <= 0) {
            verdict = "不成立";
        } else if (classLoaderCount == 0) {
            verdict = "有残留但 CL=0（类来自 bootstrap/AppCL 正常驻留，非泄漏）";
        } else {
            verdict = "成立（CL 拘留 + 类未卸载 = Metaspace 泄漏）";
        }
        log.info("[{}]   retained (loaded - unloaded) delta: +{} classes => 类元数据未卸载证候 {}",
                targetLabel, retained, verdict);
        log.info("[{}] ===========================================================", targetLabel);
    }

    /**
     * 执行 {@code jcmd <pid> GC.class_stats} 捕获存活类的"每个类同名字符串出现次数"。
     * <p>
     * GC.class_stats 按 ClassLoader 输出，同一类名被 N 个 ClassLoader 加载即出现 N 次——
     * 该计数正是"类元数据被多少份 loader 强持有、未随 job 回收"的直接度量。
     * 需要容器 JVM 带 -XX:+UnlockDiagnosticVMOptions（经 JAVA_TOOL_OPTIONS 注入才确保生效）。
     * 失败时返回空 Map，不影响主审计。
     */
    static Map<String, Integer> captureClassStats(String containerName) {
        final Map<String, Integer> counts = new HashMap<>();
        try {
            final String pid = resolveJavaPid(containerName);
            final ProcessBuilder pb = new ProcessBuilder(
                    "docker", "exec", containerName, "jcmd", pid, "GC.class_stats");
            pb.redirectErrorStream(true);
            final Process p = pb.start();
            final List<String> lines = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    lines.add(line.trim());
                }
            }
            p.waitFor(25, TimeUnit.SECONDS);
            for (String line : lines) {
                final String cls = firstClassToken(line);
                if (cls != null) {
                    counts.merge(cls, 1, Integer::sum);
                }
            }
            if (counts.isEmpty()) {
                log.warn("GC.class_stats returned no classes in container {} (unlock may be missing via JAVA_TOOL_OPTIONS); head: {}",
                        containerName, lines.isEmpty() ? "" : String.join(" | ", lines.subList(0, Math.min(3, lines.size()))));
            }
        } catch (Exception e) {
            log.warn("GC.class_stats capture failed in container {}: {}", containerName, e.getMessage());
        }
        return counts;
    }

    /**
     * 从 GC.class_stats 的一行中提取类名 token（形如 {@code a/b/C} 或 {@code pkg.Cls}），
     * 跳过索引数字列与表头。提取不到返回 {@code null}。
     */
    static String firstClassToken(String line) {
        if (line == null || line.isEmpty()) {
            return null;
        }
        if (line.startsWith("=") || line.startsWith("GC class") || line.startsWith("Class")) {
            return null;
        }
        for (String t : line.split("\\s+")) {
            if (t.length() <= 1 || t.matches("\\d+")) {
                continue;
            }
            if (!Character.isLetter(t.charAt(0))) {
                continue;
            }
            if (t.contains("/") || t.contains(".")) {
                return t;
            }
        }
        return null;
    }

    /**
     * 打印引擎/连接器类在 final 较 baseline 的净增 loader 持有副本，定位具体泄漏类。
     * <p>
     * 只统计命中 SeaTunnel/连接器/MySQL/Kafka 关键字的类，打印净增 Top 15，避免刷日志。
     */
    static void printClassStatsDiff(String targetLabel,
                                     Map<String, Integer> baseline,
                                     Map<String, Integer> finalSample) {
        log.info("[{}] ========== Class Stats Growth (GC.class_stats; +copies = 被多 ClassLoader 持有未回收) " + "==========",
                targetLabel);
        if (baseline.isEmpty() || finalSample.isEmpty()) {
            log.info("[{}]   (class stats unavailable — 需 JAVA_TOOL_OPTIONS 注入 -XX:+UnlockDiagnosticVMOptions)",
                    targetLabel);
            log.info("[{}] ===========================================================", targetLabel);
            return;
        }
        final List<String> includes = Arrays.asList(
                "org/apache/seatunnel", "SeaTunnelChildFirstClassLoader", "com/mysql", "org/apache/kafka");
        final Map<String, Integer> retained = new HashMap<>();
        for (Map.Entry<String, Integer> e : finalSample.entrySet()) {
            final String cls = e.getKey();
            if (includes.stream().noneMatch(cls::contains)) {
                continue;
            }
            final int net = e.getValue() - baseline.getOrDefault(cls, 0);
            if (net > 0) {
                retained.put(cls, net);
            }
        }
        if (retained.isEmpty()) {
            log.info("[{}]   no retained connector/engine class growth (类均已卸载或无新增占用)", targetLabel);
        } else {
            final List<Map.Entry<String, Integer>> entries = new ArrayList<>(retained.entrySet());
            entries.sort((a, b) -> b.getValue() - a.getValue());
            final int top = Math.min(15, entries.size());
            for (int i = 0; i < top; i++) {
                log.info("[{}]   +{} loader copies  {}", targetLabel,
                        entries.get(i).getValue(), entries.get(i).getKey());
            }
        }
        log.info("[{}] ===========================================================", targetLabel);
    }

    static void forceFullGcInContainer(String containerName) {
        try {
            final String pid = resolveJavaPid(containerName);
            // 三轮 Full GC 保障类元数据彻底卸载：
            // 第一轮：回收只被 weak/soft 引用持有的 ClassLoader
            // 第二轮：回收第一轮 GC 后因 WeakHashMap expunge / 缓存清理才暴露的 ClassLoader
            // 第三轮：卸载已回收 ClassLoader 加载的类元数据（Metaspace 释放）
            for (int i = 0; i < 3; i++) {
                final Process p = new ProcessBuilder("docker", "exec", containerName, "jcmd", pid, "GC.run").start();
                p.waitFor(5, TimeUnit.SECONDS);
                Thread.sleep(2000);
            }
        } catch (InterruptedException e) {
            log.warn("Full GC trigger interrupted in container {}: {}", containerName, e.getMessage());
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn("Full GC trigger failed in container {}: {}", containerName, e.getMessage());
        }
    }

    /**
     * 动态探测并获取容器内运行的 Java 进程 PID。
     * <p>
     * 容器的主进程通常是入口 Shell 脚本（占用 PID 1），真实的 SeaTunnel JVM 实例是其派生的子进程。
     * 本方法首先通过容器内 JDK 自带的 {@code jps -l} 进行探测，排除 jps 自身瞬时进程后，精准锁定真实的 JVM PID；
     * 若 jps 异常，后备使用 Linux 原生 {@code pgrep -f java} 进行兜底检索。
     *
     * @param containerName 目标 Docker 容器名
     * @return 真实的 Java 进程 PID 字符串
     * @throws IllegalStateException 若容器内未检测到任何正在运行的 Java 进程
     */
    static String resolveJavaPid(String containerName) throws Exception {
        // 1. 优先通过 jps -l 检索，提取包含 SeaTunnelServer 或非 Jps 的真实 Java 进程
        final ProcessBuilder jpsPb = new ProcessBuilder(
                "docker", "exec", containerName, "jps", "-l");
        jpsPb.redirectErrorStream(true);
        final Process jpsProcess = jpsPb.start();
        final List<String> jpsLines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(jpsProcess.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                final String trimmed = line.trim();
                if (!trimmed.isEmpty()) {
                    jpsLines.add(trimmed);
                }
            }
        }
        jpsProcess.waitFor(5, TimeUnit.SECONDS);

        // 优先匹配 SeaTunnel 核心服务类
        for (String line : jpsLines) {
            final String[] parts = line.split("\\s+");
            if (parts.length >= 2 && parts[0].matches("\\d+")) {
                final String pid = parts[0];
                final String mainClass = parts[1];
                if (!mainClass.contains("Jps") && (mainClass.contains("SeaTunnel") || mainClass.contains("starter"))) {
                    return pid;
                }
            }
        }

        // 次选：排除 Jps 后的任意合法 Java PID
        for (String line : jpsLines) {
            final String[] parts = line.split("\\s+");
            if (parts.length >= 1 && parts[0].matches("\\d+")) {
                final String pid = parts[0];
                final String mainClass = parts.length > 1 ? parts[1] : "";
                if (!mainClass.contains("Jps")) {
                    return pid;
                }
            }
        }

        // 2. 后备方案：通过 Linux 原生 pgrep -f java 查找 PID
        final ProcessBuilder pgrepPb = new ProcessBuilder(
                "docker", "exec", containerName, "sh", "-c", "pgrep -f java || pidof java");
        pgrepPb.redirectErrorStream(true);
        final Process pgrepProcess = pgrepPb.start();
        final List<String> pgrepLines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(pgrepProcess.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                final String trimmed = line.trim();
                if (!trimmed.isEmpty()) {
                    pgrepLines.add(trimmed);
                }
            }
        }
        pgrepProcess.waitFor(5, TimeUnit.SECONDS);

        for (String line : pgrepLines) {
            final String[] pids = line.split("\\s+");
            for (String pid : pids) {
                if (pid.matches("\\d+")) {
                    return pid;
                }
            }
        }

        throw new IllegalStateException("Failed to resolve Java PID in container '"
                + containerName + "'. jps output: " + jpsLines + ", pgrep output: " + pgrepLines);
    }

    static long getCurrentMetaspaceUsed(String containerName) throws Exception {
        final String pid = resolveJavaPid(containerName);

        // 1. 优先使用 JDK 8~21 通用的 jcmd <PID> GC.heap_info
        final ProcessBuilder pb = new ProcessBuilder(
                "docker", "exec", containerName,
                "jcmd", pid, "GC.heap_info");
        pb.redirectErrorStream(true);
        final Process p = pb.start();
        final StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append('\n');
            }
        }
        p.waitFor(10, TimeUnit.SECONDS);

        final Matcher matcher = HEAP_INFO_METASPACE_PATTERN.matcher(output);
        if (matcher.find()) {
            final long kb = Long.parseLong(matcher.group(1));
            return kb * 1024L;
        }

        // 2. 后备方案：使用 jstat -gc <PID> 提取 MU (Metaspace Used, KB)
        final ProcessBuilder jstatPb = new ProcessBuilder(
                "docker", "exec", containerName,
                "jstat", "-gc", pid);
        jstatPb.redirectErrorStream(true);
        final Process jstatP = jstatPb.start();
        final List<String> jstatLines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(jstatP.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    jstatLines.add(line.trim());
                }
            }
        }
        jstatP.waitFor(10, TimeUnit.SECONDS);

        // 动态定位包含 MU 的真实表头行
        int headerIdx = -1;
        for (int i = 0; i < jstatLines.size(); i++) {
            final String l = jstatLines.get(i);
            if (l.contains("MU")) {
                headerIdx = i;
                break;
            }
        }
        if (headerIdx >= 0 && headerIdx + 1 < jstatLines.size()) {
            final String[] headers = jstatLines.get(headerIdx).split("\\s+");
            final String[] values = jstatLines.get(headerIdx + 1).split("\\s+");
            int muIndex = -1;
            for (int i = 0; i < headers.length; i++) {
                if ("MU".equalsIgnoreCase(headers[i])) {
                    muIndex = i;
                    break;
                }
            }
            if (muIndex >= 0 && muIndex < values.length) {
                final double kb = Double.parseDouble(values[muIndex]);
                return (long) (kb * 1024.0);
            }
        }

        throw new IllegalStateException("Failed to parse Metaspace usage from container '"
                + containerName + "' (PID " + pid + "). jcmd output: [" + output.toString().trim() + "], jstat lines: " + jstatLines);
    }



    /**
     * 统计容器内 SeaTunnelChildFirstClassLoader 的存活实例数。
     * <p>
     * 通过 {@code jmap -histo:live <pid>} 获取堆直方图，搜索
     * {@code org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader}
     * 的实例数。此值直接反映作业结束后 ClassLoader 是否被 GC 回收——
     * 若 Agent 阻碍了 ClassLoader 释放，此值将大于 0。
     * <p>
     * 与 SeaTunnel 官方 E2E（{@code SeaTunnelContainer.classLoaderObjectCheck}）方法一致。
     *
     * @param containerName 容器名
     * @return 存活实例数，采样失败返回 Long.MIN_VALUE
     */
    static long countSeaTunnelClassLoaders(String containerName) {
        final String targetClass =
                "org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader";
        try {
            final String pid = JvmDiagnostics.resolveJavaPid(containerName);
            final ProcessBuilder pb = new ProcessBuilder(
                    "docker", "exec", containerName, "jmap", "-histo:live", pid);
            pb.redirectErrorStream(true);
            final Process p = pb.start();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.contains(targetClass)) {
                        final String[] parts = line.trim().split("\\s+");
                        if (parts.length >= 2) {
                            return Long.parseLong(parts[1]);
                        }
                    }
                }
            }
            p.waitFor(30, TimeUnit.SECONDS);
            return 0L;
        } catch (Exception e) {
            log.warn("Failed to count SeaTunnelChildFirstClassLoader in container {}: {}",
                    containerName, e.getMessage());
            return Long.MIN_VALUE;
        }
    }

}
