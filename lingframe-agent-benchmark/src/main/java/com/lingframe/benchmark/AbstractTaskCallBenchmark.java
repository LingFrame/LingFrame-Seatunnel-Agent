package com.lingframe.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

/**
 * 「完整 SeaTunnel 版」JMH 基准（A/B 同 JVM 加固版）——实测 Agent 对真实
 * {@code AbstractTask.call()} 的拦截损耗。
 * <p>
 * 测量设计（相对跨 fork 对比版的三项加固，解决「跨 fork 比微损耗噪声即信号」问题）：
 * <ol>
 *   <li><b>同 JVM A/B 对照</b>：fork 内同时测 {@link NoopBatchTask#call()}（被织入）与
 *       {@link PlainBatchTask#call()}（同方法体、未织入），损耗口径取
 *       {@code woven − plain}（附加延迟 µs/op），fork 间 JIT/GC/频率噪声被同时消除；</li>
 *   <li><b>载荷不可消除</b>：{@code call()} 内做 1k 行内存记录变换聚合（共享
 *       {@link BatchCore}，两条路径零逻辑漂移），校验和经 Blackhole 流出方法，杜绝整体消除；</li>
 *   <li><b>AverageTime 报延迟</b>：单次 call 的 µs/op 附加延迟可直接换算 job 级总耗时影响；</li>
 *   <li><b>织入自检</b>：@TearDown 探针按 fork 模式断言——governed 下被织入路径必须显著慢于
 *       对照组（否则说明切点静默未织入，测出「零损耗」是假阳性）；no-agent/default 下两者必须
 *       无差（自证测量装置灵敏，能分辨差异时才值得信）。</li>
 * </ol>
 * 三种 fork JVM（run-benchmark.sh 逐一拉起）：no-agent / agent-default / agent-governed。
 * 探针在 @TearDown 前把批次 spin 清零，让拦截成本不被批次墙钟稀释、差异放大可判。
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class AbstractTaskCallBenchmark {

    /** 模拟单批次墙钟耗时（微秒）：0=纯变换载荷地板；1000=极小批次；100000=100ms 典型批次。 */
    @Param({"0", "1000", "100000"})
    private long sliceMicros;

    /** per-job 基准的并发作业槽位数：轮转触达作业级注册表热路径，覆盖多作业规模。 */
    @Param({"64"})
    private int jobSlots;

    private NoopBatchTask wovenTask;
    private PlainBatchTask plainTask;
    /** per-job 基准任务池：jobSlots 个不同 jobId 的 AbstractTask，轮转驱动作业级治理热路径。 */
    private NoopBatchTask[] jobTasks;
    /** per-job 轮转游标（@State(Benchmark) 单线程，无需同步）。 */
    private long nextJobIndex;
    private ForkMode forkMode;

    /** 织入自检已执行标记（同一 fork 内两个 @Benchmark 方法各触发一次 @TearDown，只探一次）。 */
    private static volatile boolean probeDone;

    private enum ForkMode {
        NO_AGENT,
        AGENT_DEFAULT,
        AGENT_GOVERNED
    }

    @Setup
    public void setup() {
        final long sliceNanos = sliceMicros * 1_000L;
        wovenTask = new NoopBatchTask(1L, sliceNanos);
        plainTask = new PlainBatchTask(sliceNanos);
        jobTasks = new NoopBatchTask[jobSlots];
        for (int i = 0; i < jobSlots; i++) {
            jobTasks[i] = new NoopBatchTask(i, sliceNanos);
        }
        nextJobIndex = 0L;
        forkMode = detectForkMode();
        System.out.println("[bench] forkMode=" + forkMode + " sliceMicros=" + sliceMicros
                + " sliceNanos=" + sliceNanos + " jobSlots=" + jobSlots);
    }

    /**
     * 从 JVM 输入参数探测当前 fork 模式：
     * 无 -javaagent → no-agent；-javaagent:jar（无 '='）→ default；
     * -javaagent:jar=配置 → governed（配置经 '=' 传入才会开启批次切点）。
     */
    private static ForkMode detectForkMode() {
        String agentArg = null;
        for (String arg : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
            if (arg.startsWith("-javaagent:")) {
                agentArg = arg.substring("-javaagent:".length());
                break;
            }
        }
        if (agentArg == null) {
            return ForkMode.NO_AGENT;
        }
        return agentArg.indexOf('=') >= 0 ? ForkMode.AGENT_GOVERNED : ForkMode.AGENT_DEFAULT;
    }

    /** 被织入路径：真实 AbstractTask 子类的 call()（governed fork 中被切点包裹）。 */
    @Benchmark
    public void wovenCall(Blackhole blackhole) {
        blackhole.consume(wovenTask.call());
        // 消费校验和：使 call() 内的载荷存储可观测，防止 JIT 整体消除
        blackhole.consume(wovenTask.getLastChecksum());
    }

    /** 对照组路径：同方法体未织入类，同 JVM 内基准。 */
    @Benchmark
    public void plainCall(Blackhole blackhole) {
        blackhole.consume(plainTask.call());
        blackhole.consume(plainTask.getLastChecksum());
    }

    /**
     * per-job 路径：jobSlots 个作业轮转，每次驱动 {@code JobIdExtractor.extract →
     * JobLingRegistry.resolveLingId/touch → 完整 Pipeline}。warmup 后各作业已注册，
     * 测的是多作业规模下的作业级治理热路径（governed fork 中生效；其余 fork 与 wovenCall
     * 等价——同为被织入调用，仅 jobId 轮转，用于对照 plainCall 得出 per-job 附加损耗）。
     */
    @Benchmark
    public void wovenPerJobCall(Blackhole blackhole) {
        final NoopBatchTask task = jobTasks[(int) (nextJobIndex++ % jobSlots)];
        blackhole.consume(task.call());
        blackhole.consume(task.getLastChecksum());
    }

    /**
     * 织入自检（Trial 级 TearDown，每次 fork 只探一次）：
     * 先把两条路径的 spin 清零（放大纯拦截差异），warmup 后计时对比。
     * <ul>
     *   <li>governed：被织入路径必须显著慢于对照组（织入确实发生），否则抛错——防止
     *       「切点静默未织入却报零损耗」的假阳性；</li>
     *   <li>no-agent / default：两条路径都应无差（装置灵敏度自证），超出容忍带同样抛错。</li>
     * </ul>
     */
    @TearDown
    public void weaveSelfCheck() {
        if (probeDone) {
            return;
        }
        probeDone = true;
        wovenTask.setSliceNanos(0L);
        plainTask.setSliceNanos(0L);

        final int probeCalls = forkMode == ForkMode.AGENT_GOVERNED ? 2_000 : 50_000;
        // warmup：让两条路径的 JIT 状态对齐，进入稳态后再计时
        for (int i = 0; i < probeCalls / 10; i++) {
            plainTask.call();
            wovenTask.call();
        }
        final long plainNs = timeCalls(plainTask, probeCalls);
        final long wovenNs = timeCalls(wovenTask, probeCalls);
        final double plainUs = plainNs / 1_000.0 / probeCalls;
        final double wovenUs = wovenNs / 1_000.0 / probeCalls;
        final double ratio = wovenNs / (double) plainNs;

        final boolean pass;
        String expectation;
        if (forkMode == ForkMode.AGENT_GOVERNED) {
            expectation = "woven must be significantly slower than plain (weave evidence), ratio>=1.5";
            pass = ratio >= 1.5;
        } else {
            expectation = "no weaving expected: woven/plain within [0.8,1.25] (apparatus sanity)";
            pass = ratio >= 0.8 && ratio <= 1.25;
        }
        System.out.printf("[bench] weaveSelfCheck mode=%s probeCalls=%d plain=%.3fus/call "
                        + "woven=%.3fus/call ratio=%.3f -> %s (%s)%n",
                forkMode, probeCalls, plainUs, wovenUs, ratio,
                pass ? "PASS" : "FAIL", expectation);
        if (!pass) {
            throw new IllegalStateException(
                    "weaveSelfCheck FAILED: mode=" + forkMode + " ratio=" + ratio
                            + " (expected: " + expectation + ")");
        }
    }

    private static long timeCalls(BatchTask task, int calls) {
        long acc = 0L;
        final long start = System.nanoTime();
        for (int i = 0; i < calls; i++) {
            acc += task.call().ordinal() + task.getLastChecksum();
        }
        // acc 折叠进返回值，防计时循环被消除
        return System.nanoTime() - start + (acc == Long.MIN_VALUE ? 1 : 0);
    }
}
