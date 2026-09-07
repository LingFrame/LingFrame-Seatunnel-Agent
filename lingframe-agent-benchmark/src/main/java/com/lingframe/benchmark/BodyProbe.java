package com.lingframe.benchmark;

import java.util.Locale;

/**
 * 临时诊断探针：二分「governed 档长批次 +200µs」的来源。
 * <p>
 * 问题背景：JMH 测得 100ms 批次档附加 ~+215µs（与日志级别/GC 无关），而 1ms 档仅 +9µs、
 * 0 档仅 +1µs。本探针区分两种假设：
 * <ul>
 *   <li>B：拦截钩子在「100ms 调用节奏」下本身就贵（钩子成本随节奏/间隔变化）；</li>
 *   <li>C：钩子本身便宜，贵的是「100ms 自旋体 + 钩子」的组合（如 spin 被钩子的副作用干扰）。</li>
 * </ul>
 * B 用 slice=0 的 call + 外部 100ms sleep 模拟同样节奏；C 用 slice=100ms 的 call 内自旋。
 * 对照即出结论。运行于 governed fork（-javaagent + 全治理配置）。
 */
public final class BodyProbe {

    private static final int N = 10;

    public static void main(String[] args) {
        final NoopBatchTask woven = new NoopBatchTask(1L, 0L);
        final PlainBatchTask plain = new PlainBatchTask(0L);
        // warmup：让两条路径 JIT 到位
        for (int i = 0; i < 500; i++) {
            woven.call();
            plain.call();
        }
        report("A slice=0,no-gap", timeLoops(woven, plain, 0L, 0L));
        report("B slice=0,gap100ms", timeLoops(woven, plain, 0L, 100L));
        report("C slice=100ms(spin)", timeLoops(woven, plain, 100_000_000L, 0L));
    }

    /** 交替测量两条路径各 N 次，返回各自 µs/call 与差值。 */
    private static void report(String label, long[] plainWoven) {
        final double plainUs = plainWoven[0] / 1_000.0 / N;
        final double wovenUs = plainWoven[1] / 1_000.0 / N;
        System.out.printf(Locale.ROOT,
                "[BodyProbe] %-22s plain=%10.3f us/call woven=%10.3f us/call delta=%8.3f us%n",
                label, plainUs, wovenUs, wovenUs - plainUs);
    }

    /** 先测 plain 再测 woven；返回 {plainNanos, wovenNanos}。 */
    private static long[] timeLoops(NoopBatchTask woven, PlainBatchTask plain,
                                    long sliceNanos, long gapMs) {
        woven.setSliceNanos(sliceNanos);
        plain.setSliceNanos(sliceNanos);
        long[] result = new long[2];
        result[0] = timeCalls(plain, gapMs);
        result[1] = timeCalls(woven, gapMs);
        return result;
    }

    private static long timeCalls(BatchTask task, long gapMs) {
        long acc = 0L;
        final long start = System.nanoTime();
        for (int i = 0; i < N; i++) {
            acc += task.call().ordinal() + task.getLastChecksum();
            if (gapMs > 0) {
                try {
                    Thread.sleep(gapMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return -1L;
                }
            }
        }
        return System.nanoTime() - start + (acc == Long.MIN_VALUE ? 1 : 0);
    }
}
