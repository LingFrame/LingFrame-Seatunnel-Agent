package com.lingframe.benchmark;

import java.util.Arrays;
import java.util.Locale;

/**
 * 第四轮探针：确认「间隔依赖」成本——0-body 织入调用在受控间隔下的钩子成本。
 * <p>
 * 相位探针已揭示 before 钩子在 100ms 间隔相位 ~105µs、密集相位 ~25µs。若成本纯由
 * 「距上次织入调用的间隔」驱动，则 0-body 调用在 gap≥5~20ms 时应同样出现 ~+100µs，
 * 与 body 无关——这将彻底改写结论：不是「长批次变贵」，而是「低频批次每次调用都付
 * 一次闲置唤醒成本」。
 * <p>
 * 方法：0-body woven/plain 交错测量，两次调用之间 sleep(gap)（sleep 在计时窗之外），
 * 每个 gap 测 N=30 对。配合 -Dlingframe.bench.noEventSubs=true 复跑，隔离 EventBus
 * 异步分发（Trace/Audit 事件 → 2 线程池）是否为根源。
 */
public final class BodyProbe4 {

    public static void main(String[] args) {
        final long[] gapsMs = {0L, 1L, 5L, 20L, 100L};
        final NoopBatchTask woven = new NoopBatchTask(1L, 0L);
        final PlainBatchTask plain = new PlainBatchTask(0L);
        for (int i = 0; i < 400; i++) {
            plain.call();
            woven.call();
        }
        for (long gapMs : gapsMs) {
            phase(String.format("gap=%4dms", gapMs), woven, plain, gapMs, 30);
        }
    }

    private static void phase(String label, BatchTask w, BatchTask p, long gapMs, int n) {
        final long[] pu = new long[n];
        final long[] wu = new long[n];
        for (int i = 0; i < n; i++) {
            pu[i] = timeOne(p);
            sleep(gapMs);
            wu[i] = timeOne(w);
            sleep(gapMs);
        }
        Arrays.sort(pu);
        Arrays.sort(wu);
        System.out.printf(Locale.ROOT,
                "[BodyProbe4] %s N=%d plain p50=%9.3f woven p50=%9.3f delta(p50)=%8.3f us meanDelta=%8.3f us%n",
                label, n, pu[n / 2] / 1e3, wu[n / 2] / 1e3,
                (wu[n / 2] - pu[n / 2]) / 1e3, (mean(wu) - mean(pu)) / 1e3);
    }

    private static long timeOne(BatchTask task) {
        final long start = System.nanoTime();
        task.call();
        return System.nanoTime() - start + (task.getLastChecksum() & 1L);
    }

    private static void sleep(long ms) {
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static double mean(long[] xs) {
        double s = 0;
        for (long x : xs) {
            s += x;
        }
        return s / xs.length;
    }
}
