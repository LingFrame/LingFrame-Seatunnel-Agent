package com.lingframe.benchmark;

import java.util.Arrays;
import java.util.Locale;

/**
 * 第五轮探针：单 gap 深跑，配合 JFR 帧级采样定位「间隔依赖 ~66µs 残余成本」。
 * <p>
 * 只跑单一 gap（默认 20ms）大样本对（默认 300 对），全程输出进度便于与 JFR
 * 时间窗对齐。用法：BodyProbe5 [gapMs] [nPairs]。
 */
public final class BodyProbe5 {

    public static void main(String[] args) {
        final long gapMs = args.length > 0 ? Long.parseLong(args[0]) : 20L;
        final int n = args.length > 1 ? Integer.parseInt(args[1]) : 300;
        final String mode = args.length > 2 ? args[2] : "sleep";
        final NoopBatchTask woven = new NoopBatchTask(1L, 0L);
        final PlainBatchTask plain = new PlainBatchTask(0L);
        // keeper 模式：后台线程每 2ms 执行一次同款 woven 调用，保持 pipeline 热路径在共享 cache 常驻
        if ("keeper".equals(mode)) {
            Thread keeper = new Thread(() -> {
                final NoopBatchTask kw = new NoopBatchTask(1L, 0L);
                while (!Thread.currentThread().isInterrupted()) {
                    kw.call();
                    try {
                        Thread.sleep(2);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }, "pipeline-keeper");
            keeper.setDaemon(true);
            keeper.start();
        }
        for (int i = 0; i < 400; i++) {
            plain.call();
            woven.call();
        }
        final long[] pu = new long[n];
        final long[] wu = new long[n];
        final long t0 = System.nanoTime();
        for (int i = 0; i < n; i++) {
            pu[i] = timeOne(plain);
            gap(mode, gapMs);
            wu[i] = timeOne(woven);
            gap(mode, gapMs);
            if ((i + 1) % 100 == 0) {
                System.out.printf(Locale.ROOT, "[BodyProbe5] progress pair=%d elapsed=%.2fs%n",
                        i + 1, (System.nanoTime() - t0) / 1e9);
            }
        }
        Arrays.sort(pu);
        Arrays.sort(wu);
        System.out.printf(Locale.ROOT,
                "[BodyProbe5] gap=%dms N=%d plain p50=%9.3f woven p50=%9.3f delta(p50)=%8.3f us meanDelta=%8.3f us (elapsed=%.2fs)%n",
                gapMs, n, pu[n / 2] / 1e3, wu[n / 2] / 1e3,
                (wu[n / 2] - pu[n / 2]) / 1e3, (mean(wu) - mean(pu)) / 1e3,
                (System.nanoTime() - t0) / 1e9);
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

    /** gap 等待：sleep（线程换出）或 spin（busy 占核，保持线程上下文与 cache 热度） */
    private static void gap(String mode, long ms) {
        if (ms <= 0) {
            return;
        }
        if ("spin".equals(mode)) {
            final long end = System.nanoTime() + ms * 1_000_000L;
            while (System.nanoTime() < end) {
                // busy wait
            }
            return;
        }
        sleep(ms);
    }

    private static double mean(long[] xs) {
        double s = 0;
        for (long x : xs) {
            s += x;
        }
        return s / xs.length;
    }
}
