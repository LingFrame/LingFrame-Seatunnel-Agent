package com.lingframe.agent.adapter;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;

/**
 * per-call 钩子耗时埋点统计器（timing-enabled=true 时启用，默认零损耗）。
 * <p>
 * 从 {@code SeaTunnelAdapter} 提取，职责单一化：记录 before/after 钩子耗时并提供 JMX 可读统计。
 */
final class HookLatencyTracker {

    private final AtomicLong callCount = new AtomicLong();
    private final AtomicLong latencySumNanos = new AtomicLong();
    private final LongAccumulator latencyMinNanos = new LongAccumulator(Long::min, Long.MAX_VALUE);
    private final LongAccumulator latencyMaxNanos = new LongAccumulator(Long::max, Long.MIN_VALUE);

    void recordLatency(long nanos) {
        callCount.incrementAndGet();
        latencySumNanos.addAndGet(nanos);
        latencyMinNanos.accumulate(nanos);
        latencyMaxNanos.accumulate(nanos);
    }

    long getCallCount() {
        return callCount.get();
    }

    long getLatencyAvgNanos() {
        final long c = callCount.get();
        return c > 0 ? latencySumNanos.get() / c : 0L;
    }

    long getLatencyMinNanos() {
        final long m = latencyMinNanos.get();
        return m == Long.MAX_VALUE ? 0L : m;
    }

    long getLatencyMaxNanos() {
        return latencyMaxNanos.get();
    }
}