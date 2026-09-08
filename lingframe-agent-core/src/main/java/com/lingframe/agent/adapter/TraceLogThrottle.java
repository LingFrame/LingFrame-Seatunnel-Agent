package com.lingframe.agent.adapter;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Trace 失败事件日志的「按签名 + 时间窗口」限频器。
 * <p>
 * 熔断器 OPEN 等集群故障形态下，同一资源会在极短时间内高频重复产生同类失败 Trace 事件
 * （例如 seatunnel-job-7 熔断打开后同毫秒刷出上万条 type=ERROR 事件）。若逐条落 INFO 日志，
 * 会瞬间引爆日志量（数十 MB / 十数万行），拖垮本地磁盘与 CI 可读性。
 * <p>
 * 本限频器保证：同一 {@code key}（建议 lingId + 归一化 action）在单个时间窗口
 * {@link #DEFAULT_WINDOW_MS} 内至多放行 1 条，其余抑制并累计 {@link #suppressedCount()}；
 * 窗口过后自动重置，下一窗口可再次放行——长期故障事实不会因节流而永久丢失。
 * <p>
 * 仅观测路径使用，绝不向外抛异常、不改变业务时序；失败为高频低发，采用 {@code synchronized}
 * 串行化保证线程安全即可，无需竞态牺牲可读性。
 * <p>
 * 内存有界、无泄漏：内部以 {@link LinkedHashMap} + {@link #maxKeys} 拒不限量增长（淘汰最旧插入签名），
 * 不会因千万级不同 jobId 同时失败导致内存暴涨。
 */
final class TraceLogThrottle {

    /** 默认时间窗口（毫秒）：同签名在 1 秒窗口内至多打印 1 条。 */
    static final long DEFAULT_WINDOW_MS = 1_000L;
    /** 默认最大跟踪签名数：防止病态放大导致内存增长失去边界。 */
    static final int DEFAULT_MAX_KEYS = 4_096;

    private final long windowMs;
    private final int maxKeys;
    /** key -> 最近一次放行打印的时间戳（毫秒），按插入序淘汰最旧签名以构成有界缓存。 */
    private final LinkedHashMap<String, Long> lastPrintedAt;
    /** 被抑制（未打印）的失败事件累计计数，用于可观测与测试断言。 */
    private final AtomicLong suppressed = new AtomicLong();

    TraceLogThrottle() {
        this(DEFAULT_WINDOW_MS, DEFAULT_MAX_KEYS);
    }

    TraceLogThrottle(long windowMs, int maxKeys) {
        this.windowMs = Math.max(1L, windowMs);
        this.maxKeys = Math.max(1, maxKeys);
        this.lastPrintedAt = new LinkedHashMap<String, Long>(16, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
                return size() > TraceLogThrottle.this.maxKeys;
            }
        };
    }

    /**
     * 尝试在当前窗口内放行该 key 的一条失败日志。
     *
     * @param key   失败签名（建议 lingId + 归一化 action）
     * @param nowMs 当前时间戳（毫秒），由调用方注入以便测试推进时间而不必真实睡眠
     * @return true 表示允许打印；false 表示本窗口内已打印过同签名，应抑制
     */
    synchronized boolean tryAcquire(String key, long nowMs) {
        if (key == null) {
            return true;
        }
        final Long last = lastPrintedAt.get(key);
        if (last != null && (nowMs - last) < windowMs) {
            suppressed.incrementAndGet();
            return false;
        }
        lastPrintedAt.put(key, nowMs);
        return true;
    }

    /** 被抑制（未打印）的失败事件累计计数。 */
    long suppressedCount() {
        return suppressed.get();
    }

    /** 当前跟踪的签名数量（仅供观测）。 */
    int trackedKeys() {
        synchronized (this) {
            return lastPrintedAt.size();
        }
    }
}