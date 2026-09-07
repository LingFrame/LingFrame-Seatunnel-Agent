package com.lingframe.agent.adapter;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 治理拒绝退避控制器。
 * <p>
 * 提供「令牌间隔 + 抖动 + 每线程每秒退避预算」的退避原语，替代固定 {@code sleep(100ms)}：
 * <ul>
 *   <li>令牌间隔：限流等下一个令牌的有意义等待 = {@code 1000/rateLimit} ms。固定 100ms 在
 *       rateLimit=1000/s 时等同把吞吐过度降速 100 倍，与令牌桶语义脱钩。</li>
 *   <li>抖动：interval ± 20%，避免多方被拒后相位对齐导致冲击式重试。</li>
 *   <li>每线程每秒预算：单线程每秒退避总时长封顶 {@link #BUDGET_MS_PER_SECOND}，超过即停止等待，
 *       防止极端风暴下 Worker 被退避长时间占死（fail-open 铁律的延伸）。</li>
 * </ul>
 */
public final class BackoffController {

    /** 抖动比例：令牌间隔的 ±20% */
    private static final double JITTER_RATIO = 0.20D;
    /** 单线程每秒退避预算（毫秒）：资源底线，防止退避长时间占死 Worker 线程 */
    private static final long BUDGET_MS_PER_SECOND = 500L;

    /** 当前线程本秒已累计的退避毫秒数与所属秒的时间戳 */
    private final ThreadLocal<BudgetCursor> budgetCursor = new ThreadLocal<>();

    /**
     * 限流退避：等待一个令牌桶填充间隔（含抖动、受每秒预算约束），然后返回。
     */
    public void backoffRatelimited(int rateLimitPerSecond) {
        final int effectiveRate = Math.max(1, rateLimitPerSecond);
        final long tokenIntervalMs = Math.max(1L, 1_000L / effectiveRate);
        final long jittered = jitter(tokenIntervalMs);
        final long sleepMs = consumeBudget(jittered);
        if (sleepMs <= 0L) {
            return;
        }
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /** 对基准值施加 ±{@link #JITTER_RATIO} 抖动（至少 1ms）。 */
    private long jitter(long baseMs) {
        final double delta = baseMs * JITTER_RATIO;
        final double v = baseMs + ThreadLocalRandom.current().nextDouble(-delta, delta);
        return Math.max(1L, (long) v);
    }

    /**
     * 每秒预算内扣减并返回本次实际可退避毫秒数；预算耗尽返回 ≤0（不等待）。
     * 预算按「自然秒」滚动：当秒变化时重置计数。
     */
    private long consumeBudget(long requestedMs) {
        final long now = System.currentTimeMillis();
        BudgetCursor cursor = budgetCursor.get();
        if (cursor == null) {
            cursor = new BudgetCursor();
            budgetCursor.set(cursor);
        }
        if (now - cursor.secondStartMs >= 1_000L) {
            cursor.secondStartMs = now;
            cursor.spentMs = 0L;
        }
        final long remaining = Math.max(0L, BUDGET_MS_PER_SECOND - cursor.spentMs);
        final long grant = Math.min(requestedMs, remaining);
        cursor.spentMs += grant;
        return grant;
    }

    /** msg级退避游标：记录当前自然秒起始与已消耗预算。 */
    private static final class BudgetCursor {
        private long secondStartMs = System.currentTimeMillis();
        private long spentMs = 0L;
    }
}