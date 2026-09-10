package com.lingframe.agent.adapter;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link BackoffController} 退避语义测试。
 * <p>
 * 覆盖令牌间隔（1000/rateLimit）、抖动上限、以及每线程每秒退避预算（防占死 Worker）。
 */
class BackoffControllerTest {

    @Test
    @DisplayName("高限流阈值应退避极短（1000/1000 = 1ms，而非固定 100ms）")
    void backoffRatelimitedWhenHighRateLimitShouldBeShort() {
        final BackoffController controller = new BackoffController();
        final long t0 = System.nanoTime();
        controller.backoffRatelimited(1000);
        final long durationMs = (System.nanoTime() - t0) / 1_000_000;
        // 令牌间隔 1ms（校正核心：rateLimit=1000 时固定 100ms 等同过度降速 100 倍）
        // 阈值 100ms：Thread.sleep(1) 受 OS 调度器精度影响（Windows ~15.6ms），实际耗时可能达数十 ms
        assertThat(durationMs).isLessThan(100L);
    }

    @Test
    @DisplayName("低限流阈值应退避接近令牌间隔（1000/5 = 200ms）")
    void backoffRatelimitedWhenLowRateLimitShouldApproachTokenInterval() {
        final BackoffController controller = new BackoffController();
        final long t0 = System.nanoTime();
        controller.backoffRatelimited(5);
        final long durationMs = (System.nanoTime() - t0) / 1_000_000;
        // 200ms 令牌间隔 ± 20% 抖动下限约 160ms
        assertThat(durationMs).isGreaterThanOrEqualTo(100L);
    }

    @Nested
    @DisplayName("每秒退避预算")
    class BudgetTests {

        @Test
        @DisplayName("单线程累计退避不应耗尽后无限延长（预算 500ms 封顶当秒总等待）")
        void backoffWhenBudgetExhaustedShouldStopWaiting() {
            final BackoffController controller = new BackoffController();
            long total = 0L;
            final int lowRate = 1; // 令牌间隔 1000ms，一次即超预算
            for (int i = 0; i < 5; i++) {
                final long t0 = System.nanoTime();
                controller.backoffRatelimited(lowRate);
                total += (System.nanoTime() - t0) / 1_000_000;
            }
            // 当秒预算 500ms：多次调用应被预算封顶，绝不应累积到 5*1000ms
            assertThat(total).isLessThan(2000L);
        }
    }
}
