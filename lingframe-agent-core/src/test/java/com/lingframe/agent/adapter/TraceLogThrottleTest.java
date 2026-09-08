package com.lingframe.agent.adapter;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link TraceLogThrottle} 窗口限频逻辑测试。
 * <p>
 * 覆盖：同签名窗口内抑制、窗口过期重置、异签名互不影响、容量有界、null 兜底。
 * 时间通过 {@link #tryAcquire} 的 {@code nowMs} 参数注入推进，无需真实睡眠。
 */
@DisplayName("TraceLogThrottle Trace 失败日志窗口限频")
class TraceLogThrottleTest {

    private static final long WINDOW = 100L;

    @Nested
    @DisplayName("窗口内抑制")
    class WindowSuppression {

        @Test
        @DisplayName("同签名在窗口内仅放行 1 条，其余抑制并计数")
        void shouldSuppressRepeatedSameKeyWithinWindow() {
            final TraceLogThrottle throttle = new TraceLogThrottle(WINDOW, 16);

            assertThat(throttle.tryAcquire("job-a|ERROR|taskCall", 1_000)).isTrue();
            assertThat(throttle.tryAcquire("job-a|ERROR|taskCall", 1_020)).isFalse();
            assertThat(throttle.tryAcquire("job-a|ERROR|taskCall", 1_050)).isFalse();

            assertThat(throttle.suppressedCount()).isEqualTo(2);
            assertThat(throttle.trackedKeys()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("窗口重置")
    class WindowReset {

        @Test
        @DisplayName("窗口过期后同签名可再次放行，抑制计数持续累计")
        void shouldAllowAgainAfterWindowElapsed() {
            final TraceLogThrottle throttle = new TraceLogThrottle(WINDOW, 16);

            assertThat(throttle.tryAcquire("job-a|ERROR|taskCall", 1_000)).isTrue();
            assertThat(throttle.tryAcquire("job-a|ERROR|taskCall", 1_050)).isFalse();
            // 到达窗口边界 (now - last == window)：允许再次放行
            assertThat(throttle.tryAcquire("job-a|ERROR|taskCall", 1_100)).isTrue();
            // 新窗口内再次抑制
            assertThat(throttle.tryAcquire("job-a|ERROR|taskCall", 1_120)).isFalse();

            assertThat(throttle.suppressedCount()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("异签名隔离")
    class PerKeyIsolation {

        @Test
        @DisplayName("不同签名各自独立计时，互不抑制")
        void shouldTrackKeysIndependently() {
            final TraceLogThrottle throttle = new TraceLogThrottle(WINDOW, 16);

            assertThat(throttle.tryAcquire("job-a|ERROR|taskCall", 1_000)).isTrue();
            assertThat(throttle.tryAcquire("job-b|ERROR|taskCall", 1_000)).isTrue();
            // job-a 窗口内重复被抑制，不影响 job-b
            assertThat(throttle.tryAcquire("job-a|ERROR|taskCall", 1_010)).isFalse();

            assertThat(throttle.suppressedCount()).isEqualTo(1);
            assertThat(throttle.trackedKeys()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("容量有界与兜底")
    class BoundedCapacity {

        @Test
        @DisplayName("达到容量上限后淘汰最旧签名，跟踪数量收敛于上限，无外泄漏")
        void shouldCapTrackedKeysAtLimit() {
            final long longWindow = 10_000L;
            final TraceLogThrottle throttle = new TraceLogThrottle(longWindow, 2);

            throttle.tryAcquire("k1", 1_000);
            throttle.tryAcquire("k2", 2_000);
            throttle.tryAcquire("k3", 3_000);

            assertThat(throttle.trackedKeys()).isLessThanOrEqualTo(2);
            // 被淘汰签名再次出现时按新签名放行（有界缓存兜底，不丢观察）
            assertThat(throttle.tryAcquire("k3", 3_100)).isFalse();
            assertThat(throttle.tryAcquire("k4", 4_000)).isTrue();
            assertThat(throttle.trackedKeys()).isLessThanOrEqualTo(2);
        }

        @Test
        @DisplayName("null key 直接放行且不抑制、不占用跟踪")
        void shouldBypassForNullKey() {
            final TraceLogThrottle throttle = new TraceLogThrottle(WINDOW, 16);

            assertThat(throttle.tryAcquire(null, 1_000)).isTrue();
            assertThat(throttle.tryAcquire(null, 1_020)).isTrue();

            assertThat(throttle.suppressedCount()).isZero();
            assertThat(throttle.trackedKeys()).isZero();
        }
    }
}