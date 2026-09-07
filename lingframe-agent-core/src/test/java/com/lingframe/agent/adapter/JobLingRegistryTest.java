package com.lingframe.agent.adapter;

import com.lingframe.core.event.EventBus;
import com.lingframe.core.fsm.RuntimeCoordinator;
import com.lingframe.core.ling.DefaultLingRepository;
import com.lingframe.core.ling.LingRepository;
import com.lingframe.core.ling.LingRuntimeConfig;
import com.lingframe.core.ling.VirtualLingManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * JobLingRegistry 单元测试。
 * <p>
 * 覆盖记账、硬上限回退与 TTL 空闲回收三大职责：
 * 作业灵元按 jobId 一对一路由、超限新作业打回共享灵元、空闲作业经 Reaper 完整回收零残留。
 */
@DisplayName("JobLingRegistry 作业灵元注册表测试")
class JobLingRegistryTest {

    private EventBus eventBus;
    private RuntimeCoordinator coordinator;
    private LingRepository repository;
    private VirtualLingManager virtualLingManager;

    @BeforeEach
    void setUp() {
        eventBus = new EventBus();
        coordinator = new RuntimeCoordinator(eventBus);
        coordinator.start();
        repository = new DefaultLingRepository();
        virtualLingManager = new VirtualLingManager(repository, coordinator, eventBus);
    }

    @AfterEach
    void tearDown() {
        coordinator.stop();
    }

    private LingRuntimeConfig template(int rateLimit) {
        return LingRuntimeConfig.builder().rateLimitPerSecond(rateLimit).build();
    }

    private JobLingRegistry registry(int maxTracked, long idleTtlMs, long reapIntervalMs) {
        return new JobLingRegistry(virtualLingManager, null, null, template(1001), maxTracked, idleTtlMs, reapIntervalMs);
    }

    @Nested
    @DisplayName("作业记账与路由")
    class TrackingAndRouting {

        @Test
        @DisplayName("新作业应惰性注册 seatunnel-job-{jobId} 虚灵元并施加治理模板")
        void shouldRegisterNewJobLing() {
            final JobLingRegistry registry = registry(1024, 1800, 300);
            final String id = registry.resolveLingId(7L, 1_000L);

            assertThat(id).isEqualTo("seatunnel-job-7");
            assertThat(registry.trackedCount()).isEqualTo(1);
            assertThat(virtualLingManager.hasRuntime("seatunnel-job-7")).isTrue();
            assertThat(virtualLingManager.getRuntime("seatunnel-job-7").getConfig().getRateLimitPerSecond())
                    .isEqualTo(1001);
        }

        @Test
        @DisplayName("同一作业重复解析应返回稳定灵元 ID，不重复注册")
        void shouldReturnStableIdForSameJob() {
            final JobLingRegistry registry = registry(1024, 1800, 300);
            final String first = registry.resolveLingId(5L, 1_000L);
            final String second = registry.resolveLingId(5L, 2_000L);

            assertThat(first).isEqualTo("seatunnel-job-5").isEqualTo(second);
            assertThat(registry.trackedCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("不同作业应各自路由到独立灵元")
        void shouldRouteToIndependentLingsPerJob() {
            final JobLingRegistry registry = registry(1024, 1800, 300);
            registry.resolveLingId(1L, 1_000L);
            registry.resolveLingId(2L, 1_000L);

            assertThat(registry.trackedCount()).isEqualTo(2);
            assertThat(virtualLingManager.hasRuntime("seatunnel-job-1")).isTrue();
            assertThat(virtualLingManager.hasRuntime("seatunnel-job-2")).isTrue();
        }
    }

    @Nested
    @DisplayName("硬上限回退")
    class CapFallback {

        @Test
        @DisplayName("超限新作业应回退共享灵元（返回 null）并累计 reject 计数")
        void shouldRejectWhenCapReached() {
            final JobLingRegistry registry = registry(2, 1800, 300);
            assertThat(registry.resolveLingId(1L, 1_000L)).isEqualTo("seatunnel-job-1");
            assertThat(registry.resolveLingId(2L, 1_000L)).isEqualTo("seatunnel-job-2");
            // 硬上限=2，第三个新作业打回共享灵元
            assertThat(registry.resolveLingId(3L, 1_000L)).isNull();
            assertThat(registry.rejectCount()).isEqualTo(1);
            assertThat(registry.trackedCount()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("TTL 空闲回收")
    class IdleReap {

        @Test
        @DisplayName("空闲作业应在新作业触发 Reaper 时完整回收，零残留")
        void shouldReapIdleJobWhenReaperTriggered() {
            final JobLingRegistry registry = registry(1024, 1_000L, 1_000L);
            // 基准锚定时钟
            final long base = System.currentTimeMillis();
            // 作业 A 在 t0 注册，此后空闲
            registry.resolveLingId(10L, base);

            assertThat(virtualLingManager.hasRuntime("seatunnel-job-10")).isTrue();

            // 远超 idleTtlMs / reapIntervalMs 后，新作业 C 触发 Reaper，回收空闲作业 A
            final long t3 = base + 5_000L;
            registry.resolveLingId(30L, t3);

            assertThat(virtualLingManager.hasRuntime("seatunnel-job-10")).isFalse();
            assertThat(registry.trackedCount()).isEqualTo(1); // 仅剩新作业 C
        }

        @Test
        @DisplayName("活跃作业不应被 Reaper 误回收")
        void shouldNotReapActiveJob() {
            final JobLingRegistry registry = registry(1024, 1_000L, 1_000L);
            final long base = System.currentTimeMillis();
            // 作业 A 空闲、作业 B 在回收窗口内活跃，新作业 C 触发 Reaper
            registry.resolveLingId(10L, base);
            registry.resolveLingId(20L, base + 4_500L); // B 最近活跃于 base+4500

            final long t4 = base + 5_000L;
            registry.resolveLingId(30L, t4); // 新作业 C，触发 Reaper

            // 回收窗口 idleBefore = t4 - 1000 = base+4000：
            //   A 活跃于 base（< base+4000）→ 应被回收
            //   B 活跃于 base+4500（> base+4000）→ 不应被回收
            assertThat(virtualLingManager.hasRuntime("seatunnel-job-20")).isTrue();
            assertThat(registry.trackedCount()).isEqualTo(2); // B + C
        }
    }
}