package com.lingframe.agent.adapter;

import com.lingframe.core.ling.LingRuntimeConfig;
import com.lingframe.core.ling.VirtualLingManager;
import com.lingframe.core.metrics.MetricsCollector;
import com.lingframe.core.pipeline.InvocationPipelineEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 作业级虚拟灵元注册表（作业身份 → 灵元 一对一路由 + 生命周期管理）。
 * <p>
 * 「作业级治理」的核心记账结构：把治理身份从「单引擎灵元 seatunnel」（全作业共享限流/熔断，
 * 作业 A 故障放大到全引擎）收敛为「按 jobId 独立灵元 seatunnel-job-{jobId}」，使故障隔离单元 = 作业。
 * <p>
 * 职责：
 * <ul>
 *   <li><b>解析</b>{@link #resolveLingId(long, long)}：已跟踪作业返回 {code seatunnel-job-{jobId}}；
 *       新作业幂等注册（VirtualLingManager.register 天然幂等，收敛并发）；硬上限内回退返回 null（由调用方回退共享灵元）。</li>
 *   <li><b>回收</b>：挂靠 beforeTaskCall 的 <b>机会式</b> Reaper（零新增线程）——按墙上时钟节流扫描，
 *       TTL 到期作业经「unregister → evictLingResources → metricsCollector.remove」完整回收链清理，
 *       防灵元/熔断/限流/指标无限膨胀。</li>
 *   <li><b>硬上限</b>{@code maxTrackedJobs}：超限时新作业回退共享灵元 + 采样 WARN + 计数（MBean 可观测）。</li>
 * </ul>
 * <p>
 * 线程安全：记账用 {@link ConcurrentHashMap}；注册/回收的临界区用单一 {@link #reapLock} 串行化，
 * 避免「回收与注册同一 job」竞态（回收先从记账移除，重注册安全）。
 */
public final class JobLingRegistry {

    private static final Logger log = LoggerFactory.getLogger(JobLingRegistry.class);

    /** 作业灵元 ID 前缀。 */
    static final String JOB_LING_PREFIX = "seatunnel-job-";

    private final VirtualLingManager virtualLingManager;
    private final InvocationPipelineEngine pipelineEngine;
    private final MetricsCollector metricsCollector;
    private final LingRuntimeConfig jobLingTemplate;
    private final int maxTrackedJobs;
    private final long idleTtlMs;
    private final long reapIntervalMs;

    /** 记账：jobId → 最近活跃时间戳（写入时机见 resolve）。 */
    private final ConcurrentHashMap<Long, Long> activeJobs = new ConcurrentHashMap<>();
    /** 注册/回收临界区锁：串行化「注册新作业」与「Reaper 回收」，防同 job 竞态。 */
    private final Object reapLock = new Object();

    /** 硬上限回退计数（超限新作业被打回共享灵元的次数），供 MBean/测试断言。 */
    private final AtomicLong rejectCount = new AtomicLong();
    /** Reaper 节流游标（墙上时钟）。 */
    private volatile long lastReapAtMs;

    /**
     * @param virtualLingManager 灵元注册/注销入口（必须）
     * @param pipelineEngine     弹性资源驱逐入口（必须，unregister 后清熔断/限流缓存）
     * @param metricsCollector   健康指标注册表（必须，unregister 后清指标）
     * @param jobLingTemplate    作业灵元治理配置模板（复用 AgentConfig 限流/熔断/超时参数，各作业独立实例）
     * @param maxTrackedJobs     作业级灵元硬上限（超限新作业回退共享灵元）
     * @param idleTtlMs          空闲回收 TTL（毫秒）
     * @param reapIntervalMs     Reaper 节流间隔（毫秒）
     */
    public JobLingRegistry(VirtualLingManager virtualLingManager,
                           InvocationPipelineEngine pipelineEngine,
                           MetricsCollector metricsCollector,
                           LingRuntimeConfig jobLingTemplate,
                           int maxTrackedJobs,
                           long idleTtlMs,
                           long reapIntervalMs) {
        this.virtualLingManager = virtualLingManager;
        this.pipelineEngine = pipelineEngine;
        this.metricsCollector = metricsCollector;
        this.jobLingTemplate = jobLingTemplate == null ? LingRuntimeConfig.defaults() : jobLingTemplate;
        this.maxTrackedJobs = Math.max(1, maxTrackedJobs);
        this.idleTtlMs = Math.max(1_000L, idleTtlMs);
        this.reapIntervalMs = Math.max(1_000L, reapIntervalMs);
        this.lastReapAtMs = now();
    }

    /**
     * 解析作业灵元 ID，惰性注册并触发机会式 Reaper。
     *
     * @param jobId 作业 ID（合法为 ≥0 的 long；{@link JobIdExtractor#NO_JOB} 由调用方直接回退，不至此）
     * @param now   当前墙上时钟（毫秒）
     * @return 作业灵元 ID {@code seatunnel-job-{jobId}}；超限回退返回 null（调用方改用共享灵元）
     */
    public String resolveLingId(long jobId, long now) {
        final Long prev = activeJobs.putIfAbsent(jobId, now);
        if (prev != null) {
            // 已跟踪：热路径仅更新活跃时间（此处由调用方每批次 touch，见 touch()），直接返回
            return jobLingId(jobId);
        }
        // 新作业：进入临界区做硬上限检查 + 幂等注册（并发下同 job 多个线程同时到达，putIfAbsent 保证仅首个注册，其余转入上一分支）。
        // 注意 putIfAbsent 已把本作业计入 size，故硬上限判定用 size > max；超限时回滚该插入，避免污染记账。
        synchronized (reapLock) {
            if (activeJobs.size() > maxTrackedJobs) {
                activeJobs.remove(jobId);
                rejectCount.incrementAndGet();
                if (rejectCount.get() % WARN_SAMPLE == 1L) {
                    log.warn("Job-level tracking cap reached ({}), job [{}] falling back to shared ling", maxTrackedJobs, jobId);
                }
                return null;
            }
            registerJobLing(jobId);
        }
        maybeReap(now);
        return jobLingId(jobId);
    }

    /** 刷新作业活跃时间（beforeTaskCall 每批次调用，供 TTL 判定；null 安全）。 */
    public void touch(long jobId) {
        if (jobId < 0L) {
            return;
        }
        activeJobs.put(jobId, System.currentTimeMillis());
    }

    /** 当前跟踪的作业数（MBean/测试断言）。 */
    public int trackedCount() {
        return activeJobs.size();
    }

    /** 超限回退累计次数（MBean/测试断言）。 */
    public long rejectCount() {
        return rejectCount.get();
    }

    /* ==================== 内部 ==================== */

    private static final long WARN_SAMPLE = 1_000L;

    private String jobLingId(long jobId) {
        return JOB_LING_PREFIX + jobId;
    }

    private long now() {
        return System.currentTimeMillis();
    }

    /** 幂等注册作业灵元。register 对同 ID 自动更新配置并回 ACTIVE，故并发安全。 */
    private void registerJobLing(long jobId) {
        try {
            virtualLingManager.register(jobLingId(jobId), jobLingTemplate);
            log.debug("Registered job ling [{}]", jobLingId(jobId));
        } catch (Exception e) {
            log.warn("Failed to register job ling [{}], job falls back to shared ling",
                    jobLingId(jobId), e);
        }
    }

    /** 机会式 Reaper：挂靠调用方热路径，按墙上时钟节流触发，扫描 TTL 到期作业并完整回收。 */
    private void maybeReap(long now) {
        if (now - lastReapAtMs < reapIntervalMs) {
            return;
        }
        lastReapAtMs = now;
        reapIdleJobs(now);
    }

    private void reapIdleJobs(long now) {
        final long idleBefore = now - idleTtlMs;
        for (Long jobId : activeJobs.keySet()) {
            final Long lastActive = activeJobs.get(jobId);
            if (lastActive != null && lastActive < idleBefore) {
                synchronized (reapLock) {
                    // 二次校验：锁内确认仍是空闲（可能在锁外刚被 touch）
                    final Long cur = activeJobs.get(jobId);
                    if (cur != null && cur < idleBefore) {
                        activeJobs.remove(jobId);
                        recycleJobLing(jobId);
                    }
                }
            }
        }
    }

    /** 完整回收链：unregister（状态机+仓储）→ evict（熔断/限流缓存）→ remove（健康指标）。 */
    private void recycleJobLing(long jobId) {
        final String lingId = jobLingId(jobId);
        try {
            virtualLingManager.unregister(lingId);
        } catch (Exception e) {
            log.warn("Error unregistering job ling [{}]", lingId, e);
        }
        try {
            pipelineEngine.evictLingResources(lingId);
        } catch (Exception e) {
            log.warn("Error evicting job ling [{}] resilience resources", lingId, e);
        }
        try {
            metricsCollector.remove(lingId);
        } catch (Exception e) {
            log.warn("Error removing job ling [{}] metrics", lingId, e);
        }
        log.info("Reaped idle job ling [{}]", lingId);
    }
}