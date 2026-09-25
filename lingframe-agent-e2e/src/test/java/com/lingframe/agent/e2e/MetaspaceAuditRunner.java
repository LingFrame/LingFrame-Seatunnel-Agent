package com.lingframe.agent.e2e;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Executes one complete Native or Agent Metaspace audit. */
final class MetaspaceAuditRunner {
    private static final Logger log = LoggerFactory.getLogger(MetaspaceAuditRunner.class);

    private MetaspaceAuditRunner() {
    }

    private static String formatMb(long bytes) {
        return String.format(java.util.Locale.ROOT, "%.2f MB", bytes / (1024.0 * 1024.0));
    }

    static MetaspaceAuditResult runTestMatrixAndAudit(
            String targetLabel,
            String restUrl,
            String containerName,
            List<String> matrixJobs,
            String topicPrefix, boolean classLoaderStatsEnabled) throws Exception {
        // 1. 基线采样：记录初始 Metaspace
        JvmDiagnostics.forceFullGcInContainer(containerName);
        final long baseline = JvmDiagnostics.getCurrentMetaspaceUsed(containerName);
        // 刚性断言：真实的 SeaTunnel JVM 启动后 Metaspace 必然大于 10MB（约 10,485,760 字节）
        // 绝不容许任何抓错进程、解析错误或 0 字节伪造
        assertThat(baseline)
                .as("[%s] Baseline Metaspace must be > 10MB (real SeaTunnel JVM required), actual: %d bytes (%s)",
                        targetLabel, baseline, formatMb(baseline))
                .isGreaterThan(10 * 1024 * 1024L);
        log.info("[{}] Metaspace audit started: baseline={} bytes ({})",
                targetLabel, baseline, formatMb(baseline));
        // 基线采样并行：classCounts 与 clstats 两个独立 docker exec 并行执行
        final CompletableFuture<long[]> baselineCountsFuture = CompletableFuture.supplyAsync(() ->
                JvmDiagnostics.captureClassCounts(containerName));
        final CompletableFuture<ClassLoaderStatsResult> baselineClStatsFuture = classLoaderStatsEnabled
                ? CompletableFuture.supplyAsync(() ->
                JvmDiagnostics.captureClassLoaderStats(containerName, targetLabel + "-baseline"))
                : CompletableFuture.completedFuture(new ClassLoaderStatsResult());
        final long[] baselineClassCounts;
        final ClassLoaderStatsResult baselineClStats;
        try {
            baselineClassCounts = baselineCountsFuture.get();
            baselineClStats = baselineClStatsFuture.get();
        } catch (Exception e) {
            throw new RuntimeException("Parallel baseline sampling failed", e);
        }

        // 2. 阶段一：执行全正交矩阵作业（含文件与 SQL transform 扩展链路，每组执行 serialRounds 轮）
        final int serialRounds = Integer.getInteger("lingframe.test.serial.rounds", 2);
        final List<Long> roundUsed = new ArrayList<>();
        final List<String> allJobIds = new ArrayList<>();
        for (int round = 1; round <= serialRounds; round++) {
            for (int i = 0; i < matrixJobs.size(); i++) {
                final String jobConfig = matrixJobs.get(i);
                final String jobTag = targetLabel + "-r" + round + "-j" + (i + 1);
                final String isolatedConfig = SeaTunnelJobMatrix.applyTopicPrefix(jobConfig, topicPrefix);
                final String jobId = SeaTunnelJobClient.submitJob(restUrl, jobTag, isolatedConfig);
                allJobIds.add(jobId);
                Thread.sleep(200);
            }
            JvmDiagnostics.forceFullGcInContainer(containerName);
            final long rUsed = JvmDiagnostics.getCurrentMetaspaceUsed(containerName);
            roundUsed.add(rUsed);
            log.info("[{}] Metaspace progression [Matrix Round {}/{}]: used={} bytes ({}), deltaFromBaseline={} bytes ({})",
                    targetLabel, round, serialRounds, rUsed, formatMb(rUsed), rUsed - baseline, formatMb(rUsed - baseline));
        }

        // 3. 阶段二：多 Job 异构并发压测（4 线程并发交错提交不同异构作业）
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            final List<CompletableFuture<String>> futures = new ArrayList<>();
            final int concurrentRounds = Integer.getInteger("lingframe.test.concurrent.rounds", 5);
            for (int r = 0; r < concurrentRounds; r++) {
                final int roundIndex = r;
                for (int j = 0; j < matrixJobs.size(); j++) {
                    final int jobIndex = j;
                    final String jobConfig = matrixJobs.get(j);
                    futures.add(CompletableFuture.supplyAsync(() -> {
                        try {
                            final String tag = targetLabel + "-c-r" + roundIndex + "-j" + jobIndex;
                            return SeaTunnelJobClient.submitJob(restUrl, tag, SeaTunnelJobMatrix.applyTopicPrefix(jobConfig, topicPrefix));
                        } catch (Exception e) {
                            throw new RuntimeException("Concurrent job failed", e);
                        }
                    }, executor));
                }
            }
            for (CompletableFuture<String> f : futures) {
                allJobIds.add(f.get(120, TimeUnit.SECONDS));
            }
        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        // 3.5 并发提交后等待 60s，让 SeaTunnel 先完成一批作业再开始验证
        // 避免大量作业同时运行压垮 REST 端点（Native 组无 ClassLoader 回收，重负载下 REST 卡死）
        log.info("[{}] Waiting 60s for SeaTunnel to process batch jobs before verification...", targetLabel);
        Thread.sleep(60000);

        // 4. 验证所有作业正常 FINISHED（排除崩溃假象——作业提交成功 HTTP 200 不等于执行完成）
        // 并发验证：多个作业的 REST GET 轮询无副作用，4 线程并行大幅缩减验证耗时
        log.info("[{}] Verifying {} submitted jobs reached FINISHED state (parallel, 2 threads)...",
                targetLabel, allJobIds.size());
        final ExecutorService verifyExecutor = Executors.newFixedThreadPool(2);
        try {
            final List<CompletableFuture<Void>> verifyFutures = new ArrayList<>();
            for (int i = 0; i < allJobIds.size(); i++) {
                final int idx = i;
                final String jobId = allJobIds.get(i);
                verifyFutures.add(CompletableFuture.supplyAsync(() -> {
                    try {
                        SeaTunnelJobClient.waitForJobFinished(restUrl, jobId, targetLabel + "-job-" + (idx + 1), 240, containerName);
                    } catch (IOException e) {
                        throw new RuntimeException("Job verification failed: " + jobId, e);
                    }
                    return null;
                }, verifyExecutor));
            }
            for (CompletableFuture<Void> vf : verifyFutures) {
                vf.get();
            }
        } finally {
            verifyExecutor.shutdown();
            verifyExecutor.awaitTermination(10, TimeUnit.SECONDS);
        }
        log.info("[{}] All {} jobs confirmed FINISHED.", targetLabel, allJobIds.size());

        // 5. 等待引擎内部异步清理（ClassLoader 卸载、Hazelcast IMap evict 等）
        // 所有作业已确认 FINISHED，此处仅需短暂等待引擎内部异步释放，10 秒足够
        Thread.sleep(10000);

        // 5.5 GC 前 clstats 采样（作业全部 FINISHED 后、Full GC 前）
        final ClassLoaderStatsResult preGcClStats = classLoaderStatsEnabled
                ? JvmDiagnostics.captureClassLoaderStats(containerName, targetLabel + "-pre-gc")
                : new ClassLoaderStatsResult();
        final long preGcTotal = preGcClStats.bootstrapClasses + preGcClStats.appClasses
                + preGcClStats.subClTotalClasses + preGcClStats.otherClasses;
        if (classLoaderStatsEnabled) {
            log.info("[{}] clstats pre-GC total classes: {} (bootstrap={}, AppCL={}, STCL={}, other={})",
                    targetLabel, preGcTotal, preGcClStats.bootstrapClasses, preGcClStats.appClasses,
                    preGcClStats.subClTotalClasses, preGcClStats.otherClasses);
        }

        // 5. 阶段三：强制 Full GC 并采样终态 Metaspace
        JvmDiagnostics.forceFullGcInContainer(containerName);
        final long finalUsed = JvmDiagnostics.getCurrentMetaspaceUsed(containerName);
        assertThat(finalUsed)
                .as("[%s] Final Metaspace must be > 10MB (real SeaTunnel JVM required), actual: %d bytes (%s)",
                        targetLabel, finalUsed, formatMb(finalUsed))
                .isGreaterThan(10 * 1024 * 1024L);
        final long growth = finalUsed - baseline;

        log.info("[{}] Metaspace progression [Concurrent Final]: used={} bytes ({}), deltaFromBaseline={} bytes ({})",
                targetLabel, finalUsed, formatMb(finalUsed), growth, formatMb(growth));

        final long[] finalClassCounts = JvmDiagnostics.captureClassCounts(containerName);
        final long classLoaderCount = JvmDiagnostics.countSeaTunnelClassLoaders(containerName);
        log.info("[{}] SeaTunnelChildFirstClassLoader live instances: {}",
                targetLabel, classLoaderCount);
        JvmDiagnostics.printClassCountDiff(targetLabel, baselineClassCounts, finalClassCounts, classLoaderCount);

        final long loadedDelta;
        final long unloadedDelta;
        final long retainedClasses;
        if (baselineClassCounts[0] < 0 || finalClassCounts[0] < 0) {
            loadedDelta = Long.MIN_VALUE;
            unloadedDelta = Long.MIN_VALUE;
            retainedClasses = Long.MIN_VALUE;
        } else {
            loadedDelta = finalClassCounts[0] - baselineClassCounts[0];
            unloadedDelta = finalClassCounts[1] - baselineClassCounts[1];
            retainedClasses = loadedDelta - unloadedDelta;
        }
        return new MetaspaceAuditResult(baseline, roundUsed, finalUsed,
                loadedDelta, unloadedDelta, retainedClasses, classLoaderCount, baselineClStats,
                preGcClStats, allJobIds.size());
    }
}
