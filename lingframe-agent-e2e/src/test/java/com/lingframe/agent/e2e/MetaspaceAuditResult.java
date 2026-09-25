package com.lingframe.agent.e2e;

import java.util.List;

/** Immutable result of one Native or Agent Metaspace audit. */
final class MetaspaceAuditResult {
    private final long baseline;
    private final List<Long> roundUsed;
    private final long finalUsed;
    private final long netGrowth;
    private final long loadedDelta;
    private final long unloadedDelta;
    private final long retainedClasses;
    private final long classLoaderCount;
    private final ClassLoaderStatsResult baselineClStats;
    private final ClassLoaderStatsResult preGcClStats;
    private final int totalJobExecutions;

    MetaspaceAuditResult(long baseline, List<Long> roundUsed, long finalUsed,
                         long loadedDelta, long unloadedDelta, long retainedClasses,
                         long classLoaderCount, ClassLoaderStatsResult baselineClStats,
                         ClassLoaderStatsResult preGcClStats, int totalJobExecutions) {
        this.baseline = baseline;
        this.roundUsed = roundUsed;
        this.finalUsed = finalUsed;
        this.netGrowth = finalUsed - baseline;
        this.loadedDelta = loadedDelta;
        this.unloadedDelta = unloadedDelta;
        this.retainedClasses = retainedClasses;
        this.classLoaderCount = classLoaderCount;
        this.baselineClStats = baselineClStats;
        this.preGcClStats = preGcClStats;
        this.totalJobExecutions = totalJobExecutions;
    }

    long getBaseline() { return baseline; }
    List<Long> getRoundUsed() { return roundUsed; }
    long getFinalUsed() { return finalUsed; }
    long getNetGrowth() { return netGrowth; }
    long getLoadedDelta() { return loadedDelta; }
    long getUnloadedDelta() { return unloadedDelta; }
    long getRetainedClasses() { return retainedClasses; }
    long getClassLoaderCount() { return classLoaderCount; }
    ClassLoaderStatsResult getBaselineClStats() { return baselineClStats; }
    ClassLoaderStatsResult getPreGcClStats() { return preGcClStats; }
    int getTotalJobExecutions() { return totalJobExecutions; }
}
