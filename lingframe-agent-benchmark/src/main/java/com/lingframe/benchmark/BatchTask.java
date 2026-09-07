package com.lingframe.benchmark;

import org.apache.seatunnel.engine.server.execution.ProgressState;

/**
 * 批次任务的公共测量视图：基准只通过本接口触碰两条路径，
 * 保证 {@code call()} 经同一虚分派调用，差异只剩「是否被 Agent 织入」。
 * <p>
 * 注意：本接口刻意不继承任何 SeaTunnel 类型，避免自身落入 ByteBuddy 织入范围。
 */
public interface BatchTask {

    /** 执行一批处理（方法体由实现类转调 {@link BatchCore#process}）。 */
    ProgressState call();

    /** 最近一次 {@link #call()} 的变换校验和（防消除的可观测出口，由基准消费）。 */
    long getLastChecksum();
}
