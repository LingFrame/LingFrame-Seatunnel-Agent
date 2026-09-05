package com.lingframe.agent.bridge;

import java.util.Collection;
import java.net.URL;

/**
 * 治理微内核抽象契约。
 * <p>
 * 该接口位于 Bootstrap ClassLoader，全 JVM 可见。
 * 参数使用 Object 而非 SeaTunnel 具体类型，因为 bridge 不能依赖 SeaTunnel 类。
 * 具体类型转换由 Agent core 的 adapter 层完成。
 */
public interface LingGovernanceContract {

    boolean isGovernanceEnabled();

    void onPhysicalRelease(ClassLoader classLoader);

    String convertJarsToKey(Collection<URL> jars);

    /**
     * Task.call() 批次调度前置治理。
     * <p>
     * 在 Worker 线程每次推进一个批次/时间片前调用。
     * 可在此检查熔断状态、做批次级反压降速。
     */
    void beforeTaskCall();

    /**
     * Task.call() 批次调度后置治理。
     * <p>
     * 在 Worker 线程每次推进一个批次/时间片后调用。
     * 可在此统计异常、更新熔断器滑动窗口。
     *
     * @param error Task.call() 抛出的异常，无异常时为 null
     */
    void afterTaskCall(Throwable error);
}