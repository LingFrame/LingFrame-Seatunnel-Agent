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
     *
     * @param task 本次执行的 SeaTunnel Task 宿主实例（{@code AbstractTask} 子类），用于解析作业级治理身份；
     *             采用 {@code Object} 而非具体类型，因为本契约位于 Bootstrap，不能直接依赖 SeaTunnel 类。
     *             解析失败或不支持作业级时，由 adapter 层回退共享灵元。
     */
    void beforeTaskCall(Object task);

    /**
     * Task.call() 批次调度后置治理。
     * <p>
     * 在 Worker 线程每次推进一个批次/时间片后调用。
     * 可在此统计异常、更新熔断器滑动窗口。
     *
     * @param task  本次执行的 SeaTunnel Task 宿主实例，语义同 {@link #beforeTaskCall(Object)}
     * @param error Task.call() 抛出的异常，无异常时为 null
     */
    void afterTaskCall(Object task, Throwable error);
}