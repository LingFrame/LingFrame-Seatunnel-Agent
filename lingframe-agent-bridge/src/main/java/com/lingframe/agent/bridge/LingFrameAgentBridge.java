package com.lingframe.agent.bridge;

import java.net.URL;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * 全局可见的静态桥接入口。
 * <p>
 * 该类位于 Bootstrap ClassLoader，持有 {@code volatile} 契约指针。
 * 被 ByteBuddy 增强后的 SeaTunnel 方法通过本类静态方法进行零反射直接分发，
 * 单次开销 &lt; 0.5ns。
 */
public final class LingFrameAgentBridge {

    private static volatile LingGovernanceContract contract;

    private LingFrameAgentBridge() {
    }

    /**
     * 注册治理契约实现（由 Agent premain 调用一次）。
     * <p>
     * <b>签名刻意不用 {@code LingGovernanceContract} 而是 {@code Object}</b>：
     * HotSpot 在宿主类（{@code LingFrameAgentPremain}，AppClassLoader 加载）的类加载验证期，
     * 会强解析所有 invoke 指令方法签名中出现的类型。若此处签名直接写契约接口，
     * premain 类验证时就会把 {@code LingGovernanceContract} 从 AppClassLoader 抢载——
     * 此刻 {@code appendToBootstrapClassLoaderSearch} 尚未执行，Bootstrap 中无此接口，
     * 于是产生 App 副本；运行期注入的 Bootstrap 副本与之形成双份同类，
     * 首个被织入的 {@code AbstractTask.call()} 执行时即抛
     * {@code LinkageError: loader constraint violation}（JMH 全治理基准实测复现）。
     * 改为 {@code Object} 后，类型校验/强转全部下沉到本类（Bootstrap 加载），
     * 契约接口只经 Bootstrap 单一副本解析。
     *
     * @param contract 契约实现（null 表示清空契约，Bridge 各入口退化为 no-op）
     * @throws IllegalArgumentException 传入未实现 {@link LingGovernanceContract} 的对象
     */
    public static void registerContract(Object contract) {
        if (contract != null && !(contract instanceof LingGovernanceContract)) {
            throw new IllegalArgumentException(
                    "contract must implement LingGovernanceContract, got: " + contract.getClass().getName());
        }
        LingFrameAgentBridge.contract = (LingGovernanceContract) contract;
    }

    public static LingGovernanceContract getContract() {
        return contract;
    }

    public static boolean isGovernanceEnabled() {
        final LingGovernanceContract c = contract;
        return c != null && c.isGovernanceEnabled();
    }

    public static void onPhysicalRelease(ClassLoader classLoader) {
        final LingGovernanceContract c = contract;
        if (c != null) {
            c.onPhysicalRelease(classLoader);
        }
    }

    public static String convertJarsToKey(Collection<URL> jars) {
        final LingGovernanceContract c = contract;
        return c != null ? c.convertJarsToKey(jars)
                // 兜底与官方 DefaultClassLoaderService.buildClassLoaderKey 保持严格一致
                : (jars == null || jars.isEmpty()) ? ""
                : jars.stream().map(URL::toString).sorted().collect(Collectors.joining());
    }

    /**
     * 前置治理分发（透传 Task 宿主，供 adapter 解析作业级治理身份）。
     * 契约不存在时 no-op，符合 premain fail-open 铁律。
     *
     * @param task 本次执行的 SeaTunnel Task 宿主实例
     */
    public static void beforeTaskCall(Object task) {
        final LingGovernanceContract c = contract;
        if (c != null) {
            c.beforeTaskCall(task);
        }
    }

    /**
     * 后置治理分发（透传 Task 宿主 + 异常）。
     *
     * @param task  本次执行的 SeaTunnel Task 宿主实例
     * @param error Task.call() 抛出的异常，无异常时为 null
     */
    public static void afterTaskCall(Object task, Throwable error) {
        final LingGovernanceContract c = contract;
        if (c != null) {
            c.afterTaskCall(task, error);
        }
    }
}