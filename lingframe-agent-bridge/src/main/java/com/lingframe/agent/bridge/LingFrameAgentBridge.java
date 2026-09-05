package com.lingframe.agent.bridge;

import java.util.Collection;
import java.net.URL;

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

    public static void registerContract(LingGovernanceContract c) {
        contract = c;
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
                : jars.stream().map(URL::toString).sorted().reduce((a, b) -> a + b).orElse("");
    }

    public static void beforeTaskCall() {
        final LingGovernanceContract c = contract;
        if (c != null) {
            c.beforeTaskCall();
        }
    }

    public static void afterTaskCall(Throwable error) {
        final LingGovernanceContract c = contract;
        if (c != null) {
            c.afterTaskCall(error);
        }
    }
}