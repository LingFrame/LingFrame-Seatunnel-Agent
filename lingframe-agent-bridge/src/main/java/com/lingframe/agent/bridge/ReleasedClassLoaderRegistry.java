package com.lingframe.agent.bridge;

import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * 已释放 ClassLoader 注册表。
 * <p>
 * 位于 Bridge 模块，通过 appendToBootstrap 注入 Bootstrap ClassLoader，
 * 使 TcclGuardAdvice 织入 Thread.setContextClassLoader 后能正确解析此类。
 * <p>
 * 使用 WeakHashMap 作为底层存储，ClassLoader 被释放且无其他强引用后，
 * 弱引用自动失效，可被 GC 安全回收，不阻塞 Metaspace 回收。
 * <p>
 * 线程安全：Collections.synchronizedSet 包装提供外部同步。
 * Thread.setContextClassLoader 调用频率不高（主要在线程池初始化时），
 * synchronizedSet 的性能开销可接受。
 */
public final class ReleasedClassLoaderRegistry {

    private static final Set<ClassLoader> RELEASED =
            Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

    private ReleasedClassLoaderRegistry() {
    }

    /**
     * 注册已释放的 ClassLoader。
     *
     * @param classLoader 已释放的 ClassLoader
     */
    public static void register(ClassLoader classLoader) {
        if (classLoader != null) {
            RELEASED.add(classLoader);
        }
    }

    /**
     * 检查 ClassLoader 是否已释放。
     *
     * @param classLoader 待检查的 ClassLoader
     * @return 已释放返回 true，否则 false
     */
    public static boolean isReleased(ClassLoader classLoader) {
        return classLoader != null && RELEASED.contains(classLoader);
    }

    /**
     * 获取已注册的已释放 ClassLoader 数量。
     *
     * @return 已释放 ClassLoader 数量
     */
    public static int size() {
        return RELEASED.size();
    }

    /**
     * 清空注册表（仅供测试使用）。
     */
    public static void clear() {
        RELEASED.clear();
    }
}