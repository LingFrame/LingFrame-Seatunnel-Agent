package com.lingframe.agent.bridge;

import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 已释放 ClassLoader 注册表。
 * <p>
 * 位于 Bridge 模块，通过 appendToBootstrap 注入 Bootstrap ClassLoader，
 * 使 TcclGuardAdvice 织入 Thread.setContextClassLoader 后能正确解析此类。
 * <p>
 * 使用 WeakHashMap 作为底层存储，ClassLoader 被释放且无其他强引用后，
 * 弱引用自动失效，可被 GC 安全回收，不阻塞 Metaspace 回收。
 * <p>
 * 线程安全与性能：{@code isReleased} 是 TcclGuardAdvice 的<b>热路径</b>——
 * SeaTunnel worker 每轮 task 调度都会 {@code setContextClassLoader}（TaskExecutionService
 * 的 call() 循环内），若用 {@code synchronizedSet} 会让所有线程的 TCCL 设置撞同一把全局锁。
 * 因此采用 {@link ReadWriteLock}：读路径（isReleased，高频）用读锁（无竞争时近似无锁），
 * 写路径（register，低频，仅在 ClassLoader 释放时）用写锁。
 */
public final class ReleasedClassLoaderRegistry {

    private static final Set<ClassLoader> RELEASED =
            Collections.newSetFromMap(new WeakHashMap<>());
    private static final ReadWriteLock LOCK = new ReentrantReadWriteLock();

    private ReleasedClassLoaderRegistry() {
    }

    /**
     * 注册已释放的 ClassLoader。
     *
     * @param classLoader 已释放的 ClassLoader
     */
    public static void register(ClassLoader classLoader) {
        if (classLoader == null) {
            return;
        }
        LOCK.writeLock().lock();
        try {
            RELEASED.add(classLoader);
        } finally {
            LOCK.writeLock().unlock();
        }
    }

    /**
     * 检查 ClassLoader 是否已释放。
     * <p>
     * 热路径：TcclGuardAdvice 每次 {@code Thread.setContextClassLoader} 调用一次。
     * 读锁在无写者竞争时开销极小（ReentrantReadWriteLock 读锁为 CAS 状态位）。
     *
     * @param classLoader 待检查的 ClassLoader
     * @return 已释放返回 true，否则 false
     */
    public static boolean isReleased(ClassLoader classLoader) {
        if (classLoader == null) {
            return false;
        }
        LOCK.readLock().lock();
        try {
            return RELEASED.contains(classLoader);
        } finally {
            LOCK.readLock().unlock();
        }
    }

    /**
     * 获取已注册的已释放 ClassLoader 数量。
     *
     * @return 已释放 ClassLoader 数量
     */
    public static int size() {
        LOCK.readLock().lock();
        try {
            return RELEASED.size();
        } finally {
            LOCK.readLock().unlock();
        }
    }

    /**
     * 清空注册表（仅供测试使用）。
     */
    public static void clear() {
        LOCK.writeLock().lock();
        try {
            RELEASED.clear();
        } finally {
            LOCK.writeLock().unlock();
        }
    }
}
