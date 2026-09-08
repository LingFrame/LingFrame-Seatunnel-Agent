package com.lingframe.agent.hook;

import com.lingframe.core.resource.ThreadReferenceUnloadHook;
import com.lingframe.core.spi.LingUnloadHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 引擎级线程引用安全卸载钩子。
 * <p>
 * 包装灵核底座的 {@link ThreadReferenceUnloadHook}，提供针对宿主分布式计算引擎（如 SeaTunnel）
 * 线程池的免疫保护：
 * <ul>
 *   <li><b>源头阻断误杀</b>：SeaTunnel 的 {@code TaskExecutionService} 内部 Worker 线程
 *       （{@code BlockingWorker-...}）在执行任务时会被临时设置 Connector 的 ClassLoader 作为 TCCL，
 *       但在任务结束后未被引擎重置回系统 ClassLoader。若直接交由底座的 {@code ThreadReferenceUnloadHook}
 *       处理，其孤儿线程池探测逻辑会反射追溯该 Worker 的外层线程池，误将宿主引擎的核心工作线程池
 *       识别为“孤儿线程池”并强行调用 {@code pool.shutdownNow()}，导致引擎直接瘫痪。</li>
 *   <li><b>前置 TCCL 净化</b>：在底座 Hook 触发线程扫描前，同步将所有活动线程中残留的
 *       目标 ClassLoader 替换为 {@link ClassLoader#getSystemClassLoader()}。
 *       底座在探测活动线程时，由于所有宿主线程的 {@code contextClassLoader} 与其定义类加载器
 *       均不再等于目标 ClassLoader，直接短路跳过，从根本上杜绝误杀宿主线程池。</li>
 *   <li><b>完整保留 JVM 清理能力</b>：底座针对 MySQL 清理线程、Timer 线程、HttpClient 守护线程、
 *       ThreadLocal 强引用等深度 JVM 级清理能力全部正常工作。</li>
 * </ul>
 */
public final class EngineSafeThreadReferenceUnloadHook implements LingUnloadHook {

    private static final Logger log = LoggerFactory.getLogger(EngineSafeThreadReferenceUnloadHook.class);

    private final ThreadReferenceUnloadHook delegate;

    public EngineSafeThreadReferenceUnloadHook() {
        this(new ThreadReferenceUnloadHook());
    }

    EngineSafeThreadReferenceUnloadHook(ThreadReferenceUnloadHook delegate) {
        this.delegate = delegate;
    }

    @Override
    public void cleanup(String lingId, ClassLoader classLoader) {
        if (classLoader != null) {
            resetThreadContextClassLoaders(lingId, classLoader);
        }
        if (delegate != null) {
            delegate.cleanup(lingId, classLoader);
        }
    }

    @Override
    public void shutdown() {
        if (delegate != null) {
            delegate.shutdown();
        }
    }

    /**
     * 同步扫描当前 JVM 活动线程，将关联目标 ClassLoader 的 TCCL 重置为 SystemClassLoader。
     *
     * @param lingId      灵元标识
     * @param classLoader 待卸载的类加载器
     */
    public static void resetThreadContextClassLoaders(String lingId, ClassLoader classLoader) {
        if (classLoader == null) {
            return;
        }
        try {
            final ClassLoader systemCl = ClassLoader.getSystemClassLoader();
            final Map<Thread, StackTraceElement[]> threads = Thread.getAllStackTraces();
            int resetCount = 0;
            for (Thread t : threads.keySet()) {
                if (t != null && t.getContextClassLoader() == classLoader) {
                    try {
                        t.setContextClassLoader(systemCl);
                        resetCount++;
                    } catch (Throwable t2) {
                        log.debug("[{}] Failed to reset TCCL for thread {}: {}",
                                lingId, t.getName(), t2.getMessage());
                    }
                }
            }
            if (resetCount > 0) {
                log.info("[{}] Reset TCCL to SystemClassLoader for {} thread(s) to protect host thread pool",
                        lingId, resetCount);
            }
        } catch (Throwable t) {
            log.warn("[{}] Failed to scan and reset thread context ClassLoaders: {}", lingId, t.getMessage());
        }
    }
}
