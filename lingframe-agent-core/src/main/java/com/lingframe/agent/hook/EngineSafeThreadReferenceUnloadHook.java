package com.lingframe.agent.hook;

import com.lingframe.core.resource.ThreadReferenceUnloadHook;
import com.lingframe.core.spi.LingUnloadHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.Reference;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * 引擎级线程引用安全卸载钩子。
 * <p>
 * 包装灵核底座的 {@link ThreadReferenceUnloadHook}，提供针对宿主分布式计算引擎（如 SeaTunnel）
 * 线程池的免疫保护与深度 ThreadLocal 斩断：
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
 *   <li><b>常驻线程 ThreadLocal 深度排空</b>：底座默认对跨线程 ThreadLocal 采取保守的不侵入策略；
 *       针对引擎常驻 Worker 与 Hazelcast 线程池，本 Hook 深度扫描并清空其中关联目标 ClassLoader 的条目，
 *       彻底斩断常驻线程指向 Connector 类的 GC Root。</li>
 * </ul>
 */
public final class EngineSafeThreadReferenceUnloadHook implements LingUnloadHook {

    private static final Logger log = LoggerFactory.getLogger(EngineSafeThreadReferenceUnloadHook.class);

    private static final Field THREAD_LOCALS_FIELD;
    private static final Field INHERITABLE_THREAD_LOCALS_FIELD;
    private static final Field TLM_TABLE_FIELD;
    private static final Field TLM_ENTRY_VALUE_FIELD;

    static {
        Field tlField = null;
        Field itlField = null;
        Field tableField = null;
        Field valueField = null;
        try {
            tlField = Thread.class.getDeclaredField("threadLocals");
            setAccessibleSafely(tlField);
        } catch (Throwable t) {
            log.debug("Thread.threadLocals not accessible: {}", t.getMessage());
        }
        try {
            itlField = Thread.class.getDeclaredField("inheritableThreadLocals");
            setAccessibleSafely(itlField);
        } catch (Throwable t) {
            log.debug("Thread.inheritableThreadLocals not accessible: {}", t.getMessage());
        }
        try {
            final Class<?> tlmClass = Class.forName("java.lang.ThreadLocal$ThreadLocalMap");
            tableField = tlmClass.getDeclaredField("table");
            setAccessibleSafely(tableField);
        } catch (Throwable t) {
            log.debug("ThreadLocalMap.table not accessible: {}", t.getMessage());
        }
        try {
            final Class<?> entryClass = Class.forName("java.lang.ThreadLocal$ThreadLocalMap$Entry");
            valueField = entryClass.getDeclaredField("value");
            setAccessibleSafely(valueField);
        } catch (Throwable t) {
            log.debug("ThreadLocalMap$Entry.value not accessible: {}", t.getMessage());
        }
        THREAD_LOCALS_FIELD = tlField;
        INHERITABLE_THREAD_LOCALS_FIELD = itlField;
        TLM_TABLE_FIELD = tableField;
        TLM_ENTRY_VALUE_FIELD = valueField;
    }

    /** 安全地在 doPrivileged 块中放宽反射对象的访问权限。 */
    private static void setAccessibleSafely(final AccessibleObject accessibleObject) {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            accessibleObject.setAccessible(true);
            return null;
        });
    }

    private final ThreadReferenceUnloadHook delegate;

    public EngineSafeThreadReferenceUnloadHook() {
        this(new ThreadReferenceUnloadHook());
    }

    EngineSafeThreadReferenceUnloadHook(ThreadReferenceUnloadHook delegate) {
        this.delegate = delegate;
    }

    @Override
    public void cleanup(String lingId, ClassLoader classLoader) {
        if (delegate != null) {
            delegate.cleanup(lingId, classLoader);
        }
        if (classLoader != null) {
            resetThreadContextClassLoaders(lingId, classLoader);
            drainWorkerThreadLocals(lingId, classLoader);
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

    /**
     * 深度排空引擎常驻工作线程与 Hazelcast 线程中的 ThreadLocal 引用。
     *
     * @param lingId      灵元标识
     * @param classLoader 待卸载的类加载器
     */
    private static void drainWorkerThreadLocals(String lingId, ClassLoader classLoader) {
        if (classLoader == null || TLM_TABLE_FIELD == null || TLM_ENTRY_VALUE_FIELD == null) {
            return;
        }
        int totalCleared = 0;
        try {
            final Map<Thread, StackTraceElement[]> threads = Thread.getAllStackTraces();
            for (Thread t : threads.keySet()) {
                if (t == null) {
                    continue;
                }
                totalCleared += cleanThreadLocalMap(t, THREAD_LOCALS_FIELD, classLoader);
                totalCleared += cleanThreadLocalMap(t, INHERITABLE_THREAD_LOCALS_FIELD, classLoader);
            }
            if (totalCleared > 0) {
                log.info("[{}] Deep drained {} ThreadLocal entries referencing target ClassLoader from worker threads",
                        lingId, totalCleared);
            }
        } catch (Throwable t) {
            log.warn("[{}] Failed to drain worker ThreadLocals: {}", lingId, t.getMessage());
        }
    }

    private static int cleanThreadLocalMap(Thread t, Field mapField, ClassLoader cl) {
        if (mapField == null) {
            return 0;
        }
        int count = 0;
        try {
            final Object map = mapField.get(t);
            if (map == null) {
                return 0;
            }
            final Object[] table = (Object[]) TLM_TABLE_FIELD.get(map);
            if (table == null) {
                return 0;
            }
            for (Object entry : table) {
                if (entry == null) {
                    continue;
                }
                final Reference<?> ref = (Reference<?>) entry;
                final Object key = ref.get();
                final Object val = TLM_ENTRY_VALUE_FIELD.get(entry);

                if (isClassLoaderRelated(key, cl, 2, new IdentityHashMap<>())
                        || isClassLoaderRelated(val, cl, 2, new IdentityHashMap<>())) {
                    TLM_ENTRY_VALUE_FIELD.set(entry, null);
                    ref.clear();
                    count++;
                }
            }
        } catch (Throwable t2) {
            log.debug("ThreadLocal map clean failed: {}", t2.getMessage());
        }
        return count;
    }

    private static boolean isClassLoaderRelated(Object obj, ClassLoader cl, int depth,
                                                IdentityHashMap<Object, Boolean> visited) {
        if (obj == null || cl == null || depth < 0) {
            return false;
        }
        if (obj instanceof Class) {
            return ((Class<?>) obj).getClassLoader() == cl;
        }
        if (obj instanceof ClassLoader) {
            return obj == cl;
        }
        if (obj.getClass().getClassLoader() == cl) {
            return true;
        }
        if (visited.put(obj, Boolean.TRUE) != null) {
            return false;
        }
        if (depth == 0) {
            return false;
        }

        if (obj instanceof Reference) {
            final Object referent = ((Reference<?>) obj).get();
            if (referent != null && isClassLoaderRelated(referent, cl, depth - 1, visited)) {
                return true;
            }
        }
        if (obj instanceof Iterable) {
            try {
                for (Object item : (Iterable<?>) obj) {
                    if (isClassLoaderRelated(item, cl, depth - 1, visited)) {
                        return true;
                    }
                }
            } catch (Throwable t) {
                log.debug("Iterable traversal in isClassLoaderRelated failed: {}", t.getMessage());
            }
        }
        if (obj instanceof Map) {
            try {
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) obj).entrySet()) {
                    if (isClassLoaderRelated(entry.getKey(), cl, depth - 1, visited)
                            || isClassLoaderRelated(entry.getValue(), cl, depth - 1, visited)) {
                        return true;
                    }
                }
            } catch (Throwable t) {
                log.debug("Map traversal in isClassLoaderRelated failed: {}", t.getMessage());
            }
        }
        if (obj.getClass().isArray() && !obj.getClass().getComponentType().isPrimitive()) {
            try {
                final int len = Array.getLength(obj);
                for (int i = 0; i < len; i++) {
                    if (isClassLoaderRelated(Array.get(obj, i), cl, depth - 1, visited)) {
                        return true;
                    }
                }
            } catch (Throwable t) {
                log.debug("Array traversal in isClassLoaderRelated failed: {}", t.getMessage());
            }
        }
        return false;
    }
}
