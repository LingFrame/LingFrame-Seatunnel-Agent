package com.lingframe.agent.cleaner;

import com.lingframe.agent.bridge.LingFrameAgentBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 引擎 ClassLoader 清理器——Metaspace 泄漏根因治理。
 * <p>
 * 双端治理模型：
 * <ul>
 *   <li><b>Worker 节点</b>：缓存 {@code TaskExecutionService} 实例，周期性将
 *       {@code finishedExecutionContexts} 中已完成的 {@code TaskGroupContext} 深度断引用
 *       （清空 {@code classLoaders}/{@code jars}，置空 {@code taskGroup}），并显式
 *       从 Map 中物理移除（{@code iterator.remove()}）。</li>
 *   <li><b>Coordinator / Worker 共享缓存兜底</b>：缓存 {@code DefaultClassLoaderService}
 *       单例实例。针对因异常中断导致引用计数未对称归零而滞留在 {@code classLoaderCache} 中的
 *       {@code SeaTunnelChildFirstClassLoader}，在作业终态与上下文清理时强制剥离（{@code remove(jobId)}），
 *       并主动投递给 {@link LingFrameAgentBridge#onPhysicalRelease(ClassLoader)} 执行底座卸载钩子。</li>
 * </ul>
 * 全程反射保护、全枚举字段名均按引擎源码校准，任一阶段失败都降级跳过并告警，绝不破坏引擎主流程。
 */
public final class EngineClassLoaderCleaner {

    private static final Logger log = LoggerFactory.getLogger(EngineClassLoaderCleaner.class);

    /** 引擎字段名（与 SeaTunnel 2.3.13 源码对齐） */
    private static final String FIELD_FINISHED = "finishedExecutionContexts";
    private static final String FIELD_CLASS_LOADERS = "classLoaders";
    private static final String FIELD_JARS = "jars";
    private static final String FIELD_TASK_GROUP = "taskGroup";
    private static final String FIELD_CACHE_MODE = "cacheMode";
    private static final String FIELD_CLASS_LOADER_CACHE = "classLoaderCache";
    private static final String FIELD_REF_COUNT = "classLoaderReferenceCount";
    private static final String FIELD_JOB_ID = "jobId";

    /** 引擎 TaskExecutionService 实例（由 advice 捕获，Worker 单例） */
    private static volatile Object taskExecutionService;

    /** 引擎 DefaultClassLoaderService 实例（由 advice 捕获，单例） */
    private static volatile Object classLoaderService;

    /** 累计清理数——写入日志供观测 Metaspace 治理是否生效 */
    private static final AtomicLong CLEANED_COUNT = new AtomicLong();

    private EngineClassLoaderCleaner() {
    }

    /**
     * 捕获引擎 {@code TaskExecutionService} 实例（线程安全，once-only）。
     *
     * @param instance 织入点注入的 this
     */
    public static void register(Object instance) {
        if (instance != null && taskExecutionService == null) {
            taskExecutionService = instance;
            log.info("EngineClassLoaderCleaner captured TaskExecutionService instance");
        }
    }

    /**
     * 捕获引擎 {@code DefaultClassLoaderService} 实例（线程安全，once-only）。
     *
     * @param instance 织入点注入的 this
     */
    public static void registerClassLoaderService(Object instance) {
        if (instance != null && classLoaderService == null) {
            classLoaderService = instance;
            log.info("EngineClassLoaderCleaner captured DefaultClassLoaderService instance");
        }
    }

    /**
     * 清空并移除引擎已完成作业上下文中的 ClassLoader / jars / taskGroup 强引用。
     * <p>
     * 幂等：直接从 {@code finishedExecutionContexts} 中移除已完成条目，断绝引擎内部堆积；
     * 并联动排空对应作业在 {@code DefaultClassLoaderService} 中可能滞留的 ClassLoader 缓存。
     */
    public static void cleanFinished() {
        final Object target = taskExecutionService;
        if (target == null) {
            return;
        }
        try {
            final Object value = readFieldValue(target, FIELD_FINISHED);
            if (!(value instanceof Map)) {
                return;
            }
            final Map<?, ?> finished = (Map<?, ?>) value;
            if (finished.isEmpty()) {
                return;
            }
            int cleaned = 0;
            final Set<Long> completedJobIds = new HashSet<>();
            final Iterator<? extends Map.Entry<?, ?>> iterator = finished.entrySet().iterator();
            while (iterator.hasNext()) {
                final Map.Entry<?, ?> entry = iterator.next();
                final Object key = entry.getKey();
                if (key != null) {
                    final long jobId = extractJobId(key);
                    if (jobId > 0) {
                        completedJobIds.add(jobId);
                    }
                }
                final Object ctx = entry.getValue();
                if (ctx != null) {
                    cleanContextFields(ctx);
                }
                iterator.remove();
                cleaned++;
            }
            if (cleaned > 0) {
                final long total = CLEANED_COUNT.addAndGet(cleaned);
                log.info("EngineClassLoaderCleaner purged {} finished task-context entries (total: {}, remaining: {})",
                        cleaned, total, finished.size());
            }
            // 联动强制排空已完成作业在 DefaultClassLoaderService 中可能异常残留的 ClassLoader 缓存
            for (Long jobId : completedJobIds) {
                forceEvictJobClassLoaders(jobId);
            }
        } catch (Throwable t) {
            log.warn("EngineClassLoaderCleaner finished-context clean skipped (engine version mismatch?): {}",
                    t.getMessage());
        }
    }

    /**
     * 针对指定作业，强制从 {@code DefaultClassLoaderService.classLoaderCache} 中剥离残留项。
     * <p>
     * 防御场景：若作业在 Coordinator 端（DAG 解析/物理计划构建）或 Worker 调度执行中发生未捕获异常、
     * 或引用计数未对称归零，引擎自带的 releaseClassLoader 会静默跳过物理 remove，
     * 导致 classLoaderCache 永久强持有 SeaTunnelChildFirstClassLoader。
     * 本方法在作业完成/清理时强制介入，彻底斩断缓存持有的 GC Root 并触发底座物理卸载。
     *
     * @param jobId 作业标识
     */
    public static void forceEvictJobClassLoaders(long jobId) {
        final Object cls = classLoaderService;
        if (cls == null || jobId <= 0) {
            return;
        }
        try {
            final Object cacheModeVal = readFieldValue(cls, FIELD_CACHE_MODE);
            if (Boolean.TRUE.equals(cacheModeVal)) {
                // 共享缓存模式下 ClassLoader 被多作业复用，不能按作业剥离
                return;
            }
            final Object cacheVal = readFieldValue(cls, FIELD_CLASS_LOADER_CACHE);
            final Object refCountVal = readFieldValue(cls, FIELD_REF_COUNT);
            if (!(cacheVal instanceof Map)) {
                return;
            }
            final Map<?, ?> cache = (Map<?, ?>) cacheVal;
            final Object jobMapVal = cache.remove(jobId);
            if (refCountVal instanceof Map) {
                ((Map<?, ?>) refCountVal).remove(jobId);
            }
            if (jobMapVal instanceof Map) {
                final Map<?, ?> jobMap = (Map<?, ?>) jobMapVal;
                if (!jobMap.isEmpty()) {
                    int evicted = 0;
                    for (Object clObj : jobMap.values()) {
                        if (clObj instanceof ClassLoader) {
                            final ClassLoader cl = (ClassLoader) clObj;
                            log.warn("Force evicted leaked ClassLoader [{}] for job {} due to asymmetric reference count",
                                    cl.getClass().getName(), jobId);
                            LingFrameAgentBridge.onPhysicalRelease(cl);
                            evicted++;
                        }
                    }
                    if (evicted > 0) {
                        log.info("EngineClassLoaderCleaner evicted {} leaked ClassLoaders for job {}", evicted, jobId);
                    }
                }
            }
        } catch (Throwable t) {
            log.warn("EngineClassLoaderCleaner force evict for job {} failed: {}", jobId, t.getMessage());
        }
    }

    /**
     * 从 TaskGroupLocation 键对象中提取 jobId。
     */
    private static long extractJobId(Object locationKey) {
        try {
            final Field field = locationKey.getClass().getDeclaredField(FIELD_JOB_ID);
            field.setAccessible(true);
            final Object val = field.get(locationKey);
            if (val instanceof Number) {
                return ((Number) val).longValue();
            }
        } catch (Throwable t) {
            log.debug("Failed to extract jobId from location key: {}", t.getMessage());
        }
        return -1L;
    }

    /**
     * 深度清空单个上下文实例的字段引用。
     *
     * @param ctx TaskGroupContext 实例
     */
    private static void cleanContextFields(Object ctx) {
        try {
            final Class<?> clazz = ctx.getClass();
            clearMapField(clazz, ctx, FIELD_CLASS_LOADERS);
            clearMapField(clazz, ctx, FIELD_JARS);
            final Field tgField = clazz.getDeclaredField(FIELD_TASK_GROUP);
            tgField.setAccessible(true);
            tgField.set(ctx, null);
        } catch (Throwable t) {
            log.warn("EngineClassLoaderCleaner failed to clean context fields: {}", t.getMessage());
        }
    }

    /** 读取宿主对象的私有字段值。 */
    private static Object readFieldValue(Object owner, String fieldName) throws Exception {
        final Field field = owner.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(owner);
    }

    /** 清空宿主对象某个 Map 字段（私有字段，需放宽访问权限）。 */
    private static void clearMapField(Class<?> clazz, Object owner, String fieldName)
            throws Exception {
        final Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        final Object value = field.get(owner);
        if (value instanceof Map) {
            ((Map<?, ?>) value).clear();
        }
    }
}