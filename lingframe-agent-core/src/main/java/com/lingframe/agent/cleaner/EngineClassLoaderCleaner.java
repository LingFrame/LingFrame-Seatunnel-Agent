package com.lingframe.agent.cleaner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 引擎 ClassLoader 清理器——Metaspace 泄漏根因治理。
 * <p>
 * SeaTunnel 2.3.13 的 {@code TaskExecutionService.finishedExecutionContexts} 在作业完成后
 * 长期保留 {@code TaskGroupContext}，其 {@code classLoaders} 字段强持有每个任务的 ClassLoader，
 * {@code taskGroup} 强持有连接器任务实例——这正是类元数据无法卸载、Metaspace 线性增长的锚点。
 * engine 自带的清理（{@code cleanTaskGroupContext}）受 {@code ownedSlotProfilesIMap} 判空影响，
 * 极易静默跳过，导致该 map 只进不出。
 * <p>
 * 本清理器作为 Agent 的补偿：缓存 {@code TaskExecutionService} 实例（由
 * {@code TaskExecutionServiceCacheAdvice} 织入捕获），后台周期性地把
 * {@code finishedExecutionContexts} 中已存在的 {@link TaskGroupContext} 的
 * {@code classLoaders}/{@code jars} 清空并把 {@code taskGroup} 置空，使作业级 ClassLoader
 * 与其类元数据失去引擎侧强引用、可被 GC 回收。每个上下文实例只清理一次（实例级去重），
 * 任务执行期间反复轮询不重复触碰，避免打扰仍在读取该上下文的引擎逻辑。
 * <p>
 * 全程反射、全枚举字段名均按引擎源码校准，任一阶段失败都降级跳过并告警，绝不破坏主审计。
 * 私有字段反射统一通过 {@code Field.setAccessible(true)} 访问，不使用 AccessController。
 */
public final class EngineClassLoaderCleaner {

    private static final Logger log = LoggerFactory.getLogger(EngineClassLoaderCleaner.class);

    /** 引擎字段名（与 SeaTunnel 2.3.13 源码对齐） */
    private static final String FIELD_FINISHED = "finishedExecutionContexts";
    private static final String FIELD_CLASS_LOADERS = "classLoaders";
    private static final String FIELD_JARS = "jars";
    private static final String FIELD_TASK_GROUP = "taskGroup";

    /** 引擎 TaskExecutionService 实例（由 advice 捕获，单例） */
    private static volatile Object service;

    /** 已深度清理的 TaskGroupContext 实例集（实例身份，防重复置空损坏引擎读取） */
    private static final Set<Object> CLEANED =
            Collections.newSetFromMap(new IdentityHashMap<>());

    /** 累计清理数——写入日志供观测 Metaspace 治理是否生效 */
    private static final AtomicLong CLEANED_COUNT = new AtomicLong();

    /** 低频日志节流窗口（每 N 个打一条汇总，避免刷日志） */
    private static final long LOG_THROTTLE = 100L;

    private EngineClassLoaderCleaner() {
    }

    /**
     * 捕获引擎 {@code TaskExecutionService} 实例（线程安全，once-only）。
     *
     * @param instance 织入点注入的 this
     */
    public static void register(Object instance) {
        if (instance != null && service == null) {
            service = instance;
            log.info("EngineClassLoaderCleaner captured TaskExecutionService instance");
        }
    }

    /**
     * 清空引擎已完成作业上下文中的 ClassLoader / jars / taskGroup 引用。
     * <p>
     * 幂等：每个上下文实例仅清理一次；引擎实例未捕获或字段缺省时静默跳过。
     */
    public static void cleanFinished() {
        final Object target = service;
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
            for (Object ctx : finished.values()) {
                if (ctx != null && releaseOnce(ctx)) {
                    cleaned++;
                }
            }
            if (cleaned > 0) {
                final long total = CLEANED_COUNT.addAndGet(cleaned);
                if (total % LOG_THROTTLE == 1) {
                    log.info("EngineClassLoaderCleaner released {} finished task-context "
                            + "ClassLoader refs (total {})", cleaned, total);
                }
            }
        } catch (Throwable t) {
            log.warn("EngineClassLoaderCleaner finished-context clean skipped "
                    + "(engine version mismatch?): {}", t.getMessage());
        }
    }

    /**
     * 对单个上下文实例执行一次深度释放；已处理过的实例直接返回 false。
     *
     * @param ctx TaskGroupContext 实例
     * @return true 表示本次确实执行了释放
     */
    private static boolean releaseOnce(Object ctx) {
        if (CLEANED.contains(ctx)) {
            return false;
        }
        try {
            final Class<?> clazz = ctx.getClass();
            clearMapField(clazz, ctx, FIELD_CLASS_LOADERS);
            clearMapField(clazz, ctx, FIELD_JARS);
            final Field tgField = clazz.getDeclaredField(FIELD_TASK_GROUP);
            tgField.setAccessible(true);
            tgField.set(ctx, null);
            CLEANED.add(ctx);
            return true;
        } catch (Throwable t) {
            log.warn("EngineClassLoaderCleaner skip single context (field mismatch?): {}",
                    t.getMessage());
            return false;
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