package com.lingframe.agent.cleaner;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.lingframe.agent.bridge.LingFrameAgentBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.Introspector;
import java.lang.ref.Reference;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
 *   <li><b>Coordinator 节点</b>：深层切断 {@code JobMaster} 内部持有的 {@code physicalPlan}、
 *       {@code logicalDag} 等核心领域对象强引用，并从 {@code runningJobMasterMap} 中移除，
 *       彻底切断 Master 端对 Connector 算子与类的 GC Root。</li>
 *   <li><b>共享缓存与 ClassLoader 显式回收</b>：缓存 {@code DefaultClassLoaderService}
 *       单例实例。强制剥离滞留在 {@code classLoaderCache} 中的孤儿 ClassLoader，
 *       显式调用 {@code URLClassLoader.close()} 释放 Jar 句柄与底层 ClassPath 资源，
 *       排空全局静态 Schema 缓存与 JavaBean 反射缓存，并投递给底座卸载。</li>

 * </ul>
 * 全程反射保护、全枚举字段名均按引擎源码校准，任一阶段失败都降级跳过并告警，绝不破坏引擎主流程。
 */
public final class EngineClassLoaderCleaner {

    private static final Logger log = LoggerFactory.getLogger(EngineClassLoaderCleaner.class);

    /** 引擎字段名（与 SeaTunnel 2.3.13 源码对齐） */
    private static final String FIELD_FINISHED = "finishedExecutionContexts";
    private static final String FIELD_EXECUTION_CONTEXTS = "executionContexts";
    private static final String FIELD_CLASS_LOADERS = "classLoaders";
    private static final String FIELD_JARS = "jars";
    private static final String FIELD_TASK_GROUP = "taskGroup";
    private static final String FIELD_CACHE_MODE = "cacheMode";
    private static final String FIELD_CLASS_LOADER_CACHE = "classLoaderCache";
    private static final String FIELD_REF_COUNT = "classLoaderReferenceCount";
    private static final String FIELD_JOB_ID = "jobId";
    private static final String FIELD_RUNNING_JOB_MASTER_MAP = "runningJobMasterMap";
    private static final String PROTOSTUFF_SERIALIZER_TYPE =
            "org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer";


    /** 引擎 TaskExecutionService 实例（由 advice 捕获，Worker 单例） */
    private static volatile Object taskExecutionService;

    /** 引擎 DefaultClassLoaderService 实例（由 advice 捕获，单例） */
    private static volatile Object classLoaderService;

    /** 引擎 CoordinatorService 实例（由 advice / JobMaster 捕获，Master 单例） */
    private static volatile Object coordinatorService;

    /** 累计清理数——写入日志供观测 Metaspace 治理是否生效 */
    private static final AtomicLong CLEANED_COUNT = new AtomicLong();

    /** 登记跟踪每个作业所使用过的全部 ClassLoader 集合（作业终态物理卸载源头） */
    private static final ConcurrentMap<Long, Set<ClassLoader>> JOB_CLASS_LOADERS = new ConcurrentHashMap<>();

    /**
     * 登记指定作业所加载/使用的 ClassLoader。
     *
     * @param jobId 作业标识
     * @param classLoader 类加载器
     */
    public static void trackJobClassLoader(long jobId, ClassLoader classLoader) {
        if (jobId <= 0 || classLoader == null || isSystemOrHostClassLoader(classLoader)) {
            return;
        }
        Set<ClassLoader> set = JOB_CLASS_LOADERS.get(jobId);
        if (set == null) {
            set = Collections.newSetFromMap(new ConcurrentHashMap<ClassLoader, Boolean>());
            final Set<ClassLoader> existing = JOB_CLASS_LOADERS.putIfAbsent(jobId, set);
            if (existing != null) {
                set = existing;
            }
        }
        set.add(classLoader);
    }

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
     * 捕获引擎 {@code CoordinatorService} 实例（线程安全，once-only）。
     *
     * @param instance 织入点注入的 CoordinatorService
     */
    public static void registerCoordinatorService(Object instance) {
        if (instance != null && coordinatorService == null) {
            coordinatorService = instance;
            log.info("EngineClassLoaderCleaner captured CoordinatorService instance");
        }
    }

    /**
     * 重置缓存实例与内部探测状态（仅供单元测试隔离使用）。
     */
    static void resetForTesting() {
        taskExecutionService = null;
        classLoaderService = null;
        coordinatorService = null;

        JOB_CLASS_LOADERS.clear();
        CLEANED_COUNT.set(0);
    }

    /**
     * 清空并移除引擎已完成作业上下文中的 ClassLoader / jars / taskGroup 强引用。
     * <p>
     * 幂等：直接从 {@code finishedExecutionContexts} 中移除已完成条目，断绝引擎内部堆积；
     * 并联动排空对应作业在 {@code DefaultClassLoaderService} 中可能滞留的 ClassLoader 缓存，
     * 以及扫描排空 Coordinator 端已完成的 JobMaster 领域对象。
     */
    public static void cleanFinished() {
        final Object target = taskExecutionService;
        if (target == null) {
            cleanFinishedJobMasters();
            return;
        }
        try {
            final Set<Long> completedJobIds = new HashSet<>();
            int cleaned = 0;
            // 清理 finishedExecutionContexts
            final Object finishedValue = readFieldValue(target, FIELD_FINISHED);
            if (finishedValue instanceof Map) {
                final Map<?, ?> finished = (Map<?, ?>) finishedValue;
                if (!finished.isEmpty()) {
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
                }
            }
            // 注意：executionContexts 是正在执行中的作业上下文，绝不能被定时清理器移除。
            // 之前这里无条件清除 executionContexts 导致 AssignSplitOperation 找不到目标 TaskGroup，
            // 作业卡在 "wait split!" 无法完成。已删除该清理逻辑，让引擎自行管理正在执行中的上下文。
            if (cleaned > 0) {
                final long total = CLEANED_COUNT.addAndGet(cleaned);
                log.info("EngineClassLoaderCleaner purged {} task-context entries (total: {})",
                        cleaned, total);
            }
            // 联动强制排空已完成作业在 DefaultClassLoaderService 中可能异常残留的 ClassLoader 缓存
            for (Long jobId : completedJobIds) {
                forceEvictJobClassLoaders(jobId);
            }


            // Fallback：如果 coordinatorService 未被 advice 捕获，通过 NodeEngine 主动获取
            tryCaptureCoordinatorService();

            // Coordinator 端主动扫描：排空已完成的 JobMaster
            cleanFinishedJobMasters();

            // 安全清理 executionContexts 中已完成作业的残留条目。
            // 仅移除不在 runningJobMasterMap 中的作业（已完成作业），避免误删正在执行的作业。
            // 之前的教训：无条件清除 executionContexts 导致 AssignSplitOperation 找不到目标 TaskGroup。
            // 现在通过 runningJobMasterMap 精确区分活跃/已完成作业，安全清理。
            cleanStaleExecutionContexts(target, completedJobIds);

            // 对 executionContexts 清理新增的 completedJobIds 联动 forceEvict
            for (Long jobId : completedJobIds) {
                forceEvictJobClassLoaders(jobId);
            }

            // 全局清理 ObjectStreamClass$Caches 中所有非系统 ClassLoader 的缓存条目。
            // forceEvictJobClassLoaders 中的按 ClassLoader 清理可能因 toRelease 为空而遗漏，
            // 全局扫描确保彻底清除残留的 SoftReference → ObjectStreamClass → Class → ClassLoader 引用链。
            cleanAllObjectStreamClassCaches();
        } catch (Throwable t) {
            log.warn("EngineClassLoaderCleaner finished-context clean skipped (engine version mismatch?): {}",
                    t.getMessage());
        }
    }

    /**
     * 深度清理 Coordinator 端 {@code JobMaster} 内部的核心领域对象与缓存。
     * <p>
     * 斩断 GC Root：
     * <ol>
     *   <li>强制驱逐 {@code DefaultClassLoaderService} 中该作业的 ClassLoader；</li>
     *   <li>捕获 {@code CoordinatorService} 并确保从 {@code runningJobMasterMap} 中移除该作业；</li>
     *   <li>反射将 JobMaster 内部持有的 {@code physicalPlan}、{@code logicalDag}、
     *       {@code checkpointPlanMap}、{@code jobDAGInfo}、{@code checkpointManager}、
     *       {@code jobImmutableInformation} 彻底深层置 null，彻底解绑由 Connector ClassLoader
     *       加载的 Action、CatalogTable 等类实例。</li>
     * </ol>
     *
     * @param jobMaster 引擎 JobMaster 实例
     */
    public static void cleanJobMaster(Object jobMaster) {
        if (jobMaster == null) {
            return;
        }
        try {
            long jobId = -1L;
            try {
                final Method getJobIdMethod = jobMaster.getClass().getMethod("getJobId");
                final Object idObj = getJobIdMethod.invoke(jobMaster);
                if (idObj instanceof Number) {
                    jobId = ((Number) idObj).longValue();
                }
            } catch (Throwable t) {
                log.debug("Failed to extract jobId from JobMaster: {}", t.getMessage());
            }

            if (jobId > 0) {

                forceEvictJobClassLoaders(jobId);
            }

            // 尝试捕获 CoordinatorService 并从 runningJobMasterMap 中移除
            try {
                final Object server = readFieldValue(jobMaster, "seaTunnelServer");
                if (server != null) {
                    final Method getCoordMethod = server.getClass().getMethod("getCoordinatorService");
                    final Object coord = getCoordMethod.invoke(server);
                    if (coord != null) {
                        registerCoordinatorService(coord);
                        final Object runningMap = readFieldValue(coord, FIELD_RUNNING_JOB_MASTER_MAP);
                        if (runningMap instanceof Map && jobId > 0) {
                            ((Map<?, ?>) runningMap).remove(jobId);
                        }
                    }
                    // 精准清理 TaskExecutionService 中属于当前 jobId 的 executionContexts 和 finishedExecutionContexts
                    // 斩断 TaskGroupContext → taskGroup → tasks → Task → Action → Class → ClassLoader 引用链
                    try {
                        final Method getTesMethod = server.getClass().getMethod("getTaskExecutionService");
                        final Object tes = getTesMethod.invoke(server);
                        if (tes != null) {
                            register(tes);
                            cleanTaskExecutionContextsByJobId(tes, jobId);
                        }
                    } catch (Throwable t2) {
                        log.debug("TaskExecutionService cleanup skipped for job {}: {}", jobId, t2.getMessage());
                    }
                }
            } catch (Throwable t) {
                log.debug("CoordinatorService map eviction skipped: {}", t.getMessage());
            }

            // 深度解绑 JobMaster 内部核心大对象，切断 GC Root
            int fieldsCleared = 0;
            fieldsCleared += nullifyField(jobMaster, "physicalPlan");
            fieldsCleared += nullifyField(jobMaster, "logicalDag");
            fieldsCleared += nullifyField(jobMaster, "checkpointPlanMap");
            fieldsCleared += nullifyField(jobMaster, "jobDAGInfo");
            fieldsCleared += nullifyField(jobMaster, "checkpointManager");
            fieldsCleared += nullifyField(jobMaster, "jobImmutableInformation");
            fieldsCleared += nullifyField(jobMaster, "jobMasterCompleteFuture");

            // 排空 Hazelcast 分布式 Map 中的 Job 状态与领域对象，彻底切断集群常驻 GC Root
            if (jobId > 0) {
                cleanHazelcastJobState(jobId);
            }

            log.info("EngineClassLoaderCleaner severed Coordinator GC roots for JobMaster {} ({} fields cleared)",
                    jobId, fieldsCleared);
        } catch (Throwable t) {
            log.warn("EngineClassLoaderCleaner failed to clean JobMaster: {}", t.getMessage());
        }
    }

    /**
     * 排空 Hazelcast 分布式 Map 中对应作业的领域对象，彻底斩断集群常驻 GC Roots。
     * <p>
     * 排除 {@code IMAP_FINISHED_JOB_STATE}（名称含 {@code finished-job-state}）：该 IMap
     * 存储的是 {@code JobStatus} 枚举值，不持有 ClassLoader 或领域对象引用，删除它无助于
     * ClassLoader 回收，但会破坏 REST API {@code getJobInfoJson} 对已完成作业的状态查询。
     *
     * @param jobId 作业标识
     */
    public static void cleanHazelcastJobState(long jobId) {
        if (jobId <= 0) {
            return;
        }
        try {
            final Collection<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();
            if (instances == null || instances.isEmpty()) {
                return;
            }
            final Long boxedJobId = jobId;
            final String strJobId = String.valueOf(jobId);
            for (HazelcastInstance hz : instances) {
                if (hz == null) {
                    continue;
                }
                for (DistributedObject obj : hz.getDistributedObjects()) {
                    if (obj instanceof IMap) {
                        final String name = obj.getName();
                        if (name != null && !name.contains("finished-job-state")
                                && (name.contains("job") || name.contains("checkpoint")
                                    || name.contains("engine") || name.contains("running"))) {
                            try {
                                final IMap<?, ?> map = (IMap<?, ?>) obj;
                                map.remove(boxedJobId);
                                map.remove(strJobId);
                            } catch (Throwable t2) {
                                log.debug("Hazelcast distributed map eviction skipped for job {}: {}", jobId, t2.getMessage());
                            }
                        }
                    }
                }
            }
        } catch (Throwable t) {
            log.debug("Hazelcast job state eviction skipped: {}", t.getMessage());
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
        if (jobId <= 0) {
            return;
        }
        final Set<ClassLoader> toRelease = new HashSet<>();
        final Set<ClassLoader> tracked = JOB_CLASS_LOADERS.remove(jobId);
        if (tracked != null) {
            toRelease.addAll(tracked);
        }

        final Object cls = classLoaderService;
        if (cls != null) {
            try {
                final Object cacheModeVal = readFieldValue(cls, FIELD_CACHE_MODE);
                if (!Boolean.TRUE.equals(cacheModeVal)) {
                    final Object cacheVal = readFieldValue(cls, FIELD_CLASS_LOADER_CACHE);
                    final Object refCountVal = readFieldValue(cls, FIELD_REF_COUNT);
                    if (cacheVal instanceof Map) {
                        final Map<?, ?> cache = (Map<?, ?>) cacheVal;
                        final Object jobMapVal = cache.remove(jobId);
                        if (refCountVal instanceof Map) {
                            ((Map<?, ?>) refCountVal).remove(jobId);
                        }
                        if (jobMapVal instanceof Map) {
                            final Map<?, ?> jobMap = (Map<?, ?>) jobMapVal;
                            for (Object clObj : jobMap.values()) {
                                if (clObj instanceof ClassLoader) {
                                    toRelease.add((ClassLoader) clObj);
                                }
                            }
                        }
                    }
                }
            } catch (Throwable t) {
                log.warn("EngineClassLoaderCleaner cache extraction for job {} failed: {}", jobId, t.getMessage());
            }
        }

        if (!toRelease.isEmpty()) {
            int evicted = 0;
            for (ClassLoader cl : toRelease) {
                if (cl != null && !isSystemOrHostClassLoader(cl)) {
                    log.info("Force evicted and physically releasing ClassLoader [{}] for job {}",
                            cl.getClass().getName(), jobId);
                    LingFrameAgentBridge.onPhysicalRelease(cl);
                    cleanObjectStreamClassCaches(cl);
                    interruptThreadsByClassLoader(cl);
                    evicted++;
                }
            }
            if (evicted > 0) {
                log.info("EngineClassLoaderCleaner evicted {} leaked ClassLoaders for job {}", evicted, jobId);
                System.gc();
            }
        }
    }

    /**
     * 清理 java.io.ObjectStreamClass$Caches 中属于指定 ClassLoader 的条目。
     * <p>
     * Java 序列化机制会在 ObjectStreamClass$Caches.localDescs 和 reflectors 中
     * 缓存 SoftReference&lt;ObjectStreamClass&gt;，每个 ObjectStreamClass 持有其描述的
     * Class&lt;?&gt; 引用，Class&lt;?&gt; 又持有 classLoader 引用。
     * SoftReference 在内存充足时不会被 GC，导致已关闭的 ClassLoader 无法被回收，
     * Metaspace 泄漏。此方法精准移除属于目标 ClassLoader 的缓存条目。
     *
     * @param targetCl 已被驱逐的 ClassLoader
     */
    private static void cleanObjectStreamClassCaches(ClassLoader targetCl) {
        if (targetCl == null) {
            return;
        }
        try {
            final Class<?> cachesClass = Class.forName("java.io.ObjectStreamClass$Caches");
            int totalRemoved = 0;
            for (String fieldName : new String[]{"localDescs", "reflectors"}) {
                try {
                    final Field field = cachesClass.getDeclaredField(fieldName);
                    field.setAccessible(true);
                    final Object map = field.get(null);
                    if (map instanceof Map) {
                        totalRemoved += cleanObjectStreamClassCacheMap((Map<?, ?>) map, targetCl);
                    }
                } catch (NoSuchFieldException e) {
                    log.debug("ObjectStreamClass$Caches field {} not found on this JDK, skipping", fieldName);
                }
            }
            if (totalRemoved > 0) {
                log.info("Cleaned {} ObjectStreamClass cache entries for ClassLoader [{}]",
                        totalRemoved, targetCl.getClass().getName());
            }
        } catch (Throwable t) {
            log.debug("Failed to clean ObjectStreamClass caches: {}", t.getMessage());
        }
    }

    /**
     * 全局清理 ObjectStreamClass$Caches 中所有非系统 ClassLoader 的缓存条目。
     * <p>
     * {@code forceEvictJobClassLoaders} 中的按 ClassLoader 清理可能因 toRelease 为空而遗漏，
     * 全局扫描确保彻底清除残留的 SoftReference → ObjectStreamClass → Class → ClassLoader 引用链。
     */
    private static void cleanAllObjectStreamClassCaches() {
        try {
            final Class<?> cachesClass = Class.forName("java.io.ObjectStreamClass$Caches");
            int totalRemoved = 0;
            for (String fieldName : new String[]{"localDescs", "reflectors"}) {
                try {
                    final Field field = cachesClass.getDeclaredField(fieldName);
                    field.setAccessible(true);
                    final Object map = field.get(null);
                    if (map instanceof Map) {
                        totalRemoved += cleanAllNonSystemCacheEntries((Map<?, ?>) map);
                    }
                } catch (NoSuchFieldException e) {
                    log.debug("ObjectStreamClass$Caches field {} not found on this JDK, skipping", fieldName);
                }
            }
            if (totalRemoved > 0) {
                log.info("Cleaned {} ObjectStreamClass cache entries (global sweep)", totalRemoved);
            }
        } catch (Throwable t) {
            log.debug("Failed to clean ObjectStreamClass caches (global): {}", t.getMessage());
        }
    }

    /**
     * 从 ObjectStreamClass 缓存 Map 中移除所有非系统 ClassLoader 的条目。
     */
    private static int cleanAllNonSystemCacheEntries(Map<?, ?> cacheMap) {
        int removed = 0;
        final Iterator<? extends Map.Entry<?, ?>> it = cacheMap.entrySet().iterator();
        while (it.hasNext()) {
            final Map.Entry<?, ?> entry = it.next();
            final Object key = entry.getKey();
            final Object value = entry.getValue();

            final Class<?> keyClass = getReferenceReferentAsClass(key);
            if (keyClass != null && !isSystemOrHostClassLoader(keyClass.getClassLoader())) {
                it.remove();
                removed++;
                continue;
            }

            final Class<?> valueClass = getObjectStreamClassReferentClass(value);
            if (valueClass != null && !isSystemOrHostClassLoader(valueClass.getClassLoader())) {
                it.remove();
                removed++;
            }
        }
        return removed;
    }

    /**
     * 中断由目标 ClassLoader 加载的线程（如 Kafka admin client 后台线程）。
     * <p>
     * 线程通过 Thread → Class → ClassLoader 持有引用，只要线程存活 ClassLoader 无法回收。
     * interrupt 是最安全的终止信号，Kafka 线程通常响应中断并退出。
     */
    private static void interruptThreadsByClassLoader(ClassLoader targetCl) {
        if (targetCl == null) {
            return;
        }
        try {
            final Set<Thread> threads = getAllThreads();
            int interrupted = 0;
            for (Thread t : threads) {
                if (t == null || t == Thread.currentThread() || !t.isAlive()) {
                    continue;
                }
                Class<?> threadClass = t.getClass();
                while (threadClass != null) {
                    if (threadClass.getClassLoader() == targetCl) {
                        t.interrupt();
                        interrupted++;
                        log.info("Interrupted thread {} (class {} loaded by evicted ClassLoader)",
                                t.getName(), threadClass.getName());
                        break;
                    }
                    threadClass = threadClass.getSuperclass();
                }
            }
            if (interrupted > 0) {
                log.info("Interrupted {} threads loaded by evicted ClassLoader", interrupted);
            }
        } catch (Throwable t) {
            log.debug("Failed to interrupt threads by ClassLoader: {}", t.getMessage());
        }
    }

    /**
     * 收集 JVM 中所有活线程（跨线程组）。
     */
    private static Set<Thread> getAllThreads() {
        final Set<Thread> threads = new HashSet<>();
        final ThreadGroup root = Thread.currentThread().getThreadGroup();
        ThreadGroup parent = root;
        while (parent.getParent() != null) {
            parent = parent.getParent();
        }
        collectThreads(parent, threads);
        return threads;
    }

    /**
     * 递归收集线程组中的所有线程。
     */
    private static void collectThreads(ThreadGroup group, Set<Thread> threads) {
        if (group == null) {
            return;
        }
        final int estimate = group.activeCount() * 2;
        final Thread[] batch = new Thread[estimate];
        final int count = group.enumerate(batch, false);
        for (int i = 0; i < count; i++) {
            if (batch[i] != null) {
                threads.add(batch[i]);
            }
        }
        final int groupEstimate = group.activeGroupCount() * 2;
        final ThreadGroup[] subGroups = new ThreadGroup[groupEstimate];
        final int groupCount = group.enumerate(subGroups, false);
        for (int i = 0; i < groupCount; i++) {
            collectThreads(subGroups[i], threads);
        }
    }

    /**
     * 从 ObjectStreamClass 缓存 Map 中移除属于目标 ClassLoader 的条目。
     *
     * @param cacheMap ObjectStreamClass$Caches.localDescs 或 reflectors
     * @param targetCl 目标 ClassLoader
     * @return 移除的条目数
     */
    private static int cleanObjectStreamClassCacheMap(Map<?, ?> cacheMap, ClassLoader targetCl) {
        int removed = 0;
        for (Map.Entry<?, ?> entry : cacheMap.entrySet()) {
            final Object key = entry.getKey();
            final Object value = entry.getValue();

            // 检查 key (WeakReference<Class<?>>) 的 referent
            final Class<?> keyClass = getReferenceReferentAsClass(key);
            if (keyClass != null && keyClass.getClassLoader() == targetCl) {
                cacheMap.remove(key);
                removed++;
                continue;
            }

            // 检查 value (SoftReference<ObjectStreamClass>) 的 referent.cl
            final Class<?> valueClass = getObjectStreamClassReferentClass(value);
            if (valueClass != null && valueClass.getClassLoader() == targetCl) {
                cacheMap.remove(key);
                removed++;
            }
        }
        return removed;
    }

    /**
     * 从 Reference 对象中提取 referent，如果 referent 是 Class 则返回。
     */
    private static Class<?> getReferenceReferentAsClass(Object ref) {
        if (ref == null || !(ref instanceof Reference)) {
            return null;
        }
        final Object referent = ((Reference<?>) ref).get();
        return referent instanceof Class ? (Class<?>) referent : null;
    }

    /**
     * 从 Reference&lt;ObjectStreamClass&gt; 中提取 ObjectStreamClass 的 cl 字段（Class&lt;?&gt;）。
     */
    private static Class<?> getObjectStreamClassReferentClass(Object ref) {
        if (ref == null || !(ref instanceof Reference)) {
            return null;
        }
        final Object referent = ((Reference<?>) ref).get();
        if (referent == null) {
            return null;
        }
        try {
            final Field clField = referent.getClass().getDeclaredField("cl");
            clField.setAccessible(true);
            final Object cl = clField.get(referent);
            return cl instanceof Class ? (Class<?>) cl : null;
        } catch (Throwable t) {
            return null;
        }
    }

    /**
     * 安全显式关闭作业级自定义类加载器（释放底层的 JAR 句柄与 ClassPath 资源）。
     * <p>
     * 安全守卫机制（绝不走两极）：
     * <ol>
     *   <li>必须是 {@link* URLClassLoader} 实例；</li>
     *   <li>绝对不能是系统类加载器（SystemClassLoader/Platform/Ext）链上的加载器；</li>
     *   <li>绝对不能是 Agent/宿主核心类加载器链上的加载器；</li>
     *   <li>严格保护测试宿主（如 Surefire Forked VM / IDE 运行器）与引擎平台常驻基础组件，杜绝误杀；</li>
     *   <li>出现任何异常均降级安全忽略，绝不破坏宿主或作业主链路。</li>
     * </ol>
     *
     * @param cl 目标类加载器
     */
    public static void closeClassLoaderSafely(ClassLoader cl) {
        if (cl == null || isSystemOrHostClassLoader(cl)) {
            return;
        }
        if (cl instanceof URLClassLoader) {
            try {
                ((URLClassLoader) cl).close();
                log.info("Closed URLClassLoader safely: {}", cl.getClass().getName());
            } catch (Throwable t) {
                log.debug("URLClassLoader close skipped or failed: {}", t.getMessage());
            }
        }
    }

    /**
     * 判定目标类加载器是否属于系统或宿主 Agent 自身加载器链。
     *
     * @param cl 目标加载器
     * @return true 属于系统/宿主链，严禁关闭；false 属于隔离的作业子加载器
     */
    public static boolean isSystemOrHostClassLoader(ClassLoader cl) {
        if (cl == null) {
            return true;
        }
        try {
            ClassLoader sys = ClassLoader.getSystemClassLoader();
            while (sys != null) {
                if (cl == sys) {
                    return true;
                }
                sys = sys.getParent();
            }
        } catch (Throwable t) {
            log.debug("System ClassLoader hierarchy inspection failed: {}", t.getMessage());
        }
        try {
            ClassLoader host = EngineClassLoaderCleaner.class.getClassLoader();
            while (host != null) {
                if (cl == host) {
                    return true;
                }
                host = host.getParent();
            }
        } catch (Throwable t) {
            log.debug("Host ClassLoader hierarchy inspection failed: {}", t.getMessage());
        }
        return false;
    }

    /**
     * 清理 JVM 全局静态类型与反射缓存中关联目标 ClassLoader 的条目。
     *
     * @param cl 类加载器
     */
    public static void cleanStaticCaches(ClassLoader cl) {
        if (cl == null) {
            return;
        }
        // 1. 清理 JavaBean 属性反射缓存
        try {
            Introspector.flushCaches();
        } catch (Throwable t) {
            log.debug("Introspector.flushCaches failed: {}", t.getMessage());
        }

        // 2. 清理 ProtoStuffSerializer 的 SCHEMA_CACHE（若存在）
        try {
            final Class<?> serializerClass = Class.forName(
                    PROTOSTUFF_SERIALIZER_TYPE, false, ClassLoader.getSystemClassLoader());
            final Object cacheObj = readStaticFieldValue(serializerClass, "SCHEMA_CACHE");
            if (cacheObj instanceof Map) {
                final Map<?, ?> schemaCache = (Map<?, ?>) cacheObj;
                int removed = 0;
                final Iterator<?> it = schemaCache.keySet().iterator();
                while (it.hasNext()) {
                    final Object k = it.next();
                    if (k instanceof Class && ((Class<?>) k).getClassLoader() == cl) {
                        it.remove();
                        removed++;
                    }
                }
                if (removed > 0) {
                    log.info("Cleaned {} cached schemas from ProtoStuffSerializer for ClassLoader {}",
                            removed, cl.getClass().getName());
                }
            }
        } catch (Throwable t) {
            log.debug("ProtoStuffSerializer cache clean skipped: {}", t.getMessage());
        }

        // 3. 注销 DriverManager 中属于目标 ClassLoader 的 JDBC 驱动
        try {
            final Enumeration<Driver> drivers = DriverManager.getDrivers();
            while (drivers.hasMoreElements()) {
                final Driver driver = drivers.nextElement();
                if (driver.getClass().getClassLoader() == cl) {
                    DriverManager.deregisterDriver(driver);
                    log.info("Deregistered JDBC driver {} for ClassLoader {}",
                            driver.getClass().getName(), cl.getClass().getName());
                }
            }
        } catch (Throwable t) {
            log.debug("DriverManager deregister failed: {}", t.getMessage());
        }

        // 4. 清理 ResourceBundle 缓存
        try {
            ResourceBundle.clearCache(cl);
        } catch (Throwable t) {
            log.debug("ResourceBundle clearCache failed: {}", t.getMessage());
        }
    }

    /**

     * 精准清理 TaskExecutionService 中属于指定作业的 executionContexts 和 finishedExecutionContexts。
     * <p>
     * 斩断 TaskGroupContext → taskGroup → tasks → Task → Action → Class → ClassLoader 引用链。
     *
     * @param tes   TaskExecutionService 实例
     * @param jobId 作业标识
     */
    private static void cleanTaskExecutionContextsByJobId(Object tes, long jobId) {
        if (tes == null || jobId <= 0) {
            return;
        }
        int removed = 0;
        removed += removeContextsByJobId(tes, FIELD_FINISHED, jobId);
        removed += removeContextsByJobId(tes, FIELD_EXECUTION_CONTEXTS, jobId);
        if (removed > 0) {
            log.info("EngineClassLoaderCleaner purged {} task-context entries for job {}", removed, jobId);
        }
    }

    /**
     * 从 TaskExecutionService 的指定 Map 字段中移除属于目标作业的条目。
     *
     * @param tes       TaskExecutionService 实例
     * @param fieldName Map 字段名
     * @param jobId     作业标识
     * @return 移除的条目数
     */
    private static int removeContextsByJobId(Object tes, String fieldName, long jobId) {
        try {
            final Object mapObj = readFieldValue(tes, fieldName);
            if (mapObj instanceof Map) {
                final Map<?, ?> map = (Map<?, ?>) mapObj;
                if (map.isEmpty()) {
                    return 0;
                }
                int removed = 0;
                final Iterator<? extends Map.Entry<?, ?>> it = map.entrySet().iterator();
                while (it.hasNext()) {
                    final Map.Entry<?, ?> entry = it.next();
                    final Object key = entry.getKey();
                    if (key != null && extractJobId(key) == jobId) {
                        final Object ctx = entry.getValue();
                        if (ctx != null) {
                            cleanContextFields(ctx);
                        }
                        it.remove();
                        removed++;
                    }
                }
                return removed;
            }
        } catch (Throwable t) {
            log.debug("Failed to clean {} for job {}: {}", fieldName, jobId, t.getMessage());
        }
        return 0;
    }

    /**
     * 尝试通过 TaskExecutionService → NodeEngine → SeaTunnelServer 捕获 CoordinatorService。
     * <p>
     * Fallback：当 JobMasterCleanJobAdvice 未触发时（JobMaster.cleanJob 尚未执行），
     * 通过反射链主动获取 CoordinatorService 实例，确保 cleanFinishedJobMasters 能工作。
     */
    private static void tryCaptureCoordinatorService() {
        if (coordinatorService != null || taskExecutionService == null) {
            return;
        }
        try {
            final Object nodeEngine = readFieldValue(taskExecutionService, "nodeEngine");
            if (nodeEngine == null) {
                return;
            }
            final Method getServiceMethod = nodeEngine.getClass().getMethod("getService", String.class);
            final Object server = getServiceMethod.invoke(nodeEngine, "st:impl:seaTunnelServer");
            if (server == null) {
                return;
            }
            final Method getCoordMethod = server.getClass().getMethod("getCoordinatorService");
            final Object coord = getCoordMethod.invoke(server);
            if (coord != null) {
                registerCoordinatorService(coord);
                log.info("EngineClassLoaderCleaner captured CoordinatorService via NodeEngine fallback");
            }
        } catch (Throwable t) {
            log.debug("Failed to capture CoordinatorService via NodeEngine: {}", t.getMessage());
        }
    }

    /**
     * 扫描 Coordinator 端的已完成 JobMaster 并执行深层清理。
     */
    private static void cleanFinishedJobMasters() {
        final Object coord = coordinatorService;
        if (coord == null) {
            return;
        }
        try {
            final Object runningMap = readFieldValue(coord, FIELD_RUNNING_JOB_MASTER_MAP);
            if (runningMap instanceof Map) {
                final Map<?, ?> map = (Map<?, ?>) runningMap;
                if (!map.isEmpty()) {
                    final Iterator<? extends Map.Entry<?, ?>> it = map.entrySet().iterator();
                    while (it.hasNext()) {
                        final Map.Entry<?, ?> entry = it.next();
                        final Object jm = entry.getValue();
                        if (jm != null && isJobMasterFinished(jm)) {
                            cleanJobMaster(jm);
                            it.remove();
                        }
                    }
                }
            }
        } catch (Throwable t) {
            log.debug("Coordinator clean finished JobMasters skipped: {}", t.getMessage());
        }
    }

    /**
     * 安全清理 executionContexts 中已完成作业的残留条目。
     * <p>
     * 仅移除不在 runningJobMasterMap 中的作业条目，避免误删正在执行的作业。
     * 之前的教训：无条件清除 executionContexts 导致 AssignSplitOperation 找不到目标 TaskGroup。
     * 现在通过 runningJobMasterMap 精确区分活跃/已完成作业，安全清理。
     *
     * @param tes TaskExecutionService 实例
     * @param completedJobIds 已完成作业 ID 集合（联动 forceEvict）
     */
    private static void cleanStaleExecutionContexts(Object tes, Set<Long> completedJobIds) {
        if (tes == null || completedJobIds == null) {
            return;
        }
        try {
            final Set<Long> activeJobIds = collectActiveJobIds();
            if (activeJobIds == null) {
                return;
            }
            final Object execCtxVal = readFieldValue(tes, FIELD_EXECUTION_CONTEXTS);
            if (!(execCtxVal instanceof Map)) {
                return;
            }
            final Map<?, ?> execCtxMap = (Map<?, ?>) execCtxVal;
            if (execCtxMap.isEmpty()) {
                return;
            }
            int staleRemoved = 0;
            final Iterator<? extends Map.Entry<?, ?>> it = execCtxMap.entrySet().iterator();
            while (it.hasNext()) {
                final Map.Entry<?, ?> entry = it.next();
                final Object key = entry.getKey();
                if (key == null) {
                    continue;
                }
                final long jobId = extractJobId(key);
                if (jobId <= 0) {
                    continue;
                }
                if (!activeJobIds.contains(jobId)) {
                    final Object ctx = entry.getValue();
                    if (ctx != null) {
                        cleanContextFields(ctx);
                    }
                    it.remove();
                    staleRemoved++;
                    completedJobIds.add(jobId);
                    log.info("EngineClassLoaderCleaner force-purged stale executionContexts entry for completed job {}",
                            jobId);
                }
            }
            if (staleRemoved > 0) {
                log.info("EngineClassLoaderCleaner force-purged {} stale executionContexts entries (active jobs: {})",
                        staleRemoved, activeJobIds.size());
            }
        } catch (Throwable t) {
            log.debug("Failed to clean stale executionContexts: {}", t.getMessage());
        }
    }

    /**
     * 从 CoordinatorService.runningJobMasterMap 中收集活跃作业 ID 集合。
     *
     * @return 活跃作业 ID 集合；coordinatorService 不可用时返回 null（调用方应跳过清理）
     */
    private static Set<Long> collectActiveJobIds() {
        final Object coord = coordinatorService;
        if (coord == null) {
            return null;
        }
        final Set<Long> activeIds = new HashSet<>();
        try {
            final Object runningMap = readFieldValue(coord, FIELD_RUNNING_JOB_MASTER_MAP);
            if (runningMap instanceof Map) {
                for (Object key : ((Map<?, ?>) runningMap).keySet()) {
                    if (key instanceof Number) {
                        activeIds.add(((Number) key).longValue());
                    }
                }
            }
        } catch (Throwable t) {
            log.debug("Failed to collect active job IDs: {}", t.getMessage());
            return null;
        }
        return activeIds;
    }

    /**
     * 判断 JobMaster 是否已处于终态。
     */
    private static boolean isJobMasterFinished(Object jm) {
        try {
            final Method getStatusMethod = jm.getClass().getMethod("getJobStatus");
            final Object status = getStatusMethod.invoke(jm);
            if (status != null) {
                final Method isEndStateMethod = status.getClass().getMethod("isEndState");
                return Boolean.TRUE.equals(isEndStateMethod.invoke(status));
            }
        } catch (Throwable t) {
            log.debug("Failed to determine JobMaster end state: {}", t.getMessage());
        }
        return false;
    }

    /**
     * 反射将对象的私有字段置 null。
     *
     * @param target    目标实例
     * @param fieldName 字段名
     * @return 成功置空返回 1，字段不存在或失败返回 0
     */
    private static int nullifyField(Object target, String fieldName) {
        try {
            final Field field = target.getClass().getDeclaredField(fieldName);
            setAccessibleSafely(field);
            field.set(target, null);
            return 1;
        } catch (NoSuchFieldException e) {
            return 0;
        } catch (Throwable t) {
            log.debug("Failed to nullify field {}: {}", fieldName, t.getMessage());
            return 0;
        }
    }

    /**
     * 从 TaskGroupLocation 键对象中提取 jobId。
     */
    private static long extractJobId(Object locationKey) {
        try {
            final Field field = locationKey.getClass().getDeclaredField(FIELD_JOB_ID);
            setAccessibleSafely(field);
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
            setAccessibleSafely(tgField);
            tgField.set(ctx, null);
        } catch (Throwable t) {
            log.warn("EngineClassLoaderCleaner failed to clean context fields: {}", t.getMessage());
        }
    }

    /** 读取宿主对象的私有字段值。 */
    private static Object readFieldValue(Object owner, String fieldName) throws Exception {
        final Field field = owner.getClass().getDeclaredField(fieldName);
        setAccessibleSafely(field);
        return field.get(owner);
    }

    /** 清空宿主对象某个 Map 字段（私有字段，需放宽访问权限）。 */
    private static void clearMapField(Class<?> clazz, Object owner, String fieldName)
            throws Exception {
        final Field field = clazz.getDeclaredField(fieldName);
        setAccessibleSafely(field);
        final Object value = field.get(owner);
        if (value instanceof Map) {
            ((Map<?, ?>) value).clear();
        }
    }

    /** 读取类的静态私有字段值。 */
    private static Object readStaticFieldValue(Class<?> clazz, String fieldName) throws Exception {
        final Field field = clazz.getDeclaredField(fieldName);
        setAccessibleSafely(field);
        return field.get(null);
    }

    /** 安全地在 doPrivileged 块中放宽反射对象的访问权限。 */
    private static void setAccessibleSafely(final AccessibleObject accessibleObject) {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            accessibleObject.setAccessible(true);
            return null;
        });
    }
}