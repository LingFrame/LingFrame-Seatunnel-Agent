package com.lingframe.agent.cleaner;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 引擎类加载器清理器单元测试。
 */
@DisplayName("EngineClassLoaderCleaner 引擎类加载器清理治理测试")
class EngineClassLoaderCleanerTest {

    @BeforeEach
    void setUp() {
        EngineClassLoaderCleaner.resetForTesting();
    }

    /**
     * 模拟 SeaTunnel TaskGroupLocation。
     */
    static class MockTaskGroupLocation {
        private final long jobId;

        MockTaskGroupLocation(long jobId) {
            this.jobId = jobId;
        }

        public long getJobId() {
            return jobId;
        }
    }

    /**
     * 模拟 SeaTunnel TaskGroupContext 类结构。
     */
    static class MockTaskGroupContext {
        private Object taskGroup;
        private final Map<Object, ClassLoader> classLoaders = new HashMap<>();
        private final Map<Object, Set<URL>> jars = new HashMap<>();

        MockTaskGroupContext(Object taskGroup, Object key, ClassLoader cl, URL jar) {
            this.taskGroup = taskGroup;
            this.classLoaders.put(key, cl);
            this.jars.put(key, Collections.singleton(jar));
        }

        public Object getTaskGroup() {
            return taskGroup;
        }

        public Map<Object, ClassLoader> getClassLoaders() {
            return classLoaders;
        }

        public Map<Object, Set<URL>> getJars() {
            return jars;
        }
    }

    /**
     * 模拟 SeaTunnel TaskExecutionService 实例。
     */
    static class MockTaskExecutionService {
        private final ConcurrentHashMap<Object, Object> finishedExecutionContexts = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Object, Object> executionContexts = new ConcurrentHashMap<>();

        public ConcurrentHashMap<Object, Object> getFinishedExecutionContexts() {
            return finishedExecutionContexts;
        }

        public ConcurrentHashMap<Object, Object> getExecutionContexts() {
            return executionContexts;
        }
    }

    /**
     * 模拟 SeaTunnel DefaultClassLoaderService 实例。
     */
    static class MockClassLoaderService {
        private final boolean cacheMode = false;
        private final Map<Long, Map<String, ClassLoader>> classLoaderCache = new ConcurrentHashMap<>();
        private final Map<Long, Map<String, AtomicInteger>> classLoaderReferenceCount = new ConcurrentHashMap<>();

        public Map<Long, Map<String, ClassLoader>> getClassLoaderCache() {
            return classLoaderCache;
        }

        public Map<Long, Map<String, AtomicInteger>> getClassLoaderReferenceCount() {
            return classLoaderReferenceCount;
        }
    }

    @Test
    @DisplayName("验证 cleanFinished 物理移除 Map 条目并清空上下文内强引用")
    void testCleanFinishedPurgesAndClearsContext() throws Exception {
        final MockTaskExecutionService mockService = new MockTaskExecutionService();
        final Object taskGroupObj = new Object();
        final MockTaskGroupLocation locKey = new MockTaskGroupLocation(999L);
        final URL dummyUrl = new URL("file:///dummy.jar");
        final MockTaskGroupContext context = new MockTaskGroupContext(taskGroupObj, locKey, getClass().getClassLoader(), dummyUrl);

        mockService.getFinishedExecutionContexts().put(locKey, context);
        Assertions.assertEquals(1, mockService.getFinishedExecutionContexts().size());

        // 注册到 Cleaner
        EngineClassLoaderCleaner.register(mockService);

        // 执行清理
        EngineClassLoaderCleaner.cleanFinished();

        // 核心断言 1：finishedExecutionContexts 被物理移除，Map 变空
        Assertions.assertTrue(mockService.getFinishedExecutionContexts().isEmpty());

        // 核心断言 2：context 内部字段被清空或置为 null
        Assertions.assertNull(context.getTaskGroup());
        Assertions.assertTrue(context.getClassLoaders().isEmpty());
        Assertions.assertTrue(context.getJars().isEmpty());

        // 核心断言 3：再次执行幂等安全
        EngineClassLoaderCleaner.cleanFinished();
        Assertions.assertTrue(mockService.getFinishedExecutionContexts().isEmpty());
    }

    @Test
    @DisplayName("验证 forceEvictJobClassLoaders 强制驱逐因异常未对称归零的 classLoaderCache 滞留项")
    void testForceEvictJobClassLoaders() {
        final MockClassLoaderService mockCls = new MockClassLoaderService();
        final long leakedJobId = 888L;
        final Map<String, ClassLoader> map = new HashMap<>();
        map.put("key-1", getClass().getClassLoader());
        mockCls.getClassLoaderCache().put(leakedJobId, map);

        final Map<String, AtomicInteger> refMap = new HashMap<>();
        refMap.put("key-1", new AtomicInteger(2)); // 模拟计数失配滞留
        mockCls.getClassLoaderReferenceCount().put(leakedJobId, refMap);

        // 注册到 Cleaner
        EngineClassLoaderCleaner.registerClassLoaderService(mockCls);

        // 执行针对该作业的强制排空
        EngineClassLoaderCleaner.forceEvictJobClassLoaders(leakedJobId);

        // 核心断言：残留的作业缓存被彻底移出
        Assertions.assertFalse(mockCls.getClassLoaderCache().containsKey(leakedJobId));
        Assertions.assertFalse(mockCls.getClassLoaderReferenceCount().containsKey(leakedJobId));
    }

    @Test
    @DisplayName("验证 sweepOrphanClassLoaders 精准驱逐无活跃任务的孤儿作业并保留活跃作业")
    void testSweepOrphanClassLoadersEvictsOnlyInactiveJobs() {
        final MockClassLoaderService mockCls = new MockClassLoaderService();
        final long activeJobId = 101L;
        final long orphanJobId = 102L;

        final Map<String, ClassLoader> activeMap = new HashMap<>();
        activeMap.put("active-key", getClass().getClassLoader());
        mockCls.getClassLoaderCache().put(activeJobId, activeMap);

        final Map<String, ClassLoader> orphanMap = new HashMap<>();
        orphanMap.put("orphan-key", getClass().getClassLoader());
        mockCls.getClassLoaderCache().put(orphanJobId, orphanMap);

        // 注册到 Cleaner
        EngineClassLoaderCleaner.registerClassLoaderService(mockCls);

        // 执行全局主动 Sweep，传入当前活跃作业集合，maxStaleMs 设为 0 立即生效
        EngineClassLoaderCleaner.sweepOrphanClassLoaders(Collections.singleton(activeJobId), 0L);

        // 核心断言：活跃作业不受影响，孤儿作业被剔除
        Assertions.assertTrue(mockCls.getClassLoaderCache().containsKey(activeJobId));
        Assertions.assertFalse(mockCls.getClassLoaderCache().containsKey(orphanJobId));
    }
}
