package com.lingframe.agent.cleaner;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
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

    private static URLClassLoader createIsolatedTestClassLoader() {
        return new URLClassLoader(new URL[0], null);
    }

    @Test
    @DisplayName("验证 cleanFinished 物理移除 Map 条目并清空上下文内强引用")
    void testCleanFinishedPurgesAndClearsContext() throws Exception {
        final MockTaskExecutionService mockService = new MockTaskExecutionService();
        final Object taskGroupObj = new Object();
        final MockTaskGroupLocation locKey = new MockTaskGroupLocation(999L);
        final URL dummyUrl = new URL("file:///dummy.jar");
        final MockTaskGroupContext context = new MockTaskGroupContext(taskGroupObj, locKey, createIsolatedTestClassLoader(), dummyUrl);

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
        map.put("key-1", createIsolatedTestClassLoader());
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


    /**
     * 模拟 SeaTunnel CoordinatorService 实例。
     */
    static class MockCoordinatorService {
        private final Map<Long, Object> runningJobMasterMap = new ConcurrentHashMap<>();

        public Map<Long, Object> getRunningJobMasterMap() {
            return runningJobMasterMap;
        }
    }

    /**
     * 模拟 SeaTunnel Server 实例。
     */
    static class MockSeaTunnelServer {
        private final MockCoordinatorService coordinatorService;

        MockSeaTunnelServer(MockCoordinatorService coordinatorService) {
            this.coordinatorService = coordinatorService;
        }

        public MockCoordinatorService getCoordinatorService() {
            return coordinatorService;
        }
    }

    /**
     * 模拟 SeaTunnel JobMaster 实例。
     */
    static class MockJobMaster {
        private final long jobId;
        private Object physicalPlan = new Object();
        private Object logicalDag = new Object();
        private Object checkpointPlanMap = new HashMap<>();
        private Object jobDAGInfo = new Object();
        private Object checkpointManager = new Object();
        private Object jobImmutableInformation = new Object();
        private final MockSeaTunnelServer seaTunnelServer;

        MockJobMaster(long jobId, MockSeaTunnelServer seaTunnelServer) {
            this.jobId = jobId;
            this.seaTunnelServer = seaTunnelServer;
        }

        public long getJobId() {
            return jobId;
        }

        public Object getPhysicalPlan() {
            return physicalPlan;
        }

        public Object getLogicalDag() {
            return logicalDag;
        }
    }

    @Test
    @DisplayName("验证 cleanJobMaster 彻底置空 JobMaster 内部领域大对象并从 runningJobMasterMap 中剥离")
    void testCleanJobMasterSeveringGcRoots() {
        final MockCoordinatorService mockCoord = new MockCoordinatorService();
        final MockSeaTunnelServer mockServer = new MockSeaTunnelServer(mockCoord);
        final long testJobId = 777L;
        final MockJobMaster mockJm = new MockJobMaster(testJobId, mockServer);
        mockCoord.getRunningJobMasterMap().put(testJobId, mockJm);

        Assertions.assertNotNull(mockJm.getPhysicalPlan());
        Assertions.assertNotNull(mockJm.getLogicalDag());
        Assertions.assertEquals(1, mockCoord.getRunningJobMasterMap().size());

        // 执行 cleanJobMaster
        EngineClassLoaderCleaner.cleanJobMaster(mockJm);

        // 核心断言 1：JobMaster 内部领域大对象全被置 null 切断 GC Root
        Assertions.assertNull(mockJm.getPhysicalPlan());
        Assertions.assertNull(mockJm.getLogicalDag());

        // 核心断言 2：从 Coordinator 的 runningJobMasterMap 中物理剔除
        Assertions.assertFalse(mockCoord.getRunningJobMasterMap().containsKey(testJobId));
    }

    @Test
    @DisplayName("验证 closeClassLoaderSafely 与 cleanStaticCaches 安全守卫与幂等性")
    void testClassLoaderCloseAndCacheNullSafe() {
        // null 安全
        Assertions.assertDoesNotThrow(() -> EngineClassLoaderCleaner.closeClassLoaderSafely(null));
        Assertions.assertDoesNotThrow(() -> EngineClassLoaderCleaner.cleanStaticCaches(null));

        // 宿主与系统 ClassLoader 保护：绝对禁止执行 close 误杀环境
        final ClassLoader hostLoader = getClass().getClassLoader();
        Assertions.assertTrue(EngineClassLoaderCleaner.isSystemOrHostClassLoader(hostLoader));
        Assertions.assertTrue(EngineClassLoaderCleaner.isSystemOrHostClassLoader(ClassLoader.getSystemClassLoader()));
        Assertions.assertDoesNotThrow(() -> EngineClassLoaderCleaner.closeClassLoaderSafely(hostLoader));

        // 隔离的子 URLClassLoader：安全关闭与缓存清理
        final URLClassLoader isolated = createIsolatedTestClassLoader();
        Assertions.assertFalse(EngineClassLoaderCleaner.isSystemOrHostClassLoader(isolated));
        Assertions.assertDoesNotThrow(() -> EngineClassLoaderCleaner.closeClassLoaderSafely(isolated));
        Assertions.assertDoesNotThrow(() -> EngineClassLoaderCleaner.cleanStaticCaches(isolated));
    }
}

