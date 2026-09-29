package com.lingframe.agent.cleaner;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 复现指标线程先复制引用、清理线程随后移除条目、指标线程继续读取的交错顺序。 */
class EngineContextSnapshotTest {
    @BeforeEach
    void reset() {
        EngineClassLoaderCleaner.resetForTesting();
    }

    @Test
    void finishedContextSnapshotSurvivesCleanup() throws Exception {
        verifySnapshot("purgeFinishedExecutionContexts", "finishedExecutionContexts");
    }

    @Test
    void jobContextSnapshotSurvivesCleanup() throws Exception {
        verifySnapshot("removeContextsByJobId", "executionContexts");
    }

    @Test
    void staleContextSnapshotSurvivesCleanup() throws Exception {
        verifySnapshot("cleanStaleExecutionContexts", "executionContexts");
    }

    private void verifySnapshot(String cleanup, String mapName) throws Exception {
        final EngineClassLoaderCleanerTest.MockTaskExecutionService service =
                new EngineClassLoaderCleanerTest.MockTaskExecutionService();
        final EngineClassLoaderCleanerTest.MockCoordinatorService coordinator =
                new EngineClassLoaderCleanerTest.MockCoordinatorService();
        coordinator.getRunningJobMasterMap().put(2L, new Object());
        EngineClassLoaderCleaner.registerCoordinatorService(coordinator);
        final Object taskGroup = new Object();
        final Object location = new EngineClassLoaderCleanerTest.MockTaskGroupLocation(1L);
        final Map<Object, Object> contexts = "finishedExecutionContexts".equals(mapName)
                ? service.getFinishedExecutionContexts() : service.getExecutionContexts();
        final Object activeLocation = new EngineClassLoaderCleanerTest.MockTaskGroupLocation(2L);
        final Object activeContext = new Object();
        service.getExecutionContexts().put(activeLocation, activeContext);
        try (URLClassLoader loader = new URLClassLoader(new URL[0], null)) {
            final EngineClassLoaderCleanerTest.MockTaskGroupContext context =
                    new EngineClassLoaderCleanerTest.MockTaskGroupContext(
                            taskGroup, location, loader, new URL("file:///snapshot.jar"));
            contexts.put(location, context);
            // 与 TaskExecutionService.provideDynamicMetrics 相同：只复制 Map，不复制上下文。
            final Map<Object, Object> snapshot = new HashMap<>(contexts);
            final Method method;
            if ("purgeFinishedExecutionContexts".equals(cleanup)) {
                method = EngineClassLoaderCleaner.class.getDeclaredMethod(cleanup, Object.class);
                method.setAccessible(true);
                method.invoke(null, service);
            } else if ("removeContextsByJobId".equals(cleanup)) {
                method = EngineClassLoaderCleaner.class.getDeclaredMethod(
                        cleanup, Object.class, String.class, long.class);
                method.setAccessible(true);
                method.invoke(null, service, mapName, 1L);
            } else {
                method = EngineClassLoaderCleaner.class.getDeclaredMethod(cleanup, Object.class, Set.class);
                method.setAccessible(true);
                method.invoke(null, service, new HashSet<Long>());
            }
            assertTrue(!contexts.containsKey(location), "引擎必须释放已完成上下文的持有引用");
            assertSame(activeContext, service.getExecutionContexts().get(activeLocation));
            final EngineClassLoaderCleanerTest.MockTaskGroupContext observed =
                    (EngineClassLoaderCleanerTest.MockTaskGroupContext) snapshot.get(location);
            assertSame(taskGroup, observed.getTaskGroup(), "已取得快照的指标读者必须能继续读取");
            assertSame(loader, observed.getClassLoaders().get(location));
            assertEquals(1, observed.getJars().size());
        }
    }
}
