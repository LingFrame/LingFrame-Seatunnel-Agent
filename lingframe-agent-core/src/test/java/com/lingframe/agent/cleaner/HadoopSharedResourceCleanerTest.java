package com.lingframe.agent.cleaner;

import net.bytebuddy.ByteBuddy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ClientCache;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.net.SocketFactory;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessControlContext;
import java.security.ProtectionDomain;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@DisplayName("Hadoop 共享资源清理：保留其他作业及共享服务")
class HadoopSharedResourceCleanerTest {
    @Test
    @DisplayName("真实 ReflectionUtils 缓存仅移除目标 Class 及其 Constructor")
    void removesOnlyReleasedConstructors() throws Exception {
        final Class<?> released = new ByteBuddy().subclass(Object.class).make()
                .load(getClass().getClassLoader()).getLoaded();
        final Class<?> active = new ByteBuddy().subclass(Object.class).make()
                .load(getClass().getClassLoader()).getLoaded();
        ReflectionUtils.newInstance(released, null);
        ReflectionUtils.newInstance(active, null);
        final Map<?, ?> cache = (Map<?, ?>) field(ReflectionUtils.class, "CONSTRUCTOR_CACHE").get(null);
        try {
            assertThat(cache.containsKey(released)).isTrue();
            assertThat(cache.containsKey(active)).isTrue();
            ConnectorResourceCleaner.cleanup(released.getClassLoader());
            assertThat(cache.containsKey(released)).isFalse();
            assertThat(cache.containsKey(active)).isTrue();
            ConnectorResourceCleaner.cleanup(released.getClassLoader());
            assertThat(ReflectionUtils.newInstance(active, null)).isInstanceOf(active);
        } finally {
            cache.remove(released);
            cache.remove(active);
        }
    }

    @Test
    @DisplayName("未启动的真实 HDFS 工厂断开 TCCL 与 ACC，原线程池仍可执行任务")
    void resetsInactiveFactoryWithoutStoppingSharedPool() throws Exception {
        final Field poolField = field(DFSClient.class, "STRIPED_READ_THREAD_POOL");
        final Object original = poolField.get(null);
        final ThreadPoolExecutor pool = DFSUtilClient.getThreadPoolExecutor(
                1, 1, 1, new LinkedBlockingQueue<>(), "test-striped-", false);
        try (URLClassLoader released = loader(); URLClassLoader active = loader()) {
            final Thread factory = (Thread) pool.getThreadFactory();
            factory.setContextClassLoader(released);
            final Field inherited = field(Thread.class, "inheritedAccessControlContext");
            final AccessControlContext context = new AccessControlContext(new ProtectionDomain[]{
                new ProtectionDomain(null, null, released, null)
            });
            inherited.set(factory, context);
            poolField.set(null, pool);

            ConnectorResourceCleaner.cleanup(active);
            assertThat(factory.getContextClassLoader()).isSameAs(released);
            assertThat(inherited.get(factory)).isSameAs(context);

            ConnectorResourceCleaner.cleanup(released);
            assertThat(poolField.get(null)).isSameAs(pool);
            assertThat(pool.getThreadFactory()).isSameAs(factory);
            assertThat(pool.isShutdown()).isFalse();
            assertThat(factory.getContextClassLoader()).isSameAs(DFSClient.class.getClassLoader());
            assertThat(inherited.get(factory)).isNull();
            assertThat(pool.submit(() -> "still usable").get(5, TimeUnit.SECONDS)).isEqualTo("still usable");
        } finally {
            poolField.set(null, original);
            pool.shutdownNow();
            assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    @DisplayName("RPC Client 继续共享，只修正已释放加载器对应 Configuration")
    void rebindsRpcConfigurationWithoutEvictingClients() throws Exception {
        final ClientCache cache = (ClientCache) field(ProtobufRpcEngine.class, "CLIENTS").get(null);
        try (URLClassLoader released = loader(); URLClassLoader active = loader()) {
            final Configuration releasedConf = new Configuration(false);
            releasedConf.setClassLoader(released);
            final Configuration activeConf = new Configuration(false);
            activeConf.setClassLoader(active);
            final SocketFactory releasedSocketFactory = mock(SocketFactory.class);
            final SocketFactory activeSocketFactory = mock(SocketFactory.class);
            final Client releasedClient = cache.getClient(releasedConf, releasedSocketFactory);
            final Client activeClient = cache.getClient(activeConf, activeSocketFactory);
            try {
                ConnectorResourceCleaner.cleanup(released);
                assertThat(releasedConf.getClassLoader()).isSameAs(ProtobufRpcEngine.class.getClassLoader());
                assertThat(activeConf.getClassLoader()).isSameAs(active);
                final Map<?, ?> clients = (Map<?, ?>) field(ClientCache.class, "clients").get(cache);
                assertThat(clients.get(releasedSocketFactory)).isSameAs(releasedClient);
                assertThat(clients.get(activeSocketFactory)).isSameAs(activeClient);
                final Client reused = cache.getClient(new Configuration(false), releasedSocketFactory);
                assertThat(reused).isSameAs(releasedClient);
                cache.stopClient(reused);
            } finally {
                cache.stopClient(releasedClient);
                cache.stopClient(activeClient);
            }
        }
    }

    private static URLClassLoader loader() {
        return new URLClassLoader(new URL[0], HadoopSharedResourceCleanerTest.class.getClassLoader());
    }

    private static Field field(Class<?> type, String name) throws Exception {
        final Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }
}
