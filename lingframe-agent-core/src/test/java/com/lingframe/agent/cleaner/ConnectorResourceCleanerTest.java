package com.lingframe.agent.cleaner;

import com.lingframe.agent.adapter.SeaTunnelAdapter;
import com.lingframe.agent.config.TestAgentConfigs;
import com.mongodb.internal.connection.PowerOfTwoBufferPool;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.bson.ByteBuf;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessControlContext;
import java.security.ProtectionDomain;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("连接器资源清理：真实 Hadoop/MongoDB 持有链")
class ConnectorResourceCleanerTest {
    @Test
    @DisplayName("移除目标压缩器与解压器的池 key、计数 key，并保留其他作业")
    void removesOnlyReleasedCodecClasses() throws Exception {
        try (URLClassLoader released = isolatedLoader(); URLClassLoader active = isolatedLoader()) {
            final AtomicInteger ended = new AtomicInteger();
            final AtomicInteger activeEnded = new AtomicInteger();
            final CompressionCodec releasedCodec = pooledCodec(released, ended, false);
            final CompressionCodec activeCodec = pooledCodec(active, activeEnded, false);
            try {
                // 借出一个仍属于其他作业的实例，清理不能改写它的计数。
                final Compressor borrowed = CodecPool.getCompressor(activeCodec);
                ConnectorResourceCleaner.cleanup(released);
                assertThat(ended).hasValue(2);
                assertThat(activeEnded).hasValue(0);
                assertAbsentFromPools(releasedCodec);
                assertThat(codecMap("compressorPool")).containsKey(activeCodec.getCompressorType());
                assertThat(codecMap("decompressorPool")).containsKey(activeCodec.getDecompressorType());
                assertThat(CodecPool.getLeasedCompressorsCount(activeCodec)).isEqualTo(1);
                CodecPool.returnCompressor(borrowed);
                ConnectorResourceCleaner.cleanup(released);
                assertThat(ended).hasValue(2);
            } finally {
                ConnectorResourceCleaner.cleanup(active);
            }
        }
    }

    @Test
    @DisplayName("codec.end 失败也移除强引用，其他清理继续执行")
    void failingCodecEndDoesNotKeepCacheKeys() throws Exception {
        try (URLClassLoader loader = isolatedLoader()) {
            final CompressionCodec codec = pooledCodec(loader, new AtomicInteger(), true);
            assertThatCode(() -> ConnectorResourceCleaner.cleanup(loader)).doesNotThrowAnyException();
            assertAbsentFromPools(codec);
        }
    }

    @Test
    @DisplayName("Token renewers 保留同步锁，重新绑定宿主加载器且不触碰其他作业")
    void rebindsTokenServiceLoaderUnderExistingMonitor() throws Exception {
        final Field renewersField = field(Token.class, "renewers");
        final Object original = renewersField.get(null);
        try (URLClassLoader released = isolatedLoader(); URLClassLoader active = isolatedLoader()) {
            final ServiceLoader<TokenRenewer> renewers = ServiceLoader.load(TokenRenewer.class, released);
            renewersField.set(null, renewers);
            ConnectorResourceCleaner.cleanup(active);
            assertThat(field(ServiceLoader.class, "loader").get(renewers)).isSameAs(released);
            ConnectorResourceCleaner.cleanup(released);
            assertThat(renewersField.get(null)).isSameAs(renewers);
            assertThat(field(ServiceLoader.class, "loader").get(renewers)).isSameAs(Token.class.getClassLoader());
            // JDK 8 LazyIterator 自身有 loader 字段，验证 reload 确实切断了第二条引用。
            for (Field member : ServiceLoader.class.getDeclaredFields()) {
                if (member.getName().equals("lookupIterator")) {
                    member.setAccessible(true);
                    final Object iterator = member.get(renewers);
                    assertThat(field(iterator.getClass(), "loader").get(iterator)).isSameAs(Token.class.getClassLoader());
                }
            }
        } finally {
            renewersField.set(null, original);
        }
    }

    @Test
    @DisplayName("cleanup-only 物理释放关闭真实 Mongo pruner，保留其他作业且无需线程 TCCL")
    void physicalReleaseStopsOnlyOwnedMongoPruner() throws Exception {
        final URL[] driverJars = {
                PowerOfTwoBufferPool.class.getProtectionDomain().getCodeSource().getLocation(),
                ByteBuf.class.getProtectionDomain().getCodeSource().getLocation()
        };
        try (URLClassLoader released = new URLClassLoader(driverJars, null);
             URLClassLoader active = new URLClassLoader(driverJars, null)) {
            final ExecutorService releasedPruner = startPruner(released);
            final ExecutorService activePruner = startPruner(active);
            try {
                for (Thread thread : Thread.getAllStackTraces().keySet()) {
                    if (thread.getContextClassLoader() == released) {
                        thread.setContextClassLoader(ClassLoader.getSystemClassLoader());
                    }
                }
                final SeaTunnelAdapter adapter = new SeaTunnelAdapter(
                        TestAgentConfigs.create(false, false, false, false, false, false),
                        null, null, null, null, null);
                adapter.onPhysicalRelease(released);
                assertThat(releasedPruner.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                assertThat(activePruner.isShutdown()).isFalse();
                ConnectorResourceCleaner.cleanup(released);
            } finally {
                releasedPruner.shutdownNow();
                activePruner.shutdownNow();
                assertThat(activePruner.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Test
    @DisplayName("保留带授权上下文的 ServiceLoader，不通过清理绕过安全边界")
    void preservesServiceLoaderSecurityContext() throws Exception {
        final Field renewersField = field(Token.class, "renewers");
        final Object original = renewersField.get(null);
        try (URLClassLoader released = isolatedLoader()) {
            final ServiceLoader<TokenRenewer> renewers = ServiceLoader.load(TokenRenewer.class, released);
            final AccessControlContext context = new AccessControlContext(new ProtectionDomain[0]);
            field(ServiceLoader.class, "acc").set(renewers, context);
            renewersField.set(null, renewers);
            ConnectorResourceCleaner.cleanup(released);
            assertThat(field(ServiceLoader.class, "loader").get(renewers)).isSameAs(released);
            assertThat(field(ServiceLoader.class, "acc").get(renewers)).isSameAs(context);
        } finally {
            renewersField.set(null, original);
        }
    }

    @Test
    @DisplayName("父加载器共享的 MongoDB 默认池不能随子作业关闭")
    void preservesParentOwnedMongoPruner() throws Exception {
        final ExecutorService shared = startPruner(PowerOfTwoBufferPool.class.getClassLoader());
        try (URLClassLoader released = isolatedLoader()) {
            Class.forName(PowerOfTwoBufferPool.class.getName(), true, released);
            ConnectorResourceCleaner.cleanup(released);
            assertThat(shared.isShutdown()).isFalse();
        } finally {
            shared.shutdownNow();
            assertThat(shared.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    @DisplayName("跳过宿主、空加载器以及尚未加载的驱动")
    void skipsHostAndDoesNotLoadUnusedDrivers() throws Exception {
        ConnectorResourceCleaner.cleanup(null);
        ConnectorResourceCleaner.cleanup(ClassLoader.getSystemClassLoader());
        final AtomicInteger loadCalls = new AtomicInteger();
        try (URLClassLoader loader = new URLClassLoader(new URL[0], getClass().getClassLoader()) {
            @Override
            public Class<?> loadClass(String name) throws ClassNotFoundException {
                loadCalls.incrementAndGet();
                return super.loadClass(name);
            }
        }) {
            ConnectorResourceCleaner.cleanup(loader);
            assertThat(loadCalls).hasValue(0);
        }
    }

    private static ExecutorService startPruner(ClassLoader loader) throws Exception {
        final Thread current = Thread.currentThread();
        final ClassLoader previous = current.getContextClassLoader();
        try {
            current.setContextClassLoader(loader);
            final Class<?> type = Class.forName(PowerOfTwoBufferPool.class.getName(), true, loader);
            return (ExecutorService) field(type, "pruner").get(type.getField("DEFAULT").get(null));
        } finally {
            current.setContextClassLoader(previous);
        }
    }

    private static CompressionCodec pooledCodec(ClassLoader loader, AtomicInteger ended, boolean failEnd) {
        final Compressor compressor = proxyCodec(loader, Compressor.class, ended, failEnd);
        final Decompressor decompressor = proxyCodec(loader, Decompressor.class, ended, failEnd);
        final CompressionCodec codec = mock(CompressionCodec.class);
        doReturn(compressor.getClass()).when(codec).getCompressorType();
        doReturn(decompressor.getClass()).when(codec).getDecompressorType();
        when(codec.createCompressor()).thenReturn(compressor);
        when(codec.createDecompressor()).thenReturn(decompressor);
        CodecPool.returnCompressor(CodecPool.getCompressor(codec));
        CodecPool.returnDecompressor(CodecPool.getDecompressor(codec));
        return codec;
    }

    private static <T> T proxyCodec(ClassLoader loader, Class<T> type, AtomicInteger ended, boolean failEnd) {
        return type.cast(Proxy.newProxyInstance(loader, new Class<?>[]{type}, (proxy, method, args) -> {
            if ("end".equals(method.getName())) {
                ended.incrementAndGet();
                if (failEnd) {
                    throw new IllegalStateException("test codec end failure");
                }
            }
            if ("hashCode".equals(method.getName())) {
                return System.identityHashCode(proxy);
            }
            if ("equals".equals(method.getName())) {
                return proxy == args[0];
            }
            return null;
        }));
    }

    private static void assertAbsentFromPools(CompressionCodec codec) throws Exception {
        assertThat(codecMap("compressorPool")).doesNotContainKey(codec.getCompressorType());
        assertThat(codecMap("decompressorPool")).doesNotContainKey(codec.getDecompressorType());
        assertThat(codecMap("compressorCounts")).doesNotContainKey(codec.getCompressorType());
        assertThat(codecMap("decompressorCounts")).doesNotContainKey(codec.getDecompressorType());
    }

    @SuppressWarnings("unchecked")
    private static Map<Class<?>, ?> codecMap(String name) throws Exception {
        final Object cache = field(CodecPool.class, name).get(null);
        if (cache instanceof Map) {
            return (Map<Class<?>, ?>) cache;
        }
        return ((com.google.common.cache.LoadingCache<Class<?>, ?>) cache).asMap();
    }

    private static Field field(Class<?> type, String name) throws Exception {
        final Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }

    private static URLClassLoader isolatedLoader() {
        return new URLClassLoader(new URL[0], ConnectorResourceCleanerTest.class.getClassLoader());
    }
}
