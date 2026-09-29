package com.lingframe.agent.cleaner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * 清理已物理释放的作业加载器持有的 MongoDB/Hadoop 资源。
 * 仅查询已经加载的类型，不在卸载过程中加载未使用的连接器。
 * 不保存连接器 Class、实例或 ClassLoader，避免清理器自身形成强引用。
 */
public final class ConnectorResourceCleaner {
    private static final Logger log = LoggerFactory.getLogger(ConnectorResourceCleaner.class);
    private static final String MONGO_POOL = "com.mongodb.internal.connection.PowerOfTwoBufferPool";
    private static final String CODEC_POOL = "org.apache.hadoop.io.compress.CodecPool";
    private static final String TOKEN = "org.apache.hadoop.security.token.Token";

    private ConnectorResourceCleaner() {
    }

    /** 仅从作业终态的物理释放入口调用，必须在关闭 URLClassLoader 之前执行。 */
    public static void cleanup(ClassLoader target) {
        if (EngineClassLoaderCleaner.isSystemOrHostClassLoader(target)) {
            return;
        }
        final Method findLoaded;
        try {
            findLoaded = ClassLoader.class.getDeclaredMethod("findLoadedClass", String.class);
            findLoaded.setAccessible(true);
        } catch (Exception e) {
            log.warn("Connector cleanup unavailable: cannot inspect loaded classes", e);
            return;
        }

        final Class<?> mongoPool = findLoaded(findLoaded, target, MONGO_POOL);
        if (mongoPool != null && mongoPool.getClassLoader() == target) {
            stopMongoPruner(mongoPool);
        }

        // Hadoop 可能由宿主或作业加载器定义；同一个父加载器类型只处理一次。
        final Set<Class<?>> visited = Collections.newSetFromMap(new IdentityHashMap<Class<?>, Boolean>());
        for (ClassLoader loader = target; loader != null; loader = loader.getParent()) {
            final Class<?> codecPool = findLoaded(findLoaded, loader, CODEC_POOL);
            if (codecPool != null && visited.add(codecPool)) {
                cleanCodecPool(codecPool, target);
            }
            final Class<?> token = findLoaded(findLoaded, loader, TOKEN);
            if (token != null && visited.add(token)) {
                cleanTokenRenewers(token, target);
            }
        }
    }

    private static Class<?> findLoaded(Method method, ClassLoader loader, String name) {
        try {
            return (Class<?>) method.invoke(loader, name);
        } catch (Exception e) {
            log.warn("Cannot inspect loaded connector class {}", name, e);
            return null;
        }
    }

    private static void stopMongoPruner(Class<?> poolType) {
        try {
            final Object pool = field(poolType, "DEFAULT").get(null);
            final Method disable = poolType.getDeclaredMethod("disablePruning");
            disable.setAccessible(true);
            // MongoDB 4.7.1 自身入口：关闭 DEFAULT.pruner，让线程正常退出并释放其 ACC/栈引用。
            disable.invoke(pool);
            log.info("Stopped MongoDB default buffer pool pruner for released ClassLoader {}",
                    poolType.getClassLoader());
        } catch (Throwable t) {
            log.warn("MongoDB buffer pool cleanup failed for {}", poolType.getClassLoader(), t);
        }
    }

    private static void cleanCodecPool(Class<?> poolType, ClassLoader target) {
        for (String name : new String[]{"compressorPool", "decompressorPool", "compressorCounts", "decompressorCounts"}) {
            try {
                final Object cache = field(poolType, name).get(null);
                final boolean idlePool = cache instanceof Map;
                final Map<?, ?> map;
                if (idlePool) {
                    map = (Map<?, ?>) cache;
                } else {
                    // Hadoop 同时在 Guava LoadingCache 中用 Class 作 key；只清理池会留下另一条强引用。
                    final Method asMap = cache.getClass().getMethod("asMap");
                    asMap.setAccessible(true);
                    map = (Map<?, ?>) asMap.invoke(cache);
                }
                int removed = 0;
                // 与 CodecPool.borrow/payback 的 map 锁保持一致。
                synchronized (map) {
                    final Iterator<? extends Map.Entry<?, ?>> entries = map.entrySet().iterator();
                    while (entries.hasNext()) {
                        final Map.Entry<?, ?> entry = entries.next();
                        if (entry.getKey() instanceof Class && ((Class<?>) entry.getKey()).getClassLoader() == target) {
                            final Object idleCodecs = entry.getValue();
                            entries.remove();
                            removed++;
                            if (idlePool && idleCodecs instanceof Set) {
                                endIdleCodecs((Set<?>) idleCodecs);
                            }
                        }
                    }
                }
                if (removed > 0) {
                    log.info("Removed {} Hadoop CodecPool.{} entries for released ClassLoader {}", removed, name, target);
                }
            } catch (Throwable t) {
                log.warn("Hadoop CodecPool.{} cleanup failed for {}", name, target, t);
            }
        }
    }

    private static void endIdleCodecs(Set<?> codecs) {
        synchronized (codecs) {
            for (Object codec : codecs) {
                try {
                    final Method end = codec.getClass().getMethod("end");
                    end.setAccessible(true);
                    end.invoke(codec);
                } catch (Exception e) {
                    log.warn("Failed to release idle codec {}", codec.getClass().getName(), e);
                }
            }
            codecs.clear();
        }
    }

    private static void cleanTokenRenewers(Class<?> tokenType, ClassLoader target) {
        // 作业私有的 Token 类随加载器一起卸载，不需要改写它自己的静态字段。
        if (tokenType.getClassLoader() == target) {
            return;
        }
        try {
            final Object value = field(tokenType, "renewers").get(null);
            if (!(value instanceof ServiceLoader)) {
                return;
            }
            final ServiceLoader<?> renewers = (ServiceLoader<?>) value;
            // Token.getRenewer 同步在 renewers 上；保留这个锁对象，避免替换静态字段导致锁失效。
            synchronized (renewers) {
                final Field loader = field(ServiceLoader.class, "loader");
                if (loader.get(renewers) != target) {
                    return;
                }
                // 保留启用 SecurityManager 时的授权上下文，不能通过卸载逻辑削弱安全边界。
                if (field(ServiceLoader.class, "acc").get(renewers) != null) {
                    log.warn("Cannot rebind Hadoop Token.renewers with an AccessControlContext for {}", target);
                    return;
                }
                loader.set(renewers, tokenType.getClassLoader());
                // JDK 8 的 LazyIterator 也持有旧 loader，必须连同 provider 缓存一起重建。
                renewers.reload();
                log.info("Rebound Hadoop Token.renewers to its defining ClassLoader, released {}", target);
            }
        } catch (Throwable t) {
            log.warn("Hadoop Token.renewers cleanup failed for {}", target, t);
        }
    }

    private static Field field(Class<?> type, String name) throws ReflectiveOperationException {
        final Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }
}
