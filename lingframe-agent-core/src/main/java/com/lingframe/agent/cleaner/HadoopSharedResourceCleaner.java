package com.lingframe.agent.cleaner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.security.ProtectionDomain;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

/** Hadoop 宿主缓存的定向断引用；保留其他作业仍在使用的线程池和 RPC Client。 */
final class HadoopSharedResourceCleaner {
    private static final Logger log = LoggerFactory.getLogger(HadoopSharedResourceCleaner.class);

    private HadoopSharedResourceCleaner() {
    }

    static void cleanup(Class<?> type, ClassLoader target) {
        if (type.getClassLoader() == target) {
            return;
        }
        try {
            switch (type.getName()) {
                case "org.apache.hadoop.util.ReflectionUtils":
                    cleanConstructors(type, target);
                    break;
                case "org.apache.hadoop.hdfs.DFSClient":
                    cleanStripedReadFactory(type, target);
                    break;
                case "org.apache.hadoop.ipc.ProtobufRpcEngine":
                    cleanRpcConfigurations(type, target);
                    break;
                default:
                    break;
            }
        } catch (Throwable t) {
            log.warn("Hadoop shared resource cleanup failed for {} and {}", type.getName(), target, t);
        }
    }

    private static void cleanConstructors(Class<?> type, ClassLoader target) throws Exception {
        final Map<?, ?> constructors = (Map<?, ?>) field(type, "CONSTRUCTOR_CACHE").get(null);
        int removed = 0;
        final Iterator<?> keys = constructors.keySet().iterator();
        while (keys.hasNext()) {
            final Object key = keys.next();
            if (key instanceof Class && ((Class<?>) key).getClassLoader() == target) {
                keys.remove();
                removed++;
            }
        }
        if (removed > 0) {
            log.info("Removed {} Hadoop ReflectionUtils constructor entries for {}", removed, target);
        }
    }

    private static void cleanStripedReadFactory(Class<?> type, ClassLoader target) throws Exception {
        final Object value = field(type, "STRIPED_READ_THREAD_POOL").get(null);
        if (!(value instanceof ThreadPoolExecutor)) {
            return;
        }
        final ThreadFactory factory = ((ThreadPoolExecutor) value).getThreadFactory();
        // Hadoop DaemonFactory 继承 Thread，但从未 start；活动线程枚举看不到这个对象。
        if (!(factory instanceof Thread) || factory.getClass().getClassLoader() != type.getClassLoader()) {
            return;
        }
        final Thread factoryThread = (Thread) factory;
        if (factoryThread.getState() != Thread.State.NEW) {
            return;
        }
        if (factoryThread.getContextClassLoader() == target) {
            factoryThread.setContextClassLoader(type.getClassLoader());
            log.info("Reset Hadoop striped-read thread factory TCCL for {}", target);
        }
        // 只处理从未启动的工厂对象，不改写活动线程的权限上下文。
        if (System.getSecurityManager() != null) {
            log.warn("Skipping Hadoop factory AccessControlContext cleanup with SecurityManager enabled");
            return;
        }
        final Field inherited = field(Thread.class, "inheritedAccessControlContext");
        final Object context = inherited.get(factoryThread);
        if (context != null) {
            final ProtectionDomain[] domains = (ProtectionDomain[]) field(context.getClass(), "context").get(context);
            if (domains != null) {
                for (ProtectionDomain domain : domains) {
                    if (domain != null && domain.getClassLoader() == target) {
                        inherited.set(factoryThread, null);
                        log.info("Cleared released ClassLoader from inactive Hadoop factory AccessControlContext: {}", target);
                        break;
                    }
                }
            }
        }
    }

    private static void cleanRpcConfigurations(Class<?> type, ClassLoader target) throws Exception {
        final Object clientCache = field(type, "CLIENTS").get(null);
        if (clientCache == null) {
            return;
        }
        // 与 ClientCache.getClient / stopClient 对 clients 的访问使用同一个监视器。
        synchronized (clientCache) {
            final Map<?, ?> clients = (Map<?, ?>) field(clientCache.getClass(), "clients").get(clientCache);
            for (Object client : clients.values()) {
                if (client == null || client.getClass().getClassLoader() != type.getClassLoader()) {
                    continue;
                }
                final Object conf = field(client.getClass(), "conf").get(client);
                if (conf != null && conf.getClass().getMethod("getClassLoader").invoke(conf) == target) {
                    // ClientCache 按 SocketFactory 跨作业共享 Client；不能 stopClient 或删除缓存项。
                    conf.getClass().getMethod("setClassLoader", ClassLoader.class).invoke(conf, type.getClassLoader());
                    log.info("Rebound shared Hadoop RPC Configuration to host ClassLoader, released {}", target);
                }
            }
        }
    }

    private static Field field(Class<?> type, String name) throws ReflectiveOperationException {
        final Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }
}
