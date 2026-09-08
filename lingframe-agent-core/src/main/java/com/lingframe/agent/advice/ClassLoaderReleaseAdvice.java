package com.lingframe.agent.advice;

import com.lingframe.agent.bridge.LingFrameAgentBridge;
import com.lingframe.agent.cleaner.EngineClassLoaderCleaner;
import net.bytebuddy.asm.Advice;

import java.net.URL;
import java.util.Collection;
import java.util.Map;

/**
 * ClassLoader 释放拦截——双态自适应清理。
 * <p>
 * 拦截 {@code DefaultClassLoaderService.releaseClassLoader}，
 * 利用 {@code @Advice.FieldValue} 在织入期直接生成字段访问指令，零反射读取 cacheMode 和 classLoaderCache。
 * <p>
 * 治理策略：
 * <ol>
 *   <li>onEnter：从 classLoaderCache 中精确提取目标 ClassLoader 并登记追踪（考虑 cacheMode 下 jobId=1L 重定向）</li>
 *   <li>onExit：重置当前线程 TCCL 防止被释放的类加载器泄露挂起</li>
 * </ol>
 */
public final class ClassLoaderReleaseAdvice {

    @Advice.OnMethodEnter
    public static ClassLoader onEnter(
            @Advice.FieldValue("cacheMode") boolean cacheMode,
            @Advice.FieldValue("classLoaderCache") Map<Long, Map<String, ClassLoader>> cache,
            @Advice.Argument(0) long jobId,
            @Advice.Argument(1) Collection<URL> jars
    ) {
        // 从 cache 中精确提取目标 ClassLoader，而非盲目取 TCCL
        // SeaTunnel 在 cacheMode=true 时将 jobId 重定向到 1L，此处须对齐
        final long effectiveJobId = cacheMode ? 1L : jobId;
        final Map<String, ClassLoader> jobMap = cache.get(effectiveJobId);
        if (jobMap == null) {
            return null;
        }
        final String key = LingFrameAgentBridge.convertJarsToKey(jars);
        final ClassLoader cl = jobMap.get(key);
        if (cl != null && !cacheMode && jobId > 0) {
            EngineClassLoaderCleaner.trackJobClassLoader(jobId, cl);
        }
        return cl;
    }

    @Advice.OnMethodExit
    public static void onExit(
            @Advice.FieldValue("cacheMode") boolean cacheMode,
            @Advice.FieldValue("classLoaderCache") Map<Long, Map<String, ClassLoader>> cache,
            @Advice.Argument(0) long jobId,
            @Advice.Argument(1) Collection<URL> jars,
            @Advice.Enter ClassLoader targetLoader
    ) {
        // releaseClassLoader 仅为引擎内部引用计数递减（例如作业配置解析生成 DAG 后即会归还解析引用），
        // 绝不代表作业已终态，严禁在此阶段提前触发类加载器物理释放与关闭。
        if (Thread.currentThread().getContextClassLoader() == targetLoader) {
            Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
        }
    }

    private ClassLoaderReleaseAdvice() {
    }
}
