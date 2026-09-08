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

 * </ol>
 */
public final class ClassLoaderReleaseAdvice {

    @Advice.OnMethodEnter
    public static void onEnter(
            @Advice.FieldValue("cacheMode") boolean cacheMode,
            @Advice.FieldValue("classLoaderCache") Map<Long, Map<String, ClassLoader>> cache,
            @Advice.Argument(0) long jobId,
            @Advice.Argument(1) Collection<URL> jars
    ) {
        // 从 cache 中精确提取目标 ClassLoader 并登记追踪
        // SeaTunnel 在 cacheMode=true 时将 jobId 重定向到 1L，此处须对齐
        final long effectiveJobId = cacheMode ? 1L : jobId;
        final Map<String, ClassLoader> jobMap = cache.get(effectiveJobId);
        if (jobMap == null) {
            return;
        }
        final String key = LingFrameAgentBridge.convertJarsToKey(jars);
        final ClassLoader cl = jobMap.get(key);
        if (cl != null && !cacheMode && jobId > 0) {
            EngineClassLoaderCleaner.trackJobClassLoader(jobId, cl);
        }
    }

    private ClassLoaderReleaseAdvice() {
    }
}
