package com.lingframe.agent.advice;

import com.lingframe.agent.bridge.LingFrameAgentBridge;
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
 *   <li>onEnter：从 classLoaderCache 中精确提取目标 ClassLoader（考虑 cacheMode 下 jobId=1L 重定向）</li>
 *   <li>onExit：无论 cacheMode 为何值，均清理 ThreadLocal 槽位</li>
 *   <li>物理释放判定：仅 !cacheMode 时核验 Key 是否已从 cache 中物理剥离，
 *       若已剥离则触发底座 {@code onPhysicalRelease} 契约统一收口治理</li>
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
        return jobMap.get(key);
    }

    @Advice.OnMethodExit
    public static void onExit(
            @Advice.FieldValue("cacheMode") boolean cacheMode,
            @Advice.FieldValue("classLoaderCache") Map<Long, Map<String, ClassLoader>> cache,
            @Advice.Argument(0) long jobId,
            @Advice.Argument(1) Collection<URL> jars,
            @Advice.Enter ClassLoader targetLoader
    ) {
        if (targetLoader == null) {
            return;
        }

        // 物理卸载判定：仅非缓存模式才核验是否需要物理释放
        // cacheMode=true 时 ClassLoader 被多作业共享，绝不能物理关闭
        if (!cacheMode) {
            final Map<String, ClassLoader> jobMap = cache.get(jobId);
            final String key = LingFrameAgentBridge.convertJarsToKey(jars);
            final boolean isRemoved = (jobMap == null || !jobMap.containsKey(key));
            if (isRemoved) {
                LingFrameAgentBridge.onPhysicalRelease(targetLoader);
            }
        }
    }

    private ClassLoaderReleaseAdvice() {
    }
}
