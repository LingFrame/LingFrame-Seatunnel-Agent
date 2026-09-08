package com.lingframe.agent.advice;

import com.lingframe.agent.cleaner.EngineClassLoaderCleaner;
import net.bytebuddy.asm.Advice;

import java.net.URL;
import java.util.Collection;

/**
 * 拦截引擎 {@code DefaultClassLoaderService#getClassLoader} 的切面。
 * <p>
 * 无论是 Coordinator 还是 Worker 节点，当引擎为特定作业获取或创建 ClassLoader 时，
 * 在方法返回时自动按 {@code jobId} 登记到 {@link EngineClassLoaderCleaner} 的追踪表中，
 * 确保后续在作业终态能够精准排空该作业的所有类加载器。
 */
public final class ClassLoaderServiceGetAdvice {

    @Advice.OnMethodExit(onThrowable = Throwable.class)
    public static void onExit(
            @Advice.Argument(0) long jobId,
            @Advice.Argument(1) Collection<URL> jars,
            @Advice.Return ClassLoader classLoader
    ) {
        if (jobId > 0 && classLoader != null) {
            EngineClassLoaderCleaner.trackJobClassLoader(jobId, classLoader);
        }
    }

    private ClassLoaderServiceGetAdvice() {
    }
}
