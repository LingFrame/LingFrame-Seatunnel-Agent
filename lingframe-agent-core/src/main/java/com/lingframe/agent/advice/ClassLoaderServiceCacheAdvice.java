package com.lingframe.agent.advice;

import com.lingframe.agent.cleaner.EngineClassLoaderCleaner;
import net.bytebuddy.asm.Advice;

/**
 * 捕获引擎 {@code DefaultClassLoaderService} 单例实例的切面。
 * <p>
 * 织入 {@code DefaultClassLoaderService} 构造函数，onExit 时捕获实例并传递给
 * {@link EngineClassLoaderCleaner#registerClassLoaderService(Object)} 缓存，
 * 供后续作业终态与异常兜底时强制排空滞留的 ClassLoader 缓存强引用。
 */
public final class ClassLoaderServiceCacheAdvice {

    @Advice.OnMethodExit
    public static void onExit(@Advice.This Object self) {
        EngineClassLoaderCleaner.registerClassLoaderService(self);
    }

    private ClassLoaderServiceCacheAdvice() {
    }
}
