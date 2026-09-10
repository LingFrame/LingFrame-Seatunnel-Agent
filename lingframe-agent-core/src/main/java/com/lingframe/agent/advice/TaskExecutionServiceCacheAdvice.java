package com.lingframe.agent.advice;

import com.lingframe.agent.cleaner.EngineClassLoaderCleaner;
import net.bytebuddy.asm.Advice;

/**
 * 捕获引擎 {@code TaskExecutionService} 实例的字节码切面。
 * <p>
 * 织入 {@code org.apache.seatunnel.engine.server.TaskExecutionService#getExecutionContext}，
 * 该方法被引擎任务生命周期高频调用；onExit 时通过 {@code @Advice.This} 拿到宿主实例并交给
 * {@link EngineClassLoaderCleaner} 缓存，供其后台清空已完成作业上下文的 ClassLoader 引用。
 * <p>
 * 仅做实例捕获，不改任何方法返回语义，对引擎运行零侵入。
 */
public final class TaskExecutionServiceCacheAdvice {

    @Advice.OnMethodExit
    public static void onExit(@Advice.This Object self) {
        try {
            EngineClassLoaderCleaner.register(self);
        } catch (Throwable ignored) {
            // fail-open: advice 异常不得传播到宿主引擎
        }
    }

    private TaskExecutionServiceCacheAdvice() {
    }
}