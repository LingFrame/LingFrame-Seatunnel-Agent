package com.lingframe.agent.advice;

import com.lingframe.agent.cleaner.EngineClassLoaderCleaner;
import net.bytebuddy.asm.Advice;

/**
 * 拦截 Coordinator 端 {@code JobMaster.cleanJob()} 与 {@code JobMaster.run()} 的切面。
 * <p>
 * 当作业在 Master 终态执行清理或退出运行态时，调用
 * {@link EngineClassLoaderCleaner#cleanJobMaster(Object)}，
 * 强制排空该作业在 Coordinator 节点上的 ClassLoader 强引用缓存，从 runningJobMasterMap 中剥离，
 * 并深度置空 JobMaster 内部持有的 physicalPlan、logicalDag 等核心领域对象，
 * 彻底斩断 Coordinator 端指向 Connector 类加载器的 GC Root。
 */
public final class JobMasterCleanJobAdvice {

    @Advice.OnMethodExit(onThrowable = Throwable.class)
    public static void onExit(@Advice.This Object jobMaster) {
        try {
            if (jobMaster != null) {
                EngineClassLoaderCleaner.cleanJobMaster(jobMaster);
            }
        } catch (Throwable ignored) {
            // fail-open: advice 异常不得传播到宿主引擎
        }
    }

    private JobMasterCleanJobAdvice() {
    }
}

