package com.lingframe.agent.advice;

import com.lingframe.agent.cleaner.EngineClassLoaderCleaner;
import net.bytebuddy.asm.Advice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * 拦截 Coordinator 端 {@code JobMaster.cleanJob()} 的切面。
 * <p>
 * 当作业在 Master 终态执行清理时，提取其 jobId 并调用
 * {@link EngineClassLoaderCleaner#forceEvictJobClassLoaders(long)}，
 * 强制排空该作业在 Coordinator 节点上可能滞留的 ClassLoader 强引用缓存，
 * 防止因 DAG 解析或物理计划构建异常导致的 ClassLoader 永久泄漏。
 */
public final class JobMasterCleanJobAdvice {

    private static final Logger log = LoggerFactory.getLogger(JobMasterCleanJobAdvice.class);

    @Advice.OnMethodExit(onThrowable = Throwable.class)
    public static void onExit(@Advice.This Object jobMaster) {
        if (jobMaster == null) {
            return;
        }
        try {
            final Method getJobIdMethod = jobMaster.getClass().getMethod("getJobId");
            final Object idObj = getJobIdMethod.invoke(jobMaster);
            if (idObj instanceof Number) {
                final long jobId = ((Number) idObj).longValue();
                EngineClassLoaderCleaner.forceEvictJobClassLoaders(jobId);
            }
        } catch (Throwable t) {
            log.debug("JobMaster cleanJob interception failed: {}", t.getMessage());
        }
    }

    private JobMasterCleanJobAdvice() {
    }
}
