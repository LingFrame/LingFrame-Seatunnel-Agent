package com.lingframe.agent.advice;

import com.lingframe.agent.bridge.LingFrameAgentBridge;
import net.bytebuddy.asm.Advice;

/**
 * Task.call() 批次调度治理切面。
 * <p>
 * 织入 {@code AbstractTask} 所有子类的 {@code call()} 方法，
 * 在 Worker 线程每次推进一个批次/时间片时触发治理。
 * <p>
 * 切点选择依据（全部已从 SeaTunnel 源码核实）：
 * <ul>
 *   <li>{@code Task.call()} 是真实存在的接口方法（{@code Task.java:40}）</li>
 *   <li>Worker 线程在 {@code do-while} 循环中反复调 {@code call()}（{@code TaskExecutionService.java:1144-1148}）</li>
 *   <li>{@code call()} 入口在 {@code checkpointLock} 外部，零死锁风险</li>
 *   <li>{@code call()} 异常被 {@code catch(Throwable)} 捕获，触发 Failover 从 Checkpoint 恢复，不破坏 2PC</li>
 * </ul>
 * <p>
 * 安全开关：默认关闭，需在 {@code lingframe-governance.yaml} 中显式配置
 * {@code governance.task-execution-advice-enabled: true} 才启用。
 */
public final class TaskExecutionAdvice {

    @Advice.OnMethodEnter
    public static void onCallEnter(@Advice.This Object task) {
        try {
            LingFrameAgentBridge.beforeTaskCall(task);
        } catch (Throwable ignored) {
            // fail-open: advice 异常不得传播到宿主引擎
        }
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class)
    public static void onCallExit(@Advice.Thrown Throwable thrown, @Advice.This Object task) {
        try {
            LingFrameAgentBridge.afterTaskCall(task, thrown);
        } catch (Throwable ignored) {
            // fail-open: advice 异常不得传播到宿主引擎
        }
    }

    private TaskExecutionAdvice() {
    }
}