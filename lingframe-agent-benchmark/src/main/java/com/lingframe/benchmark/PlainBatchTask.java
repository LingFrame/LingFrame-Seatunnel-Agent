package com.lingframe.benchmark;

import org.apache.seatunnel.engine.server.execution.ProgressState;

/**
 * {@link NoopBatchTask} 的「同 JVM 内对照组」：{@code call()} 方法体与其严格同构
 * （同一份 {@link BatchCore} 载荷），但类<b>不继承</b> {@code AbstractTask}——
 * ByteBuddy 切点按 {@code hasSuperType(AbstractTask)} 匹配，本类永不织入。
 * <p>
 * A/B 语义：governed fork 中同时测量 {@code NoopBatchTask.call()}（被织入）与本类
 * {@code call()}（未织入），二者共享同一 JVM / 同一批 JIT、GC 压力——跨 fork 无法对消的
 * 频率噪声在 fork 内被同时消除，微损耗量级才可测。no-agent / default fork 中两条路径
 * 都未织入，应无差——用于自证测量装置灵敏度。
 */
public final class PlainBatchTask implements BatchTask {

    private final int[] rows = BatchCore.buildRows();
    /** 与 {@link NoopBatchTask#sliceNanos} 同义，由基准 @Setup 注入同一值。 */
    private long sliceNanos;
    private long seed;
    private long lastChecksum;

    public PlainBatchTask(long sliceNanos) {
        this.sliceNanos = sliceNanos;
    }

    public void setSliceNanos(long sliceNanos) {
        this.sliceNanos = sliceNanos;
    }

    @Override
    public ProgressState call() {
        lastChecksum = BatchCore.process(rows, sliceNanos, seed++);
        return ProgressState.MADE_PROGRESS;
    }

    @Override
    public long getLastChecksum() {
        return lastChecksum;
    }
}
