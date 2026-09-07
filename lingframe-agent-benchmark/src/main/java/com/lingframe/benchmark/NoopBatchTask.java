package com.lingframe.benchmark;

import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.AbstractTask;

import java.net.URL;
import java.util.Collections;
import java.util.Set;

/**
 * 真实 {@link AbstractTask} 子类——JMH 基准中「被 {@code -javaagent} 织入」的载体。
 * <p>
 * 设计约束：
 * <ul>
 *   <li><b>包名不能落在 {@code com.lingframe.agent.*} 下</b>：premain 的 ByteBuddy
 *       ignore matcher 会排除 {@code com.lingframe.agent.} 前缀类型，若本类在其中将永不织入，
 *       基准会虚假测出「零损耗」。</li>
 *   <li>{@code call()} 与 {@link PlainBatchTask#call()} 严格同构（都只转调
 *       {@link BatchCore#process} 并把校验和存入实例字段），差异仅为本类继承 AbstractTask
 *       从而被切点命中——同 JVM A/B 对照的语义基础。</li>
 *   <li>SeaTunnel 3.0.0-SNAPSHOT（本地源码）中 {@code AbstractTask} 构造函数签名为
 *       {@code AbstractTask(long jobID, TaskLocation taskLocation)}。</li>
 * </ul>
 */
public final class NoopBatchTask extends AbstractTask implements BatchTask {

    private static final long serialVersionUID = 1L;

    private final int[] rows = BatchCore.buildRows();
    /** 模拟单批次墙钟耗时（纳秒）；由基准 @Setup 注入。 */
    private long sliceNanos;
    private long seed;
    private long lastChecksum;

    public NoopBatchTask(long jobId, long sliceNanos) {
        super(jobId, new TaskLocation(new TaskGroupLocation(jobId, 0, 1L), jobId, 0));
        this.sliceNanos = sliceNanos;
    }

    public void setSliceNanos(long sliceNanos) {
        this.sliceNanos = sliceNanos;
    }

    @Override
    public ProgressState call() {
        lastChecksum = BatchCore.process(rows, sliceNanos, seed++);
        // MADE_PROGRESS 模拟真实批次「有推进」的返回，贴近引擎语义
        return ProgressState.MADE_PROGRESS;
    }

    @Override
    public long getLastChecksum() {
        return lastChecksum;
    }

    @Override
    public Set<URL> getJarsUrl() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConnectorJarIdentifier> getConnectorPluginJars() {
        return Collections.emptySet();
    }
}
