package com.lingframe.agent.adapter;

import com.lingframe.api.exception.LingInvocationException;

/**
 * 弹性治理硬拒绝异常。
 * <p>
 * 当 {@code governance.resilience.fail-closed: true} 且治理流水线判定为硬拒绝
 * （{@link com.lingframe.api.exception.LingInvocationException.ErrorKind#CIRCUIT_OPEN}
 * / {@link com.lingframe.api.exception.LingInvocationException.ErrorKind#BULKHEAD_FULL}）时抛出，
 * 使被织入的 {@code AbstractTask.call()} 携带异常逃逸，由 SeaTunnel Worker 的
 * {@code catch(Throwable)} 触发 Failover / 任务失败——而非 fail-open 软退避（sleep 后放行）。
 * <p>
 * 这是「熔断名副其实」的语义落点：电路打开时真正拒绝请求，让下游持续失败的任务快速失败，
 * 而非静默继续推进、把异常计入失败率自我放大延迟。
 * <p>
 * 默认 {@code fail-closed: false}（软退避）以保证向后安全：硬拒绝会让反复失败的批次持续触发
 * 引擎 Failover，故仅作显式 opt-in。软退避路径对 {@code RATE_LIMITED}（平滑限流）始终生效，
 * 因为丢弃限流批次等同数据丢失，不符合批次调度语义。
 */
public final class GovernanceRejectException extends RuntimeException {

    public GovernanceRejectException(LingInvocationException cause) {
        super("Governance circuit breaker hard-rejected task execution: " + cause.getKind(), cause);
    }
}
