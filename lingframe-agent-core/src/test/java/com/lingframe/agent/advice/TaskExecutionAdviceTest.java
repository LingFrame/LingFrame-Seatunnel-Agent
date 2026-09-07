package com.lingframe.agent.advice;

import com.lingframe.agent.bridge.LingFrameAgentBridge;
import com.lingframe.agent.bridge.LingGovernanceContract;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("TaskExecutionAdvice 批次调度治理切面测试")
class TaskExecutionAdviceTest {

    private TaskExecutionAdviceTest() {
    }

    /** 伪任务宿主：仅用于验证透传链路（@Advice.This 赋值）。 */
    private static final Object DUMMY_TASK = new Object();

    @BeforeEach
    void clearContract() {
        LingFrameAgentBridge.registerContract(null);
    }

    @AfterEach
    void teardownContract() {
        LingFrameAgentBridge.registerContract(null);
    }

    @Nested
    @DisplayName("无契约注册时的安全降级")
    class NoContractDegradation {

        @Test
        @DisplayName("onCallEnter 无契约时应安全跳过不抛异常")
        void shouldSafelySkipOnCallEnterWhenNoContract() {
            TaskExecutionAdvice.onCallEnter(DUMMY_TASK);
        }

        @Test
        @DisplayName("onCallExit 无契约时应安全跳过不抛异常")
        void shouldSafelySkipOnCallExitWhenNoContract() {
            TaskExecutionAdvice.onCallExit(null, DUMMY_TASK);
        }

        @Test
        @DisplayName("onCallExit 无契约且带异常时应安全跳过不抛异常")
        void shouldSafelySkipOnCallExitWithErrorWhenNoContract() {
            TaskExecutionAdvice.onCallExit(new RuntimeException("test"), DUMMY_TASK);
        }
    }

    @Nested
    @DisplayName("契约注册后的委托分发")
    class ContractDelegation {

        @Test
        @DisplayName("onCallEnter 应委托调用 beforeTaskCall 并透传 task")
        void shouldDelegateBeforeTaskCall() {
            final CallRecordingContract contract = new CallRecordingContract();
            LingFrameAgentBridge.registerContract(contract);
            TaskExecutionAdvice.onCallEnter(DUMMY_TASK);
            assertThat(contract.beforeCallCount.get()).isEqualTo(1);
            assertThat(contract.lastTask).isSameAs(DUMMY_TASK);
        }

        @Test
        @DisplayName("onCallExit 无异常时应委托调用 afterTaskCall")
        void shouldDelegateAfterTaskCallWithNullError() {
            final CallRecordingContract contract = new CallRecordingContract();
            LingFrameAgentBridge.registerContract(contract);
            TaskExecutionAdvice.onCallExit(null, DUMMY_TASK);
            assertThat(contract.afterCallCount.get()).isEqualTo(1);
            assertThat(contract.lastError).isNull();
            assertThat(contract.lastTask).isSameAs(DUMMY_TASK);
        }

        @Test
        @DisplayName("onCallExit 带异常时应委托调用 afterTaskCall")
        void shouldDelegateAfterTaskCallWithError() {
            final CallRecordingContract contract = new CallRecordingContract();
            LingFrameAgentBridge.registerContract(contract);
            final RuntimeException error = new RuntimeException("batch failed");
            TaskExecutionAdvice.onCallExit(error, DUMMY_TASK);
            assertThat(contract.afterCallCount.get()).isEqualTo(1);
            assertThat(contract.lastError).isSameAs(error);
        }

        @Test
        @DisplayName("多次调用应正确累计计数")
        void shouldAccumulateCallCounts() {
            final CallRecordingContract contract = new CallRecordingContract();
            LingFrameAgentBridge.registerContract(contract);
            for (int i = 0; i < 5; i++) {
                TaskExecutionAdvice.onCallEnter(DUMMY_TASK);
                TaskExecutionAdvice.onCallExit(null, DUMMY_TASK);
            }
            assertThat(contract.beforeCallCount.get()).isEqualTo(5);
            assertThat(contract.afterCallCount.get()).isEqualTo(5);
        }
    }

    private static final class CallRecordingContract implements LingGovernanceContract {

        final AtomicInteger beforeCallCount = new AtomicInteger();
        final AtomicInteger afterCallCount = new AtomicInteger();
        volatile Object lastTask;
        volatile Throwable lastError;

        @Override
        public boolean isGovernanceEnabled() {
            return true;
        }

        @Override
        public void onPhysicalRelease(ClassLoader classLoader) {
        }

        @Override
        public String convertJarsToKey(Collection<URL> jars) {
            return "";
        }

        @Override
        public void beforeTaskCall(Object task) {
            beforeCallCount.incrementAndGet();
            lastTask = task;
        }

        @Override
        public void afterTaskCall(Object task, Throwable error) {
            afterCallCount.incrementAndGet();
            lastTask = task;
            lastError = error;
        }
    }
}