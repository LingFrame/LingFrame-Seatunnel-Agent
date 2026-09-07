package com.lingframe.agent.bridge;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

@DisplayName("LingFrameAgentBridge 桥接入口测试")
class LingFrameAgentBridgeTest {

    @AfterEach
    void cleanupContract() {
        LingFrameAgentBridge.registerContract(null);
    }

    @Nested
    @DisplayName("无契约注册时的安全降级")
    class NoContractDegradation {

        @Test
        @DisplayName("isGovernanceEnabled 应返回 false")
        void shouldReturnFalseWhenNoContract() {
            assertThat(LingFrameAgentBridge.isGovernanceEnabled()).isFalse();
            assertThat(LingFrameAgentBridge.getContract()).isNull();
        }

        @Test
        @DisplayName("onPhysicalRelease 无契约时应安全静默不抛异常")
        void shouldNotThrowOnPhysicalReleaseWhenNoContract() {
            assertThatCode(() -> LingFrameAgentBridge.onPhysicalRelease(new ClassLoader() { }))
                    .doesNotThrowAnyException();
            assertThatCode(() -> LingFrameAgentBridge.onPhysicalRelease(null))
                    .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("convertJarsToKey 无契约时应安全回退到排序拼接")
        void shouldFallbackConvertJarsToKeyWhenNoContract() throws MalformedURLException {
            assertThat(LingFrameAgentBridge.convertJarsToKey(Collections.emptyList())).isEmpty();

            final List<URL> jars = Arrays.asList(
                    new URL("file:/z.jar"),
                    new URL("file:/a.jar")
            );
            final String key = LingFrameAgentBridge.convertJarsToKey(jars);
            assertThat(key).isEqualTo("file:/a.jarfile:/z.jar");
        }

        @Test
        @DisplayName("beforeTaskCall 与 afterTaskCall 无契约时应安全静默不抛异常")
        void shouldNotThrowOnTaskHooksWhenNoContract() {
            final Object task = new Object();
            assertThatCode(() -> LingFrameAgentBridge.beforeTaskCall(task))
                    .doesNotThrowAnyException();
            assertThatCode(() -> LingFrameAgentBridge.afterTaskCall(null, null))
                    .doesNotThrowAnyException();
            assertThatCode(() -> LingFrameAgentBridge.afterTaskCall(task, new RuntimeException("test")))
                    .doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("契约注册后的委托分发")
    class ContractDelegation {

        @Test
        @DisplayName("isGovernanceEnabled 应委托给契约")
        void shouldDelegateIsGovernanceEnabled() {
            final RecordingContract contract = new RecordingContract();
            LingFrameAgentBridge.registerContract(contract);

            assertThat(LingFrameAgentBridge.isGovernanceEnabled()).isTrue();
            assertThat(contract.isGovernanceEnabledCalled).isTrue();
            assertThat(LingFrameAgentBridge.getContract()).isSameAs(contract);
        }

        @Test
        @DisplayName("onPhysicalRelease 应准确委托给契约")
        void shouldDelegateOnPhysicalRelease() {
            final RecordingContract contract = new RecordingContract();
            LingFrameAgentBridge.registerContract(contract);

            final ClassLoader cl = new ClassLoader() { };
            LingFrameAgentBridge.onPhysicalRelease(cl);

            assertThat(contract.releasedClassLoader).isSameAs(cl);
        }

        @Test
        @DisplayName("convertJarsToKey 应准确委托给契约")
        void shouldDelegateConvertJarsToKey() {
            final RecordingContract contract = new RecordingContract();
            LingFrameAgentBridge.registerContract(contract);

            final String result = LingFrameAgentBridge.convertJarsToKey(Collections.emptyList());

            assertThat(result).isEqualTo("recorded-key");
            assertThat(contract.convertedJars).isEmpty();
        }

        @Test
        @DisplayName("beforeTaskCall 与 afterTaskCall 应准确委托给契约并透传 task")
        void shouldDelegateTaskHooks() {
            final RecordingContract contract = new RecordingContract();
            LingFrameAgentBridge.registerContract(contract);

            final Object task = new Object();
            LingFrameAgentBridge.beforeTaskCall(task);
            assertThat(contract.beforeTaskCallCalled).isTrue();
            assertThat(contract.beforeTaskCallTask).isSameAs(task);

            final Throwable error = new RuntimeException("boom");
            LingFrameAgentBridge.afterTaskCall(task, error);
            assertThat(contract.afterTaskCallTask).isSameAs(task);
            assertThat(contract.afterTaskCallError).isSameAs(error);
        }
    }

    private static class RecordingContract implements LingGovernanceContract {
        private boolean isGovernanceEnabledCalled;
        private ClassLoader releasedClassLoader;
        private Collection<URL> convertedJars;
        private boolean beforeTaskCallCalled;
        private Object beforeTaskCallTask;
        private Object afterTaskCallTask;
        private Throwable afterTaskCallError;

        @Override
        public boolean isGovernanceEnabled() {
            isGovernanceEnabledCalled = true;
            return true;
        }

        @Override
        public void onPhysicalRelease(ClassLoader classLoader) {
            this.releasedClassLoader = classLoader;
        }

        @Override
        public String convertJarsToKey(Collection<URL> jars) {
            this.convertedJars = jars;
            return "recorded-key";
        }

        @Override
        public void beforeTaskCall(Object task) {
            this.beforeTaskCallCalled = true;
            this.beforeTaskCallTask = task;
        }

        @Override
        public void afterTaskCall(Object task, Throwable error) {
            this.afterTaskCallTask = task;
            this.afterTaskCallError = error;
        }
    }
}
