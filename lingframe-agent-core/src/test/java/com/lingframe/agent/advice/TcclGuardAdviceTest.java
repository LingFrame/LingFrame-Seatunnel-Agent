package com.lingframe.agent.advice;

import com.lingframe.agent.bridge.ReleasedClassLoaderRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("TcclGuardAdvice TCCL 拘留防御测试")
class TcclGuardAdviceTest {

    @AfterEach
    void cleanup() {
        ReleasedClassLoaderRegistry.clear();
    }

    @Nested
    @DisplayName("ReleasedClassLoaderRegistry 注册表行为")
    class RegistryBehavior {

        @Test
        @DisplayName("register 后 isReleased 应返回 true")
        void shouldReturnTrueAfterRegister() {
            final ClassLoader loader = new ClassLoader() { };
            ReleasedClassLoaderRegistry.register(loader);
            assertThat(ReleasedClassLoaderRegistry.isReleased(loader)).isTrue();
        }

        @Test
        @DisplayName("未 register 的 ClassLoader isReleased 应返回 false")
        void shouldReturnFalseForUnregistered() {
            final ClassLoader loader = new ClassLoader() { };
            assertThat(ReleasedClassLoaderRegistry.isReleased(loader)).isFalse();
        }

        @Test
        @DisplayName("null ClassLoader isReleased 应返回 false")
        void shouldReturnFalseForNull() {
            assertThat(ReleasedClassLoaderRegistry.isReleased(null)).isFalse();
        }

        @Test
        @DisplayName("register null 应安全跳过")
        void shouldSkipRegisterNull() {
            ReleasedClassLoaderRegistry.register(null);
            assertThat(ReleasedClassLoaderRegistry.size()).isZero();
        }

        @Test
        @DisplayName("size 应反映已注册数量")
        void shouldReflectRegisteredCount() {
            final ClassLoader loader1 = new ClassLoader() { };
            final ClassLoader loader2 = new ClassLoader() { };
            ReleasedClassLoaderRegistry.register(loader1);
            ReleasedClassLoaderRegistry.register(loader2);
            assertThat(ReleasedClassLoaderRegistry.size()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("onEnter 安全性")
    class OnEnterSafety {

        @Test
        @DisplayName("null loader 不抛异常")
        void shouldNotThrowForNullLoader() {
            TcclGuardAdvice.onEnter(null);
        }

        @Test
        @DisplayName("未释放 loader 不抛异常")
        void shouldNotThrowForUnreleasedLoader() {
            final ClassLoader loader = new ClassLoader() { };
            TcclGuardAdvice.onEnter(loader);
        }

        @Test
        @DisplayName("已释放 loader 不抛异常")
        void shouldNotThrowForReleasedLoader() {
            final ClassLoader loader = new ClassLoader() { };
            ReleasedClassLoaderRegistry.register(loader);
            TcclGuardAdvice.onEnter(loader);
        }
    }
}