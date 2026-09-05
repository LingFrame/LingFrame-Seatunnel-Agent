package com.lingframe.agent.bridge;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("ReleasedClassLoaderRegistry 已释放 ClassLoader 注册表测试")
class ReleasedClassLoaderRegistryTest {

    @AfterEach
    void cleanup() {
        ReleasedClassLoaderRegistry.clear();
    }

    @Nested
    @DisplayName("注册与查询")
    class RegisterAndQuery {

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
        @DisplayName("null 入参应安全处理")
        void shouldHandleNullSafely() {
            ReleasedClassLoaderRegistry.register(null);
            assertThat(ReleasedClassLoaderRegistry.isReleased(null)).isFalse();
        }

        @Test
        @DisplayName("size 应反映已注册数量")
        void shouldReflectRegisteredCount() {
            final ClassLoader loader1 = new ClassLoader() { };
            final ClassLoader loader2 = new ClassLoader() { };
            ReleasedClassLoaderRegistry.register(loader1);
            ReleasedClassLoaderRegistry.register(loader2);
            assertThat(ReleasedClassLoaderRegistry.size()).isGreaterThanOrEqualTo(2);
        }
    }
}