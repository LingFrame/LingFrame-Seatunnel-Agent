package com.lingframe.agent.hook;

import com.lingframe.core.resource.ThreadReferenceUnloadHook;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@DisplayName("EngineSafeThreadReferenceUnloadHook 引擎安全线程卸载钩子测试")
class EngineSafeThreadReferenceUnloadHookTest {

    @Test
    @DisplayName("null ClassLoader 清理应安全跳过")
    void shouldHandleNullClassLoaderSafely() {
        final EngineSafeThreadReferenceUnloadHook hook = new EngineSafeThreadReferenceUnloadHook();
        assertThatCode(() -> hook.cleanup("test-ling", null)).doesNotThrowAnyException();
        assertThatCode(() -> EngineSafeThreadReferenceUnloadHook.resetThreadContextClassLoaders("test-ling", null))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("cleanup 应调用底座 delegate.cleanup")
    void shouldDelegateToUnderlyingHook() {
        final ThreadReferenceUnloadHook mockDelegate = mock(ThreadReferenceUnloadHook.class);
        final EngineSafeThreadReferenceUnloadHook hook = new EngineSafeThreadReferenceUnloadHook(mockDelegate);
        final ClassLoader cl = new ClassLoader() { };

        hook.cleanup("test-ling", cl);
        verify(mockDelegate).cleanup("test-ling", cl);
    }

    @Test
    @DisplayName("shutdown 应安全委托")
    void shouldDelegateShutdown() {
        final ThreadReferenceUnloadHook mockDelegate = mock(ThreadReferenceUnloadHook.class);
        final EngineSafeThreadReferenceUnloadHook hook = new EngineSafeThreadReferenceUnloadHook(mockDelegate);

        hook.shutdown();
        verify(mockDelegate).shutdown();
    }

    @Test
    @DisplayName("resetThreadContextClassLoaders 应自动将活动线程残留的 TCCL 重置为 SystemClassLoader")
    void shouldResetActiveThreadTcclToSystemClassLoader() throws Exception {
        final ClassLoader customCl = new ClassLoader() { };
        final CountDownLatch threadStarted = new CountDownLatch(1);
        final CountDownLatch threadEnd = new CountDownLatch(1);
        final AtomicBoolean tcclWasCustom = new AtomicBoolean(false);

        final Thread workerThread = new Thread(() -> {
            Thread.currentThread().setContextClassLoader(customCl);
            tcclWasCustom.set(Thread.currentThread().getContextClassLoader() == customCl);
            threadStarted.countDown();
            try {
                threadEnd.await(3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "seatunnel-mock-worker-thread");

        workerThread.start();
        try {
            assertThat(threadStarted.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(tcclWasCustom.get()).isTrue();
            assertThat(workerThread.getContextClassLoader()).isSameAs(customCl);

            // 执行安全重置
            EngineSafeThreadReferenceUnloadHook.resetThreadContextClassLoaders("seatunnel-agent", customCl);

            // 验证 TCCL 已被净化重置为 SystemClassLoader
            assertThat(workerThread.getContextClassLoader()).isSameAs(ClassLoader.getSystemClassLoader());
        } finally {
            threadEnd.countDown();
            workerThread.join(2000);
        }
    }
}
