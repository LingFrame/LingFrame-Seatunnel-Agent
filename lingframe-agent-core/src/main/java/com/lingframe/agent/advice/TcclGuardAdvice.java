package com.lingframe.agent.advice;

import com.lingframe.agent.bridge.ReleasedClassLoaderRegistry;
import net.bytebuddy.asm.Advice;

/**
 * TCCL 拘留防御字节码切面。
 * <p>
 * 织入 {@code Thread.setContextClassLoader(ClassLoader)} 方法入口，
 * 在设置前检查目标 ClassLoader 是否已被标记为已释放。
 * 如果已释放，将参数替换为其 parent，防止线程池复用时残留已释放 ClassLoader 为 TCCL。
 * <p>
 * 织入点：{@code java.lang.Thread.setContextClassLoader(ClassLoader)}
 * <p>
 * 性能影响：每次 TCCL 设置增加一次 WeakHashMap.contains 查询。
 * 已释放 ClassLoader 数量通常很少（仅卸载过的 ClassLoader），大多数调用直接放行。
 * <p>
 * 线程安全：advice 代码被织入目标方法，无共享可变状态。
 * {@link ReleasedClassLoaderRegistry} 使用 synchronizedSet(WeakHashMap)，线程安全。
 * <p>
 * Bootstrap 可见性：ReleasedClassLoaderRegistry 位于 Bridge 模块，
 * 通过 appendToBootstrap 注入 Bootstrap ClassLoader，
 * 使 Thread（Bootstrap 加载）能正确解析此类。
 */
public final class TcclGuardAdvice {

    @Advice.OnMethodEnter
    public static void onEnter(@Advice.Argument(value = 0, readOnly = false) ClassLoader loader) {
        if (loader != null && ReleasedClassLoaderRegistry.isReleased(loader)) {
            loader = loader.getParent();
        }
    }

    private TcclGuardAdvice() {
    }
}