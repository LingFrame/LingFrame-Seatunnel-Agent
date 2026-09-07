package com.lingframe.agent.adapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 从 SeaTunnel Task 宿主实例解析作业级治理身份（jobId）。
 * <p>
 * 作业级隔离的关键前提：能拿到本次批次所属的 jobId，才能路由到 `seatunnel-job-{jobId}` 灵元。
 * SeaTunnel 的 {@code AbstractTask} 在父类声明 `protected final long jobID`（字段名 `ID` 全大写，
 * 已从源码 AbstractTask.java 实证），无对应 getter（仅有 getTaskID/getTaskLocation）。因此本提取器
 * 沿继承链向上扫描该字段并缓存访问器，避免每个批次热路径重复探测。
 * <p>
 * 本方法运行在 Agent core 的 ClassLoader（App 层），SeaTunnel task 实例可能来自隔离 ClassLoader，
 * 因此**不依赖编译期类型**，全程以反射读取，天然跨类加载器安全。
 * <p>
 * 读取实现选择（字段 → {@link Field#getLong} 直读）：
 * JDK9+ 模块系统下，{@code MethodHandles.Lookup.unreflectGetter} 对「非 public 类」的字段
 * （如跨包测试宿主、封装类）受 lookup 访问模式约束会抛 IllegalAccessException——setAccessible
 * 只作用于反射 API，不提升 Lookup 权限；而反射 API 在 setAccessible(true) 后对所有可见性字段可用，
 * 且缓存 Field 后热路径开销与 MethodHandle 同级（ns 级）。故选反射实现，保证 JDK8 / JDK17 行为一致。
 * <p>
 * 诚实降级：字段名随 SeaTunnel 演进可能变为 `jobId`，故扫描同时兼容 `jobID` 与 `jobId` 两种命名，
 * 以及 `getJobId()` getter 兜底；全部未命中返回 {@link #NO_JOB}（哨兵 -1），由调用方回退共享灵元。
 */
public final class JobIdExtractor {

    private static final Logger log = LoggerFactory.getLogger(JobIdExtractor.class);

    /** 无法解析作业 ID 的哨兵值（合法 jobId 均为非负 long）。 */
    public static final long NO_JOB = -1L;

    /** 探测结果为「无 job 字段」的缓存哨兵——ConcurrentHashMap 不允许 null value，故以非 null 对象占位。 */
    private static final Object NO_GETTER = new Object();

    /** 命中缓存：task 类 → 字段访问器 {@link Field} 或 getter {@link Method}；或 {@link #NO_GETTER} 表示无 job 字段。 */
    private final ConcurrentHashMap<Class<?>, Object> getterByType = new ConcurrentHashMap<>();

    /** 作业级治理是否可用（premain 指纹门控结果，见 premain.checkJobLevelFieldPresent）。 */
    private final boolean jobLevelSupported;

    public JobIdExtractor(boolean jobLevelSupported) {
        this.jobLevelSupported = jobLevelSupported;
    }

    /**
     * 解析 task 宿主实例的 jobId。
     * <p>
     * 全程无锁：首次遇某类型做一次字段探测 + 访问器缓存，之后每次为直读（ns 级开销）。
     *
     * @param task AbstractTask 子类实例；null 或不可解析时返回 {@link #NO_JOB}
     * @return 作业 ID；无法解析返回 {@code NO_JOB}
     */
    public long extract(Object task) {
        if (!jobLevelSupported || task == null) {
            return NO_JOB;
        }
        final Object accessor;
        try {
            accessor = resolve(task.getClass());
        } catch (Throwable t) {
            // 异常路径不应击穿调用方；统一走 SLF4J（热路径仅在异常时触发）
            log.warn("JobIdExtractor resolve threw for {}: {}", task.getClass().getName(), t.toString());
            return NO_JOB;
        }
        if (accessor == NO_GETTER) {
            // 每类型仅首次探测触发一次（NO_GETTER 已缓存），warn 提示该类型无 job 字段
            log.warn("JobIdExtractor NO_GETTER for {}", task.getClass().getName());
            return NO_JOB;
        }
        try {
            if (accessor instanceof Field) {
                // long 字段直读，避免装箱；Field 已 setAccessible，跨包非 public 类亦可读
                return ((Field) accessor).getLong(task);
            }
            return (Long) ((Method) accessor).invoke(task);
        } catch (Throwable t) {
            // 暴露 JDK 模块系统 / 安全策略拦截的真实原因；统一走 SLF4J（异常路径，不刷热路径）
            log.warn("JobIdExtractor read threw for {} accessor={}: {}",
                    task.getClass().getName(), accessor.getClass().getSimpleName(), t.toString());
            return NO_JOB;
        }
    }

    /**
     * 判断作业级治理是否启用（premain 能力位的透传）。
     */
    public boolean isJobLevelSupported() {
        return jobLevelSupported;
    }

    /** 解析并缓存某类型的作业 ID 访问器；无则缓存哨兵 {@link #NO_GETTER} 防止重复探测。 */
    private Object resolve(Class<?> type) {
        final Object cached = getterByType.get(type);
        if (cached != null) {
            return cached;
        }
        final Object resolved = probe(type);
        getterByType.put(type, resolved != null ? resolved : NO_GETTER);
        return resolved != null ? resolved : NO_GETTER;
    }

    /** 沿继承链探测作业 ID 访问器：优先 long 字段（jobID/jobId），兜底 getJobId() long 方法。 */
    private Object probe(Class<?> type) {
        for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                final String name = f.getName();
                if (("jobID".equals(name) || "jobId".equals(name)) && isLongLike(f)) {
                    // setAccessible(true) 绕过类/字段可见性检查：真实 AbstractTask 的 jobID 是跨包
                    // protected 字段，e2e 伪宿主甚至可能是包私有类。失败则诚实降级 NO_JOB。
                    if (!trySetAccessible(f)) {
                        return null;
                    }
                    return f;
                }
            }
        }
        for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Method m : c.getDeclaredMethods()) {
                if ("getJobId".equals(m.getName()) && isLongLike(m)) {
                    if (!trySetAccessible(m)) {
                        return null;
                    }
                    return m;
                }
            }
        }
        return null;
    }

    /** 尝试解除字段访问限制；安全策略或模块封装拦截时返回 false（调用方诚实降级）。 */
    private static boolean trySetAccessible(Field f) {
        try {
            f.setAccessible(true);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /** 尝试解除方法访问限制；安全策略或模块封装拦截时返回 false（调用方诚实降级）。 */
    private static boolean trySetAccessible(Method m) {
        try {
            m.setAccessible(true);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static boolean isLongLike(Field f) {
        return f.getType() == long.class || f.getType() == Long.class;
    }

    private static boolean isLongLike(Method m) {
        final Class<?> r = m.getReturnType();
        return r == long.class || r == Long.class;
    }
}