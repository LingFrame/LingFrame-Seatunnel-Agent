package com.lingframe.benchmark;

/**
 * 织入载体与被织入载体的【共享批次逻辑】。
 * <p>
 * A/B 对照的前提是两条测量路径的方法体严格同构——若各自手写一份，未来改动极易漂移，
 * 让「被织入」与「对照组」差异不再是纯拦截损耗。因此把批次业务逻辑收敛到本类唯一实现，
 * {@link NoopBatchTask}（被织入）与 {@link PlainBatchTask}（对照组）的 {@code call()} 都只是
 * 一行转调 + 落 checksum，杜绝逻辑漂移。
 * <p>
 * 载荷不可消除性：{@link #process} 的变换结果通过返回值流出方法，由调用方存入实例字段、
 * 最终被 JMH Blackhole 消费——JIT 无法把整段处理判定为死代码消除。
 */
final class BatchCore {

    /** 每批次处理的「上游记录」条数：真实 SourceTask 单批典型的记录量级（1k 条内存记录）。 */
    static final int ROWS = 1024;

    private BatchCore() {
    }

    /** 确定性初始化行数据（同一份内容供两条路径使用，保证载荷一致）。 */
    static int[] buildRows() {
        final int[] rows = new int[ROWS];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = (i * 31 + 7) ^ (i >>> 5);
        }
        return rows;
    }

    /**
     * 模拟单批次处理：先自旋模拟「批次占用的墙上时间」（IO/等待主导的真实批次语义），
     * 再对 1k 行记录做变换聚合，返回必须流出方法的校验和。
     *
     * @param rows      上游记录
     * @param spinNanos 批次墙钟时间模拟（纳秒）；0 = 纯变换载荷
     * @param seed      每次调用递增的种子，保证校验和随调用变化（防结果缓存）
     * @return 变换聚合校验和（可观测副作用，防止整体消除）
     */
    static long process(int[] rows, long spinNanos, long seed) {
        busySpin(spinNanos);
        long acc = seed;
        for (int i = 0; i < rows.length; i++) {
            acc = acc * 31L + (rows[i] ^ (i * 0x9E3779B97F4A7C15L));
        }
        return acc;
    }

    /**
     * 基于 {@link System#nanoTime()} 的自旋。nanoTime 非纯函数，结果随真实时间变化，
     * 循环无法被 JIT 消除；叠加 acc 折叠 + 不可达分支双保险。
     */
    private static void busySpin(long nanos) {
        if (nanos <= 0L) {
            return;
        }
        final long end = System.nanoTime() + nanos;
        long acc = 0L;
        while (System.nanoTime() < end) {
            acc ^= System.nanoTime();
        }
        // 实际永远不可达，仅用于告诉 JIT 循环有可观测副作用
        if (acc == 0xDEADBEEFL) {
            throw new IllegalStateException("unreachable busySpin sentinel");
        }
    }
}
