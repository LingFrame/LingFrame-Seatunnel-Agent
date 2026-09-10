# LingFrame SeaTunnel Agent

> Java Agent 外挂，为 Apache SeaTunnel 提供 LingFrame 类隔离和治理能力。

## 快速接入

在 `$SEATUNNEL_HOME/config/seatunnel-env.sh` 末尾追加：

```bash
export JAVA_OPTS="$JAVA_OPTS -javaagent:$SEATUNNEL_HOME/lib/lingframe-seatunnel-agent.jar"

# JDK 17 需追加模块开放
export JAVA_OPTS="$JAVA_OPTS --add-opens java.base/java.net=ALL-UNNAMED \
                             --add-opens java.base/java.lang=ALL-UNNAMED \
                             --add-opens java.base/java.util=ALL-UNNAMED \
                             --add-opens java.sql/java.sql=ALL-UNNAMED"
```

> **💡 节点挂载指引**：
> - **混合模式（Hybrid）/ 单容器**：Master 与 Worker 共进程，直接配置即可覆盖全部调度与执行。
> - **分离集群模式（Separated）**：**Worker 节点建议挂载（强依赖）**（连接器动态加载/卸载、Task 批次执行、TCCL 泄漏均在 Worker 发生）；Master 节点建议一并挂载以实现全链路 DAG 依赖监控与 MBean 可观测性。

### 治理配置

在 `$SEATUNNEL_HOME/config/lingframe-governance.yaml` 中配置：

```yaml
governance:
  enabled: true
  dev-mode: false          # false=零信任(默认)，true=开发模式放行未声明权限
  task-execution-advice-enabled: false  # 批次级治理切点显式开关（默认 false，但熔断/限流/灰度/权限任一开启时自动生效，见下）
  routing:
    gray-routing-enabled: false
  security:
    permission-enabled: false
```

## 治理能力

Agent 借道 LingFrame 治理流水线（GOVERN_ONLY 模式），真实生效范围如下：

| 能力 | 状态 | 说明 |
|---|---|---|
| 指标收集 + 事件发布 | **已生效** | TrafficMetricsFilter / EventBus 深度集成，调用量、QPS、延迟与异常指标实时可观测 |
| 审计追踪 | **已生效** | 治理链路 Trace 采集，可通过 EventBus 订阅审计事件 |
| ClassLoader 深度清理 | **已生效** | ClassLoaderReleaseAdvice 织入 releaseClassLoader 拦截点，触发 JDBC 驱动注销 + URLClassLoader 关闭 + ThreadLocal 深度安全清理。**生效范围：`classloader-cache-mode: false`（每 job 独立 ClassLoader）场景**；`cache-mode: true`（共享缓存，SeaTunnel 默认）时 ClassLoader 为多 job 共享常驻（SeaTunnel 设计），Agent 不做物理关闭，仅提供 TCCL 残留防御 |
| 权限审计 | **默认关闭** | 需 `governance.security.permission-enabled: true` 且 `dev-mode: false` 才进入零信任（Deny-by-Default）。注意 Agent 自身不声明 `requiredPermission`，单独开启会导致每个批次刷一条拒绝告警，请配合为虚拟灵元补齐权限声明 |
| 熔断 / 限流 | **默认关闭**（纯 ClassLoader 清理，零治理）；显式开启即真实生效 | 熔断/限流与批次切点**均默认 `false`**。需要治理的部署在 `lingframe-governance.yaml` 显式开 `circuit-breaker-enabled`/`rate-limiter-enabled`（或 `task-execution-advice-enabled`）即可：任一治理特性开启时批次切点**自动联动织入**（防「开了弹性却忘开切点」的伪开启），`beforeTaskCall` 构造 `InvocationContext` 调 `pipelineEngine.invoke(ctx)` 走完整 12 Filter 链前置治理（含 `ResilienceGovernanceFilter` 限流/熔断前置检查）；`afterTaskCall` 回灌真实业务结果到 `LingHealthMetrics`，触发 `RuntimeStatus.DEGRADED → MacroStateGuardFilter` 熔断路径。**熔断默认 fail-open 软退避（`CIRCUIT_OPEN/RATE_LIMITED/BULKHEAD_FULL` 命中仅 sleep 100ms 后放行，不真正甩负载）；设置 `governance.resilience.fail-closed: true` 后 `CIRCUIT_OPEN/BULKHEAD_FULL` 改为抛出 `GovernanceRejectException` 使 `call()` 失败触发引擎 Failover，即「熔断名副其实」**。**失败率口径：仅下游可用性异常（超时/IO/连接类）计入熔断失败率，普通业务异常（Transform/数据错误）不虚高指标**。**粒度：作业级隔离（`per-job-governance-enabled` 默认 `true`）**——治理身份按作业生成（jobID 提取 + 运行期版本指纹门控），限流/熔断/健康状态按作业隔离，故障作业不波及其他作业；显式 `per-job-governance-enabled: false` 可回退引擎级共享灵元 |
| 路由 / 状态守卫 | **已生效（虚拟灵元 ACTIVE）** | 注入生产级 VirtualLingManager 生成的虚拟灵元（状态处于 ACTIVE），MacroStateGuardFilter 与指标双向闭环联动 |
| 灰度路由 | **已装配，可扩展** | LabelMatchRouter 已注册，支持结合扩展灵元定义细粒度流量路由策略 |
| 分布式动态配置 | **需治理切点生效** | HazelcastConfigCenter 通过 IMap `lingframe-governance-config` 监听 5 类 Entry 事件（新增/更新/删除/驱逐/过期），毫秒级热刷新虚拟灵元 LingRuntimeConfig（限流 / 熔断阈值 / 滑动窗口 / 超时 / **`bulkhead-max-concurrent`**），支持配置删除安全回退。热刷新的参数由 Pipeline 内的弹性治理 Filter 消费，因此需治理切点生效（任一治理特性开启即自动织入）才有可观测效果 |

> **治理链路说明**：**默认不开启批次级治理**——Agent 默认仅启用 ClassLoader 深度清理（零性能损耗与零业务干扰）。
> 熔断/限流等治理特性与批次切点（`AbstractTask.call()`）全部默认 `false`，需在 `lingframe-governance.yaml`
> 中显式开启。任一治理特性开启时批次切点**自动联动织入**（无需单独开 `task-execution-advice-enabled`，
> 织入时日志输出风险提示：吞吐下降、Checkpoint 超时风险，请监控后决定是否保持开启）。

> **⚠️ 能力边界（诚实标注，2026-09-06）**
> 1. **连接器「热加载」不属本 Agent 范围**：Agent 仅织入 `releaseClassLoader`（卸载侧）；
>    SeaTunnel 进程启动后新增/替换连接器 jar 的机制（connector 发现/注册为启动期一次性）不在当前能力内。
> 2. **热卸载（Metaspace 泄漏治理）仅在 `cache-mode: false` 场景生效**：SeaTunnel 默认 `cache-mode: true`
>    为共享缓存常驻（上游设计，非泄漏）。**本机实证（2026-09-06，source × sink 双维度矩阵）**：真实引擎
>    `cache-mode: false` + governed agent 循环 job，卸载链路真实执行（15 job 产生 60 次物理释放日志），
>    **8 组场景**（fake / jdbc-H2 / MySQL / Redis / MongoDB 作 source，console / MongoDB / Redis / JDBC 作 sink，
>    含 MySQL→MySQL 双真实同 job）5 轮 × 15 job = 75 job 的 Full GC 后 Class Metaspace **收敛至零增长**
>    （8.63 / 9.17 / 8.89 / 9.28 / 9.60 / 9.90 / 9.91 / 10.19 MB），jdbc/redis/mongodb 在 source 与 sink
>    两种角色下均无泄漏。**CI A/B 对比审计最终证据（2026-09-10）**：全真容器化（MySQL 8.0 + Kafka 3.7.0 KRaft）
>    构建 Fake / MySQL / Kafka × Console / MySQL / Kafka **3×3 = 9 组全正交矩阵**与 **4 线程异构并发交错压测**，
>    Native 对照组（无 Agent）与 Agent 实验组各跑 **72 job（共 144 job）**，逐一验证 FINISHED 终态。
>    **Agent 组 Metaspace 增长 14.06 MB**（< 15MB 阈值），`SeaTunnelChildFirstClassLoader` 拋留 **0**，类卸载率 **96.3%**；
>    **Native 对照组 Metaspace 增长 391.16 MB**，ClassLoader 拋留 **384**，类卸载率 **0.14%**。
>    **Net Overhead = -377.10 MB**（Agent 比 Native 少 377.10 MB——在本测试场景下 Agent 呈净收益）。HTTP 500 = 0，callback NPE = 0。5 轮长尾证据见 §6.3。
>    **完整复现流程与数据见 [`docs/classloader-unload-verification.md`](docs/classloader-unload-verification.md)**。
> 3. **弹性治理按作业隔离**：`per-job-governance-enabled` 默认 `true`，治理身份按作业生成（jobID 提取 + 版本指纹门控），限流/熔断/健康状态按作业隔离，故障作业不波及其他作业；显式 `false` 回退引擎级共享灵元。治理动作生效性（限流拦截/熔断打开）已在真实引擎端到端验证（`JobIsolationIT` / `DualJobFaultInjectionIT` / JMH governed 跑分）。

## 架构

```
lingframe-seatunnel-agent/
├── lingframe-agent-bridge/    # Bootstrap ClassLoader 极薄契约 (<20KB)
├── lingframe-agent-core/      # ByteBuddy Advice + 微内核桥接
├── lingframe-agent-dist/      # Fat-Jar Shade 打包
└── lingframe-agent-e2e/       # 端到端测试
```

## 构建

```bash
mvn clean install -DskipTests
# 本地交付物：lingframe-agent-dist/target/lingframe-seatunnel-agent.jar
# GitHub Release 交付物规范：lingframe-seatunnel-agent-{agent-version}-seatunnel-{seatunnel-baseline}.jar
```

## 质量门控

```bash
mvn -B clean verify -Pintegration-check
```

## 版本

版本号格式：`{agent-version}-seatunnel-{seatunnel-baseline}`。

## License

Apache License 2.0