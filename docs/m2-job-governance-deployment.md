# 作业级治理部署指南（Deployment Guide）

| 项 | 内容 |
| --- | --- |
| 适用 | lingframe-seatunnel-agent 及 lingframe-core 0.4.5 及以上 |
| 读者 | 平台运维 / SRE / 集群负责人 |

---

## 1. 范围与硬依赖

作业级故障隔离：治理身份从引擎级 `seatunnel` 收敛到 `seatunnel-job-{jobId}` 作业维度，任一作业的下游故障只熔断该作业，其余作业的批次不受影响。动态参数经 Hazelcast IMap 热刷新，改配置无需重启。

三个硬依赖，缺一不可：

| 依赖 | 版本/来源 | 缺失后果 |
| --- | --- | --- |
| lingframe-core | **必须 ≥ 0.4.0**（BreakerHolder 配置指纹修复，见 §5） | 熔断参数热刷新「纸面生效、行为不生效」（旧版只按 timeoutMs 重建熔断器） |
| Hazelcast 实例 | SeaTunnel 内部实例；worker 为 lite member | 配置中心无法初始化，降级为本地默认参数（见 §7） |
| SeaTunnel 版本 | 锁定支持 `AbstractTask.jobID` 字段的版本 | 未知版本作业级治理降级共享（见 §4） |

**升级顺序**：必须先升级 lingframe-core（含熔断热刷新修复）再启用作业级治理，否则热刷新参数不生效且无告警。

---

## 2. 配置链路与写入规范

### 2.1 数据链路

```
master 节点: 写入 IMap("lingframe-governance-config")
      │  (worker 是 lite member，不存储数据，仅 getMap 远程代理)
      ▼
worker 节点: EntryListener(added/updated/removed/evicted/expired) 同步回调
      ▼
HazelcastConfigCenter.refreshConfig → 按 key 路由
      ▼
LingRuntime.updateConfig（volatile 引用替换）
      ▼
ResilienceGovernanceFilter 指纹比对 → 参数变化即重建治理器（限流器每请求比对；熔断器 A/B 类指纹）
```

**写配置必须由 master 节点发起**；worker 节点只读。IMap 名固定为 `lingframe-governance-config`。

### 2.2 初始化时序

- Agent premain 在 SeaTunnel 创建 Hazelcast 实例**之前**执行，配置中心首次 `tryInit` 必然找不到实例；
- `SeaTunnelAdapter.beforeTaskCall` 首次拦截时触发 `ensureConfigCenterInit`，按 **5s 节流**重试，直到发现实例并完成监听器注册 + 初始配置应用；
- 重试为 fail-open：探测失败只记 WARN，**不影响批次执行**；实例就绪后自动收敛；
- `tryInit` 为 `synchronized`，多线程首次并发安全（监听器不重复注册）。

> 运维注意：集群启动期 IMap 可能为空，agent 以打包默认参数运行；写入配置后经 EntryUpdated 事件广播生效，无需重启。

### 2.3 多 worker 一致性（最终一致）

- **单写多读**：配置只由 master（数据成员）写入；worker 是 lite member，不落数据，经远程代理 getMap 监听，无多写冲突，无需分布式锁；
- **本地应用**：治理记账是 **worker JVM 本地**的，作业维 agent 独立存在于各 worker 本地。同一作业跨多 worker 运行时，各 worker 独立治理、独立应用同一份参数——治理对象是「本 worker 上该作业的批次」，无需跨节点共享熔断/限流状态；
- **广播路径**：master 写入 → 集群同步 → 各 worker EntryListener 异步回调（毫秒级延迟）。回调线程只做 volatile 引用替换 + 指纹比对，不做跨节点协调；
- **事件丢失（worker 长时间分区）**：分区期间 IMap 变更不重放；恢复后监听器自动重订阅，但丢失的变更不补刷。治理参数为宽松一致，短窗口不一致仅影响该窗口限流/熔断判定，不产生数据损坏——必要时重写 key 或重启该 worker 触发初始配置收敛。

---

## 3. 配置项清单（yaml，全部 opt-in）

```yaml
governance:
  enabled: true
  per-job-governance-enabled: true       # 作业级开关：true=作业隔离（默认）；false=引擎级共享
  per-job-max-tracked-jobs: 1024         # 作业维度硬上限，超限新作业回退共享 + WARN
  per-job-idle-ttl-ms: 1800000           # 空闲回收 TTL（30min），Reaper 按周期扫描
  per-job-reap-interval-ms: 300000       # Reaper 节流间隔（5min）
  resilience:
    circuit-breaker-enabled: true        # 熔断开关（opt-in）
    rate-limiter-enabled: true           # 限流开关（opt-in）
    fail-closed: false                   # 硬拒绝门控：false=日志+放行；true=熔断/舱满硬拒
    classifier-enabled: true             # 熔断失败判定分类器：true=两层契约化分类；false=回退启发式
    downstream-readable-failures-patterns: []   # 下游可用性失败显式 patterns（正则 match FQCN/message）
    business-exceptions-patterns: []            # 业务异常显式排除（优先级最高，不计入熔断失败率）
    rate-limit-per-second: 100           # 共享维度限流
    circuit-breaker-failure-rate-threshold: 50    # 失败率阈值 %
    circuit-breaker-sliding-window-size: 20      # 滑动窗口
    circuit-breaker-minimum-number-of-calls: 10  # 最少采样数（≤ 窗口，超限自动钳制）
    default-timeout-ms: 3000             # 慢调用阈值
```

- 全部配置项均有**系统属性覆盖**：`-Dlingframe.agent.<name>=<value>`（如 `-Dlingframe.agent.trace-level=WARN`），优先级高于 yaml，适合容器环境按环境调参；
- `per-job-max-tracked-jobs` / `idle-ttl` / `reap-interval` 三件套保持保守默认，回滚语义不变（显式 `false` 逐字节回引擎级）；
- `fail-closed` 语义：`CIRCUIT_OPEN` / `BULKHEAD_FULL` 硬拒抛 `GovernanceRejectException`，`RATE_LIMITED` 软退避（令牌间隔 + 抖动），**仅故障作业被拒**（作业级隔离前提下安全）；
- `classifier-enabled` / patterns 语义：分类器沿 cause 链裁决「异常是否代表下游可用性失败、可否喂 onError」——一层 JDK 类型契约（`IOException`/`SQLException`/`TimeoutException` 家族）默认启用；二层运维显式配置（自研 connector 免发版接入），`business-exceptions-patterns` 优先级最高（业务异常不计入熔断失败率）；`classifier-enabled=false` 跳过二层直回启发式（回滚开关）；patterns 支持 YAML list 或单字符串，非法正则跳过不阻断。

---

## 4. 作业级配置 key 与降级语义

### 4.1 key 语法

| key | 作用域 | 语义 |
| --- | --- | --- |
| `rate-limit-per-second`（裸 key） | 全局 | 广播到共享 + 全部已注册作业维度 |
| `job.{jobId}.rate-limit-per-second` | 作业级 | 仅刷新 `seatunnel-job-{jobId}`；缺省逐级回退全局裸 key |

**可热刷的 5 个 key**（其余 key 写入 IMap 不生效）：

| 裸 key | 对应参数 |
| --- | --- |
| `rate-limit-per-second` | 限流速率 |
| `circuit-breaker-failure-rate-threshold` | 熔断失败率阈值 |
| `circuit-breaker-sliding-window-size` | 滑动窗口 |
| `default-timeout-ms` | 慢调用超时 |
| `bulkhead-max-concurrent` | 舱壁并发上限 |

**作业级 key 变更路由**（代码实证）：`job.*` 前缀 key → 仅刷新目标作业；其余（全局裸 key / 格式未知 key）→ 刷新共享 + 全部已注册作业广播。

### 4.2 热刷生效机制与副作用

- `LingRuntime.updateConfig` 为 volatile 引用替换，配置中心侧立即可见；
- **限流器**每次请求比对 `rateLimit`，变化即重建 → 热刷即时生效；
- **熔断器**由 `BreakerHolder` 持有完整配置指纹（timeout + 及 failureRate / slowCallRate / slidingWindow / minimumCalls / waitDuration），任一指纹变化即重建（lingframe-core ≥ 0.4.0 生效，旧版不生效）；
- **重建副作用**：新熔断器以全新 `CLOSED` 状态启动——热刷参数会重置当前熔断/半开/窗口统计。语义等价于运维主动调整参数后治理器重新收敛，与限流器重建一致。**故障持续时调整阈值不会立即复现 OPEN，需重新累积采样**（`minimum-number-of-calls` 之后）；
- 事件回调在 Hazelcast 内部线程**异步**执行，配置中心侧写入返回后，worker 侧刷新有毫秒级延迟——验收脚本应轮询断言而非立即断言。

### 4.3 版本降级

premain 运行时指纹探测 `AbstractTask.jobID` 字段：

| 指纹结果 | 行为 | MBean 能力位 |
| --- | --- | --- |
| 命中 | 作业级治理启用 | `JobIdExtractor=INSTALLED` |
| 字段缺失（SeaTunnel 演进改名） | **降级共享** + ERROR 告警，不猜字段名 | `JobIdExtractor=UNSUPPORTED_VERSION` |
| 治理运行时缺失 | 降级共享 | `JobIdExtractor=NO_RUNTIME` |
| per-job 关闭 | 引擎级现状 | `JobIdExtractor=DISABLED` |

> 原则：未知版本上「引擎级治理」比「猜错 jobId 挂在错误隔离单元」安全。

---

## 5. 升级与回滚

### 5.1 升级前提

启用 `fail-closed: true` 前，务必确认 lingframe-core 版本 **≥ 0.4.0**：在新版本中，治理链自身抛出的治理拒绝（如 `BULKHEAD_FULL`）不会误计入熔断失败率（即不会因舱满/权限误配而误打开熔断器）。旧版存在该缺陷——`fail-closed=true` 后舱满/权限误配可能误开熔断，因此旧版下 `fail-closed` 必须保持 `false`。

> **启用硬拒前先核对版本**：确认部署的 lingframe-core ≥ 0.4.0 后再将 `fail-closed` 置 `true`。

### 5.2 回滚（秒级，无需发版）

- 关闭作业级治理：`per-job-governance-enabled: false` → 全部回共享（`seatunnel-job-*` 由 Reaper 按 TTL 回收）；
- 撤销参数：删除作业级 key（触发 EntryRemoved → 回退全局）或恢复全局裸 key 原值；
- 关闭硬拒：`fail-closed: false` → 熔断打开只日志放行；
- 全部回退后，`seatunnel` 共享仍由全局裸 key 治理。

---

## 6. 容量与性能

- **维度上限**：`per-job-max-tracked-jobs`（默认 1024）硬上限，超限新作业回退共享 + 采样 WARN；每作业 = 1 治理维度 + 1 熔断器 + 1 限流器 + 1 份指标（`seatunnel-job-{jobId}` 命名），上限内资源可控；
- **回收**：Reaper 按 `reap-interval-ms` 周期扫描空闲超 TTL 作业，回收顺序 `VirtualLingManager.unregister` → 治理器 evict → 指标 remove（本地可验证零残留，见 §9）；
- **性能红线**：governed 模式（含 per-job 路径）批次附加损耗按 job 级总耗时膨胀衡量——正式跑分 100ms 批附加 +173.2µs（per-job 轮转 +183.4µs，作业级查表仅 +10µs；不同 JMH 轮次结果有 ±15µs 波动，见 benchmark/README），典型场景 job 级总耗时影响 <0.5%，属正常范围。热路径为 `JobIdExtractor` 按类型缓存 `Field` 的 `getLong` 直读 + 作业维度引用解析，无重复反射（JDK8 / JDK17 行为一致）；
- **配置热刷开销**：仅 EntryListener 回调 + 配置引用替换 + 治理器指纹比对（int/long 比较），不随作业数放大；冷启动期 5s 节流重试无批量探测风暴。

---

## 7. 故障场景与处置

| 故障场景 | 表现 | 处置 |
| --- | --- | --- |
| Hazelcast 实例不可用 / 集群未就绪 | 配置中心初始化失败，agent 以打包默认参数治理，WARN 日志 | 实例就绪后 5s 节流自动重试收敛，无需人工干预 |
| IMap 写入非法值（非数字） | `parseInt` 回退 fallback（当前值），不崩 | 修正配置值，重写触发 EntryUpdated |
| 作业级 key 格式错误（如 `job..rate`） | 解析返回 null → 走全局广播分支 | 修正 key，或删除错误 key |
| 熔断误触发（作业被硬拒） | 事件总线 `CircuitBreakerStateEvent` + rejectedCount 指标 | 查该作业下游：恢复后熔断器半开探针自愈；阈值不合理则热刷 `job.{id}.circuit-breaker-failure-rate-threshold` 上调，故障恢复后调回 |
| 治理拒绝误开熔断（`fail-closed=true` 下） | 舱满/权限误配抛出的治理拒绝（`BULKHEAD_FULL` / `SECURITY_REJECTED`）被误计入熔断失败率 → 误 OPEN 熔断器 | **前置**：lingframe-core ≥ 0.4.0 前 `fail-closed` 必须保持 `false`（§5.1）；升级后灰度开启，并交叉核对 breakerState 与 rejectedCount，区分「下游真实故障」与「治理链自身误拒」 |
| 作业维度膨胀 / 超上限 | WARN 采样 + MBean 计数 | 核查作业身份解析是否异常；调大 `per-job-max-tracked-jobs` 或排查作业 ID 泄漏 |
| 未知 SeaTunnel 版本 | ERROR + `JobIdExtractor=UNSUPPORTED_VERSION` | 作业级治理降级共享；升级适配或锁版本 |
| 配置中心 stop（应用关闭） | 监听器全部移除，无孤儿监听器泄漏 | 正常关机路径，无处置 |

---

## 8. 可观测性

- **JMX**：`cn.lingframe.agent:type=Observability`（jconsole/jcmd 直读）——`JobIdExtractor` 能力位（INSTALLED / UNSUPPORTED_VERSION / NO_RUNTIME / DISABLED）、advice 状态、rejectedCount / budgetExhaustedCount / breakerState / gateStatus；
- **日志关键点**（英文，可 grep）：
  - `Hazelcast config center initialized on instance [...], IMap [...], listeners [...]`
  - `Config entry updated: {key} = {value}`（热刷审计）
  - `Ling [seatunnel-job-7] config refreshed: rateLimit=2/s, ...`（刷新落点）
  - `Job-level tracking cap reached (...), job [...] falling back to shared ling`（超限）
  - `JobIdExtractor fingerprint mismatch, job-level governance degraded to shared ling`（版本降级）
- **熔断事件**：LingFrame EventBus 的 `CircuitBreakerStateEvent`（CLOSED→OPEN→HALF_OPEN→CLOSED），与 agent rejectedCount 双路交叉核对。

---

## 9. 部署前验证清单

在目标集群执行（agent e2e 三件套为验收基准）：

| # | 验证项 | 通过标准 |
| --- | --- | --- |
| 1 | 熔断隔离 | 作业 A 熔断打开期间，作业 B 批次 100% 放行（JobIsolationIT） |
| 2 | 熔断热刷真实生效 | 热刷 `job.{A}.circuit-breaker-failure-rate-threshold=100` 后 A 混合失败不熔断、B 按全局阈值熔断（JobConfigRefreshIT，**依赖 lingframe-core ≥ 0.4.0**） |
| 3 | 作业级 key 隔离 | 作业级限流/阈值热刷仅影响目标作业，共享与其余作业不变 |
| 4 | 全局广播 + 覆盖优先 | 全局裸 key 变更广播到全部维度，作业级覆盖保持 |
| 5 | 版本降级 | 模拟字段缺失 → 降级共享 + ERROR + MBean 能力位 |
| 6 | 无泄漏 ✅ | 1000 作业生命周期后治理维度/弹性缓存/指标零残留——`JobLifecycleLeakIT` 本地确定性通过（约 3.4s，断言口径：活跃驱动作业在位 + 过期作业零残留） |
| 7 | 兼容回退 | `per-job-governance-enabled=false` 行为与引擎级现状一致 |
| 8 | 性能红线 ✅ | governed + per-job 路径正式跑分通过（口径：job 级总耗时膨胀）——100ms 批附加 +173.2µs、per-job 轮转 +183.4µs（作业级查表附加仅 +10µs）；典型场景 job 级总耗时影响 <0.5%，属正常范围 |