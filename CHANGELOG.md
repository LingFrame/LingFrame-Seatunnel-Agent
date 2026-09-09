# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-09-07

### Added
- Agent 骨架：三层类加载拓扑 + Bootstrap Bridge + ByteBuddy 织入框架
- `lingframe-agent-bridge`：极薄桥接契约工程（零第三方依赖，<20KB）
- `lingframe-agent-core`：ByteBuddy Advice 拦截器 + SeaTunnel 适配器
- `lingframe-agent-dist`：Maven Shade Fat-Jar 打包（ByteBuddy Relocate）
- `lingframe-agent-e2e`：端到端测试骨架（Docker Compose + Metaspace 泄漏验证）
- `AgentJarManifestTest`：本地可跑的 Fat-Jar 打包守卫（**无需 Docker / 集群**，本地 `mvn test` 即可执行），校验 Manifest 的 `Premain-Class`/`Agent-Class`/`Can-*` 标志、bridge 契约类已打入 jar、ByteBuddy/SnakeYAML 已 Relocate，守住三类致命回归（agent 挂不上、Bootstrap 注入 ClassNotFound、与 SeaTunnel 版本冲突）
- `TaskExecutionAdvice`：`AbstractTask.call()` 批次调度治理切面（默认关闭，开关启用）
- `ClassLoaderReleaseAdvice`：`releaseClassLoader` 双态自适应清理 + `@Advice.FieldValue` 零反射字段直读
- `AgentConfig`：YAML 配置加载（Agent 参数 > SEATUNNEL_HOME > 默认值）
- CI/CD：GitHub Actions 流水线 + Nightly SNAPSHOT 兼容性 CI
- 质量门控：Checkstyle + SpotBugs + JaCoCo（覆盖率门槛 60%）

### Changed
- `SeaTunnelAdapter` 弹性治理从自造 `CircuitBreaker`/`RateLimiter` 重构为 Pipeline `GOVERN_ONLY` 模式：`beforeTaskCall` 构造 `InvocationContext` 调 `pipelineEngine.invoke(ctx)` 走完整 12 Filter 链前置治理，`afterTaskCall` 回灌真实业务结果到 `LingHealthMetrics`，触发 `RuntimeStatus.DEGRADED → MacroStateGuardFilter` 熔断路径
- `AgentGovernanceRuntime` 新增 `EventBus` 和 `MetricsCollector` 字段，支撑 Pipeline 事件订阅与指标收集
- `AgentPipelineFactory` 在 `create()` 时传入 `eventBus` 和 `metricsCollector`
- `SeaTunnelAdapterTest` 更新为 7 参数构造函数，删除熔断器状态反射测试，新增 Pipeline null 安全、超时识别、连续批次测试
- `LingFrameAgentBridge.registerContract` 签名由 `LingGovernanceContract` 改为 `Object`（类型校验/强转下沉到 Bootstrap 侧），配套 `AgentJarManifestTest` 新增字节码级回归守卫 `premainClassMustNotReferenceContractType`
- `lingframe-agent-benchmark` 测量装置升级为「同 fork A/B 加固版」：`plainCall`（`PlainBatchTask`，同方法体未织入）与 `wovenCall` 同 JVM 对照、`BatchCore` 共享载荷（1k 行变换 + checksum 逃逸，防整方法消除）、指标由跨 fork ops/s 改为 fork 内 `avgt` µs/op 附加延迟、`@TearDown` 织入自检探针按 fork 模式断言
- `SeaTunnelAdapter` 弹性治理新增 `governance.resilience.fail-closed` 开关（默认 `false`）：`true` 时 `CIRCUIT_OPEN` / `BULKHEAD_FULL` 命中改为抛出 `GovernanceRejectException`，使被织入的 `AbstractTask.call()` 携带异常逃逸 → SeaTunnel Worker `catch(Throwable)` 触发 Failover / 任务快速失败（**熔断名副其实**）；`RATE_LIMITED` 无论该开关如何均软退避（丢弃限流批次等同数据丢失，不符批次调度语义）。默认保持 fail-open 软退避（sleep 后放行）以保证向后安全。新增 `GovernanceRejectException` 异常类 + `SeaTunnelAdapterTest.FailClosedCircuitBreaker` 三项单测（`fail-closed=true` 硬拒绝抛异常 / `false` 软退避 / `RATE_LIMITED` 恒软退避）
- **弹性治理默认值对齐 + 防伪开启**：默认配置回归「**不开启批次级治理**」（纯 ClassLoader 清理，零治理损耗，刻意设计）——`circuit-breaker-enabled`/`rate-limiter-enabled` 默认由 `true` 改为 `false`，与「默认不治理」的行为对齐（此前 YAML 默认显示熔断/限流开、但批次切点默认关 → 特性静默不生效，配置与行为不一致）。`AgentConfig` 新增 `isEffectiveTaskExecutionAdviceEnabled()`：批次切点有效开关 = 显式 `task-execution-advice-enabled` **或** 熔断/限流/灰度/权限任一特性启用——任一特性被**显式开启**时切点自动联动织入（无需单独记着开切点），杜绝「显式开了弹性却静默失效」的伪开启；premain 织入判定与 MBean 配置摘要改用有效开关
- **失败率误计修复**：`SeaTunnelAdapter.recordTaskMetrics` 仅对「下游可用性失败」（超时 / IOException / SQLException / 连接类 RuntimeException，遍历 cause 链做类型 + 类名 + 消息关键字启发式判定）调用 `recordFailure`；普通业务异常（Transform/数据校验 NPE 等）不再计入——此前对 `call()` 抛的任何 Throwable 都记失败，SeaTunnel 正常业务异常虚高虚拟灵元失败率、触发 `DEGRADED` 后每批次 +100ms 软退避自我放大延迟。业务失败由 SeaTunnel 自身 Failover/重试处理，agent 熔断只关心下游可用性
- **`HazelcastConfigCenter` 热刷 key 扩容**：新增 `bulkhead-max-concurrent` IMap key 并接入 `buildConfigFromMap`——此前 `bulkheadMaxConcurrent` 硬编码随 fallback 传递（固定 10），运维无法动态调整舱壁并发
- **熔断态 JMX 复位入口**：`LingFrameAgentObservabilityMBean` 新增 `resetCircuitBreaker()`，经 `SeaTunnelAdapter.resetHealthMetrics()` 调用 `LingHealthMetrics.reset()` 清空失败计数、解除 `DEGRADED`——真实下游恢复后无需等待 `circuit-breaker-wait-duration` 自动恢复窗口，可即时复位

### Fixed
- **代码/文档对齐修正（2026-09-06）**：`TcclGuardAdvice` 类注释同步为 `ReleasedClassLoaderRegistry` 实际实现（ReadWriteLock）；README「能力边界」更新为 8 组双维度矩阵最终表述（真实 JDBC/MySQL/Redis/MongoDB source+sink，8.63~10.19MB 全收敛）；测试计数对齐当前实测。
- `SeaTunnelAdapter.ensureConfigCenterInit()` 由「一次性尝试」改为「按 5 秒间隔重试直至成功」：原实现以 `configCenterInitAttempted` 布尔标记一次性封死，集群模式下 Hazelcast 成员发现未完成时首次探测失败即永久放弃重试，导致分布式动态配置静默失效
- `README.md` 能力矩阵表述修正：分布式动态配置标注为「依赖批次治理切点」（虚拟灵元限流/熔断参数仅由 Pipeline 内弹性 Filter 消费，默认模式下无消费方）；权限审计标注为「默认关闭」并补充单独开启会触发全量拒绝告警的副作用说明
- `lingframe-agent-e2e` 端到端验证收尾：补齐缺失的测试资源 `src/test/resources/seatunnel.yaml`（`classloader-cache-mode: false`，因 `ClassLoaderReleaseAdvice` 在缓存模式下跳过物理释放，开启缓存会使泄漏验证失去意义）与 `src/test/resources/lingframe-governance.yaml`（ governance 开启、批次切点关闭以纯测 ClassLoader 清理），使 `docker compose up` 挂载源不再缺失
- `MetaspaceLeakIT.isDockerContainerRunning()` 存活探测修正：原实现仅判断 `docker inspect` 退出码 `== 0` 会误判「已停止（Exited）」容器为运行中，改为解析 `{{.State.Running}}` 输出并经 `Boolean.parseBoolean` 判定，无 Docker 或容器未运行时 `Assumptions` 优雅跳过而非 `docker exec` 失败
- **全治理模式双副本 `LinkageError`（JMH 基准端到端复现）**：首个被织入的 `AbstractTask.call()` 抛 `loader constraint violation`——根因是 `LingFrameAgentPremain` 字节码对 `registerContract` 调用签名直接引用契约接口，HotSpot 在 premain 类加载验证期把 `LingGovernanceContract` 从 AppClassLoader 抢载（早于 `appendToBootstrap`），与运行期 Bootstrap 注入副本双身份。修复：`registerContract(Object)` 签名下沉 + `premain()` 委托私有 `activate()` 纵深防御。验证：`-Xlog:class+load` 实证接口仅剩 Bootstrap 单一副本、governed 档正常出分、三档织入自检 PASS
- `run-benchmark.sh` 收尾清理修复：暂存目录删除改用 git-bash 原生 `/tmp` 路径并吞掉安全删除层失败（此前沙箱把裸 `C:/...` 当相对路径拼接导致 trash 失败、遗留目录、退出码误报 1）
- 清理 `lingframe-agent-e2e/pom.xml` 中未实际使用的 `testcontainers.version` 属性（E2E 用裸 `ProcessBuilder` 调 docker CLI，未引入 Testcontainers 依赖）
- 仓库卫生：`.gitignore` 补全 `dependency-reduced-pom.xml` / `target-verify/`，并对已误跟踪的 `lingframe-agent-dist/dependency-reduced-pom.xml` 执行 `git rm --cached`（磁盘文件保留，后续不再进版本库）
- **`MetaspaceLeakIT` A/B 对比审计完善（2026-09-09）**：
  - **作业 FINISHED 验证**：`waitForJobFinished` 轮询 REST API 确认终态，API 返回无 jobStatus 时回退到容器日志搜索状态转换记录（`cleanHazelcastJobState` 误删 `IMAP_FINISHED_JOB_STATE` 致 REST API 返回 `{"jobId":"xxx"}` 无 jobStatus 字段，根因后续定位并修复，见下条）
  - **对照组放开**：取消注释 `seatunnel-native` 容器与 A/B 对比断言，两组各跑 45 job 共 90 job；对照组跑完后 `resetKafkaTopics()` 清空 Kafka topics 确保公平
  - **审计报告表格化**：`MetaspaceAuditResult` 增加 `loadedDelta`/`unloadedDelta`/`classLoaderCount` 字段，报告改为三列对齐表格（Native/Agent/Delta），含 Metaspace 各阶段 + 类加载/卸载/retained + `SeaTunnelChildFirstClassLoader` 存活实例数（`jmap -histo:live`，与 SeaTunnel 官方 E2E 一致）
  - **`formatDelta` 负值修复**：负 delta 是有意义的（Agent 比 Native 少），不应显示 N/A；采样失败改用 `Long.MIN_VALUE` 表示，`formatDelta` 只对 `Long.MIN_VALUE` 返回 N/A
  - **CI 实测结果**（run #34297752012）：Agent Metaspace growth 13.47MB < 15MB 阈值，ClassLoader 拦留 0，类卸载率 94%；Native 对照组 growth 248.95MB，ClassLoader 拦留 240，类卸载率 0.1%；Net Overhead = -235.48MB（Agent 反而比 Native 少 235.48MB）
  - **`cleanHazelcastJobState` 排除 `IMAP_FINISHED_JOB_STATE`（2026-09-09）**：`EngineClassLoaderCleaner.cleanHazelcastJobState` 遍历 Hazelcast IMap 时对名称含 `job`/`checkpoint`/`engine`/`running` 的 Map 执行 `remove(jobId)`，但 `IMAP_FINISHED_JOB_STATE`（名称含 `finished-job-state`）存储的是 `JobStatus` 枚举值，不持有 ClassLoader 或领域对象引用——删除它无助于 ClassLoader 回收，却破坏 REST API `getJobInfoJson` 对已完成作业的状态查询（API 查不到 finished IMap 记录，返回 `{"jobId":"xxx"}` 无 jobStatus 字段）。修复：条件判断增加 `!name.contains("finished-job-state")` 排除项，保留已完成作业状态供 REST API 查询
  - **`dumpClassLoaderStats` 按 ClassLoader 分组打印类统计（2026-09-09）**：审计报告末尾新增 `jmap -clstats` 诊断，按 CL 类型分组输出各类 ClassLoader 的存活类数与 alive/dead 状态——证明 retained classes 来自 `<bootstrap>` + `AppClassLoader`（live），而非 `SeaTunnelChildFirstClassLoader`（dead，0 classes）。选用 `jmap -clstats` 而非 `-XX:+TraceClassUnloading` 的原因：实证发现 Corretto JDK 8.0.442 上 `-XX:+TraceClassUnloading` / `-verbose:class` 均不产出卸载 trace（flag 存在但无输出），`jmap -clstats` 在 JDK 8 上原生可用且直接给出按 CL 分组的 alive/dead 状态
  - **`cleanFinished` force-evict 跳过活跃作业修复竞态（2026-09-09）**：`cleanFinished` 从 `finishedExecutionContexts`（任务级完成）提取 jobId 后直接调用 `forceEvictJobClassLoaders`，但 job 可能仍在 `runningJobMasterMap` 中（RUNNING 状态——其他 task 完成通知尚未被 Coordinator 处理）。此时 ClassLoader 释放与 task 完成通知竞态，通知丢失导致 job 永远卡在 RUNNING（CI run #34304804774 flaky root cause：job-30 SinkTask taskDone 与另一 job 的 fault-cleanup 触发的 force-evict 同时发生）。修复：force-evict 前调用 `collectActiveJobIds()` 收集 `runningJobMasterMap` 中的活跃作业 ID，跳过仍在 RUNNING 的 job——其 ClassLoader 由 `cleanJobMaster` 在 job 真正 FINISHED 后处理
  - **`cleanHazelcastJobState` 排除 `running-job` IMap（2026-09-09）**：`cleanHazelcastJobState` 条件 `name.contains("running")` 匹配 `running-job` IMap，`cleanFinishedJobMasters` 在引擎 `JobMaster.cleanJob()` 之前删除该 IMap 条目，导致 REST API `GET /running-job/{jobId}` 返回 `{"jobId":"xxx"}` 无 jobStatus（测试回退到容器日志）。修复：条件增加 `!name.equals("running-job")` 排除项——`running-job` 由引擎 `cleanJob()` 自行管理（job FINISHED 后从中移除并转入 `finished-job-state`），Agent 提前删除只会破坏 REST API 查询，无助于 ClassLoader 回收
  - **`dumpClassLoaderStats` 解析增强 + 诊断日志（2026-09-09）**：CI 实测 `jmap -clstats` 输出全 0，根因是解析器仅按 tab 分隔（`split("\\t")`），容器中 `jmap` 输出可能使用空格分隔。修复：tab 分隔失败时回退到 `split("\\s+")` 空格分隔；当解析到 0 行数据时打印退出码和原始输出前 20 行（WARN 级别），便于定位容器内 `jmap` 格式差异或命令失败
  - **容器加 `SYS_PTRACE` capability 修复 `jmap` attach 失败（2026-09-09）**：CI 实测 `jmap -clstats` 报 `ptrace(PTRACE_ATTACH, ..) failed: Operation not permitted`（exitCode=1），根因是 Docker 容器默认不授予 `SYS_PTRACE`，`jmap` 的 Serviceability Agent 无法 attach 到 JVM 进程。修复：`docker-compose.yml` 中 `seatunnel-server` 和 `seatunnel-native` 均添加 `cap_add: [SYS_PTRACE]`
  - **`waitForJobFinished` 查 `finished-job-state` IMap 作为 fallback（2026-09-09）**：job FINISHED 后引擎 `cleanJob()` 将条目从 `running-job` 移除并转入 `finished-job-state`，2 秒轮询可能错过 FINISHED 窗口导致 REST API 返回无 jobStatus（回退到容器日志产生 WARN）。修复：当 `running-job` 返回无 jobStatus 时，先查 `finished-job-state` IMap 确认是否已 FINISHED，命中则直接返回（`via REST API finished-job-state`）；轮询间隔从 2 秒缩短到 1 秒增加命中 `running-job` FINISHED 窗口的概率
  - **`waitForJobFinished` fallback 日志降级 + finished-job-state 诊断（2026-09-09）**：CI 实测多数 job 仍走容器日志回退（`running-job` 返回 `{"jobId":"xxx"}` 无 jobStatus 是 job FINISHED 后的正常状态），原 WARN 级别 "Falling back to container log" 产生大量噪声。修复：将 fallback 日志从 WARN 降为 INFO（正常 fallback 路径不应 WARN），仅在距超时不足 10 秒且 REST API + 容器日志均无法确定状态时才 WARN（一次）；`queryFinishedJobState` 增加 DEBUG 级别日志记录 HTTP 状态码和原始 response body，诊断 `finished-job-state` IMap 查询为何返回 null
  - **`dumpClassLoaderStats` 解析逻辑增强 + 异常诊断（2026-09-09）**：CI 实测 `<bootstrap>` 0 classes（应为数千）、`other CLs` 5314884 classes（500 万，解析错位），根因是容器中 `jmap -clstats` 输出的 bootstrap 行 type 字段可能不是 `<bootstrap>`（JDK 8 某些版本为 `<internal>`），且 SeaTunnelChildFirstClassLoader 未被 typeStr 匹配（349 个 dead CL 全归到 other）。修复：(1) bootstrap 匹配扩展为 clRef/typeStr 含 `bootstrap`/`<internal>`/`null`/全零地址；(2) SeaTunnelChildFirstClassLoader 匹配扩展为整行包含检查；(3) `total` 行跳过从 `startsWith` 改为 `contains`；(4) 当 `bootstrap==0` 或 `other>100000` 或 `subCl 全 0 但 other 有 dead` 时打印原始输出前 30 行（WARN 级别），定位容器内 `jmap` 格式差异
  - **`parseClassLoaderStats` 提取为 static 方法 + 本地离线单元测试（2026-09-09）**：将 `jmap -clstats` 解析逻辑从 `dumpClassLoaderStats` 提取为 `static ClassLoaderStatsResult parseClassLoaderStats(List<String> lines)`，供 `MetaspaceAuditOfflineTest` 离线验证。本地 Corretto JDK 8.0.442 实跑 `jmap -clstats` 获取真实输出格式（tab 分隔，bootstrap 行 clRef=`<bootstrap>` typeStr=`<internal>`），新增 5 项单元测试覆盖：本地真实输出解析、SeaTunnelChildFirstClassLoader 归类、空格分隔回退、bootstrap type 变体（`<bootstrap>` vs `<internal>`）、`total` 行前导空格跳过。解析逻辑在本地 100% 验证通过，容器格式差异由诊断日志暴露
  - **`forceEvictJobClassLoaders` 延迟释放修复 REST API 序列化破坏（2026-09-09）**：CI 实测 Agent 组 45 job 中 43 个走容器日志降级（REST API 返回 `{"jobId":"xxx"}` 无 jobStatus），对照组零降级。根因：`forceEvictJobClassLoaders` 在引擎 `JobMaster.cleanJob()` 移除 `running-job` IMap 条目之前释放 ClassLoader，`running-job` 中的 `JobInfo` 序列化时 jobStatus 字段引用的类已卸载，序列化失败致字段丢失。修复：`forceEvictJobClassLoaders` 开头检查 `running-job` IMap 中是否仍有该 jobId 条目（`isJobInRunningJobIMap`），有则跳过释放（延迟到下一清理周期），保证 REST API 在引擎自行移除 `running-job` 条目前序列化正常
  - **`jmap -clstats` 基线对照（2026-09-09）**：`dumpClassLoaderStats` 拆分为 `captureClassLoaderStats`（采集+解析）+ `dumpClassLoaderStats`（打印+对比），`runTestMatrixAndAudit` 基线采样时同步采集 CL 基线存入 `MetaspaceAuditResult.baselineClStats`，终态打印时输出各 CL 类别的 delta（bootstrap/AppCL/SeaTunnelChildFirstCL/other 的 classes 和 alive/dead 变化），直观呈现"多的类跑哪去了"
  - **`jmap -clstats` stdout/stderr 分离修复行交错（2026-09-09）**：CI 实测 Agent 组 bootstrap 行被 stderr 内容截断（`<bootstrap>\t2841liveness analysis may be inaccurate...`），根因是 `ProcessBuilder.redirectErrorStream(true)` 混合 stdout+stderr 导致 jmap 的诊断警告插入数据行中间，`parseLong` 失败跳过 bootstrap 行。修复：移除 `redirectErrorStream(true)`，用 daemon 线程排空 stderr（防管道死锁），只解析 stdout；suspicious 诊断时同步打印 stderr 前 10 行辅助定位
  - **`isJobInRunningJobIMap` INFO 诊断日志（2026-09-09）**：CI 实测 Agent 组降级日志仍大量出现（`isJobInRunningJobIMap` 检查可能未生效），但 DEBUG 日志默认不输出无法确认。修复：`isJobInRunningJobIMap` 所有分支（无 Hazelcast 实例 / 命中 running-job / 未命中 / 异常）改为 INFO 级别，`forceEvictJobClassLoaders` 延迟释放日志同步改为 INFO，CI 日志可直接确认检查是否执行及结果
  - **suspicious result 误触发修复（2026-09-09）**：对照组 baseline 无 SeaTunnelChildFirstClassLoader 但有少量 dead DelegatingClassLoader，原条件 `subClAlive+subClDead==0 && otherDead>0` 误触发 suspicious 日志。修复：去掉该条件，改为 `otherDead>100`（大量死 CL 才可疑，少量属正常）
  - **release 打包同版本覆盖更新（2026-09-09）**：CI workflow `ci.yml` 原逻辑：tag 已存在时 `exists=true`，后续步骤全部 `if: exists=='false'` 跳过，同版本重新构建无法发布。修复：tag 已存在时先 `git push origin :refs/tags/$TAG` 删除远端旧 tag + `gh release delete $TAG` 删除旧 release，后续步骤移除 `if` 条件始终执行，实现同版本覆盖更新
  - **泄漏判定逻辑综合调整（2026-09-09）**：原判定用 Metaspace growth < 15MB 和 net overhead <= 2MB 两条硬断言，CL=0（无泄漏）时仍可能因正常类加载导致 growth 超阈值误报。调整为：主要断言改为 `SeaTunnelChildFirstClassLoader 拥留实例数 == 0`（CL=0 是泄漏直接判定——ClassLoader 被回收则类随之可卸载），Metaspace growth / net overhead / retained classes 超阈值时降为 WARN 提醒不断言失败（类元数据未卸载在 CL=0 时属 bootstrap/AppCL 正常驻留）；采样失败时 `Assumptions.assumeTrue` 跳过而非误判；与 SeaTunnel 官方 E2E `classLoaderObjectCheck` 方法对齐

### Removed
- `TaskDeployAdvice`（`deployLocalTask` 拦截）：已由 `TaskExecutionAdvice`（`AbstractTask.call()` 批次调度治理）替代
- `GovernedTaskGroupHandler`（动态代理包装 + `wrapTaskGroup`）：已由 Pipeline `GOVERN_ONLY` 模式替代
- `SeaTunnelAdapter` 中自造弹性组件：`circuitBreaker`/`rateLimiter` 字段、`createCircuitBreaker`/`createRateLimiter`/`refreshResilienceComponentsIfNeeded` 方法、`lastConfigSignature` 字段

### Added（本轮）
- **`lingframe-agent-benchmark`：JMH「完整 SeaTunnel 版」基准模块**。
  fork 独立 JVM + `-javaagent` 织入**真实 `AbstractTask.call()`**（经其具体子类 `NoopBatchTask`），
  三种 fork 对比：`no-agent` 基线 / `agent-default`（仅 ClassLoader 清理）/ `agent-governed`
  （全治理切点），损耗口径 `(基线 − 治理) / 基线`。含 `run-benchmark.sh`、专用治理配置
  `benchmark-governance.yaml` 与本模块 README（测法、运行、结果解读）。
  依赖 SeaTunnel 预构建 jar（system scope 本机直引，pom 有注释），本机专用模块。

### Known（拦截损耗评估，2026-09-06）
- **拦截损耗口径（2026-09-06 实测确立）**：钩子 ~100µs 主体是**任何代码低频调用皆付的冷 cache 重建**（物理成本）；真实引擎 call() 按数据节奏连续轮询（`TaskExecutionService` `do{call()}while(!isDone)`，轮次粒度由 connector 批大小决定，冒烟 job 51 次 call/3s、每轮 body<1ms），非固定 100ms 节拍。**正确口径 = job 级总耗时膨胀 + 单 worker CPU 增量**。
- **机制归因（JFR 零阈值采样 + BodyProbe5 三模式判别）**：main 线程几乎不 park、EventBus 无订阅直接 return → 成本 = **钩子热路径闲置 ≥~20ms 后的冷 cache 重建**（keeper 后台每 2ms 踩同款 pipeline → 20ms 间隔成本 +144µs → **+38µs**；大样本间隔曲线 2/5/20/100ms → +72/+95/+124/+143µs，20ms 后饱和）。
- **实际影响评估**：钩子成本与频率自洽 + IO 型引擎每轮 body≥10ms 常态 → 典型 **job 级影响 <0.5%**（冒烟锚点 ~0.24% 量级），高频短轮流式单 worker 理论上限 ~1-2%（钩子热态最低成本档）——**属正常范围，不构成部署障碍，无需架构级改动**。
- **真实引擎端到端冒烟通过（2026-09-06）**：发行版 `apache-seatunnel-3.0.0-SNAPSHOT-bin` 单节点 Zeta（master_and_worker）+ governed agent，fake→console batch job 3s 完成 **1000 Read / 1000 Write / 0 失败**，引擎日志实证每个真实 `AbstractTask.call()` 产生 Trace（IN/OUT）+ Audit（success=true）事件并经 `ling-eventbus-async` 异步分发——织入与治理链路在真实引擎完整工作，bridge 双副本缺陷未复发。
  **Windows 部署坑**：发行包 `.sh`/`.cmd` 的 classpath 分隔符按 Unix 约定（`:`）拼装，在 Windows JVM 上整段失效 → premain 期 `HazelcastConfigCenter` 首触 `ClassNotFoundException`；需手工拼 java 命令（分号 classpath + 盘符路径，javaagent 参数路径须为 ASCII）最稳。

### Added（熔断失败判定契约化，2026-09-07）
- **`FailureClassifier`：熔断失败判定契约化分类器（两层）**：Layer 1 JDK 异常家族类型契约（`IOException`/`SQLException`/`TimeoutException`，沿 cause 链遍历）；Layer 2 运维显式配置（`downstream-readable-failures-patterns` 正向包含 / `business-exceptions-patterns` 反向排除且优先级最高），覆盖自研 connector 免发版接入；兜底保留既有消息关键字启发式（默认配置下与既有实现逐例一致，黄金样本回归）。新增 `AgentConfig` 参数：`classifier-enabled`（默认 true，false 直回既有启发式回滚）、两个 patterns 列表（YAML list / 单字符串均支持）。
- **`SeaTunnelAdapter` 失败判定统一委托分类器**：`isDownstreamAvailabilityFailure` 改由 `FailureClassifier` 裁决（删除适配器内手写启发式副本），`isTimeoutError` 类型判定优先；MBean 配置摘要新增 `classifier` 状态暴露，决策 100% 可观测。
- **`FailureClassifierTest`（§4.3 判据 1-4，6 项）**：类型契约任意消息文本命中 / 显式排除优先级高于下游判定 / 默认配置黄金样本回归 / HitLevel 决策可观测 + `classifier-enabled=false` 回滚直回启发式；非法正则跳过不阻断。
- **`DualJobFaultInjectionIT`：双作业故障注入 e2e**：场景 1 故障注入隔离（作业 A 下游故障 → A 熔断硬拒绝、故障计数记入 A 灵元、作业 B 100% 放行且零失败）；场景 2 自愈恢复闭环（OPEN→HALF_OPEN→CLOSED，A 重新放行，B 全程不受影响）；场景 3 fail-closed=false 安全基线（A 熔断打开软退避放行、不产生新拒绝语义，B 100% 放行）。

### Changed（作业级治理默认开启，2026-09-07）
- **`per-job-governance-enabled` 默认转正 `true`**：`AgentConfig` 构造器 + `fromMap` 三处默认值由 `false` 改为 `true`（含内嵌默认），治理域默认收敛到作业隔离单元——熔断/限流 opt-in 开启后即按作业隔离，无需再显式开启 per-job。安全兜底不变：运行期版本指纹门控（未知版本自动降级共享灵元）与 `isEffectiveTaskExecutionAdviceEnabled` 语义（per-job 不触发批次切点织入，纯 ClassLoader 清理模式零治理开销）。`LingFrameAgentActivationRunner` 装配注释同步。**回滚**：显式 `per-job-governance-enabled: false` 逐字节回引擎级共享灵元。
- **`lingframe-agent-e2e` MetaspaceLeakIT 资源显式 `per-job-governance-enabled: false`**：作业级治理默认开启后，该资源显式关闭以保持「纯测 ClassLoader 清理」语义（批切点关闭，不引入治理损耗噪声）。
- **基准/变更日志归档**：JMH 正式跑分 3 forks 结果归档（100ms 批附加 **+173.2µs** / per-job 轮转 **+183.4µs**，作业级查表仅 +10µs；按 job 级总耗时膨胀口径典型场景影响 **<0.5%**）。
- 项目初始化（2026-09-04 骨架，并入本版本）
- 独立仓库建立，通过 `lingframe-bom` 单点依赖收敛