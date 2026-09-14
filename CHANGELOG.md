# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-09-14

### Added

- **E2E 作业矩阵扩展至 14 组**：新增 file-to-file、file-to-mysql（SQL transform）、mysql-to-file（SQL transform）、mysql-to-kafka（SQL transform）四组链路，覆盖 LocalFile 连接器与 SQL transform 多插件 CL 共存场景
- **作业矩阵外置化**：14 组作业提取为标准 SeaTunnel JSON 配置文件，置于 `src/test/resources/e2e-jobs/` 目录。增删 JSON 文件即可调整测试矩阵，无需改代码。支持 `-Dlingframe.test.job.dir` 指定外部目录
- **作业配置校验**：`validateJobConfig` 方法在加载时校验 JSON 结构完整性与链路连通性（source.plugin_output → transform.plugin_input → sink.plugin_input），断链 fail-fast
- **并发轮次提升至 5 轮**：`lingframe.test.concurrent.rounds` 从 3 调至 5，每组 98 作业（2 轮串行×14 + 5 轮并发×14），两组共 196 作业次

### Changed

- **CI 流水线并行优化**（E2E 耗时 27m8s → 21m）：
  - Docker 镜像预拉取与 Maven build 并行
  - Native-Control / Agent-Treatment benchmark 并行执行（Kafka topic 按组别前缀隔离）
  - jmap clstats 两组并行 dump
  - 作业 FINISHED 验证从串行改为 2 线程并发（`waitForJobFinished` 超时 240s）
  - 基线采样 `captureClassCounts` 与 `captureClassLoaderStats` 并行
  - 串行提交间隔 1000ms → 200ms
  - 引擎异步清理等待 30s → 10s
  - 并发提交后加 60s 等待，让 SeaTunnel 先完成一批作业再开始验证
  - REST readTimeout 30s → 10s，快速失败走容器日志 fallback
- **Metaspace 审计阈值动态化**：定值 15 MB 阈值改为 `(7 + 0.25 × N) × 1.5 MB` 动态阈值，趋势判定（Round 2 delta / Round 1 delta < 0.2）输出显眼 WARN，终极判定锚点为 ClassLoaderCount=0

### Fixed

- **SeaTunnel REST 端点并发压力**：4 线程并发验证压垮 REST 端点致 timeout，降至 2 线程并增加 60s 等待缓冲
- **pom.xml systemPropertyVariables 覆盖代码默认值**：`concurrentRounds` 代码默认值改为 5 但 pom.xml 硬编码 3 覆盖，修正 pom.xml 属性值

## [0.1.0] - 2026-09-10

### Added

- **Agent 骨架**：三层类加载拓扑（Bootstrap Bridge → App Core → 宿主 SeaTunnel）+ ByteBuddy 织入框架
- **`lingframe-agent-bridge`**：极薄桥接契约（零第三方依赖，<20KB），`registerContract(Object)` 签名避免双副本 `LinkageError`
- **`lingframe-agent-core`**：premain 入口、配置加载、SeaTunnel 适配器、ByteBuddy Advice、可观测性、Pipeline 装配
- **`lingframe-agent-dist`**：Maven Shade Fat-Jar 打包（ByteBuddy/SnakeYAML Relocate）
- **`lingframe-agent-e2e`**：端到端测试套件（Fat-Jar 守卫 + 作业隔离 + 配置热刷 + 故障注入 + Metaspace 泄漏 A/B 审计）
- **`lingframe-agent-benchmark`**：JMH 基准模块（同 fork A/B 加固版，真实 `AbstractTask.call()` 织入对比）
- **治理能力**：
  - `ClassLoaderReleaseAdvice`：`releaseClassLoader` 双态自适应清理 + JDBC 驱动注销 + ThreadLocal 深度安全清理
  - `TaskExecutionAdvice`：`AbstractTask.call()` 批次调度治理切面（默认关闭，任一治理特性开启时自动联动织入）
  - `TcclGuardAdvice`：`Thread.setContextClassLoader` TCCL 拦留防御
  - 作业级隔离（`per-job-governance-enabled` 默认 `true`）：治理身份按作业生成，限流/熔断/健康状态按作业隔离
  - `FailureClassifier`：熔断失败判定契约化分类器（JDK 类型契约 + 运维显式配置 + 启发式兜底）
  - `HazelcastConfigCenter`：分布式动态配置热刷新（5 个 key：限流/熔断阈值/滑动窗口/超时/舱壁并发）
  - `LingFrameAgentObservability`：JMX MBean（advice 状态/EventBus/计时/熔断复位）
- **质量门控**：Checkstyle + SpotBugs + JaCoCo（覆盖率门槛 60%）+ GitHub Actions CI

### Changed

- **弹性治理走 Pipeline `GOVERN_ONLY` 模式**：`beforeTaskCall` 调 `pipelineEngine.invoke(ctx)` 走完整 12 Filter 链前置治理，`afterTaskCall` 回灌真实业务结果触发熔断路径。删除自造 `CircuitBreaker`/`RateLimiter`，全部治理走 Pipeline
- **弹性治理默认值对齐**：`circuit-breaker-enabled`/`rate-limiter-enabled` 默认 `false`（纯 ClassLoader 清理，零治理损耗）。`isEffectiveTaskExecutionAdviceEnabled()` 实现防伪开启——任一治理特性显式开启时批次切点自动联动织入
- **`fail-closed` 开关**：默认 `false`（fail-open 软退避）；`true` 时 `CIRCUIT_OPEN`/`BULKHEAD_FULL` 硬拒抛 `GovernanceRejectException` 触发引擎 Failover。`RATE_LIMITED` 恒软退避（丢弃限流批次等同数据丢失）
- **退避策略**：`RATE_LIMITED` 按令牌间隔退避（`1000/rateLimit` ms + 抖动），替代固定 `sleep(100ms)`
- **失败率口径**：仅下游可用性异常（超时/IO/连接类）计入熔断失败率，普通业务异常不计入
- **`SeaTunnelAdapter` 拆分**：提取 `HookLatencyTracker` + `PipelineEventSubscriber` 两个类（625 → 526 行）
- **`cleanFinished` 拆分**：`purgeFinishedExecutionContexts` + `evictNonActiveClassLoaders` + 主流程
- **`LingFrameAgentBridge.registerContract` 签名**：由 `LingGovernanceContract` 改为 `Object`（类型校验下沉到 Bootstrap 侧），防 premain 类加载验证期双副本

### Fixed

- **`SeaTunnelAdapter` switch(null) NPE**：提取 `ErrorKind kind`，null 走 UNKNOWN 分支
- **`HazelcastConfigCenter` 集群启动期静默失效**：由一次性尝试改为按 5 秒间隔重试直至成功
- **`EngineClassLoaderCleaner` 与引擎竞态**：
  - force-evict 跳过活跃作业（`runningJobMasterMap`），避免任务完成通知丢失
  - 延迟 ClassLoader 释放直至 `engine_runningJobInfo` IMap 条目被引擎清理，避免 REST API HTTP 500
  - 不 nullify `jobMasterCompleteFuture`，避免 callback NPE
  - 不干预 `runningJobMasterMap` 移除，由引擎 `cleanJob()` 全权负责终态管理
- **`MetaspaceLeakIT` A/B 审计**：Docker Compose 全正交矩阵（Fake/MySQL/Kafka × Console/MySQL/Kafka），Agent 组 Metaspace 增长 14.06 MB（< 15MB 阈值），`SeaTunnelChildFirstClassLoader` 拋留 0，类卸载率 96.3%；Native 对照组增长 391.16 MB，残留 384，类卸载率 0.14%。Net Overhead = -377.10 MB
- **`run-benchmark.sh` Windows 清理**：暂存目录改用 git-bash 原生 `/tmp` 路径
- **仓库卫生**：`.gitignore` 补全 `dependency-reduced-pom.xml` / `target-verify/`

### Removed

- `TaskDeployAdvice`（`deployLocalTask` 拦截）：由 `TaskExecutionAdvice` 替代
- `GovernedTaskGroupHandler`（动态代理包装）：由 Pipeline `GOVERN_ONLY` 模式替代
- `SeaTunnelAdapter` 自造弹性组件：`circuitBreaker`/`rateLimiter` 字段及相关方法
- `cleanHazelcastJobState`/`isJobInRunningJobIMap`/`isJobRecentlyFinished`：基于错误假设添加，根因修复后删除
