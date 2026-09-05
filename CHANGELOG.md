# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Agent 骨架：三层类加载拓扑 + Bootstrap Bridge + ByteBuddy 织入框架
- `lingframe-agent-bridge`：极薄桥接契约工程（零第三方依赖，<20KB）
- `lingframe-agent-core`：ByteBuddy Advice 拦截器 + SeaTunnel 适配器
- `lingframe-agent-dist`：Maven Shade Fat-Jar 打包（ByteBuddy Relocate）
- `lingframe-agent-e2e`：端到端测试骨架（Docker Compose + Metaspace 泄漏验证）
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

### Removed
- `TaskDeployAdvice`（`deployLocalTask` 拦截）：已由 `TaskExecutionAdvice`（`AbstractTask.call()` 批次调度治理）替代
- `GovernedTaskGroupHandler`（动态代理包装 + `wrapTaskGroup`）：已由 Pipeline `GOVERN_ONLY` 模式替代
- `SeaTunnelAdapter` 中自造弹性组件：`circuitBreaker`/`rateLimiter` 字段、`createCircuitBreaker`/`createRateLimiter`/`refreshResilienceComponentsIfNeeded` 方法、`lastConfigSignature` 字段

## [0.1.0] - 2026-09-04

### Added
- 项目初始化
- 独立仓库建立，通过 `lingframe-bom` 单点依赖收敛