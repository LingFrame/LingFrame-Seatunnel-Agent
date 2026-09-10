# lingframe-agent-core — Agent 核心实现

Agent 主实现模块：premain 入口、配置加载、SeaTunnel 适配器、ByteBuddy Advice、可观测性、Pipeline 装配。

## 关键包与类

| 包 | 职责 |
|---|---|
| `com.lingframe.agent` | `LingFrameAgentPremain`（premain 入口，委托 `activate()` 延迟类型解析）|
| `com.lingframe.agent.config` | `AgentConfig`（YAML + `SEATUNNEL_HOME` + `-Dlingframe.agent.*` 系统属性三级覆盖）|
| `com.lingframe.agent.adapter` | `SeaTunnelAdapter`（实现 bridge 契约，before/after 钩子接 Pipeline GOVERN_ONLY）|
| `com.lingframe.agent.advice` | `TaskExecutionAdvice`（织 `AbstractTask.call()`）/ `ClassLoaderReleaseAdvice`（织 `releaseClassLoader`）/ `TcclGuardAdvice`（织 `Thread.setContextClassLoader`）|
| `com.lingframe.agent.observability` | `LingFrameAgentObservability`（JMX MBean，暴露 advice 状态/EventBus/计时统计）|
| `com.lingframe.agent.pipeline` | `AgentPipelineFactory`（编程式装配 12-Filter 链 + EventBus + MetricsCollector + HazelcastConfigCenter）|

## 依赖

- `lingframe-agent-bridge`（契约接口，Bootstrap 加载）
- `LingFrame` 框架（Pipeline / EventBus / MetricsCollector / LingRepository 等，经 BOM 收敛）
- ByteBuddy 1.14.x（dist 阶段 Relocate）+ SnakeYAML（配置解析）

## 构建

```bash
mvn -o -pl :lingframe-agent-core -am install
```

## 测试

`mvn -o -pl :lingframe-agent-core test` — **118 个 @Test**（作业级治理：作业 ID 提取/注册表/退避控制器；熔断失败判定分类器 `FailureClassifier`；可观测性等），全绿。Checkstyle 0 违规，JaCoCo 覆盖率门槛 60%。

## 配置（lingframe-governance.yaml）

```yaml
governance:
  enabled: true
  task-execution-advice-enabled: false     # 批次治理切点开关（默认关）
  dev-mode: false
  observability:                           # 可观测性（系统属性 -Dlingframe.agent.* 覆盖优先）
    trace-level: INFO                       #   trace 事件日志级别 OFF/WARN/INFO
    audit-level: INFO
    log-sample-rate: 1                      #   日志采样率（每 N 次打 1 次）
    timing-enabled: false                   #   per-call 钩子耗时埋点（默认关，零损耗）
  resilience:
    circuit-breaker-enabled: false          # 基准/低频场景关，避免退避 sleep 主导
    rate-limiter-enabled: false
```

## JMX 可观测性

注册 MBean `cn.lingframe.agent:type=Observability`，jconsole/jcmd 可读：
- advice 安装状态 + 目标类存在性（防上游重构静默失效）
- EventBus 队列/丢弃/提交数
- per-call 钩子耗时 min/avg/max/count（timing-enabled=true 时）
- 配置摘要
