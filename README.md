# LingFrame SeaTunnel Agent

> Java Agent 外挂，为零源码修改为 Apache SeaTunnel 提供 LingFrame 治理能力。

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

### 治理配置

在 `$SEATUNNEL_HOME/config/lingframe-governance.yaml` 中配置：

```yaml
governance:
  enabled: true
  dev-mode: false          # false=零信任(默认)，true=开发模式放行未声明权限
  task-execution-advice-enabled: false  # 批次级治理切点，默认关闭
  routing:
    gray-routing-enabled: false
  security:
    permission-enabled: false
```

## 治理能力

当前版本以 `GOVERN_ONLY` 模式借道 LingFrame 治理流水线，真实生效范围如下：

| 能力 | 状态 | 说明 |
|---|---|---|
| 指标收集 + 事件发布 | **已生效** | TrafficMetricsFilter / EventBus 深度集成，调用量、QPS、延迟与异常指标实时可观测 |
| 审计追踪 | **已生效** | 治理链路 Trace 采集，可通过 EventBus 订阅审计事件 |
| ClassLoader 深度清理 | **已生效** | ClassLoaderReleaseAdvice 织入 releaseClassLoader 拦截点，触发 JDBC 驱动注销 + URLClassLoader 关闭 + ThreadLocal 深度安全清理 |
| 权限审计 | **可配置** | `governance.dev-mode: false`（默认）时零信任拒绝未声明权限；`true` 时放行便于联调 |
| 熔断 / 限流 | **默认关闭，可显式开启** | 切点在 `AbstractTask.call()` 批次调度级（非数据流 Hot Path）。默认关闭，需设置 `governance.task-execution-advice-enabled: true` 显式开启。开启后 `beforeTaskCall` 构造 `InvocationContext`（GOVERN_ONLY 模式）调 `pipelineEngine.invoke(ctx)` 走完整 12 Filter 链前置治理（含 `ResilienceGovernanceFilter` 限流/熔断前置检查）；`afterTaskCall` 回灌真实业务结果到 `LingHealthMetrics`，触发 `RuntimeStatus.DEGRADED → MacroStateGuardFilter` 熔断路径 |
| 路由 / 状态守卫 | **已生效（虚拟灵元 ACTIVE）** | 注入生产级 VirtualLingManager 生成的虚拟灵元（状态处于 ACTIVE），MacroStateGuardFilter 与指标双向闭环联动 |
| 灰度路由 | **已装配，可扩展** | LabelMatchRouter 已注册，支持结合扩展灵元定义细粒度流量路由策略 |
| 分布式动态配置 | **已生效** | HazelcastConfigCenter 通过 IMap `lingframe-governance-config` 监听 5 类 Entry 事件（新增/更新/删除/驱逐/过期），毫秒级热刷新虚拟灵元 LingRuntimeConfig，支持配置删除安全回退 |

> **治理链路说明**：Agent 默认仅启用 ClassLoader 深度清理（零性能损耗与零业务干扰）。
> 批次级治理（`AbstractTask.call()` 切点）默认关闭——需在 `lingframe-governance.yaml` 中显式设置
> `governance.task-execution-advice-enabled: true` 开启，开启时日志输出风险提示
> （吞吐下降、Checkpoint 超时风险），请监控后决定是否保持开启。

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
# 交付物：lingframe-agent-dist/target/lingframe-seatunnel-agent.jar
```

## 质量门控

```bash
mvn -B clean verify -Pintegration-check
```

## 版本

独立版本号，不跟随 LingFrame 主仓。格式：`{agent-version}-seatunnel-{seatunnel-baseline}`。

## License

Apache License 2.0