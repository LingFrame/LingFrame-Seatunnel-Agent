# lingframe-agent-e2e — 端到端测试

Agent 真实 `-javaagent` JVM 下的端到端验证套件，包含 Fat-Jar 打包守卫与 Metaspace 泄漏压测。

## 测试套件

| 测试 | 内容 | 运行环境 |
|---|---|---|
| `AgentJarManifestTest` | 扫 Fat-Jar 字节码断言：Manifest 标志（Premain/Agent/Can-*）、bridge 类已打入、ByteBuddy/SnakeYAML 已 Relocate、premain 不含 `LingGovernanceContract` 引用（防双副本回归）| 本地 `mvn test`（无需 Docker）|
| `JobIsolationIT` | 作业级隔离验收：A 熔断打开 B 100% 放行 / 未知版本降级共享灵元 / per-job 关闭回引擎级 | 本地 `mvn verify`（进程内装配真实微内核链，无需 Docker）|
| `JobConfigRefreshIT` | 作业级配置热刷：自动发现初始化 / 作业级刷新 / 全局广播与覆盖优先级 | 本地 `mvn verify`（无需 Docker）|
| `JobLifecycleLeakIT` | 千级作业零残留：1000 作业后灵元/弹性缓存/指标注册表零残留 | 本地 `mvn verify`（约 3.4s，无需 Docker）|
| `DualJobFaultInjectionIT` | 双作业故障注入：注入隔离 / OPEN→HALF_OPEN→CLOSED 自愈恢复闭环 / fail-closed=false 软退避安全基线 | 本地 `mvn verify`（无需 Docker）|
| `MetaspaceLeakIT` | Docker Compose 起 SeaTunnel 集群，连续 1000 次作业验证 ClassLoader 物理释放后 Metaspace 零增长 | **CI-only**（需 Docker，本机 `Assumptions` 优雅跳过）|

## 资源文件

- `src/test/resources/seatunnel.yaml`：`classloader-cache-mode: false`（缓存模式下 `ClassLoaderReleaseAdvice` 跳过物理释放，泄漏验证失效）
- `src/test/resources/lingframe-governance.yaml`：governance 开启、**批次切点关闭**（纯测 ClassLoader 清理，不引入治理损耗噪声）
- `src/test/resources/docker-compose.yml`：SeaTunnel + agent 挂载编排

## 运行

```bash
# 本地可跑（AgentJarManifestTest，秒级）
mvn -o -pl :lingframe-agent-e2e -Dtest=AgentJarManifestTest test

# MetaspaceLeakIT（需 Docker）
docker compose -f src/test/resources/docker-compose.yml up -d
mvn -o -pl :lingframe-agent-e2e -Dtest=MetaspaceLeakIT test
```

## 依赖

- `lingframe-agent-dist`（提供 Fat-Jar 给测试扫描/挂载）
- testcontainers 未使用——裸 `ProcessBuilder` 调 docker CLI（pom 已清理无用 `testcontainers.version` 属性）
