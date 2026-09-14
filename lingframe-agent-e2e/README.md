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
| `MetaspaceLeakIT` | Docker Compose 起 SeaTunnel 集群，连续多轮作业验证 ClassLoader 物理释放后 Metaspace 零增长（14 组作业 × 7 轮 × 2 组 = 196 作业次） | **CI-only**（需 Docker，本机 `Assumptions` 优雅跳过）|

## 作业矩阵

作业配置外置化于 `src/test/resources/e2e-jobs/` 目录，每个文件是一个标准 SeaTunnel JSON 作业配置。增删 JSON 文件即可调整测试矩阵，无需改代码。

### 现有 14 组作业

| 文件 | Source | Sink | Transform | 依赖服务 |
|------|--------|------|-----------|---------|
| `fake2console.json` | FakeSource | Console | - | 无 |
| `fake2file.json` | FakeSource | LocalFile | - | 无 |
| `fake2kafka.json` | FakeSource | Kafka | - | Kafka |
| `fake2mysql.json` | FakeSource | Jdbc | - | MySQL |
| `file2file_sql.json` | LocalFile | LocalFile | Sql | 无 |
| `file2mysql_sql.json` | LocalFile | Jdbc | Sql | MySQL |
| `kafka2console.json` | Kafka | Console | - | Kafka |
| `kafka2kafka.json` | Kafka | Kafka | - | Kafka |
| `kafka2mysql.json` | Kafka | Jdbc | - | Kafka + MySQL |
| `mysql2console.json` | Jdbc | Console | - | MySQL |
| `mysql2file_sql.json` | Jdbc | LocalFile | Sql | MySQL |
| `mysql2kafka.json` | Jdbc | Kafka | - | MySQL + Kafka |
| `mysql2kafka_sql.json` | Jdbc | Kafka | Sql | MySQL + Kafka |
| `mysql2mysql.json` | Jdbc | Jdbc | - | MySQL |

### 添加新作业

在 `e2e-jobs/` 目录新建 `.json` 文件，按 SeaTunnel 作业配置格式编写。`validateJobConfig` 会在加载时校验 JSON 结构与链路连通性。

```json
{
  "env": {"job.name": "示例名", "job.mode": "BATCH"},
  "source": [{"plugin_name": "FakeSource", "row.num": 1000, "schema": {"fields": {"id": "int", "name": "string"}}}],
  "sink": [{"plugin_name": "Console"}]
}
```

有 transform 时须显式指定 `plugin_input` / `plugin_output` 连接链路：

```json
{
  "env": {"job.name": "示例", "job.mode": "BATCH"},
  "source": [{"plugin_name": "Kafka", "plugin_output": "t_src", ...}],
  "transform": [{"plugin_name": "Sql", "plugin_input": "t_src", "plugin_output": "t_mid", "query": "select * from dual where id > 0"}],
  "sink": [{"plugin_name": "Console", "plugin_input": "t_mid"}]
}
```

## 资源文件

- `src/test/resources/e2e-jobs/`：14 组外置化作业配置（标准 SeaTunnel JSON）
- `src/test/resources/file-input/input.csv`：LocalFile 源数据（1000 行）
- `src/test/resources/seatunnel.yaml`：`classloader-cache-mode: false`（缓存模式下 `ClassLoaderReleaseAdvice` 跳过物理释放，泄漏验证失效）
- `src/test/resources/lingframe-governance.yaml`：governance 开启、**批次切点关闭**（纯测 ClassLoader 清理，不引入治理损耗噪声）
- `src/test/resources/docker-compose.yml`：SeaTunnel×2（Agent+Native）+ MySQL + Kafka 编排

## 参数配置

| 参数 | pom.xml 属性 | 默认值 | 说明 |
|------|-------------|--------|------|
| 串行轮次 | `lingframe.test.serial.rounds` | 2 | 逐轮采样 Metaspace 趋势 |
| 并发轮次 | `lingframe.test.concurrent.rounds` | 5 | 多 Job 异构并发压测轮数 |
| 外置作业目录 | `lingframe.test.job.dir` | 未设置 | 未设置时读 classpath `e2e-jobs/` |

> 改轮数须改 pom.xml 的 `<properties>`，不是改代码中 `Integer.getInteger` 的默认值。

## 运行

```bash
# 本地可跑（AgentJarManifestTest，秒级）
mvn -o -pl :lingframe-agent-e2e -Dtest=AgentJarManifestTest test

# MetaspaceLeakIT（需 Docker）
docker compose -f src/test/resources/docker-compose.yml up -d
mvn -o -pl :lingframe-agent-e2e -Dtest=MetaspaceLeakIT test

# 使用外部作业目录
mvn -pl :lingframe-agent-e2e -Dlingframe.test.job.dir=/path/to/custom-jobs verify
```

## 依赖

- `lingframe-agent-dist`（提供 Fat-Jar 给测试扫描/挂载）
- testcontainers 未使用——裸 `ProcessBuilder` 调 docker CLI（pom 已清理无用 `testcontainers.version` 属性）
