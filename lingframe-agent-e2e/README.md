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
| `MetaspaceLeakIT` | Docker Compose 起 SeaTunnel 集群，连续多轮作业验证 作业完成及 GC 后 SeaTunnel 作业类加载器零残留，并记录元空间增长趋势（15 组作业 × 7 轮 × 2 组 = 210 作业次） | **CI-only**（需 Docker，本机 `Assumptions` 优雅跳过）|

## 作业矩阵

作业配置外置化于 `src/test/resources/e2e-jobs/` 目录，每个文件是一个标准 SeaTunnel JSON 作业配置。增删 JSON 文件即可调整测试矩阵，无需改代码。

### 现有 15 组作业

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
| `mongodb2hive.json` | MongoDB | Hive（Parquet + Snappy） | - | MongoDB + Hive Metastore + HDFS |

`mongodb2hive.json` 用于验证 [SeaTunnel #12456](https://github.com/apache/seatunnel/issues/12456) 的重复批作业场景。沿用原有 A/B、轮次和断言，Hive 连接器自动建表；`__GROUP__` 替换为 native/agent，隔离表名与 HDFS 路径。不需要 HiveServer2。官方 2.3.13 单节点镜像与原报告的分离部署、定制连接器存在差异，Native 未出现残留时不能宣称已复现。

该场景的 MAT 已定位三处持有源：Hadoop `CodecPool` 中以插件 `SnappyCompressor.class` 为 key 的缓存、`Token.renewers` 的 `ServiceLoader.loader`、MongoDB `BufferPoolPruner` 的执行器/线程安全上下文。Agent 在原有作业终态物理释放入口补充 `ConnectorResourceCleaner`，cleanup-only 模式同样执行，且早于关闭 URLClassLoader：

- `CodecPool` 只删除目标加载器的压缩/解压类型条目，同时删除对应计数缓存，并调用空闲实例的 `end()`；其他作业的池与计数保留。
- `Token.renewers` 仅在其 `loader` 就是目标加载器时，沿用原同步锁，将其绑定到 Token 定义加载器并 `reload()`，一并清掉旧 provider/迭代器引用。存在安全授权上下文时保留原状并告警；JDK 9+ 需要开放 `java.base/java.util` 的反射访问。
- MongoDB 4.7.1 仅对目标加载器自己定义的 `PowerOfTwoBufferPool.DEFAULT` 调用 `disablePruning()`，通过驱动关闭执行器，使线程退出。不按线程名中断线程，不关闭父加载器共享池。
- `ReflectionUtils.CONSTRUCTOR_CACHE` 只移除目标加载器定义的类及构造器。首次修复后的 MAT 显示，这是压缩器池路径之外的另一处持有源。
- HDFS `STRIPED_READ_THREAD_POOL` 的 `DaemonFactory` 本身继承 `Thread`，但从未启动，不属于活动线程扫描范围。仅重置其指向目标加载器的 TCCL，并在未启用 SecurityManager 时清除持有该加载器的继承安全上下文；保留原线程池和工厂。
- `ProtobufRpcEngine.CLIENTS` 按 SocketFactory 跨作业共享 RPC Client；仅将其中仍指向已释放加载器的 `Configuration` 绑定回宿主加载器，不停止 Client 或移除共享缓存条目。

类型查询使用 `findLoadedClass`，不因清理去加载尚未使用的连接器；JDK 9+ 需要开放 `java.base/java.lang` 和 `java.base/java.security`。未匹配的版本/不可访问字段会告警并跳过。新增回归测试使用真实 Hadoop 3.1.4 / MongoDB 4.7.1；最终是否消除残留仍以本矩阵的原有 GC 后零 ClassLoader 断言为准。

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
  "transform": [{"plugin_name": "Sql", "plugin_input": ["t_src"], "plugin_output": "t_mid", "query": "select * from dual where id > 0"}],
  "sink": [{"plugin_name": "Console", "plugin_input": ["t_mid"]}]
}
```

## 资源文件

- `src/test/resources/e2e-jobs/`：15 组外置化作业配置（标准 SeaTunnel JSON）
- `src/test/resources/file-input/input.csv`：LocalFile 源数据（1000 行）
- `src/test/resources/seatunnel.yaml`：`classloader-cache-mode: false`（缓存模式下 `ClassLoaderReleaseAdvice` 跳过物理释放，泄漏验证失效）
- `src/test/resources/lingframe-governance.yaml`：governance 总开关关闭（纯测 ClassLoader 清理，不引入治理损耗噪声）
- `src/test/resources/docker-compose.yml`：SeaTunnel×2（Agent+Native）+ MySQL + Kafka 编排

## 参数配置

| 参数 | pom.xml 属性 | 默认值 | 说明 |
|------|-------------|--------|------|
| 串行轮次 | `lingframe.test.serial.rounds` | 2 | 逐轮采样 Metaspace 趋势 |
| 并发轮次 | `lingframe.test.concurrent.rounds` | 5 | 多 Job 异构并发压测轮数 |
| 外置作业目录 | `lingframe.test.job.dir` | 未设置 | 未设置时读 classpath `e2e-jobs/` |
| 详细类加载器统计 | `lingframe.test.classloader.stats` | `false` | 开启 `jmap -clstats` 基线、GC 前及最终统计；仅用于合并 PR 的完整审计 |

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

## 指标并发与诊断边界

引擎上下文清理只移除 Map 持有的条目，不清空 `TaskGroupContext` 内部字段。SeaTunnel 指标采集会浅拷贝这些条目，已取得引用的读者必须能够继续访问完整上下文，读者结束后对象由 GC 回收。

现有 REST 提交入口为每次提交、每个 Kafka source/sink 生成独立的 `kafka.config.client.id`，避免作业隔离类加载器中的默认计数器生成重复 JMX 名称。保留 topic、消费组及其他 Kafka 参数。当前内置矩阵使用单并行度；外置多并行度 Kafka sink 作业还需按 writer 隔离客户端名称，不能仅靠作业级配置推断已覆盖。

CI 在原有 E2E 步骤中检查两组容器日志：指标采集失败及 Kafka JMX 注册/注销异常会导致失败，并保留完整日志供定位。类加载器为零仅表示采样时无 SeaTunnel 作业类加载器残留；净增类的归属和长期元空间趋势仍需堆快照或更长时间的采样确认，A/B 元空间增长差值不代表 CPU 或延迟开销。

Hikari 固定连接池的 `idleTimeout` 警告来自 SeaTunnel JDBC 内部参数设置；延迟建表、初次获取 Kafka topic 元数据等启动警告按实际作业结果判断，未通过关闭日志掩盖。

## 0.3.1 修复验证记录

本次修复复用了上述 15 组矩阵，未增加独立复现流程。[修复代码的 CI 运行](https://github.com/LingFrame/LingFrame-Seatunnel-Agent/actions/runs/36617931333) 中，原生与 Agent 各 105 个作业全部 FINISHED，其中 MongoDB → Hive 各 7 次。Agent 最终 SeaTunnel 作业类加载器为 0，MAT 堆快照复核一致；原生组残留 567 个。Agent 最终元空间 90.41 MiB、相对启动增长 35.13 MiB；指标采集 NPE 和 Kafka JMX 异常均未再出现。

该记录是单轮修复验证，不是所有部署场景的保证：运行容器为 Java 8，治理运行时关闭，未校验 Hive 落地行数/内容；长期元空间平台期仍需更多采样。SeaTunnel 内部主键格式、Hikari 参数与 slot 收尾警告仍存在，不计作本次已修复项。
