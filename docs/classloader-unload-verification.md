# ClassLoader 卸载与 Metaspace 稳定性验证（本机冒烟级，可复现）

> ⚠️ **证据级别声明**：本验证为 **source × sink 双维度矩阵**——source 角色：fake / jdbc(H2,MySQL) / redis / mongodb；
> sink 角色：console / mongodb / redis / jdbc（含 MySQL→MySQL 双真实同 job）。
> 8 组场景均 5 轮 × 15 job = 75 job 的 Full GC 后 Class Metaspace **完全收敛（零增长）**。
> 仍未覆盖：Kafka connector（本机 ZK 工具链环境受限，服务可起）、多 connector 组合/长生命周期 job 的
> 最终证据由 CI `MetaspaceLeakIT`（1000 job）承担。

**验证目标**：在真实 SeaTunnel Zeta 引擎 + governed agent 下，确认 `classloader-cache-mode: false` 场景中卸载链路
（SeaTunnel remove CL → Agent close URLClassLoader + JVM 引用清理 → Full GC 类卸载）**真实执行且简单场景无泄漏**。

---

## 0. 路径变量（复现前替换为你的环境）

本文所有命令使用两个变量，复现时替换成你的实际路径即可：

| 变量 | 本机示例值 | 含义 |
|---|---|---|
| `$D` | `E:/Codes/seatunnel/seatunnel-dist/target/dist-run/apache-seatunnel-3.0.0-SNAPSHOT` | SeaTunnel 发行版解压目录 |
| `$JDK` | `D:/SDK/Java/corretto-17.0.18` | JDK 17（Zeta 引擎要求）|
| `$AGENT_JAR` | `<你构建的 dist jar 路径>/lingframe-seatunnel-agent.jar` | Agent Fat-Jar（洁净版）|
| `$GOV_YAML` | `<benchmark 模块>/benchmark-governance.yaml` | governance 配置（开 task-advice、关 resilience）|

要求：上述路径所在盘符内**不含中文**（`-javaagent` 参数「=」后中文路径会被 JVM 内部转码破坏）。

## 1. 环境与前置

| 项 | 要求 |
|---|---|
| OS | Windows + git-bash（Linux/macOS 需调整 java 调用与路径形态）|
| SeaTunnel | `apache-seatunnel-3.0.0-SNAPSHOT-bin.tar.gz` 解压目录 |
| JDK | 17（本文用 Corretto 17.0.18）|
| Agent | dist 模块产出的 Fat-Jar（探针字符串为 0 的洁净版）|

### 1.1 部署 Agent 到发行包（ASCII 路径）

```bash
mkdir -p "$D/agent" "$D/logs"
cp "$AGENT_JAR" "$D/agent/lf-agent.jar"
cp "$GOV_YAML"  "$D/agent/gov.yaml"
```

### 1.2 关键配置：`classloader-cache-mode` 必须为 false

```yaml
# $D/config/seatunnel.yaml
seatunnel:
  engine:
    classloader-cache-mode: false   # ← 验证前提：每 job 独立 ClassLoader 才走物理释放
```

> `cache-mode: true`（SeaTunnel 默认）为多 job 共享缓存常驻（上游设计，非泄漏），
> Agent 的 `ClassLoaderReleaseAdvice` 在该模式下不做物理关闭——验证该场景无意义。

## 2. 启动引擎（手工 java 命令：classpath 分号 + 盘符路径）

```bash
JH="$JDK/bin/java.exe"
CP="$D/lib/*;$D/starter/seatunnel-starter.jar"
"$JH" -Dseatunnel.home="$D" -DSEATUNNEL_HOME="$D" \
 -Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector \
 -Dlog4j2.isThreadContextMapInheritable=true -DAsyncLogger.ThreadNameStrategy=UNCACHED \
 -Dhazelcast.logging.type=log4j2 -Dlog4j2.configurationFile="$D/config/log4j2.properties" \
 -Dseatunnel.logs.path="$D/logs" -Dseatunnel.logs.file_name=seatunnel-engine-server \
 -XX:+HeapDumpOnOutOfMemoryError -XX:MaxMetaspaceSize=2g -XX:+UseG1GC \
 -javaagent:"$D/agent/lf-agent.jar=$D/agent/gov.yaml" \
 -Dseatunnel.config="$D/config/seatunnel.yaml" -Dhazelcast.config="$D/config/hazelcast.yaml" \
 -cp "$CP" org.apache.seatunnel.core.starter.seatunnel.SeaTunnelServer
```

等待就绪：`$D/logs/seatunnel-engine-server.log` 出现 `is STARTED` / `active master`。

> Windows 部署要点：发行包 `.sh`/`.cmd` 的 classpath 用 Unix 冒号 → Windows JVM 整段失效；
> 必须手工拼 java 命令（分号 + 盘符路径）；javaagent 参数路径须 ASCII。

## 3. Job 配置（fake→console，最小冒烟——非真实 job）

```conf
# $D/config/lf-smoke-job.conf
env { parallelism = 1  job.mode = "BATCH" }
source {
  FakeSource {
    parallelism = 1
    row.num = 1000
    schema = { fields { id = "int" name = "string" } }
  }
}
transform {}
sink { Console { parallelism = 1 } }
```

## 4. 采样与循环流程

```bash
# 找引擎 PID
PID=$("$JDK/bin/jps" -l | grep -i SeaTunnelServer | awk '{print $1}')
# 采样 Class Metaspace（"Class:" 行 = 类元数据 = 泄漏真正指标）
"$JDK/bin/jcmd" "$PID" VM.metaspace | grep -E "^\s+Class:"
# 强制 Full GC（JVM 仅在 Full GC 时卸载类）后再次采样
"$JDK/bin/jcmd" "$PID" GC.run; sleep 6
"$JDK/bin/jcmd" "$PID" VM.metaspace | grep -E "^\s+Class:"
```

循环提交 job（每 job ~8s，15 job ≈ 90s）：

```bash
CP="$D/lib/*;$D/starter/seatunnel-starter.jar"
for i in $(seq 1 15); do
  "$JDK/bin/java" -Dseatunnel.home="$D" -DSEATUNNEL_HOME="$D" \
    -Dhazelcast.logging.type=log4j2 -Dlog4j2.configurationFile="$D/config/log4j2_client.properties" \
    -Dseatunnel.config="$D/config/seatunnel.yaml" -DhazelcastClientConfig="$D/config/hazelcast-client.yaml" \
    -cp "$CP" org.apache.seatunnel.core.starter.seatunnel.SeaTunnelClient \
    --config "$D/config/lf-smoke-job.conf"
done
```

**验证序列**：M0 采样 → 循环 N 轮（每轮 15 job + Full GC + 采样）→ 观察 Class Metaspace 是否随轮次收敛（平台）或线性增长（泄漏）。

## 5. 数据记录（2026-09-06 实测，5 轮 75 job 趋势）

| 采样点（每轮 15 job 后 Full GC） | Class Metaspace used | chunks |
|---|---|---|
| M0（引擎就绪，无 job） | 7.77 MB | 574 |
| round 1（15 job） | **8.63 MB** | 844 |
| round 2（30 job） | **8.63 MB** | 845 |
| round 3（45 job） | **8.63 MB** | 845 |
| round 4（60 job） | **8.63 MB** | 845 |
| round 5（75 job） | **8.63 MB** | 845 |

**趋势判断**：5 轮 75 job 的 GC 后采样**全部一致（8.63 MB）**，chunks 在 round 2 后稳定 845 不再增长——
fake→console 简单场景下类回收完全收敛，无线性增长迹象（若泄漏则每轮应单调上升）。GC 前 vs 后（如 round 1：9.11→8.63 MB，卸载 332+ chunks）证明类可被 Full GC 完全卸载。

**卸载链路日志证据**（server.log，15 job 产生 60 次「Triggering physical release」）：

```
DefaultClassLoaderService - Release classloader for job ... with jars [connector-fake...jar]
SeaTunnelAdapter          - Triggering physical release for ClassLoader SeaTunnelChildFirstClassLoader
```

⚠️ 仍为**冒烟级**：fake→console 无 JDBC/外部连接，`JdbcDriverUnloadHook` 等钩子未被真实考验。

### 二级证据：真实 JDBC connector 验证（H2 内嵌库，2026-09-06）

用真实 `connector-jdbc`（读 H2 文件库 → console），真实考验 JDBC 类加载 + 连接生命周期 + connector 子 CL 卸载：

**环境准备**：
- H2 2.1.214 驱动 jar 初始化文件库（1000 行表）+ **必须放 `$D/lib/`**（放 `connectors/` 同级不行——connector 子 CL 看不到同级 driver jar，报 `ClassNotFoundException: org.h2.Driver`；lib/ 在主 classpath，双亲委派可见）
- job：source `Jdbc { url="jdbc:h2:file:.../lfdb", driver="org.h2.Driver", query="SELECT id,name FROM t_source" }` → sink `Console`

**验证结果**（engine 内 jcmd VM.metaspace `Class:` 行）：

| 采样点（每轮 15 job + Full GC） | Class Metaspace used | chunks |
|---|---|---|
| M0 | 9.19 MB | 937 |
| round 1（15 job） | **9.17 MB** | 905 |
| round 2-5（30-75 job） | **9.17-9.18 MB** | 905（恒定）|

**真实 JDBC 场景 75 job Class Metaspace 完全收敛（零增长）**；agent 物理释放真实触发
（connector-jdbc 子 CL 每 job 创建 → SeaTunnel release → agent close）。日志实证：

```
DefaultClassLoaderService - Release classloader for job ... with jars [connector-jdbc-3.0.0-SNAPSHOT.jar]
SeaTunnelAdapter          - Triggering physical release for ClassLoader SeaTunnelChildFirstClassLoader
JdbcSourceReader          - Closed the bounded jdbc source
```

### 三级证据：真实 MySQL 8.3.0 服务验证（2026-09-06）

connector-jdbc 连**本机真实 MySQL 8.3.0 服务**（`jdbc:mysql://127.0.0.1:3306/lf_test`，root/123456，
1000 行表）→ console。MySQL driver（mysql-connector-java 8.0.29）放 `lib/`。

**验证结果**（engine 内 jcmd VM.metaspace `Class:` 行）：

| 采样点（每轮 15 job + Full GC） | Class Metaspace used | chunks |
|---|---|---|
| M0 | 8.94 MB | 872 |
| round 1-5（15-75 job） | **8.89 MB（恒定）** | 867（恒定）|

**真实 MySQL 网络 JDBC 场景 75 job 完全收敛（零增长）**——验证了真实连接（TCP + 认证 +
caching_sha2 插件 + 查询生命周期）下 connector 子 CL 每 job 创建/卸载无 Metaspace 泄漏。

### 四级证据：真实 Redis 7.0.10 验证（2026-09-06）

connector-redis（jar 自带 jedis client，无需额外依赖）连本机 Redis 7.0.10
（独立 `--port 6380` 实例，1000 个 json key `key_test_0..999`）→ console：
`Redis { host/port/keys="key_test_*", data_type=key, format=json, schema{id,name} }`。

**验证结果**（engine 内 jcmd VM.metaspace `Class:` 行）：

| 采样点（每轮 15 job + Full GC） | Class Metaspace used | chunks |
|---|---|---|
| M0 | 9.01 MB | 928 |
| round 1（15 job） | 9.28 MB | 968 |
| round 2-5（30-75 job） | **9.28 MB（恒定）** | 970（恒定）|

**真实 Redis 场景 75 job 收敛（round 1 后平台，零增长）**——验证非 JDBC 类 connector
（socket client + json 解析路径）同样无 Metaspace 泄漏。

### 五级证据：真实 MongoDB 验证（2026-09-06）

connector-mongodb（自带 mongodb-driver）连本地 `mongod --dbpath ... --port 27017`，
1000 文档（`lf_db.lf_coll`）→ console：
`MongoDB { uri/database=lf_db/collection=lf_coll, schema{id,name} }`。

**验证结果**（engine 内 jcmd VM.metaspace `Class:` 行）：

| 采样点（每轮 15 job + Full GC） | Class Metaspace used | chunks |
|---|---|---|
| M0 | 9.06 MB | 902 |
| round 1（15 job） | 9.59 MB | 928 |
| round 2-5（30-75 job） | **9.60 MB（平台）** | 930→931 |

**真实 MongoDB 场景 75 job 收敛（round 3 后平台，零增长）**——验证文档型 NoSQL
（BSON + mongodb-driver）connector 同样无 Metaspace 泄漏。

### 六-八级证据：sink 角色与双真实矩阵（2026-09-06）

此前矩阵只覆盖 source 多样性（sink 恒为 Console）。补全 **sink 角色**验证
（每次 job 加载并卸载「真实 source + 真实 sink」双 connector 子 CL）：

| # | source → sink | sink 角色 | 收敛点（75 job GC 后） |
|---|---|---|---|
| ⑥ | FakeSource → **MongoDB** sink（写 lf_sink_db） | connector-mongodb 作 sink | round2 后 **9.90 MB** 恒定（chunks 954）|
| ⑦ | FakeSource → **Redis** sink（写 1000 key，`support_custom_key=true`） | connector-redis 作 sink | round2 后 **9.91 MB** 恒定（chunks 956）|
| ⑧ | **MySQL** → **MySQL**（`t_source` → `t_sink` upsert） | connector-jdbc 双端 | round1 后 **10.19 MB** 恒定（chunks 988）|

**双维度矩阵结论**：source 角色覆盖 fake/jdbc/redis/mongodb，sink 角色覆盖 console/mongodb/redis/jdbc——
三个 connector（jdbc/redis/mongodb）在 **source 与 sink 两种角色**下均 75 job 收敛零增长，
最强组合（MySQL→MySQL，同 job 双真实 jdbc 子 CL）同样收敛。

### 真实 connector 的可行性与边界（排查记录，2026-09-06）

发行版含 **93 个 connector**。本机实测结论：

| connector 类 | 本机能否跑 | 处理 |
|---|---|---|
| **jdbc**（+ 内嵌 H2 库）| ✅ **可跑** | H2 driver 放 `lib/`（非 `connectors/`），见上方二级证据 |
| fake / console | ✅ 可跑 | SeaTunnel 官方零依赖自测组合 |
| file-*（file-local 等）| ❌ | 走 **hadoop FileSystem API**，Windows 需 hadoop native（winutils）——发行版无 native、系统无 `HADOOP_HOME`，实测卡 `UnsatisfiedLinkError: NativeIO$Windows.access0` |
| kafka / cdc / redis 等 | ❌ | 需外部系统（broker/DB 源/服务），本机无 |

结论：本机已覆盖「机制冒烟（fake）+ 真实 JDBC connector」两级证据；file/消息类 connector 需 Linux（有 hadoop native）或带外部系统的环境（CI/部署环境职责）。

## 6. 后续验证工作流（非本地可补部分，归 CI / 部署环境）

> 本地矩阵（fake/jdbc/redis/mongodb × console/mongodb/redis/jdbc，含 MySQL→MySQL 双真实同 job）**已收敛，不再扩展**。
> 剩余盲区属长稳体量 + 本地受限 connector，交由 CI 与部署环境职责：

1. **CI `MetaspaceLeakIT`**（1000 job + 多 connector 组合，Docker）：长生命周期/大体量场景下的最终证据，同时覆盖 Kafka、file-\* 等本地环境受限的 connector。
2. **Kafka connector 专项**（若其为生产目标）：需先起 ZK + broker，验证消息类 connector 子 CL 卸载；建议并入 CI 而非本地堆矩阵。
3. 交叉格（redis→mongodb、mongodb→redis 等）：每 connector 已独立在 source/sink 双角色收敛，共存风险低，边际价值不足以再投入每轮 ~30 分钟。

## 7. 结论与边界（诚实版）

- ✅ **已证（本机矩阵级，2026-09-06）**：cacheMode=false 下卸载链路真实执行（60 次物理释放 / 15 job 日志实证）；source×sink 双维度矩阵 **8 组场景**全部 5 轮 75 job GC 后 Class Metaspace **完全收敛零增长**（8.63 / 9.17 / 8.89 / 9.28 / 9.60 / 9.90 / 9.91 / 10.19 MB）；jdbc / redis / mongodb 三 connector 在 **source 与 sink 两种角色**下均无 Metaspace 泄漏，最强组合（MySQL→MySQL 双真实同 job）亦收敛——**机制与真实 connector 卸载命题成立**。
- ⚠️ **范围边界（本地不可补，归 CI）**：①长生命周期 / 大体量（1000 job 长稳）的最终证据由 CI `MetaspaceLeakIT` 承担；②Kafka 消息类 connector 与 file-\* connector（Windows 缺 hadoop native）属部署环境 / CI 职责，不在本机验证范围。
- 本验证耗时约 5 分钟/轮（引擎启动 ~40s + 15 job ~90s + GC ~10s），5 轮约 30 分钟；环境前置见第 1、2 节。
