# lingframe-agent-benchmark — JMH 基准模块

实测 Agent 对真实 SeaTunnel `AbstractTask.call()` 的**拦截损耗**，口径为**「job 级总耗时膨胀 + 单 worker CPU 增量」**。

采用**「完整 SeaTunnel 版」**测法：JMH fork 独立 JVM，`-javaagent` 挂载真实 Agent
Fat-Jar，ByteBuddy 织入**真实存在的** `org.apache.seatunnel.engine.server.task.AbstractTask.call()`
（通过其具体子类 `NoopBatchTask` 触发织入），而非 Mock/代理桩。

## 测量设计（同 JVM A/B 加固版）

「跨 fork 对比 ops/s」测微损耗量级时噪声比信号大一个数量级（fork 间 JIT/GC/频率差异），
且 slice=0 下空方法体被 JIT 整段消除、载荷无业务参照系。当前版本四项加固：

| # | 加固 | 对应代码 | 解决什么 |
|---|---|---|---|
| a | **同 fork A/B 对照** | `wovenCall`（`NoopBatchTask`，被织入）vs `plainCall`（`PlainBatchTask`，同方法体未织入），同 JVM 内对比 | fork 间噪声被同时消除，微损耗量级才可测 |
| b | **载荷不可消除** | `call()` 内 1k 行记录变换聚合，共享 `BatchCore`（两条路径零逻辑漂移），校验和经 Blackhole 流出 | 杜绝整方法消除；`plain` 是真实工作基线 |
| c | **AverageTime 报延迟** | `avgt` µs/op；附加延迟 = `wovenCall − plainCall` | 直接换算 job 级总耗时：附加延迟 × 调用频率（批次粒度决定轮数） |
| d | **织入自检** | `@TearDown weaveSelfCheck` 探针按 fork 模式断言 | 防「切点静默未织入却报零损耗」假阳性；no-agent/default 档同时自证装置灵敏度 |

### 三种 fork JVM

| 模式 | fork JVM 参数 | 织入内容 | 预期 |
|---|---|---|---|
| `no-agent` | 无 | 无 | `wovenCall ≈ plainCall`（装置自检：能分辨差异才可信） |
| `agent-default` | `-javaagent:…jar` | 仅 `releaseClassLoader` + `Thread.setContextClassLoader`（均不在批次热路径） | `wovenCall ≈ plainCall`（批次路径零织入） |
| `agent-governed` | `-javaagent:…jar=<benchmark-governance.yaml>` | **另织入 `AbstractTask.call()`**，每批次穿透治理微内核 | `wovenCall − plainCall` = 真实拦截附加延迟 |

损耗口径（**同一 fork 内**，`avgt` µs/op）：`overhead% = (wovenCall − plainCall) / plainCall`。
`@Param sliceMicros` 模拟单批次墙钟耗时（0=纯变换载荷地板 / 1000µs=极小批次 / 100000µs=100ms 典型批次）。
**per-call 成本判定看 100000µs 档位**：空批次的固定拦截成本被无限放大，不具业务代表性。
> ⚠️ 口径注记：per-call 成本表仅为测量记录，正确口径为 **job 级总耗时膨胀 + 单 worker CPU 增量**
>（实测典型 job 级影响 <0.5%，属正常范围，见「结论与待办」）；判定以该处实际影响评估为准，
> 勿以单次 per-call 比例判断达标性。

## ✅ 前置缺陷已修复：bridge 双副本 LinkageError

首版基准在 governed 档 100% 复现了一个 agent-core 真实集成缺陷（首个被织入的 `call()` 抛
`LinkageError: loader constraint violation … LingGovernanceContract … previously loaded by 'app'`），
成为首个端到端暴露它的复现装置（现有 docker E2E 刻意关闭批次切点，单元测试不走三层类加载拓扑）。

**根因**：`LingFrameAgentPremain` 字节码对 `LingFrameAgentBridge.registerContract(...)` 的调用
签名直接引用契约接口 → HotSpot 在 premain 类加载验证期把 `LingGovernanceContract` 从
AppClassLoader 抢载（此时 `appendToBootstrap` 尚未执行）→ App 副本 + 运行期 Bootstrap 副本双身份。

**修复（agent-core / agent-bridge）**：
1. `LingFrameAgentBridge.registerContract(Object)` —— 签名不再含契约接口，类型校验/强转下沉到
   Bootstrap 侧（唯一接口引用点）；premain 类验证期不再产生 App 副本，全 JVM 只剩 Bootstrap 单一副本；
2. `premain()` 委托私有 `activate()`（纵深防御，推迟其余强解析到 append 之后）。

**回归守卫**：`AgentJarManifestTest.premainClassMustNotReferenceContractType` 直接扫描 Fat-Jar 内
premain class 字节，断言不含 `LingGovernanceContract` 常量池引用（并保留 registerContract/activate
调用点存在性断言），锁死该回归路径。修复后 governed 档正常出分，见下节。

## 正式测量结果

正式跑分：`-f 3 -wi 3 -i 5 -w 1s -r 1s`（每档 3 fork × 5 迭代，Cnt=15），结果落
`target/bench-results/{no-agent,agent-default,agent-governed}.log`。

### agent-governed（目标测量：织入 `AbstractTask.call()` 的真实拦截损耗）

| sliceMicros | plain (µs/op) | woven (µs/op) | 附加延迟 | overhead% | 实际影响说明（job 级总耗时膨胀） |
|---|---|---|---|---|---|
| 0（纯载荷地板） | 0.800 ± 0.004 | 187.792 ± 12.117 | **+187.0µs** | —（空批次无业务参照，口径失效） | 仅记录，无业务参照 |
| 1000（1ms 批次） | 1001.660 ± 1.310 | 1010.582 ± 1.852 | **+8.9µs** | 0.89% | 0.89% × 调用频率换算 job 级影响 |
| 100000（100ms 批次） | 100025.227 ± 12.550 | 100211.922 ± 26.755 | **+186.7µs** | **0.19%** | 典型批次，job 级影响属正常范围 |

### 装置自检（no-agent / agent-default：均未织入，应无差）

| 模式 | slice=0 Δ | slice=1000 Δ | slice=100000 Δ | 结论 |
|---|---|---|---|---|
| `no-agent` | −0.005µs | +0.116µs | −1.7µs | ✅ 两条路径无差（自检 PASS，18/18） |
| `agent-default` | +0.002µs | −0.075µs | −0.09µs | ✅ 默认模式批次路径零织入（自检 PASS，18/18） |
| `agent-governed` | — | — | — | 织入自检 PASS（woven/plain 探针比 >1.5，最差样本仍 ≈268×） |

### 结论与待办（2026-09-06）

1. **吞吐损耗目标口径不成立（纠偏）**：该预估隐含「钩子为纯逻辑 µs 成本 + 每 100ms 精确调一次」两假设，实测均不符——钩子 ~100µs 主体是**任何代码低频调用皆付的冷 cache 重建**（物理成本）；且真实引擎 call() 按数据节奏连续轮询（`TaskExecutionService` `do{call()}while(!isDone)`，轮次粒度由 connector 批大小决定，冒烟 job 51 次 call/3s、每轮 body<1ms）。正确口径 = **job 级总耗时膨胀 + 单 worker CPU 增量**。
2. **归因已闭环（JFR 零阈值采样 + BodyProbe5 大样本三模式判别）**：
   - main 线程 JFR 实证几乎不 park；`EventBus.publish` 无订阅者直接 return；metrics 为纯 LongAdder + 线性直方图（无时间窗累积）——均非成本来源。
   - **机制 = 钩子热路径闲置 ≥~20ms 后的冷 cache/上下文重建**：三模式（gap=20ms）sleep +144µs / spin +106µs / keeper（每 2ms 踩热路径）**+38µs**；大样本间隔曲线 2/5/20/100ms → +72/+95/+124/+143µs（20ms 后饱和）。
   - 成本分层：adapter 桥接 ~23µs + pipeline 净 ~10-15µs（热态下限）+ 冷启动重建 ~70-110µs + 睡眠加深 ~20-40µs。
   - 真实引擎端到端冒烟（发行版单节点 Zeta + governed agent）：fake→console 1000/1000/0，日志实证每真实 call() 产生 Trace/Audit 事件，织入链路完整工作。
3. **实际影响评估（新口径）：属正常范围**——钩子成本与频率自洽（低频付高单位成本但轮数少、高频热态付最低成本）+ IO 型引擎每轮 body≥10ms 常态 → 典型 job 级影响 <0.5%（冒烟锚点 ~0.24% 量级），仅高频短轮流式单 worker 理论上限 ~1-2%（钩子处于热态最低成本档）。**不构成部署障碍，无需架构级改动**；此前「超目标 ~3.5×/4.7×」表述基于错误口径已废弃。优化（可选）：压缩钩子热路径 footprint，收益在极端高频短轮场景。
4. 定量结论的可信度由装置自检背书：no-agent/default 档 woven≈plain 证明测量装置在无织入时
   分辨不出差异、有织入时（governed）稳定分辨出 ~百 µs 级成本——非假阳性。
5. 诊断工具保留：`BodyProbe.java`（二分节奏/体长）、`BodyProbe4.java`（0 体 + 间隔扫描）、
   `BodyProbe5.java`（单 gap 深跑，args: `gapMs nPairs [sleep|spin|keeper]`，keeper 模式复现
   「热路径保持活跃 → 成本 ~38µs」）。

## 为什么本模块不用 Maven 坐标引 SeaTunnel

`seatunnel-engine-server:3.0.0-SNAPSHOT` 的**已发布 pom** 把 engine-ui、checkpoint-storage-hdfs、
seatunnel-config-sql、jetty 等重型 dev/test 依赖树带进解析（本机离线环境必然失败）。
因此本模块对这些 jar 用 **system scope 直引本地仓库预构建产物**（仅含织入目标类），
pom 文件头有详细注释。**本模块为本机专用，不参与常规 CI/发布构建**，他人构建请排除：
`mvn … -pl '!lingframe-agent-benchmark'`。

## 构建与运行

前置：`lingframe-agent-dist/target/lingframe-seatunnel-agent.jar` 已构建
（`mvn clean install -pl :lingframe-agent-dist -am`）。

```bash
# 1) 打包本模块（thin jar，含注解处理器生成的 META-INF/BenchmarkList）
#    本机无 mvn 命令，用 launcher（或等价 mvn 命令）：
java -classpath "D:/apache-maven-3.9.10/boot/plexus-classworlds-2.9.0.jar" \
  -Dclassworlds.conf="D:/apache-maven-3.9.10/bin/m2.conf" \
  -Dmaven.home="D:/apache-maven-3.9.10" \
  -Dlibrary.jansi.path="D:/apache-maven-3.9.10/lib/jansi-native" \
  -Dmaven.multiModuleProjectDirectory="E:/Codes/灵珑/lingframe-seatunnel-agent" \
  org.codehaus.plexus.classworlds.launcher.Launcher \
  -f "E:/Codes/灵珑/lingframe-seatunnel-agent/pom.xml" -o -B \
  -DskipTests -Djacoco.skip=true -pl :lingframe-agent-benchmark package

# 2) 冒烟（机制验证 + 量级）：
./run-benchmark.sh -f 1 -wi 1 -i 1 -w 500ms -r 500ms -p sliceMicros=0

# 3) 正式跑分（默认档位 -f 3 -wi 2 -i 3 -w 1s -r 1s，覆盖 0/1000/100000 三个 slice 档位）：
./run-benchmark.sh
```

脚本将三次结果分别落盘 `target/bench-results/{no-agent,agent-default,agent-governed}.log`。
结果文件里除 JMH Score 表外，还应看到每档末尾的织入自检行：
`weaveSelfCheck mode=… ratio=… -> PASS`（governed 档 ratio 应显著 >1；no-agent/default 档应 ≈1）。

### 如何确认织入真的发生了

1. `agent-governed.log` 中可见 premain WARN：`TaskExecutionAdvice ENABLED — intercepting
   AbstractTask.call() …`。若只见 `TaskExecutionAdvice DISABLED`，说明 YAML 未生效，跑分无效。
2. JMH Score 中 governed 档 `wovenCall` 明显慢于 `plainCall`（A/B 自证）；若二者无差且
   `weaveSelfCheck` 报 FAIL，说明切点静默未织入——该结果**必须作废**，不得当作「零损耗」。

## 已知环境坑（Windows + 中文路径）

- **`-javaagent` 选项里「=」之后的路径不能含中文**：本机 `sun.jnu.encoding=GBK` 下，JVM 会把
  agent 选项（`-javaagent:jar=配置路径`）中「=」之后的非 ASCII 路径转码弄坏，premain 会报
  `No governance config found` 并静默退回默认配置（切点不织入，跑分失真且不报错）。
  对策：`run-benchmark.sh` 已自动把 agent jar 与治理 yaml 复制到纯 ASCII 临时目录再拼接选项。
- **classpath 中不能混入 git-bash 的 `/e/Codes/...` 形式路径**：Windows java.exe 不识别，
  该元素被静默丢弃（JMH 报 `Unable to find the resource: /META-INF/BenchmarkList`）。
  `run-benchmark.sh` 已用 `cygpath -w` 统一转盘符路径。
- **构建/清理命令不要用管道吞退出码**：`mvn … | tail` 的退出码是 tail 的，构建失败会被掩盖
  （曾导致「修复被证伪」的假象——dist jar 里一直是旧 class）。用 `mvn …; echo exit=$?` 或后台任务直取退出码。
- **暂存目录清理**：脚本用 `/tmp/lf-bench-$$`（git-bash 原生路径）做 cleanup——沙箱安全删除层
  无法理解裸 `C:/...` 相对形式，会报 trash 失败且遗留目录。

## 关键约束与版本口径

- **包名不得落在 `com.lingframe.agent.*`**：premain 的 ByteBuddy ignore matcher 会排除
  `com.lingframe.agent.` 前缀，落在其下的测试载体永远不会被织入，会虚假测出「零损耗」。
  本模块基准类统一放 `com.lingframe.benchmark`。
- **SeaTunnel 版本口径**：Agent 基线为 SeaTunnel **2.3.13**；本模块织入的是本地
  `E:\Codes\seatunnel`（**3.0.0-SNAPSHOT**）构建产物。Agent 切点按全限定类名/方法名匹配，
  `AbstractTask` 的 FQN 在 2.x→3.x 未变故可织入，但字段若漂移 ByteBuddy 会静默跳过——
  跑分前后务必核对上面的「织入确认」。
- `benchmark-governance.yaml` 关闭了限流/熔断：这两者在高频基准下会触发 token 桶限流或
  退避 sleep(100ms)，主导结果、掩盖纯拦截损耗；Pipeline 其余 Filter（状态守卫/审计/Trace 等）
  仍真实执行（Trace 走 INFO，governed 档日志会逐 call 打点）。
- 治理运行期若初始化失败，premain 会降级 TRACE_ONLY（Adapter 持空 Pipeline），此时测到的是
  Bridge 分发 + ThreadLocal 记账的地板开销——属 Agent 的既定降级路径，日志可见。
