#!/usr/bin/env bash
# =============================================================================
# JMH「完整 SeaTunnel 版」基准运行脚本（本机专用）
#
# 三次独立 fork JVM 跑同一份 AbstractTaskCallBenchmark，逐一对比吞吐：
#   1) no-agent      纯基线（不挂 javaagent）
#   2) agent-default 挂 Agent 默认模式（仅 ClassLoader 清理 + TCCL 防御，不织入 call()）
#   3) agent-governed挂 Agent + benchmark-governance.yaml（织入真实 AbstractTask.call()，
#               含 M2 作业级治理 per-job 路径：多作业轮转测作业级注册表热路径）
#
# 损耗口径：同 fork A/B 附加延迟（µs/op），以 job 级总耗时膨胀衡量（大 sliceMicros 档位反映典型批次）。
# per-job 附加损耗口径（governed fork 内同 JVM A/B）：wovenPerJobCall - plainCall。
#
# 用法：
#   ./run-benchmark.sh                          # 默认跑分档位（-f 3 -wi 2 -i 3）
#   ./run-benchmark.sh -f 1 -wi 1 -i 1 -p sliceMicros=1000   # 冒烟验证（机制 + 粗略量级）
#
# 前置：先 `mvn package -pl :lingframe-agent-benchmark` 且 lingframe-agent-dist/target 下
#       已构建 lingframe-seatunnel-agent.jar（AgentJarManifestTest 前置产物）。
# 仅适用于本机（类路径含 D:/MavenRepository 与 E:/Codes/灵珑 绝对路径），详见模块 README。
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# git-bash 的 pwd 返回 /e/Codes/... 形式，Windows java.exe 无法识别；
# 必须转成带盘符的 Windows 路径（否则 classpath 元素被静默丢弃，JMH 报找不到 BenchmarkList）。
if command -v cygpath >/dev/null 2>&1; then
  ROOT="$(cygpath -w "$ROOT")"
fi
REPO="${MVN_REPO:-D:/MavenRepository}"
DIST_SRC="$ROOT/../lingframe-agent-dist/target/lingframe-seatunnel-agent.jar"
YAML_SRC="$ROOT/benchmark-governance.yaml"
BENCH_JAR="$ROOT/target/lingframe-agent-benchmark-0.1.0.jar"
OUT_DIR="$ROOT/target/bench-results"

# Windows 类路径分隔符（git-bash 下 java 是 Windows 二进制，用 ';'）
CP_SEP=';'

# =============================================================================
# 关键坑：Windows + GBK(jnu) 下，-javaagent 选项里「=」之后的路径若含中文会被 JVM
# 内部转码弄坏（AgentConfig 报 No governance config found，切点静默不织入）。
# 对策：把 agent jar 与治理 yaml 都复制到【纯 ASCII】临时目录再拼 -javaagent，
# 保证整条选项 ASCII。
# =============================================================================
# 双形态暂存路径：JVM/java 需要 Windows 盘符形式（DIST/YAML），
# 而清理删除用 git-bash 原生 /tmp 绝对路径——沙箱/安全删除层把裸 "C:/..." 当相对
# 路径拼接会删错地方并报 trash 失败；/tmp 前缀由工具链原生识别，删除才可靠。
STAGE_WIN="$(cygpath -w /tmp)/lf-bench-$$"
STAGE_UNIX="/tmp/lf-bench-$$"
mkdir -p "$STAGE_WIN"
DIST="$STAGE_WIN/lingframe-seatunnel-agent.jar"
YAML="$STAGE_WIN/benchmark-governance.yaml"
cp "$DIST_SRC" "$DIST"
cp "$YAML_SRC" "$YAML"
cleanup() { rm -rf "$STAGE_UNIX" 2>/dev/null || true; }
trap cleanup EXIT

for f in "$BENCH_JAR" "$DIST_SRC" "$YAML_SRC"; do
  if [ ! -f "$f" ]; then
    echo "[ERROR] 缺少文件: $f（先构建 benchmark 模块与 agent dist jar）" >&2
    exit 1
  fi
done

S="$REPO/org/apache/seatunnel"
JMH="$REPO/org/openjdk/jmh/jmh-core/1.37/jmh-core-1.37.jar"
JOPT="$REPO/net/sf/jopt-simple/jopt-simple/5.0.4/jopt-simple-5.0.4.jar"
MATH="$REPO/org/apache/commons/commons-math3/3.1.1/commons-math3-3.1.1.jar"
SLF4J_SIMPLE="$REPO/org/slf4j/slf4j-simple/1.7.36/slf4j-simple-1.7.36.jar"

CP="$BENCH_JAR$CP_SEP$JMH$CP_SEP$JOPT$CP_SEP$MATH$CP_SEP$SLF4J_SIMPLE"
CP="$CP$CP_SEP$S/seatunnel-engine-server/3.0.0-SNAPSHOT/seatunnel-engine-server-3.0.0-SNAPSHOT.jar"
CP="$CP$CP_SEP$S/seatunnel-engine-core/3.0.0-SNAPSHOT/seatunnel-engine-core-3.0.0-SNAPSHOT.jar"
CP="$CP$CP_SEP$S/seatunnel-engine-common/3.0.0-SNAPSHOT/seatunnel-engine-common-3.0.0-SNAPSHOT.jar"
CP="$CP$CP_SEP$S/seatunnel-api/3.0.0-SNAPSHOT/seatunnel-api-3.0.0-SNAPSHOT.jar"
CP="$CP$CP_SEP$S/seatunnel-common/3.0.0-SNAPSHOT/seatunnel-common-3.0.0-SNAPSHOT.jar"
CP="$CP$CP_SEP$S/seatunnel-shade-hazelcast/5.1-3.0.0/seatunnel-shade-hazelcast-5.1-3.0.0.jar"

JAVA_BIN="${JAVA_HOME:+$JAVA_HOME/bin/}java"
mkdir -p "$OUT_DIR"

# 未传 JMH 参数时使用默认跑分档位
if [ "$#" -eq 0 ]; then
  JMH_ARGS=(-f 3 -wi 2 -i 3 -w 1s -r 1s)
else
  JMH_ARGS=("$@")
fi

echo "==> 1/3 no-agent（纯基线）"
"$JAVA_BIN" -cp "$CP" org.openjdk.jmh.Main AbstractTaskCallBenchmark "${JMH_ARGS[@]}" 2>&1 | tee "$OUT_DIR/no-agent.log"

echo "==> 2/3 agent-default（挂 Agent，默认模式：不织入 call()）"
"$JAVA_BIN" -cp "$CP" org.openjdk.jmh.Main AbstractTaskCallBenchmark -jvmArgsAppend "-javaagent:$DIST" "${JMH_ARGS[@]}" 2>&1 | tee "$OUT_DIR/agent-default.log"

echo "==> 3/3 agent-governed（挂 Agent + 全治理：织入 AbstractTask.call()）"
"$JAVA_BIN" -cp "$CP" org.openjdk.jmh.Main AbstractTaskCallBenchmark -jvmArgsAppend "-javaagent:$DIST=$YAML" "${JMH_ARGS[@]}" 2>&1 | tee "$OUT_DIR/agent-governed.log"

echo
echo "结果已落盘: $OUT_DIR/{no-agent,agent-default,agent-governed}.log"
echo "损耗口径（同 fork A/B，avgt us/op）："
echo "  agent-governed 内 wovenCall - plainCall = 单次 call 附加延迟（拦截真实损耗）；"
echo "  agent-governed 内 wovenPerJobCall - plainCall = M2 per-job（多作业轮转）附加延迟；"
echo "  附加延迟 = woven* - plainCall（µs/op），job 级总耗时影响按调用频率与批次粒度换算。"
echo "  no-agent / agent-default 内 wovenCall 与 plainCall 应无差（测量装置自检，见 weaveSelfCheck）。"
