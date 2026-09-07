# lingframe-agent-dist — Fat-Jar 打包

Maven Shade 打包模块，产出可挂载到 SeaTunnel JVM 的单一 `-javaagent` Fat-Jar。

## 职责

- 把 `lingframe-agent-core` + `lingframe-agent-bridge` + ByteBuddy + SnakeYAML 合并为单一 jar
- ByteBuddy / SnakeYAML **Relocate** 包名（`net.bytebuddy` → `com.lingframe.agent.shaded.bytebuddy`），避免与 SeaTunnel 上游版本冲突
- Manifest 打入 `Premain-Class` / `Agent-Class` / `Can-Redefine` / `Can-Retransform` 标志
- `lingframe-agent-bridge` 类**保留原包名**（不 relocate）——`LingFrameAgentPremain.extractBridgeJar` 依赖原路径提取注入 Bootstrap

## 产物

`lingframe-agent-dist/target/lingframe-seatunnel-agent.jar`（~5.8MB，含 relocated ByteBuddy）。

## 构建

```bash
mvn -o -pl :lingframe-agent-core -am install    # 先装 core 到本地仓库（dist 从仓库解析）
rm -rf lingframe-agent-dist/target              # 清旧产物（shade 与自身旧 jar 类重叠会胜出旧类！）
mvn -o -pl :lingframe-agent-dist package
```

⚠️ **构建坑**：dist 依赖从**本地仓库**解析 core，改 core 后必须先 `install` core；且 shade 会与自身上次产出的旧 `lingframe-seatunnel-agent.jar` 类重叠（旧类优先胜出），改 core 后必须 `rm -rf target` 再 package。

## 守卫测试

`AgentJarManifestTest`（e2e 模块）：扫描 Fat-Jar 字节码，断言 Manifest 标志、bridge 类已打入、ByteBuddy/SnakeYAML 已 Relocate、premain 字节码不含 `LingGovernanceContract` 常量池引用（防 bridge 双副本回归）。本地 `mvn -pl :lingframe-agent-e2e test` 可跑。

## 部署

```bash
java -javaagent:/path/to/lingframe-seatunnel-agent.jar=/path/to/lingframe-governance.yaml \
     -cp "lib/*;starter.jar" org.apache.seatunnel.core.starter.seatunnel.SeaTunnelServer
```
javaagent 参数路径必须 ASCII（含中文路径会触发 JVM 内部转码破坏「=」分隔）。
