# lingframe-agent-bridge — Bootstrap 桥接契约工程

极薄的桥接契约层，加载于 **Bootstrap ClassLoader**，供 ByteBuddy 织入后的 SeaTunnel 代码零反射直接分发到 Agent 治理逻辑。

## 职责

- 定义 `LingGovernanceContract` 契约接口（`beforeTaskCall` / `afterTaskCall` / `isGovernanceEnabled` / `onPhysicalRelease` / `convertJarsToKey`）
- 提供 `LingFrameAgentBridge` 静态入口（`volatile` 契约指针，单次分发 <0.5ns）
- `registerContract(Object)` 签名刻意不用契约接口类型——避免 HotSpot 在 premain 类加载验证期从 AppClassLoader 抢载契约接口形成双副本（曾导致 `LinkageError`，见 CHANGELOG）

## 关键约束

- **零第三方依赖**（<20KB），仅 JDK 标准库
- 不可引用 `lingframe-agent-core` 或任何 App 层类（否则打破 Bootstrap/App 隔离）
- `LingFrameAgentPremain` 通过 `appendToBootstrapClassLoaderSearch` 把本 jar 注入 Bootstrap

## 构建

```bash
mvn -o -pl :lingframe-agent-bridge install
```

## 测试

`LingFrameAgentBridgeTest`（12 个 @Test）：契约注册/清空、分发 no-op 降级、`registerContract` 类型校验、签名 `Object` 防双副本回归。

## 在三层拓扑中的位置

```
Bootstrap ClassLoader  ←  本模块（bridge 契约 + Bridge 静态入口）
        ↑ 双亲委派
App ClassLoader        ←  lingframe-agent-core（premain + 适配器 + Advice）
        ↑
宿主 App              ←  SeaTunnel（被织入目标）
```
