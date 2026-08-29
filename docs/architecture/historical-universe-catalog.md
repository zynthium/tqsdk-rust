# 历史 Universe Catalog 与填充合同

本文定义历史数据下载和动态回测使用的 catalog、证明与 artifact 边界。当前/live
`UniverseExpression` 的语法和语义不变；历史入口使用独立的
`HistoricalFillUniverseSpec`。

## 两条能力通道

- `physical:all` 表示 provider 在本次稳定快照中返回的全部物理期货合约。它的证明是
  `provider_current_observed`：采集必须记录查询前后 roster、完整 metadata、观察时间和
  source identity。它可以证明“本次看见了什么”，不能证明过去某时刻的上市/退市状态。
- `timeline(...)` 表示严格的 as-of membership。它要求
  `authoritative_lifecycle`：每个物理合约都有权威生命周期，continuous/index 等派生视图
  还必须固定对应 mapping/calendar/ranking identity。仅有合约名和到期时间不能升级为该证明。

`main`/`top` 需要历史 ranking 证据，首版历史 grammar 拒绝；`file`/`auto` 也不进入可重现
artifact。共享 current selector 继续拒绝 `physical:all` 和 `timeline(...)`。

## Identity DAG

采集 artifact 保存 provider 原始事实并产生 `acquisition_sha256`；语义 catalog 引用该 hash，
完成标准化和 lifecycle 验证后产生 `semantic_catalog_sha256`；可执行 plan v3 同时固定两者、
canonical universe、canonicalizer/compiler identity、proof 和可选 mapping/ranking identity。
任何 artifact 在反序列化后都必须重算自己的 hash，引用断链或字段篡改不得产生可执行对象。

plan v1/v2 保持原 JSON/hash 兼容：v1 可读取但没有 listing start，不能作为 cache fill 输入；
v2 继续作为显式 caller-supplied timeline fill；v3 才表达完整的 proof/hash chain。v3 不会把
observed proof 伪装成 authoritative lifecycle。

## 时间与目标语义

- `effective_ns` 是 membership 何时生效；`known_from_ns` 是该事实何时可知。两者不得互换。
- 可见 membership 与下载依赖是不同集合。continuous/index 可以作为可见成员，其底层物理
  合约只进入 dependency closure，不因此暴露为策略成员。
- tick、minute、daily 的 `first_available_data_ns` 分别证明；一个 kind 的起点不得复用于另一个。
- 请求起点是 `max(user_start, proven kind start)`；严格 lifecycle 场景还必须覆盖权威 listing
  start。名称/交割月/到期时间只允许作为探测提示，不能裁剪更早数据。
- 没有安全 bounds 查询或经过验证的空窗口证明时，自动全历史填充必须 fail closed，不能发送
  无界请求后把空响应解释成未上市。

## 持久化

data 层拥有 codec、验证器和 content-addressed store：

```text
<cache-dir>/historical-universe-v1/
  acquisitions/<sha256>.json
  catalogs/<sha256>.json
  plans/<sha256>.json
```

首版没有隐式 `CURRENT`。发布使用 root-scoped lock、同目录临时文件、file sync、rename 和
parent-directory sync；已有目标必须 byte-identical，否则按碰撞/损坏拒绝。artifact 路径和目标
hash 可以纯计算，`--dry-run` 不得创建 root、lock 或 temp。

## Ownership

`tqsdk-data` 拥有历史 spec、proof/catalog/plan codec、target resolver 和 store primitive；
`tqsdk-cache` 只负责认证、采集/填充编排、进度、report、取消和退出码；`tqsdk-session` 保持现有
query/metadata API；`tqsdk-task`/`tqsdk` 只消费已验证的 plan，不建立第二套 universe 状态。
