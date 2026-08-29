# 历史 Universe Catalog 与填充合同

本文定义历史数据下载和动态回测使用的 catalog、证明与 artifact 边界。当前/live
`UniverseExpression` 的语法和语义不变；历史入口使用独立的
`HistoricalFillUniverseSpec`。

## 两条能力通道

- `physical:all` 表示 provider 在本次稳定快照中返回的全部物理期货合约。它的证明是
  `provider_current_observed`：采集必须记录查询前后 roster、完整 metadata、观察时间和
  source identity。当前 scope 明确固定为 `CFFEX,SHFE,DCE,CZCE,INE,GFEX`；`complete` 只表示
  该 scope 内前后 roster 稳定且每个 roster 成员都有 metadata。缺 metadata 或 roster 漂移时仍保存
  `complete=false` 的审计制品，但不得升级为可执行证明。它可以证明“本次看见了什么”，不能证明
  过去某时刻的上市/退市状态。
- `timeline(...)` 表示严格的 as-of membership。它要求
  `authoritative_lifecycle`：每个物理合约都有权威生命周期，continuous/index 等派生视图
  还必须固定对应 mapping/calendar/ranking identity。仅有合约名和到期时间不能升级为该证明。

`main`/`top` 需要历史 ranking 证据，首版历史 grammar 拒绝；`file`/`auto` 也不进入可重现
artifact。共享 current selector 继续拒绝 `physical:all` 和 `timeline(...)`。

## Identity DAG

采集 artifact 保存 provider 原始事实并产生 `acquisition_sha256`；语义 catalog 引用该 hash，
完成标准化和 lifecycle 验证后产生 `semantic_catalog_sha256`。语义 catalog 必须与 complete
authoritative acquisition 逐合约一致，包括 symbol、exchange、product 和 lifecycle，不能只复制
`acquisition_sha256` 来“洗白”无关 catalog。可执行 plan v3 同时固定两者、canonical universe、
canonicalizer/compiler identity、proof、可见 membership hash、依赖闭包 hash、tick/minute/daily
精确 target hash 和可选 mapping/ranking identity。
任何 artifact 在反序列化后都必须重算自己的 hash，引用断链或字段篡改不得产生可执行对象。

plan v1/v2 保持原 JSON/hash 兼容：它们可以读取和验证，但 CLI 默认只执行 v3。v1 永远不能作为
cache fill 输入；v2 只有操作者显式传 `--allow-legacy-universe-plan` 才能执行，并在报告中标记
`legacy_unproven=true`。v3 才表达完整 proof/hash/execution chain，不会把 observed proof 伪装成
authoritative lifecycle，也不能通过删除 v3 字段并重算 v2 hash 获得默认执行资格。

## 时间与目标语义

- `effective_ns` 是 membership 何时生效；`known_from_ns` 是该事实何时可知。两者不得互换。
- 可见 membership 与下载依赖是不同集合。continuous/index 可以作为可见成员，其底层物理
  合约只进入 dependency closure，不因此暴露为策略成员。
- tick、minute、daily 的 `first_available_data_ns` 分别证明；一个 kind 的起点不得复用于另一个。
- v3 将三个 kind 的 resolved targets 全部写入计划并纳入 plan hash；CLI 只消费所选 kind 的固定
  targets，不在执行时重新从 lifecycle 推导。continuous 的底层物理合约和 index logical series
  都在依赖闭包中；logical series 必须在 semantic catalog 中携带独立 availability source identity
  与 kind-specific boundaries。
- 请求起点是 `max(user_start, proven kind start)`；严格 lifecycle 场景还必须覆盖权威 listing
  start。名称/交割月/到期时间只允许作为探测提示，不能裁剪更早数据。
- 没有安全 bounds 查询或经过验证的空窗口证明时，自动全历史填充必须 fail closed，不能发送
  无界请求后把空响应解释成未上市。

2026-08-30 的真实 provider spike 返回 5,372 个稳定的中国物理 futures metadata（4,505 个
expired），但最早 expiry 只到 2020；2010 起点的单合约 minute 请求被 metadata coverage 拒绝，
而 direct page/bounds 查询需要当前账号没有的 `tq_dl` 权限。因此首版 `physical:all` 只发布
`provider_current_observed` acquisition 和诊断，`kind_boundaries_proven=0` 时明确不可执行；
`timeline(...)` 也不会从 expiry/name 自动升级为 authoritative plan。

## 持久化

data 层拥有 codec、验证器和 content-addressed store：

```text
<cache-dir>/historical-universe-v1/
  acquisitions/<sha256>.json
  catalogs/<sha256>.json
  plans/<sha256>.json
```

首版没有隐式 `CURRENT`。发布使用 root-scoped lock、同目录临时文件、file sync、rename 和
parent-directory sync；新建目录逐层检查所有祖先不是 symlink，并同步新目录及其 parent；读取也拒绝
任一已存在的 symlink 祖先。已有目标必须 byte-identical，并在重试成功前同步 parent，否则按碰撞/
损坏拒绝。artifact 路径和目标 hash 可以纯计算，`--dry-run` 不得创建 root、lock 或 temp。

当前实现使用标准库 pathname 操作，无法在所有检查与 open/rename 之间提供跨平台 dirfd 级
`O_NOFOLLOW` 原子性；因此 cache root 仍属于本机可信操作者边界，不支持与能并发替换其祖先目录的恶意
本地用户共享。内容寻址、逐层 symlink 拒绝和 root publish lock 防止正常并发与误配置，不宣称抵御该
本地特权竞态。

## CLI 执行合同

`tqsdk-cache fill --universe-plan PLAN.json --kind tick|minute|daily` 只读取计划文件一次，随后验证
plan、acquisition、semantic catalog 和完整 identity chain，再进入统一的 flag validation、target
selection、progress、cancel、terminal report 与退出码路径。`--universe-timeline` 只是可见兼容 alias。
`--dry-run` 使用 CacheOnly；非 dry-run 才延迟读取认证。零 targets、缺 kind target、缺 artifact 或
hash/fact 不一致都在远端认证和 cache mutation 前失败。

## Ownership

`tqsdk-data` 拥有历史 spec、proof/catalog/plan codec、target resolver 和 store primitive；
`tqsdk-cache` 只负责认证、采集/填充编排、进度、report、取消和退出码；`tqsdk-session` 保持现有
query/metadata API；`tqsdk-task`/`tqsdk` 只消费已验证的 plan，不建立第二套 universe 状态。
