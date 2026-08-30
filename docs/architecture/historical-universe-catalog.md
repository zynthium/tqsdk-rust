# 历史 Universe Catalog 与填充合同

本文定义历史数据下载和动态回测使用的 catalog、证明与 artifact 边界。当前/live
`UniverseExpression` 的语法和语义不变；历史入口使用
`HistoricalFillUniverseSpec`，可见 CLI 入口只有 `--universe`。

## 核心语义：数据生命周期

默认历史 universe 不再试图恢复交易所法定挂牌日期。物理合约只有在 provider
首次产生可观测 native-daily 行后才成为历史 universe 成员：

- membership 起点是 `[1990-01-01, as_of)` 完整探测中的第一条 native-daily 行；
- 终态完成但没有行的候选合约不进入 universe；
- symbol batch size 1 的精确 timeout 记录为 `provider_unavailable`；它不进入当前 universe，也不冒充终态空、“从未挂牌”或永远无行情；
- 到期 metadata 仍用于 membership 终点；未到期合约的终点是本次 `as_of`；
- tick/minute/daily 的实际首行与空前缀仍由各自 cache coverage 独立证明，daily
  membership 起点只提供安全请求 floor。

这里的“严格”表示：稳定 provider roster 的每个候选合约都有不可变、可复核的观测结果，且
catalog 只包含有正向数据证据的成员。它不表示“交易所法定挂牌全集”，也不要求交易所官网、
公告爬虫或 reference lifecycle 服务。

## 证明等级

### `provider_current_observed`

`physical:all` 先查询 provider 当前可枚举的全部物理期货合约。采集记录查询前后 roster、完整
metadata、观察时间和 source identity；scope 固定为
`CFFEX,SHFE,DCE,CZCE,INE,GFEX`。`complete=true` 只表示前后 roster 稳定且每个成员都有
metadata。roster 漂移或缺 metadata 时保存 `complete=false` 的审计 acquisition，但不得执行。

### `provider_history_observed`

稳定 roster 的每个成员请求 `[1990-01-01, as_of)` 原生 1d，并持久化
`HistoricalDailyObservation`：

- `complete + first_row_ns=Some(...)`：进入数据 membership catalog；
- `complete + first_row_ns=None`：provider 明确完成空区间，不进入 catalog；
- `provider_unavailable`：隔离的 provider chart 在有界探测内不可用，不进入 catalog；实际 timeout
  纳秒值随 observation 持久化。

观测表必须与 acquisition roster 精确等键，观测区间必须与 bootstrap 合同一致，所有状态均参与
acquisition SHA-256。完成探测后再次查询 roster/metadata；任何漂移都拒绝升级。

CLI 强制 daily bootstrap 的 symbol batch size 为 1，使每个 scheduler 终态精确归属于一个候选。
调用方未显式设置 `--batch-timeout-secs` 时，单候选观察默认使用 15 秒 wall-clock 上限。精确 timeout
直接记录为 `provider_unavailable`：它只表达“本次有界 provider 观察不可用”，不表达终态空、从未
挂牌或永远没有行情；后续 acquisition 可以重新观察并升级。认证、transport、取消和非 timeout 失败
都拒绝发布。若没有任何完成请求，或者 unavailable 数超过 `max(8, ceil(roster/20))`，熔断并拒绝
发布，防止全局故障被误编译成空市场。

### `authoritative_lifecycle`

该 proof 继续作为旧 artifact 和显式调用方提供 catalog 的兼容路径，但不再是默认
`tqsdk-cache fill --universe` 的依赖。它不会触发交易所网页采集，也不会覆盖
`provider_history_observed` 的数据 membership 语义。

## Timeline 与执行目标

`physical:all` 内部等价于 `timeline(active:all)`；`timeline(...)` 可组合 `active`、`cont` 和
`index`。可见 membership 与下载依赖是两个集合：continuous 需要对应物理合约数据，index 使用独立
provider symbol。

provider-history plan 对 tick/minute/daily 使用
`max(user_start, first_native_daily_row)` 作为物理合约请求起点；对应 kind 的 cache 仍必须独立达到
terminal coverage。derived continuous/index 不从 daily 虚构 kind-specific availability，只能从相关产品
最早数据 membership 起点开始请求。

历史 plan 的 `physical_listing_starts` 是 v2/v3 wire compatibility 字段名；在
`provider_history_observed` proof 下，其值严格表示 `physical_data_membership_starts`，不表示挂牌日。
不能用合约名、交割月份或规则推断更早起点。

plan v3 固定以下 identity：

- canonical universe 与 canonicalizer/compiler identity；
- acquisition、semantic catalog、calendar 与 execution SHA-256；
- 可见 membership、dependency set 和 tick/minute/daily targets；
- continuous/ranking identity（选择相关 derived view 时）。

## 持久化与兼容

data 层拥有 codec、验证器和 content-addressed store：

```text
<cache-dir>/historical-universe-v1/
  acquisitions/<sha256>.json
  catalogs/<sha256>.json
  plans/<sha256>.json
```

`HistoricalDailyObservationStatus::Complete` 使用 serde default 且不写出 `status` 字段，因此已有
complete/terminal-empty provider-history artifact 的 body/hash 保持不变；只有新的
`provider_unavailable` outcome 增加显式字段。旧 `authoritative_lifecycle` artifact/hash 语义不变。

Rust source API 的 proof epoch 为
`HISTORICAL_CATALOG_PROOF_API_VERSION = 2`，`HistoricalCatalogProof` 是 `#[non_exhaustive]`。
旧二进制读取未知 proof 必须 fail closed。首版没有隐式 `CURRENT`：每个 plan 直接引用不可变 hash，
不会在重放时悄悄切到最新版。

发布使用 root-scoped lock、同目录临时文件、file sync、rename 和 parent-directory sync；读取与写入均
拒绝 symlink 祖先。已有目标必须 byte-identical。artifact 路径和 hash 可以纯计算，`--dry-run` 不创建
root、lock、temp、cache coverage 或 plan。

## CLI 与 ownership

用户入口：

```text
tqsdk-cache fill --kind tick|minute|daily --universe 'physical:all|timeline(...)'
```

`--universe-plan` 只作为隐藏兼容入口；`--universe-timeline` 已移除。dry-run 只审计稳定 provider roster，
返回 `preparation_required`/exit 1，因为生成数据 membership 必须写 native-daily cache。

`tqsdk-data` 拥有 spec、proof、catalog、plan、验证和 artifact store；`tqsdk-cache` 只负责认证、采集/填充
编排、进度、report、取消与退出码；`tqsdk-session` 提供 query/metadata 和 server-history substrate；
`tqsdk-task`/`tqsdk` 只消费已验证的 timeline/plan。
