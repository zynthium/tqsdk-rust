# 历史 Universe Catalog 与填充合同

本文定义历史数据下载和动态回测使用的 catalog、证明与 artifact 边界。字符串入口保持
legacy-first：既有 `UniverseExpression` / `HistoricalFillUniverseSpec` 语义不变；新
`timeline(...)` Universe Language V2 编译 current plan v5。可见 CLI 入口只有 `--universe`，语言规则见
[Universe Language V2](universe-language.md)。

`timeline(...)` 可用 `except(contract:<targets>)` 批量移除物理合约 view，或用 `except(all:<structural-targets>)` 移除所有可分类 view。该输入糖在 parser 中归一为既有 `!` 排除，因此 V5 identity、已发布 acquisition/catalog/plan 和 migration 输入保持兼容。

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

V2 `timeline(...)` 在完整 discovery 后、native-daily 请求前按 normalized AST 计算本次
bootstrap closure。closure 包含保留的物理 contract，以及保留的 continuous/index/logical-symbol
view 所需的同品种物理成员；`except(contract:CZCE.RI)` 只移除 contract provenance，不能移除
仍为 derived view 所需的 RI 依赖。closure 小于完整 discovery 时另存为 scoped
`provider_current_observed` acquisition；完整 closure 复用既有全量 acquisition，
identity 固定包含 `tqsdk.provider-history.timeline-bootstrap-closure.v1` 与 V2 AST/input-source
identity。完整 discovery acquisition 仍保留作审计；legacy `physical:all` 与 legacy timeline
不投影，仍使用全量 roster。

### `provider_history_observed`

稳定 bootstrap roster 的每个成员请求 `[1990-01-01, as_of)` 原生 1d，并持久化
`HistoricalDailyObservation`：

- `complete + first_row_ns=Some(...)`：进入数据 membership catalog；
- `complete + first_row_ns=None`：provider 明确完成空区间，不进入 catalog；
- `provider_unavailable`：隔离的 provider chart 在有界探测内不可用，不进入 catalog；实际 timeout
  纳秒值随 observation 持久化。

观测表必须与 acquisition roster 精确等键，观测区间必须与 bootstrap 合同一致，所有状态均参与
acquisition SHA-256。完成探测后再次进行全量 discovery，再投影为相同 bootstrap closure；closure
内的 roster/metadata 漂移会拒绝升级。被本次 closure 排除的合约不会发 native-daily 请求，也不会
影响该 scoped provider-history proof。

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

legacy `physical:all` 仍表示完整 provider-history 数据 membership；既有
`timeline(active:all;cont:all;index:all)` 保持 legacy 顺序语义。V2 使用明确 view：

```text
timeline(contract:all;continuous:all;index:all)
```

可见 membership 与下载依赖是两个集合。physical contract 可以直接可见；continuous/index 是逻辑
instrument，并固定其物理或 provider-series dependency closure。`timeline(main/top)` 在没有
hash-pinned historical ranking capability 时必须在 acquisition 前失败。

每个 plan 同时固定 tick、minute、daily targets。kind 起点为
`max(user_start, listing/data-membership floor, kind first-available evidence)`；若
`provider_history_observed` 只有 native-daily evidence，则 daily membership floor 是 tick/minute 的安全
请求下界，最终空前缀和完整性仍由对应 kind 的 terminal cache coverage 证明。不能从合约名、交割月份
或规则向前推断。

旧 plan 字段 `physical_listing_starts` 在 provider-history proof 下严格表示
`physical_data_membership_starts`，不表示挂牌日。plan v3 继续固定 legacy canonical universe、
acquisition/catalog/calendar、membership/dependencies/kind targets 与 execution hash。

plan v4 在此基础上固定：

- Universe Language V2 normalized AST bytes/hash 和 compiler/canonicalizer identity；
- 外部 symbol 文件的 content-derived `input_sources_sha256`；
- acquisition、semantic catalog、calendar、proof 与 execution SHA-256；
- 可见 membership、dependency set、tick/minute/daily target hashes；
- `rollback_v3_plan_sha256`，以及需要时的 continuous/ranking identity。

## 持久化与兼容

data 层拥有 codec、验证器和 content-addressed store：

```text
<cache-dir>/historical-universe-v1/
  acquisitions/<sha256>.json
  catalogs/<sha256>.json
  plans/<sha256>.json
  provider-daily-retries/<retry-state-sha256>.json
```

`provider_unavailable` 的后续探测使用独立、版本化、内容寻址的 retry receipt。它绑定不可变
acquisition hash，记录每个仍不可用 symbol 的尝试次数和下次可探测时间；它不属于
`HistoricalDailyObservation` 的 proof body，因此旧 acquisition 的 JSON/body/hash 不会因一次
timeout 重试而变化。默认退避为未到期合约 `1h, 1d, 7d, 30d`，已到期合约
`7d, 30d, 90d`。同一 acquisition 的 receipt winner 以
`(observed_at_ns, retry_state_sha256)` 确定，读取损坏或不匹配 receipt 必须 fail closed。

维护命令固定一个已有 acquisition，而不是重新解释 universe 或推进 cutoff：

```text
tqsdk-cache --cache-dir DIR refresh-provider-membership \
  --acquisition-sha256 sha256:... --max-symbols 4 [--force] [--dry-run]
```

它仅支持 futures、最多 32 个 symbol，且始终请求 native-daily history；不要传普通 fill 的
`--kind`。`--dry-run` 只选择 due candidates，不认证、不请求 provider、不创建 lock 或文件。
真实执行持有 cache-root operation lock，在每轮候选前以已完成合约的 isolated-cache remote
canary 验证 provider；canary、认证、传输、取消或非精确-timeout 失败都不发布 proof 或推进
receipt。探测前后必须重新获得全量 discovery，并投影为与 pinned acquisition 相同的 stable bootstrap
roster/metadata，且
`requested_as_of_ns` 必须完全相同；任何漂移或新 cutoff 都要求完整 bootstrap。
canary 选择成熟的未到期 complete observation，并只请求其已知首行所在的一日窗口；默认给 canary
30 秒以区分 provider 慢启动与单合约 timeout，真实候选仍保持 15 秒上限。显式
`--batch-timeout-secs` 同时覆盖两者。

重复 timeout 只发布新的 retry receipt。首次 daily row 或 terminal-empty 才生成新的
acquisition 和 semantic catalog；维护命令从不生成 plan，因为一个 acquisition 可被多个
universe 输入引用。需要新 plan 时，操作者以相同 `--universe` 和相同固定 `--end-day` 再运行
普通 `fill`。

目录名保持兼容，plan 文件使用 flat `plan_version` dispatch。旧
`HistoricalUniverseArtifactStore::publish_plan/load_plan` 只处理 v1–v3；
`publish_plan_artifact/load_plan_artifact` 保留 version-dispatched v1–v5 读取，以便迁移与受控兼容；
`publish_current_plan/load_current_plan` 是 normal V5 路径。V5 Rust 类型使用 private fields、固定
wire 与 validated constructors，避免 public struct literal 或 domain serde shape 影响持久 hash。

V2 timeline writer 默认从同一 materialized resolution 发布 canonical V5。旧
`v4-with-v3-rollback` policy token 仅作为隐藏兼容开关，实际仍产生 V5；它不再触发 V4/V3 dual write。
V4→V5 迁移先验证 V4/V3 rollback projection 和 acquisition/semantic chain，生成 source-to-current mapping，
然后才可用 `--apply` 发布 V5。source、rollback 与其内容地址都保留，且不会更新 mutable current pointer。

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
tqsdk-cache fill --kind tick|minute|daily \
  --universe 'timeline(contract:all)'
```

`physical:all` 和既有 legacy timeline 继续写 v3。V2 timeline 默认发布 V5，不需要 writer policy。
`--universe-file` 可重复并在 provider access 前一次性展开，其 identity 进入 V5。

`--universe-plan` 只作为隐藏兼容入口；V4 artifact 先用 `migrate-universe --plan-sha256 <V4_SHA256>`
迁移，V1–V3 重新编译；`--universe-timeline` 已移除。未传 `--end-day` 时 cutoff 固定为
本次启动时最新可用闭市边界。dry-run 只审计稳定 provider roster，返回
`preparation_required`/exit 1，因为生成数据 membership 必须写 native-daily cache。

`tqsdk-data` 拥有 spec、bootstrap closure、proof、catalog、plan、验证和 artifact store；`tqsdk-cache` 只负责认证、采集/填充
编排、进度、report、取消与退出码；`tqsdk-session` 提供 query/metadata 和 server-history substrate；
`tqsdk-task`/`tqsdk` 只消费已验证的 timeline/plan。`BacktestBuilder::historical_universe_artifact`
验证 V5 自身、区间和 acquisition/catalog chain；timeline 控制可见 instrument，V5 tick targets
控制物理 cache dependency 和首可用边界。V4 的 rollback chain 只在迁移时验证。
