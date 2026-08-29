# Historical Universe 自动 Catalog 调研

日期：2026-08-30

状态：research note，不是当前架构规范，也不授权修改 Rust。

## 结论

程序内部可以完成合约查询、规范化、内容哈希、artifact 持久化和 plan 编译，普通用户不应被要求手写 `PLAN.json`。应明确区分两个入口：

```text
--universe 'physical:all'
--universe 'timeline(active:all)'
```

前者用于“下载 provider 当前 catalog 已知的所有物理合约数据”，可以使用 observed coverage；后者用于“回测时按历史时钟维护 active membership”，必须满足严格生命周期契约。

但是，当前公开的天勤/TQSDK 契约还不足以自动证明“历史 physical futures 全集 + 每个合约权威上市时间 + 严格 as-of revision”。因此必须区分两类结果：

- 数据下载可以使用首条可得行情作为 provider coverage hint；
- 动态回测 Universe 的 membership 必须使用权威 listing/first-trading 时间，不能用首条行情冒充。

在拿到满足严格契约的第一方 catalog 前，自动 discovery 应保存 `complete=false` 的诊断 snapshot 并 fail closed，不能生成一个看似完整的动态回测 plan。

## 已验证的官方能力

### 合约列表

官方 `query_quotes` 按合约类型、交易所、品种和 `expired` 状态筛选，返回合约代码。文档示例表明，不传 `expired` 时可得到某品种“包括已下市以及未下市”的合约；`expired=False` 只返回未下市合约。

- 官方文档：<https://doc.shinnytech.com/tqsdk/latest/reference/tqsdk.api.html#tqsdk.api.TqApi.query_quotes>
- 本次调研固定的官方源码：<https://github.com/shinnytech/tqsdk-python/blob/78c99226f11056b2860c39369f453808938edde2/tqsdk/api.py#L2755-L2866>

这足以作为“官方服务当前保存的未过滤 roster”的采集入口，但公开契约没有承诺：

- 它永远保留历史上出现过的每个合约；
- 它能返回任意过去时点当时可见的完整 roster；
- 历史条目永不回补、删除或修订；
- 响应带不可变 catalog revision。

因此 `query_quotes(FUTURE, expired=None)` 很有价值，但不能单独构成严格 completeness proof。

### 合约 metadata

官方期货 metadata 包含：

- `instrument_id`、`exchange_id`、`product_id`；
- `expired`、`expire_datetime`；
- `delivery_year`、`delivery_month`；
- 交易时间和合约规格。

来源：

- 官方 `Quote` 文档：<https://doc.shinnytech.com/tqsdk/latest/reference/tqsdk.objs.html#tqsdk.objs.Quote>
- 官方 `query_symbol_info` 文档：<https://doc.shinnytech.com/tqsdk/latest/reference/tqsdk.api.html#tqsdk.api.TqApi.query_symbol_info>
- 官方生成的 GraphQL schema：<https://github.com/shinnytech/tqsdk-python/blob/78c99226f11056b2860c39369f453808938edde2/tqsdk/ins_schema.py#L352-L442>

公开 futures metadata 没有 `listing_datetime`、`listed_date` 或 `first_trading_datetime`。`delivery_year/month` 不能按通用规则反推出上市时间，`expire_datetime` 只能给生命周期上界。

### 主连历史

官方 `query_his_cont_quotes(symbol, n)` 返回指定主连最近 `n` 个交易日对应的 underlying。它能证明某个物理合约曾被主连采用，但不能证明当日所有 active physical contracts，也不能覆盖从未成为主连的合约。

- 官方文档：<https://doc.shinnytech.com/tqsdk/latest/reference/tqsdk.api.html#tqsdk.api.TqApi.query_his_cont_quotes>
- 官方实现：<https://github.com/shinnytech/tqsdk-python/blob/78c99226f11056b2860c39369f453808938edde2/tqsdk/api.py#L1018-L1068>
- 官方主连表读取逻辑：<https://github.com/shinnytech/tqsdk-python/blob/78c99226f11056b2860c39369f453808938edde2/tqsdk/calendar.py#L54-L105>
- 当前主连表：<https://files.shinnytech.com/continuous_table.json>

官方 TQSDK 没有公开的 `query_his_cont_underlyings` 方法。本仓同名 Rust API 是对官方主连表和交易日历的单主连投影。

### 交易日历

官方 `get_trading_calendar(start_dt, end_dt)` 返回日期及是否交易日。文档明确说明，交易日历会在交易所公布新一年的节假日安排后更新。

- 官方文档：<https://doc.shinnytech.com/tqsdk/latest/reference/tqsdk.api.html#tqsdk.api.TqApi.get_trading_calendar>
- 官方实现：<https://github.com/shinnytech/tqsdk-python/blob/78c99226f11056b2860c39369f453808938edde2/tqsdk/calendar.py#L19-L45>
- 当前假日表：<https://files.shinnytech.com/shinny_chinese_holiday.json>

这些是 current/latest 型资源。URL 不能充当 identity；必须保存本次实际 payload、覆盖范围、规范化算法和内容 hash。

### 历史行情范围

专业版接口支持按显式 start/end 下载 Kline 或 tick，可以据返回的最早一条数据计算 `first_available_data_ns`。

- Kline/tick 范围接口：<https://doc.shinnytech.com/tqsdk/latest/reference/tqsdk.api.html#tqsdk.api.TqApi.get_kline_data_series>
- 专业版数据说明：<https://doc.shinnytech.com/tqsdk/latest/profession.html>

这个时间表示 provider 当前可得数据的下界，不等于合约真实上市时间。权限、缺数、无成交或 provider 保留策略都可能让它晚于 listing time。

## 本仓现状与风险

本仓当前有两条入口：

- `--universe EXPR`：按当前 metadata 解析 selector；
- `--universe-timeline PLAN.json`：加载 hash-pinned `HistoricalUniversePlan`。

当前 `CatalogSnapshot` 要求 `complete=true` 才允许编译动态 plan，这个 fail-closed 方向是正确的。主要风险是：

1. plan v2 的 `physical_listing_starts` 容易混淆权威上市时间与首条可得数据时间；
2. daily/minute timeline 路径若接受 plan v1，空 `physical_listing_starts` 可能生成零请求并把空报告当成功；
3. tick、minute、daily 分别复制 timeline fill 流程，会造成校验、报告、取消和信号处理漂移；
4. current/latest catalog 用于过去 horizon 时会包含后来才挂牌的合约，若没有 listing time 就存在 look-ahead。

## 推荐 Grammar

### 数据下载路径

```text
--universe 'physical:all'
```

它选择 provider 当前 catalog 返回的全部 physical futures，包括已下市与未下市合约；不声称某个过去时点的 active membership。fill 可以从每个合约的 `first_available_data_ns` 下载到用户 cutoff，并在报告中标明 `catalog_completeness=provider_current_observed`。这条路径可以在现有公开接口上安全实现。

### 动态回测路径

```text
--universe 'timeline(active:all)'
```

需要 logical derived views 时：

```text
--universe 'timeline(active:all;cont:all;index:all;!exchange:KQD)'
```

语义分工：

- `active:all` 描述选择对象；
- `timeline(...)` 表示 membership 随 replay clock 变化；
- start/end、数据粒度、finality 和数据流仍由命令参数表达。

这样不会改变现有 `--universe 'active:all'` 的 current-selector 语义，也不会把 catalog 查询或文件路径混入 Universe。`physical:all` 与 `timeline(active:all)` 不应互相隐式升级：前者允许 observed coverage，后者要求 authoritative lifecycle。

parser 需要识别括号层级，不能继续对整个表达式直接 `.split(';')`。建议 v1 约束：

- 表达式要么是 current selector，要么是单个顶层 `timeline(...)`；
- 禁止嵌套 timeline；
- timeline 内必须有 physical include；
- `top:N` 和依赖当前排名的 `main` 默认禁止进入 timeline；
- canonicalization 固定顺序、去重、大小写和 exclusion，其版本进入 identity。

### Pinned plan

普通用户不需要手写 PLAN。程序自动保存 snapshot 和 plan，并在 report 中返回路径与 SHA-256。严格复现时允许：

```text
--universe 'timeline(active:all)' --universe-plan PLAN.json
```

plan 是执行输入，不是 selector。CLI 必须验证 plan 内的 canonical Universe、scope 和 horizon 与命令一致。现有 `--universe-timeline` 可作为迁移期兼容别名。

## 推荐数据流

```text
Universe + horizon + requested as-of
  -> acquire official responses
  -> normalize acquisition snapshot (complete defaults false)
  -> persist snapshot by content hash
  -> completeness/lifecycle/calendar gates
  -> compile and verify HistoricalUniversePlan
  -> persist plan by plan_sha256
  -> resolve physical target ranges once
  -> kind-specific tick/minute/daily fill
```

### Acquisition artifact

每次采集至少保存：

- requested、effective、observed as-of；
- query operation、参数、SDK/本仓版本；
- 每个响应的 exact hash、记录数、分页结束证据、ETag/Last-Modified（若有）；
- canonical Universe/scope；
- calendar/continuous exact payload hashes；
- completeness assertion 和缺失证据。

如果 source 只能给 current/latest，`effective_as_of` 必须标为 `unverified`。

### 生命周期字段必须拆分

后续 schema 应区分：

- `authoritative_listing_ns` / `physical_membership_starts`：决定 strategy-visible timeline；
- `first_available_data_ns` / `physical_data_available_starts`：provider coverage hint；
- `physical_warmup_starts`：planner 为下载计算的执行起点；
- `expire_datetime_ns`：保留原始单位、时区和来源。

在 plan v2 兼容期，`physical_listing_starts` 只能接收权威 listing time，不能回填 first-data time。

### Completeness gate

只有以下条件同时满足，才能生成可执行 catalog/plan：

- source 明确承诺 requested scope/horizon 的历史 physical 全集；
- 每个合约有权威 membership start/end；
- calendar 覆盖整个 horizon，identity 来自 exact content；
- derived views 所需主连表覆盖相关产品和日期；
- 没有重复 symbol、冲突 metadata、非法 interval 或未知单位；
- acquisition 过程中没有 source revision 漂移。

按目前公开 TQSDK 契约，这个 gate 不能严格通过。程序应保存 `complete=false` 的诊断 artifact，并拒绝严格 timeline plan/fill。

### Artifact 持久化

建议内容寻址保存：

```text
catalogs/<acquisition_sha256>.json
plans/<plan_sha256>.json
reports/<run_id>.json
```

使用 create-new 或临时文件加原子 rename；同一 hash 已存在时逐字节校验。fill 重新打开 pinned artifacts，不再查询 catalog、calendar 或 continuous endpoints；网络只用于 plan 已列出的历史行情请求。

### Shared target resolver

tick、minute、daily 共用 `resolve_historical_fill_targets(plan)`，在接触 cache 或远端前统一执行：

- timeline fill 强制 plan v2+；
- 非空 physical timeline 必须有非空 lifecycle/warmup map；
- 每个 physical add 对应一个有效 start；
- 每个请求满足 `start < end`；
- request 集合覆盖 plan 要求的每个 symbol/range；
- 非空 physical Universe 却生成零请求时返回 data-contract error；
- 三种 kind 复用标准 report、progress、cancellation 和 signal handling。

## Identity

至少区分：

1. `semantic_catalog_sha256`：canonical scope、合约、权威 lifecycle、completeness assertion；
2. `acquisition_sha256`：semantic content 加 source hashes、as-of 和 provenance；
3. `plan_sha256`：catalog/calendar/continuous identities、Universe、horizon、derived views、budget 和 compiler version。

另外：

- `calendar_identity` 应包含 exact holiday bytes、supported range、timezone 和算法版本；
- `continuous_identity` 应包含 exact table bytes 和 normalization version；
- `catalog_id` 仅作可读别名，安全判断使用内容 hash；
- `observed_at` 不单独充当 semantic identity。

## Fail-closed 与 Look-ahead

以下情况拒绝可执行 plan/fill：

1. 没有历史全集承诺；
2. listing start 缺失，只有 first-data/first-observed；
3. 用户要求历史 as-of，source 只能给 current/latest；
4. calendar/continuous payload 未固定或范围不完整；
5. acquisition 期间资源 revision 漂移；
6. unfiltered roster 与 expired true/false 并集冲突；
7. 用主连表的 underlying 并集冒充 `active:all`；
8. v1 plan、空 lifecycle map、零请求或范围覆盖不完整；
9. pinned plan 与用户 Universe/horizon 不一致。

`first_available_data_ns` 可以优化下载，但不能决定 membership。后来的 catalog 修订若用于过去回测，应显式标为 `latest_corrected`，不能声称 point-in-time。

## 实施顺序

1. 先修 timeline fill boundary：强制 v2+、非空 request、逐 symbol/range coverage，并抽出三种 kind 共用 resolver。
2. 增加 `physical:all` observed download 路径，自动查询 current provider roster 并固定 acquisition artifact。
3. 拆分 authoritative membership start、first-data 和 warmup start。
4. 增加 `timeline(...)` parser/canonicalizer，保持 current selector 兼容。
5. 增加内容寻址持久化和 `catalog inspect/discover`。
6. 接入满足 complete + listing + version/as-of 契约的第一方 source 后，才打开严格自动 timeline snapshot -> plan -> fill。
7. 增加高级 `--universe-plan`，旧 `--universe-timeline` 作为兼容别名迁移。

## 最终判断

`--universe-timeline` 的普通使用可以融合进 `--universe` 的时间化语义，但 PLAN 本身不应被塞进 selector。最佳用户体验是：`physical:all` 立即支持下载所有当前可发现的历史物理数据，`timeline(active:all)` 表达严格动态 membership；两者都由程序内部生成并固定 artifacts，但只有后者在证据不足时 fail closed。
