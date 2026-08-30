# 中国期货合约生命周期与 TQ tick 边界调研

> 决策更新（2026-08-30）：默认历史 universe 已改用 provider 数据生命周期；交易所法定挂牌日期
> 不再是 membership 输入。本报告保留为曾评估但未采用的严格法定生命周期方案，不覆盖当前
> [历史 Universe Catalog 合同](../architecture/historical-universe-catalog.md)。

日期：2026-08-30
状态：research note，不是当前架构规范，也不授权修改 Rust。

## 结论

默认 provider 的缺口不能靠一个字段或合约代码规则补齐，必须拆成两条独立证据链：

1. **交易所 lifecycle authority**：历史合约集合、挂牌/上市交易日、最后交易日来自交易所公告、交易日历和正式历史数据；
2. **TQ provider coverage**：某个账号在某次采集时实际可取得的第一条 tick，只能由 TQ 返回的数据证明。

`listing_day` 与 `first_tick_observed_ns` 不是同一事实。前者决定历史 Universe membership；后者只决定下载从哪里开始。`delivery_year/month`、合约名中的年月、最后交易日规则和第一根 K 线都不能冒充挂牌证明。

## 实际 provider 验证补充

2026-08-30 使用同一账号、同一原生 1d 图表路径和隔离 cache 做了差分验证：

- `CZCE.CY010` 请求 `[1990-01-01, 2025-01-10)` 在约 1.2 秒内显式终态完成并返回 30 行；
- `CZCE.CY011` 的全窗口、`[2018-01-01, expiry)` 和 `[2020-01-01, expiry)` 均未创建
  `charts.{chart_id}`，也没有 `mdhis_more_data` 或 `notify`，有界重试后只能判定 unresolved；
- 郑商所官方 2020 年历史资料中明确列有 `011` 合约的结算价列（例如
  [2020 年 5 月资料](https://www.czce.com.cn/cn/rootfiles/2023/02/17/1655836243383839-1655836243416042.pdf)），
  因而“provider 不返回 daily chart”不能证明“该合约从未挂牌”。

这组实证否定了“只扫 TQ native-daily 就能 closed-world 还原全部法定生命周期”的假设。native-daily
仍可作为数据可用起点和多数合约的低成本加速证据；静默 chart 缺失必须保持 unresolved，严格
`active:all` 仍需交易所/reference lifecycle 或 provider coverage manifest。

对“2025-01-01 至今曾活跃的全部物理期货合约”有一个可显著降低成本的处理：

- 2025 年以前已挂牌、且在 2025-01-01 仍活跃的合约，只需证明它在 horizon 左边界已经挂牌；计划中的 `effective_listing_start` 可以安全地截断为 `2025-01-01`，同时保存 `left_censored=true`，不必先恢复其真实历史挂牌日；
- 2025-01-01 以后首次挂牌的合约，必须拿到交易所公告中的挂牌日，或拿到一个明确承诺覆盖“全部挂牌合约”的交易所参考数据产品；
- TQ tick 下载起点另行探测，保存为 `first_tick_observed_ns`。它永远不能反写 `listing_day`。

推荐先落地“2025-now scoped authoritative catalog + TQ coverage probe”，同时向信易科技申请机器可读的 coverage manifest。公开 TQSDK 契约没有数据集 revision、逐合约最早 tick 边界或历史 roster 完整性声明；在拿到这些能力前，程序仍应对缺失的严格证据 fail closed，但可以生成可审计、不可执行的 acquisition report。

## 四类事实必须分开

| 字段 | 含义 | 合格来源 | 不合格替代 |
| --- | --- | --- | --- |
| `historical_contract_roster` | horizon 内可能成为 physical target 的完整合约集合 | 交易所 reference/history 产品；按每个交易日归档的官方全合约表；与 TQ roster 交叉核对 | 只查当前未到期合约；只取主连成分 |
| `listing_day` / `effective_listing_start` | 合约开始允许交易的日期；timeline membership 下界 | 交易所挂牌/上市通知、逐合约参考数据；horizon 左边界的官方已挂牌快照 | 从合约月份倒推；首条成交、K 线或 tick |
| `last_trading_day` / `effective_end` | 合约允许交易的最后日期 | 交易所逐合约交易日历/参考数据；TQ `expire_datetime` 只能作为 provider metadata 和交叉校验 | 通用规则自行计算后称为权威事实；最后一次成交日 |
| `first_tick_observed_ns` | 当前 TQ 账号、权限和数据版本实际返回的最早 tick | 完成的 TQ tick 范围请求中的第一行，加请求/响应 provenance | 上市日、第一根日线、产品级“2016 年以来”宣传 |

还应单独保存：

- `request_start_ns`：用户要求的下载下界；
- `provider_product_floor`：TQ 产品级覆盖说明，例如“期货 2016 年以来”；
- `first_tick_observed_ns`：正向观测到的第一条数据；
- `no_tick_before_assertion`：只有 provider 明确给出 closed-world coverage 证明时才允许为真；
- `warmup_start_ns = max(request_start_ns, provider_product_floor, chosen_coverage_start)`。

因此，对于只填充 2025 年以来数据的普通请求，`request_start=2025-01-01` 已足够，不需要为了下载而查出 2016 年或更早的第一条 tick。只有“为 2025 年回测补齐合约全部可得历史 warm-up”才需要向前探测。

## 一手来源核验

### TQSDK / 信易科技

官方能力已经足够做 roster seed 和实际 tick 读取，但不足以单独证明 lifecycle：

- [`query_quotes`](https://doc.shinnytech.com/tqsdk/latest/reference/tqsdk.api.html#tqsdk.api.TqApi.query_quotes) 可按合约类型、交易所和 `expired` 筛选。官方示例说明不限定 `expired` 时可以返回已下市和未下市合约，因此适合作为 provider 当前保存的历史 roster seed；公开文档没有承诺它是任意历史时点的完整、不可变全集。
- [`Quote`](https://doc.shinnytech.com/tqsdk/latest/reference/tqsdk.objs.html#tqsdk.objs.Quote) 和官方生成的 [GraphQL schema（固定源码 revision）](https://github.com/shinnytech/tqsdk-python/blob/78c99226f11056b2860c39369f453808938edde2/tqsdk/ins_schema.py#L352-L442) 对期货公开了 `expired`、`expire_datetime`、`delivery_year`、`delivery_month` 等字段，但 futures fragment 没有 `listing_datetime` / `first_trading_day`。当前生成 schema 中证券类型出现的 `first_trading_datetime` 不能外推为期货字段。
- [`get_tick_data_series`](https://doc.shinnytech.com/tqsdk/latest/reference/tqsdk.api.html#tqsdk.api.TqApi.get_tick_data_series) 接受显式起止时间、返回该范围 tick，并明确属于专业版；[`DataDownloader`](https://doc.shinnytech.com/tqsdk/latest/reference/tqsdk.tools.download.html#tqsdk.tools.DataDownloader) 的 `dur_sec=0` 也是 tick 下载。
- [TqSdk 专业版文档](https://doc.shinnytech.com/tqsdk/latest/profession.html#id5) 明确专业版可下载当前 TQ 提供的全部期货、期权和股票历史数据，支持 tick 精度；[信易科技产品页](https://www.shinnytech.com/products/tqsdk) 当前写明期货 tick/任意 K 线覆盖“2016 年以来”。这是产品级 floor，不是逐合约首 tick 清单，也不是稳定的数据集 revision。
- 官方 [`DataSeries` 实现](https://github.com/shinnytech/tqsdk-python/blob/master/tqsdk/data_series.py) 会把请求拆成行情 chart/range 读取并在本地缓存，但公开响应没有可供调用者 pin 的 dataset revision。URL `master` 只用于理解当前机制；生产 identity 必须记录实际 SDK 版本、源码 revision 或包 hash。

因此，TQ 侧最理想的长期接口不是继续猜测，而是由信易科技提供或确认一个机器可读 manifest：

```text
dataset_revision
generated_at
symbol
first_tick_ns
last_final_tick_ns
coverage_status          # complete | partial | unavailable
entitlement_scope
correction_revision
```

若该 manifest 有服务方签名、稳定 revision、范围完整性语义，它可以权威证明 **TQ 数据可用边界**；它仍不能替代交易所的挂牌事实。

### 六家期货交易所

| 交易所 | 可用的一手入口 | 能证明什么 | 自动化与限制 |
| --- | --- | --- | --- |
| 上海期货交易所（SHFE） | 官方[日周数据/日交易快讯](https://www.shfe.com.cn/reports/tradedata/dailyandweeklydata/)；产品页明确提示实际最后交易日以交易日历为准；例如[铸造铝合金首批合约和上市日](https://www.shfe.com.cn/index/othercontents/2025_AD/new-1.html) | 每日正式出现的合约、当日统计；挂牌公告能证明首批合约及上市日；交易日历证明逐合约最后交易日 | 日数据页面可按日期采集，但页面/API 可能改版；挂牌公告标题和 HTML/PDF 格式不统一，需要 parser + 人工例外队列 |
| 上海国际能源交易中心（INE） | 官方[历史数据下载](https://www.ine.cn/reports/tradedata/datadownload/)（页面列出 2018 年起年度期货/期权数据）、[日周数据](https://www.ine.cn/reports/tradedata/dailyandweeklydata/)和[网站地图中的交易日历](https://www.ine.cn/sitemap/) | 官方逐日/年度记录、horizon roster、最后交易日校验 | 年度文件适合 bootstrap、每日文件适合增量；信息管理规则说明交易信息由 INE 统一管理，商业再发布需另行确认授权 |
| 大连商品交易所（DCE） | 大商所门户的统计/下载能力；官方子公司飞创信息发布的 [Level-1 字段说明](https://www.dfitc.com.cn/dfitc/resource/cms/article/362287/371917/%E5%A4%A7%E8%BF%9E%E5%95%86%E5%93%81%E4%BA%A4%E6%98%93%E6%89%80Level-1%E8%A1%8C%E6%83%85%E6%95%B0%E6%8D%AE%E5%86%85%E5%AE%B9%E6%98%8E%E7%BB%86%E8%A1%A8.pdf) 和[行情信息服务报价](https://www.dfitc.com.cn/dfitc/resource/cms/article/383188/371917/%E5%A4%A7%E8%BF%9E%E5%95%86%E5%93%81%E4%BA%A4%E6%98%93%E6%89%80%E8%A1%8C%E6%83%85%E4%BF%A1%E6%81%AF%E6%9C%8D%E5%8A%A1%E6%8A%A5%E4%BB%B7.pdf) | 官方行情字段中的交易日、合约号；付费历史产品可作为更强的 closed-world 数据源 | 公共门户有会话、验证码、反爬和改版风险；报价表明确历史产品是商业产品，若要把它作为默认后端需采购/签约，不应把网页爬虫当 SLA |
| 郑州商品交易所（CZCE） | 郑商所公告及年度/逐日静态文件；例如官方文件明确[油菜籽、菜籽粕的上市日和首批合约](https://www.czce.com.cn/cn/rootfiles/2012/12/26/1356346927698038-1356346927700227.pdf)；郑商所授权子公司易盛的[行情产品说明](https://www.esunny.com.cn/exchange/info/134) | 公告可证明 listing；静态日表可构造 official-first-published observation；易盛的历史行情服务可提供授权数据产品 | 旧合约代码年份位数规则发生过变化，必须以原始代码和交易日上下文规范化，不能只靠字符串补世纪；易盛明确历史行情属于授权经营服务，费用/许可需商务确认 |
| 中国金融期货交易所（CFFEX） | 官方[历史数据下载](https://www.cffex.com.cn/lssjxz/)；[历史数据服务](https://www.cffex.com.cn/lssjfw/) 明确产品覆盖中金所挂牌交易的所有品种与合约信息，包含所有快照数据；交易所通知也会列首批合约，例如[上证 50 股指期权上市通知](https://www.cffex.com.cn/cn/jystz/20221214/30918.html) | 历史产品是六家中公开文字最明确的“所有品种与合约 + 快照”closed-world 候选；通知证明 listing | 公共下载与申购历史产品不是同一保证等级；全快照数据按官网“资料下载/申购流程”办理，需费用、授权和交付格式确认 |
| 广州期货交易所（GFEX） | 官方[历史行情与交易日历入口](https://www.gfex.com.cn/gfex/jyrl/list.shtml)、产品页和挂牌通知；例如[铂产品页](https://www.gfex.com.cn/gfex/sspzb/sspz.shtml) 同时链接挂牌基准价通知并给出最后交易日规则；官方[行情授权申请指引](https://www.gfex.com.cn/gfex/sdhqzlxa/202212/06f979ada4364863b63497b332a5d146/files/690cbed3f4954ad1a1201df7190de6e9.pdf) | 2022 年开市以来的官方历史行情、挂牌通知和 calendar 可建立完整 bootstrap 候选 | 历史较短，适合一次性全量回溯；公开页面是否承诺包含“零成交但已挂牌”的所有合约仍需书面确认，商业分发需按授权指引办理 |

交易所公开日行情通常是日统计，不是 tick 原始流。它可以发现“某合约在某日被交易所正式发布”，不能证明 TQ 保存了该日 tick。反过来，TQ 返回一条 tick 也只能证明 provider 保存了该条记录，不能证明这一天就是交易所挂牌日。

## 证据等级与执行门槛

为避免把推断升级成事实，建议每个字段携带 `evidence_class`：

| class | 证据 | 可用于什么 |
| --- | --- | --- |
| `exchange_explicit` | 交易所逐合约挂牌通知、逐合约交易日历、正式 reference product | 可决定 strict timeline membership |
| `exchange_closed_world` | 交易所或其授权数据产品明确声明覆盖 requested scope 的所有合约 | 可构造完整 roster；是否能推导 listing 取决于产品字段和合同语义 |
| `exchange_observed` | 某日官方日表出现该合约，但来源未承诺包含全部已挂牌零成交合约 | 交叉校验、发现候选和 horizon 左边界 presence；不能自动声称真实 listing day |
| `provider_metadata` | TQ `query_quotes` / `expire_datetime` | roster seed、symbol 映射、下载能力检查；不能单独证明交易所 lifecycle |
| `provider_tick_observed` | 完成的 TQ 范围请求实际返回 tick | 设置正向 `first_tick_observed_ns`；不能设置 listing |
| `inferred` | 合约名、交割月、规则计算、首根 K 线 | 仅诊断/候选；strict plan 禁止使用 |

strict `timeline(active:all)` 的门槛：

1. horizon scope 内 roster 有 `exchange_closed_world`，或由逐日官方全合约数据加完整性合同构成同等证明；
2. 每个 2025 年后新挂牌合约有 `exchange_explicit listing_day`；
3. 2025 年前合约只要能以官方材料证明在左边界已挂牌，可使用 `effective_listing_start=2025-01-01, left_censored=true`；
4. 每个退出 horizon 的合约有明确 `last_trading_day`；
5. source payload、parser、symbol mapping 和 calendar 都已 pin；
6. TQ 下载只消费编译后的 physical targets，不在执行时重新改变 membership。

缺一项时，仍保存 acquisition 和差异报告，但 `executable=false`。

## 可实施的 2025-now 方案

### 阶段 1：先解决用户当前要下载的集合

固定：

```text
horizon_start = 2025-01-01T00:00:00+08:00
horizon_end   = latest_closed_trading_cutoff
scope         = SHFE, INE, DCE, CZCE, CFFEX, GFEX physical futures
```

采集流程：

1. 用 TQ `query_quotes(ins_class=FUTURE, expired=None)` 获取 provider roster seed，保存原始响应及 SDK/schema revision；
2. 下载六所 2025 年至今的官方年度/逐日合约数据，按交易日取所有 physical futures 的并集；
3. 用 2024 年最后一个交易日和 2025 年第一个交易日的官方合约表建立左边界 baseline；
4. 对 2025 年后首次出现的 symbol，抓取相应交易所挂牌/增挂公告；无法自动解析的放入人工 review queue，禁止用首次出现日静默代替；
5. 从交易所交易日历/参考数据取最后交易日；TQ `expire_datetime` 只做差异校验；
6. 规范化 symbol 后计算三方差集：`TQ - exchanges`、`exchanges - TQ`、不同交易所源之间的 lifecycle 冲突。任何未解释差异使 catalog 不可执行。

此阶段只需要约 400 多个交易日的官方日表，而不是回溯所有交易所成立以来的数据。GFEX 从 2022 年开始，全量 bootstrap 也很小。

### 阶段 2：探测 TQ 的逐合约 tick coverage

对编译出的每个物理合约：

1. 若请求只是“2025 年至今”，直接以 `request_start=2025-01-01` 请求；返回第一条 tick 记为该请求范围内的 `first_tick_observed_ns`，无需探测更早历史；
2. 若请求包含全部可得 warm-up，初始下界为 `max(exchange_listing_day, 2016-01-01)`；对于 `left_censored` 合约，下界先取 TQ 产品 floor；
3. 按官方交易日历逐日或分小窗请求 tick。先用较粗窗口定位第一个非空交易日，再在该交易日内请求完整范围，取返回的最小时间戳；
4. 空窗口不能立即解释成“之前没有数据”。只有请求明确成功、权限有效、范围 final、没有服务端错误，才能记录 `observed_empty_window`；超时、限流、认证失败、未知 symbol 分开记录；
5. 如果从产品 floor 到第一条 tick 之前的所有交易窗口都以可验证成功状态返回空，可以生成 `exhaustive_probe_from_floor=true`。这仍是本次 TQ dataset observation，而不是交易所 lifecycle；
6. 同一 symbol 至少保存一次重试/隔日复核策略。若第一条 tick 提前或推迟，创建新 coverage revision，不覆盖旧 artifact。

不要用日线先出现的日期直接作为 tick 边界。日线可以作为加速 hint，但最终值必须来自 tick 响应。

### 阶段 3：争取 provider 原生 manifest

向信易科技提出书面/API 需求：

- `query_quotes` 是否承诺保留全部已下市物理期货，范围从何时开始；
- 是否可提供 `listing_datetime`，该字段来源是交易所还是内部首见；
- 是否可提供每个 symbol 的 `first_tick_ns`、最后 final tick、缺口区间；
- 数据修订如何标识，旧 revision 能否重放；
- 专业版账号的 entitlement 是否影响历史边界；
- 批量探测是否有 QPS、并发、流量和 fair-use 限制；
- 用户本地保存、校验 hash 和生成 derived catalog 是否符合服务协议。

若拿到服务方签名 manifest，阶段 2 可退化为抽样校验，大幅减少几千合约的空窗 probing。

### 阶段 4：持续增量

- 每个已关闭交易日抓取六所官方合约表；
- 新 symbol 必须在当日寻找挂牌/增挂公告，未找到则报警并保持 pending；
- 每次 source payload 按内容 hash 保存；修订产生新 snapshot，不原地修改；
- 到期合约在交易所 final calendar 确认后封口；
- 每月重新跑 TQ/exchange roster 差集，每季度抽样复探 earliest tick。

## Artifact 与可重现 identity

一个可执行 catalog 至少需要：

```text
catalog_scope
horizon_start / horizon_end
source_url
source_kind
retrieved_at
http_etag / last_modified          # 若有
raw_payload_sha256
parser_name / parser_version
exchange_symbol_raw
canonical_symbol
listing_day
effective_listing_start
left_censored
last_trading_day
lifecycle_evidence_class
lifecycle_source_sha256
tq_account_entitlement_fingerprint # 不含账号和凭证
tqsdk_version
tq_endpoint_identity
tq_request_range
first_tick_observed_ns
exhaustive_probe_from_floor
tq_response_manifest_sha256
coverage_observed_at
```

至少分三个 hash：

1. `raw_acquisition_sha256`：原始 bytes、HTTP metadata、采集参数；
2. `semantic_lifecycle_sha256`：规范化 roster 与 lifecycle、scope、parser/canonicalization version；
3. `provider_coverage_sha256`：TQ dataset/entitlement identity、所有 probe 请求结果和 earliest observations。

最终 `plan_sha256` 引用 lifecycle hash、coverage hash、calendar hash、Universe grammar/compiler version 和 horizon。URL、文件名、`observed_at` 或“最新版”都不能单独充当 identity。

## 失败模式与关闭策略

| 失败 | 风险 | 处理 |
| --- | --- | --- |
| 交易所页面改版、验证码、反爬、附件消失 | 漏掉新合约或拿到错误 HTML | 校验 MIME、schema、记录数和日期；保留原始 bytes；parser 失败即 pending，不返回空集合 |
| 公告只写品种上市、未列每个后续增挂月份 | 把品种上市日误用于后续月份合约 | 后续月份使用增挂公告/reference product；找不到则不提升为 `exchange_explicit` |
| 官方日表首见晚于挂牌日 | look-ahead 或缩短 membership | 首见只标 `exchange_observed`，不能静默当 listing |
| 合约当日无成交 | 把无 tick 误判为未挂牌 | lifecycle 与 data coverage 分离；空 tick 不删除合约 |
| TQ 权限不足、试用到期、限流、网络故障 | 空响应被误认为无历史 | 错误类别化；只有成功且 final 的空范围才记 observation；认证/限流导致整个 coverage snapshot 不可执行 |
| TQ 后续回补/修正历史 | earliest tick 和 hash 漂移 | 新建 coverage revision；plan 引用旧 hash；不原地覆盖 |
| `expire_datetime` 与交易所 calendar 冲突 | 时区或最后夜盘归属错误 | 交易所明确日期优先，保存冲突并阻止 strict 编译；所有 datetime 带 Asia/Shanghai 和 trading-day 语义 |
| CZCE 三位/四位年月代码规范化错误 | symbol collision | 结合原始交易日、品种规则 revision 显式映射；碰撞 fail closed |
| 当前 roster 被误作历史 as-of roster | 回测 look-ahead | current/latest 仅作 seed；membership 必须来自 pinned lifecycle artifact |

## 授权、费用与发布边界

- TQ 历史 tick 下载需要专业版权限；价格、并发和服务范围会变化，运行时应只判断 entitlement，不把网页价格写死在协议里。
- CFFEX 官网明确历史全快照产品需按资料下载中的申购流程办理；DCE 飞创报价表和 CZCE 易盛产品页也把历史/非展示行情作为商业服务。若公共网页不能提供 closed-world 保证，应优先采购 reference/历史产品，而不是长期依赖未承诺的爬虫。
- SHFE/INE 信息规则表明交易信息由交易所统一管理，未经许可不得擅自发布或用于商业用途；GFEX 也有行情授权申请流程。程序可让用户用自己的权限在本地采集，但把原始交易所数据、TQ tick 或完整 manifest 随 crate/公共镜像再分发前必须做逐所许可审查。
- 仓库默认不应内置真实账号、token、原始付费数据或可反推出用户 entitlement 的标识。`entitlement_fingerprint` 必须不可逆且仅用于判定同一权限上下文。

## 推荐决策

1. **现在可做**：以六所官方日表/公告/日历建立 `2025-now` lifecycle artifact；TQ `query_quotes` 做 roster seed 和一致性检查；TQ tick 请求生成独立 coverage artifact。
2. **CLI 仍只暴露 `--universe`**：内部自动采集、编译并 pin artifacts，不要求普通用户提供 `PLAN.json`；但报告必须返回 artifact 路径和 hashes。
3. **两种语义不要混合**：`physical:all` 可在报告明确 `provider_current_observed` 后执行下载；`timeline(active:all)` 只有 lifecycle gate 全部通过才能执行。
4. **优先商务解决 TQ boundary**：向信易科技申请带 revision 的逐合约 coverage manifest。这比对数千合约长期扫空窗口更便宜、更稳定，也更能解释回补和权限变化。
5. **交易所 lifecycle 采用“公共采集 + 授权产品升级”**：公共官网足够完成 2025-now 原型和交叉核验；生产 closed-world guarantee 优先使用交易所或其授权子公司的 reference/history 产品。
6. **永不自动升级推断**：合约名、交割月份、规则计算、官方日表首见、TQ 首 tick 都保留自身 evidence class；没有交易所明确证据就不写 `authoritative_listing_day`。

这个方案能让“填充 2025 年至今所有合约 tick”先在限定 horizon 内落地，同时保留严格可重现性：交易所回答“哪些合约何时可交易”，TQ 回答“我这次实际能给哪些 tick”，planner 只在两条证据链分别通过后执行。
