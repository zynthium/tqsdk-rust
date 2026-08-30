# 严格合约生命周期 catalog：运行时增量与交易所 fallback

> 决策更新（2026-08-30）：项目不再要求法定挂牌 lifecycle，因此本文的中心化 exchange catalog
> publisher 方案不进入默认实现。运行时只构建 provider 数据 membership；本文仅保留为备选研究。

日期：2026-08-30
检索日期：2026-08-30
状态：research note，不是当前架构规范，也不授权修改 Rust。

## 结论

保持严格语义，**不等于让每个 `tqsdk-cache` 用户进程爬取六家交易所官网**。

更合理的边界是：

1. TQ 继续承担每次运行时的合约候选集和未来新增合约发现；
2. 由项目维护者运行一个低频、中心化的 catalog publisher，集中处理交易所/reference fallback、格式变化、授权和人工例外；
3. 普通用户只下载经过签名、内容寻址的 immutable catalog/delta，或者使用随版本内置的 catalog；
4. 回测计划解析 `latest` 后必须记录实际 catalog SHA-256 和 coverage watermark，重放时不再查询“最新版”。

这使交易所网站的反爬、改版和授权问题退出 SDK 的普通执行路径。交易所 fallback 是 **catalog 生产问题**，不是 **每个最终用户的运行时依赖**。

若拒绝中心化 publisher，同时又要求“法定挂牌全集”的严格保证，那么最终程序确实必须连接一个具有同等权威性的外部来源。没有 TQ coverage manifest、交易所/reference 数据或已发布 catalog，程序无法凭合约代码和行情反推出从未成交、被修订或 provider 静默遗漏的合约；这是信息缺失，不是爬虫实现技巧可以解决的问题。

## TQ 足以承担未来增量候选发现

TQSDK 官方示例明确把不传 `expired` 的 `query_quotes(ins_class="FUTURE", product_id="au")` 描述为获取某品种“**全部合约，包括已下市以及未下市合约**”；传 `expired=False` 则是全部未下市合约。API 文档也将 `expired` 定义为可选的“是否已下市”筛选条件：

- [TQSDK 基本使用：根据合约类型、交易所、品种查询合约](https://tqsdk-python.readthedocs.io/en/stable/demo/base.html#t52)
- [TqApi.query_quotes](https://tqsdk-python.readthedocs.io/en/stable/reference/tqsdk.api.html#tqsdk.api.TqApi.query_quotes)
- [TqApi.query_symbol_info](https://tqsdk-python.readthedocs.io/en/stable/reference/tqsdk.api.html#tqsdk.api.TqApi.query_symbol_info)

`query_symbol_info` 返回的 metadata 包含 `instrument_id`、`exchange_id`、`product_id`、`expired`、`expire_datetime`、`delivery_year` 和 `delivery_month`。因此，程序独立运行时可以定期重查 TQ roster，并对相邻 snapshot 做差来发现新合约，无需为了“有新合约了”去爬交易所网站。

本仓当前实现已经采用这一路径：

- `crates/tqsdk-data/src/historical_universe_acquisition.rs:91-105` 使用 `query_quotes(Some("FUTURE"), None, None, None, None)`，覆盖 CFFEX、SHFE、DCE、CZCE、INE、GFEX，并排除主连等派生 symbol；
- 同文件 `:48-89` 在一次 acquisition 内查询 roster before/after，并对并集补齐 metadata；
- 同文件 `:185-208` 保存 TQ 的 `expired` 和 `expire_datetime`，但没有伪造不存在的 listing 字段。

这足以形成 `provider-observed candidate` 和未来增量发现。它仍不能单独证明以下更强命题：TQ 永远不会删除历史记录、当前响应带不可变 revision、或者某个无成交合约的法定挂牌起点。因此需把“候选发现”和“严格生命周期发布”分开。

## 为什么不应把六所网页爬虫嵌入用户 CLI

官方渠道存在，但形态、自动化契约和授权并不统一：

- 上期所[日周数据](https://www.shfe.com.cn/reports/tradedata/dailyandweeklydata/)页面提供按日期查询以及导出 Excel/TXT；但本次访问其[数据下载](https://www.shfe.com.cn/reports/tradedata/datadownload/)入口时直接进入 Web 应用防火墙的人机识别页。
- 中金所[历史数据服务](https://www.cffex.com.cn/lssjfw/)明确列出 Level-1、Level-2、一分钟和五分钟历史数据产品，并要求通过资料下载了解申购流程；这不是可以假定为匿名、稳定、可任意再分发的公共 API。
- 广期所官方[交易日历](https://www.gfex.com.cn/gfex/jyrl/list.shtml)导航包含“历史行情”入口，说明官方可提供网页数据，但页面导航不等于有版本化、长期兼容的机器 API 契约。
- 郑商所公开的[信息管理办法](https://www.czce.com.cn/cn/uploadfile/2019/10/30/20191030161831109.pdf)规定交易所发布每日行情字段；公开发布义务仍不等于每家交易所都提供同一种无认证下载协议。

因此，六所连接器会面对 WAF、页面改版、静态文件命名、限流、申购/授权和来源差异。把这些逻辑编译进普通 CLI 会导致：

- 每个用户重复请求官方站点，放大反爬风险；
- parser 修复依赖 SDK 发版，旧客户端持续产生不同结果；
- 同一回测日期在不同时间、网络和页面版本下得到不同 catalog；
- 部分交易所数据产品可能有使用或再分发限制。

对于受 WAF 或授权保护的来源，正确处理是 publisher 暂停发布、改用获授权的官方文件/数据服务或进入人工复核，不应在最终用户机器上尝试规避反爬。

## 推荐的双平面架构

### 1. Discovery plane：TQ 运行时发现

每次在线 fill 或显式 `catalog update`：

1. 连续读取两次 `query_quotes(FUTURE, expired=None)`；
2. roster 稳定后查询全部新增/变化 symbol 的 metadata；
3. 与本地 pinned catalog 比较，生成 `added`、`metadata_changed`、`missing_from_provider` 差集；
4. 新 symbol 立即进入 **下载候选集**，可预取 daily/minute/tick，避免等待权威核验时丢失数据；
5. 但它在严格 timeline 中仍为 `pending_lifecycle_evidence`，不得凭首根行情伪造法定 listing time。

这一路径只使用用户本来就需要连接的 TQ 服务，不增加交易所爬取依赖。

### 2. Publication plane：维护者集中发布

独立、低频 job（例如每个交易日一次）消费 TQ 差集：

1. TQ 稳定 roster 是 primary candidate source；
2. 新增合约优先使用 TQ metadata、首个明确终态的官方/provider 日线，以及官方挂牌公告或获授权 reference feed 交叉核验；
3. 对历史静默、零成交、symbol 规范化冲突等少数异常，才调用交易所 fallback；
4. WAF、格式漂移或来源冲突进入人工 review queue，严格 release fail closed；
5. publisher 保存原始 payload hash、来源 URL、检索时间、HTTP ETag/Last-Modified（若有）、parser 版本和人工裁决记录；
6. 只发布规范化生命周期事实和证据 hash。除非授权明确允许，不随 catalog 再分发交易所原始行情文件。

交易所采集成本由一个受控 publisher 承担，而不是乘以用户数量。旧历史只需一次 bootstrap 和异常补充；正常增量通常只是少量新 symbol，不需要每天重爬全部历史。

### 3. Consumption plane：不可变 catalog

发布端生成：

```text
manifest.json                   # channel -> immutable catalog id
catalogs/<semantic_sha256>.json # normalized lifecycle
evidence/<acquisition_sha256>   # provenance/index, raw bytes may remain private
signatures/<semantic_sha256>.sig
```

catalog 至少包含：

```text
catalog_schema_version
scope
coverage_start
coverage_end                 # strict coverage watermark
source_identities
previous_catalog_sha256
contracts[] {
  symbol
  exchange_id
  product_id
  membership_intervals
  evidence_class
  evidence_sha256
}
semantic_catalog_sha256
published_at
```

消费者行为：

- `latest` 只是在计划创建时解析一次；plan 保存实际 `semantic_catalog_sha256`；
- 回放只读取该 immutable object，不在执行中追随更新；
- 离线环境可以使用二进制/安装包内置的 baseline catalog；
- 请求截止时间超过 `coverage_end` 且无法获取新版时，严格模式明确失败，不能偷偷退化为猜测；
- manifest 和 catalog 分别校验签名、schema 版本、hash chain、scope 与 coverage watermark。

托管可以是项目 release、对象存储或普通静态 HTTPS；它不要求一个复杂在线查询服务。即使默认托管不可用，用户也可镜像同一内容寻址对象，identity 不变。

## 历史 bootstrap 与未来新增应分开处理

### 历史 bootstrap

历史难点是恢复过去可能已从 provider 行情接口静默消失的边角合约。推荐：

1. 以 TQ 当前返回的全历史 roster 为候选全集；
2. 用已经下载的日线快速确定 provider 数据有效起点；
3. 仅对终态不明确、没有日线、expiry 冲突和官方/TQ 差集做交易所/reference 补充；
4. 完成人工复核后冻结 `baseline-v1`，以后永不修改，只追加新 catalog version。

这是一项一次性维护工作。大模型可以辅助定位旧公告和生成 parser，但最终 artifact 必须保留可审计的一手来源与人工裁决，不能把“大模型找到了”本身当作证明。

### 未来新增

未来不需要重演历史 bootstrap：

1. TQ roster diff 发现新 symbol；
2. 下载器立即把它纳入候选下载，保存 `first_provider_seen_at` 和首条实际数据；
3. publisher 取得挂牌/首交易证据后发布 catalog delta；
4. 用户下次更新 catalog 后获得新 lifecycle；已有回测仍由旧 SHA 固定，不受影响。

如项目不愿运营 publisher，长期最佳替代是要求 TQ 提供带 dataset revision、完整性范围、逐合约 lifecycle/coverage 和服务方签名的 coverage manifest。届时 TQ manifest 可以直接成为 publication plane 的输入，交易所 fallback 只处理 dispute，不必自行爬站。

## 严格语义需要分级命名

建议公开区分两种可验证命题：

1. `provider_complete`：TQ 本次稳定 roster 中的全部物理期货；适合下载候选集和运行时新增发现。
2. `exchange_authoritative`：在指定 scope/horizon 内，生命周期由交易所/reference evidence 闭合；适合严格历史 `timeline(active:all)`。

两者都可以严格，但 authority 不同。不能把 `provider_complete` 描述成法定挂牌全集，也不能因为 `exchange_authoritative` 尚未更新就阻止提前下载一个 TQ 新发现的合约。Universe membership 与数据预取目标保持分离，正好解决“严格”和“及时”之间的冲突。

## 推荐实施顺序

1. 保留当前 TQ provider-current acquisition，作为未来增量 discovery；不要新增交易所爬虫到 `tqsdk-cache fill`。
2. 冻结一个经人工核验的历史 `baseline-v1`，随 release 内置并内容寻址。
3. 定义 catalog manifest、签名、coverage watermark、previous-hash 和更新命令；先支持本地文件/静态 HTTPS。
4. 建立维护者 publisher，只对 TQ roster diff 和异常队列读取交易所/reference 来源。
5. 下载路径允许预取 `provider_complete` candidates；严格 timeline 只消费已发布的 `exchange_authoritative` lifecycle。
6. 向 TQ 申请官方 coverage/lifecycle manifest；一旦可用，将其替换为 publisher 的主要闭集证据。

## 对当前问题的直接回答

- **是否必须爬交易所官网？** 若目标是法定生命周期严格性，必须有某个权威/reference 来源，但不必由每个用户运行时爬官网；也不必把 HTML 爬虫作为唯一来源。可以使用官方结构化下载、获授权数据服务、人工取得的公告，或者 TQ 将来提供的完整性 manifest。
- **历史由大模型补齐后，未来怎么办？** TQ `query_quotes(FUTURE, expired=None)` 可以持续发现新合约；中心 publisher 只核验增量并发布小 catalog delta。大模型不是运行时依赖。
- **反爬怎么处理？** 不在 SDK 中绕过。集中低频采集、缓存原始 payload、遵守限流/授权；被 WAF 阻断时 fail closed 并进入人工或授权数据源流程。
- **程序完全离线还能保证最新吗？** 不能。完全离线程序只能严格保证到内置 catalog 的 `coverage_end`；超过水位必须更新 catalog 或拒绝声称严格完整。

## 相关调研

- [Historical Universe 自动 Catalog 调研](./2026-08-30-historical-universe-auto-catalog.md)
- [中国期货合约生命周期与 TQ tick 边界调研](./2026-08-30-authoritative-futures-lifecycle-and-tick-boundaries.md)
