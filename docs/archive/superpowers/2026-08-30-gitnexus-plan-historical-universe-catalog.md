# Historical Universe 自动 Catalog 与统一填充：完整迭代方案

> 状态：2026-08-30 已完成。默认 provider 缺少 authoritative lifecycle/kind bounds 的能力以明确 fail-closed 收口；可执行路径要求 content-addressed authoritative artifacts 和 v3 pinned execution closure。

## 1. Objective

- [verified] 在不改变现有 live/facade current-selector 语义的前提下，让 `tqsdk-cache fill --universe 'physical:all'` 能选择 provider 当前可发现的全部物理期货，并按 tick、minute、daily 各自的数据可得边界填充到用户 cutoff。
- [verified] 让 `tqsdk-cache fill --universe 'timeline(active:all;cont:all;index:all)'` 表达按历史时钟变化的策略可见 Universe；程序负责采集、规范化、校验、哈希和持久化，普通用户不手写 plan。
- [verified] 严格区分两种证据等级：`provider_current_observed` 只服务历史数据下载；`authoritative_lifecycle` 才能生成动态回测 membership。公开源证据不足时保存可诊断结果并 fail closed，不用合约名、到期日或首条行情伪造上市时间。调研依据见 [research note](../../docs/research/2026-08-30-historical-universe-auto-catalog.md)。
- [verified] 收敛 tick/minute/daily 三条 plan fill 到同一 target-resolution 和 execution pipeline，统一报告、进度、取消、退出码、预算和零目标规则。
- [assumed] 本轮只输出实施计划，不修改 Rust；当前工作树中的 minute timeline 实现保留原样，后续实施时先建立回归保护再收敛，不能直接覆盖。

## 2. Current Behaviour

### 2.1 已有能力

- [verified] `UniverseExpression::parse` 只解析 current/static selector，顶层按 `;` 分割；现有 selector 是 `active/main/index/cont/top/symbol/file/product/exchange/auto`。它同时服务 facade、cache 和 relay-compatible live 配置，因此不是 historical wrapper 的安全归属。[universe_expression.rs](../../crates/tqsdk-data/src/universe_expression.rs#L13) [universe.rs](../../crates/tqsdk-data/src/universe.rs#L401)
- [verified] `SessionFuturesUniverseResolver::active_futures` 调用 `query_quotes(FUTURE, expired=false)` 后分批取 metadata，只得到当前活跃集合；`SessionClient` 已有 `query_quotes` 和 `query_symbol_info`，足以采集 provider-current roster，但返回不带不可变历史 catalog revision。[universe.rs](../../crates/tqsdk-data/src/universe.rs#L310) [client.rs](../../crates/tqsdk-session/src/client.rs#L593)
- [verified] `CatalogSnapshot` 当前只有 `complete: bool`，`validate()` 校验版本、complete 和 canonical ordering；`compile_timeline()` 根据 caller-supplied lifecycle 生成 physical/derived membership 与 `physical_listing_starts`。[historical_universe.rs](../../crates/tqsdk-data/src/historical_universe.rs#L144) [historical_universe.rs](../../crates/tqsdk-data/src/historical_universe.rs#L469)
- [verified] `HistoricalUniverseTimeline::prepare` 当前生成 plan v2；`HistoricalUniversePlan::verify` 保留 v1 兼容，并对 v2 校验 physical add 与 `physical_listing_starts` 的关系。[historical_universe.rs](../../crates/tqsdk-data/src/historical_universe.rs#L304) [historical_universe.rs](../../crates/tqsdk-data/src/historical_universe.rs#L376)
- [verified] facade 的 `historical_universe_plan` 会验证 plan、要求 horizon 完全相等，并把 physical add 加入回测 symbol 集合；现有回放层已支持 logical-to-physical 投影，主连 tick 复用物理 cache，minute 通过 underlying segment 保持逻辑 cache key。[lib.rs](../../crates/tqsdk/src/lib.rs#L2904) [history_backtest_replay.rs](../../crates/tqsdk-task/src/history_backtest_replay.rs#L35)
- [verified] `FillDaysArgs` 已支持只传 `--start-day` 时默认到最新 closed trading day；普通 daily/minute/tick fill 已共享 `BacktestHistoryClient` 的 batch/concurrency/timeout 配置。[main.rs](../../crates/tqsdk-cache/src/main.rs#L232) [main.rs](../../crates/tqsdk-cache/src/main.rs#L540) [main.rs](../../crates/tqsdk-cache/src/main.rs#L4028)

### 2.2 已知缺口

- [verified] daily timeline fill 和当前未提交的 minute timeline fill 都直接把 `physical_listing_starts` 映射为 Kline request；plan v1 可以合法 verify，却可能生成零请求并把空 terminal 当成功。[main.rs](../../crates/tqsdk-cache/src/main.rs#L1727) [main.rs](../../crates/tqsdk-cache/src/main.rs#L1815) [historical_universe.rs](../../crates/tqsdk-data/tests/historical_universe.rs#L199)
- [verified] tick timeline fill 走 facade warmup，minute/daily 直接调用 `orchestrate_fill`；三者在 flags、report、progress、signal cancellation、coverage reinspection 和退出码上已经漂移。[main.rs](../../crates/tqsdk-cache/src/main.rs#L1727) [main.rs](../../crates/tqsdk-cache/src/main.rs#L1815) [main.rs](../../crates/tqsdk-cache/src/main.rs#L2510)
- [verified] `CatalogSnapshot::validate` 不重算反序列化对象的 `content_sha256`，而 plan v2 hash 只覆盖 timeline+budget，不能证明 `plan -> catalog -> acquisition bytes` 完整引用链；metadata snapshot loader 已有“重算 canonical body hash 再与 pointer/embedded hash 比对”的可复用范式。[historical_universe.rs](../../crates/tqsdk-data/src/historical_universe.rs#L188) [metadata.rs](../../crates/tqsdk-data/src/backtest_history/metadata.rs#L1634)
- [verified] `DerivedView::{Continuous, Index}` 当前只是当产品 physical count 从 0 变正或从正变 0 时随同加入/移除；它没有表达 cont mapping/ranking identity，也没有区分策略可见 membership 与隐藏的 physical fill dependency。[historical_universe.rs](../../crates/tqsdk-data/src/historical_universe.rs#L135) [historical_universe.rs](../../crates/tqsdk-data/src/historical_universe.rs#L555)
- [verified] 当前 `UniverseBudget` 只有 batch/change 数，没有 symbol、target interval、request 或预计字节预算。[historical_universe.rs](../../crates/tqsdk-data/src/historical_universe.rs#L275)
- [verified] 工作树已有未提交的 minute timeline helper；GitNexus `detect-changes` 将其识别为 3 个 changed symbols、7 个 affected flows、HIGH 风险。该 diff 是后续迭代输入，不是可直接提交的完成态。

## 3. Relevant Architecture

### 3.1 Crate ownership

- [verified] `tqsdk-data` 继续拥有 historical spec/model、artifact codec、canonical identity、validator/reader primitive、membership/dependency compiler、kind-aware target resolver 和 `BacktestHistoryClient` 调度合同。[crate-boundaries.md](../../docs/architecture/crate-boundaries.md#L330) [api-data.md](../../docs/architecture/api-data.md#L95)
- [verified] `tqsdk-cache` 只拥有 operator CLI、认证/参数编排、artifact publish orchestration、progress/report 输出和实际 fill 启动；不得自建第二种 history store 或把新格式藏在 `main.rs`。[crate-boundaries.md](../../docs/architecture/crate-boundaries.md#L392)
- [verified] `tqsdk-session` 继续只提供 direct query/metadata/calendar substrate。首版 acquisition 复用现有 `SessionClient`，不扩展 session public protocol surface。[client.rs](../../crates/tqsdk-session/src/client.rs#L593)
- [verified] `tqsdk-task` 和 `tqsdk` 继续消费已验证 plan；本计划不新增第二棵状态树或旁路通知。只有 derived source acceptance 暴露现有回放缺口时，才在原有 projected source 路径做最小补齐。[history_backtest_replay.rs](../../crates/tqsdk-task/src/history_backtest_replay.rs#L35) [api_contract_s48](../../crates/tqsdk/examples/api_contract_s48_facade_historical_universe.rs#L1)
- [verified] relay 不应理解 historical wrapper。共享 `UniverseExpression` 保持 current/static；新增 historical CLI grammar 位于 `tqsdk-data` 的独立类型中，避免 live `record_universe()` 意外接受全历史 physical 或 timeline。[README.md](../../README.md#L230)

### 3.2 Two explicit semantic lanes

```text
HistoricalFillUniverseSpec
├── ObservedPhysical: physical:all
│   └── provider-current roster + kind-specific availability evidence
└── Timeline: timeline(<whitelisted current selector clauses>)
    └── authoritative lifecycle + pinned calendar/continuous/ranking evidence
```

- [verified] `physical:all` 是下载目标选择，不声明任意过去时点的 active membership。它可以覆盖 expired 和 non-expired physical futures，但报告必须写 `catalog_completeness=provider_current_observed`。
- [verified] `timeline(...)` 是 membership 随 replay clock 的变化规则，粒度和数据流仍由 `--kind`/回测请求表达；Universe 不编码 tick、1m 或 daily。
- [verified] `timeline(active:all;cont:all;index:all)` 的策略可见集合与 fill dependency 必须分开：physical 可见成员由 `active` 选择；continuous/index 是逻辑成员；continuous 依赖 pinned underlying segments，index 使用当前 cache contract 所要求的 native logical source，不能把依赖 physical 自动暴露成策略成员。
- [assumed] v3 首版定义“事后重建的 effective exchange membership”，以 `effective_ns` 驱动回放；它不声称复现当时信息 vintage。若未来需要 as-known-then 回测，必须新增 `known_from_ns`/revision 语义，不能复用本版本身份。

## 4. GitNexus Findings

- [graph] 严格 freshness 已完成：索引从旧 commit 刷新到 HEAD `9418599aebb3490f8f947336d4be0f8a216545e1`，analyzer runner identity 前后一致，重建时启用了 PDG。
- [graph] `HistoricalUniversePlan::verify` upstream impact 为 **CRITICAL**：4 个 direct callers、6 个 affected processes、2 个 modules；直接命中 tick/minute/daily plan fill。因此兼容 reader、v3 verifier 和 execution eligibility 必须分层，不能直接改变 v1/v2 hash 语义。
- [graph] `CatalogSnapshot::compile_timeline` impact 显示 LOW 但 epistemic 为 lower-bound，`receiverTyping=10`；所有计划中的调用方主张均以 CodeGraph/source 补证，不能把 LOW 当成完整影响结论。
- [graph] `UniverseExpression` struct impact 返回 UNKNOWN/零 caller，但 CodeGraph/source 明确显示它由 data、facade、relay 和测试使用；这正是必须避免扩展共享 grammar 的证据。
- [graph] 当前 dirty minute diff 的 `detect-changes --scope all` 为 HIGH，affected flows 包括 minute validation、Kline request、format/namespace 和 progress。实施前必须保留该 diff，先加 characterization tests，再收敛。
- [verified] architecture reviewer 独立审查确认：plan v3/hash chain、typed proof、独立 historical spec、membership/dependency 分离、durable artifact state machine、kind-aware availability、共享 execution pipeline 和 alias compatibility matrix 都是上线前硬门禁。

## 5. PDG Findings

- [graph] `CatalogSnapshot::compile_timeline` 有 38 条 control-dependence edges：`end_ns > start_ns`、scope equality、interval intersection、product membership underflow 和 empty-change filtering 控制最终 batches。v3 compiler 应保留这些 guard，并在其前增加 proof/hash-chain gate，在其后增加 membership/dependency 一致性 gate。
- [graph] `fill_minute_historical_universe_plan` 的 PDG 显示 market/repair flags 和 trading-day/report flags 是独立 early-return guards；plan load/verify 后，request 直接由 `physical_listing_starts` 和 `timeline.end_ns` 构造。共享 executor 应把 flag validation、target resolution、预算检查放到认证和 cache mutation之前。
- [graph] minute helper 的 `requests` data-flow 只有一个 def-use 路径，来源是 plan map、下游进入 `orchestrate_fill` 和 JSON `physical_symbols`。这使“零请求成功”和“错误 start basis”可以在单一 target resolver 层封堵。
- [graph] `HistoricalUniversePlan::verify` 的 PDG 查询无法用 qualified method name直接锚定，简单名又与 cache verify 同名而歧义；本计划不据此宣称 statement-level complete。其 CRITICAL callgraph impact 和源代码分支是负载证据。

## 6. Proposed Changes

### 6.1 独立 historical grammar

- [inferred] 新增 `HistoricalFillUniverseSpec`（名称为提案，可在实现前按仓库命名调整），只由 data/cache historical 路径使用：
  - `physical:all`；
  - `timeline(<inner>)`，inner 复用 `UniverseExpression` 的 clause AST，但通过 whitelist 限制；
  - pinned plan 是执行输入，不是 selector variant。
- [verified] V1 historical grammar 规则：只能是一个 observed physical spec 或一个顶层 timeline wrapper；禁止嵌套；timeline 至少包含一个可证明的 membership selector；`main/top` 在没有权威历史 ranking/mapping 时明确拒绝；unknown selector、空 inner、重复矛盾 include/exclude 在联网前失败。
- [verified] 现有 `UniverseExpression::parse`、`BacktestBuilder::universe`、relay `record_universe` 和 current `--universe 'active:all'` 不改变。cache CLI 先识别 historical spec，否则回落现有 current selector。
- [verified] canonical string、parser/canonicalizer version 和 whitelist version 进入 artifact/plan identity。

### 6.2 Typed proof 与时间语义

- [inferred] 用 typed evidence 取代自动路径上的 `complete=true` 自证：
  - `provider_current_observed`：绑定 source、scope、query params、observed_at、roster-before/after、每批 metadata 完整性、响应/规范化 hash；只能生成 observed download manifest。
  - `authoritative_lifecycle`：额外绑定 requested horizon、每个合约 authoritative membership start/end、calendar identity、continuous/ranking identity、source revision/proof；才能生成 timeline plan。
- [verified] 没有 immutable revision 的官方 current source至少做 roster-before/roster-after 一致性检查；中途 roster 漂移、缺批、重复/冲突 metadata、timeout 或 cancellation 都形成 incomplete acquisition，不进入 fill。
- [verified] 字段明确拆分：`authoritative_membership_start_ns`、`authoritative_membership_end_ns`、`first_available_data_ns_by_kind`、`warmup_start_ns_by_kind`、原始 `expire_datetime` 及其单位/时区/source。
- [verified] 合约名+到期日推断只允许生成带 rule-id 的 probe hint；它不能成为 membership、listing 或裁剪数据的下界。若 probe 无法证明更早数据不存在，不能据此跳过区间。

### 6.3 Catalog/plan v3 identity DAG

```text
source response evidence
        │
        ▼
acquisition_sha256 ──► semantic_catalog_sha256
                              │
calendar/continuous/ranking ──┼──► plan_sha256 (v3)
                              │
                              └──► dependency_set_sha256
                                         │
                         kind availability/warmup
                                         ▼
                              resolved_targets_sha256[kind]
```

- [verified] 新 writer 只发 plan v3；v1/v2 reader 和 `verify()` 保留，已有回测继续可读。
- [verified] v1 可 verify 不代表可 fill：共享 target resolver 对 v1 返回 typed `execution_ineligible_missing_targets`，而不是改变 v1 hash 或假装零请求成功。
- [verified] v2 的 bytes/hash/read/verify 语义保持兼容，但 CLI 默认不再把 caller-supplied listing start 当成已证明的 kind target；只有显式 `--allow-legacy-universe-plan` 才执行，并报告 `legacy_unproven=true`。
- [verified] v3 plan 分开保存 `membership_timeline`、`fill_dependencies`、kind-specific exact targets、compiler identity、canonical spec、budgets 和所有 upstream hashes；execution closure 本身也被 identity 和 plan hash 固定。
- [verified] 每次 load 都重算 canonical body hash，并逐级验证 `plan -> semantic catalog -> acquisition/proof`；unknown version、断链、同 hash 不同 bytes、乱序/重复 canonical content 全部 fail closed。

### 6.4 Membership 与 fill dependency compiler

- [verified] `active` 产生策略可见 physical lifecycle；derived selector 不得隐式添加 physical strategy membership。
- [verified] continuous member pin exact historical underlying segments，并把所需 physical ranges加入隐藏 dependency set；tick 复用 physical cache，minute 延续 logical cache key + underlying segment 的现有合同。[history_backtest_replay.rs](../../crates/tqsdk-task/src/history_backtest_replay.rs#L35)
- [inferred] index member若当前 store需要 native `KQ.i@...` series，则生成 logical native target；除非未来有经过验证的 index aggregator，不得声称它可由 physical catalog 免费派生。
- [verified] `main/top` 需要历史 ranking/mapping artifact；首版没有权威来源时拒绝，而不是用当前排名回填过去。
- [verified] timeline compiler 对 requested scope/horizon、calendar coverage、continuous/ranking coverage、source revision drift、interval overlap/gap和 membership/dependency closure做完整 gate。

### 6.5 Kind-aware shared target resolver

- [inferred] 在 `tqsdk-data` 提供一个稳定的 neutral target API，由三种 cache kind 共用；每个 target 至少含 visible symbol、cache/source symbol、kind、required interval、membership/listing basis、coverage hint、warmup start、dependency provenance、skip/error reason。
- [verified] 每个 kind独立使用 `first_available_data_ns`；tick、60s、native-1d 之间不得复用 availability start。[crate-boundaries.md](../../docs/architecture/crate-boundaries.md#L336)
- [verified] 对用户窗口的实际请求起点按证据计算：有 authoritative listing 时为 `max(requested_start, authoritative_listing_start)`；observed lane 只能使用已证明的 kind-specific first-available boundary。若 provider 没有安全 bounds 查询且 pre-listing 请求失败，`physical:all` 不得启用无界全量填充，必须先完成 bounds spike 或要求 authoritative catalog。
- [verified] timeline required coverage 晚于 membership/warmup requirement 时报告不可补缺口，不能静默裁剪；observed physical lane可以报告 `no_data_before_cutoff`，但不把它解释为“当时未上市”。
- [verified] resolver 在 auth、root write lock和远端 fill前检查：plan eligibility、hash chain、target closure、`start < end`、duplicate/overlap normalization、非空 selector的零 target、symbol/interval/request/estimated-bytes budget。
- [verified] 输出 common `dependency_set_sha256` 和 kind-specific `resolved_targets_sha256`，使三类结果可审计但不强迫不同 source family 共享相同 availability。

### 6.6 Artifact persistence

- [inferred] 建议 namespace：`<cache-dir>/historical-universe-v1/{acquisitions,catalogs,plans}/<sha256>.json`；首版不维护隐式 `CURRENT`，CLI 使用本轮精确路径并在报告中返回，pinned run 显式传 `--universe-plan`。
- [verified] codec、identity、validator、read-only store primitives 位于 `tqsdk-data`；`tqsdk-cache` 只编排 publish。durability 顺序对齐现有 snapshot contract：同 filesystem temp、file sync、atomic rename、parent sync；同 hash 已存在时全文 byte-identical revalidation，否则 collision/corruption。[history-snapshot-manifest.md](../../docs/architecture/history-snapshot-manifest.md#L150)
- [verified] root-scoped writer lock；拒绝 symlink parent、跨 filesystem、无法可靠 atomic rename/fsync 的部署。rename 后 parent sync 失败返回 indeterminate；重跑同一 publish 必须 revalidate 后补 sync，不猜测回滚。
- [verified] 首版 artifact 小且不可变，不做自动 retention/GC；orphan temp 被 reader 忽略，只有未来显式 operator maintenance 才可清理。`--dry-run` 不创建 root/lock/temp，只返回 would-be hashes/paths。[history-snapshot-manifest.md](../../docs/architecture/history-snapshot-manifest.md#L226)

### 6.7 Shared fill execution pipeline

- [inferred] `tqsdk-cache` 先把 current dirty minute helper、daily helper和 tick facade warmup适配到一个内部 pipeline：validate args -> load/acquire artifact -> resolve targets/budget -> inspect coverage -> lazy auth -> `BacktestHistoryClient` fill -> reinspect -> unified report/progress/exit。
- [verified] `BacktestHistoryClient` 仍是 batch/concurrency/timeout/cancellation/progress 的唯一 owner；CLI 不复制 scheduler。[orchestration.rs](../../crates/tqsdk-data/src/backtest_history/orchestration.rs#L42)
- [verified] 三类均支持默认 report path、显式 `--report`、Ctrl-C exit 130、唯一 terminal progress、failed/interrupted report、complete cache hit 无 auth、rows_written/remote_used 一致统计。
- [verified] “合法零行 terminal”与“解析出零 target”分开：前者可提交 provider-confirmed empty coverage；后者对非空 selector/plan永远是 preflight error。

### 6.8 CLI migration

- [verified] 新主参数是 `--universe-plan PATH`；旧 `--universe-timeline PATH` 实现为同一 clap field 的 visible alias，不能出现两个独立可同时设置的字段。
- [verified] 兼容调用 `--universe-timeline PLAN --dry-run` 保持工作；新调用可用 `--universe 'timeline(...)' --universe-plan PLAN`，并验证 plan 内 canonical spec、scope、horizon 与命令一致。仅传 plan 时使用 plan 内已 pin 的 spec/horizon。
- [verified] alias 至少保留一个明确 release 周期；help/README 标为兼容名。JSON/report 保留旧 `universe_timeline` 摘要字段一个版本，并新增 versioned artifact/target identity；若字段语义或必填项改变，提升 unified report schema，不静默改 schema v3。

## 7. Implementation Sequence

### Iteration 0 — 冻结现状并安全收敛当前 minute diff

1. [verified] 保留当前 dirty `crates/tqsdk-cache/src/main.rs`；先补 minute pinned-plan CLI characterization、v1 zero-target、flags/report/progress/cancellation tests。
2. [verified] 证明已提交 daily/tick 和未提交 minute 的当前输出差异，形成兼容矩阵；不在此步引入新 grammar/schema。
3. [verified] 把 plan load、eligibility 和 target-empty 检查抽到最小 shared helper；library `plan.verify()` 仍接受 v1，CLI fill 明确拒绝 execution-ineligible v1。

停止条件：三种 kind 对损坏 plan、v1、v2、空 member、合法无 member window 的结果已区分；minute diff 不再有零请求成功，且现有 current fill tests 不回归。

### Iteration 1 — 冻结语义、proof 和 v3 compatibility contract

1. [verified] 先更新架构文档，固定 observed vs authoritative、effective-vs-known time、identity DAG、membership-vs-dependency 和 v1/v2/v3 matrix。
2. [inferred] 在 tests 中定义 v3 fixtures、canonical bytes、tamper cases和 selector canonicalization golden vectors；此步可先只实现 codec/verifier骨架。
3. [verified] 明确 report schema bump 条件、alias lifespan和旧 JSON 字段保留期。

停止条件：同一输入产生唯一 canonical bytes/hash；任何字段是否进入 semantic/acquisition/plan identity均有书面归属，reviewer 复审无未定义安全字段。

### Iteration 2 — Historical spec、v3 codec 与 artifact store primitives

1. [inferred] 新增独立 historical spec module；`UniverseExpression` 不改语义，timeline inner 只白名单复用其 AST。
2. [inferred] 新增 acquisition/semantic catalog/plan v3 codec、hash-chain loader和 typed proof/error。
3. [inferred] 新增 data-owned content-addressed store primitive与 crash-safe publish；cache 只加薄 orchestration。
4. [verified] 添加 concurrency、collision、indeterminate retry、orphan ignore和 dry-run byte-for-byte不变测试。

停止条件：无网络 fixture 可以 round-trip acquisitions/catalogs/plans；tamper/unknown version/断链均在返回 executable object前失败。

### Iteration 3 — Membership/dependency compiler 与 kind-aware targets

1. [verified] 保留现有 interval/product underflow guards，新增 proof/scope/horizon gates。
2. [inferred] v3 compiler分别生成 visible membership、continuous/index source dependencies和 per-kind availability/warmup。
3. [inferred] 扩展预算到 symbols、membership changes、dependency intervals、remote requests和estimated bytes。
4. [verified] 给 v2 建显式 opt-in 的 legacy target adapter，不改 v2 bytes/hash；v1 只读不可 fill；默认只执行 v3 pinned targets。

停止条件：`cont-only`/`index-only` 不泄漏 physical membership；`active+cont+index` 同时存在且依赖闭合；main/top 无证据时拒绝；三种 kind target identity 可解释。

### Iteration 4 — Pinned plan 三类共享执行路径

1. [verified] 默认只接 proof-pinned v3；v2 必须显式 legacy opt-in，不接无证明自动 catalog。
2. [verified] tick/minute/daily统一单次 plan load、artifact preflight、coverage inspection、lazy auth、scheduler、signal、report和terminal status；三个 timeline helper 的重复流程已移除。
3. [verified] complete cache hit 全程不读取认证；remote failure/cancel不提交未 terminal coverage。

停止条件：同一 fixture plan 在 tick/minute/daily 都产生可审计 dependency/target hash；complete/missing/interrupted/failed 四类结果、exit 0/1/130和报告均一致。

### Iteration 5 — `physical:all` observed acquisition 与全历史下载

1. [verified] 采集 `query_quotes(FUTURE, expired=None)` 的 roster-before，分批 metadata，最后 roster-after；任何漂移/缺项生成 incomplete artifact并停止。
2. [verified] 运行 per-kind availability spike：确认是否有安全 bounds API；若无，则证明宽窗口 fill可返回首条可用数据且不会因 pre-listing start失败。无法证明时停止，不上线 all-contract minute。
3. [inferred] 生成 observed physical fill manifest，按 cutoff过滤：cutoff 前无 provider data 的 symbol标为 typed skip；不声明其未上市。
4. [verified] 名称/expiry估计仅优化 probe顺序；必须用向前/向后验证证明没有被跳过的更早 provider data。

停止条件：报告明确 `provider_current_observed`、source/query hashes、roster stability、每个 symbol/kind start basis和所有 skip/failure；无 symbol请求早于已证明 boundary，也无静默遗漏。

### Iteration 6 — Strict `timeline(...)` 自动编译

1. [verified] 接入 authoritative lifecycle adapter contract；公开 current source达不到 proof时输出 incomplete acquisition 的内存 hash/拟写路径（dry-run）或持久 artifact path（非 dry-run）后 fail closed。
2. [verified] data 层在 complete authoritative acquisition + matching semantic catalog 输入下编译并可 content-addressed persist v3 plan；默认 provider 无该 authority，因此 CLI 的 `--universe 'timeline(...)'` 明确要求先提供 pinned plan，不伪造“一步自动完成”。
3. [verified] calendar exact bytes/range/timezone/normalization、continuous table exact bytes/segments、ranking artifact若被 selector使用，都进入 identity和覆盖 gate。

停止条件：无 authoritative source时绝不生成 executable timeline；有 fixture/授权 source时，相同 pinned artifacts在完全离线环境重放得到相同 membership和target hashes。

### Iteration 7 — CLI rollout、真实验证与文档闭环

1. [verified] 上线 `--universe-plan` 和旧 alias；更新 help、root/data/cache README、架构、validation和 facade contract example。
2. [verified] 先跑 offline 全矩阵，再用显式环境变量执行真实 daily 全 catalog fill；通过后执行 minute 代表性样本和全部 catalog 的可续跑验证。
3. [verified] 第二次相同 cutoff 以 CacheOnly/dry-run复查必须 `remote_used=false`、`rows_written=0`、coverage complete；所有失败 symbol有逐项报告而不是只看进程 exit。
4. [verified] 提交前刷新 GitNexus并运行 `detect-changes --scope all`；partial/truncated/UNKNOWN必须继续查证。
5. [verified] architecture reviewer 的收口项已逐项关闭：authoritative `complete=true`、acquisition/catalog fact equality、v3 embedded execution closure、single-read plan preflight、v2 explicit opt-in、missing-kind-boundary rejection、logical-series availability identity、ancestor symlink/fsync、三类共享 executor、显式 provider scope 与 incomplete metadata audit。

停止条件：Definition of Done 全部满足；否则保持 feature gated/CLI不可见，不降低 proof或coverage标准。

## 8. Test Strategy

### 8.1 Offline unit/contract matrix

- [verified] Parser：current selector byte-for-byte兼容；historical wrapper、canonicalization、nested/empty/unknown/main/top拒绝；relay/facade继续拒绝 historical spec。[universe_selector.rs](../../crates/tqsdk-data/tests/universe_selector.rs#L1)
- [verified] Artifact：乱序、重复、metadata缺项、roster drift、revision mismatch、tamper、unknown version、断链、collision、并发 writer、crash injection、parent sync indeterminate、dry-run不写。
- [verified] Plan：v1 read/verify + fill-ineligible；v2仅显式 legacy opt-in；v3 artifact/hash/execution chain；physical add/target closure；kind-specific availability不串用；budget在auth/write前失败。[historical_universe.rs](../../crates/tqsdk-data/tests/historical_universe.rs#L156)
- [verified] Membership：active add/remove、cont/index独立成员、cont-only/index-only不泄漏 physical、缺 continuous/ranking evidence拒绝、cutoff边界和合法空 horizon。
- [verified] Executor：tick/minute/daily complete hit、remote miss、合法零行 terminal、零 target error、Ctrl-C、remote failure、report write failure、exit code、唯一 terminal progress。[cli.rs](../../crates/tqsdk-cache/tests/cli.rs#L2536)
- [verified] Backtest：同一 timestamp membership先于 market data、退市后停止新开仓、上市后自动加入、continuous underlying切换、index native source、CacheOnly pinned artifact replay。[strategy_backtest.rs](../../crates/tqsdk-task/tests/strategy_backtest.rs#L120)

### 8.2 Targeted gates

```bash
cargo fmt --all --check
cargo test -p tqsdk-data --test universe_selector
cargo test -p tqsdk-data --test historical_universe
cargo test -p tqsdk-task --test strategy_backtest
cargo test -p tqsdk --lib
cargo test -p tqsdk-cache --test cli
cargo check --examples
git diff --check
```

### 8.3 Workspace/public API gates

```bash
cargo test
cargo clippy --examples --all-targets -- -D warnings
cargo check --no-default-features
cargo check --no-default-features --examples
cargo test -p tqsdk-session --no-default-features
cargo check --all-features --examples
cargo test --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
```

### 8.4 User-authorized real-data acceptance

以下命令只在实现完成、凭证由环境变量提供且日志不打印敏感信息时运行：

```bash
# 先验证 provider-current observed daily 全 catalog 到固定 cutoff。
TQ_AUTH_USER=... TQ_AUTH_PASS=... \
cargo run -p tqsdk-cache -- \
  --cache-dir <validation-root> --kind daily fill \
  --universe 'physical:all' \
  --start-day 2010-01-01 --end-day <fixed-cutoff> \
  --symbol-concurrency 4 --report <daily-report.json>

# 再验证 1m；先单品种/小 cutoff，再运行全部 catalog，可中断续跑。
TQ_AUTH_USER=... TQ_AUTH_PASS=... \
cargo run -p tqsdk-cache -- \
  --cache-dir <validation-root> --kind minute fill \
  --universe 'physical:all' \
  --start-day 2010-01-01 --end-day <fixed-cutoff> \
  --symbol-concurrency 4 --report <minute-report.json>

# 相同输入复查：不得联网或写新行。
cargo run -p tqsdk-cache -- \
  --cache-dir <validation-root> --kind minute fill \
  --universe-plan <resolved-observed-manifest-or-plan> --dry-run
```

验收脚本必须逐项核对 catalog symbol 数、target 数、skip/failure reason、每种 start basis、最早持久行、coverage end、remote_used 和 rows_written，不能只看 exit code。

## 9. Risks and Mitigations

| Risk | Severity | Mitigation / stop rule |
| --- | --- | --- |
| 公开源没有历史全集/权威 listing/revision | HIGH | observed lane只下载；timeline保存诊断后 fail closed。没有新 authority不承诺自动严格 timeline。 |
| plan v2被悄然改变导致旧 hash失效 | HIGH | 新字段只进 v3；v1/v2 reader保留，execution eligibility单独判断。 |
| `complete=true` 或 embedded hash自证 | HIGH | typed proof + loader重算 canonical body + 完整引用链。 |
| historical syntax污染 live/relay | HIGH | 新独立 spec；共享 `UniverseExpression` 不接受 wrapper/physical。 |
| cont/index把 hidden physical暴露为 membership | HIGH | v3分离 visible timeline与 fill dependencies；selector-specific tests。 |
| kind之间误用 availability start | HIGH | per-kind typed fields/hash；跨 kind fixture故意设置不同起点。 |
| 名称/expiry推断晚于真实 listing而漏数据 | HIGH | 仅作 probe hint；没有更早数据证明前不得裁剪。 |
| artifact断电/并发/碰撞 | HIGH | root lock、same-fs temp、sync/rename/parent-sync、byte-identical revalidation、indeterminate retry。 |
| 全 catalog 1m成本/时间爆炸 | MEDIUM | preflight预算、报告、并发上限4、可续跑cache、先daily和小样本再全量。 |
| 三条执行路径继续漂移 | MEDIUM | pinned plan shared executor先于auto acquisition；统一terminal/report/cancel contract。 |
| current dirty minute diff被覆盖 | MEDIUM | Iteration 0先保存/characterize；每次提交只包含本迭代授权文件。 |
| report消费者破坏 | MEDIUM | schema变更显式bump；旧字段和alias保留迁移周期。 |

## 10. Files to Modify

### Core implementation

- [verified] `crates/tqsdk-data/src/historical_universe.rs`：v3 plan、membership/dependency model、v1/v2 compatibility adapter、kind-aware target resolver；保留现有 interval/compiler guard。
- [inferred] 新增 `crates/tqsdk-data/src/historical_fill_universe.rs`：独立 historical spec parser/canonicalizer，避免修改 shared `UniverseExpression`。
- [inferred] 新增 `crates/tqsdk-data/src/historical_universe_artifact.rs`：acquisition/proof/catalog codecs、identity DAG、content-addressed store primitives。
- [verified] `crates/tqsdk-data/src/lib.rs`：只重导出经过冻结的 stable data-facing types；store internals保持crate-private。
- [verified] `crates/tqsdk-data/src/universe.rs`：仅抽取可复用的 metadata normalization/batching helper；不向公共 `FuturesUniverseResolver` 增加必实现方法。
- [verified] `crates/tqsdk-session/src/client.rs`：首版不改 public API；acquisition 复用现有 queries。若真实 spike证明必须获得 raw response/revision，作为单独架构变更重新评审，不在本计划中偷偷扩面。
- [verified] `crates/tqsdk-cache/src/main.rs`：CLI spec/alias、acquisition orchestration、共享 fill execution、当前 minute diff收敛。
- [verified] `crates/tqsdk-cache/src/lib.rs`：versioned unified report/summary和必要的operator publish wrapper；artifact codec不放这里。
- [verified] `crates/tqsdk/src/lib.rs`：接受/验证 v3 plan、保持 v1/v2兼容、消费 membership/dependency分离结果。
- [verified] `crates/tqsdk-task/src/history_backtest_replay.rs`：默认只做验证；仅当derived source acceptance暴露缺口时，沿现有 projected source做最小补齐。

### Tests and contracts

- [verified] `crates/tqsdk-data/tests/historical_universe.rs`
- [verified] `crates/tqsdk-data/tests/universe_selector.rs`
- [inferred] 新增 `crates/tqsdk-data/tests/historical_universe_artifact.rs`
- [verified] `crates/tqsdk-cache/tests/cli.rs`
- [verified] `crates/tqsdk-task/tests/strategy_backtest.rs`
- [verified] `crates/tqsdk/examples/api_contract_s48_facade_historical_universe.rs`

### Architecture and user docs

- [verified] `docs/architecture/README.md`、`docs/architecture/crate-boundaries.md`、`docs/architecture/api-data.md`、`docs/architecture/validation.md`
- [verified] 新增 historical catalog/plan v3 专题文档；若复用 snapshot durability合同，链接而不复制全文。[history-snapshot-manifest.md](../../docs/architecture/history-snapshot-manifest.md)
- [verified] 根 `README.md`、`crates/tqsdk-data/README.md`、`crates/tqsdk-cache/README.md`、`docs/README.md`
- [verified] 完成实施后，旧动态 Universe 计划按仓库规则移入 archive；本计划在全部 DoD 完成前保持 current。[previous plan](2026-08-29-gitnexus-plan-dynamic-universe-backtest.md)

## 11. Machine-Readable Context Pack and Evidence Provenance

```json
{
  "schema_version": 1,
  "task": "historical-universe-auto-catalog-and-unified-fill",
  "planning_mode": "deep",
  "plan_form": "full",
  "freshness": "strict",
  "baseline": {
    "head_commit": "9418599aebb3490f8f947336d4be0f8a216545e1",
    "branch": "feat/historical-dynamic-universe",
    "dirty_paths": [
      "crates/tqsdk-cache/src/main.rs",
      "docs/research/2026-08-30-historical-universe-auto-catalog.md"
    ]
  },
  "graph": {
    "gitnexus_index_commit": "9418599aebb3490f8f947336d4be0f8a216545e1",
    "pdg_enabled": true,
    "historical_plan_verify_risk": "CRITICAL",
    "compile_timeline_epistemic": "lower-bound",
    "compile_timeline_receiver_typing_gaps": 10,
    "dirty_detect_changes_risk": "HIGH"
  },
  "contracts": {
    "observed_selector": "physical:all",
    "timeline_selector": "timeline(active:all;cont:all;index:all)",
    "shared_current_universe_expression_unchanged": true,
    "new_plan_writer_version": 3,
    "legacy_plan_readers": [1, 2],
  "legacy_plan_fill": {"v1": "ineligible", "v2": "explicit-opt-in-unproven"},
    "timeline_proof": "authoritative_lifecycle",
    "observed_proof": "provider_current_observed",
    "time_semantics": "effective-membership-not-as-known-vintage",
    "membership_and_fill_dependencies_separate": true,
    "availability_is_kind_specific": true,
    "zero_target_nonempty_selection": "error",
    "dry_run_persists_artifacts": false
  },
  "ownership": {
    "models_codecs_identity_validator_targets": "tqsdk-data",
    "operator_publish_cli_progress_report": "tqsdk-cache",
    "direct_query_substrate": "tqsdk-session",
    "replay_consumption": ["tqsdk", "tqsdk-task"]
  },
  "iteration_order": [
    "stabilize-current-minute-diff",
    "freeze-semantics-proof-v3",
    "historical-spec-codecs-store",
    "membership-dependencies-targets",
    "shared-pinned-plan-executor",
    "observed-physical-all",
    "authoritative-timeline-auto-compile",
    "rollout-real-validation-docs"
  ],
  "hard_stops": [
    "no-authoritative-proof-no-executable-timeline",
    "no-safe-kind-bounds-no-all-contract-fill",
    "no-hidden-dependency-leak-into-membership",
    "no-zero-target-success",
    "no-schema-v3-silent-output-change"
  ]
}
```

以下是 `evidence-provenance.mjs snapshot` 在最终组稿前生成的原样输出：

```json
{"schema_version":2,"head_commit":"9418599aebb3490f8f947336d4be0f8a216545e1","generated_plan_path":"docs/plans/2026-08-30-gitnexus-plan-historical-universe-catalog.md","global_dirty_digest":{"algorithm":"sha256","canonicalization":"gitnexus-evidence-provenance-v2 NUL-framed UTF-8 records","value":"73bcc1804025a070ebde2445f127b2325a191c1df5efbc6da3b42b31aadfd33c"},"cited_path_manifest":"[30]{head_digest:string,index_digest:string,object_kind.head:string,object_kind.index:string,object_kind.worktree:string,object_kind.untracked:string,path:string,rename_from:string?,rename_to:string?,state:string,untracked_digest:string,worktree_digest:string}\nsha256:7cc770dc901ed9cae5aef7337f85061a212c65a1f01fee3e7052370b01c34c6c,sha256:7cc770dc901ed9cae5aef7337f85061a212c65a1f01fee3e7052370b01c34c6c,regular,regular,regular,absent,AGENTS.md,,,clean,absent,sha256:7cc770dc901ed9cae5aef7337f85061a212c65a1f01fee3e7052370b01c34c6c\nsha256:1313d52674b831c6a423a9b3e57243aff4a852be65db3943ad10a8b3899fa13b,sha256:1313d52674b831c6a423a9b3e57243aff4a852be65db3943ad10a8b3899fa13b,regular,regular,regular,absent,README.md,,,clean,absent,sha256:1313d52674b831c6a423a9b3e57243aff4a852be65db3943ad10a8b3899fa13b\nsha256:1d402a4f3de356d3445d95c7d984e9a500fac218c2f750e96e8fb0ffd5481099,sha256:1d402a4f3de356d3445d95c7d984e9a500fac218c2f750e96e8fb0ffd5481099,regular,regular,regular,absent,crates/tqsdk-cache/README.md,,,clean,absent,sha256:1d402a4f3de356d3445d95c7d984e9a500fac218c2f750e96e8fb0ffd5481099\nsha256:2e624cdd43e7d526135e74ae788e5f3f02efe3379bf52b093a84f645b6bbf582,sha256:2e624cdd43e7d526135e74ae788e5f3f02efe3379bf52b093a84f645b6bbf582,regular,regular,regular,absent,crates/tqsdk-cache/src/lib.rs,,,clean,absent,sha256:2e624cdd43e7d526135e74ae788e5f3f02efe3379bf52b093a84f645b6bbf582\nsha256:99752288e76f560855f1fbea34c82c675e667e87f99f3cf4575ca897c67e0c16,sha256:99752288e76f560855f1fbea34c82c675e667e87f99f3cf4575ca897c67e0c16,regular,regular,regular,absent,crates/tqsdk-cache/src/main.rs,,,unstaged,absent,sha256:c4a488f8466747ee3b891659bfcd73245864ffe7c0b922229ea7c89b4f606909\nsha256:949b20b4142d9cc0a07b0361c037ac635ecc2241a40a41be3c5a7aa79cb49681,sha256:949b20b4142d9cc0a07b0361c037ac635ecc2241a40a41be3c5a7aa79cb49681,regular,regular,regular,absent,crates/tqsdk-cache/tests/cli.rs,,,clean,absent,sha256:949b20b4142d9cc0a07b0361c037ac635ecc2241a40a41be3c5a7aa79cb49681\nsha256:3421118fbd7c3c27988db0c194d23b8f6d9146ca5418719eba240152fd8845c5,sha256:3421118fbd7c3c27988db0c194d23b8f6d9146ca5418719eba240152fd8845c5,regular,regular,regular,absent,crates/tqsdk-data/README.md,,,clean,absent,sha256:3421118fbd7c3c27988db0c194d23b8f6d9146ca5418719eba240152fd8845c5\nsha256:11cd54f2123fa3273b6cfc7058cf24162ee71cbf7ec74700f537bcea37fa647b,sha256:11cd54f2123fa3273b6cfc7058cf24162ee71cbf7ec74700f537bcea37fa647b,regular,regular,regular,absent,crates/tqsdk-data/src/backtest_history/metadata.rs,,,clean,absent,sha256:11cd54f2123fa3273b6cfc7058cf24162ee71cbf7ec74700f537bcea37fa647b\nsha256:7684b6d7c922c539c8d2a770d55d8816d2a75837dcf5ddfbacd706b2b57f64ee,sha256:7684b6d7c922c539c8d2a770d55d8816d2a75837dcf5ddfbacd706b2b57f64ee,regular,regular,regular,absent,crates/tqsdk-data/src/backtest_history/orchestration.rs,,,clean,absent,sha256:7684b6d7c922c539c8d2a770d55d8816d2a75837dcf5ddfbacd706b2b57f64ee\nsha256:ccc8d97f3277ea352a573af423061feecdaf2c7df7dadbeb1533c0ce96e63d68,sha256:ccc8d97f3277ea352a573af423061feecdaf2c7df7dadbeb1533c0ce96e63d68,regular,regular,regular,absent,crates/tqsdk-data/src/client/cont_quotes.rs,,,clean,absent,sha256:ccc8d97f3277ea352a573af423061feecdaf2c7df7dadbeb1533c0ce96e63d68\nsha256:5e4534780dca4028873e087da09317791e98a2467118b18adad9758084e80fd9,sha256:5e4534780dca4028873e087da09317791e98a2467118b18adad9758084e80fd9,regular,regular,regular,absent,crates/tqsdk-data/src/historical_universe.rs,,,clean,absent,sha256:5e4534780dca4028873e087da09317791e98a2467118b18adad9758084e80fd9\nsha256:38f35d3446b6a34ecc0573a8cf42a0769aeab6eed81d30d673687b6e1e5b9aef,sha256:38f35d3446b6a34ecc0573a8cf42a0769aeab6eed81d30d673687b6e1e5b9aef,regular,regular,regular,absent,crates/tqsdk-data/src/lib.rs,,,clean,absent,sha256:38f35d3446b6a34ecc0573a8cf42a0769aeab6eed81d30d673687b6e1e5b9aef\nsha256:c79c42ef536d025cb6141ee15c537b4e801d78f33fa6ac182ffd2adde27c7464,sha256:c79c42ef536d025cb6141ee15c537b4e801d78f33fa6ac182ffd2adde27c7464,regular,regular,regular,absent,crates/tqsdk-data/src/universe.rs,,,clean,absent,sha256:c79c42ef536d025cb6141ee15c537b4e801d78f33fa6ac182ffd2adde27c7464\nsha256:042f9722ca1f0e08660682f82d56fdbb5d2fbd8e975bc1a93c5fdeb30649ec87,sha256:042f9722ca1f0e08660682f82d56fdbb5d2fbd8e975bc1a93c5fdeb30649ec87,regular,regular,regular,absent,crates/tqsdk-data/src/universe_expression.rs,,,clean,absent,sha256:042f9722ca1f0e08660682f82d56fdbb5d2fbd8e975bc1a93c5fdeb30649ec87\nsha256:dc1e86bce7b3ffb2f4a9f9e85b78dd11378e0a37c3a62d8b5da1bd8a590538b3,sha256:dc1e86bce7b3ffb2f4a9f9e85b78dd11378e0a37c3a62d8b5da1bd8a590538b3,regular,regular,regular,absent,crates/tqsdk-data/tests/historical_universe.rs,,,clean,absent,sha256:dc1e86bce7b3ffb2f4a9f9e85b78dd11378e0a37c3a62d8b5da1bd8a590538b3\nsha256:c399169d9c2fd311d28970664173b0a864944b2471152daf381f8e8ba443681c,sha256:c399169d9c2fd311d28970664173b0a864944b2471152daf381f8e8ba443681c,regular,regular,regular,absent,crates/tqsdk-data/tests/universe_selector.rs,,,clean,absent,sha256:c399169d9c2fd311d28970664173b0a864944b2471152daf381f8e8ba443681c\nsha256:ef101aaaba645395f9e34f933040b2b06f2d1b54cb793443592c52c5ae7a8f9a,sha256:ef101aaaba645395f9e34f933040b2b06f2d1b54cb793443592c52c5ae7a8f9a,regular,regular,regular,absent,crates/tqsdk-session/src/client.rs,,,clean,absent,sha256:ef101aaaba645395f9e34f933040b2b06f2d1b54cb793443592c52c5ae7a8f9a\nsha256:78e8811989eb727867b326f94d1fcd385eb516ca8e2b76fe88aeb56269569a7d,sha256:78e8811989eb727867b326f94d1fcd385eb516ca8e2b76fe88aeb56269569a7d,regular,regular,regular,absent,crates/tqsdk-task/src/history_backtest_replay.rs,,,clean,absent,sha256:78e8811989eb727867b326f94d1fcd385eb516ca8e2b76fe88aeb56269569a7d\nsha256:3b28620e7b767bef1b9713c45368673d3e25e254dc3073bf015d6a9a6e9d3f26,sha256:3b28620e7b767bef1b9713c45368673d3e25e254dc3073bf015d6a9a6e9d3f26,regular,regular,regular,absent,crates/tqsdk-task/tests/strategy_backtest.rs,,,clean,absent,sha256:3b28620e7b767bef1b9713c45368673d3e25e254dc3073bf015d6a9a6e9d3f26\nsha256:b63172e5d3cf6ab871f56e139fa3a7997d7cf7a9852aa5fcebcf37f7e932e2fa,sha256:b63172e5d3cf6ab871f56e139fa3a7997d7cf7a9852aa5fcebcf37f7e932e2fa,regular,regular,regular,absent,crates/tqsdk/examples/api_contract_s48_facade_historical_universe.rs,,,clean,absent,sha256:b63172e5d3cf6ab871f56e139fa3a7997d7cf7a9852aa5fcebcf37f7e932e2fa\nsha256:90defb74a0d88afeac44a8a2e190476ec6d014982570613c3df8c535db48ef90,sha256:90defb74a0d88afeac44a8a2e190476ec6d014982570613c3df8c535db48ef90,regular,regular,regular,absent,crates/tqsdk/src/lib.rs,,,clean,absent,sha256:90defb74a0d88afeac44a8a2e190476ec6d014982570613c3df8c535db48ef90\nsha256:3704a8075597ab30456f000c73499468d137dd915ab24f2ef8985c425dc918c7,sha256:3704a8075597ab30456f000c73499468d137dd915ab24f2ef8985c425dc918c7,regular,regular,regular,absent,docs/README.md,,,clean,absent,sha256:3704a8075597ab30456f000c73499468d137dd915ab24f2ef8985c425dc918c7\nsha256:41e5fa9138fb5bbd427ff3871864497c7755ba6ce490fe1a23d6f32f7604a680,sha256:41e5fa9138fb5bbd427ff3871864497c7755ba6ce490fe1a23d6f32f7604a680,regular,regular,regular,absent,docs/architecture/README.md,,,clean,absent,sha256:41e5fa9138fb5bbd427ff3871864497c7755ba6ce490fe1a23d6f32f7604a680\nsha256:55a587529488467154ba7894ea818a8116a7e9de4cfcfea91817944b49b74851,sha256:55a587529488467154ba7894ea818a8116a7e9de4cfcfea91817944b49b74851,regular,regular,regular,absent,docs/architecture/ai-workflow.md,,,clean,absent,sha256:55a587529488467154ba7894ea818a8116a7e9de4cfcfea91817944b49b74851\nsha256:05c484aa87742a5d788e8efa4a046ab51fe7b2272e81d19fa15c32e1a42c5b76,sha256:05c484aa87742a5d788e8efa4a046ab51fe7b2272e81d19fa15c32e1a42c5b76,regular,regular,regular,absent,docs/architecture/api-data.md,,,clean,absent,sha256:05c484aa87742a5d788e8efa4a046ab51fe7b2272e81d19fa15c32e1a42c5b76\nsha256:07488d245126ebc58489ed44e003f04dfa69f394a8686932ff925a90a75dc112,sha256:07488d245126ebc58489ed44e003f04dfa69f394a8686932ff925a90a75dc112,regular,regular,regular,absent,docs/architecture/crate-boundaries.md,,,clean,absent,sha256:07488d245126ebc58489ed44e003f04dfa69f394a8686932ff925a90a75dc112\nsha256:bf09c1311c8620a71b087c7c6e6f443aab79a45590de9496fe2e9b47b451cc25,sha256:bf09c1311c8620a71b087c7c6e6f443aab79a45590de9496fe2e9b47b451cc25,regular,regular,regular,absent,docs/architecture/history-snapshot-manifest.md,,,clean,absent,sha256:bf09c1311c8620a71b087c7c6e6f443aab79a45590de9496fe2e9b47b451cc25\nsha256:f48f10bae512e38f04cbadbb790d853b65e0ff397d93de0816f9716cbe0ce4b1,sha256:f48f10bae512e38f04cbadbb790d853b65e0ff397d93de0816f9716cbe0ce4b1,regular,regular,regular,absent,docs/architecture/validation.md,,,clean,absent,sha256:f48f10bae512e38f04cbadbb790d853b65e0ff397d93de0816f9716cbe0ce4b1\nsha256:2bd4c3171b2719f1f6f9ce64e8504ed045d2dfb100ec005830fe0ec6b91c8aa5,sha256:2bd4c3171b2719f1f6f9ce64e8504ed045d2dfb100ec005830fe0ec6b91c8aa5,regular,regular,regular,absent,docs/plans/2026-08-29-gitnexus-plan-dynamic-universe-backtest.md,,,clean,absent,sha256:2bd4c3171b2719f1f6f9ce64e8504ed045d2dfb100ec005830fe0ec6b91c8aa5\nabsent,absent,absent,absent,absent,regular,docs/research/2026-08-30-historical-universe-auto-catalog.md,,,untracked,sha256:55aedd4fc8a46e0ed6449d2eb49a3533cb9c5e19f7ba9a15f4a618456326db70,absent\n"}
```

## 12. Assumptions and Open Questions

- [assumed] `physical:all` 的首版目标是“provider 当前仍可发现的全部 physical futures”，不是数学意义上交易所历史全集；报告必须把该限定展示给用户。
- [verified] 当前公开 source缺少 authoritative listing/first-trading/revision，因此 strict `timeline(...)` 自动路径在默认 provider上预计 fail closed。真正解除该 stop需要第一方历史 catalog authority或用户提供的已验证 pinned artifacts。
- [assumed] effective membership 足以满足当前回测需求；不实现 as-known vintage。若用户需要模拟“当时能知道什么”，必须作为后续独立设计。
- [assumed] `--start-day` 保持普通 fill 的 lower-bound语义，per-contract实际起点为该 lower bound与已证明 listing/availability的较晚者。若产品要求无论用户 start多晚都回填到 listing，应新增明确 `--full-lifecycle`，不能悄然忽略用户窗口。
- [verified] observed lane的 first-available discovery是否能在现有 server-backtest source上低成本且无 pre-listing错误地完成，必须在 Iteration 5 spike实测；这是 `physical:all` minute上线前唯一未决技术能力。
- [assumed] 首版不做 artifact GC/current pointer；artifact体积很小且显式路径更可复现。若长期数量成为问题，再按 snapshot manifest合同增加独立 operator maintenance。
- [verified] index replay当前按 native logical source处理；若未来要本地合成指数，需要另一个有成分、权重、结算规则和版本身份的设计，不能由本 catalog plan顺手推断。

## 13. Definition of Done

1. [verified] 现有 current/static `UniverseExpression`、facade/live/relay和普通 `--universe` 行为无回归；historical syntax由独立 spec拥有。
2. [verified] `physical:all` 能稳定采集 provider-current 全 physical roster，保存/报告 acquisition identity，按 kind-specific已证明边界填充到 cutoff；每个 skipped/failed symbol都有 typed reason。
3. [verified] `timeline(...)` 只有 authoritative lifecycle、calendar、continuous/ranking和hash chain全部通过才生成 plan v3；默认公开源不足时留下诊断并 fail closed。
4. [verified] plan v1/v2继续可读/可验证；v1 fill明确不可执行，v2只在显式 legacy opt-in 后执行并报告 unproven；新 writer只发v3，默认 CLI 只执行 v3。
5. [verified] membership和hidden dependencies完全分开；`cont-only`/`index-only`不泄漏 physical，`active+cont+index`在回测时同步加入/移除且数据源可用。
6. [verified] tick/minute/daily共享 target resolver和execution pipeline；报告、progress、cancel、exit、lazy auth、coverage reinspection语义一致。
7. [verified] 非空 selection/plan绝不以零 target成功；合法 provider零行与无目标有不同typed outcome。
8. [verified] artifact publish满足 hash重算、byte-identical collision check、atomic durability、concurrency和dry-run零写入合同。
9. [verified] offline targeted/workspace/public API gates全部通过；真实 daily全 catalog和minute全 catalog验证完成，第二次CacheOnly复查无remote、无新增rows、无未解释缺口。
10. [verified] 所有受影响权威架构文档、README、validation和contract example同轮更新；提交前 GitNexus detect-changes非partial/non-truncated，所有HIGH/CRITICAL/UNKNOWN均有处理记录。
