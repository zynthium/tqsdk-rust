---
name: tqsdk-rust
description: Use when 用户需要 Rust 量化 SDK 或 TQSDK Rust 能力：实时行情/quote/盘口/K线/tick、品种/合约列表、主连/连续合约、期权链、合约规格、metadata/direct query、历史数据下载/缓存/CSV/Greeks、回测/实盘共享 tick cache、交易账户/下单/撤单/订单状态、TargetPosTask/风控/多账户/策略执行、低延迟交易柜台/trading desk、fan-out/event consumers、replay/backtest/live-sim-replay；也用于智能体需要实时或历史量化数据、交易执行 substrate、交易柜台能力时，即使未明确提到 TQSDK。
---

# TQSDK Rust

使用本 skill 时，把 TQSDK Rust 请求映射到正确的入口、crate、调用形态和最小代码，同时保持 workspace 的 crate 边界。

## 先路由请求

只读取当前问题需要的 reference。

1. 每个请求先读 [references/scenario-router.md](references/scenario-router.md)。按用户想持有或消费的对象分类，不要按用户第一个提到的 API 名分类。
2. 用户不确定该用哪个 crate、或问题涉及依赖写法和 crate 边界时，读 [references/crate-selection.md](references/crate-selection.md)。
3. 写示例代码或修复示例编译错误前，读 [references/code-patterns.md](references/code-patterns.md)。
4. 用户要求按角色给示例、完整覆盖场景、场景契约、public API 证据，或问“每类用户应该怎么做”时，读 [references/scenario-contracts.md](references/scenario-contracts.md)。
5. 策略循环、事件总线、研究、回放、测试、低延迟柜台工作流，读 [references/quant-workflows.md](references/quant-workflows.md)。
6. 凭证、权限、实盘交易、模拟、下单副作用、风控、live smoke test，读 [references/safety-and-operations.md](references/safety-and-operations.md)。
7. 只有用户要求新建独立 starter project 时，才使用 [scripts/new-tqsdk-rust-project.py](scripts/new-tqsdk-rust-project.py) 和 [assets/templates/tq-strategy-loop](assets/templates/tq-strategy-loop)。明确要求 Python-style wait starter 时才选 `--template wait-quote-loop`。

## 核心规则

- 写代码前先选择能覆盖场景的最高层入口。普通策略、目标持仓和轻量历史访问优先从默认 `tqsdk` crate 开始；明确需要内部能力时再下钻。
- 官方 Python TqSdk 行为是语义参考，但 Rust 要映射到 crate 归属，不要重建 Python 单体 `TqApi`。
- 默认 facade 放在 `tqsdk`；one-shot query 放在 `tqsdk-session`，Python-style live ref 放在 `tqsdk-wait`，执行工具放在 `tqsdk-task`，离线/历史数据放在 `tqsdk-data`。多消费者 event/fan-out 是调用方基于 `tqsdk-session + RuntimeReader/UpdateCursor` 自建的集成层。
- 默认 facade 回测入口统一为 `.backtest(start_ns, end_ns)`：默认使用共享持久 tick cache（`$HOME/.tqsdk/data_series_1`，可用 `TQSDK_HISTORY_CACHE_DIR` 覆盖）和 `RemoteOnMiss`。已知或静态解析 symbol 的 cache hit 不需要 auth；只有缺 tick range 时才需 auth，并通过官方 server-side backtest stream 补洞后在本地 `TqSim` 回放。`.cache_dir(...)`、`.cache_store(...)` 或 `.market_cache(...)` 覆盖默认 cache；只有 `.disabled_cache()` 才是纯官方服务端回测且不落盘。
- 用户要按时间区间取得历史 Tick / Kline rows，且明确优先回测或回测缓存时，优先用 `tqsdk::advanced::data::BacktestHistoryClient` + `BacktestHistoryRequest`，不要先路由到 `DataClient`。`BacktestHistoryPolicy::RemoteOnMiss` 先读共享回测缓存，缺口才走官方 server-side backtest stream 并写回；完整缓存后改用 `BacktestHistoryPolicy::CacheOnly`。`Tq::backtest(...)` 留给策略回放，`DataClient` 留给显式 generic 历史下载、CSV/研究工作流，或回测缓存合同不覆盖的来源。
- 不要再生成或推荐 `server_backtest(...)`。该 public 兼容 alias 已删除；旧代码统一迁移为 `.backtest(start_ns, end_ns)`，需要本地严格只读时加 `.cache_only()`，只有有意禁用缓存时才加 `.disabled_cache()`；自定义数据源使用 `.replay_backtest(...)`。
- `DataClient` 的 history-series cache 默认 opt-in；这不改变 facade `.backtest(...)` 默认共享 tick cache 的语义。持久化 live tick 必须显式使用 `MarketCachePolicy` / `Tq::record_ticks(cache_dir, symbols)`；`HistorySeriesCache` 仍只服务 offline data-series。
- `MarketCachePolicy` 是默认 facade 维护“live 增量记录 + 回测共享缓存”的首选入口：`MarketCachePolicy::new(cache_dir).record_ticks(symbols)` 或 `.record_universe(expression)?` + `TqBuilder::market_cache(policy)` 会在 live `connect()` 后启动 tick recording，也会给 `.backtest(...)` 提供默认 cache 目录和 symbol 集合。
- 共享期货 universe selector 在默认 facade 的 `.backtest(...).universe(...)`、实时 `quotes_universe(...)` 和 `MarketCachePolicy::record_universe(...)` 上复用同一套解析语义；动态 selector 需要 live/session 或显式 auth，静态 `symbol:...` selector 可本地解析。
- `Tq::record_ticks(...)` 仍是默认 facade 的显式运行时 tick-only live cache recorder：只记录声明的 symbol，复用 `BacktestTickCache`，由 `next()` / `wait_update()` 驱动；连续 rows 每 symbol 最多 `128` 行或约 `250 ms` 批量提交，首批、跳号和正常 `Tq` 销毁时强制 flush。coverage 只在 rows 已提交且 tick id 连续时推进；跳号、断线或异常退出前未提交的尾部保留 coverage 缺口，后续用 `.warmup()` / `.remote_on_miss()` 补齐。
- `record_ticks_health()` 暴露累计写入、最近 flush、per-symbol last id 和 gap 状态；`recorded_market_cache_policy()` 可从当前 recording health 派生补洞用 policy，但补洞仍要用户显式重新提供 auth，不隐式复用 live session 明文凭证。
- `tqsdk-data` 的 `LiveTickCacheWriter` 是纯数据层 writer，接收已解码 tick rows 并写共享回测缓存；它不拥有 session、订阅、后台线程或跨进程协调。
- `HistorySeriesCache` 只服务 offline `get_*_data_series` / cache-only `read_*_data_series` / scan / maintenance；不要使用它作为 live serial 缓存或外部最新行情 API。
- 官方 Python serial 的 `id` 列来自序列路径 key / 行序号，不要求 raw Kline/Tick payload 自带 `id`；Rust 解码应保持 path-key id 兼容。
- 只有低层 runtime、自定义 facade、adapter、command 状态机、commit/cursor、hot-path `RuntimeReader` 才使用 `tqsdk-core`。
- 所有可见状态变化都必须经过 runtime commit 和 `RuntimeReader` / `UpdateCursor`；不要发明私有状态树、本地订单 overlay 或旁路通知。
- live/network 示例默认需要 Tokio、凭证、行情权限，以及明确的交易权限。
- 优先使用 `futures_market()`、`stock_market()`、`trade_target_tqkq()`、`enable_query()` 这类命名 builder，不要使用裸 bool route flag。
- 下单示例默认使用模拟/TqKq 风格；只有用户明确要求实盘接入并接受副作用时，才给 real-account 集成。
- 精确 API 形状重要时，先检查目标 crate README 和 `crates/*/examples/api_contract_sXX_*.rs`，再定稿代码。

## 历史 Tick 缓存补齐

- 区间历史 rows 的回测优先路径：`BacktestHistoryClient::builder(cache_dir)` 配置 `RemoteOnMiss` 和 `.auth_env()`，再以 `BacktestHistoryRequest::tick(...)` / `.kline(...)` 调用 `query()`；调用方可消费 chunk/terminal event，或对单请求使用 `collect()`。它命中同一份回测缓存，只有缺口才使用官方回测流并持久化；预热或成功读取后，后续 reader 用 `CacheOnly`。这不是策略循环；策略回放继续用 `.backtest(...)`。
- 按需补齐：单个策略可直接使用默认 `.backtest(...)`；完整缓存直接本地回放，缺口由默认 `RemoteOnMiss` 补齐。这个路径不使用 `tq_dl` / 专业历史下载权限，但实际远端补数需要 `.auth_env()?`。
- 预热作业：对固定 cache root 和明确的 symbol/universe，使用 `.remote_on_miss().warmup().await?`。它只填每个 symbol 的 `missing_ranges`，连续 tick id 的成功 slice 才提交 coverage；失败或未确认尾部保留缺口，下一次 warmup 可继续补齐。
- operator CLI：固定 root 的 inventory、closed-day fill、report-bound verify 或 TQBN doctor 使用可选
  `tqsdk-cache` binary（`cargo run -p tqsdk-cache -- --help`）。它只编排已有 facade/data cache
  合同，不是 relay、守护进程或 custom store；normal fill 的 cache hit 不需 auth，缺口才使用
  `TQ_AUTH_USER` / `TQ_AUTH_PASS`。默认输出摘要；需要脚本 JSON 时显式使用
  `--output-format json`，stderr 才是进度；
  `fill --dry-run` 不加锁、不写文件、不远端补数，V1 拒绝 current open day。operator 需要最近
  N 日时优先用 `fill --last-trading-days N --calendar auto`；它先复用 root 的
  `meta/trading-calendar-v1.json`，日历只用于 selector/进度，不是 coverage truth。`--calendar off`
  拒绝该 selector，`required` 禁止 fallback；`--progress off` 保持 stderr 安静，普通模式显示当前
  physical symbol、trading day 和完整分区日计数。
- “最近 N 个交易日”必须先用官方交易日历确定窗口，不要把 N 个工作日当作交易日。只填到最后一个已结束的交易日；对 SHFE 贵金属等夜盘品种，通常从首个交易日前一日 `18:00:00` CST 起，到最后交易日 `15:00:01` CST 止。其他市场以合约 `trading_time` 为准。按日分区的休市日空覆盖是正常结果，不能仅凭“某日没有 tick”判定失败。
- 调用方要观察远端 warmup 计划时使用 `.on_remote_fill_telemetry(...)`，而不是复制 scheduler。
  每个 physical range 完成 coverage inspection 后都会给出累计 `Inspecting`（已检查/总范围、命中、
  缺口及当前范围）；`PlanReady` 随后提供 logical/physical/missing-range 计划；后续 lifecycle event 带
  physical symbol、batch、cursor、retry/split 状态。该 callback 位于 cache inspection 和远端填充路径，
  只能做快速内存 reducer，不得同步打印、阻塞或做网络 I/O。
- 开始远端预热前先检查目标 root 的已有 coverage，并只运行一个远端 writer。`RemoteOnMiss` 会跳过完整覆盖并只写缺口；除非用户明确要求刷新或清理，否则不要删除 `.tqbn`、使用 `RefreshAll`，或把 benchmark 的临时 cache 当成共享 cache。
- 新建或 compact 后的日分区会有内部 coverage index chain，加速 `inspect_cache()` / `CacheOnly` 的完整性检查；旧文件、盘中尾部追加或任一 index/coverage 校验异常会保守回退完整扫描。调用方不需要也不得手工创建、修改 index。
- 预热后必须用同一 symbol/universe 和时间窗口运行 `CacheOnly`，再实际回放 tick 流。以 `missing = 0`、symbol coverage complete 和可读取的 replay tick 为成功标准；日文件数量和远端写入行数只用于辅助诊断。
- 多策略/生产：每个 cache root 只安排一个定时远端 warmup owner；策略实例复用同一 root 并用 `.cache_only()`。文件锁可串行化 TQBN 写入，但不是跨进程远端补数调度器，多个 `RemoteOnMiss` 实例仍可能重复下载。
- 实时增量：用 `MarketCachePolicy::record_ticks(...)` / `.record_universe(...)` 加 `.market_cache(...)`。只记录 policy 声明的 symbol，且必须持续 `next()` / `wait_update()`；首次初始化或失败重扫之外，每个 update 只解码变更集命中的 tick serial，连续 rows 每 symbol 按 `128` 行或约 `250 ms` 批量写入，首批、跳号和正常对象销毁会 flush。断线、跳号和异常退出前未提交尾部留下的 coverage gap 交给后续 warmup 补齐。
- 不要手工改写 `.tqbn`，也不要把 relay 当作 canonical historical-cache owner。缓存检查、清理和补齐走 `.inspect_cache()`、`.purge_cache_symbols()`、`.warmup()` 或 `.remote_on_miss()`。
- tick cache 与 native K 线 cache 分开：`duration <= 60s` K 线从 tick 合成；`duration > 60s` 使用 `HistorySeriesCache` 的另一条补齐路径。

完整示例见 [references/code-patterns.md](references/code-patterns.md) 的 `Shared Live/Backtest Tick Cache`。

## 常见错误

- 不要用 `tqsdk-wait` 回答 direct-query 问题；使用 `tqsdk-session` 或 `api.session()`。
- live facade 或调用方 event consumer 里不要为了 metadata 再建第二个 client；复用 shared session。
- 不要把历史下载当作 live ref；使用 `tqsdk-data`。如果用户要维护指定合约的 live/backtest 共享 tick 缓存，优先使用 `MarketCachePolicy` + `.market_cache(...)`，运行中临时开启可用 `Tq::record_ticks(...)`；如果要持久化 live K 线、任意 row batch、commit events、跨进程 WAL 或审计流，使用调用方自己的 sidecar，不要把旧 Python mmap history cache 接进 live 热路径。
- 不要把官方服务端回测写成 `server_backtest(...)` 或 `TqBuilder::server_backtest(...)`；唯一默认 facade public 入口是 `.backtest(start_ns, end_ns)`，它默认 cache-backed + `RemoteOnMiss`。只有显式 `.disabled_cache()` 才改为纯 remote market stream。
- SDK runtime 不内置同进程监控面板或 cache manager；固定 root 的历史 tick 运维使用可选
  `tqsdk-cache` CLI，程序内集成仍使用显式 data/facade API 或调用方 sidecar，relay dashboard
  仅随 relay 进程可用。
- 普通用户示例不要直接从 sibling crate taxonomy 起步；先尝试 `tqsdk::prelude::*` / `Tq::futures()`，除非用户明确要 wait、session、task、data、core 或自建 event/fan-out consumer 的完整 surface。
- 普通用户示例不要从 `tqsdk-core` 起步，除非用户明确要 runtime internals。
- typed ticket、ref 或 status helper 已存在时，不要发明本地订单 overlay，也不要解析 status 字符串。
- 不要用字符串或 adapter-local 判断绕过 `record_command_status()` 和 runtime command lifecycle。
- 示例里不要隐藏凭证、权限或实盘订单副作用。
- 回答用法问题时，不要跨 crate 移动 direct query、downloader、task 或 research 语义。

## 回答风格

- 开头先说明使用哪个入口或 crate 以及原因：默认 facade、Python-style live ref、caller-owned event/fan-out、one-shot query、task execution、offline rows 或 runtime substrate。
- 优先给和当前 example 匹配的短 Rust snippet，不要写大段伪代码。
- 覆盖用户角色或宽工作流时，引用 `scenario-contracts.md` 中对应的 `api_contract_sXX_*.rs` 示例。
- 点名用户下一步应调用的具体 API。
- 如果 Rust 答案刻意不同于 Python TqSdk，要说明原因是 Rust workspace 有默认 `tqsdk` facade，并把高级能力拆成了 `session`、`wait`、`task`、`data` 和低层 runtime substrate；多消费者管线留给调用方组合。
- 代码会下单、撤单、使用实盘账户或依赖付费行情权限时，先说明安全门槛。
- 请求不明确时，只问一个形状问题：“你需要一个带 ref 的单 live loop、多个事件消费者、one-shot query、task/order 抽象、历史 rows，还是 runtime commits？”

## 验证闭环

- 只回答用法或短 snippet 时，说明已核对的 crate README、contract example 或 reference；没能核对时明确标注。
- 修改本仓库 Rust API、examples 或 contract 后，至少运行 `cargo fmt --all --check` 和 `cargo check --workspace --examples`；涉及行为时再运行相关 `cargo test`。
- 修改本 skill、脚本或模板后，运行 `git diff --check`；模板或脚本变更还要把 starter project 生成到临时目录，并在可行时运行 `cargo check --manifest-path <tmp>/Cargo.toml`。
- live、交易或下单 smoke test 只有在用户显式提供凭证、权限和副作用许可后才运行；否则说明未跑 live 验证。

## 项目脚手架

从内置 asset template 创建默认 `tqsdk` quote loop 项目：

```bash
python3 scripts/new-tqsdk-rust-project.py ./my-tqsdk-app \
  --sdk-source git \
  --sdk-value https://github.com/OWNER/tqsdk-rust \
  --symbol SHFE.au2602
```

本地开发使用 `--sdk-source path --sdk-value /path/to/tqsdk-rust`；crate 发布后可使用 `--sdk-source version --sdk-value <version>`。
明确需要 Python-style wait facade starter 时，加 `--template wait-quote-loop`。
