---
name: tqsdk-rust
description: Use when 用户需要 Rust 量化 SDK 或 TQSDK Rust 能力：实时行情/quote/盘口/K线/tick、品种/合约列表、主连/连续合约、期权链、合约规格、metadata/direct query、历史数据下载/缓存/CSV/Greeks、交易账户/下单/撤单/订单状态、TargetPosTask/风控/多账户/策略执行、低延迟交易柜台/trading desk、fan-out/event consumers、replay/backtest/live-sim-replay；也用于智能体需要实时或历史量化数据、交易执行 substrate、交易柜台能力时，即使未明确提到 TQSDK。
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
- history cache 默认关闭；只有显式 `DataClientBuilder::history_cache_enabled(true)` 或显式 `HistorySeriesCache::open(...)` cache-only reader 才生效。
- `tqsdk-data` 不提供 live event 写 Python-compatible mmap history cache 的 bridge；SDK 不为 live 热路径增加内置持久化语义。
- `HistorySeriesCache` 只服务 offline `get_*_data_series` / cache-only `read_*_data_series` / scan / maintenance；不要使用它作为 live serial 缓存或外部最新行情 API。
- 官方 Python serial 的 `id` 列来自序列路径 key / 行序号，不要求 raw Kline/Tick payload 自带 `id`；Rust 解码应保持 path-key id 兼容。
- 只有低层 runtime、自定义 facade、adapter、command 状态机、commit/cursor、hot-path `RuntimeReader` 才使用 `tqsdk-core`。
- 所有可见状态变化都必须经过 runtime commit 和 `RuntimeReader` / `UpdateCursor`；不要发明私有状态树、本地订单 overlay 或旁路通知。
- live/network 示例默认需要 Tokio、凭证、行情权限，以及明确的交易权限。
- 优先使用 `futures_market()`、`stock_market()`、`trade_target_tqkq()`、`enable_query()` 这类命名 builder，不要使用裸 bool route flag。
- 下单示例默认使用模拟/TqKq 风格；只有用户明确要求实盘接入并接受副作用时，才给 real-account 集成。
- 精确 API 形状重要时，先检查目标 crate README 和 `crates/*/examples/api_contract_sXX_*.rs`，再定稿代码。

## 常见错误

- 不要用 `tqsdk-wait` 回答 direct-query 问题；使用 `tqsdk-session` 或 `api.session()`。
- live facade 或调用方 event consumer 里不要为了 metadata 再建第二个 client；复用 shared session。
- 不要把历史下载当作 live ref；使用 `tqsdk-data`。如果要持久化 live `KlineRowBatch` / `TickRowBatch` 或 commit events，使用调用方自己的 sidecar，不要把 Python-compatible mmap history cache 接进 live 热路径。
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
