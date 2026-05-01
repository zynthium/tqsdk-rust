# `tqsdk-rust`

这是一个 Cargo workspace，用来承载 Rust 版 TQSDK 的核心层、共享 session 层，以及后续不同风格的 facade 层。

仓库当前已经落地的成员如下：

| Crate | 路径 | 角色 |
| --- | --- | --- |
| `tqsdk-core` | `crates/tqsdk-core` | 面向官方服务交互的低层 async substrate |
| `tqsdk-session` | `crates/tqsdk-session` | mode-agnostic 的共享 session / direct-query thin layer |
| `tqsdk-wait` | `crates/tqsdk-wait` | Python 风格 single-owner wait facade，基于 core/session |
| `tqsdk-stream` | `crates/tqsdk-stream` | Rust async-native multi-consumer commit stream facade，基于 core/session |
| `tqsdk-task` | `crates/tqsdk-task` | 建立在 wait facade 之上的执行工具层 |
| `tqsdk-data` | `crates/tqsdk-data` | 研究 / 离线数据与批量查询能力的独立落点 |

后续计划继续在这个 workspace 下补充多种 V2 facade crate，例如：

- `tqsdk-callback`：面向 callback / event handler 风格的 facade。

## 分层原则

- `tqsdk-core` 只负责协议、状态树、commit/revision、session/runtime orchestration。
- facade crate 只消费 `tqsdk-core` 的 substrate，不反向侵入 core。
- core 保持纯 async，不内建 runtime，不附带高层用户便利接口。
- 仓库结构按“稳定底座 + 可替换 facade”组织，优先保证性能、稳定性和长期可维护性。

## 仓库结构

```text
crates/
  tqsdk-core/      # 当前 V1 核心基座
  tqsdk-session/   # 共享 session / direct-query 层
  tqsdk-wait/      # Python 风格 wait facade
  tqsdk-stream/    # Rust 风格 stream facade
  tqsdk-task/      # 执行工具层
  tqsdk-data/      # 研究 / 离线数据层
docs/
  README.md        # 文档职责、权威层级和 AI 读取入口
  architecture/    # 当前架构权威、分层设计与验证矩阵
  scenarios/       # 场景契约草案与 API gap
  reviews/         # 当前审查记录与 public API 决策矩阵
  archive/         # 已闭环或已转化为计划的历史审查输入、spec 与 plan
  superpowers/     # 当前仍在执行的 agentic specs / plans 记录
```

## 文档入口

- core crate 说明见 [crates/tqsdk-core/README.md](crates/tqsdk-core/README.md)
- session crate 说明见 [crates/tqsdk-session/README.md](crates/tqsdk-session/README.md)
- wait crate 说明见 [crates/tqsdk-wait/README.md](crates/tqsdk-wait/README.md)
- stream crate 说明见 [crates/tqsdk-stream/README.md](crates/tqsdk-stream/README.md)
- task crate 说明见 [crates/tqsdk-task/README.md](crates/tqsdk-task/README.md)
- data crate 说明见 [crates/tqsdk-data/README.md](crates/tqsdk-data/README.md)
- 仓库级路线图见 [ROADMAP.md](ROADMAP.md)
- 文档总入口见 [docs/README.md](docs/README.md)
- 架构总览见 [docs/architecture/README.md](docs/architecture/README.md)
- AI 工作流与架构守则见 [docs/architecture/ai-workflow.md](docs/architecture/ai-workflow.md)
- 验证矩阵见 [docs/architecture/validation.md](docs/architecture/validation.md)
- 场景契约与 API gap 见 [docs/scenarios/README.md](docs/scenarios/README.md)
- public API 审查记录见 [docs/reviews/README.md](docs/reviews/README.md)

## 常用命令

```bash
cargo check --workspace --examples
cargo test --workspace
cargo clippy --workspace --examples --all-targets -- -D warnings
cargo check --workspace --no-default-features
cargo check --workspace --all-features --examples
cargo test -p tqsdk-core -q
cargo test -p tqsdk-core -q --test runtime_contract_v1_capability
```

## 当前状态

- V1 core 已独立为子 crate，可单独发布。
- `tqsdk-session` 已承载共享 session shell、lazy establish、route/pending-route 驱动原语，以及 direct-query/schema 薄层入口。
- `tqsdk-session` 现已提供 `progress_once()` 与 `wait_command_completed()` 这两个最小 substrate/control-plane 原语，并用可编译示例覆盖行情订阅、raw query command 等待与 `TqKq` trade 登录路径。
- `tqsdk-wait` 已具备 market/trade 对象引用、serial window、可工作的 `wait_update()` 驱动链路与 trade 命令包装。
- `tqsdk-stream` 已落地最小 commit-stream facade，当前提供共享 session 驱动、raw commit fan-out、显式 lag/closed/error surface、path/scope/domain/object/field 级 commit 过滤、typed 单对象 stream、ready-window `kline/tick` stream、账户级 trade object 事件流与统一 `trade_object_event_stream`，并已用真实示例验证 `stream.session()` 复用同一底层 session 做 direct query 的边界；后续重点转向 notification/transport-error 级 trade session 事件流。
- `tqsdk-task` 已落地 `TaskHost`、`TargetPosTask`、`TargetPosScheduler`、typed order builder、pre-trade risk gate、execution group foundation、account group foundation、`StrategyHost` / `StrategyContext` / `StrategyEnvironment` / `StrategyDeployment` / `StrategySupervisor`、public fake market / fake broker test harness、ownership / guarded order / execution report（原始事件流 + 聚合摘要）；生产观测以 typed health/metrics/shutdown report 为边界，不内置 GUI、web helper 或 HTTP health/metrics endpoint。
- `tqsdk-data` 已落地独立 crate 骨架、`DataClient`、`query_his_cont_quotes`、history `data_page` / `data_series` 与 pull-based `data_download` substrate。
- workspace 根 README 现在只承载仓库级说明。
- crate 级使用说明和 API 契约已经分别下沉到各子 crate 的 `README.md`。

当前 workspace 里的最小可编译示例：

- `crates/tqsdk-session/examples/query_symbol_info.rs`
- `crates/tqsdk-session/examples/query_command_wait.rs`
- `crates/tqsdk-session/examples/quote_progress.rs`
- `crates/tqsdk-session/examples/trade_login_tqkq.rs`
- `crates/tqsdk-wait/examples/quote_wait.rs`
- `crates/tqsdk-wait/examples/quote_wait_with_session_query.rs`
- `crates/tqsdk-stream/examples/quote_stream.rs`
- `crates/tqsdk-stream/examples/quote_stream_with_session_query.rs`
- `crates/tqsdk-stream/examples/kline_stream.rs`
- `crates/tqsdk-stream/examples/trade_session_events.rs`
- `crates/tqsdk-task/examples/target_pos.rs`
- `crates/tqsdk-task/examples/target_pos_scheduler.rs`
- `crates/tqsdk-task/examples/api_contract_s11_simple_strategy.rs`
- `crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs`
- `crates/tqsdk-task/examples/api_contract_s13_multi_account_ordering.rs`
- `crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs`
- `crates/tqsdk-task/examples/api_contract_s24_testable_strategy.rs`
- `crates/tqsdk-wait/examples/api_contract_s25_wait_serial_trading_status.rs`
- `crates/tqsdk-wait/examples/api_contract_s26_trade_system_refs.rs`
- `crates/tqsdk-wait/examples/api_contract_s26_security_trade_refs.rs`
- `crates/tqsdk-session/examples/api_contract_s27_metadata_service_queries.rs`
- `crates/tqsdk-data/examples/api_contract_s28_download_export.rs`
- `crates/tqsdk-data/examples/api_contract_s28_option_greeks.rs`
- `crates/tqsdk-task/examples/api_contract_s29_target_pos_ownership.rs`
- `crates/tqsdk-data/examples/his_cont_quotes.rs`
- `crates/tqsdk-data/examples/kline_data_page.rs`
- `crates/tqsdk-data/examples/kline_data_series.rs`
- `crates/tqsdk-data/examples/kline_data_download.rs`
- `crates/tqsdk-data/examples/kline_export_csv.rs`
- `crates/tqsdk-data/examples/option_greeks.rs`

## 架构文档

仓库里的 [`docs/architecture`](docs/architecture) 目录给出了完整分层说明。文档目录职责和 AI 读取顺序见 [`docs/README.md`](docs/README.md)。

- [`docs/architecture/README.md`](docs/architecture/README.md)
- [`docs/architecture/ai-workflow.md`](docs/architecture/ai-workflow.md)
- [`docs/architecture/runtime-core/overview.md`](docs/architecture/runtime-core/overview.md)
- [`docs/architecture/validation.md`](docs/architecture/validation.md)

一句话总结就是：

- V1 交付的是 protocol-complete runtime contract。
- V2 及以后所有 facade 都应建立在 `RuntimeReader` 和 `UpdateCursor` 之上，而不是反向改写 runtime core。
