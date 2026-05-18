# tqsdk-rust

面向天勤 / TQSDK 生态的 Rust SDK 工作区，用一套共享的异步 runtime 支撑行情、
交易、策略执行和研究数据工作流。

[![CI](https://github.com/zynthium/tqsdk-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/zynthium/tqsdk-rust/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## 项目定位

`tqsdk-rust` 是 Rust 版 TQSDK 的 Cargo workspace。它把底层协议、session、
状态树、commit/revision 和 cursor 语义收敛到同一套 runtime contract，再在上层提供
不同风格的用户 API：Python 风格的 `wait_update()`、Rust async stream、多账户执行工具、
历史数据下载与离线研究能力。

这个项目适合下面几类使用者：

- 已熟悉 TQSDK / 天勤生态，希望在 Rust 中编写交易或研究程序的用户
- 需要 `wait_update()` 心智，但希望获得 Rust 类型系统和 async 能力的策略开发者
- 需要多个异步消费者共享同一 live session 的服务端程序
- 需要订单任务、风控检查、策略 host、可测试 fake market / fake broker 的交易工具开发者
- 希望直接使用底层 runtime、状态树和 commit/cursor 模型搭建自定义 facade 的 SDK 开发者

## 当前状态

项目正在积极开发中，当前 crate 版本为 `0.1.0`。建议先通过本仓库 workspace 或 Git
dependency 使用；正式 crates.io 发布前，public API 仍可能继续收敛。

仓库中的 `crates/*/examples` 不只是示例代码，也承担 public API contract 的作用。涉及
用户可见 API 的改动应保持这些示例清晰、可编译。

## Crate 选择

| Crate | 适合场景 |
| --- | --- |
| [`tqsdk-core`](crates/tqsdk-core) | 底层 async protocol substrate、状态树、commit/revision、runtime reader、cursor、adapter 和 schema types |
| [`tqsdk-session`](crates/tqsdk-session) | 共享 session、lazy connection、命令推进、one-shot direct query、metadata、schema 和 service query |
| [`tqsdk-wait`](crates/tqsdk-wait) | Python 风格 `TqApi`、`wait_update()`、`is_changing()`、live object refs、serial window 和 wait-style 交易命令 |
| [`tqsdk-stream`](crates/tqsdk-stream) | Rust async-native 多消费者 commit stream、object stream、row-batch kline/tick stream、过滤器、lag diagnostics 和 health status |
| [`tqsdk-task`](crates/tqsdk-task) | `TargetPosTask`、scheduler、typed order builder、pre-trade risk gate、strategy host、fake market / fake broker、Python-compatible local backtest sim、低延迟 trading desk profile |
| [`tqsdk-data`](crates/tqsdk-data) | 历史数据 page/series/download、CSV export、option greeks、主连数据、离线 cache 和 replay foundation |

一般使用建议：

- 想快速写策略或迁移 Python TQSDK 心智：从 `tqsdk-wait` 开始。
- 想写 Rust async 服务或多消费者事件处理：从 `tqsdk-stream` 开始。
- 只做合约、日历、metadata、schema 等一次性查询：使用 `tqsdk-session`。
- 做历史数据、批量导出、离线研究和 replay：使用 `tqsdk-data`。
- 做订单任务、仓位目标、策略调度、风控和测试 harness：使用 `tqsdk-task`。
- 需要自己搭 facade 或极低层控制：使用 `tqsdk-core + tqsdk-session`。

## 环境要求

- Rust 1.85 或更新版本
- Tokio runtime
- 天勤 / TQSDK 账号，用于 live 行情、交易、query 和历史数据示例

live 示例默认读取以下环境变量：

```bash
export TQ_AUTH_USER="your-account"
export TQ_AUTH_PASS="your-password"
```

交易、调仓、导出和特定市场查询示例还会读取更细的 `TQ_*` 环境变量。下方
“常用示例”表标明了默认是否有限运行、是否会写文件、以及是否可能下单；更完整的
变量清单见对应 crate README 和 example 源码。

## 安装

在本仓库内开发时，直接使用 workspace crate：

```toml
[dependencies]
tqsdk-wait = { path = "crates/tqsdk-wait" }
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

在其他项目中使用时，可以先依赖 Git 仓库：

```toml
[dependencies]
tqsdk-wait = { git = "https://github.com/zynthium/tqsdk-rust" }
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

根据你的使用场景，把 `tqsdk-wait` 替换为 `tqsdk-session`、`tqsdk-stream`、
`tqsdk-task` 或 `tqsdk-data`。

## 快速开始

如果只想先验证不依赖真实账号和网络的策略测试 harness，可以运行：

```bash
cargo run -p tqsdk-task --example api_contract_s24_testable_strategy
```

如果要验证不依赖真实账号和网络的 Python-compatible 本地回测模拟账户闭环，可以运行：

```bash
cargo run -p tqsdk-task --example api_contract_s32_python_backtest_sim
```

如果已经配置好天勤账号，可以运行一次 `wait_update()` 行情示例：

```bash
export TQ_AUTH_USER="your-account"
export TQ_AUTH_PASS="your-password"
TQ_WAIT_ONCE=1 cargo run -p tqsdk-wait --example quote_wait
```

让 AI 助手在你的项目里使用 TQSDK Rust 上下文：

```bash
npx skills add https://github.com/zynthium/tqsdk-rust
```

## API 形态示例

### Python 风格 wait facade

适合单 owner 的策略主循环：

```rust
let mut api = tqsdk_wait::TqApiBuilder::new(user, pass).build().await?;
let quotes = api.quotes(["SHFE.au2602", "DCE.m2609"]).await?;
api.wait_update(None).await?;
let snapshot = quotes.get("SHFE.au2602").unwrap().load()?;
```

`tqsdk-wait` 的 `quotes(...)` 会一次表达批量 quote interest；`quote(...)` 仍是单合约便利入口。`kline(...)` / `tick(...)` 会立即返回 live serial handle；如果需要在启动阶段等待 chart 初始化，使用 `kline_ready(...)` / `tick_ready(...)`。

### Rust async stream facade

适合多个异步消费者共享同一个 live session：

```rust
use futures::StreamExt;

let stream = tqsdk_stream::TqStreamBuilder::new(user, pass).build().await?;
let mut batches = stream
    .quote_batches(["SHFE.au2602", "DCE.m2609"])
    .await?;
let batch = batches.next().await.ok_or("quote stream closed")??;
```

`quote_batches(...)` 是多品种实时 quote 的推荐高性能入口：每个 runtime commit
最多产出一个 batch，内部只 decode 本轮实际变化的合约。`quotes(...)` 仍保留为
兼容的逐 quote item stream。

### Direct query / metadata

适合一次性查询，不需要绑定 `wait_update()` 或 stream：

```rust
let session = tqsdk_session::SessionClientBuilder::new(user, pass)
    .enable_query()
    .build()?;
let rows = session.query_symbol_info(&["SHFE.au2602"]).await?;
```

### 历史数据与研究工作流

适合 kline/tick 历史数据、导出、cache 和 replay：

```rust
use std::time::Duration;

let session = tqsdk_session::SessionClientBuilder::new(user, pass)
    .futures_market()
    .build()?;
let client = tqsdk_data::DataClient::from_session(session);
let request = tqsdk_data::KlineDataPageRequest::new(
    "SHFE.au2602",
    Duration::from_secs(60),
    128,
);
let page = client.get_kline_data_page(request).await?;
```

## 常用示例

| 场景 | 命令 | 运行说明 |
| --- | --- | --- |
| 不依赖真实账号的策略测试 harness | `cargo run -p tqsdk-task --example api_contract_s24_testable_strategy` | 使用 fake market / fake broker，不连接真实服务 |
| Python-compatible 本地回测模拟账户 | `cargo run -p tqsdk-task --example api_contract_s32_python_backtest_sim` | 使用本地 quote replay + `TqSim`，不连接真实服务 |
| `wait_update()` 行情更新 | `TQ_WAIT_ONCE=1 cargo run -p tqsdk-wait --example quote_wait` | 需要 `TQ_AUTH_USER` / `TQ_AUTH_PASS`；去掉 `TQ_WAIT_ONCE=1` 后持续运行 |
| quote stream 消费 | `TQ_STREAM_ONCE=1 cargo run -p tqsdk-stream --example quote_stream` | 需要账号；去掉 `TQ_STREAM_ONCE=1` 后持续运行 |
| 合约 metadata 查询 | `cargo run -p tqsdk-session --example query_symbol_info` | 需要账号；可用 `TQ_TEST_SYMBOL` 覆盖默认合约 |
| command wait helper | `cargo run -p tqsdk-session --example query_command_wait` | 需要账号；默认查询 `SSE.000300`，可用 `TQ_QUERY_SYMBOL` 覆盖 |
| K 线分页查询 | `cargo run -p tqsdk-data --example kline_data_page` | 需要账号和历史数据权限；可用 `TQ_TEST_SYMBOL` 覆盖默认合约 |
| K 线 CSV 导出 | `cargo run -p tqsdk-data --example kline_export_csv` | 需要账号和历史数据权限；默认写入 `/tmp/tqsdk-kline-export.csv`，可用 `TQ_EXPORT_PATH` 覆盖 |
| 目标持仓任务 | `cargo run -p tqsdk-task --example target_pos` | 需要账号；默认 TqKq dry-run，不会下单；只有设置 `TQ_TASK_ALLOW_ORDERS=1` 和 `TQ_TARGET_VOLUME` 才进入调仓循环 |
| 低延迟 trading desk profile | `cargo run -p tqsdk-task --example api_contract_s31_low_latency_trading_desk` | 需要账号；默认不会下单；只有设置 `TQ_DESK_ALLOW_ORDER=1` 才提交示例订单 |

更多场景契约示例见各 crate 的 `examples/` 目录。

## 架构概览

仓库采用“稳定底座 + 可替换 facade”的分层：

```text
tqsdk-core
    ^
    |
tqsdk-session
    ^
    |
tqsdk-wait / tqsdk-stream / tqsdk-data
    ^
    |
tqsdk-task
```

所有对外可见的状态变化都经过同一套 runtime commit model：

```text
RuntimeCommand / RuntimeInput
    -> ProtocolAdapter
    -> NormalizedMutation
    -> RuntimeHandle
    -> StateStore
    -> CommitResult
    -> RuntimeReader / UpdateCursor
```

这样可以保证 `wait_update()`、async stream、task tooling 和 research pipeline
看到的是同一棵状态树、同一套 revision 和同一套因果解释。底层 crate 保持克制：
direct query 属于 `tqsdk-session`，live diff 消费属于 `tqsdk-wait` / `tqsdk-stream`，
执行工具属于 `tqsdk-task`，研究和离线数据属于 `tqsdk-data`。

完整架构说明见 [docs/architecture](docs/architecture)，验证矩阵见
[docs/architecture/validation.md](docs/architecture/validation.md)。

## 本地开发

克隆仓库并检查 workspace：

```bash
git clone https://github.com/zynthium/tqsdk-rust.git
cd tqsdk-rust
cargo check --workspace --examples
```

常用验证命令：

```bash
cargo fmt --all --check
cargo check --workspace --examples
cargo test --workspace
cargo clippy --workspace --examples --all-targets -- -D warnings
```

修改 feature flags、workspace 依赖或 crate feature 传播时，补充：

```bash
cargo check --workspace --no-default-features
cargo check --workspace --no-default-features --examples
cargo check --workspace --all-features --examples
```

## 文档入口

- [文档索引](docs/README.md)
- [架构总览](docs/architecture/README.md)
- [runtime core overview](docs/architecture/runtime-core/overview.md)
- [crate 边界审计](docs/architecture/crate-boundaries.md)
- [验证矩阵](docs/architecture/validation.md)
- [路线图](ROADMAP.md)

每个 crate 也有自己的 README，说明该 crate 的职责边界、示例和 public surface。

## 贡献

欢迎 issue 和 pull request。开始改动前，请先阅读架构总览和受影响 crate 的 README。
改动应尽量聚焦，并保持 crate 归属边界清晰。

如果改动涉及 public API、feature flags、runtime contract 或 facade 职责归属，请同步更新
相关架构文档或 crate README。影响用户可见行为时，优先补充 focused tests 或
`api_contract_sXX_*` 示例。

## License

本项目采用 [MIT License](LICENSE)。
