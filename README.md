# tqsdk-rust

面向天勤 / TQSDK 生态的 Rust SDK 工作区，用一套共享的异步 runtime 支撑行情、
交易、策略执行和研究数据工作流。

[![CI](https://github.com/zynthium/tqsdk-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/zynthium/tqsdk-rust/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## 项目定位

普通用户优先从顶层 `tqsdk` crate 开始：连接账号、订阅行情、等待更新、读取账户/持仓、设置目标持仓，或按需下钻到 `wait` / `task` 执行接口、访问历史数据。内部 crate 仍保持独立边界，但第一次阅读不需要先理解整个 workspace taxonomy。

`tqsdk-rust` 的核心约束是所有可见状态变化都经过同一套 runtime state tree、commit/revision 和 cursor 语义。`tqsdk` 只是默认 facade；它不会复制 direct query、stream、task 或 data 实现。

## 当前状态

项目正在积极开发中，当前 crate 版本为 `0.1.0`。建议先通过本仓库 workspace 或 Git
dependency 使用；正式 crates.io 发布前，public API 仍可能继续收敛。

仓库中的 `crates/*/examples` 不只是示例代码，也承担 public API contract 的作用。涉及
用户可见 API 的改动应保持这些示例清晰、可编译。

## 默认入口与高级 crate

高级用户可以按需要下钻：

| Crate | 适合场景 |
| --- | --- |
| [`tqsdk`](crates/tqsdk) | 默认用户入口：`prelude`、`Tq` 主循环、常用 live refs、target position、history helper，以及 `advanced::*` 下钻入口 |
| [`tqsdk-core`](crates/tqsdk-core) | 底层 async protocol substrate、状态树、commit/revision、runtime reader、cursor、adapter 和 schema types |
| [`tqsdk-session`](crates/tqsdk-session) | 共享 session、lazy connection、命令推进、one-shot direct query、metadata、schema 和 service query |
| [`tqsdk-wait`](crates/tqsdk-wait) | Python 风格 `TqApi`、`wait_update()`、`is_changing()`、live object refs、serial window 和 wait-style 交易命令 |
| [`tqsdk-stream`](crates/tqsdk-stream) | 高级 Rust async-native 多消费者 commit stream、row-batch kline/tick stream、过滤器、lag diagnostics 和 health status |
| [`tqsdk-task`](crates/tqsdk-task) | `TargetPosTask`、scheduler、typed order builder、pre-trade risk gate、strategy host、fake market / fake broker、task-owned replay source、Python-compatible local backtest sim、低延迟 trading desk profile |
| [`tqsdk-data`](crates/tqsdk-data) | 历史数据 page/series/download、CSV export、option greeks、主连数据和 Python-compatible history series mmap cache |
| [`tqsdk-relay`](crates/tqsdk-relay) | 可选 market relay / cache service：用共享上游 tick 源服务多个 SDK 客户端的 quote / tick / K 线请求；未配置 relay 时 SDK 仍直连天勤 |

一般使用建议：

- 普通策略、目标持仓和轻量历史访问：先用 `tqsdk`。
- 已明确需要 Python 风格单 owner 推进点：直接用 `tqsdk-wait`。
- 需要多个异步消费者、独立 consumer 进度、fan-out、lag diagnostics 或事件管道：用 `tqsdk-stream`。
- 只做合约、日历、metadata、schema 等一次性查询：用 `tqsdk-session`。
- 做历史数据、批量导出和 history series cache：用 `tqsdk-data`。
- 做确定性 replay / 本地回测行情输入：用 `tqsdk-task::ReplayMarketSource`。
- 做执行工具、风控、策略 host、fake broker 或本地 sim：用 `tqsdk-task`。
- 自建 facade 或极低层热路径：用 `tqsdk-core + tqsdk-session`。

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
tqsdk = { path = "crates/tqsdk" }
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

在其他项目中使用时，可以先依赖 Git 仓库：

```toml
[dependencies]
tqsdk = { git = "https://github.com/zynthium/tqsdk-rust" }
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

高级用户仍可直接依赖 `tqsdk-session`、`tqsdk-stream`、`tqsdk-wait`、
`tqsdk-task` 或 `tqsdk-data`，但普通策略和研究入口应先尝试 `tqsdk`。
`tqsdk-stream` 是多消费者异步集成入口，不是普通 quote 订阅的默认性能路径。

`tqsdk-relay` 是可选基础设施。普通 SDK 使用不需要启动 relay；只有需要降低多进程、
全品种、多周期行情订阅压力时，才显式把 market endpoint 指向 relay。
relay 侧推荐配置 `TQSDK_RELAY_FUTURES_PRODUCTS=ALL` 或产品代码列表，由 relay 动态
查询当前活跃合约集合，并默认在本地时间每天 `08:30:00` 重新发现。relay 会暴露上游
合约数、`ins_list` 长度和阈值命中 metrics，并可用 `TQSDK_RELAY_DRY_RUN=1` 在启动前
检查订阅规模；`/health` 会区分下游监听、上游连接、订阅/补历史阶段、合约集合刷新和数据 freshness；
`/metrics` 和 `/dashboard` 会暴露上游 `connecting` / `subscribing` / `backfilling` / `live` 阶段，
dashboard 还会展示 backfilling 已持续时间、frame 速率和最近 frame idle；
可用 `TQSDK_RELAY_UPSTREAM_TICK_VIEW_WIDTH=1` 调小启动时的上游 tick 历史窗口；
`/symbol-metrics` 可用于查看每个合约的数据接收状态与延迟；
静态完整合约文件只作为兼容覆盖入口。

## 快速开始

最小普通策略入口：

```rust
use tqsdk::prelude::*;

let mut tq = Tq::futures()
    .auth_env()?
    .trade_target_tqkq()
    .connect()
    .await?;

let quote = tq.quote("SHFE.au2602").await?;
let target = tq.target_pos_tqkq("SHFE.au2602").await?;

while tq.next().await? {
    if quote.load()?.last_price > 3600.0 {
        target.set(1)?;
    }
}
```

如果只想先验证不依赖真实账号和网络的策略测试 harness，可以运行：

```bash
cargo run -p tqsdk-task --example api_contract_s24_testable_strategy
```

如果要验证不依赖真实账号和网络的 Python-compatible 本地回测模拟账户闭环，可以运行：

```bash
cargo run -p tqsdk-task --example api_contract_s32_python_backtest_sim
```

如果要对齐 Python `TqApi(backtest=TqBacktest(...))` 的 live/backtest 同策略主体心智，
使用 `tqsdk-wait` 的 `TqApiBuilder::futures_backtest(...)` /
`stock_backtest(...)`；如果要本地历史行情或显式 replay 事件 + `TqSim` 账户撮合，则使用
`tqsdk-task::StrategyBacktest` 搭配 task-owned `ReplayMarketSource`。当前本地路径已覆盖
quote/tick/kline replay event 的最小 quote synthesis 和轻量 `summary()`；完整报告、
自动分钟线和主连历史映射仍是后续范围。

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

`tqsdk-wait` 的 `quotes(...)` 会一次表达批量 quote interest；`quote(...)` 仍是单合约便利入口。`kline(...)` / `tick(...)` 会立即返回 live serial handle；如果需要在启动阶段等待 chart 初始化，使用 `kline_ready(...)` / `tick_ready(...)`。多合约 K 线序列使用 `kline_multi([...], ...)`：它提交一个共享 `chart_id` 的逗号 `ins_list`，服务端初始 `view_width=10000`，客户端按主合约 `binding` 对齐副合约；Tick 序列保持单合约，逗号合约输入会报错。

### Advanced Rust async stream facade

适合多个异步消费者共享同一个 live session，并且需要独立进度、显式背压或事件管道。
单 owner 策略应继续优先使用 `tqsdk` / `tqsdk-wait`：

```rust
use futures::StreamExt;

let stream = tqsdk_stream::TqStreamBuilder::new(user, pass).build().await?;
let mut batches = stream
    .quote_batches(["SHFE.au2602", "DCE.m2609"])
    .await?;
let batch = batches.next().await.ok_or("quote stream closed")??;
```

`quote_batches(...)` 是 multi-consumer stream 场景下的批量 quote 入口：每个 runtime commit
最多产出一个 batch，内部只 decode 本轮实际变化的合约。它的价值是消费模型和
fan-out 隔离，不应作为单消费者 quote throughput 的默认推荐。`quotes(...)` 仍保留为
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

适合 kline/tick 历史数据、导出、history series cache 和研究查询：

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
| Python-compatible 本地回测模拟账户 | `cargo run -p tqsdk-task --example api_contract_s32_python_backtest_sim` | 使用本地 quote/tick/kline replay + `TqSim`，不连接真实服务 |
| `wait_update()` 行情更新 | `TQ_WAIT_ONCE=1 cargo run -p tqsdk-wait --example quote_wait` | 需要 `TQ_AUTH_USER` / `TQ_AUTH_PASS`；去掉 `TQ_WAIT_ONCE=1` 后持续运行 |
| 高级 quote stream 消费 | `TQ_STREAM_ONCE=1 cargo run -p tqsdk-stream --example quote_stream` | 多消费者 async 集成示例，需要账号；去掉 `TQ_STREAM_ONCE=1` 后持续运行 |
| 合约 metadata 查询 | `cargo run -p tqsdk-session --example query_symbol_info` | 需要账号；可用 `TQ_TEST_SYMBOL` 覆盖默认合约 |
| command wait helper | `cargo run -p tqsdk-session --example query_command_wait` | 需要账号；默认查询 `SSE.000300`，可用 `TQ_QUERY_SYMBOL` 覆盖 |
| K 线分页查询 | `cargo run -p tqsdk-data --example kline_data_page` | 需要账号和历史数据权限；可用 `TQ_TEST_SYMBOL` 覆盖默认合约 |
| K 线 CSV 导出 | `cargo run -p tqsdk-data --example kline_export_csv` | 需要账号和历史数据权限；默认写入 `/tmp/tqsdk-kline-export.csv`，可用 `TQ_EXPORT_PATH` 覆盖 |
| 目标持仓任务 | `cargo run -p tqsdk-task --example target_pos` | 需要账号；默认 TqKq dry-run，不会下单；只有设置 `TQ_TASK_ALLOW_ORDERS=1` 和 `TQ_TARGET_VOLUME` 才进入调仓循环 |
| 低延迟 trading desk profile | `cargo run -p tqsdk-task --example api_contract_s31_low_latency_trading_desk` | 需要账号；默认不会下单；只有设置 `TQ_DESK_ALLOW_ORDER=1` 才提交示例订单 |

更多场景契约示例见各 crate 的 `examples/` 目录。

## 架构概览

仓库采用“稳定底座 + 可替换 facade”的分层。下图表示用户能力层级，不是 Cargo 依赖图：

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
    ^
    |
tqsdk
```

实际 Cargo 依赖中，`tqsdk` 作为默认入口会直接依赖 `tqsdk-core`、`tqsdk-session`、
`tqsdk-wait`、`tqsdk-stream`、`tqsdk-task` 和 `tqsdk-data`；内部能力归属仍由这些
crate 自己维护。

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
