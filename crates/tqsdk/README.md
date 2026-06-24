# tqsdk

`tqsdk` 是 `tqsdk-rust` 的默认用户入口。它不物理合并内部 crate，也不改变
runtime contract；它只提供一个更容易开始的 facade：

- `tqsdk::prelude::*`
- `Tq::new()` (and `Tq::futures()` alias)
- Server-side backtest (`.backtest()`, optional `.replay_url(...)`)
- Server-side single-day replay (`.server_replay(date)?`)
- Local offline backtest (`.local_backtest()`, `.local_backtest_klines(...)`, `.local_backtest_ticks(...)`, `.local_backtest_kline_history(...)`, `.local_backtest_kline_histories(...)`, `.local_backtest_minute_history(...)`, `.local_backtest_quote_minute_history(...)`, `.local_backtest_continuous_minute_history(...)`, `_as` alias helpers, optional `.instrument_spec(...)` / `.default_price_tick(...)`)
- `Tq::next()` 主循环
- 常用 wait-style live refs 和 `Quote` 统一定义
- `TargetPos` 轻量 wrapper
- Local backtest 默认模拟账户常量 `LOCAL_BACKTEST_ACCOUNT_ID`
- Local backtest summary / performance metrics / performance report、cash/equity 曲线点、买卖/开平次数、日收益统计（含显式交易日窗口）和最大回撤
- `Tq::history()` helper
- `tqsdk::advanced::*` 下钻到底层 crate

本地回测的 `_as` helper 让 caller-provided replay symbol 与实际 history symbol 分离：
可以把 `SHFE.rb2601` / `SHFE.rb2605` 等 underlying series 显式组合到
`KQ.m@SHFE.rb` 这样的主连代码下回放，并保留 quote `underlying_symbol` metadata。
常用主连分钟线回测可以用 `.local_backtest_continuous_minute_history(...)`
自动查询 underlying segment、按交易日窗口裁剪并组合 replay source。
若只需要 Python TqBacktest 风格的分钟线 quote fallback，可先用 `.quote_symbol(...)`
声明普通合约，再用 `.local_backtest_quote_minute_history(...)` 显式取这些 symbol 的
一分钟 K 线进入本地回测；该路径不做隐藏订阅或隐式联网。
如果已经通过 `tqsdk-session` 查询到合约 metadata，可以把 `InstrumentSpec` 传给
`.instrument_spec(...)`，让本地 kline replay 自动获得 `price_tick` 和合约乘数。
服务端单日复盘可用 `.server_replay(date)?`：connect 时创建官方 replay session，
把返回的 `md_url` 接入正常行情 loop。复盘速度、heartbeat 和 terminate 可通过
`Tq::set_replay_speed(...)` / `send_replay_heartbeat()` / `terminate_server_replay()`
显式控制；当前不会自动启动 Python 风格后台 heartbeat。
本地回测结束前会 drain 已进入 runtime 的 task updates，因此 `TargetPos` 的
`execution_report()` 能看到最后一个 replay step 产生的本地模拟成交；需要类似
task channel 的增量消费时，可用 `execution_events_since(cursor)` /
`execution_trades_since(cursor)` 读取新执行事件和新成交。

## 示例

```rust
use tqsdk::prelude::*;

# async fn run() -> tqsdk::Result<()> {
let mut tq = Tq::futures()
    .auth_env()?
    .trade_target_tqkq()
    .connect()
    .await?;

let quote = tq.quote("SHFE.au2602").await?;
let target = tq.target_pos_tqkq("SHFE.au2602").await?;

while tq.next().await? {
    let q = quote.load()?;
    if q.last_price > 3600.0 {
        target.set(1)?;
    }
}
# Ok(())
# }
```

## Features

- `default = ["live", "services"]`：默认用户入口，包含 live 连接与服务查询能力。
- `live`：向内部 `session` / `wait` / `stream` / `task` / `data` crate 传播 live feature，并启用 TQ auth 派生的 TQKQ helper。
- `services`：向内部 crate 传播服务查询相关 HTTP 能力。
- `default-features = false`：保留 facade 类型和不依赖 live auth 的组合入口；live-only helper 不参与编译。

`tqsdk::advanced::*` 是 curated convenience，不是完整 sibling crate mirror。它只暴露默认 facade 常见下钻点：

```rust
use tqsdk::advanced::session::SessionClientBuilder;
use tqsdk::advanced::session::InstrumentSpec;
use tqsdk::advanced::stream::TqStreamBuilder;
use tqsdk::advanced::runtime::RuntimeReader;
use tqsdk::advanced::task::StrategyReplaySourceBuilder;
```

需要完整 stream、task、data、session 或 core surface 的用户应直接依赖对应 sibling crate。这样可以让 `tqsdk` 的 semver surface 保持小，同时不限制高级用户使用底层能力。
其中 `advanced::stream` 面向已经明确需要多消费者 async stream 的用户；普通单 owner
策略仍应优先通过 `tqsdk::prelude::*` / `Tq::next()` 或直接使用 `tqsdk-wait`。

## 边界

`tqsdk` 不拥有第二棵状态树，不复制 direct query、stream、task 或 data
实现。能力归属仍然保持在内部 crate：

- direct query / metadata：`tqsdk-session`
- single-owner `wait_update()`：`tqsdk-wait`
- async multi-consumer stream：`tqsdk-stream`
- execution tooling：`tqsdk-task`
- research/offline data：`tqsdk-data`
