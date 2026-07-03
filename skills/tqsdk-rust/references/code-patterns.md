# 代码模式

如果精确 API 名必须匹配某个 SDK revision，先检查目标 crate README 和 `crates/*/examples/api_contract_sXX_*.rs`。优先使用仓库里的示例，不要根据 Python TqSdk 名字猜 Rust API。

## 目录

- Wait Quote Loop 行情循环
- Session Metadata Query
- 品种/合约查询
- Caller-Owned Commit Consumer
- Historical Data Client
- Shared Live/Backtest Tick Cache
- Trading Task Pattern
- Direct Order Wrapper

## Default Tq Strategy Loop

普通策略、目标持仓和轻量历史访问优先使用默认 `tqsdk` facade。

```rust
use tqsdk::prelude::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    let mut tq = Tq::futures()
        .auth_env()?
        .trade_target_tqkq()
        .connect()
        .await?;

    let quote = tq.quote("SHFE.au2602").await?;
    let target = tq.target_pos_tqkq("SHFE.au2602").await?;

    while tq.next().await? {
        let snapshot = quote.load()?;
        if snapshot.last_price > 3600.0 {
            target.set(1)?;
        } else {
            target.close()?;
        }
    }
    Ok(())
}
```

## Wait Quote Loop 行情循环

明确需要 Python-style `step()` / `WaitStep::is_changing()` 时使用 `tqsdk-wait`。

```rust
use tqsdk_wait::TqApiBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let mut api = TqApiBuilder::new(user, pass).build().await?;
    let quote = api.quote("SHFE.au2602").await?;

    loop {
        let Some(step) = api.step().await? else { continue };
        if step.is_changing(&quote) {
            let snapshot = quote.load()?;
            println!("{} {}", snapshot.datetime, snapshot.last_price);
        }
    }
}
```

## Session Metadata Query

one-shot metadata 直接使用 `tqsdk-session`；如果已经在 facade 或 session-backed loop 内，则复用 shared session。

```rust
use tqsdk_session::SessionClientBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let session = SessionClientBuilder::new(user, pass)
        .futures_market()
        .enable_query()
        .build()?;

    let specs = session.query_instrument_specs(["SHFE.au2602"]).await?;
    println!("{specs:#?}");
    Ok(())
}
```

### 品种/合约查询

按交易所和品种查询所有未过期合约代码，用 `query_quotes`。主连/连续合约用 `query_cont_quotes`。拿到代码后再用 `query_instrument_specs` 查 tick size、合约乘数等规格。

```rust
use tqsdk_session::{OptionQueryFilter, SessionClientBuilder};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let session = SessionClientBuilder::new(user, pass)
        .futures_market()
        .enable_query()
        .build()?;

    let symbols = session
        .query_quotes(Some("FUTURE"), Some("SHFE"), Some("au"), Some(false), None)
        .await?;
    let cont_symbols = session
        .query_cont_quotes(Some("SHFE"), Some("au"), None)
        .await?;
    let symbol_refs: Vec<&str> = symbols.iter().map(String::as_str).collect();
    let specs = session.query_instrument_specs(&symbol_refs).await?;
    let options = session
        .query_options("SHFE.au2602", &OptionQueryFilter::new())
        .await?;

    println!(
        "contracts={} cont={} specs={} options={}",
        symbols.len(),
        cont_symbols.len(),
        specs.len(),
        options.len()
    );
    Ok(())
}
```

多档期权查询使用 `query_all_level_options`；金融期权多档查询使用 `query_all_level_finance_options`。

## Caller-Owned Commit Consumer

```rust
use std::time::Duration;

use tokio::time::Instant;
use tqsdk_core::Symbol;
use tqsdk_session::SessionClientBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let symbol = "SHFE.au2602";
    let symbol_key = Symbol::new(symbol.to_string());

    let session = SessionClientBuilder::new(user, pass)
        .futures_market()
        .build()?;
    session.subscribe_quotes([symbol]).await?;

    let reader = session.reader().clone();
    let mut cursor = reader.cursor();

    loop {
        while let Some(commit) = reader.next(&mut cursor) {
            if let Some(quote) = reader.read_market_state().quote(&symbol_key)? {
                println!(
                    "revision={:?} symbol={} last_price={}",
                    commit.revision, symbol, quote.last_price
                );
            }
        }

        let deadline = Instant::now() + Duration::from_millis(250);
        session.progress_once(Some(deadline)).await?;
    }
}
```

## Historical Data Client

owned historical materialization 和导出使用 `tqsdk-data`。CSV export 优先使用 async writer，并把 live session 和离线研究流程分开。

```rust
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tqsdk_data::{DataClient, KlineDataSeriesRequest};
use tqsdk_session::SessionClientBuilder;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let user = std::env::var("TQ_AUTH_USER")?;
let pass = std::env::var("TQ_AUTH_PASS")?;
let session = SessionClientBuilder::new(user, pass)
    .futures_market()
    .build()?;

let client = DataClient::from_session(session);
let end_ns = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
let start_ns = end_ns - i64::try_from(Duration::from_secs(4 * 60 * 60).as_nanos())?;
let series = client
    .get_kline_data_series(KlineDataSeriesRequest::new(
        "SHFE.au2602",
        Duration::from_secs(60),
        start_ns,
        end_ns,
    ))
    .await?;
println!("rows={}", series.len());
# Ok(())
# }
```

## Shared Live/Backtest Tick Cache

指定合约的 live tick 可以写入和回测共享的持久 tick cache。普通策略优先用
`MarketCachePolicy` 一次声明 cache 目录和 symbol 集合；live builder 会在 `connect()`
后自动启动 recording，backtest builder 可复用同一个 policy 作为默认 cache 输入：

```rust
use tqsdk::prelude::*;

async fn run(start_ns: i64, end_ns: i64) -> tqsdk::Result<()> {
    let cache = MarketCachePolicy::new(".tqsdk/backtest_ticks")
        .record_ticks(["KQ.i@SHFE.au"]);

    let mut tq = Tq::futures()
        .auth_env()?
        .market_cache(cache.clone())
        .connect()
        .await?;

    while tq.next().await? {
        if let Some(health) = tq.record_ticks_health() {
            if health.gap_detected {
                eprintln!("cache gap detected; schedule explicit warmup later");
            }
        }
    }

    let fill_policy = tq.recorded_market_cache_policy().unwrap_or(cache);
    Tq::futures()
        .auth_env()? // explicit again; live session auth is not retained for fill
        .market_cache(fill_policy)
        .backtest(start_ns, end_ns)
        .remote_on_miss()
        .warmup()
        .await?;

    Ok(())
}
```

`Tq::record_ticks(cache_dir, symbols).await?` 仍保留为运行中临时开启 recording 的显式入口。
两种方式都只记录声明的 symbol，不会自动记录所有订阅，也不会启动后台守护进程。coverage
只在 tick id 连续时推进；断线、跳号或程序退出前未确认的尾部会留下缺口，后续需要显式
`.warmup()` / `.remote_on_miss()` 补齐。

已有 tick rows 的上层 host 或 relay-like 进程可以下钻到 `tqsdk-data` 的纯 writer：
`LiveTickCacheWriter::new(cache).push_ticks(symbol, rows)`。它只追加 rows 并按连续 tick id
推进 coverage，不负责 session、订阅或后台运行。

泛化 live event/K 线/commit 持久化、审计 WAL、跨进程 queue 或旧 Python mmap history
bridge 仍属于调用方 sidecar；`HistorySeriesCache` 保持 offline
`get_*_data_series` 缓存和 cache-only reader。

如果用户使用的 SDK revision 中 struct 形状不同，先检查对应 crate example，再定稿代码。

## Trading Task Pattern

用户需要 execution ownership、target position、risk gate 或 test harness 时使用 `tqsdk-task`。副作用必须显式说明，默认优先使用模拟路径。

```rust
use tqsdk_task::TaskHost;
use tqsdk_wait::TqApiBuilder;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let user = std::env::var("TQ_AUTH_USER")?;
let pass = std::env::var("TQ_AUTH_PASS")?;
let api = TqApiBuilder::new(user, pass).build().await?;
let mut host = TaskHost::new(api);
let target = host.target_pos("sim", "SHFE.au2602").build()?;

target.set_target_volume(1)?;
while !target.is_finished() {
    host.wait_update(None).await?;
}
# Ok(())
# }
```

## Direct Order Wrapper

这是 real-account opt-in 示例，只在用户明确要求实盘 broker integration、并接受下单副作用时使用。默认下单答案继续使用上面的 `Trading Task Pattern`、模拟/TqKq 路径或 `tqsdk-task` ownership。只有不需要 task ownership 的薄下单/撤单才使用 `tqsdk-wait` order wrapper；策略级 ownership、retry safety 或 risk gate 应路由到 `tqsdk-task`。

```rust
use tqsdk_core::TradeAccountType;
use tqsdk_wait::TqApiBuilder;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let user = std::env::var("TQ_AUTH_USER")?;
let pass = std::env::var("TQ_AUTH_PASS")?;
let broker_id = std::env::var("TQ_TRADE_BROKER")?;
let account_id = std::env::var("TQ_TRADE_ACCOUNT")?;
let account_pass = std::env::var("TQ_TRADE_PASS")?;

let mut api = TqApiBuilder::new(user, pass).futures_market().build().await?;
api.login_trade_account(
    &broker_id,
    &account_id,
    &account_pass,
    TradeAccountType::Future,
    None,
)
.await?;

let ticket = api
    .limit_order(&account_id, "SHFE.au2602")
    .client_intent("example-buy-open-001")
    .buy_open(1)
    .at(480.0)
    .send_once()
    .await?;

let order = ticket.wait_terminal(&mut api).await?;
println!("order_id={} lifecycle={}", order.order_id, order.lifecycle.as_str());
# Ok(())
# }
```
