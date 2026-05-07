# 代码模式

如果精确 API 名必须匹配某个 SDK revision，先检查目标 crate README 和 `crates/*/examples/api_contract_sXX_*.rs`。优先使用仓库里的示例，不要根据 Python TqSdk 名字猜 Rust API。

## 目录

- Wait Quote Loop 行情循环
- Session Metadata Query
- 品种/合约查询
- Stream Commit Consumer
- Historical Data Client
- Trading Task Pattern
- Direct Order Wrapper

## Wait Quote Loop 行情循环

```rust
use tqsdk_wait::TqApiBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let mut api = TqApiBuilder::new(user, pass).build().await?;
    let quote = api.get_quote("SHFE.au2602").await?;

    loop {
        if !api.wait_update(None).await? {
            continue;
        }
        if api.is_changing(&quote)? {
            let snapshot = quote.load(&api)?;
            println!("{} {}", snapshot.datetime, snapshot.last_price);
        }
    }
}
```

## Session Metadata Query

one-shot metadata 直接使用 `tqsdk-session`；如果已经在 facade 内，则使用 `api.session()` / `stream.session()`。

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

## Stream Commit Consumer

```rust
use futures::StreamExt;
use tqsdk_stream::TqStreamBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let stream = TqStreamBuilder::new(user, pass).build().await?;
    let mut commits = stream.commit_stream()?;

    while let Some(update) = commits.next().await {
        let commit = update?;
        let snapshot = stream.reader().read();
        println!("revision={} scope={:?}", commit.revision, commit.scope);
        println!("head={}", snapshot.revision());
    }
    Ok(())
}
```

## Historical Data Client

owned historical materialization 和导出使用 `tqsdk-data`。CSV export 优先使用 async writer，并把 live session 和离线研究流程分开。

```rust
use std::time::Duration;

use tqsdk_data::{DataClient, KlineDataSeriesRequest};
use tqsdk_session::SessionClientBuilder;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let user = std::env::var("TQ_AUTH_USER")?;
let pass = std::env::var("TQ_AUTH_PASS")?;
let session = SessionClientBuilder::new(user, pass)
    .futures_market()
    .build()?;

let client = DataClient::from_session(session);
let series = client
    .get_kline_data_series(KlineDataSeriesRequest::new(
        "SHFE.au2602",
        Duration::from_secs(60),
        0,
        0,
    ))
    .await?;
println!("rows={}", series.len());
# Ok(())
# }
```

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

只有不需要 task ownership 的薄下单/撤单才使用 `tqsdk-wait` order wrapper。策略级 ownership、retry safety 或 risk gate 应路由到 `tqsdk-task`。

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
