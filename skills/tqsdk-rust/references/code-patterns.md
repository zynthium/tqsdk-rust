# Code Patterns

Check the target crate README and `crates/*/examples/api_contract_sXX_*.rs` when exact API names must match a specific SDK revision.

## Wait Quote Loop

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

Use `tqsdk-session` directly for one-shot metadata, or `api.session()` / `stream.session()` from a facade.

```rust
use tqsdk_session::SessionClientBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let session = SessionClientBuilder::new(user, pass)
        .futures_market()
        .enable_query()
        .build()
        .await?;

    let specs = session.query_instrument_specs(["SHFE.au2602"]).await?;
    println!("{specs:#?}");
    Ok(())
}
```

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

Use `tqsdk-data` for owned historical materialization and exports. Prefer async writers for CSV export and keep live sessions separate from offline research flows.

```rust
use tqsdk_data::{DataClient, KlineDataSeriesRequest};
use tqsdk_session::SessionClientBuilder;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let user = std::env::var("TQ_AUTH_USER")?;
let pass = std::env::var("TQ_AUTH_PASS")?;
let session = SessionClientBuilder::new(user, pass)
    .futures_market()
    .build()
    .await?;

let client = DataClient::from_session(session);
let series = client
    .get_kline_data_series(KlineDataSeriesRequest {
        symbol: "SHFE.au2602".into(),
        duration_ns: 60_000_000_000,
        start_datetime_ns: 0,
        end_datetime_ns: 0,
    })
    .await?;
println!("rows={}", series.klines.len());
# Ok(())
# }
```

If a struct shape differs in the user's SDK revision, inspect the crate example before finalizing code.

## Trading Task Pattern

Use `tqsdk-task` when the user wants execution ownership, target position, risk gates, or test harnesses. Keep side effects explicit and prefer simulation defaults.

```rust
use tqsdk_task::TaskHost;
use tqsdk_wait::TqApiBuilder;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let user = std::env::var("TQ_AUTH_USER")?;
let pass = std::env::var("TQ_AUTH_PASS")?;
let api = TqApiBuilder::new(user, pass).build().await?;
let mut host = TaskHost::new(api);
let mut target = host.target_pos("sim", "SHFE.au2602")?;

target.set_target_volume(1)?;
while !target.is_finished() {
    host.wait_update(None).await?;
}
# Ok(())
# }
```
