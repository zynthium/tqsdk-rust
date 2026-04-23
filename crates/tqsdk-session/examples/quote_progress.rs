use std::error::Error;
use std::time::Duration;

use tokio::time::Instant;
use tqsdk_core::{MarketCommand, Quote, RuntimeCommand, Symbol};
use tqsdk_session::SessionClientBuilder;

fn read_env(key: &str) -> Result<String, Box<dyn Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

fn is_stock_symbol(symbol: &str) -> bool {
    symbol.starts_with("SSE.") || symbol.starts_with("SZSE.") || symbol.starts_with("BSE.")
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_TEST_SYMBOL").unwrap_or_else(|_| "SHFE.ao2609".to_string());

    let builder = SessionClientBuilder::new(user, pass);
    let session = if is_stock_symbol(symbol.as_str()) {
        builder.stock_market()
    } else {
        builder.futures_market()
    }
    .build()?;

    session
        .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
            symbols: vec![Symbol::new(symbol.clone())],
        }))
        .await?;

    let quote = wait_for_quote_update(&session, symbol.as_str(), Duration::from_secs(30)).await?;

    println!(
        "quote ready symbol={} datetime={} last_price={}",
        symbol, quote.datetime, quote.last_price
    );
    Ok(())
}

async fn wait_for_quote_update(
    session: &tqsdk_session::SessionClient,
    symbol: &str,
    timeout: Duration,
) -> Result<Quote, Box<dyn Error>> {
    let reader = session.reader().clone();
    let mut cursor = reader.cursor();
    let deadline = Instant::now() + timeout;

    loop {
        while reader.next(&mut cursor).is_some() {
            if let Some(quote) = reader.read().decode_path::<Quote>(&["quotes", symbol])?
                && !quote.datetime.is_empty()
            {
                return Ok(quote);
            }
        }

        let now = Instant::now();
        if now >= deadline {
            return Err("timed out waiting for quote snapshot".into());
        }

        let progress = session
            .progress_once(Some(now + Duration::from_millis(250)))
            .await?;
        if !progress.is_progress() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
