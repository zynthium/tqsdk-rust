use std::error::Error;

use futures::StreamExt;
use tqsdk_session::SessionClientBuilder;
use tqsdk_stream::TqStreamBuilder;

fn read_env(key: &str) -> Result<String, Box<dyn Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_QUERY_SYMBOL").unwrap_or_else(|_| "SSE.000300".to_string());
    let stream_once = std::env::var_os("TQ_STREAM_ONCE").is_some();

    if !is_stock_symbol(symbol.as_str()) {
        return Err("quote_stream_with_session_query requires a stock symbol unless an explicit HTTP query route is configured".into());
    }

    let session_builder = SessionClientBuilder::new(user, pass)
        .stock_market()
        .enable_query();
    let stream = TqStreamBuilder::from_session_builder(session_builder)
        .build()
        .await?;

    let metadata = stream
        .session()
        .query_symbol_info(&[symbol.as_str()])
        .await?;
    let instrument = metadata
        .first()
        .ok_or("query_symbol_info returned no rows")?;
    println!(
        "metadata {} {} tick={:?}",
        instrument.instrument_id, instrument.ins_class, instrument.price_tick
    );

    stream.subscribe_quotes([symbol.as_str()]).await?;
    let mut quotes = stream.quote_stream(symbol.as_str())?;

    loop {
        let update = quotes.next().await.ok_or("quote stream closed")??;
        println!(
            "revision={:?} symbol={} datetime={} last_price={}",
            update.commit.revision,
            update.value.instrument_id,
            update.value.datetime,
            update.value.last_price
        );

        if stream_once {
            break;
        }
    }

    Ok(())
}

fn is_stock_symbol(symbol: &str) -> bool {
    symbol.starts_with("SSE.") || symbol.starts_with("SZSE.") || symbol.starts_with("BSE.")
}
