use std::error::Error;

use futures::StreamExt;
use tqsdk_core::{MarketCommand, RuntimeCommand, Symbol};
use tqsdk_stream::TqStreamBuilder;

fn read_env(key: &str) -> Result<String, Box<dyn Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_TEST_SYMBOL").unwrap_or_else(|_| "SHFE.ao2609".to_string());
    let stream_once = std::env::var_os("TQ_STREAM_ONCE").is_some();

    let stream = TqStreamBuilder::new(user, pass).build().await?;
    stream
        .session()
        .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
            symbols: vec![Symbol::new(symbol.clone())],
        }))
        .await?;

    let mut quotes = stream.quote_stream(&symbol)?;

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
