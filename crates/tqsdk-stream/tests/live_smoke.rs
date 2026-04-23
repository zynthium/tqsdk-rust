use std::time::Duration;

use futures::StreamExt;
use tqsdk_core::{MarketCommand, RuntimeCommand, Symbol};
use tqsdk_stream::TqStreamBuilder;

#[tokio::test(flavor = "current_thread")]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS and market access"]
async fn live_quote_stream_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let symbol = read_env("TQ_TEST_SYMBOL").unwrap_or_else(|| "SHFE.ao2609".to_string());

    let stream = build_stream_for_symbol(auth_user, auth_pass, symbol.as_str())
        .build()
        .await
        .expect("live stream facade should build");
    stream
        .session()
        .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
            symbols: vec![Symbol::new(symbol.clone())],
        }))
        .await
        .expect("SubscribeQuotes should succeed");

    let mut quotes = stream
        .quote_stream(symbol.as_str())
        .expect("quote_stream should construct");
    let update = tokio::time::timeout(Duration::from_secs(30), quotes.next())
        .await
        .expect("quote stream should produce an item before timeout")
        .expect("quote stream should stay open")
        .expect("quote stream item should decode");

    assert!(!update.value.instrument_id.is_empty());
    assert!(!update.value.datetime.is_empty());
}

fn build_stream_for_symbol(auth_user: String, auth_pass: String, symbol: &str) -> TqStreamBuilder {
    let builder = TqStreamBuilder::new(auth_user, auth_pass);
    if is_stock_symbol(symbol) {
        builder.stock_market()
    } else {
        builder.futures_market()
    }
}

fn is_stock_symbol(symbol: &str) -> bool {
    symbol.starts_with("SSE.") || symbol.starts_with("SZSE.") || symbol.starts_with("BSE.")
}

fn read_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
