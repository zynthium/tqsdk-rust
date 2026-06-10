#![cfg(feature = "live")]

use std::time::Duration;

use tqsdk_session::SessionClientBuilder;
use tqsdk_wait::TqApiBuilder;

#[tokio::test(flavor = "current_thread")]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS and market access"]
async fn live_quote_wait_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let symbol = read_env("TQ_TEST_SYMBOL").unwrap_or_else(|| "SHFE.ao2609".to_string());

    let mut api = build_api_for_symbol(auth_user, auth_pass, symbol.as_str())
        .build()
        .await
        .expect("live wait api should build");
    let quote = api
        .quote(symbol.as_str())
        .await
        .expect("quote should subscribe successfully");

    for _ in 0..12 {
        let Some(step) = api
            .step_until(Some(tokio::time::Instant::now() + Duration::from_secs(5)))
            .await
            .expect("step should not fail")
        else {
            continue;
        };
        if !step.is_changing(&quote) {
            continue;
        }

        let snapshot = quote.load().expect("quote snapshot should decode");
        assert!(!snapshot.instrument_id.is_empty());
        assert!(!snapshot.datetime.is_empty());
        return;
    }

    panic!("live quote wait smoke did not observe a quote update before timeout");
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS, stock market access, and validates api.session() direct-query reuse"]
async fn live_quote_wait_with_session_query_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let symbol = read_env("TQ_QUERY_SYMBOL").unwrap_or_else(|| "SSE.000300".to_string());
    assert!(
        is_stock_symbol(symbol.as_str()),
        "TQ_QUERY_SYMBOL must be a stock symbol when query rides the official stock websocket"
    );

    let session_builder = SessionClientBuilder::new(auth_user, auth_pass)
        .stock_market()
        .enable_query();
    let mut api = TqApiBuilder::from_session_builder(session_builder)
        .build()
        .await
        .expect("live wait api should build");

    let metadata = api
        .session()
        .query_symbol_info(&[symbol.as_str()])
        .await
        .expect("query_symbol_info should succeed over the shared session");
    let instrument = metadata
        .first()
        .expect("query_symbol_info should return at least one row");
    assert!(!instrument.instrument_id.as_str().is_empty());

    let quote = api
        .quote(symbol.as_str())
        .await
        .expect("quote should subscribe successfully");

    for _ in 0..12 {
        let Some(step) = api
            .step_until(Some(tokio::time::Instant::now() + Duration::from_secs(5)))
            .await
            .expect("step should not fail")
        else {
            continue;
        };
        if !step.is_changing(&quote) {
            continue;
        }

        let snapshot = quote.load().expect("quote snapshot should decode");
        assert!(!snapshot.instrument_id.is_empty());
        assert!(!snapshot.datetime.is_empty());
        return;
    }

    panic!(
        "live quote wait-with-session-query smoke did not observe a quote update before timeout"
    );
}

fn build_api_for_symbol(auth_user: String, auth_pass: String, symbol: &str) -> TqApiBuilder {
    let builder = TqApiBuilder::new(auth_user, auth_pass);
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
