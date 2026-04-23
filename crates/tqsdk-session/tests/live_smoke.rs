use tqsdk_session::SessionClientBuilder;

#[tokio::test(flavor = "current_thread")]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS and query access"]
async fn live_query_symbol_info_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let symbol = read_env("TQ_TEST_SYMBOL").unwrap_or_else(|| "SHFE.ao2609".to_string());

    let session = SessionClientBuilder::new(auth_user, auth_pass)
        .enable_query()
        .build()
        .expect("live session should build");
    let quotes = session
        .query_symbol_info(&[symbol.as_str()])
        .await
        .expect("query_symbol_info should succeed");
    let quote = quotes
        .into_iter()
        .next()
        .expect("query_symbol_info should return at least one row");

    assert!(!quote.instrument_id.is_empty());
    assert!(!quote.ins_class.is_empty());
    assert!(quote.price_tick.is_finite());
}

fn read_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
