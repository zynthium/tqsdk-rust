mod support;

fn compact_source(source: &str) -> String {
    source.split_whitespace().collect::<String>()
}

#[test]
fn market_refs_read_market_partitions_instead_of_full_snapshot() {
    let quote_ref = include_str!("../src/refs/quote.rs");
    let trading_status_ref = include_str!("../src/refs/trading_status.rs");

    assert!(quote_ref.contains("read_market_state()"));
    assert!(trading_status_ref.contains("read_market_state()"));
    assert!(!compact_source(quote_ref).contains("reader.read()"));
    assert!(!compact_source(trading_status_ref).contains("reader.read()"));
}

#[tokio::test(flavor = "current_thread")]
async fn get_quote_returns_ref_without_waiting_for_first_tick() {
    let mut api = support::seeded_api();
    let quote = api.get_quote("SHFE.au2602").await.unwrap();
    assert!(!quote.is_ready(&api).unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn get_trading_status_returns_ref_without_blocking() {
    let mut api = support::seeded_api();
    let status = api.get_trading_status("SHFE.au2602").await.unwrap();
    assert!(status.load(&api).is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn get_kline_serial_waits_for_initial_ready_and_preserves_commit_for_user() {
    let mut api = support::seeded_api();
    let quote = api.get_quote("SHFE.au2602").await.unwrap();
    support::seed_quote_commit(&mut api, "SHFE.au2602", 618.0);
    assert!(api.wait_update(None).await.unwrap());
    let previous_revision = api
        .last_commit()
        .expect("seed quote commit should be visible")
        .revision;
    assert!(api.is_changing(&quote).unwrap());

    support::seed_ready_kline_chart(&mut api, "SHFE.au2602", 60_000_000_000, 64);

    let serial = api
        .get_kline_serial("SHFE.au2602", std::time::Duration::from_secs(60), 64)
        .await
        .unwrap();
    assert!(serial.is_ready(&api).unwrap());

    assert_eq!(
        api.last_commit().map(|commit| commit.revision),
        Some(previous_revision)
    );
    assert!(api.is_changing(&quote).unwrap());
    assert!(api.wait_update(None).await.unwrap());
    assert_ne!(
        api.last_commit().map(|commit| commit.revision),
        Some(previous_revision)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn get_tick_serial_uses_chart_ready_semantics_and_preserves_commit_for_user() {
    let mut api = support::seeded_api();
    let quote = api.get_quote("SHFE.au2602").await.unwrap();
    support::seed_quote_commit(&mut api, "SHFE.au2602", 618.0);
    assert!(api.wait_update(None).await.unwrap());
    let previous_revision = api
        .last_commit()
        .expect("seed quote commit should be visible")
        .revision;
    assert!(api.is_changing(&quote).unwrap());

    support::seed_ready_tick_chart(&mut api, "SHFE.au2602", 32);

    let serial = api.get_tick_serial("SHFE.au2602", 32).await.unwrap();
    let window = serial.load(&api).unwrap();

    assert!(serial.is_ready(&api).unwrap());
    assert_eq!(window.symbol(), "SHFE.au2602");
    assert_eq!(window.view_width(), 32);
    assert_eq!(window.last().unwrap().last_price, 618.5);

    assert_eq!(
        api.last_commit().map(|commit| commit.revision),
        Some(previous_revision)
    );
    assert!(api.is_changing(&quote).unwrap());
    assert!(api.wait_update(None).await.unwrap());
    assert_ne!(
        api.last_commit().map(|commit| commit.revision),
        Some(previous_revision)
    );
}
