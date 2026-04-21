mod support;

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
    support::seed_ready_kline_chart(&mut api, "SHFE.au2602", 60_000_000_000, 64);

    let serial = api
        .get_kline_serial("SHFE.au2602", std::time::Duration::from_secs(60), 64)
        .await
        .unwrap();
    assert!(serial.is_ready(&api).unwrap());
    assert!(api.wait_update(None).await.unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn get_tick_serial_uses_chart_ready_semantics_and_preserves_commit_for_user() {
    let mut api = support::seeded_api();
    support::seed_ready_tick_chart(&mut api, "SHFE.au2602", 32);

    let serial = api.get_tick_serial("SHFE.au2602", 32).await.unwrap();
    let window = serial.load(&api).unwrap();

    assert!(serial.is_ready(&api).unwrap());
    assert_eq!(window.symbol(), "SHFE.au2602");
    assert_eq!(window.view_width(), 32);
    assert_eq!(window.last().unwrap().last_price, 618.5);
    assert!(api.wait_update(None).await.unwrap());
}
