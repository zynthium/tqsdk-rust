mod support;

#[tokio::test(flavor = "current_thread")]
async fn quote_change_is_visible_after_wait_update() {
    let mut api = support::seeded_api();
    let quote = api.quote_ref("SHFE.au2602");
    support::seed_quote_commit(&mut api, "SHFE.au2602", 619.0);

    assert!(api.wait_update(None).await.unwrap());
    assert!(api.is_changing(&quote).unwrap());
    assert!(api.is_changing_fields(&quote, &["last_price"]).unwrap());
    assert!(!api.is_changing_fields(&quote, &["ask_price1"]).unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn kline_and_tick_serial_changes_are_visible_after_wait_update() {
    let mut api = support::seeded_api();

    support::seed_ready_kline_chart(&mut api, "SHFE.au2602", 60_000_000_000, 32);
    let klines = api
        .get_kline_serial("SHFE.au2602", std::time::Duration::from_secs(60), 32)
        .await
        .unwrap();

    assert!(api.wait_update(None).await.unwrap());
    assert!(api.is_changing(&klines).unwrap());

    support::seed_ready_tick_chart(&mut api, "SHFE.au2602", 32);
    let ticks = api.get_tick_serial("SHFE.au2602", 32).await.unwrap();

    assert!(api.wait_update(None).await.unwrap());
    assert!(api.is_changing(&ticks).unwrap());
}
