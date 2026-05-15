mod support;

#[tokio::test(flavor = "current_thread")]
async fn quote_change_is_visible_after_wait_update() {
    let mut api = support::seeded_api();
    let quote = api.quote("SHFE.au2602").await.unwrap();
    support::seed_quote_commit(&mut api, "SHFE.au2602", 619.0);

    let step = api.step().await.unwrap().expect("quote commit");
    assert!(step.is_changing(&quote));
    assert!(step.is_changing_fields(&quote, &["last_price"]));
    assert!(!step.is_changing_fields(&quote, &["ask_price1"]));
}

#[tokio::test(flavor = "current_thread")]
async fn is_changing_is_false_until_a_commit_is_consumed() {
    let mut api = support::seeded_api();
    let quote = api.quote("SHFE.au2602").await.unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(10);

    assert!(api.step_until(Some(deadline)).await.unwrap().is_none());
    support::seed_quote_commit(&mut api, "SHFE.au2602", 619.0);

    assert!(api.last_commit().is_none());

    let step = api.step().await.unwrap().expect("quote commit");
    assert!(step.is_changing(&quote));
    assert!(step.is_changing_fields(&quote, &["last_price"]));
}

#[tokio::test(flavor = "current_thread")]
async fn is_changing_ignores_unrelated_commit_paths() {
    let mut api = support::seeded_api();
    let quote = api.quote("SHFE.au2602").await.unwrap();

    support::seed_quote_commit(&mut api, "SHFE.ag2602", 8_000.0);

    let step = api.step().await.unwrap().expect("unrelated quote commit");
    assert!(!step.is_changing(&quote));
    assert!(!step.is_changing_fields(&quote, &["last_price"]));
}

#[tokio::test(flavor = "current_thread")]
async fn kline_and_tick_serial_changes_are_visible_after_wait_update() {
    let mut api = support::seeded_api();

    support::seed_ready_kline_chart(&mut api, "SHFE.au2602", 60_000_000_000, 32);
    let klines = api
        .kline("SHFE.au2602", std::time::Duration::from_secs(60), 32)
        .await
        .unwrap();

    let step = api.step().await.unwrap().expect("kline chart commit");
    assert!(step.is_changing(&klines));

    support::seed_ready_tick_chart(&mut api, "SHFE.au2602", 32);
    let ticks = api.tick("SHFE.au2602", 32).await.unwrap();

    let step = api.step().await.unwrap().expect("tick chart commit");
    assert!(step.is_changing(&ticks));
}

#[tokio::test(flavor = "current_thread")]
async fn serial_refs_report_data_row_field_changes() {
    let mut api = support::seeded_api();

    support::seed_ready_kline_chart(&mut api, "SHFE.au2602", 60_000_000_000, 32);
    let klines = api
        .kline("SHFE.au2602", std::time::Duration::from_secs(60), 32)
        .await
        .unwrap();
    assert!(api.step().await.unwrap().is_some());

    support::seed_kline_row_update(&mut api, "SHFE.au2602", 60_000_000_000, 101, 621.5);
    let step = api.step().await.unwrap().expect("kline row update");
    assert!(step.is_changing(&klines));
    assert!(step.is_changing_fields(&klines, &["close"]));
    assert!(!step.is_changing_fields(&klines, &["open"]));

    support::seed_ready_tick_chart(&mut api, "SHFE.au2602", 32);
    let ticks = api.tick("SHFE.au2602", 32).await.unwrap();
    assert!(api.step().await.unwrap().is_some());

    support::seed_tick_row_update(&mut api, "SHFE.au2602", 201, 619.5);
    let step = api.step().await.unwrap().expect("tick row update");
    assert!(step.is_changing(&ticks));
    assert!(step.is_changing_fields(&ticks, &["last_price"]));
    assert!(!step.is_changing_fields(&ticks, &["volume"]));
}

#[tokio::test(flavor = "current_thread")]
async fn trade_risk_and_notification_refs_report_changes_after_wait_update() {
    let mut api = support::seeded_api();

    support::seed_trade_extended_snapshot(&mut api, "sim", "SHFE.ao2602");
    let pre_insert = api.pre_insert_order("sim", "pre-1");
    let rule = api.risk_management_rule("sim", "SSE");
    let data = api.risk_management_data("sim", "SHFE.ao2602");
    let settlement = api.settlement_info("sim", "20260420");

    let step = api.step().await.unwrap().expect("trade extended snapshot");
    assert!(step.is_changing(&pre_insert));
    assert!(step.is_changing_fields(&pre_insert, &["pre_margin"]));
    assert!(step.is_changing(&rule));
    assert!(step.is_changing(&data));
    assert!(step.is_changing(&settlement));

    support::seed_risk_management_rule_nested_update(&mut api, "sim", "SSE", 4);
    support::seed_risk_management_data_nested_update(&mut api, "sim", "SHFE.ao2602", 16);

    let step = api.step().await.unwrap().expect("risk rule update");
    assert!(step.is_changing(&rule));

    let step = api.step().await.unwrap().expect("risk data update");
    assert!(step.is_changing(&data));

    let notification = api.notification("notify-1");
    support::seed_notification_commit(&mut api, "notify-1");

    let step = api.step().await.unwrap().expect("notification update");
    assert!(step.is_changing(&notification));
    assert!(step.is_changing_fields(&notification, &["content"]));
}

#[tokio::test(flavor = "current_thread")]
async fn security_refs_report_changes_after_wait_update() {
    let mut api = support::seeded_api();
    let account = api.security_account("stock-sim");
    let position = api.security_position("stock-sim", "SSE.600000");
    let order = api.security_order("stock-sim", "stock-order-1");
    let trade = api.security_trade("stock-sim", "stock-trade-1");

    support::seed_security_trade_snapshot(&mut api, "stock-sim", "SSE.600000");

    let step = api.step().await.unwrap().expect("security snapshot");
    assert!(step.is_changing(&account));
    assert!(step.is_changing(&position));
    assert!(step.is_changing(&order));
    assert!(step.is_changing_fields(&order, &["limit_price"]));
    assert!(step.is_changing(&trade));
}
