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

#[tokio::test(flavor = "current_thread")]
async fn trade_risk_and_notification_refs_report_changes_after_wait_update() {
    let mut api = support::seeded_api();

    support::seed_trade_extended_snapshot(&mut api, "sim", "SHFE.ao2602");
    let pre_insert = api.get_pre_insert_order("sim", "pre-1");
    let rule = api.get_risk_management_rule("sim", "SSE");
    let data = api.get_risk_management_data("sim", "SHFE.ao2602");
    let settlement = api.get_settlement_info("sim", "20260420");

    assert!(api.wait_update(None).await.unwrap());
    assert!(api.is_changing(&pre_insert).unwrap());
    assert!(
        api.is_changing_fields(&pre_insert, &["pre_margin"])
            .unwrap()
    );
    assert!(api.is_changing(&rule).unwrap());
    assert!(api.is_changing(&data).unwrap());
    assert!(api.is_changing(&settlement).unwrap());

    support::seed_risk_management_rule_nested_update(&mut api, "sim", "SSE", 4);
    support::seed_risk_management_data_nested_update(&mut api, "sim", "SHFE.ao2602", 16);

    assert!(api.wait_update(None).await.unwrap());
    assert!(api.is_changing(&rule).unwrap());

    assert!(api.wait_update(None).await.unwrap());
    assert!(api.is_changing(&data).unwrap());

    let notification = api.get_notification("notify-1");
    support::seed_notification_commit(&mut api, "notify-1");

    assert!(api.wait_update(None).await.unwrap());
    assert!(api.is_changing(&notification).unwrap());
    assert!(api.is_changing_fields(&notification, &["content"]).unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn security_refs_report_changes_after_wait_update() {
    let mut api = support::seeded_api();
    let account = api.get_security_account("stock-sim");
    let position = api.get_security_position("stock-sim", "SSE.600000");
    let order = api.get_security_order("stock-sim", "stock-order-1");
    let trade = api.get_security_trade("stock-sim", "stock-trade-1");

    support::seed_security_trade_snapshot(&mut api, "stock-sim", "SSE.600000");

    assert!(api.wait_update(None).await.unwrap());
    assert!(api.is_changing(&account).unwrap());
    assert!(api.is_changing(&position).unwrap());
    assert!(api.is_changing(&order).unwrap());
    assert!(api.is_changing_fields(&order, &["limit_price"]).unwrap());
    assert!(api.is_changing(&trade).unwrap());
}
