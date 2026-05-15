mod support;

fn compact_source(source: &str) -> String {
    source.split_whitespace().collect::<String>()
}

#[test]
fn market_refs_read_market_partitions_instead_of_full_snapshot() {
    let quote_ref = include_str!("../src/refs/quote.rs");
    let trading_status_ref = include_str!("../src/refs/trading_status.rs");
    let kline_ref = include_str!("../src/refs/kline.rs");
    let tick_ref = include_str!("../src/refs/tick.rs");

    assert!(quote_ref.contains("read_market_state()"));
    assert!(trading_status_ref.contains("read_market_state()"));
    assert!(kline_ref.contains("read_market_state()"));
    assert!(tick_ref.contains("read_market_state()"));
    assert!(!compact_source(quote_ref).contains("reader.read()"));
    assert!(!compact_source(trading_status_ref).contains("reader.read()"));
    assert!(!compact_source(kline_ref).contains("reader.read()"));
    assert!(!compact_source(tick_ref).contains("reader.read()"));
}

#[tokio::test(flavor = "current_thread")]
async fn get_quote_returns_ref_without_waiting_for_first_tick() {
    let mut api = support::seeded_api();
    let quote = api.get_quote("SHFE.au2602").await.unwrap();
    assert!(!quote.is_ready(&api).unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn quote_snapshot_returns_ready_quote_without_manual_wait_loop() {
    let mut api = support::seeded_api();
    support::seed_quote_commit_with_datetime(
        &mut api,
        "SHFE.au2602",
        618.0,
        "2024-04-22 09:00:00.000000",
    );

    let quote = api.quote_snapshot("SHFE.au2602", None).await.unwrap();

    assert_eq!(quote.instrument_id, "SHFE.au2602");
    assert_eq!(quote.datetime, "2024-04-22 09:00:00.000000");
    assert_eq!(quote.last_price, 618.0);
}

#[tokio::test(flavor = "current_thread")]
async fn quote_snapshot_reports_not_ready_when_deadline_expires() {
    let mut api = support::seeded_api();

    let error = api
        .quote_snapshot(
            "SHFE.au2602",
            Some(tokio::time::Instant::now() + std::time::Duration::from_millis(10)),
        )
        .await
        .unwrap_err();

    assert_eq!(
        error,
        tqsdk_wait::WaitFacadeError::InvalidState("quote snapshot not ready")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn startup_recovery_waits_for_quote_and_trade_sync_without_manual_flags() {
    let mut api = support::seeded_api();
    support::seed_quote_commit_with_datetime(
        &mut api,
        "SHFE.au2602",
        618.0,
        "2026-04-26 09:00:00.000000",
    );
    support::seed_trade_snapshot(&mut api, "sim", "SHFE.au2602");

    let status = api
        .startup_recovery()
        .quotes(["SHFE.au2602"])
        .trade_account("sim")
        .await
        .unwrap();

    assert!(status.is_ready());
    assert!(status.market_ready);
    assert!(status.trade_ready);
    assert!(status.missing_quotes.is_empty());
    assert!(status.pending_trade_accounts.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn startup_recovery_reports_not_ready_when_deadline_expires() {
    let mut api = support::seeded_api();

    let error = api
        .startup_recovery()
        .quotes(["SHFE.au2602"])
        .deadline(tokio::time::Instant::now() + std::time::Duration::from_millis(10))
        .await
        .unwrap_err();

    assert_eq!(
        error,
        tqsdk_wait::WaitFacadeError::InvalidState("startup recovery not ready")
    );
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
    let window = serial.load(&api).unwrap();
    assert!(serial.is_ready(&api).unwrap());
    assert_eq!(window.symbol(), "SHFE.au2602");
    assert_eq!(window.len(), 2);
    assert_eq!(window.last().unwrap().close, 620.0);

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
async fn get_kline_serial_validates_length_and_clamps_large_length() {
    let mut api = support::seeded_api();

    let zero_duration = api
        .get_kline_serial("SHFE.au2602", std::time::Duration::ZERO, 32)
        .await
        .unwrap_err();
    assert_eq!(
        zero_duration,
        tqsdk_wait::WaitFacadeError::InvalidState("kline duration must be positive")
    );

    let zero_length = api
        .get_kline_serial("SHFE.au2602", std::time::Duration::from_secs(60), 0)
        .await
        .unwrap_err();
    assert_eq!(
        zero_length,
        tqsdk_wait::WaitFacadeError::InvalidState("serial data_length must be greater than zero")
    );

    support::seed_ready_kline_chart(&mut api, "SHFE.au2602", 60_000_000_000, 10_000);
    let serial = api
        .get_kline_serial("SHFE.au2602", std::time::Duration::from_secs(60), 20_000)
        .await
        .unwrap();

    assert!(api.is_serial_ready(&serial).unwrap());
    assert_eq!(serial.load(&api).unwrap().view_width(), 10_000);
}

#[tokio::test(flavor = "current_thread")]
async fn get_kline_serial_reuses_existing_chart_without_resubmitting_set_chart() {
    let mut api = support::seeded_api();

    support::seed_ready_kline_chart(&mut api, "SHFE.au2602", 60_000_000_000, 64);
    let _first = api
        .get_kline_serial("SHFE.au2602", std::time::Duration::from_secs(60), 64)
        .await
        .unwrap();
    let first_dispatch_count = api.session().handle().drain_dispatches().unwrap().len();

    let _second = api
        .get_kline_serial("SHFE.au2602", std::time::Duration::from_secs(60), 64)
        .await
        .unwrap();
    let second_dispatch_count = api.session().handle().drain_dispatches().unwrap().len();

    assert!(first_dispatch_count > 0);
    assert_eq!(second_dispatch_count, 0);
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
    assert_eq!(window.len(), 2);
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

#[tokio::test(flavor = "current_thread")]
async fn get_tick_serial_validates_length_and_clamps_large_length() {
    let mut api = support::seeded_api();

    let zero_length = api.get_tick_serial("SHFE.au2602", 0).await.unwrap_err();
    assert_eq!(
        zero_length,
        tqsdk_wait::WaitFacadeError::InvalidState("serial data_length must be greater than zero")
    );

    support::seed_ready_tick_chart(&mut api, "SHFE.au2602", 10_000);
    let serial = api.get_tick_serial("SHFE.au2602", 20_000).await.unwrap();

    assert!(api.is_serial_ready(&serial).unwrap());
    assert_eq!(serial.load(&api).unwrap().view_width(), 10_000);
}

#[tokio::test(flavor = "current_thread")]
async fn get_tick_serial_reuses_existing_chart_without_resubmitting_set_chart() {
    let mut api = support::seeded_api();

    support::seed_ready_tick_chart(&mut api, "SHFE.au2602", 64);
    let _first = api.get_tick_serial("SHFE.au2602", 64).await.unwrap();
    let first_dispatch_count = api.session().handle().drain_dispatches().unwrap().len();

    let _second = api.get_tick_serial("SHFE.au2602", 64).await.unwrap();
    let second_dispatch_count = api.session().handle().drain_dispatches().unwrap().len();

    assert!(first_dispatch_count > 0);
    assert_eq!(second_dispatch_count, 0);
}
