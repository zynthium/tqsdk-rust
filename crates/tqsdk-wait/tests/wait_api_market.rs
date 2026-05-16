use std::time::Duration;

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
async fn quote_handle_returns_ref_without_waiting_for_first_tick() {
    let mut api = support::seeded_api();
    let quote = api.quote("SHFE.au2602").await.unwrap();
    assert!(!quote.is_ready().unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn quote_handle_reads_snapshot_without_api_argument_after_step() {
    let mut api = support::seeded_api();
    support::seed_quote_commit_with_datetime(
        &mut api,
        "SHFE.au2602",
        618.0,
        "2024-04-22 09:00:00.000000",
    );

    let quote = api.quote("SHFE.au2602").await.unwrap();
    let step = api
        .step()
        .await
        .unwrap()
        .expect("seed commit should produce step");

    assert!(step.is_changing(&quote));
    let snapshot = quote.load().unwrap();
    assert_eq!(snapshot.instrument_id, "SHFE.au2602");
    assert_eq!(snapshot.datetime, "2024-04-22 09:00:00.000000");
    assert_eq!(snapshot.last_price, 618.0);
}

#[tokio::test(flavor = "current_thread")]
async fn quote_handle_returns_changed_snapshot_for_matching_step() {
    let mut api = support::seeded_api();
    support::seed_quote_commit_with_datetime(
        &mut api,
        "SHFE.au2602",
        618.0,
        "2024-04-22 09:00:00.000000",
    );

    let quote = api.quote("SHFE.au2602").await.unwrap();
    let other_quote = api.quote("SHFE.ag2602").await.unwrap();
    let step = api
        .step()
        .await
        .unwrap()
        .expect("seed commit should produce step");

    assert_eq!(
        quote.changed_snapshot(&step).unwrap().unwrap().last_price,
        618.0
    );
    assert!(other_quote.changed_snapshot(&step).unwrap().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn quote_step_until_reports_not_ready_when_deadline_expires() {
    let mut api = support::seeded_api();
    let quote = api.quote("SHFE.au2602").await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(10);

    let ready = loop {
        match api.step_until(Some(deadline)).await.unwrap() {
            Some(step) if step.is_changing(&quote) && quote.snapshot().unwrap().is_some() => {
                break true;
            }
            Some(_) => {}
            None => break false,
        }
    };

    assert!(!ready);
    assert_eq!(
        quote.load().unwrap_err(),
        tqsdk_wait::WaitFacadeError::InvalidState("quote not ready")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn trading_status_handle_returns_ref_without_blocking() {
    let mut api = support::seeded_api();
    let status = api.trading_status("SHFE.au2602").await.unwrap();
    assert_eq!(
        status.load().unwrap_err(),
        tqsdk_wait::WaitFacadeError::InvalidState("trading status not ready")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn kline_handle_reads_bounded_window_without_api_argument_after_step() {
    let mut api = support::seeded_api();
    support::seed_ready_kline_chart(&mut api, "SHFE.au2602", 60_000_000_000, 64);

    let bars = api
        .kline("SHFE.au2602", Duration::from_secs(60), 64)
        .await
        .unwrap();
    let step = api
        .step()
        .await
        .unwrap()
        .expect("chart commit should produce step");

    assert!(step.is_changing(&bars));
    let window = bars.window().unwrap();
    assert_eq!(window.symbol(), "SHFE.au2602");
    assert_eq!(window.view_width(), 64);
    assert_eq!(window.len(), 2);
    assert!(
        window
            .rows()
            .iter()
            .all(|row| row.id >= 100 && row.id <= 101)
    );
    assert_eq!(window.last().unwrap().close, 620.0);
}

#[tokio::test(flavor = "current_thread")]
async fn kline_handle_exposes_last_rows_since_and_changed_rows() {
    let mut api = support::seeded_api();

    support::seed_ready_kline_chart(&mut api, "SHFE.au2602", 60_000_000_000, 32);
    let klines = api
        .kline("SHFE.au2602", Duration::from_secs(60), 32)
        .await
        .unwrap();

    let ready_step = api.step().await.unwrap().expect("kline chart commit");
    assert_eq!(klines.last().unwrap().unwrap().id, 101);
    assert_eq!(klines.last_completed().unwrap().unwrap().id, 100);
    assert_eq!(
        klines
            .rows_since(100)
            .unwrap()
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![101]
    );
    assert_eq!(
        klines
            .changed_rows(&ready_step)
            .unwrap()
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![100, 101]
    );

    support::seed_kline_row_update(&mut api, "SHFE.au2602", 60_000_000_000, 101, 621.5);
    let update_step = api.step().await.unwrap().expect("kline row update");
    let changed = klines.changed_rows(&update_step).unwrap();
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].id, 101);
    assert_eq!(changed[0].close, 621.5);
}

#[tokio::test(flavor = "current_thread")]
async fn kline_handle_validates_length_and_clamps_large_length() {
    let mut api = support::seeded_api();

    let zero_duration = api
        .kline("SHFE.au2602", Duration::ZERO, 32)
        .await
        .unwrap_err();
    assert_eq!(
        zero_duration,
        tqsdk_wait::WaitFacadeError::InvalidState("kline duration must be positive")
    );

    let zero_length = api
        .kline("SHFE.au2602", Duration::from_secs(60), 0)
        .await
        .unwrap_err();
    assert_eq!(
        zero_length,
        tqsdk_wait::WaitFacadeError::InvalidState("serial data_length must be greater than zero")
    );

    support::seed_ready_kline_chart(&mut api, "SHFE.au2602", 60_000_000_000, 10_000);
    let bars = api
        .kline("SHFE.au2602", Duration::from_secs(60), 20_000)
        .await
        .unwrap();

    assert!(bars.is_ready().unwrap());
    assert_eq!(bars.window().unwrap().view_width(), 10_000);
}

#[tokio::test(flavor = "current_thread")]
async fn kline_handle_reuses_existing_chart_without_resubmitting_set_chart() {
    let mut api = support::seeded_api();

    support::seed_ready_kline_chart(&mut api, "SHFE.au2602", 60_000_000_000, 64);
    let _first = api
        .kline("SHFE.au2602", Duration::from_secs(60), 64)
        .await
        .unwrap();
    let first_dispatch_count = api.session().handle().drain_dispatches().unwrap().len();

    let _second = api
        .kline("SHFE.au2602", Duration::from_secs(60), 64)
        .await
        .unwrap();
    let second_dispatch_count = api.session().handle().drain_dispatches().unwrap().len();

    assert!(first_dispatch_count > 0);
    assert_eq!(second_dispatch_count, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn tick_handle_reads_bounded_window_without_api_argument_after_step() {
    let mut api = support::seeded_api();
    support::seed_ready_tick_chart(&mut api, "SHFE.au2602", 32);

    let ticks = api.tick("SHFE.au2602", 32).await.unwrap();
    let step = api
        .step()
        .await
        .unwrap()
        .expect("chart commit should produce step");

    assert!(step.is_changing(&ticks));
    let window = ticks.window().unwrap();
    assert_eq!(window.symbol(), "SHFE.au2602");
    assert_eq!(window.view_width(), 32);
    assert_eq!(window.len(), 2);
    assert!(
        window
            .rows()
            .iter()
            .all(|row| row.id >= 200 && row.id <= 201)
    );
    assert_eq!(window.last().unwrap().last_price, 618.5);
}

#[tokio::test(flavor = "current_thread")]
async fn tick_handle_exposes_last_rows_since_and_changed_rows() {
    let mut api = support::seeded_api();

    support::seed_ready_tick_chart(&mut api, "SHFE.au2602", 32);
    let ticks = api.tick("SHFE.au2602", 32).await.unwrap();

    let ready_step = api.step().await.unwrap().expect("tick chart commit");
    assert_eq!(ticks.last().unwrap().unwrap().id, 201);
    assert_eq!(
        ticks
            .rows_since(200)
            .unwrap()
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![201]
    );
    assert_eq!(
        ticks
            .changed_rows(&ready_step)
            .unwrap()
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![200, 201]
    );

    support::seed_tick_row_update(&mut api, "SHFE.au2602", 201, 619.5);
    let update_step = api.step().await.unwrap().expect("tick row update");
    let changed = ticks.changed_rows(&update_step).unwrap();
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].id, 201);
    assert_eq!(changed[0].last_price, 619.5);
}

#[tokio::test(flavor = "current_thread")]
async fn tick_handle_validates_length_and_clamps_large_length() {
    let mut api = support::seeded_api();

    let zero_length = api.tick("SHFE.au2602", 0).await.unwrap_err();
    assert_eq!(
        zero_length,
        tqsdk_wait::WaitFacadeError::InvalidState("serial data_length must be greater than zero")
    );

    support::seed_ready_tick_chart(&mut api, "SHFE.au2602", 10_000);
    let ticks = api.tick("SHFE.au2602", 20_000).await.unwrap();

    assert!(ticks.is_ready().unwrap());
    assert_eq!(ticks.window().unwrap().view_width(), 10_000);
}

#[tokio::test(flavor = "current_thread")]
async fn tick_handle_reuses_existing_chart_without_resubmitting_set_chart() {
    let mut api = support::seeded_api();

    support::seed_ready_tick_chart(&mut api, "SHFE.au2602", 64);
    let _first = api.tick("SHFE.au2602", 64).await.unwrap();
    let first_dispatch_count = api.session().handle().drain_dispatches().unwrap().len();

    let _second = api.tick("SHFE.au2602", 64).await.unwrap();
    let second_dispatch_count = api.session().handle().drain_dispatches().unwrap().len();

    assert!(first_dispatch_count > 0);
    assert_eq!(second_dispatch_count, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn backtest_step_returns_none_after_end_datetime() {
    let mut api = support::backtest_api_for_test(1_000, 2_000);
    support::seed_replay_cursor_commit(&mut api, 2_000);

    let first = api.step().await.unwrap();
    assert!(first.is_some());
    assert_eq!(first.unwrap().current_dt(), Some(2_000));

    let second = api.step().await.unwrap();
    assert!(second.is_none());
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
        .deadline(tokio::time::Instant::now() + Duration::from_millis(10))
        .await
        .unwrap_err();

    assert_eq!(
        error,
        tqsdk_wait::WaitFacadeError::InvalidState("startup recovery not ready")
    );
}
