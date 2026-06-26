use chrono::{FixedOffset, TimeZone};
use tqsdk_relay::{RelayEngine, RelayTickRow};

fn tick(id: i64) -> RelayTickRow {
    RelayTickRow {
        id,
        datetime: id,
        last_price: 610.0 + id as f64,
        volume: id * 10,
        open_interest: 100 + id,
    }
}

fn local_millis_at(hour: u32, minute: u32, second: u32) -> u64 {
    let timestamp = FixedOffset::east_opt(8 * 3600)
        .unwrap()
        .with_ymd_and_hms(2026, 6, 23, hour, minute, second)
        .single()
        .expect("fixed China test time should be valid")
        .timestamp_millis();
    u64::try_from(timestamp).expect("local test time should be after unix epoch")
}

#[test]
fn read_model_keeps_global_summary_when_page_is_filtered() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let now = local_millis_at(9, 30, 0);
    engine.record_universe_refresh_success_for_symbols(
        ["SHFE.au2602", "DCE.m2609"],
        21,
        Some(32_000),
        None,
        now / 1_000 - 2,
    );
    engine
        .ingest_tick_at_for_test("SHFE.au2602", tick(1), now - 1_000)
        .unwrap();

    let query = tqsdk_relay::SymbolMetricsQuery {
        statuses: vec![tqsdk_relay::SymbolStatus::Live],
        limit: Some(1),
        ..Default::default()
    };
    let inputs = engine.dashboard_snapshot_inputs_at(now);
    let dashboard = inputs.dashboard_snapshot(&query);

    assert_eq!(dashboard.received_at_unix_millis, now);
    assert_eq!(dashboard.metrics.upstream_symbols, 2);
    assert_eq!(dashboard.global.total, 2);
    assert_eq!(dashboard.global.live, 1);
    assert_eq!(dashboard.global.initializing, 1);
    assert_eq!(dashboard.global.missing, 0);
    assert_eq!(dashboard.timeline.global.total, 2);
    assert_eq!(dashboard.timeline.global.problem, 0);
    assert_eq!(dashboard.page.filtered_total, 1);
    assert_eq!(dashboard.page.symbols.len(), 1);
    assert_eq!(dashboard.page.symbols[0].symbol, "SHFE.au2602");
}

#[test]
fn read_model_exposes_aggregate_timeline_without_global_symbol_rows() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let now = local_millis_at(9, 30, 0);
    engine.record_universe_refresh_success_for_symbols(
        ["SHFE.au2602", "DCE.m2609"],
        21,
        Some(32_000),
        None,
        now / 1_000 - 2,
    );
    engine
        .ingest_tick_at_for_test("SHFE.au2602", tick(1), now - 1_000)
        .unwrap();

    let query = tqsdk_relay::SymbolMetricsQuery {
        limit: Some(1),
        ..Default::default()
    };
    let inputs = engine.dashboard_snapshot_inputs_at(now);
    let dashboard = inputs.dashboard_snapshot(&query);

    assert_eq!(dashboard.global.total, 2);
    assert_eq!(dashboard.page.symbols.len(), 1);
    assert_eq!(dashboard.timeline.global.total, 2);
    assert!(dashboard.timeline.exchanges.contains_key("SHFE"));
    assert!(dashboard.timeline.exchanges.contains_key("DCE"));
}

#[test]
fn read_model_exposes_unfiltered_timeline_symbol_rows() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let now = local_millis_at(9, 30, 0);
    engine.record_universe_refresh_success_for_symbols(
        ["SHFE.au2602", "DCE.m2609"],
        21,
        Some(32_000),
        None,
        now / 1_000 - 2,
    );
    engine
        .ingest_tick_at_for_test("SHFE.au2602", tick(1), now - 1_000)
        .unwrap();

    let query = tqsdk_relay::SymbolMetricsQuery {
        limit: Some(1),
        ..Default::default()
    };
    let inputs = engine.dashboard_snapshot_inputs_at(now);
    let dashboard = inputs.dashboard_snapshot(&query);

    assert_eq!(dashboard.page.symbols.len(), 1);
    assert_eq!(dashboard.timeline.global.total, 2);
    assert_eq!(dashboard.timeline_symbols.len(), 2);
    assert!(
        dashboard
            .timeline_symbols
            .iter()
            .any(|row| row.symbol == "DCE.m2609")
    );
}

#[test]
fn read_model_serializes_compact_page_rows() {
    let mut engine = RelayEngine::new_memory_only(256, 256);
    let now = local_millis_at(9, 30, 0);
    let symbols: Vec<String> = (0..200)
        .map(|index| format!("SHFE.au{:04}", 2600 + index))
        .collect();
    engine.record_universe_refresh_success_for_symbols(
        symbols.iter().map(String::as_str),
        21,
        Some(32_000),
        None,
        now / 1_000 - 2,
    );
    for (index, symbol) in symbols.iter().enumerate() {
        engine
            .ingest_tick_at_for_test(symbol, tick(i64::try_from(index + 1).unwrap()), now - 1_000)
            .unwrap();
    }

    let inputs = engine.dashboard_snapshot_inputs_at(now);
    let dashboard = inputs.dashboard_snapshot(&tqsdk_relay::SymbolMetricsQuery::default());
    let json = serde_json::to_value(&dashboard).unwrap();
    let first = json["page"]["symbols"][0].as_object().unwrap();

    assert_eq!(dashboard.page.symbols.len(), 200);
    assert_eq!(first["symbol"].as_str(), Some(symbols[0].as_str()));
    assert!(!first.contains_key("status"));
    assert!(!first.contains_key("coverage"));
    assert!(!first.contains_key("in_universe"));
    assert!(!first.contains_key("last_receive_unix_millis"));
    assert!(!first.contains_key("last_tick_datetime_ns"));
    assert!(
        serde_json::to_vec(&dashboard).unwrap().len() < 70_000,
        "dashboard snapshot should stay compact for 200 symbols"
    );
}

#[test]
fn read_model_groups_continuous_contracts_by_underlying_exchange() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let now = local_millis_at(9, 30, 0);
    engine.record_universe_refresh_success_for_symbols(
        ["KQ.i@DCE.m", "KQ.m@SHFE.au"],
        24,
        Some(32_000),
        None,
        now / 1_000 - 2,
    );

    let inputs = engine.dashboard_snapshot_inputs_at(now);
    let dashboard = inputs.dashboard_snapshot(&tqsdk_relay::SymbolMetricsQuery::default());

    assert!(dashboard.timeline.exchanges.contains_key("DCE"));
    assert!(dashboard.timeline.exchanges.contains_key("SHFE"));
    assert!(!dashboard.timeline.exchanges.contains_key("KQ"));
}

#[test]
fn read_model_inputs_are_classified_after_detached_copy() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let now = local_millis_at(9, 30, 0);
    engine.record_universe_refresh_success_for_symbols(
        ["SHFE.au2602"],
        11,
        None,
        None,
        now / 1_000 - 2,
    );

    let inputs = engine.dashboard_snapshot_inputs_at(now);
    engine.record_universe_refresh_success_for_symbols(
        ["DCE.m2609"],
        10,
        None,
        None,
        now / 1_000 - 1,
    );

    let dashboard = inputs.dashboard_snapshot(&tqsdk_relay::SymbolMetricsQuery::default());

    assert_eq!(dashboard.global.total, 1);
    assert_eq!(dashboard.timeline.global.total, 1);
    assert!(dashboard.timeline.exchanges.contains_key("SHFE"));
}
