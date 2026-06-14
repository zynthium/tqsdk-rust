use std::collections::BTreeMap;

use chrono::{Datelike, NaiveDate, TimeZone};
use tqsdk_core::{Quote, TradingTime};
use tqsdk_relay::{
    RelayTickRow, SymbolCoverage, SymbolFlow, SymbolIntegrity, SymbolMetricsContext,
    SymbolMetricsQuery, SymbolProblemSeverity, SymbolSession, SymbolSort, SymbolStatus,
    SymbolSubscriptionCounts, SymbolTelemetryStore,
};

fn tick(id: i64, datetime_ns: i64, price: f64) -> RelayTickRow {
    RelayTickRow {
        id,
        datetime: datetime_ns,
        last_price: price,
        volume: id * 10,
        open_interest: 100 + id,
    }
}

fn quote(symbol: &str, datetime_ns: i64, price: f64) -> Quote {
    Quote {
        instrument_id: symbol.to_string(),
        datetime: datetime_ns.to_string(),
        last_price: price,
        volume: 12,
        open_interest: 34,
        ..Quote::default()
    }
}

fn quote_with_name(symbol: &str, instrument_name: &str, datetime_ns: i64, price: f64) -> Quote {
    Quote {
        instrument_name: instrument_name.to_string(),
        ..quote(symbol, datetime_ns, price)
    }
}

fn quote_with_trading_time(
    symbol: &str,
    datetime_ns: i64,
    price: f64,
    day: &[(&str, &str)],
    night: &[(&str, &str)],
) -> Quote {
    Quote {
        trading_time: TradingTime {
            day: day
                .iter()
                .map(|(start, end)| vec![(*start).to_string(), (*end).to_string()])
                .collect(),
            night: night
                .iter()
                .map(|(start, end)| vec![(*start).to_string(), (*end).to_string()])
                .collect(),
        },
        ..quote(symbol, datetime_ns, price)
    }
}

fn local_millis_at(hour: u32, minute: u32, second: u32) -> u64 {
    china_millis_at(
        NaiveDate::from_ymd_opt(2026, 6, 12).expect("test date should be valid"),
        hour,
        minute,
        second,
    )
}

fn china_millis_at(date: NaiveDate, hour: u32, minute: u32, second: u32) -> u64 {
    let timestamp = chrono::FixedOffset::east_opt(8 * 3600)
        .expect("china offset should be valid")
        .with_ymd_and_hms(date.year(), date.month(), date.day(), hour, minute, second)
        .single()
        .expect("local test time should be unambiguous")
        .timestamp_millis();
    u64::try_from(timestamp).expect("local test time should be after unix epoch")
}

#[test]
fn universe_symbol_without_tick_is_missing() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(9, 30, 0);
    store.record_universe(["SHFE.au2602"], now - 10_000);

    let snapshot = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );

    assert_eq!(snapshot.summary.total, 1);
    assert_eq!(snapshot.summary.missing, 1);
    assert_eq!(snapshot.symbols[0].symbol, "SHFE.au2602");
    assert_eq!(snapshot.symbols[0].status, SymbolStatus::Missing);
    assert_eq!(snapshot.symbols[0].session, SymbolSession::Unknown);
    assert_eq!(snapshot.symbols[0].flow, SymbolFlow::NoSample);
    assert_eq!(snapshot.symbols[0].integrity, SymbolIntegrity::Suspected);
    assert!(snapshot.symbols[0].receive_gap_ms.is_none());
}

#[test]
fn unobserved_symbol_inside_closed_session_is_closed_with_no_sample_flow() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(10, 20, 0);
    store.record_universe(["DCE.m2609", "KQ.i@DCE.m"], now - 10_000);

    let snapshot = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );

    assert_eq!(snapshot.summary.closed, 2);
    assert_eq!(snapshot.summary.missing, 0);
    assert_eq!(snapshot.summary.problem, 0);
    for symbol in &snapshot.symbols {
        assert_eq!(symbol.status, SymbolStatus::Closed);
        assert_eq!(symbol.session, SymbolSession::Closed);
        assert_eq!(symbol.flow, SymbolFlow::NoSample);
        assert_eq!(symbol.problem_severity, SymbolProblemSeverity::Closed);
        assert!(!symbol.problem);
    }
}

#[test]
fn closed_session_overrides_pending_initial_sample_status() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(10, 20, 0);
    store.record_universe(["DCE.m2609"], now - 10_000);

    let snapshot = store.snapshot_at_with_context(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
        SymbolMetricsContext {
            initializing_universe: true,
            initializing_pending_samples: true,
        },
    );

    assert_eq!(snapshot.summary.closed, 1);
    assert_eq!(snapshot.summary.initializing, 0);
    assert_eq!(snapshot.summary.missing, 0);
    assert_eq!(snapshot.symbols[0].status, SymbolStatus::Closed);
    assert_eq!(snapshot.symbols[0].flow, SymbolFlow::NoSample);
}

#[test]
fn pending_universe_symbol_can_remain_initializing_after_first_sample() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(9, 30, 0);
    store.record_universe(["SHFE.au2602", "DCE.m2609"], now - 10_000);
    store.record_tick_at(
        "SHFE.au2602",
        &tick(1, i64::try_from((now - 1_000) * 1_000_000).unwrap(), 610.0),
        now - 800,
    );

    let default = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    assert_eq!(default.summary.live, 1);
    assert_eq!(default.summary.missing, 1);
    assert_eq!(default.summary.problem, 1);

    let initializing = store.snapshot_at_with_context(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
        SymbolMetricsContext {
            initializing_pending_samples: true,
            ..Default::default()
        },
    );

    let dce = initializing
        .symbols
        .iter()
        .find(|symbol| symbol.symbol == "DCE.m2609")
        .unwrap();
    assert_eq!(initializing.summary.live, 1);
    assert_eq!(initializing.summary.initializing, 1);
    assert_eq!(initializing.summary.missing, 0);
    assert_eq!(initializing.summary.problem, 0);
    assert_eq!(dce.status, SymbolStatus::Initializing);
    assert_eq!(dce.flow, SymbolFlow::NoSample);
    assert_eq!(dce.problem_severity, SymbolProblemSeverity::Initializing);
    assert!(!dce.problem);
}

#[test]
fn ticked_symbol_transitions_from_live_to_stale() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(9, 30, 0);
    store.record_universe(["SHFE.au2602"], now - 2_000);
    store.record_tick_at(
        "SHFE.au2602",
        &tick(1, i64::try_from((now - 1_000) * 1_000_000).unwrap(), 610.0),
        now - 800,
    );

    let live = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    assert_eq!(live.symbols[0].status, SymbolStatus::Live);
    assert_eq!(live.symbols[0].receive_gap_ms, Some(800));
    assert_eq!(live.symbols[0].market_time_lag_ms, Some(1_000));

    let stale = store.snapshot_at(
        now + 30_201,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    assert_eq!(stale.symbols[0].status, SymbolStatus::Stale);
    assert_eq!(stale.summary.stale, 1);
}

#[test]
fn future_tick_datetime_does_not_report_zero_market_lag() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(9, 30, 0);
    let future_tick_datetime_ns = i64::try_from((now + 500) * 1_000_000).unwrap();
    store.record_universe(["SHFE.au2602"], now - 2_000);
    store.record_tick_at(
        "SHFE.au2602",
        &tick(1, future_tick_datetime_ns, 610.0),
        now - 100,
    );

    let snapshot = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );

    assert_eq!(snapshot.symbols[0].status, SymbolStatus::Live);
    assert_eq!(snapshot.symbols[0].receive_gap_ms, Some(100));
    assert_eq!(snapshot.symbols[0].market_time_lag_ms, None);
    assert_eq!(
        snapshot.symbols[0].last_tick_datetime_ns,
        Some(future_tick_datetime_ns)
    );
}

#[test]
fn sequential_tick_ids_do_not_create_continuity_problems() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(9, 30, 0);
    store.record_universe(["SHFE.au2602"], now - 2_000);
    store.record_tick_at(
        "SHFE.au2602",
        &tick(1, i64::try_from((now - 2_000) * 1_000_000).unwrap(), 610.0),
        now - 2_000,
    );
    store.record_tick_at(
        "SHFE.au2602",
        &tick(2, i64::try_from((now - 1_000) * 1_000_000).unwrap(), 611.0),
        now - 1_000,
    );

    let snapshot = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    let symbol = &snapshot.symbols[0];

    assert_eq!(symbol.last_tick_id, Some(2));
    assert_eq!(symbol.gap_event_count, 0);
    assert_eq!(symbol.estimated_missing_rows, 0);
    assert_eq!(symbol.duplicate_rows, 0);
    assert_eq!(symbol.out_of_order_rows, 0);
    assert!(!symbol.problem);
    assert_eq!(snapshot.summary.gap_event_count, 0);
}

#[test]
fn skipped_tick_row_ids_are_diff_diagnostics_not_confirmed_gaps() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(9, 30, 0);
    store.record_universe(["SHFE.au2602"], now - 2_000);
    store.record_tick_at(
        "SHFE.au2602",
        &tick(1, i64::try_from((now - 2_000) * 1_000_000).unwrap(), 610.0),
        now - 2_000,
    );
    store.record_tick_at(
        "SHFE.au2602",
        &tick(4, i64::try_from((now - 1_000) * 1_000_000).unwrap(), 613.0),
        now - 1_000,
    );

    let snapshot = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    let symbol = &snapshot.symbols[0];

    assert_eq!(symbol.last_tick_id, Some(4));
    assert_eq!(symbol.gap_event_count, 1);
    assert_eq!(symbol.estimated_missing_rows, 2);
    assert_eq!(symbol.last_gap_unix_millis, Some(now - 1_000));
    assert_eq!(symbol.integrity, SymbolIntegrity::Intact);
    assert_eq!(symbol.problem_severity, SymbolProblemSeverity::Live);
    assert!(!symbol.problem);
    assert_eq!(snapshot.summary.gap_event_count, 1);
    assert_eq!(snapshot.summary.estimated_missing_rows, 2);
}

#[test]
fn duplicate_tick_ids_are_counted_without_advancing_last_id() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(9, 30, 0);
    store.record_universe(["SHFE.au2602"], now - 2_000);
    store.record_tick_at(
        "SHFE.au2602",
        &tick(4, i64::try_from((now - 2_000) * 1_000_000).unwrap(), 613.0),
        now - 2_000,
    );
    store.record_tick_at(
        "SHFE.au2602",
        &tick(4, i64::try_from((now - 1_000) * 1_000_000).unwrap(), 613.0),
        now - 1_000,
    );

    let snapshot = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    let symbol = &snapshot.symbols[0];

    assert_eq!(symbol.last_tick_id, Some(4));
    assert_eq!(symbol.duplicate_rows, 1);
    assert_eq!(symbol.out_of_order_rows, 0);
    assert_eq!(symbol.problem_severity, SymbolProblemSeverity::Live);
    assert!(!symbol.problem);
    assert_eq!(snapshot.summary.duplicate_rows, 1);
}

#[test]
fn out_of_order_tick_ids_are_ignored_as_historical_updates() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(9, 30, 0);
    store.record_universe(["SHFE.au2602"], now - 2_000);
    store.record_tick_at(
        "SHFE.au2602",
        &tick(4, i64::try_from((now - 2_000) * 1_000_000).unwrap(), 613.0),
        now - 2_000,
    );
    store.record_tick_at(
        "SHFE.au2602",
        &tick(3, i64::try_from((now - 1_000) * 1_000_000).unwrap(), 612.0),
        now - 1_000,
    );

    let snapshot = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    let symbol = &snapshot.symbols[0];

    assert_eq!(symbol.last_tick_id, Some(4));
    assert_eq!(symbol.out_of_order_rows, 0);
    assert_eq!(symbol.duplicate_rows, 0);
    assert_eq!(symbol.problem_severity, SymbolProblemSeverity::Live);
    assert!(!symbol.problem);
    assert_eq!(snapshot.summary.out_of_order_rows, 0);
}

#[test]
fn source_epoch_reset_prevents_false_gap_from_new_source_start_id() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(9, 30, 0);
    store.record_universe(["SHFE.au2602"], now - 2_000);
    store.record_tick_at(
        "SHFE.au2602",
        &tick(1, i64::try_from((now - 2_000) * 1_000_000).unwrap(), 610.0),
        now - 2_000,
    );
    store.advance_source_epoch();
    store.record_tick_at(
        "SHFE.au2602",
        &tick(
            100,
            i64::try_from((now - 1_000) * 1_000_000).unwrap(),
            700.0,
        ),
        now - 1_000,
    );

    let snapshot = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    let symbol = &snapshot.symbols[0];

    assert_eq!(symbol.source_epoch, 1);
    assert_eq!(symbol.last_tick_id, Some(100));
    assert_eq!(symbol.gap_event_count, 0);
    assert_eq!(symbol.estimated_missing_rows, 0);
    assert_eq!(snapshot.summary.gap_event_count, 0);
}

#[test]
fn quote_only_symbol_transitions_from_live_to_stale_without_tick_count() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(9, 30, 0);
    store.record_universe(["SHFE.ag2705"], now - 2_000);
    store.record_quote_at(
        "SHFE.ag2705",
        &quote(
            "SHFE.ag2705",
            i64::try_from((now - 500) * 1_000_000).unwrap(),
            16666.0,
        ),
        now - 300,
    );

    let live = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    assert_eq!(live.symbols[0].status, SymbolStatus::Live);
    assert_eq!(live.symbols[0].ticks_ingested, 0);

    assert_eq!(live.symbols[0].receive_gap_ms, Some(300));
    assert_eq!(live.symbols[0].market_time_lag_ms, Some(500));

    let stale = store.snapshot_at(
        now + 30_001,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    assert_eq!(stale.symbols[0].status, SymbolStatus::Stale);
    assert_eq!(stale.summary.stale, 1);
    assert_eq!(stale.summary.missing, 0);
}

#[test]
fn quote_snapshot_serializes_instrument_name() {
    let mut store = SymbolTelemetryStore::default();
    store.record_universe(["SHFE.au2602"], 1_700_000_000_000);
    store.record_quote_at(
        "SHFE.au2602",
        &quote_with_name("SHFE.au2602", "沪金2602", 1_700_000_001_500_000_000, 610.0),
        1_700_000_001_700,
    );

    let snapshot = store.snapshot_at(
        1_700_000_002_000,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    let json = serde_json::to_value(snapshot).unwrap();

    assert_eq!(json["symbols"][0]["symbol"], "SHFE.au2602");
    assert_eq!(json["symbols"][0]["instrument_name"], "沪金2602");
}

#[test]
fn query_matches_instrument_name() {
    let mut store = SymbolTelemetryStore::default();
    store.record_universe(["SHFE.au2602", "DCE.m2609"], 1_700_000_000_000);
    store.record_quote_at(
        "SHFE.au2602",
        &quote_with_name("SHFE.au2602", "沪金2602", 1_700_000_001_500_000_000, 610.0),
        1_700_000_001_700,
    );
    store.record_quote_at(
        "DCE.m2609",
        &quote_with_name("DCE.m2609", "豆粕2609", 1_700_000_001_500_000_000, 3100.0),
        1_700_000_001_700,
    );

    let query = SymbolMetricsQuery {
        q: Some("沪金".to_string()),
        ..SymbolMetricsQuery::default()
    };
    let snapshot = store.snapshot_at(1_700_000_002_000, 30_000, &Default::default(), &query);

    assert_eq!(snapshot.symbols.len(), 1);
    assert_eq!(snapshot.symbols[0].symbol, "SHFE.au2602");
}

#[test]
fn continuous_contract_symbols_get_chinese_display_names() {
    let mut store = SymbolTelemetryStore::default();
    store.record_universe(
        ["KQ.m@SHFE.au", "KQ.i@SHFE.au", "KQ.i@DCE.m"],
        1_700_000_000_000,
    );

    let snapshot = store.snapshot_at(
        1_700_000_002_000,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );

    assert_eq!(snapshot.symbols[0].symbol, "KQ.i@DCE.m");
    assert_eq!(
        snapshot.symbols[0].instrument_name.as_deref(),
        Some("豆粕加权")
    );
    assert_eq!(snapshot.symbols[1].symbol, "KQ.i@SHFE.au");
    assert_eq!(
        snapshot.symbols[1].instrument_name.as_deref(),
        Some("沪金加权")
    );
    assert_eq!(snapshot.symbols[2].symbol, "KQ.m@SHFE.au");
    assert_eq!(
        snapshot.symbols[2].instrument_name.as_deref(),
        Some("沪金主连")
    );
}

#[test]
fn symbol_query_matches_continuous_contract_chinese_display_name() {
    let mut store = SymbolTelemetryStore::default();
    store.record_universe(["KQ.m@SHFE.au", "DCE.m2609"], 1_700_000_000_000);

    let query = SymbolMetricsQuery {
        q: Some("沪金".to_string()),
        ..SymbolMetricsQuery::default()
    };
    let snapshot = store.snapshot_at(1_700_000_002_000, 30_000, &Default::default(), &query);

    assert_eq!(snapshot.symbols.len(), 1);
    assert_eq!(snapshot.symbols[0].symbol, "KQ.m@SHFE.au");
    assert_eq!(
        snapshot.symbols[0].instrument_name.as_deref(),
        Some("沪金主连")
    );
}

#[test]
fn subscribed_symbol_outside_universe_is_inactive() {
    let store = SymbolTelemetryStore::default();
    let now = local_millis_at(9, 30, 0);
    let mut subscriptions: BTreeMap<String, SymbolSubscriptionCounts> = Default::default();
    subscriptions.insert(
        "DCE.m2609".to_string(),
        SymbolSubscriptionCounts {
            quote_subscriber_count: 1,
            chart_subscriber_count: 0,
        },
    );

    let snapshot = store.snapshot_at(now, 30_000, &subscriptions, &SymbolMetricsQuery::default());

    assert_eq!(snapshot.summary.total, 1);
    assert_eq!(snapshot.summary.inactive, 1);
    assert_eq!(snapshot.summary.subscribed, 1);
    assert_eq!(snapshot.symbols[0].status, SymbolStatus::Inactive);
    assert!(snapshot.symbols[0].subscribed);
}

#[test]
fn subscribed_uncovered_symbol_stays_inactive_during_closed_session() {
    let store = SymbolTelemetryStore::default();
    let now = local_millis_at(11, 0, 0);
    let mut subscriptions: BTreeMap<String, SymbolSubscriptionCounts> = Default::default();
    subscriptions.insert(
        "DCE.m2609".to_string(),
        SymbolSubscriptionCounts {
            quote_subscriber_count: 1,
            chart_subscriber_count: 0,
        },
    );

    let snapshot = store.snapshot_at(now, 30_000, &subscriptions, &SymbolMetricsQuery::default());

    assert_eq!(snapshot.summary.total, 1);
    assert_eq!(snapshot.summary.inactive, 1);
    assert_eq!(snapshot.summary.subscribed_problem, 1);
    assert_eq!(snapshot.symbols[0].status, SymbolStatus::Inactive);
    assert_eq!(snapshot.symbols[0].coverage, SymbolCoverage::Uncovered);
    assert_eq!(snapshot.symbols[0].session, SymbolSession::Unknown);
    assert_eq!(snapshot.symbols[0].flow, SymbolFlow::NoSample);
    assert_eq!(snapshot.symbols[0].integrity, SymbolIntegrity::Intact);
    assert!(snapshot.symbols[0].problem);
}

#[test]
fn snapshot_filters_sorts_limits_and_computes_p95_receive_gap() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(9, 30, 0);
    store.record_universe(["SHFE.au2602", "DCE.m2609", "CZCE.AP610"], now - 2_000);
    store.record_tick_at(
        "SHFE.au2602",
        &tick(1, i64::try_from((now - 1_000) * 1_000_000).unwrap(), 610.0),
        now - 1_000,
    );
    store.record_tick_at(
        "DCE.m2609",
        &tick(2, i64::try_from((now - 1_500) * 1_000_000).unwrap(), 3100.0),
        now - 1_500,
    );

    let query = SymbolMetricsQuery {
        statuses: vec![SymbolStatus::Live, SymbolStatus::Stale],
        sessions: Vec::new(),
        subscribed_only: false,
        q: Some("260".to_string()),
        sort: SymbolSort::ReceiveGapDesc,
        limit: Some(1),
    };
    let snapshot = store.snapshot_at(now, 30_000, &Default::default(), &query);

    assert_eq!(snapshot.summary.total, 3);
    assert_eq!(snapshot.summary.live, 2);
    assert_eq!(snapshot.summary.missing, 1);
    assert_eq!(snapshot.summary.p95_receive_gap_ms, Some(1_500));
    assert_eq!(snapshot.symbols.len(), 1);
    assert_eq!(snapshot.symbols[0].symbol, "DCE.m2609");
}

#[test]
fn symbol_snapshot_exposes_server_side_average_receive_gap() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(9, 30, 0);
    store.record_universe(["SHFE.au2602"], now - 10_000);
    store.record_tick_at(
        "SHFE.au2602",
        &tick(1, i64::try_from((now - 3_000) * 1_000_000).unwrap(), 610.0),
        now - 3_000,
    );
    store.record_tick_at(
        "SHFE.au2602",
        &tick(2, i64::try_from((now - 2_000) * 1_000_000).unwrap(), 611.0),
        now - 2_000,
    );
    store.record_tick_at(
        "SHFE.au2602",
        &tick(3, i64::try_from((now - 500) * 1_000_000).unwrap(), 612.0),
        now - 500,
    );

    let snapshot = store.snapshot_at(now, 30_000, &Default::default(), &Default::default());

    assert_eq!(snapshot.symbols[0].receive_gap_ms, Some(500));
    assert_eq!(snapshot.symbols[0].avg_receive_gap_ms, Some(1_250));
}

#[test]
fn average_receive_gap_uses_tick_receive_times_not_quote_updates() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(9, 30, 0);
    store.record_universe(["SHFE.au2602"], now - 10_000);
    store.record_tick_at(
        "SHFE.au2602",
        &tick(1, i64::try_from((now - 3_000) * 1_000_000).unwrap(), 610.0),
        now - 3_000,
    );
    store.record_quote_at(
        "SHFE.au2602",
        &quote(
            "SHFE.au2602",
            i64::try_from((now - 2_500) * 1_000_000).unwrap(),
            610.5,
        ),
        now - 2_500,
    );
    store.record_tick_at(
        "SHFE.au2602",
        &tick(2, i64::try_from((now - 1_000) * 1_000_000).unwrap(), 611.0),
        now - 1_000,
    );

    let snapshot = store.snapshot_at(now, 30_000, &Default::default(), &Default::default());

    assert_eq!(snapshot.symbols[0].receive_gap_ms, Some(1_000));
    assert_eq!(snapshot.symbols[0].avg_receive_gap_ms, Some(2_000));
}

#[test]
fn snapshot_summary_stays_global_when_query_filters_and_limits_rows() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(9, 30, 0);
    store.record_universe(["SHFE.au2602", "DCE.m2609", "CZCE.AP610"], now - 2_000);
    store.record_tick_at(
        "SHFE.au2602",
        &tick(1, i64::try_from((now - 1_000) * 1_000_000).unwrap(), 610.0),
        now - 1_000,
    );
    store.record_tick_at(
        "DCE.m2609",
        &tick(
            2,
            i64::try_from((now - 90_000) * 1_000_000).unwrap(),
            3100.0,
        ),
        now - 90_000,
    );

    let query = SymbolMetricsQuery {
        statuses: vec![SymbolStatus::Live],
        sort: SymbolSort::SymbolAsc,
        limit: Some(1),
        ..SymbolMetricsQuery::default()
    };
    let snapshot = store.snapshot_at(now, 30_000, &Default::default(), &query);

    assert_eq!(snapshot.summary.total, 3);
    assert_eq!(snapshot.summary.live, 1);
    assert_eq!(snapshot.summary.stale, 1);
    assert_eq!(snapshot.summary.missing, 1);
    assert_eq!(snapshot.summary.problem, 2);
    assert_eq!(snapshot.summary.universe_total, 3);
    assert_eq!(snapshot.summary.universe_observed, 2);
    assert_eq!(snapshot.filtered_total, 1);
    assert_eq!(snapshot.symbols.len(), 1);
    assert_eq!(snapshot.symbols[0].symbol, "SHFE.au2602");
}

#[test]
fn historical_telemetry_outside_current_universe_is_omitted_when_unsubscribed() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(9, 30, 0);
    store.record_universe(["SHFE.au2602", "DCE.m2609"], now - 10_000);
    store.record_tick_at(
        "DCE.m2609",
        &tick(1, i64::try_from((now - 1_000) * 1_000_000).unwrap(), 3100.0),
        now - 1_000,
    );
    store.record_universe(["SHFE.au2602"], now - 1_000);

    let snapshot = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );

    assert_eq!(snapshot.summary.total, 1);
    assert_eq!(snapshot.summary.universe_total, 1);
    assert_eq!(snapshot.symbols.len(), 1);
    assert_eq!(snapshot.symbols[0].symbol, "SHFE.au2602");
}

#[test]
fn historical_telemetry_outside_current_universe_stays_visible_when_subscribed() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(9, 30, 0);
    store.record_universe(["SHFE.au2602", "DCE.m2609"], now - 10_000);
    store.record_tick_at(
        "DCE.m2609",
        &tick(1, i64::try_from((now - 1_000) * 1_000_000).unwrap(), 3100.0),
        now - 1_000,
    );
    store.record_universe(["SHFE.au2602"], now - 1_000);

    let mut subscriptions: BTreeMap<String, SymbolSubscriptionCounts> = Default::default();
    subscriptions.insert(
        "DCE.m2609".to_string(),
        SymbolSubscriptionCounts {
            quote_subscriber_count: 1,
            chart_subscriber_count: 0,
        },
    );

    let snapshot = store.snapshot_at(now, 30_000, &subscriptions, &SymbolMetricsQuery::default());
    let dce = snapshot
        .symbols
        .iter()
        .find(|symbol| symbol.symbol == "DCE.m2609")
        .unwrap();

    assert_eq!(snapshot.summary.total, 2);
    assert_eq!(snapshot.summary.universe_total, 1);
    assert!(dce.subscribed);
    assert_eq!(dce.coverage, SymbolCoverage::Uncovered);
    assert_eq!(dce.problem_severity, SymbolProblemSeverity::Bad);
}

#[test]
fn read_model_snapshot_is_detached_from_store_mutation() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(9, 30, 0);
    store.record_universe(["SHFE.au2602"], now - 10_000);
    let read_model = store.read_model();

    store.record_universe(["DCE.m2609"], now - 1_000);

    let snapshot = read_model.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );

    assert_eq!(snapshot.summary.total, 1);
    assert_eq!(snapshot.symbols[0].symbol, "SHFE.au2602");
}

#[test]
fn symbol_metrics_query_parses_http_query_shape() {
    let query = SymbolMetricsQuery::from_query_string(
        "status=live,stale&subscribed=true&q=260&sort=receive_gap_ms_desc&limit=10",
    )
    .unwrap();

    assert_eq!(
        query.statuses,
        vec![SymbolStatus::Live, SymbolStatus::Stale]
    );
    assert!(query.subscribed_only);
    assert_eq!(query.q.as_deref(), Some("260"));
    assert_eq!(query.sort, SymbolSort::ReceiveGapDesc);
    assert_eq!(query.limit, Some(10));

    assert_eq!(
        SymbolMetricsQuery::from_query_string("status=unknown").unwrap_err(),
        "invalid status"
    );
}

#[test]
fn symbol_metrics_query_decodes_statuses_and_filters_case_insensitively() {
    let query =
        SymbolMetricsQuery::from_query_string("status=live%2Cmissing&q=AU2602&limit=10").unwrap();
    assert_eq!(
        query.statuses,
        vec![SymbolStatus::Live, SymbolStatus::Missing]
    );
    assert_eq!(query.q.as_deref(), Some("AU2602"));

    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(9, 30, 0);
    store.record_universe(["SHFE.au2602", "DCE.m2609"], now - 10_000);

    let snapshot = store.snapshot_at(now, 30_000, &Default::default(), &query);

    assert_eq!(snapshot.summary.total, 2);
    assert_eq!(snapshot.summary.missing, 2);
    assert_eq!(snapshot.symbols.len(), 1);
    assert_eq!(snapshot.symbols[0].symbol, "SHFE.au2602");
}

#[test]
fn stale_quote_inside_day_rest_session_is_closed_not_problematic() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(11, 0, 0);
    store.record_universe(["SHFE.au2602"], now - 90_000);
    store.record_quote_at(
        "SHFE.au2602",
        &quote_with_trading_time(
            "SHFE.au2602",
            i64::try_from((now - 90_000) * 1_000_000).unwrap(),
            610.0,
            &[("09:00:00", "10:15:00"), ("13:30:00", "15:00:00")],
            &[],
        ),
        now - 90_000,
    );

    let snapshot = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    let json = serde_json::to_value(&snapshot).unwrap();

    assert_eq!(json["symbols"][0]["status"], "closed");
    assert_eq!(json["summary"]["closed"], 1);
    assert_eq!(json["summary"]["stale"], 0);
    assert_eq!(
        json["symbols"][0]["receive_gap_ms"],
        serde_json::Value::Null
    );
    assert_eq!(
        json["symbols"][0]["avg_receive_gap_ms"],
        serde_json::Value::Null
    );
    assert_eq!(
        json["symbols"][0]["market_time_lag_ms"],
        serde_json::Value::Null
    );
    assert_eq!(
        json["summary"]["p95_receive_gap_ms"],
        serde_json::Value::Null
    );
    assert!(snapshot.symbols[0].last_receive_unix_millis.is_some());
}

#[test]
fn closed_session_nulls_active_delay_fields_but_keeps_raw_timestamps() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(11, 0, 0);
    store.record_universe(["SHFE.au2602"], now - 120_000);
    store.record_symbol_trading_time(
        "SHFE.au2602",
        &TradingTime {
            day: vec![vec!["09:00:00".to_string(), "10:15:00".to_string()]],
            night: Vec::new(),
        },
    );
    store.record_tick_at(
        "SHFE.au2602",
        &tick(
            1,
            i64::try_from((now - 120_000) * 1_000_000).unwrap(),
            610.0,
        ),
        now - 120_000,
    );
    store.record_tick_at(
        "SHFE.au2602",
        &tick(2, i64::try_from((now - 90_000) * 1_000_000).unwrap(), 611.0),
        now - 90_000,
    );

    let snapshot = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    let symbol = &snapshot.symbols[0];

    assert_eq!(symbol.status, SymbolStatus::Closed);
    assert_eq!(symbol.session, SymbolSession::Closed);
    assert_eq!(symbol.flow, SymbolFlow::NoSample);
    assert_eq!(symbol.receive_gap_ms, None);
    assert_eq!(symbol.avg_receive_gap_ms, None);
    assert_eq!(symbol.market_time_lag_ms, None);
    assert_eq!(snapshot.summary.p95_receive_gap_ms, None);
    assert_eq!(symbol.last_receive_unix_millis, Some(now - 90_000));
    assert_eq!(
        symbol.last_tick_datetime_ns,
        Some(i64::try_from((now - 90_000) * 1_000_000).unwrap())
    );
}

#[test]
fn china_futures_session_uses_shanghai_time_not_host_timezone() {
    let mut store = SymbolTelemetryStore::default();
    let china_09_30 = 1_787_967_000_000;
    store.record_universe(["DCE.m2609"], china_09_30 - 90_000);
    store.record_tick_at(
        "DCE.m2609",
        &tick(
            1,
            i64::try_from((china_09_30 - 90_000) * 1_000_000).unwrap(),
            3100.0,
        ),
        china_09_30 - 90_000,
    );

    let snapshot = store.snapshot_at(
        china_09_30,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );

    assert_eq!(snapshot.symbols[0].session, SymbolSession::Open);
    assert_eq!(snapshot.symbols[0].status, SymbolStatus::Stale);
    assert!(snapshot.symbols[0].problem);
}

#[test]
fn closed_symbol_with_invalid_rows_is_not_problematic() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(11, 0, 0);
    store.record_universe(["SHFE.au2602"], now - 90_000);
    store.record_quote_at(
        "SHFE.au2602",
        &quote_with_trading_time(
            "SHFE.au2602",
            i64::try_from((now - 90_000) * 1_000_000).unwrap(),
            610.0,
            &[("09:00:00", "10:15:00"), ("13:30:00", "15:00:00")],
            &[],
        ),
        now - 90_000,
    );
    store.record_invalid_row("SHFE.au2602", "historical decode error");

    let snapshot = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    let json = serde_json::to_value(&snapshot).unwrap();

    assert_eq!(json["symbols"][0]["status"], "closed");
    assert_eq!(json["symbols"][0]["problem"], false);
    assert_eq!(json["symbols"][0]["problem_severity"], "closed");
    assert_eq!(json["symbols"][0]["invalid_rows"], 1);
}

#[test]
fn tick_only_symbol_uses_futures_session_fallback_for_midday_break() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(10, 20, 0);
    store.record_universe(["DCE.m2609"], now - 90_000);
    store.record_tick_at(
        "DCE.m2609",
        &tick(
            1,
            i64::try_from((now - 90_000) * 1_000_000).unwrap(),
            3100.0,
        ),
        now - 90_000,
    );

    let snapshot = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    let json = serde_json::to_value(&snapshot).unwrap();

    assert_eq!(json["symbols"][0]["status"], "closed");
    assert_eq!(json["symbols"][0]["problem"], false);
    assert_eq!(json["summary"]["closed"], 1);
    assert_eq!(json["summary"]["stale"], 0);
}

#[test]
fn continuous_contract_uses_underlying_futures_session_fallback_for_midday_break() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(10, 20, 0);
    store.record_universe(["KQ.i@DCE.m"], now - 90_000);
    store.record_tick_at(
        "KQ.i@DCE.m",
        &tick(
            1,
            i64::try_from((now - 90_000) * 1_000_000).unwrap(),
            3100.0,
        ),
        now - 90_000,
    );

    let snapshot = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    let json = serde_json::to_value(&snapshot).unwrap();

    assert_eq!(json["symbols"][0]["status"], "closed");
    assert_eq!(json["symbols"][0]["session"], "closed");
    assert_eq!(json["symbols"][0]["problem"], false);
    assert_eq!(json["summary"]["closed"], 1);
    assert_eq!(json["summary"]["stale"], 0);
}

#[test]
fn sunday_night_does_not_open_before_monday_trading_day() {
    let mut store = SymbolTelemetryStore::default();
    let sunday_night = china_millis_at(
        NaiveDate::from_ymd_opt(2026, 6, 14).expect("test date should be valid"),
        23,
        30,
        0,
    );
    store.record_trading_calendar(&[
        tqsdk_core::TradingCalendarDay {
            date: "2026-06-14".to_string(),
            trading: false,
        },
        tqsdk_core::TradingCalendarDay {
            date: "2026-06-15".to_string(),
            trading: true,
        },
    ]);
    store.record_universe(["SHFE.ao2609", "KQ.i@SHFE.ao"], sunday_night - 90_000);
    store.record_tick_at(
        "SHFE.ao2609",
        &tick(
            1,
            i64::try_from((sunday_night - 90_000) * 1_000_000).unwrap(),
            3100.0,
        ),
        sunday_night - 90_000,
    );
    store.record_tick_at(
        "KQ.i@SHFE.ao",
        &tick(
            1,
            i64::try_from((sunday_night - 90_000) * 1_000_000).unwrap(),
            3100.0,
        ),
        sunday_night - 90_000,
    );

    let snapshot = store.snapshot_at(
        sunday_night,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );

    assert_eq!(snapshot.summary.closed, 2);
    assert_eq!(snapshot.summary.stale, 0);
    assert!(snapshot.symbols.iter().all(|symbol| {
        symbol.session == SymbolSession::Closed
            && symbol.status == SymbolStatus::Closed
            && !symbol.problem
    }));
}

#[test]
fn monday_early_morning_after_non_trading_sunday_is_closed() {
    let mut store = SymbolTelemetryStore::default();
    let monday_early = china_millis_at(
        NaiveDate::from_ymd_opt(2026, 6, 15).expect("test date should be valid"),
        0,
        30,
        0,
    );
    store.record_trading_calendar(&[
        tqsdk_core::TradingCalendarDay {
            date: "2026-06-14".to_string(),
            trading: false,
        },
        tqsdk_core::TradingCalendarDay {
            date: "2026-06-15".to_string(),
            trading: true,
        },
    ]);
    store.record_universe(["SHFE.ao2609", "KQ.i@SHFE.ao"], monday_early - 90_000);
    store.record_tick_at(
        "SHFE.ao2609",
        &tick(
            1,
            i64::try_from((monday_early - 90_000) * 1_000_000).unwrap(),
            3100.0,
        ),
        monday_early - 90_000,
    );
    store.record_tick_at(
        "KQ.i@SHFE.ao",
        &tick(
            1,
            i64::try_from((monday_early - 90_000) * 1_000_000).unwrap(),
            3100.0,
        ),
        monday_early - 90_000,
    );

    let snapshot = store.snapshot_at(
        monday_early,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );

    assert_eq!(snapshot.summary.closed, 2);
    assert_eq!(snapshot.summary.stale, 0);
    assert!(snapshot.symbols.iter().all(|symbol| {
        symbol.session == SymbolSession::Closed
            && symbol.status == SymbolStatus::Closed
            && !symbol.problem
    }));
}

#[test]
fn tick_only_symbol_uses_query_symbol_info_trading_time_before_fallback() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(14, 0, 0);
    store.record_universe(["DCE.m2609"], now - 90_000);
    store.record_symbol_trading_time(
        "DCE.m2609",
        &TradingTime {
            day: vec![vec!["09:00:00".to_string(), "10:15:00".to_string()]],
            night: Vec::new(),
        },
    );
    store.record_tick_at(
        "DCE.m2609",
        &tick(
            1,
            i64::try_from((now - 90_000) * 1_000_000).unwrap(),
            3100.0,
        ),
        now - 90_000,
    );

    let snapshot = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    let json = serde_json::to_value(&snapshot).unwrap();

    assert_eq!(json["symbols"][0]["status"], "closed");
    assert_eq!(json["symbols"][0]["problem"], false);
    assert_eq!(json["summary"]["closed"], 1);
    assert_eq!(json["summary"]["stale"], 0);
}

#[test]
fn quote_without_trading_time_does_not_erase_query_symbol_info_schedule() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(14, 0, 0);
    store.record_universe(["DCE.m2609"], now - 90_000);
    store.record_symbol_trading_time(
        "DCE.m2609",
        &TradingTime {
            day: vec![vec!["09:00:00".to_string(), "10:15:00".to_string()]],
            night: Vec::new(),
        },
    );
    store.record_quote_at(
        "DCE.m2609",
        &quote(
            "DCE.m2609",
            i64::try_from((now - 90_000) * 1_000_000).unwrap(),
            3100.0,
        ),
        now - 90_000,
    );

    let snapshot = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    let json = serde_json::to_value(&snapshot).unwrap();

    assert_eq!(json["symbols"][0]["status"], "closed");
    assert_eq!(json["symbols"][0]["problem"], false);
    assert_eq!(json["summary"]["closed"], 1);
    assert_eq!(json["summary"]["stale"], 0);
}

#[test]
fn tick_only_symbol_inside_futures_session_remains_problematic_when_stale() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(9, 30, 0);
    store.record_universe(["DCE.m2609"], now - 90_000);
    store.record_tick_at(
        "DCE.m2609",
        &tick(
            1,
            i64::try_from((now - 90_000) * 1_000_000).unwrap(),
            3100.0,
        ),
        now - 90_000,
    );

    let snapshot = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    let json = serde_json::to_value(&snapshot).unwrap();

    assert_eq!(json["symbols"][0]["status"], "stale");
    assert_eq!(json["symbols"][0]["problem"], true);
    assert_eq!(json["symbols"][0]["problem_severity"], "warn");
    assert_eq!(json["summary"]["stale"], 1);
}

#[test]
fn fallback_schedule_distinguishes_product_night_sessions() {
    let now = local_millis_at(23, 30, 0);

    let mut metal_store = SymbolTelemetryStore::default();
    metal_store.record_universe(["SHFE.au2602"], now - 90_000);
    metal_store.record_tick_at(
        "SHFE.au2602",
        &tick(1, i64::try_from((now - 90_000) * 1_000_000).unwrap(), 610.0),
        now - 90_000,
    );
    let metal = metal_store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );

    let mut day_only_store = SymbolTelemetryStore::default();
    day_only_store.record_universe(["SHFE.wr2607"], now - 90_000);
    day_only_store.record_tick_at(
        "SHFE.wr2607",
        &tick(
            1,
            i64::try_from((now - 90_000) * 1_000_000).unwrap(),
            3300.0,
        ),
        now - 90_000,
    );
    let day_only = day_only_store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );

    assert_eq!(metal.symbols[0].status, SymbolStatus::Stale);
    assert!(metal.symbols[0].problem);
    assert_eq!(day_only.symbols[0].status, SymbolStatus::Closed);
    assert!(!day_only.symbols[0].problem);
}

#[test]
fn stale_quote_inside_open_session_remains_stale() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(9, 30, 0);
    store.record_universe(["SHFE.au2602"], now - 90_000);
    store.record_quote_at(
        "SHFE.au2602",
        &quote_with_trading_time(
            "SHFE.au2602",
            i64::try_from((now - 90_000) * 1_000_000).unwrap(),
            610.0,
            &[("09:00:00", "10:15:00"), ("13:30:00", "15:00:00")],
            &[],
        ),
        now - 90_000,
    );

    let snapshot = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );

    assert_eq!(snapshot.symbols[0].status, SymbolStatus::Stale);
    assert_eq!(snapshot.summary.stale, 1);
}

#[test]
fn product_specific_sessions_are_classified_independently() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(14, 0, 0);
    store.record_universe(["SHFE.au2602", "DCE.m2609"], now - 90_000);
    store.record_quote_at(
        "SHFE.au2602",
        &quote_with_trading_time(
            "SHFE.au2602",
            i64::try_from((now - 90_000) * 1_000_000).unwrap(),
            610.0,
            &[("09:00:00", "10:15:00")],
            &[],
        ),
        now - 90_000,
    );
    store.record_quote_at(
        "DCE.m2609",
        &quote_with_trading_time(
            "DCE.m2609",
            i64::try_from((now - 1_000) * 1_000_000).unwrap(),
            3100.0,
            &[("13:30:00", "15:00:00")],
            &[],
        ),
        now - 1_000,
    );

    let snapshot = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    let json = serde_json::to_value(&snapshot).unwrap();

    assert_eq!(json["summary"]["closed"], 1);
    assert_eq!(json["summary"]["live"], 1);
    assert_eq!(json["symbols"][0]["symbol"], "DCE.m2609");
    assert_eq!(json["symbols"][0]["status"], "live");
    assert_eq!(json["symbols"][1]["symbol"], "SHFE.au2602");
    assert_eq!(json["symbols"][1]["status"], "closed");
}

#[test]
fn night_session_wraps_midnight_for_status_classification() {
    let mut open_store = SymbolTelemetryStore::default();
    let open_now = local_millis_at(23, 30, 0);
    open_store.record_universe(["SHFE.rb2605"], open_now - 90_000);
    open_store.record_quote_at(
        "SHFE.rb2605",
        &quote_with_trading_time(
            "SHFE.rb2605",
            i64::try_from((open_now - 90_000) * 1_000_000).unwrap(),
            3300.0,
            &[],
            &[("21:00:00", "01:00:00")],
        ),
        open_now - 90_000,
    );

    let open = open_store.snapshot_at(
        open_now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    assert_eq!(open.symbols[0].status, SymbolStatus::Stale);

    let mut closed_store = SymbolTelemetryStore::default();
    let closed_now = local_millis_at(2, 0, 0);
    closed_store.record_universe(["SHFE.rb2605"], closed_now - 90_000);
    closed_store.record_quote_at(
        "SHFE.rb2605",
        &quote_with_trading_time(
            "SHFE.rb2605",
            i64::try_from((closed_now - 90_000) * 1_000_000).unwrap(),
            3300.0,
            &[],
            &[("21:00:00", "01:00:00")],
        ),
        closed_now - 90_000,
    );

    let closed = closed_store.snapshot_at(
        closed_now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    let json = serde_json::to_value(&closed).unwrap();

    assert_eq!(json["symbols"][0]["status"], "closed");
    assert_eq!(json["summary"]["closed"], 1);
}

#[test]
fn official_night_session_end_after_24_keeps_session_open() {
    let mut store = SymbolTelemetryStore::default();
    let now = local_millis_at(0, 30, 0);
    store.record_universe(["SHFE.au2608"], now - 90_000);
    store.record_symbol_trading_time(
        "SHFE.au2608",
        &TradingTime {
            day: vec![
                vec!["09:00:00".to_string(), "10:15:00".to_string()],
                vec!["10:30:00".to_string(), "11:30:00".to_string()],
                vec!["13:30:00".to_string(), "15:00:00".to_string()],
            ],
            night: vec![vec!["21:00:00".to_string(), "26:30:00".to_string()]],
        },
    );
    store.record_tick_at(
        "SHFE.au2608",
        &tick(1, i64::try_from((now - 90_000) * 1_000_000).unwrap(), 800.0),
        now - 90_000,
    );

    let snapshot = store.snapshot_at(
        now,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );

    assert_eq!(snapshot.symbols[0].session, SymbolSession::Open);
    assert_eq!(snapshot.symbols[0].status, SymbolStatus::Stale);
    assert!(snapshot.symbols[0].problem);
}

#[test]
fn symbol_metrics_query_accepts_closed_status_filter() {
    assert!(SymbolMetricsQuery::from_query_string("status=closed").is_ok());
}
