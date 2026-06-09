use std::collections::BTreeMap;

use tqsdk_core::Quote;
use tqsdk_relay::{
    RelayTickRow, SymbolMetricsQuery, SymbolSort, SymbolStatus, SymbolSubscriptionCounts,
    SymbolTelemetryStore,
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

#[test]
fn universe_symbol_without_tick_is_missing() {
    let mut store = SymbolTelemetryStore::default();
    store.record_universe(["SHFE.au2602"], 1_700_000_000_000);

    let snapshot = store.snapshot_at(
        1_700_000_010_000,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );

    assert_eq!(snapshot.summary.total, 1);
    assert_eq!(snapshot.summary.missing, 1);
    assert_eq!(snapshot.symbols[0].symbol, "SHFE.au2602");
    assert_eq!(snapshot.symbols[0].status, SymbolStatus::Missing);
    assert!(snapshot.symbols[0].receive_gap_ms.is_none());
}

#[test]
fn ticked_symbol_transitions_from_live_to_stale() {
    let mut store = SymbolTelemetryStore::default();
    store.record_universe(["SHFE.au2602"], 1_700_000_000_000);
    store.record_tick_at(
        "SHFE.au2602",
        &tick(1, 1_700_000_001_000_000_000, 610.0),
        1_700_000_001_200,
    );

    let live = store.snapshot_at(
        1_700_000_002_000,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    assert_eq!(live.symbols[0].status, SymbolStatus::Live);
    assert_eq!(live.symbols[0].receive_gap_ms, Some(800));
    assert_eq!(live.symbols[0].market_time_lag_ms, Some(1_000));

    let stale = store.snapshot_at(
        1_700_000_032_201,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    assert_eq!(stale.symbols[0].status, SymbolStatus::Stale);
    assert_eq!(stale.summary.stale, 1);
}

#[test]
fn quote_only_symbol_transitions_from_live_to_stale_without_tick_count() {
    let mut store = SymbolTelemetryStore::default();
    store.record_universe(["SHFE.ag2705"], 1_700_000_000_000);
    store.record_quote_at(
        "SHFE.ag2705",
        &quote("SHFE.ag2705", 1_700_000_001_500_000_000, 16666.0),
        1_700_000_001_700,
    );

    let live = store.snapshot_at(
        1_700_000_002_000,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    assert_eq!(live.symbols[0].status, SymbolStatus::Live);
    assert_eq!(live.symbols[0].ticks_ingested, 0);
    assert_eq!(live.symbols[0].last_price, Some(16666.0));
    assert_eq!(live.symbols[0].receive_gap_ms, Some(300));
    assert_eq!(live.symbols[0].market_time_lag_ms, Some(500));

    let stale = store.snapshot_at(
        1_700_000_032_001,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    assert_eq!(stale.symbols[0].status, SymbolStatus::Stale);
    assert_eq!(stale.summary.stale, 1);
    assert_eq!(stale.summary.missing, 0);
}

#[test]
fn subscribed_symbol_outside_universe_is_inactive() {
    let store = SymbolTelemetryStore::default();
    let mut subscriptions: BTreeMap<String, SymbolSubscriptionCounts> = Default::default();
    subscriptions.insert(
        "DCE.m2609".to_string(),
        SymbolSubscriptionCounts {
            quote_subscriber_count: 1,
            chart_subscriber_count: 0,
        },
    );

    let snapshot = store.snapshot_at(
        1_700_000_010_000,
        30_000,
        &subscriptions,
        &SymbolMetricsQuery::default(),
    );

    assert_eq!(snapshot.summary.total, 1);
    assert_eq!(snapshot.summary.inactive, 1);
    assert_eq!(snapshot.summary.subscribed, 1);
    assert_eq!(snapshot.symbols[0].status, SymbolStatus::Inactive);
    assert!(snapshot.symbols[0].subscribed);
}

#[test]
fn snapshot_filters_sorts_limits_and_computes_p95_receive_gap() {
    let mut store = SymbolTelemetryStore::default();
    store.record_universe(
        ["SHFE.au2602", "DCE.m2609", "CZCE.AP610"],
        1_700_000_000_000,
    );
    store.record_tick_at(
        "SHFE.au2602",
        &tick(1, 1_700_000_001_000_000_000, 610.0),
        1_700_000_001_000,
    );
    store.record_tick_at(
        "DCE.m2609",
        &tick(2, 1_700_000_000_500_000_000, 3100.0),
        1_700_000_000_500,
    );

    let query = SymbolMetricsQuery {
        statuses: vec![SymbolStatus::Live, SymbolStatus::Stale],
        subscribed_only: false,
        q: Some("260".to_string()),
        sort: SymbolSort::ReceiveGapDesc,
        limit: Some(1),
    };
    let snapshot = store.snapshot_at(1_700_000_002_000, 30_000, &Default::default(), &query);

    assert_eq!(snapshot.summary.total, 3);
    assert_eq!(snapshot.summary.live, 2);
    assert_eq!(snapshot.summary.missing, 1);
    assert_eq!(snapshot.summary.p95_receive_gap_ms, Some(1_500));
    assert_eq!(snapshot.symbols.len(), 1);
    assert_eq!(snapshot.symbols[0].symbol, "DCE.m2609");
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
