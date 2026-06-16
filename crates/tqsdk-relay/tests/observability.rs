use chrono::{Datelike, Local, TimeZone};
use tqsdk_core::TradingTime;
use tqsdk_relay::{
    ClientId, DecodeHealth, DownstreamCommand, FlowIdleHealth, FuturesContract, RelayConfig,
    RelayEngine, RelayEventKind, RelaySourceStage, RelaySourceStatus, RelayStartupReport,
    RelayTickRow, SetChartCommand, UniverseExpression, UpstreamSourceProgress,
};

fn tick(id: i64) -> RelayTickRow {
    RelayTickRow {
        id,
        datetime: id,
        last_price: 610.0 + id as f64,
        volume: id * 10,
        open_interest: 100 + id,
    }
}

fn chart_command(chart_id: &str) -> DownstreamCommand {
    DownstreamCommand::SetChart(SetChartCommand {
        chart_id: chart_id.to_string(),
        symbols: vec!["SHFE.au2602".to_string()],
        duration_ns: 60_000_000_000,
        view_width: 64,
        left_kline_id: None,
        focus_datetime_ns: None,
        focus_position: None,
    })
}

fn local_millis_at(hour: u32, minute: u32, second: u32) -> u64 {
    let today = Local::now();
    let timestamp = Local
        .with_ymd_and_hms(
            today.year(),
            today.month(),
            today.day(),
            hour,
            minute,
            second,
        )
        .single()
        .expect("local test time should be unambiguous")
        .timestamp_millis();
    u64::try_from(timestamp).expect("local test time should be after unix epoch")
}

#[test]
fn health_reports_up_when_engine_is_constructed() {
    let engine = RelayEngine::new_memory_only(16, 16);

    let health = engine.health_snapshot();

    assert!(health.ready);
    assert_eq!(health.upstream_status, RelaySourceStatus::Connecting);
    assert_eq!(health.downstream_clients, 0);
}

#[test]
fn health_snapshot_exposes_readiness_dimensions() {
    let mut engine = RelayEngine::new_memory_only(16, 16);

    let initial = serde_json::to_value(engine.health_snapshot()).unwrap();
    assert_eq!(initial["ready"], true);
    assert_eq!(initial["process_started"], true);
    assert_eq!(initial["downstream_listening"], true);
    assert_eq!(initial["upstream_connected"], false);
    assert_eq!(initial["universe_ready"], false);
    assert_eq!(initial["data_fresh"], false);
    assert_eq!(initial["market_data_ready"], false);
    assert_eq!(initial["upstream_stage"], "connecting");
    assert_eq!(initial["upstream_transport_connected"], false);
    assert_eq!(initial["upstream_subscription_sent"], false);
    assert_eq!(initial["upstream_frames_received"], 0);
    assert_eq!(initial["upstream_events_decoded"], 0);
    assert_eq!(initial["upstream_frame_idle_health"], "no_sample");
    assert_eq!(initial["upstream_event_idle_health"], "no_sample");
    assert_eq!(initial["current_decode_health"], "healthy");
    assert_eq!(initial["recent_invalid_rows_1m"], 0);
    assert_eq!(
        initial["last_upstream_frame_unix_secs"],
        serde_json::Value::Null
    );

    engine.record_universe_refresh_success(2, 21, Some(32_000), None, 1_700_000_000);
    engine.ingest_tick("SHFE.au2602", tick(1)).unwrap();

    let live = serde_json::to_value(engine.health_snapshot()).unwrap();
    assert_eq!(live["ready"], true);
    assert_eq!(live["upstream_connected"], true);
    assert_eq!(live["universe_ready"], true);
    assert_eq!(live["data_fresh"], true);
    assert_eq!(live["market_data_ready"], true);
    assert_eq!(live["upstream_symbols"], 2);
    assert_eq!(live["ticks_ingested"], 1);
    assert_eq!(live["upstream_stage"], "live");
}

#[test]
fn metrics_expose_upstream_bootstrap_progress_before_market_data_arrives() {
    let mut engine = RelayEngine::new_memory_only(16, 16);

    engine.record_upstream_transport_connected_at(1_700_000_000);
    engine.record_upstream_subscription_sent_at(1_700_000_001);

    let subscribed = engine.health_snapshot_at(1_700_000_001);
    assert_eq!(subscribed.upstream_stage, RelaySourceStage::Backfilling);
    assert!(subscribed.upstream_transport_connected);
    assert!(subscribed.upstream_subscription_sent);
    assert_eq!(subscribed.upstream_frames_received, 0);
    assert_eq!(subscribed.last_upstream_frame_unix_secs, None);
    assert_eq!(
        subscribed.upstream_stage_started_unix_secs,
        Some(1_700_000_001)
    );

    engine.record_upstream_frame_received_at(1_700_000_002, 0);

    let health = engine.health_snapshot_at(1_700_000_003);
    assert_eq!(health.upstream_status, RelaySourceStatus::Connecting);
    assert_eq!(health.upstream_stage, RelaySourceStage::Backfilling);
    assert!(health.upstream_transport_connected);
    assert!(health.upstream_subscription_sent);
    assert_eq!(health.upstream_frames_received, 1);
    assert_eq!(health.upstream_events_decoded, 0);
    assert_eq!(health.last_upstream_frame_unix_secs, Some(1_700_000_002));
    assert_eq!(health.upstream_stage_started_unix_secs, Some(1_700_000_001));
    assert!(!health.upstream_connected);
    assert!(!health.market_data_ready);

    let metrics = engine.metrics_snapshot();
    assert_eq!(metrics.upstream_stage, RelaySourceStage::Backfilling);
    assert!(metrics.upstream_transport_connected);
    assert!(metrics.upstream_subscription_sent);
    assert_eq!(metrics.upstream_frames_received, 1);
    assert_eq!(metrics.upstream_events_decoded, 0);
    assert_eq!(metrics.last_upstream_frame_unix_secs, Some(1_700_000_002));
    assert_eq!(
        metrics.upstream_stage_started_unix_secs,
        Some(1_700_000_001)
    );
}

#[test]
fn metrics_expose_upstream_peek_and_decode_timing() {
    let mut engine = RelayEngine::new_memory_only(16, 16);

    engine.record_upstream_progress(UpstreamSourceProgress {
        frames_received: 1,
        events_decoded: 2,
        unix_secs: 1_700_000_002,
        last_peek_delay_ms: Some(1),
        last_decode_ms: Some(7),
        ..Default::default()
    });

    let health = engine.health_snapshot_at(1_700_000_003);
    assert_eq!(health.last_upstream_peek_delay_ms, Some(1));
    assert_eq!(health.last_upstream_decode_ms, Some(7));

    let metrics = engine.metrics_snapshot_at(1_700_000_003);
    assert_eq!(metrics.last_upstream_peek_delay_ms, Some(1));
    assert_eq!(metrics.last_upstream_decode_ms, Some(7));
}

#[test]
fn dashboard_marks_no_sample_universe_symbols_initializing_during_backfill() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let now = local_millis_at(9, 30, 0);
    engine.record_universe_refresh_success_for_symbols(
        ["SHFE.au2602", "DCE.m2609"],
        21,
        Some(32_000),
        None,
        now / 1_000 - 2,
    );
    engine.record_upstream_transport_connected_at(now / 1_000 - 1);
    engine.record_upstream_subscription_sent_at(now / 1_000 - 1);

    let symbols =
        engine.symbol_metrics_snapshot_at(now, &tqsdk_relay::SymbolMetricsQuery::default());
    assert_eq!(symbols.summary.initializing, 2);
    assert_eq!(symbols.summary.missing, 0);
    assert_eq!(symbols.summary.problem, 0);
    assert!(symbols.symbols.iter().all(|symbol| !symbol.problem));

    let dashboard = engine.dashboard_snapshot_at(now, &tqsdk_relay::SymbolMetricsQuery::default());

    assert_eq!(
        dashboard.metrics.upstream_stage,
        RelaySourceStage::Backfilling
    );
    assert_eq!(dashboard.global.total, 2);
    assert_eq!(dashboard.global.initializing, 2);
    assert_eq!(dashboard.global.missing, 0);
    assert_eq!(dashboard.global.problem, 0);
    assert_eq!(
        dashboard.timeline.global.severity,
        tqsdk_relay::DashboardTimelineSeverity::NoSample
    );
    assert_eq!(dashboard.timeline.global.problem, 0);
    assert!(dashboard.page.symbols.iter().all(|symbol| !symbol.problem));

    let json = serde_json::to_value(&dashboard).unwrap();
    assert_eq!(json["global"]["initializing"], 2);
    assert_eq!(json["global"]["missing"], 0);
    assert_eq!(json["global"]["problem"], 0);
    assert_eq!(json["page"]["symbols"][0]["status"], "initializing");
    assert_eq!(
        json["page"]["symbols"][0]["problem_severity"],
        "initializing"
    );
    assert!(
        json["page"]["symbols"][0]
            .as_object()
            .unwrap()
            .get("problem")
            .is_none()
    );
}

#[test]
fn dashboard_marks_auction_ordering_open_without_continuity_problem() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let now = local_millis_at(20, 56, 0);
    engine.record_universe_refresh_success_for_symbols(
        ["SHFE.au2602"],
        11,
        Some(32_000),
        None,
        now / 1_000 - 2,
    );

    engine
        .ingest_trading_status_at_for_test("SHFE.au2602", "AUCTIONORDERING", now - 1_000)
        .unwrap();

    let dashboard = engine.dashboard_snapshot_at(now, &tqsdk_relay::SymbolMetricsQuery::default());
    let row = &dashboard.page.symbols[0];
    let json = serde_json::to_value(&dashboard).unwrap();

    assert_eq!(row.status, tqsdk_relay::SymbolStatus::Live);
    assert_eq!(row.session, tqsdk_relay::SymbolSession::Open);
    assert_eq!(dashboard.global.problem, 0);
    assert_eq!(
        dashboard.timeline.global.severity,
        tqsdk_relay::DashboardTimelineSeverity::Auction
    );
    assert_eq!(json["page"]["symbols"][0]["phase"], "auction_ordering");
    assert_eq!(json["page"]["symbols"][0]["phase_source"], "trading_status");
    assert_eq!(
        json["page"]["symbols"][0]["raw_trade_status"],
        "AUCTIONORDERING"
    );
    assert_eq!(
        json["page"]["symbols"][0]["receive_gap_ms"],
        serde_json::Value::Null
    );
}

#[test]
fn dashboard_keeps_unobserved_universe_symbols_initializing_after_live_tick() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let now = local_millis_at(9, 30, 0);
    engine.record_universe_refresh_success_for_symbols(
        ["SHFE.au2602", "DCE.m2609"],
        21,
        Some(32_000),
        None,
        now / 1_000 - 2,
    );
    engine.record_upstream_transport_connected_at(now / 1_000 - 1);
    engine.record_upstream_subscription_sent_at(now / 1_000 - 1);
    engine
        .ingest_tick_at_for_test("SHFE.au2602", tick(1), now - 500)
        .unwrap();

    let inputs = engine.dashboard_snapshot_inputs_at(now);
    let (dashboard, timeline_sample) = inputs
        .into_dashboard_snapshot_and_timeline_sample(&tqsdk_relay::SymbolMetricsQuery::default());

    assert_eq!(dashboard.metrics.upstream_stage, RelaySourceStage::Live);
    assert_eq!(dashboard.global.live, 1);
    assert_eq!(dashboard.global.initializing, 1);
    assert_eq!(dashboard.global.missing, 0);
    assert_eq!(dashboard.global.problem, 0);
    assert_eq!(dashboard.timeline.global.problem, 0);
    assert_eq!(
        dashboard.timeline.exchanges["DCE"].severity,
        tqsdk_relay::DashboardTimelineSeverity::NoSample
    );
    assert!(dashboard.page.symbols.iter().all(|symbol| !symbol.problem));
    assert_eq!(
        dashboard
            .page
            .symbols
            .iter()
            .find(|symbol| symbol.symbol == "DCE.m2609")
            .unwrap()
            .status,
        tqsdk_relay::SymbolStatus::Initializing
    );
    assert_eq!(
        timeline_sample.symbols["DCE.m2609"].severity,
        tqsdk_relay::DashboardTimelineSeverity::NoSample
    );
}

#[test]
fn upstream_subscription_sent_advances_symbol_source_epoch() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let now = local_millis_at(9, 30, 0);
    engine.record_universe_refresh_success_for_symbols(
        ["SHFE.au2602"],
        11,
        None,
        None,
        now / 1_000 - 2,
    );
    engine
        .ingest_tick_at_for_test("SHFE.au2602", tick(1), now - 2_000)
        .unwrap();

    engine.record_upstream_subscription_sent_at(now / 1_000 - 1);
    engine
        .ingest_tick_at_for_test("SHFE.au2602", tick(100), now - 1_000)
        .unwrap();

    let snapshot =
        engine.symbol_metrics_snapshot_at(now, &tqsdk_relay::SymbolMetricsQuery::default());
    let symbol = snapshot
        .symbols
        .iter()
        .find(|symbol| symbol.symbol == "SHFE.au2602")
        .unwrap();

    assert_eq!(symbol.source_epoch, 1);
    assert_eq!(symbol.last_tick_id, Some(100));
    assert_eq!(symbol.gap_event_count, 0);
    assert_eq!(snapshot.summary.gap_event_count, 0);
}

#[test]
fn health_snapshot_marks_data_stale_after_freshness_window() {
    let mut engine = RelayEngine::new_memory_only(16, 16);

    engine.record_data_activity_at(1_700_000_000);

    let fresh = serde_json::to_value(engine.health_snapshot_at(1_700_000_030)).unwrap();
    assert_eq!(fresh["data_fresh"], true);

    let stale = serde_json::to_value(engine.health_snapshot_at(1_700_000_031)).unwrap();
    assert_eq!(stale["data_fresh"], false);
}

#[test]
fn metrics_include_clients_subscriptions_and_cache_events() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    engine
        .handle_command(
            ClientId::new(1),
            DownstreamCommand::SubscribeQuote {
                symbols: vec!["SHFE.au2602".to_string()],
            },
        )
        .unwrap();
    engine.ingest_tick("SHFE.au2602", tick(1)).unwrap();

    let metrics = engine.metrics_snapshot();

    assert_eq!(metrics.downstream_clients, 1);
    assert_eq!(metrics.quote_subscriptions, 1);
    assert_eq!(metrics.chart_subscriptions, 0);
    assert_eq!(metrics.ticks_ingested, 1);
    assert_eq!(metrics.bootstrap_pending, 0);
    assert_eq!(metrics.bootstrap_inflight, 0);
    assert_eq!(
        engine.health_snapshot().upstream_status,
        RelaySourceStatus::Up
    );
}

#[test]
fn engine_symbol_metrics_keep_pending_universe_symbols_initializing() {
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

    let live = engine.symbol_metrics_snapshot_at(now, &tqsdk_relay::SymbolMetricsQuery::default());
    assert_eq!(live.summary.total, 2);
    assert_eq!(live.summary.live, 1);
    assert_eq!(live.summary.initializing, 1);
    assert_eq!(live.summary.missing, 0);
    assert_eq!(live.summary.problem, 0);

    let stale = engine
        .symbol_metrics_snapshot_at(now + 30_001, &tqsdk_relay::SymbolMetricsQuery::default());
    let au = stale
        .symbols
        .iter()
        .find(|symbol| symbol.symbol == "SHFE.au2602")
        .unwrap();
    assert_eq!(au.status, tqsdk_relay::SymbolStatus::Stale);
    assert_eq!(stale.summary.initializing, 1);
    assert_eq!(stale.summary.problem, 1);
}

#[test]
fn engine_dashboard_snapshot_keeps_global_summary_when_page_is_filtered() {
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
    let dashboard = engine.dashboard_snapshot_at(now, &query);

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
fn engine_dashboard_snapshot_exposes_aggregate_timeline_without_global_symbol_rows() {
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
    let dashboard = engine.dashboard_snapshot_at(now, &query);

    assert_eq!(dashboard.global.total, 2);
    assert_eq!(dashboard.page.symbols.len(), 1);
    assert_eq!(dashboard.timeline.global.total, 2);
    assert!(dashboard.timeline.exchanges.contains_key("SHFE"));
    assert!(dashboard.timeline.exchanges.contains_key("DCE"));
}

#[test]
fn engine_dashboard_snapshot_exposes_unfiltered_timeline_symbol_rows() {
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
    let dashboard = engine.dashboard_snapshot_at(now, &query);

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
fn dashboard_snapshot_serializes_compact_page_rows() {
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

    let dashboard = engine.dashboard_snapshot_at(now, &tqsdk_relay::SymbolMetricsQuery::default());
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
fn dashboard_timeline_groups_continuous_contracts_by_underlying_exchange() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let now = local_millis_at(9, 30, 0);
    engine.record_universe_refresh_success_for_symbols(
        ["KQ.i@DCE.m", "KQ.m@SHFE.au"],
        24,
        Some(32_000),
        None,
        now / 1_000 - 2,
    );

    let dashboard = engine.dashboard_snapshot_at(now, &tqsdk_relay::SymbolMetricsQuery::default());

    assert!(dashboard.timeline.exchanges.contains_key("DCE"));
    assert!(dashboard.timeline.exchanges.contains_key("SHFE"));
    assert!(!dashboard.timeline.exchanges.contains_key("KQ"));
}

#[test]
fn dashboard_snapshot_inputs_are_classified_after_detached_copy() {
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

    let dashboard = inputs.into_dashboard_snapshot(&tqsdk_relay::SymbolMetricsQuery::default());

    assert_eq!(dashboard.global.total, 1);
    assert_eq!(dashboard.timeline.global.total, 1);
    assert!(dashboard.timeline.exchanges.contains_key("SHFE"));
}

#[test]
fn dashboard_event_ledger_keeps_fixed_capacity_universe_events() {
    let mut engine = RelayEngine::new_memory_only(16, 16);

    for index in 0..130 {
        engine.record_universe_refresh_success_for_symbols(
            [format!("SHFE.au{index:04}")],
            11,
            None,
            None,
            1_700_000_000 + index,
        );
    }

    let events = engine.event_ledger_snapshot();

    assert_eq!(events.len(), 128);
    assert_eq!(events.first().unwrap().sequence, 3);
    assert_eq!(events.last().unwrap().sequence, 130);
    assert_eq!(events[0].kind, RelayEventKind::UniverseRefreshed);
}

#[test]
fn dashboard_event_ledger_records_universe_flow_and_decode_incidents() {
    let mut engine = RelayEngine::new_memory_only(16, 16);

    engine.record_universe_refresh_error("metadata unavailable", 1_700_000_001);
    engine.mark_upstream_degraded();
    engine.record_upstream_invalid_tick_rows_at(2, Some("bad row".to_string()), 1_700_000_003);

    let events = engine.event_ledger_snapshot();

    assert!(events.iter().any(|event| {
        event.kind == RelayEventKind::UniverseRefreshFailed
            && event.detail.contains("metadata unavailable")
    }));
    assert!(
        events
            .iter()
            .any(|event| event.kind == RelayEventKind::FlowIncident)
    );
    assert!(events.iter().any(|event| {
        event.kind == RelayEventKind::DecodeIncident && event.detail.contains("count=2")
    }));
}

#[test]
fn engine_records_query_symbol_info_trading_time_on_universe_refresh() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let now = local_millis_at(14, 0, 0);
    let contract = FuturesContract::new_with_trading_time(
        "DCE.m2609",
        "DCE",
        "m",
        false,
        TradingTime {
            day: vec![vec!["09:00:00".to_string(), "10:15:00".to_string()]],
            night: Vec::new(),
        },
    )
    .unwrap();
    engine.record_universe_refresh_success_for_contracts(&[contract], 9, None, None, now / 1_000);
    engine
        .ingest_tick_at_for_test("DCE.m2609", tick(1), now - 90_000)
        .unwrap();

    let snapshot =
        engine.symbol_metrics_snapshot_at(now, &tqsdk_relay::SymbolMetricsQuery::default());
    let symbol = snapshot
        .symbols
        .iter()
        .find(|symbol| symbol.symbol == "DCE.m2609")
        .unwrap();

    assert_eq!(symbol.status, tqsdk_relay::SymbolStatus::Closed);
    assert!(!symbol.problem);
    assert_eq!(snapshot.summary.closed, 1);
    assert_eq!(snapshot.summary.stale, 0);
}

#[test]
fn engine_records_query_symbol_info_name_on_universe_refresh() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let now = local_millis_at(14, 0, 0);
    let mut contract = FuturesContract::new_with_trading_time(
        "DCE.m2609",
        "DCE",
        "m",
        false,
        TradingTime {
            day: vec![vec!["09:00:00".to_string(), "10:15:00".to_string()]],
            night: Vec::new(),
        },
    )
    .unwrap();
    contract.instrument_name = Some("豆粕2609".to_string());

    engine.record_universe_refresh_success_for_contracts(&[contract], 9, None, None, now / 1_000);
    engine
        .ingest_tick_at_for_test("DCE.m2609", tick(1), now - 90_000)
        .unwrap();

    let snapshot =
        engine.symbol_metrics_snapshot_at(now, &tqsdk_relay::SymbolMetricsQuery::default());
    let symbol = snapshot
        .symbols
        .iter()
        .find(|symbol| symbol.symbol == "DCE.m2609")
        .unwrap();

    assert_eq!(symbol.instrument_name.as_deref(), Some("豆粕2609"));
}

#[test]
fn engine_symbol_metrics_include_quote_and_chart_subscriptions() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let now = local_millis_at(9, 30, 0);
    engine
        .handle_command(
            ClientId::new(1),
            DownstreamCommand::SubscribeQuote {
                symbols: vec!["SHFE.au2602".to_string()],
            },
        )
        .unwrap();
    engine
        .handle_command(ClientId::new(2), chart_command("chart-2"))
        .unwrap();

    let snapshot =
        engine.symbol_metrics_snapshot_at(now, &tqsdk_relay::SymbolMetricsQuery::default());
    let symbol = snapshot
        .symbols
        .iter()
        .find(|symbol| symbol.symbol == "SHFE.au2602")
        .unwrap();

    assert_eq!(symbol.status, tqsdk_relay::SymbolStatus::Inactive);
    assert!(symbol.subscribed);
    assert_eq!(symbol.quote_subscriber_count, 1);
    assert_eq!(symbol.chart_subscriber_count, 1);

    engine.remove_client(ClientId::new(1));
    let after_remove =
        engine.symbol_metrics_snapshot_at(now, &tqsdk_relay::SymbolMetricsQuery::default());
    let symbol = after_remove
        .symbols
        .iter()
        .find(|symbol| symbol.symbol == "SHFE.au2602")
        .unwrap();
    assert_eq!(symbol.quote_subscriber_count, 0);
    assert_eq!(symbol.chart_subscriber_count, 1);
}

#[test]
fn metrics_include_chart_subscriptions_and_bootstrap_queue() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    engine
        .handle_command(ClientId::new(1), chart_command("chart-1"))
        .unwrap();
    engine
        .handle_command(ClientId::new(2), chart_command("chart-2"))
        .unwrap();

    let metrics = engine.metrics_snapshot();

    assert_eq!(metrics.downstream_clients, 2);
    assert_eq!(metrics.quote_subscriptions, 0);
    assert_eq!(metrics.chart_subscriptions, 2);
    assert_eq!(metrics.bootstrap_pending, 1);
}

#[test]
fn metrics_include_upstream_universe_subscription_state() {
    let mut engine = RelayEngine::new_memory_only(16, 16);

    engine.record_universe_refresh_success(2, 21, Some(20), Some(64), 1_700_000_000);

    let metrics = engine.metrics_snapshot();
    assert_eq!(metrics.upstream_symbols, 2);
    assert_eq!(metrics.upstream_ins_list_chars, 21);
    assert_eq!(metrics.upstream_ins_list_warn_chars, Some(20));
    assert_eq!(metrics.upstream_ins_list_max_chars, Some(64));
    assert!(metrics.upstream_ins_list_over_warn);
    assert_eq!(metrics.last_universe_refresh_unix_secs, Some(1_700_000_000));
    assert_eq!(metrics.last_universe_refresh_error, None);
}

#[test]
fn metrics_remember_last_universe_refresh_error() {
    let mut engine = RelayEngine::new_memory_only(16, 16);

    engine.record_universe_refresh_error("metadata unavailable", 1_700_000_001);

    let metrics = engine.metrics_snapshot();
    assert_eq!(metrics.last_universe_refresh_unix_secs, Some(1_700_000_001));
    assert_eq!(
        metrics.last_universe_refresh_error.as_deref(),
        Some("metadata unavailable")
    );
}

#[test]
fn health_and_metrics_include_invalid_upstream_tick_row_diagnostics() {
    let mut engine = RelayEngine::new_memory_only(16, 16);

    engine.record_upstream_invalid_tick_rows_at(
        2,
        Some(
            "SHFE.au2602 row 17: invalid relay protocol: upstream tick row missing last_price"
                .to_string(),
        ),
        1_700_000_001,
    );

    let health = engine.health_snapshot_at(1_700_000_001);
    assert_eq!(health.upstream_invalid_tick_rows, 2);
    assert_eq!(health.lifetime_invalid_rows, 2);
    assert_eq!(health.recent_invalid_rows_1m, 2);
    assert_eq!(health.current_decode_health, DecodeHealth::Degraded);
    assert_eq!(health.last_invalid_row_unix_secs, Some(1_700_000_001));
    assert_eq!(
        health.last_upstream_invalid_tick_row_error.as_deref(),
        Some("SHFE.au2602 row 17: invalid relay protocol: upstream tick row missing last_price")
    );

    let metrics = engine.metrics_snapshot_at(1_700_000_001);
    assert_eq!(metrics.upstream_invalid_tick_rows, 2);
    assert_eq!(metrics.lifetime_invalid_rows, 2);
    assert_eq!(metrics.recent_invalid_rows_1m, 2);
    assert_eq!(metrics.current_decode_health, DecodeHealth::Degraded);
    assert_eq!(metrics.last_invalid_row_unix_secs, Some(1_700_000_001));
    assert_eq!(
        metrics.last_upstream_invalid_tick_row_error.as_deref(),
        Some("SHFE.au2602 row 17: invalid relay protocol: upstream tick row missing last_price")
    );
}

#[test]
fn metrics_report_frame_idle_warning_and_critical_thresholds() {
    let mut engine = RelayEngine::new_memory_only(16, 16);

    engine.record_upstream_frame_received_at(1_700_000_000, 1);

    let fresh = engine.metrics_snapshot_at(1_700_000_002);
    assert_eq!(fresh.upstream_frame_idle_ms, Some(2_000));
    assert_eq!(fresh.upstream_frame_idle_health, FlowIdleHealth::Live);

    let warn = engine.metrics_snapshot_at(1_700_000_003);
    assert_eq!(warn.upstream_frame_idle_ms, Some(3_000));
    assert_eq!(warn.upstream_frame_idle_health, FlowIdleHealth::Warn);

    let critical = engine.metrics_snapshot_at(1_700_000_006);
    assert_eq!(critical.upstream_frame_idle_ms, Some(6_000));
    assert_eq!(
        critical.upstream_frame_idle_health,
        FlowIdleHealth::Critical
    );
}

#[test]
fn metrics_report_event_idle_warning_and_critical_thresholds() {
    let mut engine = RelayEngine::new_memory_only(16, 16);

    engine.record_upstream_frame_received_at(1_700_000_000, 1);
    engine.record_upstream_frame_received_at(1_700_000_004, 0);

    let warn = engine.metrics_snapshot_at(1_700_000_004);
    assert_eq!(warn.upstream_frame_idle_ms, Some(0));
    assert_eq!(warn.upstream_frame_idle_health, FlowIdleHealth::Live);
    assert_eq!(warn.upstream_event_idle_ms, Some(4_000));
    assert_eq!(warn.upstream_event_idle_health, FlowIdleHealth::Warn);

    let critical = engine.metrics_snapshot_at(1_700_000_009);
    assert_eq!(critical.upstream_event_idle_ms, Some(9_000));
    assert_eq!(
        critical.upstream_event_idle_health,
        FlowIdleHealth::Critical
    );
}

#[test]
fn decode_health_recovers_after_quiet_window() {
    let mut engine = RelayEngine::new_memory_only(16, 16);

    engine.record_upstream_invalid_tick_rows_at(3, Some("bad row".to_string()), 1_700_000_000);

    let degraded = engine.metrics_snapshot_at(1_700_000_030);
    assert_eq!(degraded.lifetime_invalid_rows, 3);
    assert_eq!(degraded.recent_invalid_rows_1m, 3);
    assert_eq!(degraded.current_decode_health, DecodeHealth::Degraded);

    let recovered = engine.metrics_snapshot_at(1_700_000_061);
    assert_eq!(recovered.lifetime_invalid_rows, 3);
    assert_eq!(recovered.recent_invalid_rows_1m, 0);
    assert_eq!(recovered.current_decode_health, DecodeHealth::Healthy);
}

#[test]
fn startup_report_serializes_operational_summary() {
    let config = RelayConfig {
        futures_universe_expression: Some(
            UniverseExpression::parse("symbol:SHFE.au2602,DCE.m2609").unwrap(),
        ),
        ..RelayConfig::default()
    };
    let charts = config
        .upstream_tick_charts_for_symbols(["SHFE.au2602", "DCE.m2609"])
        .unwrap();

    let report = RelayStartupReport::from_config_and_charts(&config, &charts);
    let line = report.log_line();

    assert_eq!(report.upstream_symbols, 2);
    assert_eq!(report.upstream_tick_view_width, 10_000);
    assert_eq!(report.upstream_ins_list_chars, "SHFE.au2602".len());
    assert_eq!(report.upstream_source, "universe-expression");
    assert!(line.contains("\"event\":\"relay_startup\""));
    assert!(line.contains("\"upstream_symbols\":2"));
    assert!(line.contains("\"futures_universe_expression\":\"symbol:SHFE.au2602,DCE.m2609\""));
    assert!(line.contains("\"upstream_tick_view_width\":10000"));
    assert!(line.contains("\"metrics_listen\":\"127.0.0.1:7789\""));
}

#[test]
fn startup_report_serializes_universe_expression_summary() {
    let config = RelayConfig {
        futures_universe_expression: Some(
            UniverseExpression::parse("symbol:SHFE.au2602,KQ.i@DCE.m;!DCE.m2609").unwrap(),
        ),
        ..RelayConfig::default()
    };
    let charts = config
        .upstream_tick_charts_for_symbols(["SHFE.au2602", "KQ.i@DCE.m"])
        .unwrap();

    let report = RelayStartupReport::from_config_and_charts(&config, &charts);
    let line = report.log_line();

    assert_eq!(report.upstream_source, "universe-expression");
    assert_eq!(
        report.futures_universe_expression.as_deref(),
        Some("symbol:SHFE.au2602,KQ.i@DCE.m;!DCE.m2609")
    );
    assert_eq!(report.futures_universe_include_clauses, Some(1));
    assert_eq!(report.futures_universe_exclude_clauses, Some(1));
    assert_eq!(report.futures_universe_final_symbols, Some(2));
    assert!(line.contains("\"upstream_source\":\"universe-expression\""));
    assert!(
        line.contains(
            "\"futures_universe_expression\":\"symbol:SHFE.au2602,KQ.i@DCE.m;!DCE.m2609\""
        )
    );
    assert!(line.contains("\"futures_universe_final_symbols\":2"));
}
