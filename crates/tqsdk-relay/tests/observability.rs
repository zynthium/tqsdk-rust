use tqsdk_relay::{
    ClientId, DownstreamCommand, RelayConfig, RelayEngine, RelaySourceStage, RelaySourceStatus,
    RelayStartupReport, RelayTickRow, SetChartCommand,
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
fn engine_symbol_metrics_include_universe_missing_live_and_stale_states() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    engine.record_universe_refresh_success_for_symbols(
        ["SHFE.au2602", "DCE.m2609"],
        21,
        Some(32_000),
        None,
        1_700_000_000,
    );
    engine
        .ingest_tick_at_for_test("SHFE.au2602", tick(1), 1_700_000_001_000)
        .unwrap();

    let live = engine.symbol_metrics_snapshot_at(
        1_700_000_002_000,
        &tqsdk_relay::SymbolMetricsQuery::default(),
    );
    assert_eq!(live.summary.total, 2);
    assert_eq!(live.summary.live, 1);
    assert_eq!(live.summary.missing, 1);

    let stale = engine.symbol_metrics_snapshot_at(
        1_700_000_032_001,
        &tqsdk_relay::SymbolMetricsQuery::default(),
    );
    let au = stale
        .symbols
        .iter()
        .find(|symbol| symbol.symbol == "SHFE.au2602")
        .unwrap();
    assert_eq!(au.status, tqsdk_relay::SymbolStatus::Stale);
}

#[test]
fn engine_symbol_metrics_include_quote_and_chart_subscriptions() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
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

    let snapshot = engine.symbol_metrics_snapshot_at(
        1_700_000_002_000,
        &tqsdk_relay::SymbolMetricsQuery::default(),
    );
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
    let after_remove = engine.symbol_metrics_snapshot_at(
        1_700_000_002_000,
        &tqsdk_relay::SymbolMetricsQuery::default(),
    );
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

    engine.record_upstream_invalid_tick_rows(
        2,
        Some(
            "SHFE.au2602 row 17: invalid relay protocol: upstream tick row missing last_price"
                .to_string(),
        ),
    );

    let health = engine.health_snapshot();
    assert_eq!(health.upstream_invalid_tick_rows, 2);
    assert_eq!(
        health.last_upstream_invalid_tick_row_error.as_deref(),
        Some("SHFE.au2602 row 17: invalid relay protocol: upstream tick row missing last_price")
    );

    let metrics = engine.metrics_snapshot();
    assert_eq!(metrics.upstream_invalid_tick_rows, 2);
    assert_eq!(
        metrics.last_upstream_invalid_tick_row_error.as_deref(),
        Some("SHFE.au2602 row 17: invalid relay protocol: upstream tick row missing last_price")
    );
}

#[test]
fn startup_report_serializes_operational_summary() {
    let config = RelayConfig {
        futures_symbols: vec!["SHFE.au2602".to_string(), "DCE.m2609".to_string()],
        ..RelayConfig::default()
    };
    let charts = config.upstream_tick_charts().unwrap();

    let report = RelayStartupReport::from_config_and_charts(&config, &charts);
    let line = report.log_line();

    assert_eq!(report.upstream_symbols, 2);
    assert_eq!(report.upstream_tick_view_width, 10_000);
    assert_eq!(report.futures_active_contracts_per_product, None);
    assert_eq!(report.upstream_ins_list_chars, "SHFE.au2602".len());
    assert_eq!(report.upstream_source, "static-symbols");
    assert!(line.contains("\"event\":\"relay_startup\""));
    assert!(line.contains("\"upstream_symbols\":2"));
    assert!(line.contains("\"futures_active_contracts_per_product\":null"));
    assert!(line.contains("\"upstream_tick_view_width\":10000"));
    assert!(line.contains("\"metrics_listen\":\"127.0.0.1:7789\""));
}
