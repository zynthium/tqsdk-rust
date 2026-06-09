use tqsdk_core::Quote;
use tqsdk_relay::{
    ClientId, DownstreamCommand, FakeUpstreamTickSource, RelayEngine, RelayTickRow,
    SetChartCommand, SourceKey, UpstreamTick, UpstreamTickSource,
};

fn tick(id: i64, datetime: i64, price: f64) -> RelayTickRow {
    RelayTickRow {
        id,
        datetime,
        last_price: price,
        volume: id * 10,
        open_interest: 1000 + id,
    }
}

fn quote(symbol: &str, datetime: &str, price: f64) -> Quote {
    Quote {
        instrument_id: symbol.to_string(),
        datetime: datetime.to_string(),
        last_price: price,
        volume: 12,
        open_interest: 34,
        ..Quote::default()
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
fn relay_engine_fans_out_quotes_from_ticks() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let client = ClientId::new(1);

    engine
        .handle_command(
            client,
            DownstreamCommand::SubscribeQuote {
                symbols: vec!["SHFE.au2602".to_string()],
            },
        )
        .unwrap();

    let frames = engine
        .ingest_tick("SHFE.au2602", tick(1, 1_000, 610.0))
        .unwrap();

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].client_id, client);
    assert_eq!(frames[0].payload["aid"], "rtn_data");
    assert_eq!(
        frames[0].payload["data"][0]["quotes"]["SHFE.au2602"]["last_price"],
        610.0
    );
}

#[test]
fn relay_engine_does_not_emit_quotes_without_interest() {
    let mut engine = RelayEngine::new_memory_only(16, 16);

    let frames = engine
        .ingest_tick("SHFE.au2602", tick(1, 1_000, 610.0))
        .unwrap();

    assert!(frames.is_empty());
}

#[test]
fn relay_engine_fans_out_quotes_from_upstream_quote_updates() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let client = ClientId::new(1);

    engine
        .handle_command(
            client,
            DownstreamCommand::SubscribeQuote {
                symbols: vec!["SHFE.ag2705".to_string()],
            },
        )
        .unwrap();
    engine.record_universe_refresh_success_for_symbols(
        ["SHFE.ag2705"],
        "SHFE.ag2705".len(),
        None,
        None,
        1,
    );

    let frames = engine
        .ingest_quote_at(
            "SHFE.ag2705",
            quote("SHFE.ag2705", "1780985438500000000", 16666.0),
            1_780_985_438_500,
        )
        .unwrap();

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].client_id, client);
    assert_eq!(
        frames[0].payload["data"][0]["quotes"]["SHFE.ag2705"]["last_price"],
        16666.0
    );
    let snapshot = engine.symbol_metrics_snapshot_at(1_780_985_438_500, &Default::default());
    let symbol = snapshot
        .symbols
        .iter()
        .find(|row| row.symbol == "SHFE.ag2705")
        .expect("quote update should create symbol telemetry");
    assert_eq!(symbol.ticks_ingested, 0);
    assert_eq!(symbol.last_price, Some(16666.0));
    assert_eq!(symbol.receive_gap_ms, Some(0));
    assert_eq!(symbol.status, tqsdk_relay::SymbolStatus::Live);
}

#[test]
fn subscribe_quote_replays_cached_quote_snapshot() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    engine
        .ingest_quote_at(
            "SHFE.au2706",
            quote("SHFE.au2706", "1780985437000000000", 962.34),
            1_780_985_437_000,
        )
        .unwrap();

    let frames = engine
        .handle_command(
            ClientId::new(1),
            DownstreamCommand::SubscribeQuote {
                symbols: vec!["SHFE.au2706".to_string()],
            },
        )
        .unwrap();

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].client_id, ClientId::new(1));
    assert_eq!(
        frames[0].payload["data"][0]["quotes"]["SHFE.au2706"]["last_price"],
        962.34
    );
}

#[test]
fn relay_engine_rewrites_chart_payload_to_downstream_chart_id() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let client = ClientId::new(1);

    engine
        .handle_command(client, chart_command("client-chart"))
        .unwrap();

    engine
        .ingest_tick("SHFE.au2602", tick(1, 0, 610.0))
        .unwrap();
    let frames = engine
        .ingest_tick("SHFE.au2602", tick(2, 60_000_000_000, 620.0))
        .unwrap();

    let chart_frame = frames
        .iter()
        .find(|frame| frame.payload["data"][0].get("charts").is_some())
        .expect("completed bar should emit chart metadata for downstream chart");
    assert_eq!(
        chart_frame.payload["data"][0]["charts"]["client-chart"]["right_id"],
        0
    );
}

#[test]
fn relay_engine_tracks_bootstrap_request_without_subscribing_remote_kline_immediately() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let client = ClientId::new(1);

    engine
        .handle_command(client, chart_command("client-chart"))
        .unwrap();

    let source = SourceKey {
        symbols: vec!["SHFE.au2602".to_string()],
        duration_ns: 60_000_000_000,
        view_width: 64,
    };
    assert_eq!(engine.bootstrap_pending_len(), 1);
    assert_eq!(engine.interests().chart_interest_count(&source), 1);
}

#[test]
fn relay_engine_drops_pending_bootstrap_when_last_chart_client_disconnects() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let client = ClientId::new(1);

    engine
        .handle_command(client, chart_command("client-chart"))
        .unwrap();
    assert_eq!(engine.bootstrap_pending_len(), 1);

    engine.remove_client(client);

    assert_eq!(engine.bootstrap_pending_len(), 0);
}

#[test]
fn relay_engine_keeps_pending_bootstrap_for_shared_chart_source() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let first = ClientId::new(1);
    let second = ClientId::new(2);

    engine
        .handle_command(first, chart_command("first-chart"))
        .unwrap();
    engine
        .handle_command(second, chart_command("second-chart"))
        .unwrap();
    assert_eq!(engine.bootstrap_pending_len(), 1);

    engine.remove_client(first);

    assert_eq!(engine.bootstrap_pending_len(), 1);
}

#[test]
fn relay_engine_replays_tick_ring_for_new_kline_chart_subscription() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let client = ClientId::new(1);

    engine
        .ingest_tick("SHFE.au2602", tick(1, 0, 610.0))
        .unwrap();
    engine
        .ingest_tick("SHFE.au2602", tick(2, 30_000_000_000, 612.0))
        .unwrap();
    engine
        .ingest_tick("SHFE.au2602", tick(3, 60_000_000_000, 620.0))
        .unwrap();

    let frames = engine
        .handle_command(client, chart_command("client-chart"))
        .unwrap();

    let kline_frame = frames
        .iter()
        .find(|frame| frame.payload["data"][0].get("klines").is_some())
        .expect("cold start should emit completed kline from cached ticks");
    assert_eq!(kline_frame.client_id, client);
    assert_eq!(
        kline_frame.payload["data"][0]["klines"]["SHFE.au2602"]["60000000000"]["data"]["0"]["datetime"],
        0
    );
    assert_eq!(
        kline_frame.payload["data"][0]["klines"]["SHFE.au2602"]["60000000000"]["data"]["0"]["close"],
        612.0
    );

    let chart_frame = frames
        .iter()
        .find(|frame| frame.payload["data"][0].get("charts").is_some())
        .expect("cold start should mark downstream chart ready");
    assert_eq!(
        chart_frame.payload["data"][0]["charts"]["client-chart"]["right_id"],
        0
    );
}

#[tokio::test]
async fn fake_upstream_tick_source_pops_ticks_fifo() {
    let mut upstream = FakeUpstreamTickSource::default();
    upstream.push(UpstreamTick {
        symbol: "SHFE.au2602".to_string(),
        row: tick(1, 1_000, 610.0),
    });
    upstream.push(UpstreamTick {
        symbol: "DCE.m2609".to_string(),
        row: tick(2, 2_000, 3300.0),
    });

    assert_eq!(upstream.next_tick().await.unwrap().symbol, "SHFE.au2602");
    assert_eq!(upstream.next_tick().await.unwrap().symbol, "DCE.m2609");
    assert!(upstream.next_tick().await.is_none());
}
