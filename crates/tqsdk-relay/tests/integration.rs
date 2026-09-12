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
    chart_command_for(chart_id, vec!["SHFE.au2602"])
}

fn chart_command_for(chart_id: &str, symbols: Vec<&str>) -> DownstreamCommand {
    DownstreamCommand::SetChart(SetChartCommand {
        chart_id: chart_id.to_string(),
        symbols: symbols.into_iter().map(str::to_string).collect(),
        duration_ns: 60_000_000_000,
        view_width: 64,
        left_kline_id: None,
        focus_datetime_ns: None,
        focus_position: None,
    })
}

fn tick_chart_command(chart_id: &str, view_width: usize) -> DownstreamCommand {
    DownstreamCommand::SetChart(SetChartCommand {
        chart_id: chart_id.to_string(),
        symbols: vec!["SHFE.au2602".to_string()],
        duration_ns: 0,
        view_width,
        left_kline_id: None,
        focus_datetime_ns: None,
        focus_position: None,
    })
}

fn delete_chart_command(chart_id: &str) -> DownstreamCommand {
    DownstreamCommand::SetChart(SetChartCommand {
        chart_id: chart_id.to_string(),
        symbols: Vec::new(),
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
    assert_eq!(
        chart_frame.payload["data"][0]["charts"]["client-chart"]["state"]["ins_list"],
        "SHFE.au2602"
    );
    assert_eq!(
        chart_frame.payload["data"][0]["charts"]["client-chart"]["state"]["duration"],
        60_000_000_000i64
    );
    assert_eq!(
        chart_frame.payload["data"][0]["charts"]["client-chart"]["state"]["view_width"],
        64
    );
}

#[test]
fn relay_engine_deletes_chart_subscription_on_empty_ins_list() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let client = ClientId::new(1);
    let source = SourceKey {
        symbols: vec!["SHFE.au2602".to_string()],
        duration_ns: 60_000_000_000,
        view_width: 64,
    };

    engine
        .handle_command(client, chart_command("client-chart"))
        .unwrap();
    let frames = engine
        .handle_command(client, delete_chart_command("client-chart"))
        .unwrap();

    assert_eq!(engine.interests().chart_interest_count(&source), 0);
    assert_eq!(engine.bootstrap_pending_len(), 0);
    assert_eq!(frames.len(), 1);
    assert_eq!(
        frames[0].payload["data"][0]["charts"]["client-chart"],
        serde_json::Value::Null
    );

    engine
        .ingest_tick("SHFE.au2602", tick(1, 0, 610.0))
        .unwrap();
    let frames = engine
        .ingest_tick("SHFE.au2602", tick(2, 60_000_000_000, 620.0))
        .unwrap();
    assert!(
        frames
            .iter()
            .all(|frame| frame.payload["data"][0].get("charts").is_none()),
        "deleted chart should not receive further chart updates"
    );
}

#[test]
fn relay_engine_synthesizes_klines_from_quote_updates() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let client = ClientId::new(1);

    engine
        .handle_command(client, chart_command_for("client-chart", vec!["DCE.m2609"]))
        .unwrap();

    engine
        .ingest_quote_at(
            "DCE.m2609",
            quote("DCE.m2609", "2026-06-17 13:56:00.000000", 3100.0),
            1_780_985_400_000,
        )
        .unwrap();
    let frames = engine
        .ingest_quote_at(
            "DCE.m2609",
            quote("DCE.m2609", "2026-06-17 13:57:00.000000", 3110.0),
            1_780_985_460_000,
        )
        .unwrap();

    let chart_frame = frames
        .iter()
        .find(|frame| frame.payload["data"][0].get("charts").is_some())
        .expect("quote update crossing a K-line window should emit chart metadata");
    assert_eq!(chart_frame.client_id, client);
    assert_eq!(
        chart_frame.payload["data"][0]["charts"]["client-chart"]["right_id"],
        0
    );
    let snapshot = engine.symbol_metrics_snapshot_at(1_780_985_460_000, &Default::default());
    let symbol = snapshot
        .symbols
        .iter()
        .find(|row| row.symbol == "DCE.m2609")
        .expect("quote update should create symbol telemetry");
    assert_eq!(symbol.ticks_ingested, 0);
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
fn chart_subscription_queues_tick_chart_even_when_universe_quote_is_subscribed() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let client = ClientId::new(1);

    engine.record_universe_refresh_success_for_symbols(
        ["SHFE.au2602"],
        "SHFE.au2602".len(),
        None,
        None,
        1,
    );
    engine
        .handle_command(client, chart_command("client-chart"))
        .unwrap();

    assert_eq!(
        engine.drain_pending_upstream_subscription_symbols(),
        vec!["SHFE.au2602".to_string()]
    );
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

#[test]
fn relay_engine_replays_and_fans_out_tick_charts_with_real_bounds() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    for id in 1..=3 {
        let mut row = tick(id, id * 1_000, 610.0 + id as f64);
        row.volume = 66_094_980 + id;
        assert!(engine.ingest_tick("SHFE.au2602", row).unwrap().is_empty());
    }
    let mut sparse_patch = tick(3, 3_000, 613.5);
    sparse_patch.volume = 66_094_985;
    assert!(
        engine
            .ingest_tick("SHFE.au2602", sparse_patch)
            .unwrap()
            .is_empty()
    );

    let first = ClientId::new(1);
    let replay = engine
        .handle_command(first, tick_chart_command("tick-a", 2))
        .unwrap();
    let tick_frame = replay
        .iter()
        .find(|frame| frame.payload["data"][0].get("ticks").is_some())
        .expect("tick replay should emit cached rows");
    let ticks = &tick_frame.payload["data"][0]["ticks"]["SHFE.au2602"];
    assert_eq!(ticks["last_id"], 3);
    assert_eq!(ticks["data"].as_object().unwrap().len(), 2);
    assert!(ticks["data"].get("2").is_some());
    assert_eq!(ticks["data"]["3"]["volume"], 66_094_985);
    let chart = replay
        .iter()
        .find_map(|frame| frame.payload["data"][0]["charts"].get("tick-a"))
        .expect("tick replay should mark the chart ready");
    assert_eq!(chart["left_id"], 2);
    assert_eq!(chart["right_id"], 3);
    assert_eq!(chart["ready"], true);
    assert_eq!(chart["more_data"], false);

    let second = ClientId::new(2);
    engine
        .handle_command(second, tick_chart_command("tick-b", 1))
        .unwrap();
    let mut live = tick(4, 4_000, 614.0);
    live.volume = 66_094_984;
    let frames = engine.ingest_tick("SHFE.au2602", live).unwrap();

    for (client, chart_id, left_id) in [(first, "tick-a", 3), (second, "tick-b", 4)] {
        assert!(frames.iter().any(|frame| {
            frame.client_id == client
                && frame.payload["data"][0]["ticks"]["SHFE.au2602"]["data"]["4"]["volume"]
                    == 66_094_984
        }));
        let chart = frames
            .iter()
            .find_map(|frame| {
                (frame.client_id == client)
                    .then(|| frame.payload["data"][0]["charts"].get(chart_id))
                    .flatten()
            })
            .expect("live tick should update every tick chart");
        assert_eq!(chart["left_id"], left_id);
        assert_eq!(chart["right_id"], 4);
        assert_eq!(chart["ready"], true);
        assert_eq!(chart["more_data"], false);
    }
}

#[test]
fn relay_engine_marks_empty_tick_chart_ready() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let frames = engine
        .handle_command(ClientId::new(1), tick_chart_command("empty-tick", 32))
        .unwrap();

    assert_eq!(
        frames[0].payload["data"][0]["ticks"]["SHFE.au2602"]["last_id"],
        -1
    );
    assert!(
        frames[0].payload["data"][0]["ticks"]["SHFE.au2602"]["data"]
            .as_object()
            .unwrap()
            .is_empty()
    );
    let chart = &frames[1].payload["data"][0]["charts"]["empty-tick"];
    assert_eq!(chart["left_id"], -1);
    assert_eq!(chart["right_id"], -1);
    assert_eq!(chart["ready"], true);
    assert_eq!(chart["more_data"], false);
}

#[test]
fn relay_engine_rejects_unbounded_or_multi_symbol_tick_charts() {
    let mut engine = RelayEngine::new_memory_only(16, 16);

    let oversized = engine
        .handle_command(ClientId::new(1), tick_chart_command("oversized", 10_001))
        .unwrap_err();
    assert!(oversized.to_string().contains("view_width exceeds 10000"));

    let multi_symbol = engine
        .handle_command(
            ClientId::new(1),
            DownstreamCommand::SetChart(SetChartCommand {
                chart_id: "multi-tick".to_string(),
                symbols: vec!["SHFE.au2602".to_string(), "DCE.m2609".to_string()],
                duration_ns: 0,
                view_width: 32,
                left_kline_id: None,
                focus_datetime_ns: None,
                focus_position: None,
            }),
        )
        .unwrap_err();
    assert!(
        multi_symbol
            .to_string()
            .contains("tick chart requires exactly one symbol")
    );
    assert_eq!(engine.metrics_snapshot().chart_subscriptions, 0);
}

#[test]
fn relay_engine_batches_max_wait_tick_window_into_one_frame() {
    let mut engine = RelayEngine::new_memory_only(10_000, 16);
    for id in 0..10_000 {
        assert!(
            engine
                .ingest_tick("SHFE.au2602", tick(id, id * 1_000, 610.0))
                .unwrap()
                .is_empty()
        );
    }

    let frames = engine
        .handle_command(
            ClientId::new(1),
            tick_chart_command("max-wait-window", 10_000),
        )
        .unwrap();
    assert_eq!(frames.len(), 2);
    let ticks = &frames[0].payload["data"][0]["ticks"]["SHFE.au2602"];
    assert_eq!(ticks["data"].as_object().unwrap().len(), 10_000);
    assert_eq!(ticks["last_id"], 9_999);
    assert!(
        serde_json::to_vec(frames[0].payload.as_ref())
            .unwrap()
            .len()
            < 4 * 1024 * 1024
    );
}

#[test]
fn relay_engine_keeps_multi_symbol_kline_state_separate_and_emits_binding() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let client = ClientId::new(1);

    engine
        .handle_command(
            client,
            chart_command_for("multi-chart", vec!["SHFE.au2602", "DCE.m2609"]),
        )
        .unwrap();

    engine
        .ingest_tick("SHFE.au2602", tick(1, 0, 610.0))
        .unwrap();
    engine
        .ingest_tick("DCE.m2609", tick(10, 0, 3300.0))
        .unwrap();
    let primary_frames = engine
        .ingest_tick("SHFE.au2602", tick(2, 60_000_000_000, 612.0))
        .unwrap();
    let secondary_frames = engine
        .ingest_tick("DCE.m2609", tick(11, 60_000_000_000, 3310.0))
        .unwrap();

    let primary_kline = primary_frames
        .iter()
        .find_map(|frame| {
            frame.payload["data"][0]["klines"]["SHFE.au2602"]["60000000000"]["data"]["0"]
                .as_object()
        })
        .expect("primary completed kline should be emitted");
    assert_eq!(primary_kline["close"], 610.0);
    assert_eq!(primary_kline["high"], 610.0);

    let secondary_kline = secondary_frames
        .iter()
        .find_map(|frame| {
            frame.payload["data"][0]["klines"]["DCE.m2609"]["60000000000"]["data"]["0"].as_object()
        })
        .expect("secondary completed kline should be emitted");
    assert_eq!(secondary_kline["close"], 3300.0);
    assert_eq!(secondary_kline["high"], 3300.0);

    let binding = secondary_frames
        .iter()
        .find_map(|frame| {
            frame.payload["data"][0]["klines"]["SHFE.au2602"]["60000000000"]["binding"]["DCE.m2609"]
                ["0"]
                .as_i64()
        })
        .expect("multi-symbol chart should bind secondary row to primary row");
    assert_eq!(binding, 0);
}

#[test]
fn relay_engine_replays_cached_multi_symbol_klines_with_binding() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let client = ClientId::new(1);

    engine
        .ingest_tick("SHFE.au2602", tick(1, 0, 610.0))
        .unwrap();
    engine
        .ingest_tick("DCE.m2609", tick(10, 0, 3300.0))
        .unwrap();
    engine
        .ingest_tick("SHFE.au2602", tick(2, 60_000_000_000, 612.0))
        .unwrap();
    engine
        .ingest_tick("DCE.m2609", tick(11, 60_000_000_000, 3310.0))
        .unwrap();

    let frames = engine
        .handle_command(
            client,
            chart_command_for("multi-chart", vec!["SHFE.au2602", "DCE.m2609"]),
        )
        .unwrap();

    assert!(
        frames.iter().any(|frame| {
            frame.payload["data"][0]["klines"]["SHFE.au2602"]["60000000000"]["data"]["0"]["close"]
                == 610.0
        }),
        "primary cached kline should be replayed"
    );
    assert!(
        frames.iter().any(|frame| {
            frame.payload["data"][0]["klines"]["DCE.m2609"]["60000000000"]["data"]["0"]["close"]
                == 3300.0
        }),
        "secondary cached kline should be replayed"
    );
    assert!(
        frames.iter().any(|frame| {
            frame.payload["data"][0]["klines"]["SHFE.au2602"]["60000000000"]["binding"]["DCE.m2609"]
                ["0"]
                == 0
        }),
        "cached replay should include primary-to-secondary binding"
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
