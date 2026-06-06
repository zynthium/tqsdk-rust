use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::json;
use tqsdk_relay::{RelayConfig, RelayEngine, UpstreamTickChart, decode_upstream_ticks};

#[path = "../../tqsdk-core/tests/support/websocket.rs"]
mod websocket_support;

#[test]
fn config_accepts_explicit_futures_universe() {
    let config = RelayConfig {
        futures_symbols: vec!["SHFE.au2602".to_string(), "DCE.m2609".to_string()],
        ..RelayConfig::default()
    };

    config.validate().unwrap();
    assert_eq!(config.futures_symbols.len(), 2);
}

#[test]
fn config_rejects_empty_futures_symbol() {
    let config = RelayConfig {
        futures_symbols: vec!["SHFE.au2602".to_string(), " ".to_string()],
        ..RelayConfig::default()
    };

    let err = config.validate().unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid relay config: futures_symbols must not contain empty symbols"
    );
}

#[test]
fn upstream_tick_chart_uses_duration_zero_and_sorted_symbols() {
    let chart = UpstreamTickChart::new(
        "relay-upstream-all-futures-ticks",
        ["DCE.m2609", "SHFE.au2602"],
        10_000,
    )
    .unwrap();

    assert_eq!(chart.chart_id(), "relay-upstream-all-futures-ticks");
    assert_eq!(chart.duration_ns(), 0);
    assert_eq!(chart.view_width(), 10_000);
    assert_eq!(
        chart.symbols(),
        &["DCE.m2609".to_string(), "SHFE.au2602".to_string()]
    );
}

#[test]
fn config_builds_upstream_tick_chart_from_futures_symbols() {
    let config = RelayConfig {
        futures_symbols: vec!["SHFE.au2602".to_string(), "DCE.m2609".to_string()],
        ..RelayConfig::default()
    };

    let chart = config.upstream_tick_chart().unwrap().unwrap();

    assert_eq!(chart.chart_id(), "relay-upstream-all-futures-ticks");
    assert_eq!(chart.symbols(), &["DCE.m2609", "SHFE.au2602"]);
    assert_eq!(chart.duration_ns(), 0);
    assert_eq!(chart.view_width(), 10_000);
}

#[test]
fn config_omits_upstream_tick_chart_without_futures_symbols() {
    let config = RelayConfig::default();

    assert!(config.upstream_tick_chart().unwrap().is_none());
}

#[test]
fn decode_upstream_ticks_extracts_tick_rows_from_rtn_data() {
    let ticks = decode_upstream_ticks(json!({
        "aid": "rtn_data",
        "data": [
            {
                "ticks": {
                    "SHFE.au2602": {
                        "last_id": 17,
                        "data": {
                            "17": {
                                "id": 17,
                                "datetime": 1_000,
                                "last_price": 610.0,
                                "volume": 170,
                                "open_interest": 1007
                            }
                        }
                    }
                }
            }
        ]
    }))
    .unwrap();

    assert_eq!(ticks.len(), 1);
    assert_eq!(ticks[0].symbol, "SHFE.au2602");
    assert_eq!(ticks[0].row.id, 17);
    assert_eq!(ticks[0].row.datetime, 1_000);
    assert_eq!(ticks[0].row.last_price, 610.0);
    assert_eq!(ticks[0].row.volume, 170);
    assert_eq!(ticks[0].row.open_interest, 1007);
}

#[test]
fn decode_upstream_ticks_uses_data_key_as_row_id_when_id_field_is_absent() {
    let ticks = decode_upstream_ticks(json!({
        "aid": "rtn_data",
        "data": [
            {
                "ticks": {
                    "DCE.m2609": {
                        "data": {
                            "9": {
                                "datetime": 2_000,
                                "last_price": 3300.0,
                                "volume": 90,
                                "open_interest": 900
                            }
                        }
                    }
                }
            }
        ]
    }))
    .unwrap();

    assert_eq!(ticks.len(), 1);
    assert_eq!(ticks[0].symbol, "DCE.m2609");
    assert_eq!(ticks[0].row.id, 9);
}

#[test]
fn decode_upstream_ticks_ignores_non_tick_rtn_data_frames() {
    let ticks = decode_upstream_ticks(json!({
        "aid": "rtn_data",
        "data": [
            {
                "quotes": {
                    "SHFE.au2602": {
                        "last_price": 610.0
                    }
                }
            }
        ]
    }))
    .unwrap();

    assert!(ticks.is_empty());
}

#[test]
fn decode_upstream_ticks_rejects_tick_rows_missing_required_fields() {
    let err = decode_upstream_ticks(json!({
        "aid": "rtn_data",
        "data": [
            {
                "ticks": {
                    "SHFE.au2602": {
                        "data": {
                            "17": {
                                "datetime": 1_000,
                                "volume": 170,
                                "open_interest": 1007
                            }
                        }
                    }
                }
            }
        ]
    }))
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid relay protocol: upstream tick row missing last_price"
    );
}

#[tokio::test]
async fn websocket_upstream_tick_source_reads_tick_frame() {
    use tqsdk_relay::{UpstreamTickSource, WebSocketUpstreamTickSource};
    use websocket_support::TestWebSocketServer;

    let server = TestWebSocketServer::spawn(|mut socket| {
        socket
            .send_text(
                json!({
                    "aid": "rtn_data",
                    "data": [
                        {
                            "ticks": {
                                "SHFE.au2602": {
                                    "data": {
                                        "17": {
                                            "datetime": 1_000,
                                            "last_price": 610.0,
                                            "volume": 170,
                                            "open_interest": 1007
                                        }
                                    }
                                }
                            }
                        }
                    ]
                })
                .to_string(),
            )
            .unwrap();
        socket.send_close().unwrap();
    })
    .unwrap();

    let mut source = WebSocketUpstreamTickSource::connect(server.url("/market"))
        .await
        .unwrap();

    let tick = source.next_tick().await.unwrap();
    assert_eq!(tick.symbol, "SHFE.au2602");
    assert_eq!(tick.row.id, 17);
    assert_eq!(tick.row.last_price, 610.0);
    assert!(source.next_tick().await.is_none());
    server.join();
}

#[tokio::test]
async fn websocket_upstream_tick_source_buffers_multiple_ticks_from_one_frame() {
    use tqsdk_relay::{UpstreamTickSource, WebSocketUpstreamTickSource};
    use websocket_support::TestWebSocketServer;

    let server = TestWebSocketServer::spawn(|mut socket| {
        socket
            .send_text(
                json!({
                    "aid": "rtn_data",
                    "data": [
                        {
                            "ticks": {
                                "SHFE.au2602": {
                                    "data": {
                                        "17": {
                                            "datetime": 1_000,
                                            "last_price": 610.0,
                                            "volume": 170,
                                            "open_interest": 1007
                                        },
                                        "18": {
                                            "datetime": 2_000,
                                            "last_price": 611.0,
                                            "volume": 180,
                                            "open_interest": 1008
                                        }
                                    }
                                }
                            }
                        }
                    ]
                })
                .to_string(),
            )
            .unwrap();
        socket.send_close().unwrap();
    })
    .unwrap();

    let mut source = WebSocketUpstreamTickSource::connect(server.url("/market"))
        .await
        .unwrap();

    let first = source.next_tick().await.unwrap();
    let second = source.next_tick().await.unwrap();
    assert_eq!(first.row.id, 17);
    assert_eq!(second.row.id, 18);
    assert!(source.next_tick().await.is_none());
    server.join();
}

#[tokio::test]
async fn websocket_upstream_tick_source_subscribes_tick_chart_on_connect() {
    use tqsdk_relay::{UpstreamTickChart, WebSocketUpstreamTickSource};
    use websocket_support::{ClientFrame, TestWebSocketServer};

    let server = TestWebSocketServer::spawn(|mut socket| {
        let ClientFrame::Text(set_chart) = socket.recv().unwrap() else {
            panic!("expected upstream set_chart text frame");
        };
        let set_chart: serde_json::Value = serde_json::from_str(&set_chart).unwrap();
        assert_eq!(set_chart["aid"], "set_chart");
        assert_eq!(set_chart["chart_id"], "relay-upstream-all-futures-ticks");
        assert_eq!(set_chart["ins_list"], "DCE.m2609,SHFE.au2602");
        assert_eq!(set_chart["duration"], 0);
        assert_eq!(set_chart["view_width"], 10_000);

        let ClientFrame::Text(peek) = socket.recv().unwrap() else {
            panic!("expected upstream peek_message text frame");
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&peek).unwrap(),
            json!({"aid": "peek_message"})
        );
        socket.send_close().unwrap();
    })
    .unwrap();
    let chart = UpstreamTickChart::new(
        "relay-upstream-all-futures-ticks",
        ["SHFE.au2602", "DCE.m2609"],
        10_000,
    )
    .unwrap();

    let _source =
        WebSocketUpstreamTickSource::connect_with_tick_chart(server.url("/market"), chart)
            .await
            .unwrap();
    server.join();
}

#[tokio::test]
async fn configured_upstream_source_subscribes_configured_futures_symbols() {
    use tqsdk_relay::{RelayConfig, connect_configured_upstream};
    use websocket_support::{ClientFrame, TestWebSocketServer};

    let server = TestWebSocketServer::spawn(|mut socket| {
        let ClientFrame::Text(set_chart) = socket.recv().unwrap() else {
            panic!("expected upstream set_chart text frame");
        };
        let set_chart: serde_json::Value = serde_json::from_str(&set_chart).unwrap();
        assert_eq!(set_chart["aid"], "set_chart");
        assert_eq!(set_chart["chart_id"], "relay-upstream-all-futures-ticks");
        assert_eq!(set_chart["ins_list"], "DCE.m2609,SHFE.au2602");
        assert_eq!(set_chart["duration"], 0);
        assert_eq!(set_chart["view_width"], 10_000);

        let ClientFrame::Text(peek) = socket.recv().unwrap() else {
            panic!("expected upstream peek_message text frame");
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&peek).unwrap(),
            json!({"aid": "peek_message"})
        );
        socket.send_close().unwrap();
    })
    .unwrap();
    let config = RelayConfig {
        upstream_market_url: server.url("/market"),
        futures_symbols: vec!["SHFE.au2602".to_string(), "DCE.m2609".to_string()],
        ..RelayConfig::default()
    };

    let _source = connect_configured_upstream(&config).await.unwrap().unwrap();
    server.join();
}

#[tokio::test]
async fn configured_upstream_source_is_absent_without_futures_symbols() {
    use tqsdk_relay::{RelayConfig, connect_configured_upstream};

    assert!(
        connect_configured_upstream(&RelayConfig::default())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn configured_upstream_pump_ingests_upstream_ticks() {
    use tqsdk_relay::{RelayConfig, RelayServer, spawn_configured_upstream_pump};
    use websocket_support::{ClientFrame, TestWebSocketServer};

    let upstream = TestWebSocketServer::spawn(|mut socket| {
        let ClientFrame::Text(set_chart) = socket.recv().unwrap() else {
            panic!("expected upstream set_chart text frame");
        };
        let set_chart: serde_json::Value = serde_json::from_str(&set_chart).unwrap();
        assert_eq!(set_chart["aid"], "set_chart");
        assert_eq!(set_chart["ins_list"], "SHFE.au2602");

        let ClientFrame::Text(peek) = socket.recv().unwrap() else {
            panic!("expected upstream peek_message text frame");
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&peek).unwrap(),
            json!({"aid": "peek_message"})
        );
        socket
            .send_text(
                json!({
                    "aid": "rtn_data",
                    "data": [
                        {
                            "ticks": {
                                "SHFE.au2602": {
                                    "data": {
                                        "17": {
                                            "datetime": 1_000,
                                            "last_price": 610.0,
                                            "volume": 170,
                                            "open_interest": 1007
                                        }
                                    }
                                }
                            }
                        }
                    ]
                })
                .to_string(),
            )
            .unwrap();
        socket.send_close().unwrap();
    })
    .unwrap();
    let config = RelayConfig {
        upstream_market_url: upstream.url("/market"),
        futures_symbols: vec!["SHFE.au2602".to_string()],
        ..RelayConfig::default()
    };
    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
    let server = RelayServer::new(engine.clone());

    let _shutdown = spawn_configured_upstream_pump(&config, server)
        .await
        .unwrap()
        .unwrap();

    wait_for_ticks_ingested(&engine, 1).await;
    assert_eq!(engine.lock().unwrap().metrics_snapshot().ticks_ingested, 1);
    assert_eq!(
        engine.lock().unwrap().health_snapshot().upstream_status,
        tqsdk_relay::RelaySourceStatus::Up
    );
    upstream.join();
}

async fn wait_for_ticks_ingested(engine: &Arc<Mutex<RelayEngine>>, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let ticks_ingested = engine.lock().unwrap().metrics_snapshot().ticks_ingested;
        if ticks_ingested >= expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {expected} ingested ticks; saw {ticks_ingested}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
