use std::io::ErrorKind;
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{Local, Timelike};
use serde_json::json;
use tqsdk_relay::{
    DailyRefreshTime, FuturesUniverseRefreshSchedule, RelayConfig, RelayEngine, UniverseExpression,
    UpstreamMarketEvent, UpstreamTickChart, decode_upstream_market_report,
    decode_upstream_tick_report, decode_upstream_ticks,
};

#[path = "../../tqsdk-core/tests/support/websocket.rs"]
mod websocket_support;

fn recv_text_json(
    socket: &mut websocket_support::TestWebSocketConnection,
    expected: &str,
) -> serde_json::Value {
    let websocket_support::ClientFrame::Text(text) = socket.recv().unwrap() else {
        panic!("expected upstream {expected} text frame");
    };
    serde_json::from_str(&text).unwrap()
}

fn expect_set_chart(
    socket: &mut websocket_support::TestWebSocketConnection,
    ins_list: &str,
) -> serde_json::Value {
    let set_chart = recv_text_json(socket, "set_chart");
    assert_eq!(set_chart["aid"], "set_chart");
    assert_eq!(set_chart["ins_list"], ins_list);
    set_chart
}

fn expect_peek_message(socket: &mut websocket_support::TestWebSocketConnection) {
    assert_eq!(
        recv_text_json(socket, "peek_message"),
        json!({"aid": "peek_message"})
    );
}

fn expect_subscribe_quote(socket: &mut websocket_support::TestWebSocketConnection, ins_list: &str) {
    assert_eq!(
        recv_text_json(socket, "subscribe_quote"),
        json!({"aid": "subscribe_quote", "ins_list": ins_list})
    );
}

fn expect_initial_universe_subscriptions(
    socket: &mut websocket_support::TestWebSocketConnection,
    ins_list: &str,
) {
    expect_subscribe_quote(socket, ins_list);
    expect_peek_message(socket);
}

#[test]
fn config_accepts_explicit_futures_universe() {
    let config = RelayConfig {
        futures_universe_expression: Some(
            UniverseExpression::parse("symbol:SHFE.au2602,DCE.m2609").unwrap(),
        ),
        ..RelayConfig::default()
    };

    config.validate().unwrap();
    assert!(config.has_upstream_futures_source());
}

#[test]
fn universe_expression_rejects_empty_symbol_value() {
    let err = UniverseExpression::parse("symbol:SHFE.au2602, ").unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid data query input: futures universe selector values must not contain empty value"
    );
}

#[test]
fn upstream_tick_chart_uses_duration_zero_and_single_symbol() {
    let chart =
        UpstreamTickChart::new("relay-upstream-tick-DCE_m2609-10000", ["DCE.m2609"], 10_000)
            .unwrap();

    assert_eq!(chart.chart_id(), "relay-upstream-tick-DCE_m2609-10000");
    assert_eq!(chart.duration_ns(), 0);
    assert_eq!(chart.view_width(), 10_000);
    assert_eq!(chart.symbol(), "DCE.m2609");
    assert_eq!(chart.symbols(), &["DCE.m2609".to_string()]);
    assert_eq!(chart.ins_list_chars(), "DCE.m2609".len());
}

#[test]
fn upstream_tick_chart_rejects_multiple_symbols() {
    let err = UpstreamTickChart::new(
        "relay-upstream-tick-multi-10000",
        ["DCE.m2609", "SHFE.au2602"],
        10_000,
    )
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid relay config: upstream tick chart requires exactly one symbol"
    );
}

#[test]
fn config_builds_upstream_tick_charts_from_symbols() {
    let config = RelayConfig::default();

    let charts = config
        .upstream_tick_charts_for_symbols(["SHFE.au2602", "DCE.m2609"])
        .unwrap();

    assert_eq!(charts.len(), 2);
    assert_eq!(charts[0].chart_id(), "relay-upstream-tick-DCE_m2609-10000");
    assert_eq!(charts[0].symbol(), "DCE.m2609");
    assert_eq!(charts[0].duration_ns(), 0);
    assert_eq!(charts[0].view_width(), 10_000);
    assert_eq!(
        charts[1].chart_id(),
        "relay-upstream-tick-SHFE_au2602-10000"
    );
    assert_eq!(charts[1].symbol(), "SHFE.au2602");
    assert_eq!(charts[1].duration_ns(), 0);
    assert_eq!(charts[1].view_width(), 10_000);
}

#[test]
fn config_uses_configured_upstream_tick_view_width() {
    let config = RelayConfig {
        upstream_tick_view_width: 1,
        ..RelayConfig::default()
    };

    let charts = config
        .upstream_tick_charts_for_symbols(["SHFE.au2602"])
        .unwrap();

    assert_eq!(charts[0].view_width(), 1);
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
fn decode_upstream_market_report_extracts_quote_updates() {
    let report = decode_upstream_market_report(json!({
        "aid": "rtn_data",
        "data": [
            {
                "quotes": {
                    "SHFE.ag2705": {
                        "datetime": "1780985438500000000",
                        "instrument_id": "SHFE.ag2705",
                        "last_price": 16666.0,
                        "volume": 12,
                        "open_interest": 34
                    }
                }
            }
        ]
    }))
    .unwrap();

    assert!(report.ticks().is_empty());
    assert_eq!(report.quotes().len(), 1);
    assert_eq!(report.quotes()[0].symbol, "SHFE.ag2705");
    assert_eq!(report.quotes()[0].quote.instrument_id, "SHFE.ag2705");
    assert_eq!(report.quotes()[0].quote.datetime, "1780985438500000000");
    assert_eq!(report.quotes()[0].quote.last_price, 16666.0);
    assert_eq!(report.quotes()[0].quote.volume, 12);
    assert_eq!(report.quotes()[0].quote.open_interest, 34);
}

#[test]
fn decode_upstream_market_report_extracts_trading_status_updates() {
    let report = decode_upstream_market_report(json!({
        "aid": "rtn_data",
        "data": [
            {
                "trading_status": {
                    "SHFE.au2602": {
                        "symbol": "SHFE.au2602",
                        "trade_status": "AUCTIONORDERING"
                    }
                }
            }
        ]
    }))
    .unwrap();

    assert!(report.ticks().is_empty());
    assert!(report.quotes().is_empty());
    assert_eq!(report.trading_statuses().len(), 1);
    assert_eq!(report.trading_statuses()[0].symbol, "SHFE.au2602");
    assert_eq!(
        report.trading_statuses()[0].trading_status.trade_status,
        "AUCTIONORDERING"
    );

    let events = report.into_events();
    assert_eq!(events.len(), 1);
    let UpstreamMarketEvent::TradingStatus(status) = &events[0] else {
        panic!("expected trading status event");
    };
    assert_eq!(status.symbol, "SHFE.au2602");
    assert_eq!(status.trading_status.trade_status, "AUCTIONORDERING");
}

#[test]
fn decode_upstream_ticks_decodes_python_default_last_price_rows() {
    let ticks = decode_upstream_ticks(json!({
        "aid": "rtn_data",
        "data": [
            {
                "ticks": {
                    "SHFE.rb2606": {
                        "data": {
                            "3299418": {
                                "datetime": 1_780_569_599_093_000_000i64,
                                "last_price": "",
                                "volume": 0,
                                "open_interest": 660
                            },
                            "3299419": {
                                "datetime": 1_780_569_600_000_000_000i64,
                                "last_price": null,
                                "volume": 20,
                                "open_interest": 660
                            },
                            "3299420": {
                                "datetime": 1_780_569_600_250_000_000i64,
                                "last_price": "not-a-number",
                                "volume": 25,
                                "open_interest": 660
                            },
                            "3299421": {
                                "datetime": 1_780_569_600_500_000_000i64,
                                "last_price": 3094.0,
                                "volume": 30,
                                "open_interest": 660
                            }
                        }
                    }
                }
            }
        ]
    }))
    .unwrap();

    assert_eq!(ticks.len(), 4);
    assert_eq!(ticks[0].symbol, "SHFE.rb2606");
    assert_eq!(ticks[0].row.id, 3_299_418);
    assert!(ticks[0].row.last_price.is_nan());
    assert_eq!(ticks[1].row.id, 3_299_419);
    assert!(ticks[1].row.last_price.is_nan());
    assert_eq!(ticks[2].row.id, 3_299_420);
    assert!(ticks[2].row.last_price.is_nan());
    assert_eq!(ticks[3].row.id, 3_299_421);
    assert_eq!(ticks[3].row.last_price, 3094.0);
}

#[test]
fn decode_upstream_tick_report_counts_invalid_required_fields_and_keeps_valid_rows() {
    let report = decode_upstream_tick_report(json!({
        "aid": "rtn_data",
        "data": [
            {
                "ticks": {
                    "SHFE.rb2606": {
                        "data": {
                            "3299418": {
                                "datetime": 1_780_569_599_093_000_000i64,
                                "last_price": "",
                                "volume": 0,
                                "open_interest": 660
                            },
                            "3299419": {
                                "datetime": 1_780_569_600_250_000_000i64,
                                "last_price": 3093.0,
                                "volume": "",
                                "open_interest": 660
                            },
                            "3299420": {
                                "datetime": 1_780_569_600_500_000_000i64,
                                "last_price": 3094.0,
                                "volume": 30,
                                "open_interest": 660
                            }
                        }
                    }
                }
            }
        ]
    }))
    .unwrap();

    assert_eq!(report.invalid_rows(), 1);
    assert_eq!(
        report.invalid_rows_by_symbol().get("SHFE.rb2606").copied(),
        Some(1)
    );
    assert!(
        report
            .last_invalid_row_error()
            .unwrap()
            .contains("SHFE.rb2606 row 3299419")
    );
    assert!(report.last_invalid_row_error().unwrap().contains("volume"));
    assert_eq!(report.ticks().len(), 2);
    assert_eq!(report.ticks()[0].row.id, 3_299_418);
    assert!(report.ticks()[0].row.last_price.is_nan());
    assert_eq!(report.ticks()[1].row.id, 3_299_420);
}

#[test]
fn decode_upstream_ticks_skips_rows_missing_required_fields() {
    let ticks = decode_upstream_ticks(json!({
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
    .unwrap();

    assert!(ticks.is_empty());
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
async fn websocket_upstream_tick_source_merges_sparse_tick_row_patches() {
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
        expect_peek_message(&mut socket);
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
                                            "last_price": 611.0,
                                            "volume": 180
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
        expect_peek_message(&mut socket);
        socket.send_close().unwrap();
    })
    .unwrap();

    let mut source = WebSocketUpstreamTickSource::connect(server.url("/market"))
        .await
        .unwrap();

    let first = source.next_tick().await.unwrap();
    let second = source.next_tick().await.unwrap();

    assert_eq!(first.row.id, 17);
    assert_eq!(first.row.datetime, 1_000);
    assert_eq!(first.row.last_price, 610.0);
    assert_eq!(first.row.volume, 170);
    assert_eq!(first.row.open_interest, 1007);
    assert_eq!(second.row.id, 17);
    assert_eq!(second.row.datetime, 1_000);
    assert_eq!(second.row.last_price, 611.0);
    assert_eq!(second.row.volume, 180);
    assert_eq!(second.row.open_interest, 1007);
    assert_eq!(source.take_invalid_tick_rows(), 0);
    assert!(source.next_tick().await.is_none());
    server.join();
}

#[tokio::test]
async fn websocket_upstream_tick_source_buffers_incomplete_sparse_tick_rows_until_complete() {
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
                                            "volume": 170
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
        expect_peek_message(&mut socket);
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
                                            "last_price": 611.0,
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
        expect_peek_message(&mut socket);
        socket.send_close().unwrap();
    })
    .unwrap();

    let mut source = WebSocketUpstreamTickSource::connect(server.url("/market"))
        .await
        .unwrap();

    let tick = source.next_tick().await.unwrap();

    assert_eq!(tick.row.id, 17);
    assert_eq!(tick.row.datetime, 1_000);
    assert_eq!(tick.row.last_price, 611.0);
    assert_eq!(tick.row.volume, 170);
    assert_eq!(tick.row.open_interest, 1007);
    assert_eq!(source.take_invalid_tick_rows(), 0);
    assert!(source.next_tick().await.is_none());
    server.join();
}

#[tokio::test]
async fn websocket_upstream_tick_source_decodes_string_last_price_as_nan() {
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
                                            "last_price": "not-a-number",
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
    assert_eq!(tick.row.id, 17);
    assert!(tick.row.last_price.is_nan());
    assert_eq!(source.take_invalid_tick_rows(), 0);
    assert!(source.take_invalid_tick_rows_by_symbol().is_empty());
    assert!(source.take_last_invalid_tick_row_error().is_none());
    assert!(source.next_tick().await.is_none());
    server.join();
}

#[tokio::test]
async fn websocket_upstream_tick_source_exposes_invalid_tick_row_diagnostics() {
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
                                            "volume": "",
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

    assert!(source.next_tick().await.is_none());
    assert_eq!(source.take_invalid_tick_rows(), 1);
    assert_eq!(
        source
            .take_invalid_tick_rows_by_symbol()
            .get("SHFE.au2602")
            .copied(),
        Some(1)
    );
    let error = source.take_last_invalid_tick_row_error().unwrap();
    assert!(error.contains("SHFE.au2602 row 17"));
    assert!(error.contains("volume"));
    assert_eq!(source.take_invalid_tick_rows(), 0);
    assert!(source.take_invalid_tick_rows_by_symbol().is_empty());
    assert!(source.take_last_invalid_tick_row_error().is_none());
    server.join();
}

#[tokio::test]
async fn websocket_upstream_tick_source_bootstraps_quote_before_tick_chart() {
    use tqsdk_relay::{UpstreamTickChart, WebSocketUpstreamTickSource};
    use websocket_support::TestWebSocketServer;

    let server = TestWebSocketServer::spawn(|mut socket| {
        expect_initial_universe_subscriptions(&mut socket, "SHFE.au2602");
        let set_chart = expect_set_chart(&mut socket, "SHFE.au2602");
        assert_eq!(
            set_chart["chart_id"],
            "relay-upstream-tick-SHFE_au2602-10000"
        );
        assert_eq!(set_chart["duration"], 0);
        assert_eq!(set_chart["view_width"], 10_000);
        expect_peek_message(&mut socket);
        socket.send_close().unwrap();
    })
    .unwrap();
    let chart = UpstreamTickChart::new(
        "relay-upstream-tick-SHFE_au2602-10000",
        ["SHFE.au2602"],
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
async fn websocket_upstream_tick_source_subscribes_tick_chart_on_connect() {
    use tqsdk_relay::{UpstreamTickChart, WebSocketUpstreamTickSource};
    use websocket_support::TestWebSocketServer;

    let server = TestWebSocketServer::spawn(|mut socket| {
        expect_initial_universe_subscriptions(&mut socket, "DCE.m2609,SHFE.au2602");
        let set_chart = expect_set_chart(&mut socket, "DCE.m2609");
        assert_eq!(set_chart["chart_id"], "relay-upstream-tick-DCE_m2609-10000");
        assert_eq!(set_chart["duration"], 0);
        assert_eq!(set_chart["view_width"], 10_000);
        expect_peek_message(&mut socket);

        let set_chart = expect_set_chart(&mut socket, "SHFE.au2602");
        assert_eq!(
            set_chart["chart_id"],
            "relay-upstream-tick-SHFE_au2602-10000"
        );
        assert_eq!(set_chart["duration"], 0);
        assert_eq!(set_chart["view_width"], 10_000);
        expect_peek_message(&mut socket);
        socket.send_close().unwrap();
    })
    .unwrap();
    let charts = vec![
        UpstreamTickChart::new("relay-upstream-tick-DCE_m2609-10000", ["DCE.m2609"], 10_000)
            .unwrap(),
        UpstreamTickChart::new(
            "relay-upstream-tick-SHFE_au2602-10000",
            ["SHFE.au2602"],
            10_000,
        )
        .unwrap(),
    ];

    let _source =
        WebSocketUpstreamTickSource::connect_with_tick_charts(server.url("/market"), charts)
            .await
            .unwrap();
    server.join();
}

#[tokio::test]
async fn websocket_upstream_tick_source_reports_progress_for_empty_rtn_data() {
    use tqsdk_relay::{
        UpstreamSourceUpdate, UpstreamTickChart, UpstreamTickSource, WebSocketUpstreamTickSource,
    };
    use websocket_support::TestWebSocketServer;

    let server = TestWebSocketServer::spawn(|mut socket| {
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        expect_initial_universe_subscriptions(&mut socket, "SHFE.au2602");
        expect_set_chart(&mut socket, "SHFE.au2602");
        expect_peek_message(&mut socket);
        socket
            .send_text(json!({"aid": "rtn_data", "data": [{}]}).to_string())
            .unwrap();
        expect_peek_message(&mut socket);
        socket.send_close().unwrap();
    })
    .unwrap();
    let chart = UpstreamTickChart::new(
        "relay-upstream-tick-SHFE_au2602-10000",
        ["SHFE.au2602"],
        10_000,
    )
    .unwrap();

    let mut source =
        WebSocketUpstreamTickSource::connect_with_tick_chart(server.url("/market"), chart)
            .await
            .unwrap();
    let initial = source.take_progress();
    assert!(initial.transport_connected);
    assert!(initial.subscription_sent);

    let update = source.next_update().await.unwrap();
    assert!(matches!(update, UpstreamSourceUpdate::Progress));
    let progress = source.take_progress();
    assert_eq!(progress.frames_received, 1);
    assert_eq!(progress.events_decoded, 0);
    assert!(progress.last_peek_delay_ms.is_some());
    assert!(progress.last_decode_ms.is_some());
    assert!(progress.unix_secs > 0);
    server.join();
}

#[tokio::test]
async fn websocket_upstream_tick_source_peeks_after_each_received_frame() {
    use tqsdk_relay::{UpstreamTickChart, UpstreamTickSource, WebSocketUpstreamTickSource};
    use websocket_support::TestWebSocketServer;

    let server = TestWebSocketServer::spawn(|mut socket| {
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        expect_initial_universe_subscriptions(&mut socket, "SHFE.au2602");
        expect_set_chart(&mut socket, "SHFE.au2602");
        expect_peek_message(&mut socket);
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

        expect_peek_message(&mut socket);
        socket
            .send_text(
                json!({
                    "aid": "rtn_data",
                    "data": [
                        {
                            "ticks": {
                                "SHFE.au2602": {
                                    "data": {
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

        expect_peek_message(&mut socket);
        socket.send_close().unwrap();
    })
    .unwrap();
    let chart = UpstreamTickChart::new(
        "relay-upstream-tick-SHFE_au2602-10000",
        ["SHFE.au2602"],
        10_000,
    )
    .unwrap();

    let mut source =
        WebSocketUpstreamTickSource::connect_with_tick_chart(server.url("/market"), chart)
            .await
            .unwrap();

    let first = source.next_tick().await.unwrap();
    let second = source.next_tick().await.unwrap();
    assert_eq!(first.row.id, 17);
    assert_eq!(second.row.id, 18);
    server.join();
}

#[tokio::test]
async fn websocket_upstream_tick_source_peeks_while_idle() {
    use tqsdk_relay::{
        UpstreamSourceUpdate, UpstreamTickChart, UpstreamTickSource, WebSocketUpstreamTickSource,
    };
    use websocket_support::TestWebSocketServer;

    let server = TestWebSocketServer::spawn(|mut socket| {
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        expect_initial_universe_subscriptions(&mut socket, "SHFE.au2602");
        expect_set_chart(&mut socket, "SHFE.au2602");
        expect_peek_message(&mut socket);
        expect_peek_message(&mut socket);
        socket.send_close().unwrap();
    })
    .unwrap();
    let chart =
        UpstreamTickChart::new("relay-upstream-tick-SHFE_au2602-1", ["SHFE.au2602"], 1).unwrap();

    let mut source =
        WebSocketUpstreamTickSource::connect_with_tick_chart(server.url("/market"), chart)
            .await
            .unwrap();

    let update = tokio::time::timeout(Duration::from_secs(2), source.next_update())
        .await
        .expect("idle upstream should produce progress after sending peek");
    assert!(matches!(update, Some(UpstreamSourceUpdate::Progress)));
    let progress = source.take_progress();
    assert_eq!(progress.frames_received, 0);
    assert_eq!(progress.events_decoded, 0);
    assert!(progress.last_peek_delay_ms.is_some());
    server.join();
}

#[tokio::test]
async fn websocket_upstream_tick_source_peeks_before_json_decode() {
    use tqsdk_relay::{UpstreamTickChart, UpstreamTickSource, WebSocketUpstreamTickSource};
    use websocket_support::TestWebSocketServer;

    let server = TestWebSocketServer::spawn(|mut socket| {
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        expect_initial_universe_subscriptions(&mut socket, "SHFE.au2602");
        expect_set_chart(&mut socket, "SHFE.au2602");
        expect_peek_message(&mut socket);

        socket.send_text("{not-json".to_string()).unwrap();
        expect_peek_message(&mut socket);
    })
    .unwrap();
    let chart =
        UpstreamTickChart::new("relay-upstream-tick-SHFE_au2602-1", ["SHFE.au2602"], 1).unwrap();

    let mut source =
        WebSocketUpstreamTickSource::connect_with_tick_chart(server.url("/market"), chart)
            .await
            .unwrap();

    assert!(source.next_update().await.is_none());
    server.join();
}

#[tokio::test]
async fn configured_upstream_source_decodes_quote_without_startup_tick_charts() {
    use tqsdk_relay::{RelayConfig, UpstreamTickSource, connect_configured_upstream};
    use websocket_support::TestWebSocketServer;

    let server = TestWebSocketServer::spawn(|mut socket| {
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        expect_initial_universe_subscriptions(&mut socket, "DCE.m2609,SHFE.au2602");
        socket
            .send_text(
                json!({
                    "aid": "rtn_data",
                    "data": [
                        {
                            "quotes": {
                                "SHFE.au2602": {
                                    "datetime": "1781658240000000000",
                                    "instrument_id": "SHFE.au2602",
                                    "last_price": 944.5,
                                    "volume": 12,
                                    "open_interest": 34
                                }
                            }
                        }
                    ]
                })
                .to_string(),
            )
            .unwrap();
        expect_peek_message(&mut socket);
        socket.send_close().unwrap();
    })
    .unwrap();
    let config = RelayConfig {
        upstream_market_url: server.url("/market"),
        futures_universe_expression: Some(
            UniverseExpression::parse("symbol:SHFE.au2602,DCE.m2609").unwrap(),
        ),
        ..RelayConfig::default()
    };

    let mut source = connect_configured_upstream(&config).await.unwrap().unwrap();
    let update = source.next_update().await.unwrap();
    let tqsdk_relay::UpstreamSourceUpdate::Event(UpstreamMarketEvent::Quote(quote)) = update else {
        panic!("expected quote event from configured upstream");
    };
    assert_eq!(quote.symbol, "SHFE.au2602");
    assert_eq!(quote.quote.last_price, 944.5);
    server.join();
}

#[tokio::test]
async fn websocket_upstream_tick_source_merges_sparse_quote_patches() {
    use tqsdk_relay::{
        UpstreamMarketEvent, UpstreamSourceUpdate, UpstreamTickSource, WebSocketUpstreamTickSource,
    };
    use websocket_support::TestWebSocketServer;

    let server = TestWebSocketServer::spawn(|mut socket| {
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        expect_initial_universe_subscriptions(&mut socket, "SHFE.au2602");
        socket
            .send_text(
                json!({
                    "aid": "rtn_data",
                    "data": [
                        {
                            "quotes": {
                                "SHFE.au2602": {
                                    "datetime": "2026-06-17 13:56:00.000000",
                                    "instrument_id": "SHFE.au2602",
                                    "last_price": 944.5,
                                    "volume": 12,
                                    "open_interest": 34
                                }
                            }
                        }
                    ]
                })
                .to_string(),
            )
            .unwrap();
        expect_peek_message(&mut socket);
        socket
            .send_text(
                json!({
                    "aid": "rtn_data",
                    "data": [
                        {
                            "quotes": {
                                "SHFE.au2602": {
                                    "last_price": 945.0
                                }
                            }
                        }
                    ]
                })
                .to_string(),
            )
            .unwrap();
        expect_peek_message(&mut socket);
        socket.send_close().unwrap();
    })
    .unwrap();

    let mut source = WebSocketUpstreamTickSource::connect_with_quote_symbols(
        server.url("/market"),
        ["SHFE.au2602"],
    )
    .await
    .unwrap();

    let first = source.next_update().await.unwrap();
    let UpstreamSourceUpdate::Event(UpstreamMarketEvent::Quote(first)) = first else {
        panic!("expected first quote event");
    };
    assert_eq!(first.quote.last_price, 944.5);
    assert_eq!(first.quote.volume, 12);
    assert_eq!(first.quote.open_interest, 34);

    let second = source.next_update().await.unwrap();
    let UpstreamSourceUpdate::Event(UpstreamMarketEvent::Quote(second)) = second else {
        panic!("expected second quote event");
    };
    assert_eq!(second.quote.last_price, 945.0);
    assert_eq!(second.quote.volume, 12);
    assert_eq!(second.quote.open_interest, 34);
    assert_eq!(second.quote.datetime, "2026-06-17 13:56:00.000000");
    server.join();
}

#[tokio::test]
async fn configured_upstream_source_subscribes_universe_expression_symbols() {
    use tqsdk_relay::{RelayConfig, connect_configured_upstream};
    use websocket_support::TestWebSocketServer;

    let server = TestWebSocketServer::spawn(|mut socket| {
        expect_initial_universe_subscriptions(&mut socket, "DCE.m2609,SHFE.au2602");
        socket.send_close().unwrap();
    })
    .unwrap();
    let config = RelayConfig {
        upstream_market_url: server.url("/market"),
        futures_universe_expression: Some(
            UniverseExpression::parse("symbol:SHFE.au2602,DCE.m2609").unwrap(),
        ),
        ..RelayConfig::default()
    };

    let _source = connect_configured_upstream(&config).await.unwrap().unwrap();
    server.join();
}

#[tokio::test]
async fn configured_upstream_source_subscribes_universe_v2_snapshot_symbols() {
    use tqsdk_relay::{
        RelayConfig, RelayRuntimeConfig, connect_configured_upstream_with_runtime_config,
    };
    use websocket_support::TestWebSocketServer;

    let server = TestWebSocketServer::spawn(|mut socket| {
        expect_initial_universe_subscriptions(&mut socket, "DCE.m2609,SHFE.au2602");
        socket.send_close().unwrap();
    })
    .unwrap();
    let config = RelayRuntimeConfig::new(RelayConfig {
        upstream_market_url: server.url("/market"),
        ..RelayConfig::default()
    })
    .with_futures_universe("snapshot(symbol:SHFE.au2602,DCE.m2609)")
    .unwrap();

    let _source = connect_configured_upstream_with_runtime_config(&config)
        .await
        .unwrap()
        .unwrap();
    server.join();
}

#[tokio::test]
async fn configured_upstream_rejects_typed_timeline_before_network() {
    use tqsdk_relay::{RelayConfig, RelayRuntimeConfig, UniverseSpec};

    let error = RelayRuntimeConfig::new(RelayConfig {
        upstream_market_url: "ws://127.0.0.1:1/should-not-connect".to_string(),
        ..RelayConfig::default()
    })
    .with_futures_universe_spec(UniverseSpec::parse_v2("timeline(contract:all)").unwrap())
    .unwrap_err();
    assert!(error.to_string().contains("snapshot-only entry point"));
}

#[tokio::test]
async fn configured_upstream_source_is_absent_without_universe_expression() {
    use tqsdk_relay::{RelayConfig, connect_configured_upstream};

    assert!(
        connect_configured_upstream(&RelayConfig::default())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn configured_product_discovery_requires_auth_credentials() {
    use tqsdk_relay::{RelayConfig, connect_configured_upstream};

    let config = RelayConfig {
        futures_universe_expression: Some(UniverseExpression::parse("active:all").unwrap()),
        ..RelayConfig::default()
    };

    let err = match connect_configured_upstream(&config).await {
        Ok(_) => panic!("product discovery without auth should fail"),
        Err(err) => err,
    };

    assert_eq!(
        err.to_string(),
        "invalid relay config: TQ_AUTH_USER is required for futures product discovery"
    );
}

#[tokio::test]
async fn configured_upstream_pump_ingests_upstream_quotes() {
    use tqsdk_relay::{RelayConfig, RelayServer, spawn_configured_upstream_pump};
    use websocket_support::TestWebSocketServer;

    let upstream = TestWebSocketServer::spawn(|mut socket| {
        expect_initial_universe_subscriptions(&mut socket, "SHFE.au2602");
        socket
            .send_text(
                json!({
                    "aid": "rtn_data",
                    "data": [
                        {
                            "quotes": {
                                "SHFE.au2602": {
                                    "datetime": "1781658240000000000",
                                    "instrument_id": "SHFE.au2602",
                                    "last_price": 610.0,
                                    "volume": 170,
                                    "open_interest": 1007
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
        futures_universe_expression: Some(UniverseExpression::parse("symbol:SHFE.au2602").unwrap()),
        ..RelayConfig::default()
    };
    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
    let server = RelayServer::new(engine.clone());

    let _shutdown = spawn_configured_upstream_pump(&config, server)
        .await
        .unwrap()
        .unwrap();

    wait_for_upstream_events_decoded(&engine, 1).await;
    let metrics = engine.lock().unwrap().metrics_snapshot();
    assert_eq!(metrics.ticks_ingested, 0);
    assert_eq!(metrics.upstream_events_decoded, 1);
    assert_eq!(metrics.upstream_symbols, 1);
    assert_eq!(metrics.upstream_ins_list_chars, "SHFE.au2602".len());
    assert_eq!(
        engine.lock().unwrap().health_snapshot().upstream_status,
        tqsdk_relay::RelaySourceStatus::Up
    );
    upstream.join();
}

#[tokio::test]
async fn configured_upstream_pump_degrades_without_blocking_when_connect_fails() {
    use tqsdk_relay::{
        RelayConfig, RelayServer, RelaySourceStatus,
        spawn_configured_upstream_pump_with_retry_interval,
    };

    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
    let server = RelayServer::new(engine.clone());
    let config = RelayConfig {
        upstream_market_url: "ws://127.0.0.1:9/market".to_string(),
        futures_universe_expression: Some(UniverseExpression::parse("symbol:SHFE.au2602").unwrap()),
        ..RelayConfig::default()
    };

    let shutdown = spawn_configured_upstream_pump_with_retry_interval(
        &config,
        server,
        Duration::from_millis(20),
    )
    .await
    .unwrap();

    assert!(shutdown.is_some());
    wait_for_upstream_status(&engine, RelaySourceStatus::Degraded).await;
}

#[tokio::test]
async fn configured_upstream_pump_retries_after_startup_connect_failure() {
    use tqsdk_relay::{
        RelayConfig, RelayServer, RelaySourceStatus,
        spawn_configured_upstream_pump_with_retry_interval,
    };
    use websocket_support::TestWebSocketServer;

    let addr = free_loopback_addr();
    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
    let server = RelayServer::new(engine.clone());
    let config = RelayConfig {
        upstream_market_url: format!("ws://{addr}/market"),
        futures_universe_expression: Some(UniverseExpression::parse("symbol:SHFE.au2602").unwrap()),
        ..RelayConfig::default()
    };

    let shutdown = spawn_configured_upstream_pump_with_retry_interval(
        &config,
        server,
        Duration::from_millis(20),
    )
    .await
    .unwrap()
    .unwrap();
    wait_for_upstream_status(&engine, RelaySourceStatus::Degraded).await;

    let upstream = TestWebSocketServer::spawn_on(addr, |mut socket| {
        expect_initial_universe_subscriptions(&mut socket, "SHFE.au2602");
        socket
            .send_text(
                json!({
                    "aid": "rtn_data",
                    "data": [
                        {
                            "quotes": {
                                "SHFE.au2602": {
                                    "datetime": "1781658241000000000",
                                    "instrument_id": "SHFE.au2602",
                                    "last_price": 611.0,
                                    "volume": 180,
                                    "open_interest": 1008
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

    wait_for_upstream_events_decoded(&engine, 1).await;
    wait_for_upstream_status(&engine, RelaySourceStatus::Up).await;
    let _ = shutdown.send(());
    upstream.join();
}

#[tokio::test]
async fn configured_upstream_refresh_failure_keeps_existing_source_live() {
    use tqsdk_relay::{
        RelayRuntimeConfig, RelayServer, RelaySourceStatus,
        spawn_configured_upstream_pump_with_runtime_config_and_retry_interval,
    };
    use websocket_support::TestWebSocketServer;

    let (send_second_quote_tx, send_second_quote_rx) = std::sync::mpsc::channel();
    let upstream = TestWebSocketServer::spawn(move |mut socket| {
        expect_initial_universe_subscriptions(&mut socket, "SHFE.au2602");
        socket
            .send_text(
                json!({
                    "aid": "rtn_data",
                    "data": [
                        {
                            "quotes": {
                                "SHFE.au2602": {
                                    "datetime": "1781658240000000000",
                                    "instrument_id": "SHFE.au2602",
                                    "last_price": 610.0,
                                    "volume": 170,
                                    "open_interest": 1007
                                }
                            }
                        }
                    ]
                })
                .to_string(),
            )
            .unwrap();
        socket
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        loop {
            match send_second_quote_rx.try_recv() {
                Ok(()) => break,
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                Err(std::sync::mpsc::TryRecvError::Disconnected) => return,
            }
            match socket.recv() {
                Ok(websocket_support::ClientFrame::Text(_)) => {}
                Ok(websocket_support::ClientFrame::Close) => return,
                Ok(_) => {}
                Err(err) if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
                Err(err) => panic!("unexpected upstream client frame error: {err}"),
            }
        }
        socket.set_read_timeout(None).unwrap();
        socket
            .send_text(
                json!({
                    "aid": "rtn_data",
                    "data": [
                        {
                            "quotes": {
                                "SHFE.au2602": {
                                    "datetime": "1781658241000000000",
                                    "instrument_id": "SHFE.au2602",
                                    "last_price": 611.0,
                                    "volume": 180,
                                    "open_interest": 1008
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
    let universe_file = std::env::temp_dir().join(format!(
        "tqsdk-relay-refresh-universe-{}-{}.txt",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(&universe_file, "SHFE.au2602\n").unwrap();
    let config = RelayRuntimeConfig::new(RelayConfig {
        upstream_market_url: upstream.url("/market"),
        futures_universe_refresh: FuturesUniverseRefreshSchedule::daily(refresh_time_after(
            Duration::from_secs(4),
        )),
        ..RelayConfig::default()
    })
    .universe_symbol_file(universe_file.clone());
    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
    let server = RelayServer::new(engine.clone());

    let shutdown = spawn_configured_upstream_pump_with_runtime_config_and_retry_interval(
        &config,
        server,
        Duration::from_millis(20),
    )
    .await
    .unwrap()
    .unwrap();
    wait_for_upstream_events_decoded(&engine, 1).await;
    wait_for_upstream_status(&engine, RelaySourceStatus::Up).await;

    std::fs::remove_file(&universe_file).unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(
        engine.lock().unwrap().health_snapshot().upstream_status,
        RelaySourceStatus::Up
    );

    send_second_quote_tx.send(()).unwrap();
    wait_for_upstream_events_decoded(&engine, 2).await;
    let _ = shutdown.send(());
    upstream.join();
}

async fn wait_for_upstream_events_decoded(engine: &Arc<Mutex<RelayEngine>>, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let events_decoded = engine
            .lock()
            .unwrap()
            .metrics_snapshot()
            .upstream_events_decoded;
        if events_decoded >= expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {expected} decoded upstream events; saw {events_decoded}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_upstream_status(
    engine: &Arc<Mutex<RelayEngine>>,
    expected: tqsdk_relay::RelaySourceStatus,
) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let status = engine.lock().unwrap().health_snapshot().upstream_status;
        if status == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for upstream status {expected:?}; saw {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn refresh_time_after(delay: Duration) -> DailyRefreshTime {
    let target = Local::now() + chrono::Duration::from_std(delay).unwrap();
    DailyRefreshTime::from_hms(target.hour(), target.minute(), target.second()).unwrap()
}

fn free_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}
