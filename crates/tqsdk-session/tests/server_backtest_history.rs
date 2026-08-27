use std::time::Duration;

use serde_json::{Value, json};
use tokio::time::Instant;
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, MarketSessionTarget, OutboundFrame,
    OutboundRequest, ProtocolDomain, RuntimeHandle, RuntimeInput,
};
use tqsdk_session::{
    SERVER_BACKTEST_CANONICAL_DAILY_NS, ServerBacktestHistoryChart, ServerBacktestHistoryEvent,
    ServerBacktestHistoryKind, ServerBacktestHistoryRequest, ServerBacktestHistoryStream,
    ServerBacktestMarketKind, testing::ManualSession,
};

const MINUTE_NS: i64 = 60_000_000_000;
const PAGE_WIDTH: usize = 10_000;

#[derive(Clone, Copy)]
struct PageStatus {
    ready: bool,
    more_data: bool,
    mdhis_more_data: bool,
}

const PAGE_COMPLETE: PageStatus = PageStatus {
    ready: true,
    more_data: false,
    mdhis_more_data: false,
};

fn manual_session() -> ManualSession {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    ManualSession::from_runtime(RuntimeHandle::with_adapters(adapters))
}

fn manual_session_with_target(market_target: MarketSessionTarget) -> ManualSession {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    ManualSession::from_runtime_with_market_target(
        RuntimeHandle::with_adapters(adapters),
        market_target,
    )
}

fn request(charts: Vec<ServerBacktestHistoryChart>) -> ServerBacktestHistoryRequest {
    ServerBacktestHistoryRequest {
        market_kind: ServerBacktestMarketKind::Futures,
        start_ns: 1_000,
        end_ns: 2_000,
        charts,
    }
}

fn chart(
    chart_id: &str,
    symbol: &str,
    kind: ServerBacktestHistoryKind,
) -> ServerBacktestHistoryChart {
    ServerBacktestHistoryChart {
        chart_id: chart_id.to_string(),
        symbol: symbol.to_string(),
        kind,
    }
}

fn transport_bodies(session: &ManualSession) -> Vec<Value> {
    session
        .drain_dispatches()
        .unwrap()
        .into_iter()
        .filter_map(|dispatch| match dispatch.request {
            OutboundRequest::Transport(OutboundFrame::Text(text)) => {
                Some(serde_json::from_str(&text).unwrap())
            }
            _ => None,
        })
        .collect()
}

fn ingest(session: &ManualSession, value: Value) {
    session
        .client()
        .handle()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(value),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap();
}

fn tick_page(
    chart_id: &str,
    symbol: &str,
    left_id: i64,
    right_id: i64,
    rows: Value,
    status: PageStatus,
) -> Value {
    json!({
        "aid": "rtn_data",
        "data": [{
            "mdhis_more_data": status.mdhis_more_data,
            "charts": {
                chart_id: {
                    "state": {
                        "aid": "set_chart",
                        "chart_id": chart_id,
                        "ins_list": symbol,
                        "duration": 0,
                        "view_width": PAGE_WIDTH,
                        "focus_datetime": 1_000,
                        "focus_position": 0,
                    },
                    "left_id": left_id,
                    "right_id": right_id,
                    "ready": status.ready,
                    "more_data": status.more_data,
                }
            },
            "ticks": {
                symbol: {
                    "last_id": right_id,
                    "data": rows,
                }
            }
        }]
    })
}

fn follow_up_tick_page(mut page: Value, chart_id: &str, left_kline_id: i64) -> Value {
    page["data"][0]["charts"][chart_id]["state"]["left_kline_id"] = json!(left_kline_id);
    page
}

#[tokio::test(flavor = "current_thread")]
async fn tick_first_page_uses_start_focus_and_zero_position() {
    let session = manual_session();
    let _stream = ServerBacktestHistoryStream::open(
        session.client_clone(),
        request(vec![chart(
            "ticks-au",
            "SHFE.au2608",
            ServerBacktestHistoryKind::Tick,
        )]),
    )
    .await
    .unwrap();

    let bodies = transport_bodies(&session);
    let body = bodies
        .iter()
        .find(|body| body.get("aid") == Some(&json!("set_chart")))
        .expect("history stream should submit its first tick page");
    assert_eq!(body.get("chart_id"), Some(&json!("ticks-au")));
    assert_eq!(body.get("duration"), Some(&json!(0)));
    assert_eq!(body.get("focus_datetime"), Some(&json!(1_000)));
    assert_eq!(body.get("focus_position"), Some(&json!(0)));
    assert_eq!(body.get("view_width"), Some(&json!(PAGE_WIDTH)));
}

#[tokio::test(flavor = "current_thread")]
async fn tick_follow_up_page_uses_left_id_and_never_reemits_the_overlap() {
    let session = manual_session();
    let mut stream = ServerBacktestHistoryStream::open(
        session.client_clone(),
        request(vec![chart(
            "ticks-au",
            "SHFE.au2608",
            ServerBacktestHistoryKind::Tick,
        )]),
    )
    .await
    .unwrap();
    let _ = transport_bodies(&session);

    ingest(
        &session,
        tick_page(
            "ticks-au",
            "SHFE.au2608",
            1,
            2,
            json!({
                "1": { "id": 1, "datetime": 1_000, "last_price": 1.0 },
                "2": { "id": 2, "datetime": 1_100, "last_price": 2.0 },
            }),
            PAGE_COMPLETE,
        ),
    );
    let event = stream
        .next_event(Some(Instant::now() + Duration::from_millis(20)))
        .await;
    let ServerBacktestHistoryEvent::Ticks { rows, .. } = event.unwrap().unwrap() else {
        panic!("first tick page should emit tick rows");
    };
    assert_eq!(
        rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![1, 2]
    );

    let bodies = transport_bodies(&session);
    let next_page = bodies
        .iter()
        .find(|body| body.get("left_kline_id") == Some(&json!(2)))
        .expect("follow-up tick page should use the prior right id");
    let next_chart_id = next_page
        .get("chart_id")
        .and_then(Value::as_str)
        .expect("follow-up chart should have an id");
    assert_ne!(next_chart_id, "ticks-au");

    ingest(
        &session,
        follow_up_tick_page(
            tick_page(
                next_chart_id,
                "SHFE.au2608",
                2,
                4,
                json!({
                    "2": { "id": 2, "datetime": 1_100, "last_price": 2.0 },
                    "3": { "id": 3, "datetime": 1_900, "last_price": 3.0 },
                    "4": { "id": 4, "datetime": 2_000, "last_price": 4.0 },
                }),
                PAGE_COMPLETE,
            ),
            next_chart_id,
            2,
        ),
    );
    let event = stream
        .next_event(Some(Instant::now() + Duration::from_millis(20)))
        .await;
    let ServerBacktestHistoryEvent::Ticks { rows, .. } = event.unwrap().unwrap() else {
        panic!("follow-up page should emit only new rows");
    };
    assert_eq!(rows.iter().map(|row| row.id).collect::<Vec<_>>(), vec![3]);
    assert!(matches!(
        stream
            .next_event(Some(Instant::now() + Duration::from_millis(20)))
            .await
            .unwrap(),
        Some(ServerBacktestHistoryEvent::ChartCompleted { chart_id, symbol })
            if chart_id == "ticks-au" && symbol == "SHFE.au2608"
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn canonical_minute_uses_the_fixed_60_second_duration() {
    let session = manual_session();
    let _stream = ServerBacktestHistoryStream::open(
        session.client_clone(),
        request(vec![chart(
            "minutes-au",
            "SHFE.au2608",
            ServerBacktestHistoryKind::CanonicalMinute,
        )]),
    )
    .await
    .unwrap();

    let bodies = transport_bodies(&session);
    let body = bodies
        .iter()
        .find(|body| body.get("chart_id") == Some(&json!("minutes-au")))
        .expect("minute stream should submit a chart");
    assert_eq!(body.get("duration"), Some(&json!(MINUTE_NS)));
}

#[tokio::test(flavor = "current_thread")]
async fn canonical_minute_reads_only_the_60_second_kline_path() {
    let session = manual_session();
    let mut stream = ServerBacktestHistoryStream::open(
        session.client_clone(),
        request(vec![chart(
            "minutes-au",
            "SHFE.au2608",
            ServerBacktestHistoryKind::CanonicalMinute,
        )]),
    )
    .await
    .unwrap();
    let _ = transport_bodies(&session);

    ingest(
        &session,
        json!({
            "aid": "rtn_data",
            "data": [{
                "mdhis_more_data": false,
                "charts": {
                    "minutes-au": {
                        "state": {
                            "aid": "set_chart",
                            "chart_id": "minutes-au",
                            "ins_list": "SHFE.au2608",
                            "duration": MINUTE_NS,
                            "view_width": PAGE_WIDTH,
                            "focus_datetime": 1_000,
                            "focus_position": 0,
                        },
                        "left_id": 1,
                        "right_id": 1,
                        "ready": true,
                        "more_data": false,
                    }
                },
                "klines": {
                    "SHFE.au2608": {
                        (MINUTE_NS.to_string()): {
                            "last_id": 1,
                            "data": {
                                "1": {
                                    "id": 1,
                                    "datetime": 1_000,
                                    "open": 1.0,
                                    "high": 2.0,
                                    "low": 1.0,
                                    "close": 2.0,
                                }
                            }
                        }
                    }
                }
            }]
        }),
    );
    let event = stream
        .next_event(Some(Instant::now() + Duration::from_millis(20)))
        .await
        .unwrap()
        .expect("canonical minute chart should emit its row");
    let ServerBacktestHistoryEvent::CanonicalMinutes { rows, .. } = event else {
        panic!("expected canonical-minute rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].datetime, 1_000);
}

#[tokio::test(flavor = "current_thread")]
async fn canonical_daily_uses_native_one_day_chart_path_and_event() {
    let session = manual_session();
    let mut stream = ServerBacktestHistoryStream::open(
        session.client_clone(),
        request(vec![chart(
            "daily-au",
            "SHFE.au2608",
            ServerBacktestHistoryKind::CanonicalDaily,
        )]),
    )
    .await
    .unwrap();

    let bodies = transport_bodies(&session);
    let body = bodies
        .iter()
        .find(|body| body.get("chart_id") == Some(&json!("daily-au")))
        .expect("daily stream should submit a chart");
    assert_eq!(
        body.get("duration"),
        Some(&json!(SERVER_BACKTEST_CANONICAL_DAILY_NS))
    );

    ingest(
        &session,
        json!({
            "aid": "rtn_data",
            "data": [{
                "mdhis_more_data": false,
                "charts": {
                    "daily-au": {
                        "state": {
                            "aid": "set_chart",
                            "chart_id": "daily-au",
                            "ins_list": "SHFE.au2608",
                            "duration": SERVER_BACKTEST_CANONICAL_DAILY_NS,
                            "view_width": PAGE_WIDTH,
                            "focus_datetime": 1_000,
                            "focus_position": 0,
                        },
                        "left_id": 1,
                        "right_id": 1,
                        "ready": true,
                        "more_data": false,
                    }
                },
                "klines": {
                    "SHFE.au2608": {
                        (SERVER_BACKTEST_CANONICAL_DAILY_NS.to_string()): {
                            "last_id": 1,
                            "data": {
                                "1": {
                                    "id": 1,
                                    "datetime": 1_000,
                                    "open": 1.0,
                                    "high": 2.0,
                                    "low": 1.0,
                                    "close": 2.0,
                                }
                            }
                        }
                    }
                }
            }]
        }),
    );
    let event = stream
        .next_event(Some(Instant::now() + Duration::from_millis(20)))
        .await
        .unwrap()
        .expect("canonical daily chart should emit its row");
    let ServerBacktestHistoryEvent::CanonicalDaily { rows, .. } = event else {
        panic!("expected canonical-daily rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].datetime, 1_000);
}

#[tokio::test(flavor = "current_thread")]
async fn canonical_minute_terminal_page_allows_last_id_before_chart_right_id() {
    let session = manual_session();
    let mut stream = ServerBacktestHistoryStream::open(
        session.client_clone(),
        request(vec![chart(
            "minutes-au",
            "SHFE.au2608",
            ServerBacktestHistoryKind::CanonicalMinute,
        )]),
    )
    .await
    .unwrap();
    let _ = transport_bodies(&session);

    ingest(
        &session,
        json!({
            "aid": "rtn_data",
            "data": [{
                "mdhis_more_data": false,
                "charts": {
                    "minutes-au": {
                        "state": {
                            "aid": "set_chart",
                            "chart_id": "minutes-au",
                            "ins_list": "SHFE.au2608",
                            "duration": MINUTE_NS,
                            "view_width": PAGE_WIDTH,
                            "focus_datetime": 1_000,
                            "focus_position": 0,
                        },
                        "left_id": 1,
                        "right_id": 2,
                        "ready": true,
                        "more_data": false,
                    }
                },
                "klines": {
                    "SHFE.au2608": {
                        (MINUTE_NS.to_string()): {
                            "last_id": 1,
                            "data": {
                                "1": {
                                    "id": 1,
                                    "datetime": 1_000,
                                    "open": 1.0,
                                    "high": 2.0,
                                    "low": 1.0,
                                    "close": 2.0,
                                }
                            }
                        }
                    }
                }
            }]
        }),
    );

    let event = stream
        .next_event(Some(Instant::now() + Duration::from_millis(20)))
        .await
        .unwrap()
        .expect("terminal page should emit its available canonical minute");
    let ServerBacktestHistoryEvent::CanonicalMinutes { rows, .. } = event else {
        panic!("expected canonical-minute rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 1);

    assert!(matches!(
        stream
            .next_event(Some(Instant::now() + Duration::from_millis(20)))
            .await
            .unwrap(),
        Some(ServerBacktestHistoryEvent::ChartCompleted { .. })
    ));
    assert!(matches!(
        stream
            .next_event(Some(Instant::now() + Duration::from_millis(20)))
            .await
            .unwrap(),
        Some(ServerBacktestHistoryEvent::StreamCompleted)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn history_stream_rejects_a_non_backtest_session_target() {
    let session = manual_session_with_target(MarketSessionTarget::futures_live());
    let result = ServerBacktestHistoryStream::open(
        session.client_clone(),
        request(vec![chart(
            "ticks-au",
            "SHFE.au2608",
            ServerBacktestHistoryKind::Tick,
        )]),
    )
    .await;

    assert!(result.is_err());
    assert!(transport_bodies(&session).is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_the_stream_asynchronously_releases_its_chart_lease() {
    let session = manual_session();
    let stream = ServerBacktestHistoryStream::open(
        session.client_clone(),
        request(vec![chart(
            "ticks-au",
            "SHFE.au2608",
            ServerBacktestHistoryKind::Tick,
        )]),
    )
    .await
    .unwrap();
    let _ = transport_bodies(&session);

    drop(stream);
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }

    assert!(transport_bodies(&session).iter().any(|body| {
        body.get("aid") == Some(&json!("set_chart"))
            && body.get("chart_id") == Some(&json!("ticks-au"))
            && body.get("ins_list") == Some(&json!(""))
    }));

    let reused = ServerBacktestHistoryStream::open(
        session.client_clone(),
        request(vec![chart(
            "ticks-au-reused",
            "SHFE.au2608",
            ServerBacktestHistoryKind::Tick,
        )]),
    )
    .await
    .unwrap();
    assert!(transport_bodies(&session).iter().any(|body| {
        body.get("aid") == Some(&json!("set_chart"))
            && body.get("chart_id") == Some(&json!("ticks-au-reused"))
            && body.get("ins_list") == Some(&json!("SHFE.au2608"))
    }));
    reused.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn closing_the_stream_waits_until_its_chart_lease_is_released() {
    let session = manual_session();
    let stream = ServerBacktestHistoryStream::open(
        session.client_clone(),
        request(vec![chart(
            "ticks-au",
            "SHFE.au2608",
            ServerBacktestHistoryKind::Tick,
        )]),
    )
    .await
    .unwrap();
    let _ = transport_bodies(&session);

    stream.close().await.unwrap();

    assert!(transport_bodies(&session).iter().any(|body| {
        body.get("aid") == Some(&json!("set_chart"))
            && body.get("chart_id") == Some(&json!("ticks-au"))
            && body.get("ins_list") == Some(&json!(""))
    }));
}

#[tokio::test(flavor = "current_thread")]
async fn ready_chart_waits_for_both_server_more_data_flags_to_clear() {
    let session = manual_session();
    let mut stream = ServerBacktestHistoryStream::open(
        session.client_clone(),
        request(vec![chart(
            "ticks-au",
            "SHFE.au2608",
            ServerBacktestHistoryKind::Tick,
        )]),
    )
    .await
    .unwrap();
    let _ = transport_bodies(&session);

    ingest(
        &session,
        tick_page(
            "ticks-au",
            "SHFE.au2608",
            1,
            1,
            json!({ "1": { "id": 1, "datetime": 1_000, "last_price": 1.0 } }),
            PageStatus {
                ready: true,
                more_data: true,
                mdhis_more_data: false,
            },
        ),
    );
    assert!(
        stream
            .next_event(Some(Instant::now() + Duration::from_millis(5)))
            .await
            .unwrap()
            .is_none()
    );

    ingest(
        &session,
        tick_page(
            "ticks-au",
            "SHFE.au2608",
            1,
            1,
            json!({ "1": { "id": 1, "datetime": 1_000, "last_price": 1.0 } }),
            PAGE_COMPLETE,
        ),
    );
    assert!(matches!(
        stream
            .next_event(Some(Instant::now() + Duration::from_millis(20)))
            .await
            .unwrap(),
        Some(ServerBacktestHistoryEvent::Ticks { .. })
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_empty_interval_completes_after_the_server_terminal_signal() {
    let session = manual_session();
    let mut stream = ServerBacktestHistoryStream::open(
        session.client_clone(),
        request(vec![chart(
            "ticks-empty",
            "SHFE.au2608",
            ServerBacktestHistoryKind::Tick,
        )]),
    )
    .await
    .unwrap();
    let _ = transport_bodies(&session);

    ingest(
        &session,
        tick_page(
            "ticks-empty",
            "SHFE.au2608",
            -1,
            -1,
            json!({}),
            PAGE_COMPLETE,
        ),
    );
    assert!(matches!(
        stream
            .next_event(Some(Instant::now() + Duration::from_millis(20)))
            .await
            .unwrap(),
        Some(ServerBacktestHistoryEvent::ChartCompleted { chart_id, symbol })
            if chart_id == "ticks-empty" && symbol == "SHFE.au2608"
    ));
    assert!(matches!(
        stream
            .next_event(Some(Instant::now() + Duration::from_millis(20)))
            .await
            .unwrap(),
        Some(ServerBacktestHistoryEvent::StreamCompleted)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn one_ready_chart_can_emit_while_another_chart_is_still_pending() {
    let session = manual_session();
    let mut stream = ServerBacktestHistoryStream::open(
        session.client_clone(),
        request(vec![
            chart(
                "ticks-pending",
                "SHFE.au2608",
                ServerBacktestHistoryKind::Tick,
            ),
            chart("ticks-ready", "DCE.m2609", ServerBacktestHistoryKind::Tick),
        ]),
    )
    .await
    .unwrap();
    let _ = transport_bodies(&session);

    ingest(
        &session,
        tick_page(
            "ticks-ready",
            "DCE.m2609",
            1,
            1,
            json!({ "1": { "id": 1, "datetime": 1_000, "last_price": 1.0 } }),
            PAGE_COMPLETE,
        ),
    );
    let event = stream
        .next_event(Some(Instant::now() + Duration::from_millis(20)))
        .await
        .unwrap()
        .expect("ready chart should not wait for another chart");
    let ServerBacktestHistoryEvent::Ticks {
        chart_id, symbol, ..
    } = event
    else {
        panic!("ready chart should emit its rows");
    };
    assert_eq!(chart_id, "ticks-ready");
    assert_eq!(symbol, "DCE.m2609");
}
