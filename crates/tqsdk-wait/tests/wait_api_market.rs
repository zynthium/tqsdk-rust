use std::time::Duration;

use serde_json::json;
use tqsdk_core::{
    CommitScope, InputPayload, IoEvent, OutboundFrame, OutboundRequest, ProtocolDomain,
    RuntimeInput,
};

mod support;

fn compact_source(source: &str) -> String {
    source.split_whitespace().collect::<String>()
}

fn transport_payload(request: &OutboundRequest) -> serde_json::Value {
    match request {
        OutboundRequest::Transport(OutboundFrame::Text(text)) => {
            serde_json::from_str(text).expect("transport frame should contain valid json payload")
        }
        OutboundRequest::Transport(OutboundFrame::Binary(bytes)) => serde_json::from_slice(bytes)
            .expect("transport frame should contain valid json payload"),
        other => panic!("expected transport request, got {other:?}"),
    }
}

fn hidden_backtest_chart_id(api: &tqsdk_wait::TqApi, symbol: &str) -> String {
    let dispatches = api.session().handle().drain_dispatches().unwrap();
    dispatches
        .iter()
        .map(|dispatch| transport_payload(&dispatch.request))
        .find(|payload| {
            payload["aid"] == "set_chart"
                && payload["ins_list"] == symbol
                && payload["duration"] == 0
                && payload["view_width"] == 10_000
                && payload["focus_position"] == 10_000
        })
        .and_then(|payload| payload["chart_id"].as_str().map(ToOwned::to_owned))
        .expect("backtest tick subscription should request a hidden history chart")
}

fn seed_backtest_tick_page(api: &mut tqsdk_wait::TqApi, chart_id: &str, symbol: &str) {
    seed_backtest_tick_page_with_bounds(api, chart_id, symbol, 11, false);
}

fn seed_backtest_tick_page_with_bounds(
    api: &mut tqsdk_wait::TqApi,
    chart_id: &str,
    symbol: &str,
    tick_last_id: i64,
    chart_more_data: bool,
) {
    api.session()
        .handle()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "mdhis_more_data": false,
                        "charts": {
                            chart_id: {
                                "state": {
                                    "aid": "set_chart",
                                    "chart_id": chart_id,
                                    "ins_list": symbol,
                                    "duration": 0,
                                    "view_width": 10_000,
                                    "focus_datetime": 1_000,
                                    "focus_position": 10_000,
                                },
                                "left_id": 10,
                                "right_id": 11,
                                "more_data": chart_more_data,
                                "ready": true,
                            }
                        },
                        "ticks": {
                            symbol: {
                                "last_id": tick_last_id,
                                "data": {
                                    "10": {
                                        "datetime": 1_500,
                                        "last_price": 618.0,
                                        "average": 618.0,
                                        "highest": 618.0,
                                        "lowest": 618.0,
                                        "ask_price1": 618.2,
                                        "ask_volume1": 4,
                                        "bid_price1": 617.8,
                                        "bid_volume1": 5,
                                        "volume": 12,
                                        "amount": 7_416.0,
                                        "open_interest": 101
                                    },
                                    "11": {
                                        "datetime": 2_500,
                                        "last_price": 619.0,
                                        "average": 618.4,
                                        "highest": 619.0,
                                        "lowest": 618.0,
                                        "ask_price1": 619.2,
                                        "ask_volume1": 3,
                                        "bid_price1": 618.8,
                                        "bid_volume1": 6,
                                        "volume": 15,
                                        "amount": 9_285.0,
                                        "open_interest": 102
                                    }
                                }
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("backtest tick page should produce a commit");
}

fn seed_backtest_tick_page_with_rows(
    api: &mut tqsdk_wait::TqApi,
    chart_id: &str,
    symbol: &str,
    left_id: i64,
    right_id: i64,
    tick_last_id: i64,
    rows: Vec<(i64, i64, f64)>,
) {
    let mut data = serde_json::Map::new();
    for (id, datetime, last_price) in rows {
        data.insert(
            id.to_string(),
            json!({
                "datetime": datetime,
                "last_price": last_price,
                "average": last_price,
                "highest": last_price,
                "lowest": last_price,
                "ask_price1": last_price + 0.2,
                "ask_volume1": 3,
                "bid_price1": last_price - 0.2,
                "bid_volume1": 4,
                "volume": 12,
                "amount": 7_416.0,
                "open_interest": 101
            }),
        );
    }

    api.session()
        .handle()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "mdhis_more_data": false,
                        "charts": {
                            chart_id: {
                                "state": {
                                    "aid": "set_chart",
                                    "chart_id": chart_id,
                                    "ins_list": symbol,
                                    "duration": 0,
                                    "view_width": 10_000,
                                    "focus_datetime": 1_000,
                                    "focus_position": 10_000,
                                },
                                "left_id": left_id,
                                "right_id": right_id,
                                "more_data": false,
                                "ready": true,
                            }
                        },
                        "ticks": {
                            symbol: {
                                "last_id": tick_last_id,
                                "data": data
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("backtest tick page should produce a commit");
}

fn seed_backtest_tick_page_header_only(api: &mut tqsdk_wait::TqApi, chart_id: &str, symbol: &str) {
    api.session()
        .handle()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "mdhis_more_data": true,
                        "charts": {
                            chart_id: {
                                "state": {
                                    "aid": "set_chart",
                                    "chart_id": chart_id,
                                    "ins_list": symbol,
                                    "duration": 0,
                                    "view_width": 10_000,
                                    "focus_datetime": 1_000,
                                    "focus_position": 10_000,
                                },
                                "left_id": 10,
                                "right_id": 11,
                                "more_data": false,
                                "ready": true,
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("backtest tick page header should produce a commit");
}

fn seed_backtest_tick_page_rows_only(api: &mut tqsdk_wait::TqApi, symbol: &str) {
    api.session()
        .handle()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "mdhis_more_data": false,
                        "ticks": {
                            symbol: {
                                "last_id": 11,
                                "data": {
                                    "10": {
                                        "datetime": 1_500,
                                        "last_price": 618.0,
                                        "average": 618.0,
                                        "highest": 618.0,
                                        "lowest": 618.0,
                                        "ask_price1": 618.2,
                                        "ask_volume1": 4,
                                        "bid_price1": 617.8,
                                        "bid_volume1": 5,
                                        "volume": 12,
                                        "amount": 7_416.0,
                                        "open_interest": 101
                                    },
                                    "11": {
                                        "datetime": 2_500,
                                        "last_price": 619.0,
                                        "average": 618.4,
                                        "highest": 619.0,
                                        "lowest": 618.0,
                                        "ask_price1": 619.2,
                                        "ask_volume1": 3,
                                        "bid_price1": 618.8,
                                        "bid_volume1": 6,
                                        "volume": 15,
                                        "amount": 9_285.0,
                                        "open_interest": 102
                                    }
                                }
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("backtest tick page rows should produce a commit");
}

fn drain_backtest_set_chart_payloads(api: &tqsdk_wait::TqApi) -> Vec<serde_json::Value> {
    api.session()
        .handle()
        .drain_dispatches()
        .unwrap()
        .iter()
        .map(|dispatch| transport_payload(&dispatch.request))
        .filter(|payload| payload["aid"] == "set_chart")
        .collect()
}

#[test]
fn market_refs_read_market_partitions_instead_of_full_snapshot() {
    let quote_ref = include_str!("../src/refs/quote.rs");
    let trading_status_ref = include_str!("../src/refs/trading_status.rs");
    let kline_ref = include_str!("../src/refs/kline.rs");
    let tick_ref = include_str!("../src/refs/tick.rs");

    assert!(quote_ref.contains("read_market_state()"));
    assert!(trading_status_ref.contains("read_market_state()"));
    assert!(kline_ref.contains("read_market_state()"));
    assert!(tick_ref.contains("read_market_state()"));
    assert!(!compact_source(quote_ref).contains("reader.read()"));
    assert!(!compact_source(trading_status_ref).contains("reader.read()"));
    assert!(!compact_source(kline_ref).contains("reader.read()"));
    assert!(!compact_source(tick_ref).contains("reader.read()"));
}

#[test]
fn changed_rows_uses_row_decoding_instead_of_materializing_window_first() {
    let kline_ref = include_str!("../src/refs/kline.rs");
    let tick_ref = include_str!("../src/refs/tick.rs");

    assert!(kline_ref.contains("pub fn row(&self, id: i64)"));
    assert!(tick_ref.contains("pub fn row(&self, id: i64)"));
    assert!(!compact_source(kline_ref).contains("letwindow=self.window()?;letchanged_ids"));
    assert!(!compact_source(tick_ref).contains("letwindow=self.window()?;letchanged_ids"));
}

#[tokio::test(flavor = "current_thread")]
async fn quote_handle_returns_ref_without_waiting_for_first_tick() {
    let mut api = support::seeded_api();
    let quote = api.quote("SHFE.au2602").await.unwrap();
    assert!(!quote.is_ready().unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn quotes_submits_one_batch_and_returns_symbol_indexed_refs() {
    let mut api = support::seeded_api();

    let quotes = api.quotes(["SHFE.au2602", "DCE.m2609"]).await.unwrap();

    assert!(quotes.get("SHFE.au2602").is_some());
    assert!(quotes.get("DCE.m2609").is_some());
    assert!(quotes.get("CZCE.MA607").is_none());
    assert_eq!(
        quotes
            .iter()
            .map(|quote| quote.symbol().to_string())
            .collect::<Vec<_>>(),
        vec!["DCE.m2609", "SHFE.au2602"]
    );

    let dispatches = api.session().handle().drain_dispatches().unwrap();
    let payloads = dispatches
        .iter()
        .map(|dispatch| transport_payload(&dispatch.request))
        .filter(|payload| payload["aid"] == "subscribe_quote")
        .collect::<Vec<_>>();
    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0]["ins_list"], "DCE.m2609,SHFE.au2602");
}

#[tokio::test(flavor = "current_thread")]
async fn quote_handle_reads_snapshot_without_api_argument_after_step() {
    let mut api = support::seeded_api();
    support::seed_quote_commit_with_datetime(
        &mut api,
        "SHFE.au2602",
        618.0,
        "2024-04-22 09:00:00.000000",
    );

    let quote = api.quote("SHFE.au2602").await.unwrap();
    let step = api
        .step()
        .await
        .unwrap()
        .expect("seed commit should produce step");

    assert!(step.is_changing(&quote));
    let snapshot = quote.load().unwrap();
    assert_eq!(snapshot.instrument_id, "SHFE.au2602");
    assert_eq!(snapshot.datetime, "2024-04-22 09:00:00.000000");
    assert_eq!(snapshot.last_price, 618.0);
}

#[tokio::test(flavor = "current_thread")]
async fn quote_handle_returns_changed_snapshot_for_matching_step() {
    let mut api = support::seeded_api();
    support::seed_quote_commit_with_datetime(
        &mut api,
        "SHFE.au2602",
        618.0,
        "2024-04-22 09:00:00.000000",
    );

    let quote = api.quote("SHFE.au2602").await.unwrap();
    let other_quote = api.quote("SHFE.ag2602").await.unwrap();
    let step = api
        .step()
        .await
        .unwrap()
        .expect("seed commit should produce step");

    assert_eq!(
        quote.changed_snapshot(&step).unwrap().unwrap().last_price,
        618.0
    );
    assert!(other_quote.changed_snapshot(&step).unwrap().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn quote_step_until_reports_not_ready_when_deadline_expires() {
    let mut api = support::seeded_api();
    let quote = api.quote("SHFE.au2602").await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(10);

    let ready = loop {
        match api.step_until(Some(deadline)).await.unwrap() {
            Some(step) if step.is_changing(&quote) && quote.snapshot().unwrap().is_some() => {
                break true;
            }
            Some(_) => {}
            None => break false,
        }
    };

    assert!(!ready);
    assert_eq!(
        quote.load().unwrap_err(),
        tqsdk_wait::WaitFacadeError::InvalidState("quote not ready")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn trading_status_handle_returns_ref_without_blocking() {
    let mut api = support::seeded_api();
    let status = api.trading_status("SHFE.au2602").await.unwrap();
    assert_eq!(
        status.load().unwrap_err(),
        tqsdk_wait::WaitFacadeError::InvalidState("trading status not ready")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn kline_handle_reports_ready_even_when_window_is_empty() {
    let mut api = support::seeded_api();
    support::seed_ready_empty_kline_chart(&mut api, "CZCE.PF607", 60_000_000_000, 64);

    let bars = api
        .kline_ready("CZCE.PF607", Duration::from_secs(60), 64, None)
        .await
        .unwrap();
    let step = api
        .step()
        .await
        .unwrap()
        .expect("chart commit should be replayed to caller");

    assert!(step.is_changing(&bars));
    assert!(bars.is_ready().unwrap());
    assert!(!bars.has_rows().unwrap());
    assert!(bars.window().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn kline_returns_handle_without_waiting_for_chart_ready() {
    let mut api = support::seeded_api();

    let bars = api
        .kline("CZCE.PF607", Duration::from_secs(60), 64)
        .await
        .unwrap();

    assert!(!bars.is_ready().unwrap());
    assert!(!bars.has_rows().unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn kline_set_chart_uses_protocol_safe_chart_id() {
    let mut api = support::seeded_api();

    let _bars = api
        .kline("SHFE.au2602", Duration::from_secs(60), 64)
        .await
        .unwrap();
    let dispatches = api.session().handle().drain_dispatches().unwrap();
    let set_chart = dispatches
        .iter()
        .map(|dispatch| transport_payload(&dispatch.request))
        .find(|payload| payload["aid"] == "set_chart")
        .expect("kline should submit set_chart");

    assert_eq!(
        set_chart["chart_id"],
        "wait-kline-SHFE_au2602-60000000000-64"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn kline_multi_uses_one_chart_with_joined_symbols_and_bootstrap_width() {
    let mut api = support::seeded_api();

    let bars = api
        .kline_multi(["SHFE.au2602", "DCE.m2609"], Duration::from_secs(60), 64)
        .await
        .unwrap();
    let dispatches = api.session().handle().drain_dispatches().unwrap();
    let set_chart = dispatches
        .iter()
        .map(|dispatch| transport_payload(&dispatch.request))
        .find(|payload| payload["aid"] == "set_chart")
        .expect("multi kline should submit set_chart");

    assert_eq!(bars.primary_symbol(), "SHFE.au2602");
    assert_eq!(bars.symbols(), ["SHFE.au2602", "DCE.m2609"]);
    assert_eq!(
        set_chart["chart_id"],
        "wait-kline-multi-SHFE_au2602_DCE_m2609-60000000000-64"
    );
    assert_eq!(set_chart["ins_list"], "SHFE.au2602,DCE.m2609");
    assert_eq!(set_chart["duration"], 60_000_000_000_i64);
    assert_eq!(set_chart["view_width"], 10_000);
}

#[tokio::test(flavor = "current_thread")]
async fn kline_multi_window_aligns_secondary_rows_with_binding() {
    let mut api = support::seeded_api();
    support::seed_ready_multi_kline_chart(
        &mut api,
        &["SHFE.au2602", "DCE.m2609"],
        60_000_000_000,
        2,
    );

    let bars = api
        .kline_multi(["SHFE.au2602", "DCE.m2609"], Duration::from_secs(60), 2)
        .await
        .unwrap();
    let step = api
        .step()
        .await
        .unwrap()
        .expect("chart commit should produce step");

    assert!(step.is_changing(&bars));
    let window = bars.window().unwrap();
    assert_eq!(window.primary_symbol(), "SHFE.au2602");
    assert_eq!(window.symbols(), ["SHFE.au2602", "DCE.m2609"]);
    assert_eq!(window.view_width(), 2);
    assert_eq!(window.len(), 2);
    assert_eq!(
        window
            .rows()
            .iter()
            .map(|row| row.primary_id())
            .collect::<Vec<_>>(),
        vec![101, 103]
    );
    assert_eq!(
        window.rows()[0].get("DCE.m2609").expect("secondary row").id,
        301
    );
    assert_eq!(
        window.rows()[1]
            .get("DCE.m2609")
            .expect("secondary row")
            .close,
        3205.0
    );
}

#[tokio::test(flavor = "current_thread")]
async fn kline_handle_reads_bounded_window_without_api_argument_after_step() {
    let mut api = support::seeded_api();
    support::seed_ready_kline_chart(&mut api, "SHFE.au2602", 60_000_000_000, 64);

    let bars = api
        .kline("SHFE.au2602", Duration::from_secs(60), 64)
        .await
        .unwrap();
    let step = api
        .step()
        .await
        .unwrap()
        .expect("chart commit should produce step");

    assert!(step.is_changing(&bars));
    let window = bars.window().unwrap();
    assert_eq!(window.symbol(), "SHFE.au2602");
    assert_eq!(window.view_width(), 64);
    assert_eq!(window.len(), 2);
    assert!(
        window
            .rows()
            .iter()
            .all(|row| row.id >= 100 && row.id <= 101)
    );
    assert_eq!(window.last().unwrap().close, 620.0);
}

#[tokio::test(flavor = "current_thread")]
async fn kline_handle_exposes_last_rows_since_and_changed_rows() {
    let mut api = support::seeded_api();

    support::seed_ready_kline_chart(&mut api, "SHFE.au2602", 60_000_000_000, 32);
    let klines = api
        .kline("SHFE.au2602", Duration::from_secs(60), 32)
        .await
        .unwrap();

    let ready_step = api.step().await.unwrap().expect("kline chart commit");
    assert_eq!(klines.last().unwrap().unwrap().id, 101);
    assert_eq!(klines.last_completed().unwrap().unwrap().id, 100);
    assert_eq!(
        klines
            .rows_since(100)
            .unwrap()
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![101]
    );
    assert_eq!(
        klines
            .changed_rows(&ready_step)
            .unwrap()
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![100, 101]
    );

    support::seed_kline_row_update(&mut api, "SHFE.au2602", 60_000_000_000, 101, 621.5);
    let update_step = api.step().await.unwrap().expect("kline row update");
    let changed = klines.changed_rows(&update_step).unwrap();
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].id, 101);
    assert_eq!(changed[0].close, 621.5);
}

#[tokio::test(flavor = "current_thread")]
async fn kline_handle_validates_length_and_clamps_large_length() {
    let mut api = support::seeded_api();

    let zero_duration = api
        .kline("SHFE.au2602", Duration::ZERO, 32)
        .await
        .unwrap_err();
    assert_eq!(
        zero_duration,
        tqsdk_wait::WaitFacadeError::InvalidState("kline duration must be positive")
    );

    let zero_length = api
        .kline("SHFE.au2602", Duration::from_secs(60), 0)
        .await
        .unwrap_err();
    assert_eq!(
        zero_length,
        tqsdk_wait::WaitFacadeError::InvalidState("serial data_length must be greater than zero")
    );

    support::seed_ready_kline_chart(&mut api, "SHFE.au2602", 60_000_000_000, 10_000);
    let bars = api
        .kline("SHFE.au2602", Duration::from_secs(60), 20_000)
        .await
        .unwrap();

    assert!(bars.is_ready().unwrap());
    assert_eq!(bars.window().unwrap().view_width(), 10_000);
}

#[tokio::test(flavor = "current_thread")]
async fn kline_handle_reuses_existing_chart_without_resubmitting_set_chart() {
    let mut api = support::seeded_api();

    support::seed_ready_kline_chart(&mut api, "SHFE.au2602", 60_000_000_000, 64);
    let _first = api
        .kline("SHFE.au2602", Duration::from_secs(60), 64)
        .await
        .unwrap();
    let first_dispatch_count = api.session().handle().drain_dispatches().unwrap().len();

    let _second = api
        .kline("SHFE.au2602", Duration::from_secs(60), 64)
        .await
        .unwrap();
    let second_dispatch_count = api.session().handle().drain_dispatches().unwrap().len();

    assert!(first_dispatch_count > 0);
    assert_eq!(second_dispatch_count, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn single_symbol_serial_rejects_comma_joined_symbols() {
    let mut api = support::seeded_api();

    let kline_error = api
        .kline("SHFE.au2602,DCE.m2609", Duration::from_secs(60), 64)
        .await
        .unwrap_err();
    assert_eq!(
        kline_error,
        tqsdk_wait::WaitFacadeError::InvalidState(
            "kline accepts one symbol; use kline_multi for multi-contract kline serials"
        )
    );

    let tick_error = api.tick("SHFE.au2602,DCE.m2609", 64).await.unwrap_err();
    assert_eq!(
        tick_error,
        tqsdk_wait::WaitFacadeError::InvalidState(
            "tick serials accept one symbol; multi-contract tick serials are not supported"
        )
    );
}

#[tokio::test(flavor = "current_thread")]
async fn tick_handle_reports_ready_even_when_window_is_empty() {
    let mut api = support::seeded_api();
    support::seed_ready_empty_tick_chart(&mut api, "CZCE.PF607", 32);

    let ticks = api.tick_ready("CZCE.PF607", 32, None).await.unwrap();
    let step = api
        .step()
        .await
        .unwrap()
        .expect("chart commit should be replayed to caller");

    assert!(step.is_changing(&ticks));
    assert!(ticks.is_ready().unwrap());
    assert!(!ticks.has_rows().unwrap());
    assert!(ticks.window().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn tick_returns_handle_without_waiting_for_chart_ready() {
    let mut api = support::seeded_api();

    let ticks = api.tick("CZCE.PF607", 32).await.unwrap();

    assert!(!ticks.is_ready().unwrap());
    assert!(!ticks.has_rows().unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn tick_set_chart_uses_protocol_safe_chart_id() {
    let mut api = support::seeded_api();

    let _ticks = api.tick("SHFE.au2602", 32).await.unwrap();
    let dispatches = api.session().handle().drain_dispatches().unwrap();
    let set_chart = dispatches
        .iter()
        .map(|dispatch| transport_payload(&dispatch.request))
        .find(|payload| payload["aid"] == "set_chart")
        .expect("tick should submit set_chart");

    assert_eq!(set_chart["chart_id"], "wait-tick-SHFE_au2602-32");
}

#[tokio::test(flavor = "current_thread")]
async fn tick_handle_reads_bounded_window_without_api_argument_after_step() {
    let mut api = support::seeded_api();
    support::seed_ready_tick_chart(&mut api, "SHFE.au2602", 32);

    let ticks = api.tick("SHFE.au2602", 32).await.unwrap();
    let step = api
        .step()
        .await
        .unwrap()
        .expect("chart commit should produce step");

    assert!(step.is_changing(&ticks));
    let window = ticks.window().unwrap();
    assert_eq!(window.symbol(), "SHFE.au2602");
    assert_eq!(window.view_width(), 32);
    assert_eq!(window.len(), 2);
    assert!(
        window
            .rows()
            .iter()
            .all(|row| row.id >= 200 && row.id <= 201)
    );
    assert_eq!(window.last().unwrap().last_price, 618.5);
}

#[tokio::test(flavor = "current_thread")]
async fn tick_handle_exposes_last_rows_since_and_changed_rows() {
    let mut api = support::seeded_api();

    support::seed_ready_tick_chart(&mut api, "SHFE.au2602", 32);
    let ticks = api.tick("SHFE.au2602", 32).await.unwrap();

    let ready_step = api.step().await.unwrap().expect("tick chart commit");
    assert_eq!(ticks.last().unwrap().unwrap().id, 201);
    assert_eq!(
        ticks
            .rows_since(200)
            .unwrap()
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![201]
    );
    assert_eq!(
        ticks
            .changed_rows(&ready_step)
            .unwrap()
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![200, 201]
    );

    support::seed_tick_row_update(&mut api, "SHFE.au2602", 201, 619.5);
    let update_step = api.step().await.unwrap().expect("tick row update");
    let changed = ticks.changed_rows(&update_step).unwrap();
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].id, 201);
    assert_eq!(changed[0].last_price, 619.5);
}

#[tokio::test(flavor = "current_thread")]
async fn tick_handle_validates_length_and_clamps_large_length() {
    let mut api = support::seeded_api();

    let zero_length = api.tick("SHFE.au2602", 0).await.unwrap_err();
    assert_eq!(
        zero_length,
        tqsdk_wait::WaitFacadeError::InvalidState("serial data_length must be greater than zero")
    );

    support::seed_ready_tick_chart(&mut api, "SHFE.au2602", 10_000);
    let ticks = api.tick("SHFE.au2602", 20_000).await.unwrap();

    assert!(ticks.is_ready().unwrap());
    assert_eq!(ticks.window().unwrap().view_width(), 10_000);
}

#[tokio::test(flavor = "current_thread")]
async fn tick_handle_reuses_existing_chart_without_resubmitting_set_chart() {
    let mut api = support::seeded_api();

    support::seed_ready_tick_chart(&mut api, "SHFE.au2602", 64);
    let _first = api.tick("SHFE.au2602", 64).await.unwrap();
    let first_dispatch_count = api.session().handle().drain_dispatches().unwrap().len();

    let _second = api.tick("SHFE.au2602", 64).await.unwrap();
    let second_dispatch_count = api.session().handle().drain_dispatches().unwrap().len();

    assert!(first_dispatch_count > 0);
    assert_eq!(second_dispatch_count, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn backtest_tick_ready_and_steps_emit_page_rows_one_tick_at_a_time() {
    let mut api = support::backtest_api_for_test(1_000, 3_000);

    let ticks = api.tick_ready("SHFE.au2602", 2, None).await.unwrap();
    let chart_id = hidden_backtest_chart_id(&api, "SHFE.au2602");
    seed_backtest_tick_page(&mut api, &chart_id, "SHFE.au2602");

    let ready_step = api.step().await.unwrap().expect("ready chart commit");
    assert!(ready_step.is_changing(&ticks));
    assert!(ticks.is_ready().unwrap());
    assert!(ticks.window().unwrap().is_empty());

    let first = api.step().await.unwrap().expect("first backtest tick");
    assert_eq!(first.current_dt(), Some(1_500));
    assert!(first.is_changing(&ticks));
    let first_rows = ticks.changed_rows(&first).unwrap();
    assert_eq!(
        first_rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![10]
    );
    assert_eq!(first_rows[0].last_price, 618.0);

    let second = api.step().await.unwrap().expect("second backtest tick");
    assert_eq!(second.current_dt(), Some(2_500));
    assert!(second.is_changing(&ticks));
    let second_rows = ticks.changed_rows(&second).unwrap();
    assert_eq!(
        second_rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![11]
    );
    assert_eq!(second_rows[0].last_price, 619.0);
    assert_eq!(
        ticks
            .window()
            .unwrap()
            .rows()
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![10, 11]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn backtest_tick_page_waits_for_rows_and_mdhis_completion_before_emit() {
    let mut api = support::backtest_api_for_test(1_000, 3_000);

    let ticks = api.tick_ready("SHFE.au2602", 2, None).await.unwrap();
    let chart_id = hidden_backtest_chart_id(&api, "SHFE.au2602");
    seed_backtest_tick_page_header_only(&mut api, &chart_id, "SHFE.au2602");

    let ready_step = api.step().await.unwrap().expect("ready chart commit");
    assert!(ready_step.is_changing(&ticks));

    let header_only = api
        .step_until(Some(tokio::time::Instant::now()))
        .await
        .unwrap();
    assert!(
        header_only.is_none(),
        "header without tick rows and mdhis completion must stay hidden"
    );

    seed_backtest_tick_page_rows_only(&mut api, "SHFE.au2602");

    let first = api
        .step()
        .await
        .unwrap()
        .expect("first synthetic tick after split hidden page");
    assert_eq!(first.current_dt(), Some(1_500));
    assert!(first.is_changing(&ticks));
    let first_rows = ticks.changed_rows(&first).unwrap();
    assert_eq!(
        first_rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![10]
    );
    assert_eq!(first_rows[0].last_price, 618.0);
}

#[tokio::test(flavor = "current_thread")]
async fn backtest_tick_next_page_starts_from_previous_right_id() {
    let mut api = support::backtest_api_for_test(1_000, 4_000);

    let ticks = api.tick_ready("SHFE.au2602", 2, None).await.unwrap();
    let chart_id = hidden_backtest_chart_id(&api, "SHFE.au2602");
    seed_backtest_tick_page_with_bounds(&mut api, &chart_id, "SHFE.au2602", 12, false);

    let _ready_step = api.step().await.unwrap().expect("ready chart commit");
    let _first = api.step().await.unwrap().expect("first backtest tick");
    let second = api.step().await.unwrap().expect("second backtest tick");
    assert_eq!(second.current_dt(), Some(2_500));
    assert!(second.is_changing(&ticks));

    let no_synthetic = api
        .step_until(Some(tokio::time::Instant::now()))
        .await
        .unwrap();
    assert!(no_synthetic.is_none());

    let next_chart = drain_backtest_set_chart_payloads(&api)
        .into_iter()
        .find(|payload| payload.get("left_kline_id").is_some())
        .expect("next hidden chart request should be submitted");
    assert_eq!(next_chart["left_kline_id"], 11);
}

#[tokio::test(flavor = "current_thread")]
async fn backtest_tick_requests_next_page_when_loaded_last_id_equals_right_id() {
    let mut api = support::backtest_api_for_test(1_000, 4_000);

    let ticks = api.tick_ready("SHFE.au2602", 2, None).await.unwrap();
    let chart_id = hidden_backtest_chart_id(&api, "SHFE.au2602");
    seed_backtest_tick_page_with_bounds(&mut api, &chart_id, "SHFE.au2602", 11, false);

    let _ready_step = api.step().await.unwrap().expect("ready chart commit");
    let _first = api.step().await.unwrap().expect("first backtest tick");
    let second = api.step().await.unwrap().expect("second backtest tick");
    assert_eq!(second.current_dt(), Some(2_500));
    assert!(second.is_changing(&ticks));

    let no_synthetic = api
        .step_until(Some(tokio::time::Instant::now()))
        .await
        .unwrap();
    assert!(no_synthetic.is_none());

    let next_chart = drain_backtest_set_chart_payloads(&api)
        .into_iter()
        .find(|payload| payload.get("left_kline_id").is_some())
        .expect("next hidden chart request should not depend on tick last_id exceeding right_id");
    assert_eq!(next_chart["left_kline_id"], 11);
}

#[tokio::test(flavor = "current_thread")]
async fn backtest_tick_skips_page_before_start_and_requests_next_page() {
    let mut api = support::backtest_api_for_test(10_000, 20_000);

    let ticks = api.tick_ready("SHFE.au2602", 2, None).await.unwrap();
    let chart_id = hidden_backtest_chart_id(&api, "SHFE.au2602");
    seed_backtest_tick_page_with_rows(
        &mut api,
        &chart_id,
        "SHFE.au2602",
        10,
        11,
        12,
        vec![(10, 1_500, 618.0), (11, 2_500, 619.0)],
    );

    let ready_step = api.step().await.unwrap().expect("ready chart commit");
    assert!(ready_step.is_changing(&ticks));

    let no_synthetic = api
        .step_until(Some(tokio::time::Instant::now()))
        .await
        .unwrap();
    assert!(
        no_synthetic.is_none(),
        "a hidden page entirely before backtest start must stay hidden"
    );

    let next_chart = drain_backtest_set_chart_payloads(&api)
        .into_iter()
        .find(|payload| payload.get("left_kline_id").is_some())
        .expect("next hidden chart request should be submitted after skipping old rows");
    assert_eq!(next_chart["left_kline_id"], 11);
}

#[tokio::test(flavor = "current_thread")]
async fn backtest_step_returns_none_after_end_datetime() {
    let mut api = support::backtest_api_for_test(1_000, 2_000);
    support::seed_replay_cursor_commit(&mut api, 2_000);

    let first = api.step().await.unwrap();
    assert!(first.is_some());
    assert_eq!(first.unwrap().current_dt(), Some(2_000));

    let second = api.step().await.unwrap();
    assert!(second.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn startup_recovery_waits_for_quote_and_trade_sync_without_manual_flags() {
    let mut api = support::seeded_api();
    support::seed_quote_commit_with_datetime(
        &mut api,
        "SHFE.au2602",
        618.0,
        "2026-04-26 09:00:00.000000",
    );
    support::seed_trade_snapshot(&mut api, "sim", "SHFE.au2602");

    let status = api
        .startup_recovery()
        .quotes(["SHFE.au2602"])
        .trade_account("sim")
        .await
        .unwrap();

    assert!(status.is_ready());
    assert!(status.market_ready);
    assert!(status.trade_ready);
    assert!(status.missing_quotes.is_empty());
    assert!(status.pending_trade_accounts.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn startup_recovery_reports_not_ready_when_deadline_expires() {
    let mut api = support::seeded_api();

    let error = api
        .startup_recovery()
        .quotes(["SHFE.au2602"])
        .deadline(tokio::time::Instant::now() + Duration::from_millis(10))
        .await
        .unwrap_err();

    assert_eq!(
        error,
        tqsdk_wait::WaitFacadeError::InvalidState("startup recovery not ready")
    );
}
