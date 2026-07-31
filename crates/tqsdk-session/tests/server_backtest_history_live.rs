#![cfg(all(feature = "live", feature = "services"))]

use std::time::Duration;

use chrono::Utc;
use tokio::time::Instant;
use tqsdk_core::{Chart, MarketChartCommand, Symbol};
use tqsdk_session::{
    ServerBacktestHistoryChart, ServerBacktestHistoryEvent, ServerBacktestHistoryKind,
    ServerBacktestHistoryRequest, ServerBacktestHistoryStream, ServerBacktestMarketKind,
    SessionClientBuilder,
};

const SYMBOL: &str = "KQ.i@SHFE.au";

/// A small, non-persistent liveness probe for the session-owned server-history
/// substrate. It is intentionally narrower than the data-layer acceptance:
/// it makes a single canonical-minute request and never opens a cache.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires TQ_AUTH_* and official server-backtest network access"]
async fn canonical_minute_stream_yields_a_terminal_page() {
    let user = std::env::var("TQ_AUTH_USER").expect("TQ_AUTH_USER is required");
    let pass = std::env::var("TQ_AUTH_PASS").expect("TQ_AUTH_PASS is required");
    let end_ns = Utc::now()
        .timestamp_nanos_opt()
        .expect("current timestamp must fit i64")
        - 10 * 24 * 60 * 60 * 1_000_000_000_i64;
    let start_ns = end_ns - 4 * 24 * 60 * 60 * 1_000_000_000_i64;
    let chart_id = "server-history-live-probe-60s";
    let session = SessionClientBuilder::new(user, pass)
        .futures_backtest_market()
        .build()
        .expect("construct futures server-backtest session");
    session
        .ensure_quotes([SYMBOL])
        .await
        .expect("subscribe probe quote before opening the history chart");
    let reader = session.reader_clone();
    let mut stream = ServerBacktestHistoryStream::open(
        session,
        ServerBacktestHistoryRequest {
            market_kind: ServerBacktestMarketKind::Futures,
            start_ns,
            end_ns,
            charts: vec![ServerBacktestHistoryChart {
                chart_id: chart_id.to_string(),
                symbol: SYMBOL.to_string(),
                kind: ServerBacktestHistoryKind::CanonicalMinute,
            }],
        },
    )
    .await
    .expect("open canonical-minute server history stream");

    let deadline = Instant::now() + Duration::from_secs(60);
    let mut row_count = 0usize;
    loop {
        match stream
            .next_event(Some(deadline))
            .await
            .expect("advance canonical-minute server history stream")
        {
            Some(ServerBacktestHistoryEvent::CanonicalMinutes { rows, .. }) => {
                row_count += rows.len();
            }
            Some(ServerBacktestHistoryEvent::StreamCompleted) => break,
            Some(_) => {}
            None => {
                let market = reader.read_market_state();
                let chart = market
                    .decode_path::<Chart>(&["charts", chart_id])
                    .expect("decode probe chart state");
                let mdhis_more_data = market
                    .get_path(&["mdhis_more_data"])
                    .and_then(serde_json::Value::as_bool);
                let series = market
                    .get_path(&["klines", SYMBOL, "60000000000"])
                    .map(|series| {
                        let data_summary = series
                            .get("data")
                            .and_then(serde_json::Value::as_object)
                            .map(|data| {
                                let datetimes = data
                                    .values()
                                    .filter_map(|row| {
                                        row.get("datetime").and_then(serde_json::Value::as_i64)
                                    })
                                    .collect::<Vec<_>>();
                                (
                                    data.len(),
                                    datetimes.iter().min().copied(),
                                    datetimes.iter().max().copied(),
                                )
                            });
                        (
                            series.get("last_id").and_then(serde_json::Value::as_i64),
                            data_summary,
                        )
                    });
                let chart_ids = market
                    .get_path(&["charts"])
                    .and_then(serde_json::Value::as_object)
                    .map(|charts| charts.keys().cloned().collect::<Vec<_>>());
                let page_state = chart.as_ref().map(|chart| {
                    (
                        chart
                            .state
                            .get("ins_list")
                            .and_then(serde_json::Value::as_str),
                        chart
                            .state
                            .get("duration")
                            .and_then(serde_json::Value::as_i64),
                        chart
                            .state
                            .get("view_width")
                            .and_then(serde_json::Value::as_u64),
                        chart
                            .state
                            .get("focus_datetime")
                            .and_then(serde_json::Value::as_i64),
                        chart
                            .state
                            .get("focus_position")
                            .and_then(serde_json::Value::as_u64),
                    )
                });
                panic!(
                    "server history probe timed out: start_ns={start_ns}, mdhis_more_data={mdhis_more_data:?}, chart={chart:?}, chart_ids={chart_ids:?}, series={series:?}, page_state={page_state:?}"
                );
            }
        }
    }

    assert!(row_count > 0, "server history probe returned no 60s rows");
}

/// A direct session control for the same request. Keeping it next to the
/// stream probe makes a live failure localizable without involving the wait
/// facade or a persistent cache.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires TQ_AUTH_* and official server-backtest network access"]
async fn direct_session_progresses_a_short_canonical_minute_chart() {
    let user = std::env::var("TQ_AUTH_USER").expect("TQ_AUTH_USER is required");
    let pass = std::env::var("TQ_AUTH_PASS").expect("TQ_AUTH_PASS is required");
    let end_ns = Utc::now()
        .timestamp_nanos_opt()
        .expect("current timestamp must fit i64")
        - 10 * 24 * 60 * 60 * 1_000_000_000_i64;
    let start_ns = end_ns - 4 * 24 * 60 * 60 * 1_000_000_000_i64;
    let chart_id = "server-history-direct-session-probe-60s";
    let session = SessionClientBuilder::new(user, pass)
        .futures_backtest_market()
        .build()
        .expect("construct futures server-backtest session");
    session
        .ensure_quotes([SYMBOL])
        .await
        .expect("subscribe direct-session probe quote");
    let _lease = session
        .ensure_chart(MarketChartCommand {
            chart_id: chart_id.to_string(),
            symbols: vec![Symbol::new(SYMBOL)],
            duration_ns: 60_000_000_000,
            view_width: 10_000,
            left_kline_id: None,
            focus_datetime_ns: Some(start_ns),
            focus_position: Some(0),
        })
        .await
        .expect("subscribe direct-session canonical-minute chart");
    let reader = session.reader_clone();
    let mut cursor = reader.cursor();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let market = reader.read_market_state();
        let chart = market
            .decode_path::<Chart>(&["charts", chart_id])
            .expect("decode direct-session probe chart state");
        let complete = market
            .get_path(&["mdhis_more_data"])
            .and_then(serde_json::Value::as_bool)
            == Some(false);
        drop(market);
        if chart.as_ref().is_some_and(|chart| chart.ready) && complete {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "direct session did not initialize the server-backtest canonical-minute chart: chart={chart:?}"
        );
        if reader.next(&mut cursor).is_some() {
            continue;
        }
        if session
            .progress_once(Some(deadline))
            .await
            .expect("advance direct-session server-backtest chart")
            .is_progress()
        {
            continue;
        }
    }
}
