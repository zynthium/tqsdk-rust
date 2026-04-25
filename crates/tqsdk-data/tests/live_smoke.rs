#![cfg(feature = "live")]

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use tokio::io::AsyncWrite;
use tqsdk_data::{DataClient, OptionGreeksRequest, TickDataSeriesRequest};
use tqsdk_session::{OptionQueryFilter, SessionClientBuilder};

#[test]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS and market/query access"]
fn live_option_greeks_smoke() {
    run_on_tokio(async {
        let Some(auth_user) = read_env("TQ_AUTH_USER") else {
            return;
        };
        let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
            return;
        };

        let session = SessionClientBuilder::new(auth_user, auth_pass)
            .enable_query()
            .build()
            .expect("live session should build");
        let option_symbols = if let Some(symbol) = read_env("TQ_OPTION_SYMBOL") {
            vec![symbol]
        } else if session.check_md_grants(&["SSE.510300"]).await.is_ok() {
            let mut filter = OptionQueryFilter::new();
            filter.expired = Some(false);
            session
                .query_options("SSE.510300", &filter)
                .await
                .expect("query_options should succeed")
                .into_iter()
                .take(16)
                .collect::<Vec<_>>()
        } else {
            let mut filter = OptionQueryFilter::new();
            filter.expired = Some(false);
            session
                .query_options("SHFE.au2606", &filter)
                .await
                .expect("query_options should succeed")
                .into_iter()
                .take(16)
                .collect::<Vec<_>>()
        };
        let client = DataClient::from_session(session);
        let greeks = client
            .query_option_greeks(OptionGreeksRequest::new(
                option_symbols.iter().map(String::as_str),
            ))
            .await
            .expect("query_option_greeks should succeed");
        let row = greeks
            .rows()
            .iter()
            .find(|row| {
                row.option_last_price.is_finite()
                    && row.underlying_last_price.is_finite()
                    && row.delta.is_finite()
            })
            .expect("query_option_greeks should return at least one stable row");

        assert!(option_symbols.contains(&row.symbol));
        assert!(!row.underlying_symbol.is_empty());
        assert!(row.option_last_price.is_finite());
        assert!(row.underlying_last_price.is_finite());
        assert!(row.delta.is_finite(), "delta={}", row.delta);
    });
}

#[test]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS and tq_dl history access"]
fn live_export_kline_csv_smoke() {
    run_on_tokio(async {
        let Some(auth_user) = read_env("TQ_AUTH_USER") else {
            return;
        };
        let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
            return;
        };
        let symbol = read_env("TQ_TEST_SYMBOL").unwrap_or_else(|| "SHFE.ao2609".to_string());
        let end_dt = Utc::now();
        let start_dt = end_dt - ChronoDuration::minutes(30);

        let session = SessionClientBuilder::new(auth_user, auth_pass)
            .futures_market()
            .build()
            .expect("live session should build");
        if !session
            .has_feature("tq_dl")
            .await
            .expect("history grant check should succeed")
        {
            return;
        }
        let client = DataClient::from_session(session);
        let mut writer = MemoryWriter::default();

        let summary = client
            .export_kline_data_csv(
                tqsdk_data::KlineDataSeriesRequest::new(
                    symbol.as_str(),
                    Duration::from_secs(60),
                    start_dt
                        .timestamp_nanos_opt()
                        .expect("start_dt should fit in i64"),
                    end_dt
                        .timestamp_nanos_opt()
                        .expect("end_dt should fit in i64"),
                )
                .with_page_view_width(128)
                .with_timeout(Duration::from_secs(30)),
                &mut writer,
            )
            .await
            .expect("export_kline_data_csv should succeed");

        let csv = String::from_utf8(writer.into_inner()).expect("writer output should be UTF-8");
        assert_eq!(summary.symbol, symbol);
        assert!(summary.rows_written > 0);
        assert!(summary.pages_written > 0);
        assert!(
            csv.starts_with("id,datetime,open,high,low,close,volume,open_oi,close_oi,_epoch\n")
        );
        assert!(csv.lines().count() > 1, "csv should contain data rows");
    });
}

#[test]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS and tq_dl history access"]
fn live_export_tick_csv_smoke() {
    run_on_tokio(async {
        let Some(auth_user) = read_env("TQ_AUTH_USER") else {
            return;
        };
        let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
            return;
        };
        let symbol = read_env("TQ_TEST_SYMBOL").unwrap_or_else(|| "SHFE.ao2609".to_string());
        let end_dt = Utc::now();
        let start_dt = end_dt - ChronoDuration::minutes(30);

        let session = SessionClientBuilder::new(auth_user, auth_pass)
            .futures_market()
            .build()
            .expect("live session should build");
        if !session
            .has_feature("tq_dl")
            .await
            .expect("history grant check should succeed")
        {
            return;
        }
        let client = DataClient::from_session(session);
        let mut writer = MemoryWriter::default();

        let summary = client
            .export_tick_data_csv(
                TickDataSeriesRequest::new(
                    symbol.as_str(),
                    start_dt
                        .timestamp_nanos_opt()
                        .expect("start_dt should fit in i64"),
                    end_dt
                        .timestamp_nanos_opt()
                        .expect("end_dt should fit in i64"),
                )
                .with_page_view_width(128)
                .with_timeout(Duration::from_secs(30)),
                &mut writer,
            )
            .await
            .expect("export_tick_data_csv should succeed");

        let csv = String::from_utf8(writer.into_inner()).expect("writer output should be UTF-8");
        assert_eq!(summary.symbol, symbol);
        assert!(summary.rows_written > 0);
        assert!(summary.pages_written > 0);
        assert!(
            csv.starts_with(
                "id,datetime,last_price,average,highest,lowest,ask_price1,ask_volume1,"
            )
        );
        assert!(csv.lines().count() > 1, "csv should contain data rows");
    });
}

fn run_on_tokio<F, T>(future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("tokio runtime should build");
    runtime.block_on(future)
}

fn read_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[derive(Default)]
struct MemoryWriter {
    buffer: Vec<u8>,
}

impl MemoryWriter {
    fn into_inner(self) -> Vec<u8> {
        self.buffer
    }
}

impl AsyncWrite for MemoryWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
