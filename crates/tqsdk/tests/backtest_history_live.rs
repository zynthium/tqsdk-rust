#![cfg(all(feature = "live", feature = "services"))]

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use chrono::{Datelike, FixedOffset, NaiveDate, TimeZone, Utc};
use tqsdk::advanced::data::{
    BacktestHistoryClient, BacktestHistoryPolicy, BacktestHistoryRequest, BacktestHistoryRows,
};
use tqsdk::advanced::wait::TqApiBuilder;
use tqsdk_core::Kline;

const SYMBOL: &str = "KQ.i@SHFE.au";
const SECOND_NS: i64 = 1_000_000_000;
const MAX_REMOTE_CHART_ROWS: usize = 10_000;
const REMOTE_SLICE_NS: i64 = 4 * 24 * 60 * 60 * SECOND_NS;
const MAX_REQUESTED_DURATION_NS: i64 = 60 * 60 * SECOND_NS;
const MAX_LOCAL_COLLECT_BYTES: usize = 512 * 1024 * 1024;

const DURATIONS: [Duration; 6] = [
    Duration::from_secs(15),
    Duration::from_secs(60),
    Duration::from_secs(5 * 60),
    Duration::from_secs(15 * 60),
    Duration::from_secs(30 * 60),
    Duration::from_secs(60 * 60),
];

/// Validates the durable-source matrix against the official server-backtest
/// chart path. The test deliberately keeps every downloaded Tick and 60s
/// partition in the normal persistent cache root for later CacheOnly queries.
#[tokio::test]
#[ignore = "requires TQ_AUTH_* and official server-backtest network access"]
async fn kqi_au_six_complete_months_matches_server_oracle() {
    let window = six_complete_month_window();
    let cache_dir = tqsdk_data::default_history_cache_dir();
    eprintln!(
        "backtest-history live acceptance: symbol={SYMBOL} window=[{:04}-{:02}, {:04}-{:02}) cache_dir={}",
        window.start_label.0,
        window.start_label.1,
        window.end_label.0,
        window.end_label.1,
        cache_dir.display(),
    );

    let requests = requests_for_window(window.start_ns, window.end_ns);
    let fill_client = BacktestHistoryClient::builder(&cache_dir)
        .policy(BacktestHistoryPolicy::RemoteOnMiss)
        .auth_env()
        .build()
        .expect("construct RemoteOnMiss backtest-history client");
    let fill_report = fill_client
        .materialize_cache(requests.clone())
        .await
        .unwrap_or_else(|error| panic!("materialize local durable sources failed: {error}"));
    assert_eq!(
        fill_report.completed.len(),
        DURATIONS.len(),
        "remote materialization must complete every requested period"
    );
    assert!(
        fill_report.failed.is_empty(),
        "remote materialization reported failures: {:?}",
        fill_report.failed
    );

    let local = collect_cache_only_klines(&cache_dir, requests).await;
    let oracle =
        collect_server_klines(SYMBOL, window.start_ns, window.end_ns, DURATIONS.as_slice()).await;

    for duration in DURATIONS {
        let duration_ns = duration_ns(duration);
        let local_rows = local
            .get(&duration_ns)
            .unwrap_or_else(|| panic!("CacheOnly result missing {duration_ns}ns Klines"));
        let remote_rows = oracle
            .rows_by_duration
            .get(&duration_ns)
            .unwrap_or_else(|| panic!("server oracle missing {duration_ns}ns Klines"));
        assert_local_klines_match_server(
            SYMBOL,
            duration_ns,
            oracle.price_decs,
            local_rows,
            remote_rows,
        );
    }
}

#[tokio::test]
#[ignore = "requires TQ_AUTH_* and official server-backtest network access"]
async fn wait_facade_receives_a_short_canonical_minute_chart() {
    let user = std::env::var("TQ_AUTH_USER").expect("TQ_AUTH_USER is required");
    let pass = std::env::var("TQ_AUTH_PASS").expect("TQ_AUTH_PASS is required");
    let end_ns = Utc::now()
        .timestamp_nanos_opt()
        .expect("current timestamp must fit i64")
        - 10 * 24 * 60 * 60 * SECOND_NS;
    let start_ns = end_ns - 4 * 24 * 60 * 60 * SECOND_NS;
    let mut api = TqApiBuilder::new(user, pass)
        .futures_backtest(start_ns, end_ns)
        .expect("construct short server-backtest range")
        .backtest_cache_fill_mode()
        .build()
        .await
        .expect("construct short server-backtest wait facade");
    let quote = api.quote(SYMBOL).await.expect("subscribe probe quote");
    let chart = api
        .kline(SYMBOL, Duration::from_secs(60), MAX_REMOTE_CHART_ROWS)
        .await
        .expect("subscribe probe canonical-minute chart");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let complete = server_backtest_history_complete(&api);
        if chart.is_ready().expect("read probe chart readiness")
            && quote.is_ready().expect("read probe quote readiness")
            && complete
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "wait facade did not initialize the short server-backtest canonical-minute chart"
        );
        api.step_until(Some(deadline))
            .await
            .expect("advance short server-backtest wait facade");
    }
    assert!(
        !chart
            .rows()
            .expect("read probe canonical-minute rows")
            .is_empty(),
        "wait facade received no short canonical-minute rows"
    );
}

#[derive(Clone, Copy)]
struct ValidationWindow {
    start_ns: i64,
    end_ns: i64,
    start_label: (i32, u32),
    end_label: (i32, u32),
}

fn six_complete_month_window() -> ValidationWindow {
    let cst = FixedOffset::east_opt(8 * 60 * 60).expect("CST offset is valid");
    let now = Utc::now().with_timezone(&cst);
    let end_label = (now.year(), now.month());
    let month_index = now.year() * 12 + i32::try_from(now.month0()).expect("month fits i32");
    let start_index = month_index - 6;
    let start_label = (
        start_index.div_euclid(12),
        start_index.rem_euclid(12) as u32 + 1,
    );

    ValidationWindow {
        start_ns: cst_month_start_ns(cst, start_label.0, start_label.1),
        end_ns: cst_month_start_ns(cst, end_label.0, end_label.1),
        start_label,
        end_label,
    }
}

fn cst_month_start_ns(cst: FixedOffset, year: i32, month: u32) -> i64 {
    let date = NaiveDate::from_ymd_opt(year, month, 1)
        .unwrap_or_else(|| panic!("invalid CST month boundary {year}-{month}"));
    cst.from_local_datetime(
        &date
            .and_hms_opt(0, 0, 0)
            .expect("midnight is valid for a fixed offset"),
    )
    .single()
    .expect("fixed CST has no ambiguous local time")
    .timestamp_nanos_opt()
    .expect("validation window must fit i64 nanoseconds")
}

fn requests_for_window(start_ns: i64, end_ns: i64) -> Vec<BacktestHistoryRequest> {
    DURATIONS
        .into_iter()
        .enumerate()
        .map(|(index, duration)| {
            BacktestHistoryRequest::kline(
                u64::try_from(index + 1).expect("request id fits u64"),
                SYMBOL,
                duration,
                start_ns,
                end_ns,
            )
        })
        .collect()
}

async fn collect_cache_only_klines(
    cache_dir: &std::path::Path,
    requests: Vec<BacktestHistoryRequest>,
) -> BTreeMap<i64, Vec<Kline>> {
    let client = BacktestHistoryClient::builder(cache_dir)
        .policy(BacktestHistoryPolicy::CacheOnly)
        .build()
        .expect("construct CacheOnly backtest-history client");
    let collected = client
        .query_batch(requests)
        .await
        .expect("start CacheOnly local query")
        .collect_all(MAX_LOCAL_COLLECT_BYTES)
        .await
        .unwrap_or_else(|error| panic!("read CacheOnly local query failed: {error}"));

    assert!(
        collected.failed.is_empty(),
        "CacheOnly local query reported failures: {:?}",
        collected.failed
    );
    assert_eq!(
        collected.completed.len(),
        DURATIONS.len(),
        "CacheOnly local query must complete every requested period"
    );

    let mut rows_by_duration = BTreeMap::new();
    for collected in collected.completed {
        assert!(
            !collected.request.remote_used,
            "CacheOnly request {} unexpectedly used remote data",
            collected.request.request_id
        );
        let BacktestHistoryRows::Klines { duration_ns, rows } = collected.rows else {
            panic!(
                "Kline request {} returned Tick rows",
                collected.request.request_id
            );
        };
        assert!(
            rows_by_duration.insert(duration_ns, rows).is_none(),
            "CacheOnly local query returned duplicate {duration_ns}ns results"
        );
    }
    rows_by_duration
}

struct ServerKlineOracle {
    price_decs: i64,
    rows_by_duration: BTreeMap<i64, Vec<Kline>>,
}

async fn collect_server_klines(
    symbol: &str,
    start_ns: i64,
    end_ns: i64,
    durations: &[Duration],
) -> ServerKlineOracle {
    let user = std::env::var("TQ_AUTH_USER").expect("TQ_AUTH_USER is required");
    let pass = std::env::var("TQ_AUTH_PASS").expect("TQ_AUTH_PASS is required");
    let mut rows_by_duration = BTreeMap::<i64, BTreeMap<i64, Kline>>::new();
    let mut price_decs = None;

    for (slice_index, (slice_start_ns, slice_end_ns)) in
        remote_slices(start_ns, end_ns).into_iter().enumerate()
    {
        eprintln!(
            "server oracle slice {}/{}: [{slice_start_ns}, {slice_end_ns})",
            slice_index + 1,
            remote_slices(start_ns, end_ns).len(),
        );
        let (slice_price_decs, slice_rows) = collect_server_kline_slice(
            &user,
            &pass,
            symbol,
            start_ns,
            end_ns,
            slice_start_ns,
            slice_end_ns,
            durations,
        )
        .await;
        if let Some(expected) = price_decs {
            assert_eq!(
                expected, slice_price_decs,
                "server quote price_decs changed while collecting {symbol}"
            );
        } else {
            price_decs = Some(slice_price_decs);
        }

        for (duration_ns, rows) in slice_rows {
            let merged = rows_by_duration.entry(duration_ns).or_default();
            for row in rows {
                assert!(
                    merged.insert(row.datetime, row).is_none(),
                    "server oracle returned duplicate {duration_ns}ns bar timestamps for {symbol}"
                );
            }
        }
    }

    let rows_by_duration = rows_by_duration
        .into_iter()
        .map(|(duration_ns, rows)| (duration_ns, rows.into_values().collect()))
        .collect();
    ServerKlineOracle {
        price_decs: price_decs.expect("server oracle must collect at least one slice"),
        rows_by_duration,
    }
}

#[allow(clippy::too_many_arguments)]
async fn collect_server_kline_slice(
    user: &str,
    pass: &str,
    symbol: &str,
    global_start_ns: i64,
    global_end_ns: i64,
    slice_start_ns: i64,
    slice_end_ns: i64,
    durations: &[Duration],
) -> (i64, BTreeMap<i64, Vec<Kline>>) {
    let request_start_ns = slice_start_ns.saturating_sub(MAX_REQUESTED_DURATION_NS);
    let request_end_ns = slice_end_ns.saturating_add(MAX_REQUESTED_DURATION_NS);
    let mut api = TqApiBuilder::new(user.to_owned(), pass.to_owned())
        .futures_backtest(request_start_ns, request_end_ns)
        .expect("construct server-backtest query")
        .backtest_cache_fill_mode()
        .build()
        .await
        .unwrap_or_else(|error| panic!("server Kline connection failed for {symbol}: {error}"));
    let quote = api
        .quote(symbol)
        .await
        .unwrap_or_else(|error| panic!("server quote subscription failed for {symbol}: {error}"));
    let mut charts = Vec::with_capacity(durations.len());
    for duration in durations {
        let duration_ns = duration_ns(*duration);
        let chart = api
            .kline(symbol, *duration, MAX_REMOTE_CHART_ROWS)
            .await
            .unwrap_or_else(|error| {
                panic!("server {duration_ns}ns Kline subscription failed for {symbol}: {error}")
            });
        charts.push((duration_ns, chart));
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let charts_ready = charts.iter().all(|(_, chart)| chart.is_ready().unwrap());
        if charts_ready && quote.is_ready().unwrap() && server_backtest_history_complete(&api) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "server Kline history did not finish for {symbol} in [{request_start_ns}, {request_end_ns})"
        );
        api.step_until(Some(deadline))
            .await
            .unwrap_or_else(|error| panic!("server Kline polling failed for {symbol}: {error}"));
    }

    let price_decs = quote
        .load()
        .unwrap_or_else(|error| panic!("server quote was not readable for {symbol}: {error}"))
        .price_decs;
    let rows_by_duration = charts
        .into_iter()
        .map(|(duration_ns, chart)| {
            let rows = chart
                .rows()
                .unwrap_or_else(|error| panic!("server chart rows failed for {symbol}: {error}"))
                .into_iter()
                .filter(|row| {
                    row.datetime >= global_start_ns
                        && row.datetime >= slice_start_ns
                        && row.datetime < slice_end_ns
                        && row
                            .datetime
                            .checked_add(duration_ns)
                            .is_some_and(|bar_end_ns| bar_end_ns <= global_end_ns)
                })
                .collect();
            (duration_ns, rows)
        })
        .collect();
    (price_decs, rows_by_duration)
}

fn remote_slices(start_ns: i64, end_ns: i64) -> Vec<(i64, i64)> {
    let mut slices = Vec::new();
    let mut cursor = start_ns;
    while cursor < end_ns {
        let next = cursor.saturating_add(REMOTE_SLICE_NS).min(end_ns);
        slices.push((cursor, next));
        cursor = next;
    }
    slices
}

fn server_backtest_history_complete(api: &tqsdk::advanced::wait::TqApi) -> bool {
    api.session()
        .reader()
        .read_market_state()
        .get_path(&["mdhis_more_data"])
        .and_then(|value| value.as_bool())
        == Some(false)
}

fn assert_local_klines_match_server(
    symbol: &str,
    duration_ns: i64,
    price_decs: i64,
    local: &[Kline],
    remote: &[Kline],
) {
    assert!(
        !local.is_empty(),
        "local {duration_ns}ns Kline query produced no complete bars for {symbol}"
    );
    assert!(
        !remote.is_empty(),
        "server {duration_ns}ns Kline query produced no complete bars for {symbol}"
    );
    let local = index_klines_by_datetime(symbol, duration_ns, "local", local);
    let remote = index_klines_by_datetime(symbol, duration_ns, "server", remote);
    let datetimes = local
        .keys()
        .chain(remote.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    let mut mismatch = KlineMismatchCounts::default();
    let mut examples = Vec::new();

    for datetime in datetimes {
        match (local.get(&datetime), remote.get(&datetime)) {
            (Some(local_row), Some(remote_row)) => {
                let row_mismatch = mismatch.compare_row(local_row, remote_row, price_decs);
                if row_mismatch && examples.len() < 20 {
                    examples.push(format!(
                        "datetime={datetime}: local={local_row:?}; server={remote_row:?}"
                    ));
                }
            }
            (local_row, remote_row) => {
                mismatch.datetime += 1;
                if examples.len() < 20 {
                    examples.push(format!(
                        "datetime={datetime}: local={local_row:?}; server={remote_row:?}"
                    ));
                }
            }
        }
    }

    eprintln!(
        "official Kline comparison: symbol={symbol} duration_ns={duration_ns} local={} server={} datetime={} open={} high={} low={} close={} volume={} open_oi={} close_oi={}",
        local.len(),
        remote.len(),
        mismatch.datetime,
        mismatch.open,
        mismatch.high,
        mismatch.low,
        mismatch.close,
        mismatch.volume,
        mismatch.open_oi,
        mismatch.close_oi,
    );
    if !examples.is_empty() {
        eprintln!(
            "official Kline comparison first {} mismatches for {symbol} {duration_ns}ns:\n{}",
            examples.len(),
            examples.join("\n"),
        );
    }
    assert!(
        mismatch.is_zero(),
        "{symbol} {duration_ns}ns local/server Kline comparison has mismatches: {mismatch:?}"
    );
}

fn index_klines_by_datetime<'a>(
    symbol: &str,
    duration_ns: i64,
    source: &str,
    rows: &'a [Kline],
) -> BTreeMap<i64, &'a Kline> {
    let mut indexed = BTreeMap::new();
    for row in rows {
        assert!(
            indexed.insert(row.datetime, row).is_none(),
            "{source} {duration_ns}ns Kline result has duplicate bar timestamp {} for {symbol}",
            row.datetime
        );
    }
    indexed
}

#[derive(Debug, Default)]
struct KlineMismatchCounts {
    datetime: usize,
    open: usize,
    high: usize,
    low: usize,
    close: usize,
    volume: usize,
    open_oi: usize,
    close_oi: usize,
}

impl KlineMismatchCounts {
    fn compare_row(&mut self, local: &Kline, remote: &Kline, price_decs: i64) -> bool {
        let mut mismatch = false;
        if !same_kline_price(local.open, remote.open, price_decs) {
            self.open += 1;
            mismatch = true;
        }
        if !same_kline_price(local.high, remote.high, price_decs) {
            self.high += 1;
            mismatch = true;
        }
        if !same_kline_price(local.low, remote.low, price_decs) {
            self.low += 1;
            mismatch = true;
        }
        if !same_kline_price(local.close, remote.close, price_decs) {
            self.close += 1;
            mismatch = true;
        }
        if local.volume != remote.volume {
            self.volume += 1;
            mismatch = true;
        }
        if local.open_oi != remote.open_oi {
            self.open_oi += 1;
            mismatch = true;
        }
        if local.close_oi != remote.close_oi {
            self.close_oi += 1;
            mismatch = true;
        }
        mismatch
    }

    fn is_zero(&self) -> bool {
        self.datetime == 0
            && self.open == 0
            && self.high == 0
            && self.low == 0
            && self.close == 0
            && self.volume == 0
            && self.open_oi == 0
            && self.close_oi == 0
    }
}

fn same_kline_price(left: f64, right: f64, price_decs: i64) -> bool {
    if left.is_nan() || right.is_nan() {
        return left.is_nan() && right.is_nan();
    }
    if !left.is_finite() || !right.is_finite() {
        return left == right;
    }
    let scale = 10_f64.powi(price_decs.clamp(0, 12) as i32);
    (left * scale).round() == (right * scale).round()
}

fn duration_ns(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).expect("validation duration must fit i64 nanoseconds")
}
