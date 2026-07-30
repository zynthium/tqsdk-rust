use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use tokio::task::JoinSet;
use tqsdk_core::Kline;
use tqsdk_data::{MINUTE_KLINE_DURATION_NS, MinuteKlineCache, MinuteKlineCacheSnapshot};
use tqsdk_wait::BacktestMarketKind;

use crate::backtest_remote::{BacktestRemoteFillProgress, RemoteBacktestFillRuntime};
use crate::{Auth, Result, data_validation};

const REMOTE_MINUTE_KLINE_DATA_LENGTH: usize = 10_000;
// A server chart retains at most `REMOTE_MINUTE_KLINE_DATA_LENGTH` rows. Keep
// each request within that wall-clock span even when the caller did not choose
// an explicit remote-fill slice; sparse sessions remain safe, while 24-hour
// instruments cannot silently lose the older canonical minutes in a month.
const REMOTE_MINUTE_KLINE_MAX_SPAN_NS: i64 =
    MINUTE_KLINE_DURATION_NS * REMOTE_MINUTE_KLINE_DATA_LENGTH as i64;
const REMOTE_MINUTE_KLINE_READY_TIMEOUT: Duration = Duration::from_secs(30);
const REMOTE_MINUTE_CONNECT_RETRY_ATTEMPTS: usize = 5;

/// One final canonical-minute range that must be materialized from an
/// official server-side backtest stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BacktestMinuteKlineFillRequest {
    pub(crate) symbol: String,
    pub(crate) start_ns: i64,
    pub(crate) end_ns: i64,
}

impl BacktestMinuteKlineFillRequest {
    pub(crate) fn new(symbol: impl Into<String>, start_ns: i64, end_ns: i64) -> Self {
        Self {
            symbol: symbol.into(),
            start_ns,
            end_ns,
        }
    }
}

pub(crate) struct BacktestMinuteKlineFillReport {
    pub(crate) rows_by_symbol: BTreeMap<String, usize>,
}

#[derive(Debug, Clone)]
struct MinuteKlineFillBatch {
    start_ns: i64,
    end_ns: i64,
    requests: Vec<BacktestMinuteKlineFillRequest>,
}

struct MinuteKlineFillBatchReport {
    batch_index: usize,
    start_ns: i64,
    end_ns: i64,
    symbols: Vec<String>,
    elapsed: Duration,
    rows_by_symbol: BTreeMap<String, usize>,
}

struct MinuteKlineFillBatchTask {
    batch_index: usize,
    total_batches: usize,
    auth: Auth,
    cache: MinuteKlineCache,
    snapshot: MinuteKlineCacheSnapshot,
    market_kind: BacktestMarketKind,
    batch: MinuteKlineFillBatch,
    runtime: RemoteBacktestFillRuntime,
}

/// Fill the independent canonical-minute cache from official server-side
/// backtest Kline streams.
///
/// The server chart-complete signal is the only completion authority.  We never
/// infer coverage from timestamp gaps: empty sessions and suspended symbols
/// are still valid final ranges.  A batch keeps every Kline row in memory
/// until its server stream terminates successfully; only then are final
/// coverage records committed.  Cancellation therefore cannot claim final
/// coverage for an interrupted batch.
pub(crate) async fn fill_backtest_minute_kline_cache(
    auth: &Auth,
    cache: &MinuteKlineCache,
    snapshot: &MinuteKlineCacheSnapshot,
    market_kind: BacktestMarketKind,
    requests: Vec<BacktestMinuteKlineFillRequest>,
    runtime: RemoteBacktestFillRuntime,
) -> Result<BacktestMinuteKlineFillReport> {
    for request in &requests {
        validate_request(request)?;
    }
    let requests = split_minute_fill_requests(requests, runtime.config())?;
    let mut pending_batches = minute_fill_batches(requests, runtime.config().symbol_batch_size)?;
    let total_batches = pending_batches.len();
    let requested_symbols = pending_batches
        .iter()
        .flat_map(|batch| batch.requests.iter().map(|request| request.symbol.as_str()))
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    if total_batches == 0 {
        return Ok(BacktestMinuteKlineFillReport {
            rows_by_symbol: BTreeMap::new(),
        });
    }

    let config = runtime.config();
    runtime.emit(BacktestRemoteFillProgress::FillStarted {
        requested_symbols,
        total_batches,
        symbol_batch_size: config.symbol_batch_size,
        symbol_concurrency: config.symbol_concurrency,
        batch_timeout: config.batch_timeout,
    });

    let mut tasks = JoinSet::new();
    let mut next_batch_index = 0usize;
    let mut completed_batches = 0usize;
    let mut rows_by_symbol = BTreeMap::new();
    let mut errors = Vec::new();
    while !pending_batches.is_empty() || !tasks.is_empty() {
        while !runtime.is_cancelled() && tasks.len() < config.symbol_concurrency {
            let Some(batch) = pending_batches.pop_front() else {
                break;
            };
            let batch_index = next_batch_index;
            next_batch_index = next_batch_index.saturating_add(1);
            let symbols = batch
                .requests
                .iter()
                .map(|request| request.symbol.clone())
                .collect::<Vec<_>>();
            runtime.emit(BacktestRemoteFillProgress::BatchStarted {
                batch_number: batch_index + 1,
                total_batches,
                pending_batches: pending_batches.len(),
                active_batches: tasks.len() + 1,
                requested_range: (batch.start_ns, batch.end_ns),
                symbols,
            });
            let task = MinuteKlineFillBatchTask {
                batch_index,
                total_batches,
                auth: auth.clone(),
                cache: cache.clone(),
                snapshot: snapshot.clone(),
                market_kind,
                batch,
                runtime: runtime.clone(),
            };
            tasks.spawn(task.run());
        }

        let Some(result) = tasks.join_next().await else {
            break;
        };
        match result {
            Ok(Ok(report)) => {
                completed_batches = completed_batches.saturating_add(1);
                let rows = report.rows_by_symbol.values().copied().sum();
                runtime.emit(BacktestRemoteFillProgress::BatchFinished {
                    batch_number: report.batch_index + 1,
                    total_batches,
                    completed_batches,
                    requested_range: (report.start_ns, report.end_ns),
                    symbols: report.symbols,
                    elapsed: report.elapsed,
                    rows,
                });
                for (symbol, count) in report.rows_by_symbol {
                    *rows_by_symbol.entry(symbol).or_insert(0) += count;
                }
            }
            Ok(Err(error)) => errors.push(error.to_string()),
            Err(error) => errors.push(format!("canonical-minute fill task failed: {error}")),
        }
    }
    if runtime.is_cancelled() {
        return Err(minute_fill_cancelled_error());
    }
    if !errors.is_empty() {
        return Err(data_validation(format!(
            "remote canonical-minute fill completed {completed_batches}/{total_batches} batches; {} batch(es) failed: {}",
            errors.len(),
            errors.join(" | ")
        )));
    }
    Ok(BacktestMinuteKlineFillReport { rows_by_symbol })
}

impl MinuteKlineFillBatchTask {
    async fn run(self) -> Result<MinuteKlineFillBatchReport> {
        let started = tokio::time::Instant::now();
        let symbols = self
            .batch
            .requests
            .iter()
            .map(|request| request.symbol.clone())
            .collect::<Vec<_>>();
        let range = (self.batch.start_ns, self.batch.end_ns);
        let fill = fill_minute_kline_batch(
            &self.auth,
            &self.cache,
            &self.snapshot,
            self.market_kind,
            &self.batch,
            self.runtime.clone(),
        );
        let result = match self.runtime.config().batch_timeout {
            Some(timeout) => tokio::time::timeout(timeout, fill).await.map_err(|_| {
                data_validation(format!(
                    "remote canonical-minute fill batch timed out after {}s for {} symbols ({}) in range [{}, {})",
                    timeout.as_secs(),
                    symbols.len(),
                    symbols.join(","),
                    range.0,
                    range.1
                ))
            })?,
            None => fill.await,
        };
        let rows_by_symbol = match result {
            Ok(rows_by_symbol) => rows_by_symbol,
            Err(error) => {
                self.runtime.emit(BacktestRemoteFillProgress::BatchFailed {
                    batch_number: self.batch_index + 1,
                    total_batches: self.total_batches,
                    requested_range: range,
                    symbols: symbols.clone(),
                    error: error.to_string(),
                });
                return Err(error);
            }
        };
        Ok(MinuteKlineFillBatchReport {
            batch_index: self.batch_index,
            start_ns: range.0,
            end_ns: range.1,
            symbols,
            elapsed: started.elapsed(),
            rows_by_symbol,
        })
    }
}

async fn fill_minute_kline_batch(
    auth: &Auth,
    cache: &MinuteKlineCache,
    snapshot: &MinuteKlineCacheSnapshot,
    market_kind: BacktestMarketKind,
    batch: &MinuteKlineFillBatch,
    runtime: RemoteBacktestFillRuntime,
) -> Result<BTreeMap<String, usize>> {
    let mut attempt = 1usize;
    loop {
        if runtime.is_cancelled() {
            return Err(minute_fill_cancelled_error());
        }
        let result =
            collect_server_minutes_batch(auth, market_kind, &batch.requests, &runtime).await;
        match result {
            Ok(rows) => {
                if runtime.is_cancelled() {
                    return Err(minute_fill_cancelled_error());
                }
                let mut rows_by_symbol = BTreeMap::new();
                for request in &batch.requests {
                    let rows = rows
                        .get(request.symbol.as_str())
                        .cloned()
                        .unwrap_or_default();
                    cache.store_final_range(
                        &request.symbol,
                        request.start_ns,
                        request.end_ns,
                        snapshot,
                        rows.as_slice(),
                    )?;
                    rows_by_symbol.insert(request.symbol.clone(), rows.len());
                }
                return Ok(rows_by_symbol);
            }
            Err(error)
                if attempt < REMOTE_MINUTE_CONNECT_RETRY_ATTEMPTS
                    && should_retry_minute_fill(&error) =>
            {
                tokio::time::sleep(minute_retry_delay(attempt)).await;
                attempt = attempt.saturating_add(1);
            }
            Err(error) => return Err(error),
        }
    }
}

async fn collect_server_minutes_batch(
    auth: &Auth,
    market_kind: BacktestMarketKind,
    requests: &[BacktestMinuteKlineFillRequest],
    runtime: &RemoteBacktestFillRuntime,
) -> Result<BTreeMap<String, Vec<Kline>>> {
    let first = requests
        .first()
        .ok_or_else(|| data_validation("canonical-minute fill batch is empty"))?;
    if requests
        .iter()
        .any(|request| request.start_ns != first.start_ns || request.end_ns != first.end_ns)
    {
        return Err(data_validation(
            "canonical-minute fill batch mixes requested time ranges",
        ));
    }
    let builder = tqsdk_wait::TqApiBuilder::new(auth.user.clone(), auth.pass.clone());
    let builder = match market_kind {
        BacktestMarketKind::Futures => builder.futures_backtest(first.start_ns, first.end_ns)?,
        BacktestMarketKind::Stock => builder.stock_backtest(first.start_ns, first.end_ns)?,
    };
    let mut api = builder.backtest_cache_fill_mode().build().await?;
    let ready_deadline = tokio::time::Instant::now() + REMOTE_MINUTE_KLINE_READY_TIMEOUT;
    let mut handles = BTreeMap::new();
    for request in requests {
        if runtime.is_cancelled() {
            return Err(minute_fill_cancelled_error());
        }
        let handle = api
            .kline(
                &request.symbol,
                Duration::from_nanos(MINUTE_KLINE_DURATION_NS as u64),
                REMOTE_MINUTE_KLINE_DATA_LENGTH,
            )
            .await?;
        handles.insert(request.symbol.clone(), handle);
    }
    let mut rows_by_symbol = BTreeMap::<String, BTreeMap<i64, Kline>>::new();
    let mut ready_symbols = BTreeSet::new();
    loop {
        if runtime.is_cancelled() {
            return Err(minute_fill_cancelled_error());
        }
        let step = api.step_until(Some(ready_deadline)).await?;
        let Some(step) = step else {
            if ready_symbols.len() == requests.len() && server_history_batch_complete(&api) {
                break;
            }
            if tokio::time::Instant::now() >= ready_deadline {
                if ready_symbols.len() == requests.len() {
                    return Err(data_validation(
                        "server-side 60-second Kline history initialization did not complete",
                    ));
                }
                let missing = requests
                    .iter()
                    .filter(|request| !ready_symbols.contains(request.symbol.as_str()))
                    .map(|request| request.symbol.as_str())
                    .collect::<Vec<_>>();
                return Err(data_validation(format!(
                    "server-side 60-second Kline chart did not become ready for {}",
                    missing.join(",")
                )));
            }
            continue;
        };
        for request in requests {
            let handle = handles
                .get(request.symbol.as_str())
                .ok_or_else(|| data_validation("canonical-minute Kline handle is missing"))?;
            if !handle.is_ready()? {
                continue;
            }
            let rows = rows_by_symbol.entry(request.symbol.clone()).or_default();
            if ready_symbols.insert(request.symbol.clone()) {
                accept_rows(rows, handle.rows()?, request);
            }
            accept_rows(rows, handle.changed_rows(&step)?, request);
        }
        if ready_symbols.len() == requests.len() && server_history_batch_complete(&api) {
            break;
        }
    }
    Ok(rows_by_symbol
        .into_iter()
        .map(|(symbol, rows)| (symbol, rows.into_values().collect()))
        .collect())
}

fn server_history_batch_complete(api: &tqsdk_wait::TqApi) -> bool {
    api.session()
        .reader()
        .read_market_state()
        .get_path(&["mdhis_more_data"])
        .and_then(|value| value.as_bool())
        == Some(false)
}

fn minute_fill_batches(
    requests: Vec<BacktestMinuteKlineFillRequest>,
    symbol_batch_size: usize,
) -> Result<VecDeque<MinuteKlineFillBatch>> {
    let mut grouped =
        BTreeMap::<(i64, i64), BTreeMap<String, BacktestMinuteKlineFillRequest>>::new();
    for request in requests {
        validate_request(&request)?;
        grouped
            .entry((request.start_ns, request.end_ns))
            .or_default()
            .insert(request.symbol.clone(), request);
    }
    let mut batches = VecDeque::new();
    let symbol_batch_size = symbol_batch_size.max(1);
    for ((start_ns, end_ns), requests) in grouped {
        let requests = requests.into_values().collect::<Vec<_>>();
        for requests in requests.chunks(symbol_batch_size) {
            batches.push_back(MinuteKlineFillBatch {
                start_ns,
                end_ns,
                requests: requests.to_vec(),
            });
        }
    }
    Ok(batches)
}

fn split_minute_fill_requests(
    requests: Vec<BacktestMinuteKlineFillRequest>,
    config: crate::backtest_remote::BacktestRemoteFillConfig,
) -> Result<Vec<BacktestMinuteKlineFillRequest>> {
    let slice_ns = config
        .slice
        .and_then(|slice| i64::try_from(slice.as_nanos()).ok())
        .filter(|slice| *slice > 0)
        .map_or(REMOTE_MINUTE_KLINE_MAX_SPAN_NS, |slice| {
            slice.min(REMOTE_MINUTE_KLINE_MAX_SPAN_NS)
        });
    let mut slices = Vec::new();
    for request in requests {
        validate_request(&request)?;
        let mut start_ns = request.start_ns;
        while start_ns < request.end_ns {
            let end_ns = start_ns.saturating_add(slice_ns).min(request.end_ns);
            if end_ns <= start_ns {
                return Err(data_validation(
                    "canonical-minute fill slice did not advance",
                ));
            }
            slices.push(BacktestMinuteKlineFillRequest::new(
                request.symbol.clone(),
                start_ns,
                end_ns,
            ));
            start_ns = end_ns;
        }
    }
    Ok(slices)
}

fn accept_rows(
    target: &mut BTreeMap<i64, Kline>,
    rows: Vec<Kline>,
    request: &BacktestMinuteKlineFillRequest,
) {
    for row in rows {
        if row.datetime >= request.start_ns && row.datetime < request.end_ns {
            target.insert(row.datetime, row);
        }
    }
}

fn validate_request(request: &BacktestMinuteKlineFillRequest) -> Result<()> {
    if request.symbol.is_empty() {
        return Err(data_validation(
            "remote canonical minute cache fill symbol is empty",
        ));
    }
    if request.start_ns >= request.end_ns {
        return Err(data_validation(format!(
            "remote canonical minute cache fill range is invalid for {}: [{}, {})",
            request.symbol, request.start_ns, request.end_ns
        )));
    }
    Ok(())
}

fn minute_fill_cancelled_error() -> crate::Error {
    data_validation(
        "remote canonical-minute cache fill cancelled before final coverage was committed",
    )
}

fn should_retry_minute_fill(error: &crate::Error) -> bool {
    let error = error.to_string().to_ascii_lowercase();
    [
        "connection",
        "connect",
        "timeout",
        "timed out",
        "token",
        "endpoint",
        "temporar",
        "transport",
        "dns",
    ]
    .iter()
    .any(|needle| error.contains(needle))
}

fn minute_retry_delay(attempt: usize) -> Duration {
    Duration::from_millis(
        u64::try_from(attempt)
            .unwrap_or(u64::MAX)
            .saturating_mul(250)
            .min(2_000),
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        BacktestMinuteKlineFillRequest, REMOTE_MINUTE_KLINE_MAX_SPAN_NS, minute_fill_batches,
        split_minute_fill_requests, validate_request,
    };
    use crate::backtest_remote::BacktestRemoteFillConfig;

    #[test]
    fn minute_fill_request_rejects_empty_symbol_and_invalid_range() {
        assert!(validate_request(&BacktestMinuteKlineFillRequest::new("", 1, 2)).is_err());
        assert!(
            validate_request(&BacktestMinuteKlineFillRequest::new("SHFE.rb2601", 2, 2)).is_err()
        );
        assert!(
            validate_request(&BacktestMinuteKlineFillRequest::new("SHFE.rb2601", 1, 2)).is_ok()
        );
    }

    #[test]
    fn minute_fill_batches_only_group_equal_ranges_and_deduplicate_symbols() {
        let batches = minute_fill_batches(
            vec![
                BacktestMinuteKlineFillRequest::new("SHFE.rb2601", 1_000, 2_000),
                BacktestMinuteKlineFillRequest::new("DCE.m2601", 1_000, 2_000),
                BacktestMinuteKlineFillRequest::new("SHFE.rb2601", 3_000, 4_000),
                BacktestMinuteKlineFillRequest::new("SHFE.rb2601", 1_000, 2_000),
            ],
            2,
        )
        .unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].start_ns, 1_000);
        assert_eq!(batches[0].end_ns, 2_000);
        assert_eq!(
            batches[0]
                .requests
                .iter()
                .map(|request| request.symbol.as_str())
                .collect::<Vec<_>>(),
            vec!["DCE.m2601", "SHFE.rb2601"]
        );
        assert_eq!(batches[1].start_ns, 3_000);
        assert_eq!(batches[1].end_ns, 4_000);
    }

    #[test]
    fn minute_fill_respects_explicit_slice_fallback() {
        let config =
            BacktestRemoteFillConfig::default().with_slice(Some(Duration::from_nanos(2_000)));
        let requests = split_minute_fill_requests(
            vec![BacktestMinuteKlineFillRequest::new(
                "SHFE.rb2601",
                1_000,
                6_000,
            )],
            config,
        )
        .unwrap();

        assert_eq!(
            requests
                .iter()
                .map(|request| (request.start_ns, request.end_ns))
                .collect::<Vec<_>>(),
            vec![(1_000, 3_000), (3_000, 5_000), (5_000, 6_000)]
        );
    }

    #[test]
    fn minute_fill_defaults_to_server_chart_capacity() {
        let max_span = REMOTE_MINUTE_KLINE_MAX_SPAN_NS;
        let requests = split_minute_fill_requests(
            vec![BacktestMinuteKlineFillRequest::new(
                "SHFE.rb2601",
                0,
                max_span * 2 + 1,
            )],
            BacktestRemoteFillConfig::default(),
        )
        .unwrap();

        assert_eq!(
            requests
                .iter()
                .map(|request| (request.start_ns, request.end_ns))
                .collect::<Vec<_>>(),
            vec![
                (0, max_span),
                (max_span, max_span * 2),
                (max_span * 2, max_span * 2 + 1)
            ]
        );
    }

    #[test]
    fn minute_fill_caps_explicit_slice_at_server_chart_capacity() {
        let max_span = REMOTE_MINUTE_KLINE_MAX_SPAN_NS;
        let requests = split_minute_fill_requests(
            vec![BacktestMinuteKlineFillRequest::new(
                "SHFE.rb2601",
                0,
                max_span + 1,
            )],
            BacktestRemoteFillConfig::default().with_slice(Some(Duration::from_nanos(
                u64::try_from(max_span + 1).unwrap(),
            ))),
        )
        .unwrap();

        assert_eq!(
            requests
                .iter()
                .map(|request| (request.start_ns, request.end_ns))
                .collect::<Vec<_>>(),
            vec![(0, max_span), (max_span, max_span + 1)]
        );
    }
}
