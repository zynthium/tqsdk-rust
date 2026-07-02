use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use tqsdk_core::Tick;
use tqsdk_data::{BacktestTickCache, BacktestTickFill, DataError};
use tqsdk_task::ReplayMarketEvent;

use crate::{Result, data_validation};

const REMOTE_TICK_DATA_LENGTH: usize = 10_000;
const REMOTE_FILL_END_TOLERANCE_NS: i64 = 1_000_000_000;
const REMOTE_STEP_POLL_TIMEOUT: Duration = Duration::from_secs(5);
const REMOTE_FILL_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const REMOTE_FILL_SYMBOL_BATCH_SIZE: usize = 1;
const REMOTE_FILL_SYMBOL_BATCH_SIZE_MAX: usize = 4;
const REMOTE_FILL_SYMBOL_CONCURRENCY: usize = 2;
const REMOTE_FILL_SYMBOL_CONCURRENCY_MAX: usize = 4;
const REMOTE_CONNECT_RETRY_ATTEMPTS: usize = 5;
const REMOTE_TICK_WRITE_BUFFER_ROWS: usize = 8_192;

pub(crate) struct RemoteBacktestCachingStream {
    api: tqsdk_wait::TqApi,
    handles: BTreeMap<String, tqsdk_wait::TickHandle>,
    cache: BacktestTickCache,
    fills: BTreeMap<String, BacktestTickFill>,
    write_buffer: RemoteTickWriteBuffer,
    pending: VecDeque<ReplayMarketEvent>,
    range_start_ns: i64,
    range_end_ns: i64,
    accepted_rows_total: usize,
    last_progress: tokio::time::Instant,
    finalized: bool,
}

pub(crate) struct RemoteBacktestCacheFillReport {
    pub(crate) rows_by_symbol: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Copy)]
enum FinalizeMode {
    Strict,
    Idle,
}

#[derive(Debug)]
struct RemoteTickWriteBuffer {
    threshold_rows: usize,
    rows_by_symbol: BTreeMap<String, Vec<Tick>>,
}

impl Default for RemoteTickWriteBuffer {
    fn default() -> Self {
        Self::new(REMOTE_TICK_WRITE_BUFFER_ROWS)
    }
}

impl RemoteTickWriteBuffer {
    fn new(threshold_rows: usize) -> Self {
        Self {
            threshold_rows: threshold_rows.max(1),
            rows_by_symbol: BTreeMap::new(),
        }
    }

    fn push_rows(
        &mut self,
        cache: &BacktestTickCache,
        symbol: &str,
        rows: Vec<Tick>,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let buffered_rows = {
            let buffer = self.rows_by_symbol.entry(symbol.to_string()).or_default();
            buffer.extend(rows);
            buffer.len()
        };
        if buffered_rows >= self.threshold_rows {
            self.flush_symbol(cache, symbol)?;
        }
        Ok(())
    }

    fn flush_symbol(&mut self, cache: &BacktestTickCache, symbol: &str) -> Result<()> {
        let Some(rows) = self.rows_by_symbol.remove(symbol) else {
            return Ok(());
        };
        if rows.is_empty() {
            return Ok(());
        }
        cache.append_partial_ticks(symbol, rows)?;
        Ok(())
    }

    fn flush_all(&mut self, cache: &BacktestTickCache) -> Result<()> {
        let symbols = self.rows_by_symbol.keys().cloned().collect::<Vec<_>>();
        for symbol in symbols {
            self.flush_symbol(cache, symbol.as_str())?;
        }
        Ok(())
    }
}

pub(crate) async fn fill_backtest_tick_cache(
    user: String,
    pass: String,
    start_ns: i64,
    end_ns: i64,
    symbols: Vec<String>,
    cache: BacktestTickCache,
) -> Result<RemoteBacktestCacheFillReport> {
    let requested_symbol_count = symbols.len();
    let mut pending_batches = symbols
        .chunks(remote_fill_symbol_batch_size())
        .map(<[String]>::to_vec)
        .collect::<VecDeque<_>>();
    let max_concurrency = remote_fill_symbol_concurrency();
    let mut tasks = tokio::task::JoinSet::new();
    let mut rows_by_symbol = BTreeMap::new();

    while !pending_batches.is_empty() || !tasks.is_empty() {
        while tasks.len() < max_concurrency {
            let Some(symbol_batch) = pending_batches.pop_front() else {
                break;
            };
            tasks.spawn(fill_backtest_tick_cache_symbol_batch(
                user.clone(),
                pass.clone(),
                start_ns,
                end_ns,
                symbol_batch,
                cache.clone(),
            ));
        }

        let Some(result) = tasks.join_next().await else {
            continue;
        };
        let fill_report = result.map_err(|error| {
            data_validation(format!("remote backtest cache fill task failed: {error}"))
        })??;
        for (symbol, rows) in fill_report.rows_by_symbol {
            *rows_by_symbol.entry(symbol).or_insert(0) += rows;
        }
    }
    let accepted_rows_total = rows_by_symbol.values().copied().sum();
    if should_reject_empty_remote_fill(
        requested_symbol_count,
        accepted_rows_total,
        remote_fill_allow_empty_idle(),
    ) {
        return Err(data_validation(format!(
            "remote backtest cache fill completed without accepted ticks for {requested_symbol_count} symbols in range [{start_ns}, {end_ns}); refusing to mark complete empty coverage"
        )));
    }
    Ok(RemoteBacktestCacheFillReport { rows_by_symbol })
}

async fn fill_backtest_tick_cache_symbol_batch(
    user: String,
    pass: String,
    start_ns: i64,
    end_ns: i64,
    symbols: Vec<String>,
    cache: BacktestTickCache,
) -> Result<RemoteBacktestCacheFillReport> {
    let result = fill_backtest_tick_cache_symbol_batch_once(
        user.clone(),
        pass.clone(),
        start_ns,
        end_ns,
        symbols.clone(),
        cache.clone(),
    )
    .await;
    if !matches!(
        result.as_ref(),
        Err(error) if should_split_empty_idle_batch(error, symbols.len())
    ) {
        return result;
    }

    let mut rows_by_symbol = BTreeMap::new();
    for symbol in symbols {
        let fill_report = fill_backtest_tick_cache_symbol_batch_once(
            user.clone(),
            pass.clone(),
            start_ns,
            end_ns,
            vec![symbol],
            cache.clone(),
        )
        .await?;
        for (symbol, rows) in fill_report.rows_by_symbol {
            *rows_by_symbol.entry(symbol).or_insert(0) += rows;
        }
    }

    Ok(RemoteBacktestCacheFillReport { rows_by_symbol })
}

async fn fill_backtest_tick_cache_symbol_batch_once(
    user: String,
    pass: String,
    start_ns: i64,
    end_ns: i64,
    symbols: Vec<String>,
    cache: BacktestTickCache,
) -> Result<RemoteBacktestCacheFillReport> {
    let mut attempt = 1usize;
    loop {
        let result = fill_backtest_tick_cache_symbol_batch_attempt(
            user.clone(),
            pass.clone(),
            start_ns,
            end_ns,
            symbols.clone(),
            cache.clone(),
        )
        .await;
        match result {
            Ok(report) => return Ok(report),
            Err(error)
                if attempt < REMOTE_CONNECT_RETRY_ATTEMPTS
                    && should_retry_remote_fill_attempt_error(&error) =>
            {
                tokio::time::sleep(remote_connect_retry_delay(attempt)).await;
                attempt = attempt.saturating_add(1);
            }
            Err(error) => return Err(error),
        }
    }
}

async fn fill_backtest_tick_cache_symbol_batch_attempt(
    user: String,
    pass: String,
    start_ns: i64,
    end_ns: i64,
    symbols: Vec<String>,
    cache: BacktestTickCache,
) -> Result<RemoteBacktestCacheFillReport> {
    let mut rows_by_symbol = BTreeMap::new();
    for (slice_start_ns, slice_end_ns) in remote_fill_ranges(start_ns, end_ns) {
        let mut stream = connect_remote_backtest_caching_stream(
            user.clone(),
            pass.clone(),
            slice_start_ns,
            slice_end_ns,
            symbols.clone(),
            cache.clone(),
        )
        .await
        .map_err(|error| remote_slice_error(slice_start_ns, slice_end_ns, error))?;
        while let Some(event) = stream
            .next_remote_event()
            .await
            .map_err(|error| remote_slice_error(slice_start_ns, slice_end_ns, error))?
        {
            *rows_by_symbol
                .entry(event.symbol().to_string())
                .or_insert(0) += 1;
        }
    }
    Ok(RemoteBacktestCacheFillReport { rows_by_symbol })
}

async fn connect_remote_backtest_caching_stream(
    user: String,
    pass: String,
    start_ns: i64,
    end_ns: i64,
    symbols: Vec<String>,
    cache: BacktestTickCache,
) -> Result<RemoteBacktestCachingStream> {
    let mut attempt = 1usize;
    loop {
        let result = RemoteBacktestCachingStream::connect(
            user.clone(),
            pass.clone(),
            start_ns,
            end_ns,
            symbols.clone(),
            cache.clone(),
        )
        .await;
        match result {
            Ok(stream) => return Ok(stream),
            Err(error)
                if attempt < REMOTE_CONNECT_RETRY_ATTEMPTS
                    && should_retry_remote_connect_error(&error) =>
            {
                tokio::time::sleep(remote_connect_retry_delay(attempt)).await;
                attempt = attempt.saturating_add(1);
            }
            Err(error) => return Err(error),
        }
    }
}

fn remote_connect_retry_delay(attempt: usize) -> Duration {
    Duration::from_secs((attempt as u64).saturating_mul(2))
}

fn remote_fill_ranges(start_ns: i64, end_ns: i64) -> Vec<(i64, i64)> {
    remote_fill_ranges_for_slice_ns(start_ns, end_ns, remote_fill_slice_ns())
}

fn remote_fill_ranges_for_slice_ns(
    start_ns: i64,
    end_ns: i64,
    slice_ns: Option<i64>,
) -> Vec<(i64, i64)> {
    match slice_ns {
        Some(slice_ns) => remote_fill_ranges_with_slice_ns(start_ns, end_ns, slice_ns),
        None => vec![(start_ns, end_ns)],
    }
}

fn remote_fill_ranges_with_slice_ns(start_ns: i64, end_ns: i64, slice_ns: i64) -> Vec<(i64, i64)> {
    let mut ranges = Vec::new();
    let mut cursor = start_ns;
    while cursor < end_ns {
        let next = cursor.saturating_add(slice_ns).min(end_ns);
        ranges.push((cursor, next));
        cursor = next;
    }
    ranges
}

fn remote_slice_error(slice_start_ns: i64, slice_end_ns: i64, error: crate::Error) -> crate::Error {
    data_validation(format!(
        "remote backtest cache fill failed for slice [{slice_start_ns}, {slice_end_ns}): {error}"
    ))
}

impl RemoteBacktestCachingStream {
    pub(crate) async fn connect(
        user: String,
        pass: String,
        start_ns: i64,
        end_ns: i64,
        symbols: Vec<String>,
        cache: BacktestTickCache,
    ) -> Result<Self> {
        let mut api = tqsdk_wait::TqApiBuilder::new(user, pass)
            .futures_backtest(start_ns, end_ns)?
            .backtest_cache_fill_mode()
            .build()
            .await?;
        let mut handles = BTreeMap::new();
        let mut fills = BTreeMap::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        for symbol in symbols {
            let handle = api
                .tick_ready(&symbol, REMOTE_TICK_DATA_LENGTH, Some(deadline))
                .await?;
            fills.insert(
                symbol.clone(),
                BacktestTickFill::new(symbol.clone(), start_ns, end_ns),
            );
            handles.insert(symbol, handle);
        }
        Ok(Self {
            api,
            handles,
            cache,
            fills,
            write_buffer: RemoteTickWriteBuffer::default(),
            pending: VecDeque::new(),
            range_start_ns: start_ns,
            range_end_ns: end_ns,
            accepted_rows_total: 0,
            last_progress: tokio::time::Instant::now(),
            finalized: false,
        })
    }

    async fn next_remote_event(&mut self) -> Result<Option<ReplayMarketEvent>> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Ok(Some(event));
            }
            if self.fills_complete()? {
                self.finalize_cache(FinalizeMode::Strict)?;
                return Ok(None);
            }
            if self.last_progress.elapsed() >= remote_fill_idle_timeout() {
                if self
                    .poll_remote_step_until(tokio::time::Instant::now())
                    .await?
                {
                    continue;
                }
                // Closed-session tails can legitimately idle before the requested slice end.
                let unconfirmed_incomplete_idle_symbols =
                    self.unconfirmed_incomplete_idle_symbols()?;
                if should_reject_incomplete_idle_finalize(
                    !unconfirmed_incomplete_idle_symbols.is_empty(),
                ) {
                    return Err(data_validation(format!(
                        "remote backtest cache fill idled before tick ranges were confirmed \
                         for {} symbols ({}) in range [{}, {}); refusing to mark complete partial coverage",
                        unconfirmed_incomplete_idle_symbols.len(),
                        unconfirmed_incomplete_idle_symbols.join(","),
                        self.range_start_ns,
                        self.range_end_ns
                    )));
                }

                let unconfirmed_empty_idle_symbols = self.unconfirmed_empty_idle_symbols()?;
                if should_reject_empty_idle_finalize(
                    remote_fill_allow_empty_idle(),
                    !unconfirmed_empty_idle_symbols.is_empty(),
                ) {
                    return Err(data_validation(format!(
                        "remote backtest cache fill idled before empty tick ranges were confirmed \
                         for {} symbols ({}) in range [{}, {}); refusing to mark complete empty coverage",
                        unconfirmed_empty_idle_symbols.len(),
                        unconfirmed_empty_idle_symbols.join(","),
                        self.range_start_ns,
                        self.range_end_ns
                    )));
                }
                self.finalize_cache(FinalizeMode::Idle)?;
                return Ok(None);
            }

            let deadline = tokio::time::Instant::now() + REMOTE_STEP_POLL_TIMEOUT;
            if !self.poll_remote_step_until(deadline).await? {
                continue;
            }
        }
    }

    async fn poll_remote_step_until(&mut self, deadline: tokio::time::Instant) -> Result<bool> {
        let Some(step) = self.api.step_until(Some(deadline)).await? else {
            return Ok(false);
        };
        self.process_remote_step(&step)?;
        Ok(true)
    }

    fn process_remote_step(&mut self, step: &tqsdk_wait::WaitStep) -> Result<()> {
        let mut made_progress = false;
        for (symbol, handle) in &self.handles {
            if !step.is_changing(handle) {
                continue;
            }

            let mut accepted_rows = Vec::new();
            let mut accepted_events = Vec::new();
            for row in handle.changed_rows(step)? {
                let Some(fill) = self.fills.get_mut(symbol) else {
                    continue;
                };
                if !fill.push(row.clone())? {
                    continue;
                }

                accepted_events.push(ReplayMarketEvent::tick(
                    "server-backtest",
                    symbol,
                    row.datetime,
                    Some(row.datetime),
                    row.clone(),
                )?);
                accepted_rows.push(row);
            }

            if !accepted_rows.is_empty() {
                self.accepted_rows_total =
                    self.accepted_rows_total.saturating_add(accepted_rows.len());
                self.write_buffer
                    .push_rows(&self.cache, symbol, accepted_rows)?;
                made_progress = true;
                self.pending.extend(accepted_events);
            }
        }

        if made_progress {
            self.last_progress = tokio::time::Instant::now();
        }
        Ok(())
    }

    fn unconfirmed_incomplete_idle_symbols(&self) -> Result<Vec<String>> {
        let mut symbols = Vec::new();
        for (symbol, fill) in &self.fills {
            let report = fill.finish(REMOTE_FILL_END_TOLERANCE_NS)?;
            if report.complete || report.unique_rows == 0 {
                continue;
            }
            let Some(handle) = self.handles.get(symbol) else {
                symbols.push(symbol.clone());
                continue;
            };
            if self.api.backtest_tick_serial_exhausted(handle) != Some(true) {
                symbols.push(symbol.clone());
            }
        }
        symbols.sort();
        Ok(symbols)
    }

    fn unconfirmed_empty_idle_symbols(&self) -> Result<Vec<String>> {
        let mut symbols = Vec::new();
        for (symbol, fill) in &self.fills {
            if fill
                .finish_after_idle(REMOTE_FILL_END_TOLERANCE_NS)?
                .unique_rows
                != 0
            {
                continue;
            }
            let Some(handle) = self.handles.get(symbol) else {
                symbols.push(symbol.clone());
                continue;
            };
            if self.api.backtest_tick_serial_exhausted(handle) != Some(true) {
                symbols.push(symbol.clone());
            }
        }
        symbols.sort();
        Ok(symbols)
    }

    fn fills_complete(&self) -> Result<bool> {
        for fill in self.fills.values() {
            if !fill.finish(REMOTE_FILL_END_TOLERANCE_NS)?.complete {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn finalize_cache(&mut self, mode: FinalizeMode) -> Result<()> {
        if self.finalized {
            return Ok(());
        }
        self.write_buffer.flush_all(&self.cache)?;
        for (symbol, fill) in &self.fills {
            let report = match mode {
                FinalizeMode::Strict => fill.finish(REMOTE_FILL_END_TOLERANCE_NS)?,
                FinalizeMode::Idle => fill.finish_after_idle(REMOTE_FILL_END_TOLERANCE_NS)?,
            };
            if !report.complete {
                return Err(data_validation(format!(
                    "incomplete remote backtest cache fill for {symbol}: {:?}",
                    report.gap_summary
                )));
            }
            self.cache.mark_complete(
                symbol,
                report.requested_range.0,
                report.requested_range.1,
                report.unique_rows,
                report.id_range,
            )?;
            self.cache.compact_symbol_ticks(symbol)?;
        }
        self.finalized = true;
        Ok(())
    }
}

fn remote_fill_idle_timeout() -> Duration {
    let value = std::env::var("TQSDK_REMOTE_FILL_IDLE_TIMEOUT_SECS").ok();
    parse_remote_fill_idle_timeout(value.as_deref())
}

fn remote_fill_allow_empty_idle() -> bool {
    let value = std::env::var("TQSDK_REMOTE_FILL_ALLOW_EMPTY_IDLE").ok();
    parse_remote_fill_allow_empty_idle(value.as_deref())
}

fn remote_fill_symbol_batch_size() -> usize {
    let value = std::env::var("TQSDK_REMOTE_FILL_SYMBOL_BATCH_SIZE").ok();
    parse_remote_fill_symbol_batch_size(value.as_deref())
}

fn remote_fill_symbol_concurrency() -> usize {
    let value = std::env::var("TQSDK_REMOTE_FILL_SYMBOL_CONCURRENCY").ok();
    parse_remote_fill_symbol_concurrency(value.as_deref())
}

fn remote_fill_slice_ns() -> Option<i64> {
    let value = std::env::var("TQSDK_REMOTE_FILL_SLICE_SECS").ok();
    parse_remote_fill_slice_ns(value.as_deref())
}

fn parse_remote_fill_idle_timeout(value: Option<&str>) -> Duration {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(REMOTE_FILL_IDLE_TIMEOUT)
}

fn parse_remote_fill_allow_empty_idle(value: Option<&str>) -> bool {
    matches!(
        value.map(str::trim).map(str::to_ascii_lowercase).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

fn parse_remote_fill_symbol_batch_size(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|batch_size| *batch_size > 0)
        .unwrap_or(REMOTE_FILL_SYMBOL_BATCH_SIZE)
        .min(REMOTE_FILL_SYMBOL_BATCH_SIZE_MAX)
}

fn parse_remote_fill_symbol_concurrency(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|concurrency| *concurrency > 0)
        .unwrap_or(REMOTE_FILL_SYMBOL_CONCURRENCY)
        .min(REMOTE_FILL_SYMBOL_CONCURRENCY_MAX)
}

fn parse_remote_fill_slice_ns(value: Option<&str>) -> Option<i64> {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .and_then(|secs| secs.checked_mul(1_000_000_000))
        .and_then(|ns| i64::try_from(ns).ok())
        .filter(|ns| *ns > 0)
}

fn should_reject_empty_idle_finalize(
    allow_empty_idle: bool,
    has_unconfirmed_empty_idle_symbols: bool,
) -> bool {
    has_unconfirmed_empty_idle_symbols && !allow_empty_idle
}

fn should_reject_incomplete_idle_finalize(has_unconfirmed_incomplete_idle_symbols: bool) -> bool {
    has_unconfirmed_incomplete_idle_symbols
}

fn should_reject_empty_remote_fill(
    symbol_count: usize,
    accepted_rows_total: usize,
    allow_empty_idle: bool,
) -> bool {
    symbol_count > 1 && accepted_rows_total == 0 && !allow_empty_idle
}

fn should_split_empty_idle_batch(error: &crate::Error, symbol_count: usize) -> bool {
    symbol_count > 1
        && matches!(
            error,
            crate::Error::Data(data)
                if matches!(
                    &**data,
                    DataError::Validation(message)
                        if is_remote_fill_idle_error_message(message)
                )
        )
}

fn should_retry_remote_fill_attempt_error(error: &crate::Error) -> bool {
    should_retry_remote_connect_error(error)
        || matches!(
            error,
            crate::Error::Data(data)
                if matches!(
                    &**data,
                    DataError::Validation(message) if is_remote_fill_idle_error_message(message)
                )
        )
}

fn is_remote_fill_idle_error_message(message: &str) -> bool {
    message.contains("remote backtest cache fill idled without accepted ticks")
        || message
            .contains("remote backtest cache fill idled before empty tick ranges were confirmed")
        || message.contains("remote backtest cache fill idled before tick ranges were confirmed")
}

fn should_retry_remote_connect_error(error: &crate::Error) -> bool {
    matches!(
        error,
        crate::Error::Data(data)
            if matches!(
                &**data,
                DataError::Validation(message)
                    if (message.contains("token request failed")
                        || message.contains("market endpoint request failed"))
                        && message.contains("error sending request")
            )
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        REMOTE_FILL_IDLE_TIMEOUT, RemoteTickWriteBuffer, parse_remote_fill_allow_empty_idle,
        parse_remote_fill_idle_timeout, parse_remote_fill_slice_ns,
        parse_remote_fill_symbol_batch_size, parse_remote_fill_symbol_concurrency,
        remote_fill_ranges_for_slice_ns, remote_fill_ranges_with_slice_ns,
        should_reject_empty_idle_finalize, should_reject_empty_remote_fill,
        should_reject_incomplete_idle_finalize, should_retry_remote_connect_error,
        should_retry_remote_fill_attempt_error, should_split_empty_idle_batch,
    };
    use tqsdk_core::Tick;
    use tqsdk_data::{BacktestTickCache, TickDataSeriesRequest};

    #[test]
    fn remote_fill_ranges_default_to_single_python_style_backtest_session() {
        let start_ns = 1_781_182_800_000_000_000;
        let end_ns = start_ns + 48 * 60 * 60 * 1_000_000_000;

        let ranges = remote_fill_ranges_for_slice_ns(start_ns, end_ns, None);

        assert_eq!(ranges, vec![(start_ns, end_ns)]);
    }

    #[test]
    fn remote_fill_ranges_can_split_long_requests_for_fallback() {
        let start_ns = 1_781_182_800_000_000_000;
        let two_hours_ns = 2 * 60 * 60 * 1_000_000_000;
        let end_ns = start_ns + 3 * two_hours_ns;

        let ranges = remote_fill_ranges_with_slice_ns(start_ns, end_ns, two_hours_ns);

        assert_eq!(
            ranges,
            vec![
                (start_ns, start_ns + two_hours_ns),
                (start_ns + two_hours_ns, start_ns + 2 * two_hours_ns),
                (start_ns + 2 * two_hours_ns, end_ns),
            ]
        );
    }

    #[test]
    fn remote_tick_write_buffer_batches_cache_appends() {
        let symbol = "SHFE.rb2601";
        let cache_dir = temp_cache_dir("remote-tick-write-buffer");
        let _ = std::fs::remove_dir_all(&cache_dir);
        let cache = BacktestTickCache::open(&cache_dir).unwrap();
        let mut buffer = RemoteTickWriteBuffer::new(2);

        buffer
            .push_rows(&cache, symbol, vec![tick(1, 1_000, 100.0)])
            .unwrap();
        assert!(!cache.tick_series_path(symbol).exists());

        buffer
            .push_rows(&cache, symbol, vec![tick(2, 2_000, 101.0)])
            .unwrap();
        assert!(cache.tick_series_path(symbol).exists());
        cache
            .mark_complete(symbol, 1_000, 3_000, 2, Some((1, 3)))
            .unwrap();
        let first_batch = cache
            .load_series(TickDataSeriesRequest::new(symbol, 1_000, 3_000))
            .unwrap();
        assert_eq!(
            first_batch.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec![1, 2]
        );

        buffer
            .push_rows(&cache, symbol, vec![tick(3, 3_000, 102.0)])
            .unwrap();
        buffer.flush_all(&cache).unwrap();
        cache
            .mark_complete(symbol, 3_000, 4_000, 1, Some((3, 4)))
            .unwrap();
        let all_rows = cache
            .load_series(TickDataSeriesRequest::new(symbol, 1_000, 4_000))
            .unwrap();
        assert_eq!(
            all_rows.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );

        let _ = std::fs::remove_dir_all(&cache_dir);
    }

    #[test]
    fn remote_fill_idle_timeout_can_be_overridden_for_validation() {
        assert_eq!(
            parse_remote_fill_idle_timeout(Some("5")),
            Duration::from_secs(5)
        );
        assert_eq!(
            parse_remote_fill_idle_timeout(Some("invalid")),
            REMOTE_FILL_IDLE_TIMEOUT
        );
        assert_eq!(
            parse_remote_fill_idle_timeout(None),
            REMOTE_FILL_IDLE_TIMEOUT
        );
    }

    #[test]
    fn remote_fill_empty_idle_flag_accepts_common_truthy_values() {
        for value in ["1", "true", "TRUE", "yes", "on"] {
            assert!(parse_remote_fill_allow_empty_idle(Some(value)));
        }
        for value in [None, Some("0"), Some("false"), Some("off"), Some("invalid")] {
            assert!(!parse_remote_fill_allow_empty_idle(value));
        }
    }

    #[test]
    fn remote_fill_rejects_unconfirmed_empty_idle_finalize() {
        assert!(should_reject_empty_idle_finalize(false, true));
        assert!(!should_reject_empty_idle_finalize(false, false));
        assert!(!should_reject_empty_idle_finalize(true, true));
    }

    #[test]
    fn remote_fill_rejects_unconfirmed_incomplete_idle_finalize() {
        assert!(should_reject_incomplete_idle_finalize(true));
        assert!(!should_reject_incomplete_idle_finalize(false));
    }

    #[test]
    fn remote_fill_splits_only_multi_symbol_empty_idle_errors() {
        let empty_idle = crate::data_validation(
            "remote backtest cache fill idled without accepted ticks for 4 symbols in range [1, 2)",
        );
        let unconfirmed_empty_idle = crate::data_validation(
            "remote backtest cache fill idled before empty tick ranges were confirmed for 4 symbols (A,B,C,D) in range [1, 2)",
        );
        let unconfirmed_incomplete_idle = crate::data_validation(
            "remote backtest cache fill idled before tick ranges were confirmed for 4 symbols (A,B,C,D) in range [1, 2)",
        );
        let other = crate::data_validation("remote backtest cache fill failed for another reason");

        assert!(should_split_empty_idle_batch(&empty_idle, 4));
        assert!(should_split_empty_idle_batch(&unconfirmed_empty_idle, 4));
        assert!(should_split_empty_idle_batch(
            &unconfirmed_incomplete_idle,
            4
        ));
        assert!(!should_split_empty_idle_batch(&empty_idle, 1));
        assert!(!should_split_empty_idle_batch(&unconfirmed_empty_idle, 1));
        assert!(!should_split_empty_idle_batch(
            &unconfirmed_incomplete_idle,
            1
        ));
        assert!(!should_split_empty_idle_batch(&other, 4));
    }

    #[test]
    fn remote_fill_retries_transient_remote_connect_errors() {
        let token_error = crate::data_validation(
            "auth error: token request failed: error sending request for url (https://auth.example/token)",
        );
        let endpoint_error = crate::data_validation(
            "market endpoint request failed: error sending request for url (https://api.example/ns)",
        );
        let validation_error = crate::data_validation("remote fill rejected empty coverage");

        assert!(should_retry_remote_connect_error(&token_error));
        assert!(should_retry_remote_connect_error(&endpoint_error));
        assert!(!should_retry_remote_connect_error(&validation_error));
    }

    #[test]
    fn remote_fill_retries_transient_attempt_idle_errors() {
        let wrapped_unconfirmed_empty_idle = crate::data_validation(
            "remote backtest cache fill failed for slice [1, 2): invalid data query input: \
             remote backtest cache fill idled before empty tick ranges were confirmed \
             for 1 symbols (KQ.i@SHFE.ao) in range [1, 2); refusing to mark complete empty coverage",
        );
        let wrapped_unconfirmed_incomplete_idle = crate::data_validation(
            "remote backtest cache fill failed for slice [1, 2): invalid data query input: \
             remote backtest cache fill idled before tick ranges were confirmed \
             for 1 symbols (SHFE.ao2609) in range [1, 2); refusing to mark complete partial coverage",
        );
        let other = crate::data_validation("remote fill rejected empty coverage");

        assert!(should_retry_remote_fill_attempt_error(
            &wrapped_unconfirmed_empty_idle
        ));
        assert!(should_retry_remote_fill_attempt_error(
            &wrapped_unconfirmed_incomplete_idle
        ));
        assert!(!should_retry_remote_fill_attempt_error(&other));
    }

    #[test]
    fn remote_fill_symbol_batch_defaults_to_single_symbol_safe_mode() {
        assert_eq!(parse_remote_fill_symbol_batch_size(None), 1);
        assert_eq!(parse_remote_fill_symbol_batch_size(Some("0")), 1);
        assert_eq!(parse_remote_fill_symbol_batch_size(Some("invalid")), 1);
        assert_eq!(parse_remote_fill_symbol_batch_size(Some("2")), 2);
        assert_eq!(parse_remote_fill_symbol_batch_size(Some("128")), 4);
    }

    #[test]
    fn remote_fill_symbol_concurrency_defaults_to_bounded_parallelism() {
        assert_eq!(parse_remote_fill_symbol_concurrency(None), 2);
        assert_eq!(parse_remote_fill_symbol_concurrency(Some("0")), 2);
        assert_eq!(parse_remote_fill_symbol_concurrency(Some("invalid")), 2);
        assert_eq!(parse_remote_fill_symbol_concurrency(Some("1")), 1);
        assert_eq!(parse_remote_fill_symbol_concurrency(Some("2")), 2);
        assert_eq!(parse_remote_fill_symbol_concurrency(Some("8")), 4);
    }

    #[test]
    fn remote_fill_rejects_multi_symbol_empty_overall_fill_by_default() {
        assert!(should_reject_empty_remote_fill(2, 0, false));
        assert!(should_reject_empty_remote_fill(128, 0, false));
        assert!(!should_reject_empty_remote_fill(1, 0, false));
        assert!(!should_reject_empty_remote_fill(2, 1, false));
        assert!(!should_reject_empty_remote_fill(2, 0, true));
    }

    #[test]
    fn remote_fill_slice_can_be_overridden_for_fallback() {
        assert_eq!(
            parse_remote_fill_slice_ns(Some("172800")),
            Some(172_800_000_000_000)
        );
        assert_eq!(parse_remote_fill_slice_ns(Some("0")), None);
        assert_eq!(parse_remote_fill_slice_ns(Some("invalid")), None);
        assert_eq!(parse_remote_fill_slice_ns(None), None);
    }

    fn temp_cache_dir(name: &str) -> std::path::PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "tqsdk-backtest-remote-{name}-{}-{unique}",
            std::process::id()
        ))
    }

    fn tick(id: i64, datetime: i64, last_price: f64) -> Tick {
        Tick {
            id,
            datetime,
            last_price,
            ask_price1: last_price + 0.5,
            ask_volume1: 1,
            bid_price1: last_price - 0.5,
            bid_volume1: 1,
            ..Tick::default()
        }
    }
}
