use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use tqsdk_data::{BacktestTickCache, BacktestTickFill};
use tqsdk_task::{BacktestMarketStream, ReplayMarketEvent};

use crate::{Result, data_validation};

const REMOTE_TICK_DATA_LENGTH: usize = 10_000;
const REMOTE_FILL_END_TOLERANCE_NS: i64 = 1_000_000_000;
const REMOTE_STEP_POLL_TIMEOUT: Duration = Duration::from_secs(5);
const REMOTE_FILL_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

pub(crate) struct SlicedRemoteBacktestCachingStream {
    user: String,
    pass: String,
    symbols: Vec<String>,
    cache: BacktestTickCache,
    ranges: VecDeque<(i64, i64)>,
    current: Option<((i64, i64), RemoteBacktestCachingStream)>,
}

pub(crate) struct RemoteBacktestCachingStream {
    api: tqsdk_wait::TqApi,
    handles: BTreeMap<String, tqsdk_wait::TickHandle>,
    cache: BacktestTickCache,
    fills: BTreeMap<String, BacktestTickFill>,
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

pub(crate) async fn fill_backtest_tick_cache(
    user: String,
    pass: String,
    start_ns: i64,
    end_ns: i64,
    symbols: Vec<String>,
    cache: BacktestTickCache,
) -> Result<RemoteBacktestCacheFillReport> {
    let mut rows_by_symbol = BTreeMap::new();
    for (slice_start_ns, slice_end_ns) in remote_fill_ranges(start_ns, end_ns) {
        let mut stream = RemoteBacktestCachingStream::connect(
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

impl SlicedRemoteBacktestCachingStream {
    pub(crate) fn connect(
        user: String,
        pass: String,
        start_ns: i64,
        end_ns: i64,
        symbols: Vec<String>,
        cache: BacktestTickCache,
    ) -> Result<Self> {
        Ok(Self {
            user,
            pass,
            symbols,
            cache,
            ranges: remote_fill_ranges(start_ns, end_ns).into(),
            current: None,
        })
    }

    async fn next_remote_event(&mut self) -> Result<Option<ReplayMarketEvent>> {
        loop {
            if let Some(((slice_start_ns, slice_end_ns), stream)) = &mut self.current {
                if let Some(event) = stream
                    .next_remote_event()
                    .await
                    .map_err(|error| remote_slice_error(*slice_start_ns, *slice_end_ns, error))?
                {
                    return Ok(Some(event));
                }
                self.current = None;
            }

            let Some((slice_start_ns, slice_end_ns)) = self.ranges.pop_front() else {
                return Ok(None);
            };
            let stream = RemoteBacktestCachingStream::connect(
                self.user.clone(),
                self.pass.clone(),
                slice_start_ns,
                slice_end_ns,
                self.symbols.clone(),
                self.cache.clone(),
            )
            .await
            .map_err(|error| remote_slice_error(slice_start_ns, slice_end_ns, error))?;
            self.current = Some(((slice_start_ns, slice_end_ns), stream));
        }
    }
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
                // Closed-session tails can legitimately idle before the requested slice end.
                if should_reject_empty_idle_finalize(
                    self.handles.len(),
                    self.accepted_rows_total,
                    remote_fill_allow_empty_idle(),
                ) {
                    return Err(data_validation(format!(
                        "remote backtest cache fill idled without accepted ticks for {} symbols \
                         in range [{}, {}); refusing to mark complete empty coverage",
                        self.handles.len(),
                        self.range_start_ns,
                        self.range_end_ns
                    )));
                }
                self.finalize_cache(FinalizeMode::Idle)?;
                return Ok(None);
            }

            let deadline = tokio::time::Instant::now() + REMOTE_STEP_POLL_TIMEOUT;
            let Some(step) = self.api.step_until(Some(deadline)).await? else {
                continue;
            };

            let mut made_progress = false;
            for (symbol, handle) in &self.handles {
                if !step.is_changing(handle) {
                    continue;
                }

                let mut accepted_rows = Vec::new();
                let mut accepted_events = Vec::new();
                for row in handle.changed_rows(&step)? {
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
                    self.cache.append_partial_ticks(symbol, accepted_rows)?;
                    made_progress = true;
                    self.pending.extend(accepted_events);
                }
            }

            if made_progress {
                self.last_progress = tokio::time::Instant::now();
            }
        }
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
        self.finalized = true;
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

fn parse_remote_fill_slice_ns(value: Option<&str>) -> Option<i64> {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .and_then(|secs| secs.checked_mul(1_000_000_000))
        .and_then(|ns| i64::try_from(ns).ok())
        .filter(|ns| *ns > 0)
}

fn should_reject_empty_idle_finalize(
    symbol_count: usize,
    accepted_rows_total: usize,
    allow_empty_idle: bool,
) -> bool {
    symbol_count > 1 && accepted_rows_total == 0 && !allow_empty_idle
}

impl BacktestMarketStream for RemoteBacktestCachingStream {
    fn next_event<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = tqsdk_task::Result<Option<ReplayMarketEvent>>> + 'a>> {
        Box::pin(async move {
            self.next_remote_event()
                .await
                .map_err(|error| tqsdk_task::TaskError::External(error.to_string()))
        })
    }
}

impl BacktestMarketStream for SlicedRemoteBacktestCachingStream {
    fn next_event<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = tqsdk_task::Result<Option<ReplayMarketEvent>>> + 'a>> {
        Box::pin(async move {
            self.next_remote_event()
                .await
                .map_err(|error| tqsdk_task::TaskError::External(error.to_string()))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        REMOTE_FILL_IDLE_TIMEOUT, parse_remote_fill_allow_empty_idle,
        parse_remote_fill_idle_timeout, parse_remote_fill_slice_ns,
        remote_fill_ranges_for_slice_ns, remote_fill_ranges_with_slice_ns,
        should_reject_empty_idle_finalize,
    };

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
    fn remote_fill_rejects_multi_symbol_empty_idle_finalize_by_default() {
        assert!(should_reject_empty_idle_finalize(2, 0, false));
        assert!(should_reject_empty_idle_finalize(128, 0, false));
        assert!(!should_reject_empty_idle_finalize(1, 0, false));
        assert!(!should_reject_empty_idle_finalize(2, 1, false));
        assert!(!should_reject_empty_idle_finalize(2, 0, true));
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
}
