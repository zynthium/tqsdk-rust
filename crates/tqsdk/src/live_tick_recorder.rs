use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tqsdk_core::Tick;
use tqsdk_data::{BacktestTickCache, LiveTickCacheWriteReport, LiveTickCacheWriter};
use tqsdk_wait::{TickHandle, WaitStep};

use crate::{Error, Result};

const RECORD_TICK_DATA_LENGTH: usize = 10_000;
const RECORD_TICK_WRITE_BUFFER_ROWS: usize = 128;
const RECORD_TICK_WRITE_MAX_LATENCY: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordTicksReport {
    pub cache_dir: PathBuf,
    pub symbols: Vec<String>,
    pub data_length: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordTicksHealth {
    pub cache_dir: PathBuf,
    pub flush_count: u64,
    pub total_appended_rows: usize,
    pub gap_detected: bool,
    pub symbols: Vec<RecordTicksSymbolHealth>,
    pub last_flush: Option<RecordTicksFlushReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordTicksSymbolHealth {
    pub symbol: String,
    pub total_appended_rows: usize,
    pub last_seen_id: Option<i64>,
    pub gap_detected: bool,
    pub last_committed_ranges: Vec<(i64, i64)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordTicksFlushReport {
    pub appended_rows: usize,
    pub gap_detected: bool,
    pub symbols: Vec<RecordTicksSymbolFlushReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordTicksSymbolFlushReport {
    pub symbol: String,
    pub appended_rows: usize,
    pub committed_ranges: Vec<(i64, i64)>,
    pub last_seen_id: Option<i64>,
    pub gap_detected: bool,
}

pub(crate) struct LiveTickRecorder {
    writer: LiveTickCacheWriter,
    symbols: Vec<RecordedTickSymbol>,
    health: RecordTicksHealth,
}

struct RecordedTickSymbol {
    symbol: String,
    handle: TickHandle,
    last_observed_id: Option<i64>,
    last_persisted_id: Option<i64>,
    pending_rows: Vec<Tick>,
    last_persisted_at: Option<Instant>,
    pending_gap_detected: bool,
    needs_rescan: bool,
}

impl LiveTickRecorder {
    pub(crate) async fn start(
        api: &mut tqsdk_wait::TqApi,
        cache_dir: impl AsRef<Path>,
        symbols: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<(Self, RecordTicksReport)> {
        let cache = BacktestTickCache::open(cache_dir)?;
        Self::start_with_cache(api, cache, symbols).await
    }

    async fn start_with_cache(
        api: &mut tqsdk_wait::TqApi,
        cache: BacktestTickCache,
        symbols: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<(Self, RecordTicksReport)> {
        let symbols = normalize_symbols(symbols)?;
        let mut recorded = Vec::with_capacity(symbols.len());
        for symbol in &symbols {
            let handle = api.tick(symbol, RECORD_TICK_DATA_LENGTH).await?;
            recorded.push(RecordedTickSymbol {
                symbol: symbol.clone(),
                handle,
                last_observed_id: None,
                last_persisted_id: None,
                pending_rows: Vec::new(),
                last_persisted_at: None,
                pending_gap_detected: false,
                needs_rescan: false,
            });
        }

        let report = RecordTicksReport {
            cache_dir: cache.cache_dir().to_path_buf(),
            symbols,
            data_length: RECORD_TICK_DATA_LENGTH,
        };

        Ok((
            Self {
                writer: LiveTickCacheWriter::new(cache),
                symbols: recorded,
                health: RecordTicksHealth::new(report.cache_dir.clone(), &report.symbols),
            },
            report,
        ))
    }

    pub(crate) fn flush(&mut self, step: Option<&WaitStep>) -> Result<RecordTicksFlushReport> {
        for recorded in &mut self.symbols {
            let rows = recorded.rows_for_step(step)?;
            recorded.observe_rows(rows);
        }

        self.flush_pending(false)
    }

    fn flush_pending(&mut self, force: bool) -> Result<RecordTicksFlushReport> {
        let now = Instant::now();
        let mut symbol_reports = Vec::new();
        for recorded in &mut self.symbols {
            if !recorded.should_persist(now, force) {
                continue;
            }
            let last_persisted_id = recorded.last_persisted_id;
            let rows = std::mem::take(&mut recorded.pending_rows);
            let write_report = match self.writer.push_ticks(recorded.symbol.as_str(), rows) {
                Ok(report) => report,
                Err(error) => {
                    recorded.last_observed_id = last_persisted_id;
                    recorded.needs_rescan = true;
                    return Err(error.into());
                }
            };
            recorded.last_persisted_id = write_report.last_seen_id;
            recorded.last_persisted_at = Some(now);
            recorded.pending_gap_detected = false;
            recorded.needs_rescan = false;
            symbol_reports.push(RecordTicksSymbolFlushReport::from(write_report));
        }

        let flush_report = RecordTicksFlushReport::from_symbols(symbol_reports);
        self.health.apply_flush(flush_report.clone());
        Ok(flush_report)
    }

    pub(crate) fn health(&self) -> &RecordTicksHealth {
        &self.health
    }
}

impl Drop for LiveTickRecorder {
    fn drop(&mut self) {
        let _ = self.flush_pending(true);
    }
}

impl RecordedTickSymbol {
    fn rows_for_step(&self, step: Option<&WaitStep>) -> Result<Vec<Tick>> {
        let Some(last_seen_id) = self.last_observed_id else {
            return self.handle.rows().map_err(Into::into);
        };

        if self.needs_rescan {
            return self.handle.rows_since(last_seen_id).map_err(Into::into);
        }

        let Some(step) = step else {
            return self.handle.rows_since(last_seen_id).map_err(Into::into);
        };

        let mut rows = self.handle.changed_rows(step)?;
        rows.retain(|row| row.id > last_seen_id);
        Ok(rows)
    }

    fn observe_rows(&mut self, rows: Vec<Tick>) {
        for row in &rows {
            if self
                .last_observed_id
                .is_some_and(|last_seen| row.id > last_seen.saturating_add(1))
            {
                self.pending_gap_detected = true;
            }
            self.last_observed_id = Some(
                self.last_observed_id
                    .map_or(row.id, |last_seen| last_seen.max(row.id)),
            );
        }
        self.pending_rows.extend(rows);
    }

    fn should_persist(&self, now: Instant, force: bool) -> bool {
        !self.pending_rows.is_empty()
            && (force
                || self.needs_rescan
                || self.pending_gap_detected
                || self.last_persisted_at.is_none()
                || self.pending_rows.len() >= RECORD_TICK_WRITE_BUFFER_ROWS
                || self.last_persisted_at.is_some_and(|last_persisted_at| {
                    now.saturating_duration_since(last_persisted_at)
                        >= RECORD_TICK_WRITE_MAX_LATENCY
                }))
    }
}

impl RecordTicksHealth {
    fn new(cache_dir: PathBuf, symbols: &[String]) -> Self {
        Self {
            cache_dir,
            flush_count: 0,
            total_appended_rows: 0,
            gap_detected: false,
            symbols: symbols
                .iter()
                .map(|symbol| RecordTicksSymbolHealth::new(symbol.clone()))
                .collect(),
            last_flush: None,
        }
    }

    fn apply_flush(&mut self, report: RecordTicksFlushReport) {
        self.flush_count = self.flush_count.saturating_add(1);
        self.total_appended_rows = self
            .total_appended_rows
            .saturating_add(report.appended_rows);
        self.gap_detected |= report.gap_detected;

        for symbol_report in &report.symbols {
            if let Some(health) = self
                .symbols
                .iter_mut()
                .find(|health| health.symbol == symbol_report.symbol)
            {
                health.apply_flush(symbol_report);
            }
        }
        self.last_flush = Some(report);
    }
}

impl RecordTicksSymbolHealth {
    fn new(symbol: String) -> Self {
        Self {
            symbol,
            total_appended_rows: 0,
            last_seen_id: None,
            gap_detected: false,
            last_committed_ranges: Vec::new(),
        }
    }

    fn apply_flush(&mut self, report: &RecordTicksSymbolFlushReport) {
        self.total_appended_rows = self
            .total_appended_rows
            .saturating_add(report.appended_rows);
        self.last_seen_id = report.last_seen_id;
        self.gap_detected |= report.gap_detected;
        self.last_committed_ranges = report.committed_ranges.clone();
    }
}

impl RecordTicksFlushReport {
    fn from_symbols(symbols: Vec<RecordTicksSymbolFlushReport>) -> Self {
        let appended_rows = symbols.iter().map(|report| report.appended_rows).sum();
        let gap_detected = symbols.iter().any(|report| report.gap_detected);
        Self {
            appended_rows,
            gap_detected,
            symbols,
        }
    }
}

impl From<LiveTickCacheWriteReport> for RecordTicksSymbolFlushReport {
    fn from(report: LiveTickCacheWriteReport) -> Self {
        Self {
            symbol: report.symbol,
            appended_rows: report.appended_rows,
            committed_ranges: report.committed_ranges,
            last_seen_id: report.last_seen_id,
            gap_detected: report.gap_detected,
        }
    }
}

fn normalize_symbols(symbols: impl IntoIterator<Item = impl AsRef<str>>) -> Result<Vec<String>> {
    let mut seen = BTreeSet::new();
    let mut normalized = Vec::new();
    for symbol in symbols {
        let symbol = symbol.as_ref().trim();
        if symbol.is_empty() {
            return Err(Error::from(tqsdk_wait::WaitFacadeError::InvalidState(
                "record_ticks symbols must not be empty",
            )));
        }
        if seen.insert(symbol.to_string()) {
            normalized.push(symbol.to_string());
        }
    }
    if normalized.is_empty() {
        return Err(Error::from(tqsdk_wait::WaitFacadeError::InvalidState(
            "record_ticks requires at least one symbol",
        )));
    }
    Ok(normalized)
}
