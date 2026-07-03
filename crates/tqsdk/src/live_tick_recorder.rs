use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use tqsdk_data::{BacktestTickCache, LiveTickCacheWriteReport, LiveTickCacheWriter};
use tqsdk_wait::TickHandle;

use crate::{Error, Result};

const RECORD_TICK_DATA_LENGTH: usize = 10_000;

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
    last_seen_id: Option<i64>,
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
                last_seen_id: None,
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

    pub(crate) fn flush(&mut self) -> Result<RecordTicksFlushReport> {
        let mut symbol_reports = Vec::new();
        for recorded in &mut self.symbols {
            let rows = match recorded.last_seen_id {
                Some(last_seen_id) => recorded.handle.rows_since(last_seen_id)?,
                None => recorded.handle.rows()?,
            };

            if rows.is_empty() {
                continue;
            }

            let write_report = self.writer.push_ticks(recorded.symbol.as_str(), rows)?;
            recorded.last_seen_id = write_report.last_seen_id;
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
