use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use tqsdk_data::{BacktestTickCache, LiveTickCacheWriter};
use tqsdk_wait::TickHandle;

use crate::{Error, Result};

const RECORD_TICK_DATA_LENGTH: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordTicksReport {
    pub cache_dir: PathBuf,
    pub symbols: Vec<String>,
    pub data_length: usize,
}

pub(crate) struct LiveTickRecorder {
    writer: LiveTickCacheWriter,
    symbols: Vec<RecordedTickSymbol>,
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
            },
            report,
        ))
    }

    pub(crate) fn flush(&mut self) -> Result<()> {
        for recorded in &mut self.symbols {
            let rows = match recorded.last_seen_id {
                Some(last_seen_id) => recorded.handle.rows_since(last_seen_id)?,
                None => recorded.handle.rows()?,
            };

            if rows.is_empty() {
                continue;
            }

            let next_last_seen_id = rows.iter().map(|row| row.id).max();
            self.writer.push_ticks(recorded.symbol.as_str(), rows)?;
            if let Some(last_seen_id) = next_last_seen_id {
                recorded.last_seen_id = Some(last_seen_id);
            }
        }
        Ok(())
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
