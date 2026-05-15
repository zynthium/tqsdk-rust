#![cfg_attr(not(test), forbid(unsafe_code))]

use crate::{HistorySeriesCache, Result};

/// Options for explicitly writing live stream windows into [`HistorySeriesCache`].
#[derive(Debug, Clone, Default)]
pub struct LiveHistoryCacheOptions;

/// Summary returned after a live history cache write.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LiveHistoryCacheWriteReport {
    pub rows_seen: usize,
    pub rows_written: usize,
    pub skipped_mutable_tail: bool,
}

/// Explicit opt-in bridge from `tqsdk-stream` market windows into history series cache.
pub struct LiveHistoryCacheWriter {
    cache: HistorySeriesCache,
    _options: LiveHistoryCacheOptions,
}

impl LiveHistoryCacheWriter {
    #[must_use]
    pub fn new(cache: HistorySeriesCache, options: LiveHistoryCacheOptions) -> Self {
        Self {
            cache,
            _options: options,
        }
    }

    pub fn write_kline_window(
        &mut self,
        window: &tqsdk_stream::KlineWindow,
    ) -> Result<LiveHistoryCacheWriteReport> {
        let rows_seen = window.len();
        let max_id = window.iter().map(|row| row.id).max();
        let rows = window
            .iter()
            .filter(|row| Some(row.id) != max_id)
            .cloned()
            .collect::<Vec<_>>();
        let rows_written =
            self.cache
                .append_kline_rows(window.symbol(), window.duration_ns(), &rows)?;

        Ok(LiveHistoryCacheWriteReport {
            rows_seen,
            rows_written,
            skipped_mutable_tail: max_id.is_some(),
        })
    }

    pub fn write_tick_window(
        &mut self,
        window: &tqsdk_stream::TickWindow,
    ) -> Result<LiveHistoryCacheWriteReport> {
        let rows = window.iter().cloned().collect::<Vec<_>>();
        let rows_written = self.cache.append_tick_rows(window.symbol(), &rows)?;

        Ok(LiveHistoryCacheWriteReport {
            rows_seen: window.len(),
            rows_written,
            skipped_mutable_tail: false,
        })
    }

    pub fn write_market_event(
        &mut self,
        event: tqsdk_stream::MarketEvent,
    ) -> Result<LiveHistoryCacheWriteReport> {
        match event {
            tqsdk_stream::MarketEvent::Quote(_) => Ok(LiveHistoryCacheWriteReport::default()),
            tqsdk_stream::MarketEvent::KlineWindow(update) => {
                self.write_kline_window(&update.value)
            }
            tqsdk_stream::MarketEvent::TickWindow(update) => self.write_tick_window(&update.value),
        }
    }
}
