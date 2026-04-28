#![cfg_attr(not(test), forbid(unsafe_code))]

use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::{Stream, StreamExt};
use tqsdk_stream::MarketEvent;

use crate::{DataError, MarketCacheEvent, MarketCacheWriter, Result};

/// Single-process bridge from typed live market events into a local cache writer.
pub struct MarketCacheStreamWriter<W: Write> {
    source: String,
    writer: MarketCacheWriter<W>,
}

impl<W: Write> MarketCacheStreamWriter<W> {
    pub fn new(source: impl Into<String>, writer: MarketCacheWriter<W>) -> Result<Self> {
        let source = source.into();
        if source.trim().is_empty() {
            return Err(DataError::Validation(
                "cache stream source must not be empty".into(),
            ));
        }
        Ok(Self { source, writer })
    }

    pub fn write_market_event(&mut self, event: MarketEvent) -> Result<usize> {
        let received_at_ns = system_time_ns()?;
        let cache_events = market_event_to_cache_events(&self.source, received_at_ns, event)?;
        let count = cache_events.len();
        for event in cache_events {
            self.writer.write_event(&event)?;
        }
        Ok(count)
    }

    pub async fn pipe_market_events<S>(
        &mut self,
        mut events: S,
        max_events: Option<usize>,
    ) -> Result<usize>
    where
        S: Stream<Item = tqsdk_stream::Result<MarketEvent>> + Unpin,
    {
        let mut written = 0usize;
        while max_events.is_none_or(|max| written < max) {
            let Some(event) = events.next().await else {
                break;
            };
            let event = event.map_err(|error| DataError::Validation(error.to_string()))?;
            written += self.write_market_event(event)?;
        }
        self.writer.flush()?;
        Ok(written)
    }
}

fn market_event_to_cache_events(
    source: &str,
    received_at_ns: i64,
    event: MarketEvent,
) -> Result<Vec<MarketCacheEvent>> {
    match event {
        MarketEvent::Quote(update) => {
            let quote = update.value;
            if quote.instrument_id.trim().is_empty() {
                return Err(DataError::Validation(
                    "market event quote is missing instrument_id".into(),
                ));
            }
            Ok(vec![MarketCacheEvent::quote(
                source,
                quote.instrument_id.clone(),
                received_at_ns,
                None,
                quote,
            )?])
        }
        MarketEvent::KlineWindow(update) => {
            let window = update.value;
            let Some(row) = window.last().cloned() else {
                return Ok(Vec::new());
            };
            Ok(vec![MarketCacheEvent::kline(
                source,
                window.symbol(),
                received_at_ns,
                Some(row.datetime),
                window.duration_ns(),
                row,
            )?])
        }
        MarketEvent::TickWindow(update) => {
            let window = update.value;
            let Some(row) = window.last().cloned() else {
                return Ok(Vec::new());
            };
            Ok(vec![MarketCacheEvent::tick(
                source,
                window.symbol(),
                received_at_ns,
                Some(row.datetime),
                row,
            )?])
        }
    }
}

fn system_time_ns() -> Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| DataError::InvalidState("system clock is before unix epoch"))?;
    i64::try_from(elapsed.as_nanos())
        .map_err(|_| DataError::InvalidState("system clock nanoseconds overflow i64"))
}
