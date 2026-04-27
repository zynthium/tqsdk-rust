#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Lines, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use tqsdk_core::{Kline, Quote, Tick};

use crate::{DataError, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data")]
pub enum MarketCachePayload {
    Quote(Box<Quote>),
    Kline { duration_ns: i64, row: Kline },
    Tick(Tick),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketCacheEvent {
    pub source: String,
    pub symbol: String,
    pub received_at_ns: i64,
    pub exchange_time_ns: Option<i64>,
    pub payload: MarketCachePayload,
}

impl MarketCacheEvent {
    pub fn quote(
        source: impl Into<String>,
        symbol: impl Into<String>,
        received_at_ns: i64,
        exchange_time_ns: Option<i64>,
        quote: Quote,
    ) -> Result<Self> {
        Self::new(
            source,
            symbol,
            received_at_ns,
            exchange_time_ns,
            MarketCachePayload::Quote(Box::new(quote)),
        )
    }

    pub fn kline(
        source: impl Into<String>,
        symbol: impl Into<String>,
        received_at_ns: i64,
        exchange_time_ns: Option<i64>,
        duration_ns: i64,
        row: Kline,
    ) -> Result<Self> {
        if duration_ns <= 0 {
            return Err(DataError::Validation(
                "kline duration must be positive".into(),
            ));
        }
        Self::new(
            source,
            symbol,
            received_at_ns,
            exchange_time_ns,
            MarketCachePayload::Kline { duration_ns, row },
        )
    }

    pub fn tick(
        source: impl Into<String>,
        symbol: impl Into<String>,
        received_at_ns: i64,
        exchange_time_ns: Option<i64>,
        tick: Tick,
    ) -> Result<Self> {
        Self::new(
            source,
            symbol,
            received_at_ns,
            exchange_time_ns,
            MarketCachePayload::Tick(tick),
        )
    }

    fn new(
        source: impl Into<String>,
        symbol: impl Into<String>,
        received_at_ns: i64,
        exchange_time_ns: Option<i64>,
        payload: MarketCachePayload,
    ) -> Result<Self> {
        let source = source.into();
        let symbol = symbol.into();
        if source.trim().is_empty() {
            return Err(DataError::Validation(
                "cache event source must not be empty".into(),
            ));
        }
        if symbol.trim().is_empty() {
            return Err(DataError::Validation(
                "cache event symbol must not be empty".into(),
            ));
        }
        if received_at_ns < 0 {
            return Err(DataError::Validation(
                "received_at_ns must be non-negative".into(),
            ));
        }
        if exchange_time_ns.is_some_and(|time| time < 0) {
            return Err(DataError::Validation(
                "exchange_time_ns must be non-negative".into(),
            ));
        }
        Ok(Self {
            source,
            symbol,
            received_at_ns,
            exchange_time_ns,
            payload,
        })
    }

    #[must_use]
    pub fn event_time_ns(&self) -> i64 {
        self.exchange_time_ns.unwrap_or(self.received_at_ns)
    }
}

pub struct MarketCacheWriter<W: Write> {
    inner: BufWriter<W>,
}

impl MarketCacheWriter<File> {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::new(File::create(path)?))
    }
}

impl<W: Write> MarketCacheWriter<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner: BufWriter::new(inner),
        }
    }

    pub fn write_event(&mut self, event: &MarketCacheEvent) -> Result<()> {
        serde_json::to_writer(&mut self.inner, event)?;
        self.inner.write_all(b"\n")?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.inner.flush()?;
        Ok(())
    }
}

pub struct MarketCacheReader<R: BufRead> {
    lines: Lines<R>,
}

impl MarketCacheReader<BufReader<File>> {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::new(BufReader::new(File::open(path)?)))
    }
}

impl<R: BufRead> MarketCacheReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            lines: inner.lines(),
        }
    }
}

impl<R: BufRead> Iterator for MarketCacheReader<R> {
    type Item = Result<MarketCacheEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        let line = self.lines.next()?;
        Some(line.map_err(DataError::from).and_then(|line| {
            if line.trim().is_empty() {
                Err(DataError::InvalidResponse("empty market cache line".into()))
            } else {
                serde_json::from_str(&line).map_err(DataError::from)
            }
        }))
    }
}

pub struct MarketCacheReplay {
    events: Vec<MarketCacheEvent>,
    index: usize,
}

impl MarketCacheReplay {
    #[must_use]
    pub fn new(mut events: Vec<MarketCacheEvent>) -> Self {
        events.sort_by_key(|event| (event.event_time_ns(), event.received_at_ns));
        Self { events, index: 0 }
    }

    pub fn from_reader<R: BufRead>(reader: MarketCacheReader<R>) -> Result<Self> {
        let events = reader.collect::<Result<Vec<_>>>()?;
        Ok(Self::new(events))
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.events.len().saturating_sub(self.index)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Iterator for MarketCacheReplay {
    type Item = MarketCacheEvent;

    fn next(&mut self) -> Option<Self::Item> {
        let event = self.events.get(self.index)?.clone();
        self.index += 1;
        Some(event)
    }
}
