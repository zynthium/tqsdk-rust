#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Lines, Write};
use std::path::{Path, PathBuf};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum MarketCachePayloadKind {
    Quote,
    Kline,
    Tick,
}

impl MarketCachePayload {
    #[must_use]
    pub fn kind(&self) -> MarketCachePayloadKind {
        match self {
            Self::Quote(_) => MarketCachePayloadKind::Quote,
            Self::Kline { .. } => MarketCachePayloadKind::Kline,
            Self::Tick(_) => MarketCachePayloadKind::Tick,
        }
    }
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

    #[must_use]
    pub fn payload_kind(&self) -> MarketCachePayloadKind {
        self.payload.kind()
    }
}

pub struct MarketCacheWriter<W: Write> {
    inner: BufWriter<W>,
}

impl MarketCacheWriter<File> {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::new(File::create(path)?))
    }

    pub fn append(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::new(
            OpenOptions::new().create(true).append(true).open(path)?,
        ))
    }
}

impl<W: Write> MarketCacheWriter<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner: BufWriter::new(inner),
        }
    }

    pub fn write_event(&mut self, event: &MarketCacheEvent) -> Result<()> {
        write_market_cache_event_line(&mut self.inner, event)
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MarketCacheIndexKey {
    pub source: String,
    pub symbol: String,
    pub payload_kind: MarketCachePayloadKind,
}

impl MarketCacheIndexKey {
    #[must_use]
    pub fn new(
        source: impl Into<String>,
        symbol: impl Into<String>,
        payload_kind: MarketCachePayloadKind,
    ) -> Self {
        Self {
            source: source.into(),
            symbol: symbol.into(),
            payload_kind,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketCacheIndexEntry {
    pub events: usize,
    pub min_event_time_ns: i64,
    pub max_event_time_ns: i64,
    pub min_received_at_ns: i64,
    pub max_received_at_ns: i64,
}

impl MarketCacheIndexEntry {
    fn new(event: &MarketCacheEvent) -> Self {
        Self {
            events: 1,
            min_event_time_ns: event.event_time_ns(),
            max_event_time_ns: event.event_time_ns(),
            min_received_at_ns: event.received_at_ns,
            max_received_at_ns: event.received_at_ns,
        }
    }

    fn add_event(&mut self, event: &MarketCacheEvent) {
        self.events += 1;
        self.min_event_time_ns = self.min_event_time_ns.min(event.event_time_ns());
        self.max_event_time_ns = self.max_event_time_ns.max(event.event_time_ns());
        self.min_received_at_ns = self.min_received_at_ns.min(event.received_at_ns);
        self.max_received_at_ns = self.max_received_at_ns.max(event.received_at_ns);
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MarketCacheIndex {
    total_events: usize,
    entries: BTreeMap<MarketCacheIndexKey, MarketCacheIndexEntry>,
}

impl MarketCacheIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn from_events<'a, I>(events: I) -> Self
    where
        I: IntoIterator<Item = &'a MarketCacheEvent>,
    {
        let mut index = Self::new();
        for event in events {
            index.add_event(event);
        }
        index
    }

    pub fn from_reader<R: BufRead>(reader: MarketCacheReader<R>) -> Result<Self> {
        let mut index = Self::new();
        for event in reader {
            index.add_event(&event?);
        }
        Ok(index)
    }

    pub fn add_event(&mut self, event: &MarketCacheEvent) {
        self.total_events += 1;
        let key = MarketCacheIndexKey::new(&event.source, &event.symbol, event.payload_kind());
        self.entries
            .entry(key)
            .and_modify(|entry| entry.add_event(event))
            .or_insert_with(|| MarketCacheIndexEntry::new(event));
    }

    #[must_use]
    pub fn total_events(&self) -> usize {
        self.total_events
    }

    #[must_use]
    pub fn entry(
        &self,
        source: &str,
        symbol: &str,
        payload_kind: MarketCachePayloadKind,
    ) -> Option<&MarketCacheIndexEntry> {
        self.entries
            .get(&MarketCacheIndexKey::new(source, symbol, payload_kind))
    }

    pub fn entries(&self) -> impl Iterator<Item = (&MarketCacheIndexKey, &MarketCacheIndexEntry)> {
        self.entries.iter()
    }
}

#[derive(Debug)]
pub struct MarketCacheLock {
    path: PathBuf,
    _file: File,
}

impl MarketCacheLock {
    pub fn acquire(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    DataError::InvalidState("market cache lock is already held")
                } else {
                    DataError::Io(error)
                }
            })?;
        writeln!(file, "pid={}", std::process::id())?;
        file.flush()?;
        Ok(Self { path, _file: file })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for MarketCacheLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[derive(Debug, Clone)]
pub struct MarketCacheQueue {
    path: PathBuf,
    sync_on_enqueue: bool,
}

impl MarketCacheQueue {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            path,
            sync_on_enqueue: false,
        })
    }

    #[must_use]
    pub fn with_sync_on_enqueue(mut self, sync_on_enqueue: bool) -> Self {
        self.sync_on_enqueue = sync_on_enqueue;
        self
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn enqueue_event(&self, event: &MarketCacheEvent) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        write_market_cache_event_line(&mut file, event)?;
        file.flush()?;
        if self.sync_on_enqueue {
            file.sync_data()?;
        }
        Ok(())
    }

    pub fn reader(&self) -> Result<MarketCacheReader<BufReader<File>>> {
        MarketCacheReader::open(&self.path)
    }

    pub fn replay(&self) -> Result<MarketCacheReplay> {
        MarketCacheReplay::from_reader(self.reader()?)
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(std::fs::metadata(&self.path)?.len() == 0)
    }

    pub fn clear(&self) -> Result<()> {
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        Ok(())
    }

    pub fn drain_to_writer<W: Write>(
        &self,
        writer: &mut MarketCacheWriter<W>,
    ) -> Result<MarketCacheQueueDrainReport> {
        let mut read_events = 0usize;
        let mut written_events = 0usize;
        for event in self.reader()? {
            let event = event?;
            read_events += 1;
            writer.write_event(&event)?;
            written_events += 1;
        }
        writer.flush()?;
        self.clear()?;
        Ok(MarketCacheQueueDrainReport {
            queue_path: self.path.clone(),
            read_events,
            written_events,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketCacheQueueDrainReport {
    pub queue_path: PathBuf,
    pub read_events: usize,
    pub written_events: usize,
}

#[derive(Debug, Clone, Default)]
pub struct MarketCacheCompaction {
    min_event_time_ns: Option<i64>,
    max_event_time_ns: Option<i64>,
    symbols: BTreeSet<String>,
    sources: BTreeSet<String>,
    payload_kinds: BTreeSet<MarketCachePayloadKind>,
}

impl MarketCacheCompaction {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn retain_event_time_from(mut self, min_event_time_ns: i64) -> Self {
        self.min_event_time_ns = Some(min_event_time_ns);
        self
    }

    #[must_use]
    pub fn retain_event_time_until(mut self, max_event_time_ns: i64) -> Self {
        self.max_event_time_ns = Some(max_event_time_ns);
        self
    }

    #[must_use]
    pub fn retain_symbol(mut self, symbol: impl Into<String>) -> Self {
        self.symbols.insert(symbol.into());
        self
    }

    #[must_use]
    pub fn retain_source(mut self, source: impl Into<String>) -> Self {
        self.sources.insert(source.into());
        self
    }

    #[must_use]
    pub fn retain_payload_kind(mut self, payload_kind: MarketCachePayloadKind) -> Self {
        self.payload_kinds.insert(payload_kind);
        self
    }

    pub fn compact_file(
        &self,
        input_path: impl AsRef<Path>,
        output_path: impl AsRef<Path>,
    ) -> Result<MarketCacheCompactionReport> {
        let input_path = input_path.as_ref();
        let output_path = output_path.as_ref();
        if input_path == output_path {
            return Err(DataError::Validation(
                "market cache compaction input and output paths must differ".into(),
            ));
        }
        let reader = MarketCacheReader::open(input_path)?;
        let mut writer = MarketCacheWriter::create(output_path)?;
        self.compact_reader_to_writer(reader, &mut writer)
    }

    pub fn compact_reader_to_writer<R: BufRead, W: Write>(
        &self,
        reader: MarketCacheReader<R>,
        writer: &mut MarketCacheWriter<W>,
    ) -> Result<MarketCacheCompactionReport> {
        self.validate()?;
        let mut report = MarketCacheCompactionReport {
            read_events: 0,
            written_events: 0,
            dropped_events: 0,
            index: MarketCacheIndex::new(),
        };

        for event in reader {
            let event = event?;
            report.read_events += 1;
            if self.retains(&event) {
                writer.write_event(&event)?;
                report.index.add_event(&event);
                report.written_events += 1;
            } else {
                report.dropped_events += 1;
            }
        }
        writer.flush()?;
        Ok(report)
    }

    fn validate(&self) -> Result<()> {
        if self
            .min_event_time_ns
            .into_iter()
            .chain(self.max_event_time_ns)
            .any(|time| time < 0)
        {
            return Err(DataError::Validation(
                "market cache compaction event time bounds must be non-negative".into(),
            ));
        }
        if matches!(
            (self.min_event_time_ns, self.max_event_time_ns),
            (Some(min), Some(max)) if min > max
        ) {
            return Err(DataError::Validation(
                "market cache compaction min event time exceeds max event time".into(),
            ));
        }
        if self
            .symbols
            .iter()
            .chain(self.sources.iter())
            .any(|value| value.trim().is_empty())
        {
            return Err(DataError::Validation(
                "market cache compaction filters must not be empty".into(),
            ));
        }
        Ok(())
    }

    fn retains(&self, event: &MarketCacheEvent) -> bool {
        if self
            .min_event_time_ns
            .is_some_and(|min| event.event_time_ns() < min)
        {
            return false;
        }
        if self
            .max_event_time_ns
            .is_some_and(|max| event.event_time_ns() > max)
        {
            return false;
        }
        if !self.symbols.is_empty() && !self.symbols.contains(&event.symbol) {
            return false;
        }
        if !self.sources.is_empty() && !self.sources.contains(&event.source) {
            return false;
        }
        if !self.payload_kinds.is_empty() && !self.payload_kinds.contains(&event.payload_kind()) {
            return false;
        }
        true
    }
}

#[derive(Debug, Clone)]
pub struct MarketCacheCompactionReport {
    pub read_events: usize,
    pub written_events: usize,
    pub dropped_events: usize,
    pub index: MarketCacheIndex,
}

fn write_market_cache_event_line<W: Write>(writer: &mut W, event: &MarketCacheEvent) -> Result<()> {
    serde_json::to_writer(&mut *writer, event)?;
    writer.write_all(b"\n")?;
    Ok(())
}
