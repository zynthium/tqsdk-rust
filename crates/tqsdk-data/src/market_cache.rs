#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Lines, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketCacheReaderCheckpoint {
    pub reader_id: String,
    pub checkpoint_id: String,
    pub source: String,
    pub symbol: String,
    pub payload_kind: MarketCachePayloadKind,
    pub event_time_ns: i64,
    pub received_at_ns: i64,
}

impl MarketCacheReaderCheckpoint {
    #[must_use]
    pub fn from_event(
        reader_id: impl Into<String>,
        checkpoint_id: impl Into<String>,
        event: &MarketCacheEvent,
    ) -> Self {
        Self {
            reader_id: reader_id.into(),
            checkpoint_id: checkpoint_id.into(),
            source: event.source.clone(),
            symbol: event.symbol.clone(),
            payload_kind: event.payload_kind(),
            event_time_ns: event.event_time_ns(),
            received_at_ns: event.received_at_ns,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.reader_id.trim().is_empty() {
            return Err(DataError::Validation(
                "market cache reader id must not be empty".into(),
            ));
        }
        if self.checkpoint_id.trim().is_empty() {
            return Err(DataError::Validation(
                "market cache reader checkpoint id must not be empty".into(),
            ));
        }
        if self.source.trim().is_empty() {
            return Err(DataError::Validation(
                "market cache reader checkpoint source must not be empty".into(),
            ));
        }
        if self.symbol.trim().is_empty() {
            return Err(DataError::Validation(
                "market cache reader checkpoint symbol must not be empty".into(),
            ));
        }
        if self.event_time_ns < 0 || self.received_at_ns < 0 {
            return Err(DataError::Validation(
                "market cache reader checkpoint times must be non-negative".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketCacheReaderLag {
    pub reader_id: String,
    pub checkpoint_id: String,
    pub event_time_ns: i64,
    pub lag_event_time_ns: i64,
}

#[derive(Debug, Clone)]
pub struct MarketCacheReaderManifest {
    path: PathBuf,
}

impl MarketCacheReaderManifest {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let manifest = Self {
            path: path.as_ref().to_path_buf(),
        };
        if !manifest.path.exists() || std::fs::metadata(&manifest.path)?.len() == 0 {
            manifest.write_state(&MarketCacheReaderManifestState::default())?;
        } else {
            manifest.read_state()?;
        }
        Ok(manifest)
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn record_checkpoint(&self, checkpoint: MarketCacheReaderCheckpoint) -> Result<()> {
        checkpoint.validate()?;
        let mut state = self.read_state()?;
        state
            .checkpoints
            .insert(checkpoint.reader_id.clone(), checkpoint);
        self.write_state(&state)
    }

    pub fn checkpoint(&self, reader_id: &str) -> Result<Option<MarketCacheReaderCheckpoint>> {
        validate_market_cache_reader_id(reader_id)?;
        Ok(self.read_state()?.checkpoints.get(reader_id).cloned())
    }

    pub fn checkpoints(&self) -> Result<Vec<MarketCacheReaderCheckpoint>> {
        Ok(self.read_state()?.checkpoints.into_values().collect())
    }

    pub fn remove_reader(&self, reader_id: &str) -> Result<bool> {
        validate_market_cache_reader_id(reader_id)?;
        let mut state = self.read_state()?;
        let removed = state.checkpoints.remove(reader_id).is_some();
        if removed {
            self.write_state(&state)?;
        }
        Ok(removed)
    }

    pub fn compaction_floor_event_time_ns(&self) -> Result<Option<i64>> {
        Ok(self
            .read_state()?
            .checkpoints
            .values()
            .map(|checkpoint| checkpoint.event_time_ns)
            .min())
    }

    pub fn reader_lag_report(&self, head_event_time_ns: i64) -> Result<Vec<MarketCacheReaderLag>> {
        if head_event_time_ns < 0 {
            return Err(DataError::Validation(
                "market cache reader lag head event time must be non-negative".into(),
            ));
        }
        let mut report = self
            .read_state()?
            .checkpoints
            .into_values()
            .map(|checkpoint| MarketCacheReaderLag {
                reader_id: checkpoint.reader_id,
                checkpoint_id: checkpoint.checkpoint_id,
                event_time_ns: checkpoint.event_time_ns,
                lag_event_time_ns: head_event_time_ns.saturating_sub(checkpoint.event_time_ns),
            })
            .collect::<Vec<_>>();
        report.sort_by(|left, right| {
            right
                .lag_event_time_ns
                .cmp(&left.lag_event_time_ns)
                .then_with(|| left.reader_id.cmp(&right.reader_id))
        });
        Ok(report)
    }

    fn read_state(&self) -> Result<MarketCacheReaderManifestState> {
        let content = match std::fs::read_to_string(&self.path) {
            Ok(content) => content,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(MarketCacheReaderManifestState::default());
            }
            Err(error) => return Err(DataError::Io(error)),
        };
        if content.trim().is_empty() {
            return Ok(MarketCacheReaderManifestState::default());
        }
        let state: MarketCacheReaderManifestState = serde_json::from_str(&content)?;
        for checkpoint in state.checkpoints.values() {
            checkpoint.validate()?;
        }
        Ok(state)
    }

    fn write_state(&self, state: &MarketCacheReaderManifestState) -> Result<()> {
        let staging_path = path_with_suffix(&self.path, ".tmp");
        {
            let mut file = File::create(&staging_path)?;
            serde_json::to_writer_pretty(&mut file, state)?;
            file.write_all(b"\n")?;
            file.flush()?;
        }
        std::fs::rename(staging_path, &self.path)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct MarketCacheReaderManifestState {
    checkpoints: BTreeMap<String, MarketCacheReaderCheckpoint>,
}

fn validate_market_cache_reader_id(reader_id: &str) -> Result<()> {
    if reader_id.trim().is_empty() {
        return Err(DataError::Validation(
            "market cache reader id must not be empty".into(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketCacheRecoveryFileKind {
    Cache,
    Queue,
    ProcessingQueue,
    CompactionStaging,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketCacheRecoveryFileReport {
    pub kind: MarketCacheRecoveryFileKind,
    pub path: PathBuf,
    pub exists: bool,
    pub bytes: u64,
    pub readable_events: usize,
    pub first_event_time_ns: Option<i64>,
    pub last_event_time_ns: Option<i64>,
    pub read_error: Option<String>,
}

impl MarketCacheRecoveryFileReport {
    #[must_use]
    pub fn has_events(&self) -> bool {
        self.readable_events > 0
    }

    #[must_use]
    pub fn has_bytes(&self) -> bool {
        self.bytes > 0
    }

    #[must_use]
    pub fn is_readable(&self) -> bool {
        self.read_error.is_none()
    }
}

#[derive(Debug, Clone)]
pub struct MarketCacheRecoveryScan {
    cache_path: PathBuf,
    queue_path: PathBuf,
    processing_queue_path: PathBuf,
    compaction_staging_path: PathBuf,
}

impl MarketCacheRecoveryScan {
    #[must_use]
    pub fn new(cache_path: impl AsRef<Path>) -> Self {
        let cache_path = cache_path.as_ref().to_path_buf();
        let queue_path = path_with_suffix(&cache_path, ".queue");
        let processing_queue_path = path_with_suffix(&queue_path, ".processing");
        let compaction_staging_path = path_with_suffix(&cache_path, ".compact");
        Self {
            cache_path,
            queue_path,
            processing_queue_path,
            compaction_staging_path,
        }
    }

    #[must_use]
    pub fn queue_path(mut self, queue_path: impl AsRef<Path>) -> Self {
        self.queue_path = queue_path.as_ref().to_path_buf();
        self.processing_queue_path = path_with_suffix(&self.queue_path, ".processing");
        self
    }

    #[must_use]
    pub fn processing_queue_path(mut self, processing_queue_path: impl AsRef<Path>) -> Self {
        self.processing_queue_path = processing_queue_path.as_ref().to_path_buf();
        self
    }

    #[must_use]
    pub fn compaction_staging_path(mut self, compaction_staging_path: impl AsRef<Path>) -> Self {
        self.compaction_staging_path = compaction_staging_path.as_ref().to_path_buf();
        self
    }

    pub fn scan(&self) -> Result<MarketCacheRecoveryReport> {
        self.validate()?;
        Ok(MarketCacheRecoveryReport {
            cache: scan_market_cache_recovery_file(
                MarketCacheRecoveryFileKind::Cache,
                &self.cache_path,
            )?,
            queue: scan_market_cache_recovery_file(
                MarketCacheRecoveryFileKind::Queue,
                &self.queue_path,
            )?,
            processing_queue: scan_market_cache_recovery_file(
                MarketCacheRecoveryFileKind::ProcessingQueue,
                &self.processing_queue_path,
            )?,
            compaction_staging: scan_market_cache_recovery_file(
                MarketCacheRecoveryFileKind::CompactionStaging,
                &self.compaction_staging_path,
            )?,
        })
    }

    fn validate(&self) -> Result<()> {
        if self.cache_path == self.queue_path {
            return Err(DataError::Validation(
                "market cache recovery cache and queue paths must differ".into(),
            ));
        }
        if self.queue_path == self.processing_queue_path {
            return Err(DataError::Validation(
                "market cache recovery queue and processing queue paths must differ".into(),
            ));
        }
        if self.cache_path == self.compaction_staging_path {
            return Err(DataError::Validation(
                "market cache recovery cache and compaction staging paths must differ".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketCacheRecoveryReport {
    pub cache: MarketCacheRecoveryFileReport,
    pub queue: MarketCacheRecoveryFileReport,
    pub processing_queue: MarketCacheRecoveryFileReport,
    pub compaction_staging: MarketCacheRecoveryFileReport,
}

impl MarketCacheRecoveryReport {
    #[must_use]
    pub fn has_pending_queue_events(&self) -> bool {
        self.queue.has_events() || self.processing_queue.has_events()
    }

    #[must_use]
    pub fn has_interrupted_drain(&self) -> bool {
        self.processing_queue.has_bytes()
    }

    #[must_use]
    pub fn has_interrupted_compaction(&self) -> bool {
        self.compaction_staging.has_bytes()
    }

    #[must_use]
    pub fn has_read_errors(&self) -> bool {
        self.files().any(|file| !file.is_readable())
    }

    #[must_use]
    pub fn requires_writer_recovery(&self) -> bool {
        self.has_interrupted_drain() || self.has_interrupted_compaction() || self.has_read_errors()
    }

    pub fn files(&self) -> impl Iterator<Item = &MarketCacheRecoveryFileReport> {
        [
            &self.cache,
            &self.queue,
            &self.processing_queue,
            &self.compaction_staging,
        ]
        .into_iter()
    }
}

fn scan_market_cache_recovery_file(
    kind: MarketCacheRecoveryFileKind,
    path: &Path,
) -> Result<MarketCacheRecoveryFileReport> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(MarketCacheRecoveryFileReport {
                kind,
                path: path.to_path_buf(),
                exists: false,
                bytes: 0,
                readable_events: 0,
                first_event_time_ns: None,
                last_event_time_ns: None,
                read_error: None,
            });
        }
        Err(error) => return Err(DataError::Io(error)),
    };
    let mut report = MarketCacheRecoveryFileReport {
        kind,
        path: path.to_path_buf(),
        exists: true,
        bytes: metadata.len(),
        readable_events: 0,
        first_event_time_ns: None,
        last_event_time_ns: None,
        read_error: None,
    };
    if metadata.len() == 0 {
        return Ok(report);
    }

    for event in MarketCacheReader::open(path)? {
        let event = match event {
            Ok(event) => event,
            Err(error) => {
                report.read_error = Some(error.to_string());
                break;
            }
        };
        let event_time_ns = event.event_time_ns();
        report.first_event_time_ns = Some(
            report
                .first_event_time_ns
                .map_or(event_time_ns, |time| time.min(event_time_ns)),
        );
        report.last_event_time_ns = Some(
            report
                .last_event_time_ns
                .map_or(event_time_ns, |time| time.max(event_time_ns)),
        );
        report.readable_events += 1;
    }
    Ok(report)
}

#[derive(Debug, Clone)]
pub struct MarketCacheLockOptions {
    path: PathBuf,
    stale_after: Option<Duration>,
}

impl MarketCacheLockOptions {
    #[must_use]
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            stale_after: None,
        }
    }

    #[must_use]
    pub fn stale_after(mut self, stale_after: Duration) -> Self {
        self.stale_after = Some(stale_after);
        self
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn stale_after_duration(&self) -> Option<Duration> {
        self.stale_after
    }
}

#[derive(Debug)]
pub struct MarketCacheLock {
    path: PathBuf,
    file: File,
    lease_started_at_ns: i64,
}

impl MarketCacheLock {
    pub fn acquire(path: impl AsRef<Path>) -> Result<Self> {
        Self::acquire_with_options(MarketCacheLockOptions::new(path))
    }

    pub fn acquire_with_options(options: MarketCacheLockOptions) -> Result<Self> {
        let path = options.path.clone();
        match create_lock_file(&path) {
            Ok(file) => Self::from_file(path, file),
            Err(error)
                if error.kind() == std::io::ErrorKind::AlreadyExists
                    && lock_file_is_stale(&path, options.stale_after)? =>
            {
                std::fs::remove_file(&path)?;
                Self::from_file(path.clone(), create_lock_file(&path)?)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                Err(DataError::InvalidState("market cache lock is already held"))
            }
            Err(error) => Err(DataError::Io(error)),
        }
    }

    fn from_file(path: PathBuf, mut file: File) -> Result<Self> {
        let lease_started_at_ns = write_lock_lease(&mut file)?;
        Ok(Self {
            path,
            file,
            lease_started_at_ns,
        })
    }

    pub fn renew(&mut self) -> Result<()> {
        let lease_started_at_ns = write_lock_lease(&mut self.file)?;
        if read_lock_lease_started_at_ns(&self.path)? != Some(lease_started_at_ns) {
            return Err(DataError::InvalidState(
                "market cache lock lease file was replaced",
            ));
        }
        self.lease_started_at_ns = lease_started_at_ns;
        Ok(())
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn lease_started_at_ns(&self) -> i64 {
        self.lease_started_at_ns
    }
}

fn create_lock_file(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().write(true).create_new(true).open(path)
}

fn write_lock_lease(file: &mut File) -> Result<i64> {
    let lease_started_at_ns = system_time_ns()?;
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    writeln!(file, "pid={}", std::process::id())?;
    writeln!(file, "lease_started_at_ns={lease_started_at_ns}")?;
    file.flush()?;
    Ok(lease_started_at_ns)
}

fn lock_file_is_stale(path: &Path, stale_after: Option<Duration>) -> Result<bool> {
    let Some(stale_after) = stale_after else {
        return Ok(false);
    };
    if stale_after.is_zero() {
        return Ok(true);
    }
    if let Some(lease_started_at_ns) = read_lock_lease_started_at_ns(path)? {
        let now = system_time_ns()?;
        return Ok(now
            .saturating_sub(lease_started_at_ns)
            .try_into()
            .is_ok_and(|age_ns: u128| age_ns >= stale_after.as_nanos()));
    }
    Ok(std::fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| SystemTime::now().duration_since(modified).ok())
        .is_some_and(|age| age >= stale_after))
}

fn read_lock_lease_started_at_ns(path: &Path) -> Result<Option<i64>> {
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(DataError::Io(error)),
    };
    Ok(content.lines().find_map(|line| {
        line.strip_prefix("lease_started_at_ns=")
            .and_then(|value| value.trim().parse::<i64>().ok())
    }))
}

impl Drop for MarketCacheLock {
    fn drop(&mut self) {
        if read_lock_lease_started_at_ns(&self.path)
            .ok()
            .flatten()
            .is_some_and(|lease| lease == self.lease_started_at_ns)
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[derive(Debug, Clone)]
pub struct MarketCacheWriterElection {
    lock_path: PathBuf,
    stale_after: Option<Duration>,
}

impl MarketCacheWriterElection {
    #[must_use]
    pub fn new(lock_path: impl AsRef<Path>) -> Self {
        Self {
            lock_path: lock_path.as_ref().to_path_buf(),
            stale_after: None,
        }
    }

    #[must_use]
    pub fn stale_after(mut self, stale_after: Duration) -> Self {
        self.stale_after = Some(stale_after);
        self
    }

    pub fn elect(&self) -> Result<MarketCacheWriterElectionOutcome> {
        match create_lock_file(&self.lock_path) {
            Ok(file) => self.elected(file, false),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if !lock_file_is_stale(&self.lock_path, self.stale_after)? {
                    return Ok(MarketCacheWriterElectionOutcome::busy(&self.lock_path));
                }
                match std::fs::remove_file(&self.lock_path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(DataError::Io(error)),
                }
                match create_lock_file(&self.lock_path) {
                    Ok(file) => self.elected(file, true),
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        Ok(MarketCacheWriterElectionOutcome::busy(&self.lock_path))
                    }
                    Err(error) => Err(DataError::Io(error)),
                }
            }
            Err(error) => Err(DataError::Io(error)),
        }
    }

    fn elected(
        &self,
        file: File,
        recovered_stale: bool,
    ) -> Result<MarketCacheWriterElectionOutcome> {
        let lease = MarketCacheWriterLease {
            lock: MarketCacheLock::from_file(self.lock_path.clone(), file)?,
            recovered_stale,
        };
        let lease_started_at_ns = lease.lease_started_at_ns();
        Ok(MarketCacheWriterElectionOutcome {
            report: MarketCacheWriterElectionReport {
                lock_path: self.lock_path.clone(),
                status: MarketCacheWriterElectionStatus::Elected,
                recovered_stale,
                lease_started_at_ns: Some(lease_started_at_ns),
            },
            lease: Some(lease),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketCacheWriterElectionStatus {
    Elected,
    Busy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketCacheWriterElectionReport {
    pub lock_path: PathBuf,
    pub status: MarketCacheWriterElectionStatus,
    pub recovered_stale: bool,
    pub lease_started_at_ns: Option<i64>,
}

#[derive(Debug)]
pub struct MarketCacheWriterElectionOutcome {
    report: MarketCacheWriterElectionReport,
    lease: Option<MarketCacheWriterLease>,
}

impl MarketCacheWriterElectionOutcome {
    fn busy(lock_path: &Path) -> Self {
        Self {
            report: MarketCacheWriterElectionReport {
                lock_path: lock_path.to_path_buf(),
                status: MarketCacheWriterElectionStatus::Busy,
                recovered_stale: false,
                lease_started_at_ns: None,
            },
            lease: None,
        }
    }

    #[must_use]
    pub fn report(&self) -> &MarketCacheWriterElectionReport {
        &self.report
    }

    #[must_use]
    pub fn is_elected(&self) -> bool {
        self.lease.is_some()
    }

    #[must_use]
    pub fn is_busy(&self) -> bool {
        self.lease.is_none()
    }

    #[must_use]
    pub fn into_lease(self) -> Option<MarketCacheWriterLease> {
        self.lease
    }
}

#[derive(Debug)]
pub struct MarketCacheWriterLease {
    lock: MarketCacheLock,
    recovered_stale: bool,
}

impl MarketCacheWriterLease {
    #[must_use]
    pub fn path(&self) -> &Path {
        self.lock.path()
    }

    #[must_use]
    pub fn recovered_stale(&self) -> bool {
        self.recovered_stale
    }

    #[must_use]
    pub fn lease_started_at_ns(&self) -> i64 {
        self.lock.lease_started_at_ns()
    }

    pub fn renew(&mut self) -> Result<()> {
        self.lock.renew()
    }
}

#[derive(Debug)]
pub struct MarketCacheQueueDrainError {
    pub report: MarketCacheQueueDrainReport,
    pub error: DataError,
}

impl Display for MarketCacheQueueDrainError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "market cache queue drain failed after reading {} event(s) and writing {} event(s): {}",
            self.report.read_events, self.report.written_events, self.error
        )
    }
}

impl std::error::Error for MarketCacheQueueDrainError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

impl From<MarketCacheQueueDrainError> for DataError {
    fn from(error: MarketCacheQueueDrainError) -> Self {
        error.error
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
        self.drain_to_writer_with_report(writer)
            .map_err(DataError::from)
    }

    pub fn drain_to_writer_with_report<W: Write>(
        &self,
        writer: &mut MarketCacheWriter<W>,
    ) -> std::result::Result<MarketCacheQueueDrainReport, MarketCacheQueueDrainError> {
        let mut report = MarketCacheQueueDrainReport {
            queue_path: self.path.clone(),
            read_events: 0,
            written_events: 0,
        };
        let reader = self.reader().map_err(|error| MarketCacheQueueDrainError {
            report: report.clone(),
            error,
        })?;
        for event in reader {
            let event = event.map_err(|error| MarketCacheQueueDrainError {
                report: report.clone(),
                error,
            })?;
            report.read_events += 1;
            writer
                .write_event(&event)
                .map_err(|error| MarketCacheQueueDrainError {
                    report: report.clone(),
                    error,
                })?;
            report.written_events += 1;
        }
        writer.flush().map_err(|error| MarketCacheQueueDrainError {
            report: report.clone(),
            error,
        })?;
        self.clear().map_err(|error| MarketCacheQueueDrainError {
            report: report.clone(),
            error,
        })?;
        Ok(report)
    }

    pub fn drain_to_writer_rotating<W: Write>(
        &self,
        writer: &mut MarketCacheWriter<W>,
        processing_path: impl AsRef<Path>,
    ) -> std::result::Result<MarketCacheQueueDrainReport, MarketCacheQueueDrainError> {
        let processing_path = processing_path.as_ref();
        let mut report = MarketCacheQueueDrainReport {
            queue_path: self.path.clone(),
            read_events: 0,
            written_events: 0,
        };
        if processing_path == self.path {
            return Err(MarketCacheQueueDrainError {
                report,
                error: DataError::Validation(
                    "market cache processing queue path must differ from queue path".into(),
                ),
            });
        }

        self.drain_processing_file(writer, processing_path, &mut report)?;
        if self
            .is_empty()
            .map_err(|error| MarketCacheQueueDrainError {
                report: report.clone(),
                error,
            })?
        {
            writer.flush().map_err(|error| MarketCacheQueueDrainError {
                report: report.clone(),
                error,
            })?;
            return Ok(report);
        }

        std::fs::rename(&self.path, processing_path).map_err(|error| {
            MarketCacheQueueDrainError {
                report: report.clone(),
                error: DataError::Io(error),
            }
        })?;
        self.clear().map_err(|error| MarketCacheQueueDrainError {
            report: report.clone(),
            error,
        })?;
        self.drain_processing_file(writer, processing_path, &mut report)?;
        writer.flush().map_err(|error| MarketCacheQueueDrainError {
            report: report.clone(),
            error,
        })?;
        Ok(report)
    }

    fn drain_processing_file<W: Write>(
        &self,
        writer: &mut MarketCacheWriter<W>,
        processing_path: &Path,
        report: &mut MarketCacheQueueDrainReport,
    ) -> std::result::Result<(), MarketCacheQueueDrainError> {
        let metadata = match std::fs::metadata(processing_path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(MarketCacheQueueDrainError {
                    report: report.clone(),
                    error: DataError::Io(error),
                });
            }
        };
        if metadata.len() == 0 {
            std::fs::remove_file(processing_path).map_err(|error| MarketCacheQueueDrainError {
                report: report.clone(),
                error: DataError::Io(error),
            })?;
            return Ok(());
        }

        let reader = MarketCacheReader::open(processing_path).map_err(|error| {
            MarketCacheQueueDrainError {
                report: report.clone(),
                error,
            }
        })?;
        for event in reader {
            let event = event.map_err(|error| MarketCacheQueueDrainError {
                report: report.clone(),
                error,
            })?;
            report.read_events += 1;
            writer
                .write_event(&event)
                .map_err(|error| MarketCacheQueueDrainError {
                    report: report.clone(),
                    error,
                })?;
            report.written_events += 1;
        }
        writer.flush().map_err(|error| MarketCacheQueueDrainError {
            report: report.clone(),
            error,
        })?;
        std::fs::remove_file(processing_path).map_err(|error| MarketCacheQueueDrainError {
            report: report.clone(),
            error: DataError::Io(error),
        })?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketCacheQueueDrainReport {
    pub queue_path: PathBuf,
    pub read_events: usize,
    pub written_events: usize,
}

#[derive(Debug, Clone)]
pub struct MarketCacheRecoveryAction {
    cache_path: PathBuf,
    queue_path: PathBuf,
    processing_queue_path: PathBuf,
    compaction_staging_path: PathBuf,
}

impl MarketCacheRecoveryAction {
    #[must_use]
    pub fn new(cache_path: impl AsRef<Path>) -> Self {
        let cache_path = cache_path.as_ref().to_path_buf();
        let queue_path = path_with_suffix(&cache_path, ".queue");
        let processing_queue_path = path_with_suffix(&queue_path, ".processing");
        let compaction_staging_path = path_with_suffix(&cache_path, ".compact");
        Self {
            cache_path,
            queue_path,
            processing_queue_path,
            compaction_staging_path,
        }
    }

    #[must_use]
    pub fn queue_path(mut self, queue_path: impl AsRef<Path>) -> Self {
        self.queue_path = queue_path.as_ref().to_path_buf();
        self.processing_queue_path = path_with_suffix(&self.queue_path, ".processing");
        self
    }

    #[must_use]
    pub fn processing_queue_path(mut self, processing_queue_path: impl AsRef<Path>) -> Self {
        self.processing_queue_path = processing_queue_path.as_ref().to_path_buf();
        self
    }

    #[must_use]
    pub fn compaction_staging_path(mut self, compaction_staging_path: impl AsRef<Path>) -> Self {
        self.compaction_staging_path = compaction_staging_path.as_ref().to_path_buf();
        self
    }

    pub fn recover(
        &self,
        writer_lease: &mut MarketCacheWriterLease,
    ) -> Result<MarketCacheRecoveryActionReport> {
        writer_lease.renew()?;
        let scan_before = self.scan()?;
        if scan_before.has_read_errors() {
            return Err(DataError::InvalidState(
                "market cache recovery action requires readable cache and queue files",
            ));
        }

        let mut writer = MarketCacheWriter::append(&self.cache_path)?;
        let queue = MarketCacheQueue::open(&self.queue_path)?;
        let queue_drain_report = queue
            .drain_to_writer_rotating(&mut writer, &self.processing_queue_path)
            .map_err(DataError::from)?;
        writer_lease.renew()?;
        let scan_after = self.scan()?;
        Ok(MarketCacheRecoveryActionReport {
            scan_before,
            queue_drain_report,
            scan_after,
        })
    }

    fn scan(&self) -> Result<MarketCacheRecoveryReport> {
        MarketCacheRecoveryScan::new(&self.cache_path)
            .queue_path(&self.queue_path)
            .processing_queue_path(&self.processing_queue_path)
            .compaction_staging_path(&self.compaction_staging_path)
            .scan()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketCacheRecoveryActionReport {
    pub scan_before: MarketCacheRecoveryReport,
    pub queue_drain_report: MarketCacheQueueDrainReport,
    pub scan_after: MarketCacheRecoveryReport,
}

impl MarketCacheRecoveryActionReport {
    #[must_use]
    pub fn recovered_events(&self) -> usize {
        self.queue_drain_report.written_events
    }

    #[must_use]
    pub fn requires_follow_up(&self) -> bool {
        self.scan_after.requires_writer_recovery()
    }
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

    pub fn compact_file_in_place(
        &self,
        cache_path: impl AsRef<Path>,
        staging_path: impl AsRef<Path>,
    ) -> Result<MarketCacheAtomicCompactionReport> {
        let cache_path = cache_path.as_ref().to_path_buf();
        let staging_path = staging_path.as_ref().to_path_buf();
        if cache_path == staging_path {
            return Err(DataError::Validation(
                "market cache compaction cache and staging paths must differ".into(),
            ));
        }
        let compaction = self.compact_file(&cache_path, &staging_path)?;
        std::fs::rename(&staging_path, &cache_path)?;
        Ok(MarketCacheAtomicCompactionReport {
            cache_path,
            staging_path,
            compaction,
        })
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

#[derive(Debug, Clone)]
pub struct MarketCacheAtomicCompactionReport {
    pub cache_path: PathBuf,
    pub staging_path: PathBuf,
    pub compaction: MarketCacheCompactionReport,
}

#[derive(Debug, Clone)]
pub struct MarketCacheDaemonConfig {
    cache_path: PathBuf,
    queue_path: PathBuf,
    lock_path: PathBuf,
    compaction_staging_path: PathBuf,
    sync_on_enqueue: bool,
    stale_lock_after: Option<Duration>,
    compaction_policy: Option<MarketCacheCompaction>,
}

impl MarketCacheDaemonConfig {
    #[must_use]
    pub fn new(cache_path: impl AsRef<Path>) -> Self {
        let cache_path = cache_path.as_ref().to_path_buf();
        Self {
            queue_path: path_with_suffix(&cache_path, ".queue"),
            lock_path: path_with_suffix(&cache_path, ".lock"),
            compaction_staging_path: path_with_suffix(&cache_path, ".compact"),
            cache_path,
            sync_on_enqueue: false,
            stale_lock_after: None,
            compaction_policy: None,
        }
    }

    #[must_use]
    pub fn cache_path(&self) -> &Path {
        &self.cache_path
    }

    #[must_use]
    pub fn queue_path_ref(&self) -> &Path {
        &self.queue_path
    }

    #[must_use]
    pub fn lock_path_ref(&self) -> &Path {
        &self.lock_path
    }

    #[must_use]
    pub fn compaction_staging_path_ref(&self) -> &Path {
        &self.compaction_staging_path
    }

    #[must_use]
    pub fn with_sync_on_enqueue(mut self, sync_on_enqueue: bool) -> Self {
        self.sync_on_enqueue = sync_on_enqueue;
        self
    }

    #[must_use]
    pub fn queue_path(mut self, queue_path: impl AsRef<Path>) -> Self {
        self.queue_path = queue_path.as_ref().to_path_buf();
        self
    }

    #[must_use]
    pub fn lock_path(mut self, lock_path: impl AsRef<Path>) -> Self {
        self.lock_path = lock_path.as_ref().to_path_buf();
        self
    }

    #[must_use]
    pub fn compaction_staging_path(mut self, compaction_staging_path: impl AsRef<Path>) -> Self {
        self.compaction_staging_path = compaction_staging_path.as_ref().to_path_buf();
        self
    }

    #[must_use]
    pub fn stale_lock_after(mut self, stale_lock_after: Duration) -> Self {
        self.stale_lock_after = Some(stale_lock_after);
        self
    }

    #[must_use]
    pub fn compaction_policy(mut self, compaction_policy: MarketCacheCompaction) -> Self {
        self.compaction_policy = Some(compaction_policy);
        self
    }
}

#[derive(Debug)]
pub struct MarketCacheDaemon {
    config: MarketCacheDaemonConfig,
    queue: MarketCacheQueue,
    lock: MarketCacheLock,
}

impl MarketCacheDaemon {
    pub fn open(config: MarketCacheDaemonConfig) -> Result<Self> {
        let lock = MarketCacheLock::acquire_with_options({
            let mut options = MarketCacheLockOptions::new(&config.lock_path);
            if let Some(stale_after) = config.stale_lock_after {
                options = options.stale_after(stale_after);
            }
            options
        })?;
        let queue = MarketCacheQueue::open(&config.queue_path)?
            .with_sync_on_enqueue(config.sync_on_enqueue);
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.cache_path)?;
        Ok(Self {
            config,
            queue,
            lock,
        })
    }

    #[must_use]
    pub fn cache_path(&self) -> &Path {
        &self.config.cache_path
    }

    #[must_use]
    pub fn queue_path(&self) -> &Path {
        self.queue.path()
    }

    pub fn enqueue_event(&self, event: &MarketCacheEvent) -> Result<()> {
        self.queue.enqueue_event(event)
    }

    pub fn flush_queue(
        &self,
    ) -> std::result::Result<MarketCacheQueueDrainReport, MarketCacheQueueDrainError> {
        let mut writer = MarketCacheWriter::append(&self.config.cache_path).map_err(|error| {
            MarketCacheQueueDrainError {
                report: MarketCacheQueueDrainReport {
                    queue_path: self.config.queue_path.clone(),
                    read_events: 0,
                    written_events: 0,
                },
                error,
            }
        })?;
        self.queue.drain_to_writer_with_report(&mut writer)
    }

    pub fn flush_queue_rotating(
        &self,
        processing_path: impl AsRef<Path>,
    ) -> std::result::Result<MarketCacheQueueDrainReport, MarketCacheQueueDrainError> {
        let mut writer = MarketCacheWriter::append(&self.config.cache_path).map_err(|error| {
            MarketCacheQueueDrainError {
                report: MarketCacheQueueDrainReport {
                    queue_path: self.config.queue_path.clone(),
                    read_events: 0,
                    written_events: 0,
                },
                error,
            }
        })?;
        self.queue
            .drain_to_writer_rotating(&mut writer, processing_path)
    }

    pub fn renew_lock(&mut self) -> Result<()> {
        self.lock.renew()
    }

    pub fn spawn_supervisor(
        self,
        config: MarketCacheSupervisorConfig,
    ) -> Result<MarketCacheSupervisor> {
        config.validate()?;
        let processing_queue_path = config
            .processing_queue_path
            .clone()
            .unwrap_or_else(|| path_with_suffix(&self.config.queue_path, ".processing"));
        if processing_queue_path == self.config.queue_path {
            return Err(DataError::Validation(
                "market cache supervisor processing queue path must differ from queue path".into(),
            ));
        }

        let queue = self.queue.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            run_market_cache_supervisor(self, config, processing_queue_path, thread_stop)
        });

        Ok(MarketCacheSupervisor {
            queue,
            stop,
            handle: Some(handle),
        })
    }

    pub fn shutdown(self) -> Result<MarketCacheDaemonShutdownReport> {
        let flush_report = self.flush_queue().map_err(DataError::from)?;
        let compaction_report = self
            .config
            .compaction_policy
            .as_ref()
            .map(|policy| {
                policy.compact_file_in_place(
                    &self.config.cache_path,
                    &self.config.compaction_staging_path,
                )
            })
            .transpose()?;
        let queue_empty = self.queue.is_empty()?;
        Ok(MarketCacheDaemonShutdownReport {
            flush_report,
            compaction_report,
            queue_empty,
        })
    }
}

#[derive(Debug, Clone)]
pub struct MarketCacheDaemonShutdownReport {
    pub flush_report: MarketCacheQueueDrainReport,
    pub compaction_report: Option<MarketCacheAtomicCompactionReport>,
    pub queue_empty: bool,
}

#[derive(Debug, Clone)]
pub struct MarketCacheSupervisorConfig {
    flush_interval: Duration,
    lease_renew_interval: Duration,
    idle_sleep: Duration,
    processing_queue_path: Option<PathBuf>,
}

impl MarketCacheSupervisorConfig {
    #[must_use]
    pub fn new() -> Self {
        Self {
            flush_interval: Duration::from_secs(1),
            lease_renew_interval: Duration::from_secs(5),
            idle_sleep: Duration::from_millis(10),
            processing_queue_path: None,
        }
    }

    #[must_use]
    pub fn flush_interval(mut self, flush_interval: Duration) -> Self {
        self.flush_interval = flush_interval;
        self
    }

    #[must_use]
    pub fn lease_renew_interval(mut self, lease_renew_interval: Duration) -> Self {
        self.lease_renew_interval = lease_renew_interval;
        self
    }

    #[must_use]
    pub fn idle_sleep(mut self, idle_sleep: Duration) -> Self {
        self.idle_sleep = idle_sleep;
        self
    }

    #[must_use]
    pub fn processing_queue_path(mut self, processing_queue_path: impl AsRef<Path>) -> Self {
        self.processing_queue_path = Some(processing_queue_path.as_ref().to_path_buf());
        self
    }

    fn validate(&self) -> Result<()> {
        if self.flush_interval.is_zero() {
            return Err(DataError::Validation(
                "market cache supervisor flush interval must be positive".into(),
            ));
        }
        if self.lease_renew_interval.is_zero() {
            return Err(DataError::Validation(
                "market cache supervisor lease renew interval must be positive".into(),
            ));
        }
        if self.idle_sleep.is_zero() {
            return Err(DataError::Validation(
                "market cache supervisor idle sleep must be positive".into(),
            ));
        }
        Ok(())
    }
}

impl Default for MarketCacheSupervisorConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct MarketCacheSupervisor {
    queue: MarketCacheQueue,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<Result<MarketCacheSupervisorShutdownReport>>>,
}

impl MarketCacheSupervisor {
    pub fn enqueue_event(&self, event: &MarketCacheEvent) -> Result<()> {
        self.queue.enqueue_event(event)
    }

    pub fn shutdown(mut self) -> Result<MarketCacheSupervisorShutdownReport> {
        self.stop.store(true, Ordering::Release);
        let handle = self.handle.take().ok_or(DataError::InvalidState(
            "market cache supervisor is already shut down",
        ))?;
        handle
            .join()
            .map_err(|_| DataError::InvalidState("market cache supervisor thread panicked"))?
    }
}

impl Drop for MarketCacheSupervisor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[derive(Debug, Clone)]
pub struct MarketCacheSupervisorShutdownReport {
    pub periodic_flushes: usize,
    pub lease_renewals: usize,
    pub periodic_errors: usize,
    pub pre_shutdown_flush_report: MarketCacheQueueDrainReport,
    pub shutdown: MarketCacheDaemonShutdownReport,
}

fn run_market_cache_supervisor(
    mut daemon: MarketCacheDaemon,
    config: MarketCacheSupervisorConfig,
    processing_queue_path: PathBuf,
    stop: Arc<AtomicBool>,
) -> Result<MarketCacheSupervisorShutdownReport> {
    let mut periodic_flushes = 0;
    let mut lease_renewals = 0;
    let mut periodic_errors = 0;
    let now = Instant::now();
    let mut last_flush = now - config.flush_interval;
    let mut last_renew = now - config.lease_renew_interval;

    while !stop.load(Ordering::Acquire) {
        let now = Instant::now();
        if now.duration_since(last_flush) >= config.flush_interval {
            match daemon.flush_queue_rotating(&processing_queue_path) {
                Ok(_) => periodic_flushes += 1,
                Err(_) => periodic_errors += 1,
            }
            last_flush = now;
        }
        if now.duration_since(last_renew) >= config.lease_renew_interval {
            match daemon.renew_lock() {
                Ok(()) => lease_renewals += 1,
                Err(_) => periodic_errors += 1,
            }
            last_renew = now;
        }
        thread::sleep(config.idle_sleep);
    }

    let pre_shutdown_flush_report = daemon
        .flush_queue_rotating(&processing_queue_path)
        .map_err(DataError::from)?;
    let shutdown = daemon.shutdown()?;
    Ok(MarketCacheSupervisorShutdownReport {
        periodic_flushes,
        lease_renewals,
        periodic_errors,
        pre_shutdown_flush_report,
        shutdown,
    })
}

fn write_market_cache_event_line<W: Write>(writer: &mut W, event: &MarketCacheEvent) -> Result<()> {
    serde_json::to_writer(&mut *writer, event)?;
    writer.write_all(b"\n")?;
    Ok(())
}

fn path_with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn system_time_ns() -> Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| DataError::InvalidState("system clock is before unix epoch"))?;
    i64::try_from(elapsed.as_nanos())
        .map_err(|_| DataError::InvalidState("system clock nanoseconds overflow i64"))
}
