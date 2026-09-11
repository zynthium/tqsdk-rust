#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tqsdk_core::{Kline, Tick};
use tqsdk_data::{RollingMarketCache, RollingMarketCacheKind, RollingMarketCacheMetadata};

use crate::config::RollingCacheConfig;
use crate::error::{RelayError, RelayResult};

const WRITER_QUEUE_CAPACITY: usize = 2_048;

enum WriteIntent {
    SourceEpoch(u64),
    Tick(String, Tick),
    Kline(String, i64, Kline),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RollingWriterStatus {
    pub source_epoch: u64,
    pub enqueued_revision: u64,
    pub durable_revision: u64,
    pub discontinuities: u64,
    pub degraded: bool,
}

#[derive(Clone)]
pub struct RelayRollingCacheWriter {
    sender: mpsc::Sender<WriteIntent>,
    status: Arc<Mutex<RollingWriterStatus>>,
    next_source_epoch: Arc<std::sync::atomic::AtomicU64>,
}

impl RelayRollingCacheWriter {
    pub fn start(config: &RollingCacheConfig) -> RelayResult<Self> {
        let cache = RollingMarketCache::open(
            config.root().clone(),
            config.capacity(),
            config.session_hash(),
            config.aggregation_algorithm_version(),
        )
        .map_err(|error| RelayError::invalid_config(error.to_string()))?;
        let source_epoch_floor = rolling_cache_source_epoch_floor(&cache)?;
        let (sender, mut receiver) = mpsc::channel::<WriteIntent>(WRITER_QUEUE_CAPACITY);
        let status = Arc::new(Mutex::new(RollingWriterStatus::default()));
        let next_source_epoch = Arc::new(std::sync::atomic::AtomicU64::new(source_epoch_floor));
        let task_status = Arc::clone(&status);
        let capacity = config.capacity().get();
        let metadata_session_hash = config.session_hash().to_owned();
        let algorithm = config.aggregation_algorithm_version();
        tokio::spawn(async move {
            let mut rings = BTreeMap::<String, VecDeque<Tick>>::new();
            let mut kline_rings = BTreeMap::<(String, i64), VecDeque<Kline>>::new();
            let mut revision = 0_u64;
            let mut source_epoch = source_epoch_floor.saturating_add(1).max(1);
            while let Some(intent) = receiver.recv().await {
                let WriteIntent::Tick(symbol, tick) = intent else {
                    let WriteIntent::Kline(symbol, duration_ns, row) = intent else {
                        let WriteIntent::SourceEpoch(epoch) = intent else {
                            unreachable!();
                        };
                        source_epoch = source_epoch.max(epoch.max(1));
                        if let Ok(mut status) = task_status.lock() {
                            status.source_epoch = source_epoch;
                        } else {
                            break;
                        }
                        continue;
                    };
                    let key = (symbol.clone(), duration_ns);
                    if !kline_rings.contains_key(&key) {
                        let cache = cache.clone();
                        let load_symbol = symbol.clone();
                        let load_cache = cache.clone();
                        let restored = tokio::task::spawn_blocking(move || {
                            load_cache.load_klines(&load_symbol, duration_ns)
                        })
                        .await
                        .ok()
                        .and_then(Result::ok)
                        .flatten();
                        kline_rings.insert(
                            key.clone(),
                            restored.map_or_else(VecDeque::new, |snapshot| snapshot.rows.into()),
                        );
                    }
                    let ring = kline_rings.get_mut(&key).expect("initialized Kline ring");
                    if let Some(existing) = ring.iter_mut().find(|existing| existing.id == row.id) {
                        *existing = row;
                    } else {
                        ring.push_back(row);
                    }
                    while ring.len() > capacity {
                        let _ = ring.pop_front();
                    }
                    revision = revision.saturating_add(1);
                    let rows = ring.iter().cloned().collect::<Vec<_>>();
                    let metadata = RollingMarketCacheMetadata::new(
                        std::num::NonZeroUsize::new(capacity)
                            .expect("configured capacity is nonzero"),
                        metadata_session_hash.clone(),
                        algorithm,
                        source_epoch,
                        revision,
                        revision,
                        None,
                    );
                    let cache = cache.clone();
                    let persisted = tokio::task::spawn_blocking(move || {
                        cache.replace_klines(&symbol, duration_ns, metadata, &rows)
                    })
                    .await
                    .ok()
                    .and_then(Result::ok);
                    let Ok(mut status) = task_status.lock() else {
                        break;
                    };
                    match persisted {
                        Some(()) => status.durable_revision = revision,
                        None => status.degraded = true,
                    }
                    continue;
                };
                if !rings.contains_key(&symbol) {
                    let cache = cache.clone();
                    let load_symbol = symbol.clone();
                    let load_cache = cache.clone();
                    let restored =
                        tokio::task::spawn_blocking(move || load_cache.load_ticks(&load_symbol))
                            .await
                            .ok()
                            .and_then(Result::ok)
                            .flatten();
                    let Some(restored) = restored else {
                        rings.insert(symbol.clone(), VecDeque::new());
                        let ring = rings.get_mut(&symbol).expect("inserted empty ring");
                        ring.push_back(tick);
                        revision = revision.saturating_add(1);
                        let rows = ring.iter().cloned().collect::<Vec<_>>();
                        let metadata = RollingMarketCacheMetadata::new(
                            std::num::NonZeroUsize::new(capacity)
                                .expect("configured capacity is nonzero"),
                            metadata_session_hash.clone(),
                            algorithm,
                            source_epoch,
                            revision,
                            revision,
                            None,
                        );
                        let cache = cache.clone();
                        let persisted = tokio::task::spawn_blocking(move || {
                            cache.replace_ticks(&symbol, metadata, &rows)
                        })
                        .await
                        .ok()
                        .and_then(Result::ok);
                        let Ok(mut status) = task_status.lock() else {
                            break;
                        };
                        match persisted {
                            Some(()) => status.durable_revision = revision,
                            None => status.degraded = true,
                        }
                        continue;
                    };
                    rings.insert(symbol.clone(), restored.rows.into());
                }
                let ring = rings.entry(symbol.clone()).or_default();
                if let Some(existing) = ring.iter_mut().find(|existing| existing.id == tick.id) {
                    *existing = tick;
                } else {
                    let discontinuity = ring
                        .back()
                        .is_some_and(|last| tick.id != last.id.saturating_add(1));
                    if discontinuity {
                        ring.clear();
                        if let Ok(mut status) = task_status.lock() {
                            status.discontinuities = status.discontinuities.saturating_add(1);
                        }
                    }
                    ring.push_back(tick);
                }
                while ring.len() > capacity {
                    let _ = ring.pop_front();
                }
                revision = revision.saturating_add(1);
                let rows = ring.iter().cloned().collect::<Vec<_>>();
                let metadata = RollingMarketCacheMetadata::new(
                    std::num::NonZeroUsize::new(capacity).expect("configured capacity is nonzero"),
                    metadata_session_hash.clone(),
                    algorithm,
                    source_epoch,
                    revision,
                    revision,
                    None,
                );
                let cache = cache.clone();
                let persisted = tokio::task::spawn_blocking(move || {
                    cache.replace_ticks(&symbol, metadata, &rows)
                })
                .await
                .ok()
                .and_then(Result::ok);
                let Ok(mut status) = task_status.lock() else {
                    break;
                };
                match persisted {
                    Some(()) => status.durable_revision = revision,
                    None => status.degraded = true,
                }
            }
        });
        Ok(Self {
            sender,
            status,
            next_source_epoch,
        })
    }

    pub fn begin_source_epoch(&self) -> RelayResult<()> {
        let epoch = self
            .next_source_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .saturating_add(1);
        self.sender
            .try_send(WriteIntent::SourceEpoch(epoch))
            .map_err(|error| {
                if let Ok(mut status) = self.status.lock() {
                    status.degraded = true;
                }
                RelayError::Internal(format!("rolling cache writer unavailable: {error}"))
            })
    }

    pub fn enqueue(&self, ticks: Vec<(String, Tick)>) -> RelayResult<()> {
        for tick in ticks {
            self.sender
                .try_send(WriteIntent::Tick(tick.0, tick.1))
                .map_err(|error| {
                    if let Ok(mut status) = self.status.lock() {
                        status.degraded = true;
                    }
                    RelayError::Internal(format!("rolling cache writer unavailable: {error}"))
                })?;
            if let Ok(mut status) = self.status.lock() {
                status.enqueued_revision = status.enqueued_revision.saturating_add(1);
            }
        }
        Ok(())
    }

    pub fn enqueue_klines(&self, klines: Vec<(String, i64, Kline)>) -> RelayResult<()> {
        for (symbol, duration_ns, row) in klines {
            self.sender
                .try_send(WriteIntent::Kline(symbol, duration_ns, row))
                .map_err(|error| {
                    if let Ok(mut status) = self.status.lock() {
                        status.degraded = true;
                    }
                    RelayError::Internal(format!("rolling cache writer unavailable: {error}"))
                })?;
            if let Ok(mut status) = self.status.lock() {
                status.enqueued_revision = status.enqueued_revision.saturating_add(1);
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn status(&self) -> RollingWriterStatus {
        self.status
            .lock()
            .map(|status| *status)
            .unwrap_or(RollingWriterStatus {
                degraded: true,
                ..RollingWriterStatus::default()
            })
    }
}

fn rolling_cache_source_epoch_floor(cache: &RollingMarketCache) -> RelayResult<u64> {
    let mut floor = 0_u64;
    for entry in cache
        .entries()
        .map_err(|error| RelayError::invalid_config(error.to_string()))?
    {
        match entry.kind {
            RollingMarketCacheKind::Tick => {
                let snapshot = cache
                    .load_ticks(&entry.symbol)
                    .map_err(|error| RelayError::invalid_config(error.to_string()))?
                    .ok_or_else(|| {
                        RelayError::invalid_config(format!(
                            "rolling cache entry disappeared during writer startup: {}",
                            entry.symbol
                        ))
                    })?;
                floor = floor.max(snapshot.metadata.source_epoch);
            }
            RollingMarketCacheKind::Kline { duration_ns } => {
                let snapshot = cache
                    .load_klines(&entry.symbol, duration_ns)
                    .map_err(|error| RelayError::invalid_config(error.to_string()))?
                    .ok_or_else(|| {
                        RelayError::invalid_config(format!(
                            "rolling cache entry disappeared during writer startup: {}",
                            entry.symbol
                        ))
                    })?;
                floor = floor.max(snapshot.metadata.source_epoch);
            }
        }
    }
    Ok(floor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn writer_persists_lossless_tick_before_reporting_durable() {
        let root = std::env::temp_dir().join(format!(
            "relay-rolling-writer-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::create_dir(&root).unwrap();
        let config = RollingCacheConfig::new(&root, "session-v1", 1).unwrap();
        let writer = RelayRollingCacheWriter::start(&config).unwrap();
        writer.begin_source_epoch().unwrap();
        writer
            .enqueue(vec![(
                "SHFE.au2602".to_owned(),
                Tick {
                    id: 7,
                    datetime: 100,
                    last_price: 610.5,
                    ..Tick::default()
                },
            )])
            .unwrap();
        for _ in 0..100 {
            if writer.status().durable_revision == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(writer.status().durable_revision, 1);
        let cache = RollingMarketCache::open(
            &root,
            config.capacity(),
            config.session_hash(),
            config.aggregation_algorithm_version(),
        )
        .unwrap();
        let snapshot = cache.load_ticks("SHFE.au2602").unwrap().unwrap();
        assert_eq!(snapshot.metadata.source_epoch, 1);
        let rows = snapshot.rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, 7);
        assert_eq!(rows[0].last_price, 610.5);
        writer
            .enqueue_klines(vec![(
                "SHFE.au2602".to_owned(),
                60_000_000_000,
                Kline {
                    id: 3,
                    datetime: 120,
                    close: 611.0,
                    ..Kline::default()
                },
            )])
            .unwrap();
        for _ in 0..100 {
            if writer.status().durable_revision == 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let klines = cache
            .load_klines("SHFE.au2602", 60_000_000_000)
            .unwrap()
            .unwrap()
            .rows;
        assert_eq!(klines[0].close, 611.0);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn tick_gap_rebaselines_persisted_ring() {
        let root = std::env::temp_dir().join(format!(
            "relay-rolling-writer-gap-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        let config = RollingCacheConfig::new(&root, "session-v1", 1).unwrap();
        let writer = RelayRollingCacheWriter::start(&config).unwrap();
        writer.begin_source_epoch().unwrap();
        writer
            .enqueue(vec![
                (
                    "SHFE.au2602".to_owned(),
                    Tick {
                        id: 7,
                        ..Tick::default()
                    },
                ),
                (
                    "SHFE.au2602".to_owned(),
                    Tick {
                        id: 9,
                        ..Tick::default()
                    },
                ),
            ])
            .unwrap();
        for _ in 0..100 {
            if writer.status().durable_revision == 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        let cache = RollingMarketCache::open(
            &root,
            config.capacity(),
            config.session_hash(),
            config.aggregation_algorithm_version(),
        )
        .unwrap();
        let rows = cache.load_ticks("SHFE.au2602").unwrap().unwrap().rows;
        assert_eq!(rows.iter().map(|row| row.id).collect::<Vec<_>>(), vec![9]);
        assert_eq!(writer.status().discontinuities, 1);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn source_epoch_advances_across_writer_restart() {
        let root = std::env::temp_dir().join(format!(
            "relay-rolling-writer-epoch-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        let config = RollingCacheConfig::new(&root, "session-v1", 1).unwrap();
        let first = RelayRollingCacheWriter::start(&config).unwrap();
        first.begin_source_epoch().unwrap();
        first
            .enqueue(vec![(
                "SHFE.au2602".to_owned(),
                Tick {
                    id: 1,
                    ..Tick::default()
                },
            )])
            .unwrap();
        for _ in 0..100 {
            if first.status().durable_revision == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        drop(first);

        let second = RelayRollingCacheWriter::start(&config).unwrap();
        second.begin_source_epoch().unwrap();
        second
            .enqueue(vec![(
                "SHFE.au2602".to_owned(),
                Tick {
                    id: 2,
                    ..Tick::default()
                },
            )])
            .unwrap();
        for _ in 0..100 {
            if second.status().durable_revision == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        let cache = RollingMarketCache::open(
            &root,
            config.capacity(),
            config.session_hash(),
            config.aggregation_algorithm_version(),
        )
        .unwrap();
        assert_eq!(
            cache
                .load_ticks("SHFE.au2602")
                .unwrap()
                .unwrap()
                .metadata
                .source_epoch,
            2
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
