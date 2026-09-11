#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tqsdk_core::Tick;
use tqsdk_data::{RollingMarketCache, RollingMarketCacheMetadata};

use crate::config::RollingCacheConfig;
use crate::error::{RelayError, RelayResult};

const WRITER_QUEUE_CAPACITY: usize = 2_048;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RollingWriterStatus {
    pub enqueued_revision: u64,
    pub durable_revision: u64,
    pub degraded: bool,
}

#[derive(Clone)]
pub struct RelayRollingCacheWriter {
    sender: mpsc::Sender<(String, Tick)>,
    status: Arc<Mutex<RollingWriterStatus>>,
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
        let (sender, mut receiver) = mpsc::channel::<(String, Tick)>(WRITER_QUEUE_CAPACITY);
        let status = Arc::new(Mutex::new(RollingWriterStatus::default()));
        let task_status = Arc::clone(&status);
        let capacity = config.capacity().get();
        let metadata_session_hash = config.session_hash().to_owned();
        let algorithm = config.aggregation_algorithm_version();
        tokio::spawn(async move {
            let mut rings = BTreeMap::<String, VecDeque<Tick>>::new();
            let mut revision = 0_u64;
            while let Some((symbol, tick)) = receiver.recv().await {
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
                            1,
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
                ring.push_back(tick);
                while ring.len() > capacity {
                    let _ = ring.pop_front();
                }
                revision = revision.saturating_add(1);
                let rows = ring.iter().cloned().collect::<Vec<_>>();
                let metadata = RollingMarketCacheMetadata::new(
                    std::num::NonZeroUsize::new(capacity).expect("configured capacity is nonzero"),
                    metadata_session_hash.clone(),
                    algorithm,
                    1,
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
        Ok(Self { sender, status })
    }

    pub fn enqueue(&self, ticks: Vec<(String, Tick)>) -> RelayResult<()> {
        for tick in ticks {
            self.sender.try_send(tick).map_err(|error| {
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
        let rows = cache.load_ticks("SHFE.au2602").unwrap().unwrap().rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, 7);
        assert_eq!(rows[0].last_price, 610.5);
        let _ = std::fs::remove_dir_all(root);
    }
}
