//! Relay-private pointer to the immutable generation selected by `CURRENT`.
//!
//! This type only opens `tqsdk-data`'s public snapshot seam. It neither reads
//! cache files directly nor reaches the relay market runtime.

use std::ops::Deref;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::sync::{Mutex, oneshot};
use tqsdk_data::BacktestHistorySnapshot;
use tqsdk_relay::{RelayError, RelayResult};

use super::observability::HistoryObservability;

const RELOAD_INTERVAL: Duration = Duration::from_secs(5);

/// One immutable generation and its generation-local integrity signal.
///
/// Clones retain both the `tqsdk-data` lease pin and the same one-shot health
/// flag, so an in-flight request cannot report an integrity failure against a
/// generation loaded later by the slot.
#[derive(Clone, Debug)]
pub(super) struct PinnedSnapshot {
    snapshot: Arc<BacktestHistorySnapshot>,
    unhealthy: Arc<AtomicBool>,
    observability: Option<Arc<HistoryObservability>>,
}

impl PinnedSnapshot {
    fn new(snapshot: Arc<BacktestHistorySnapshot>) -> Self {
        Self {
            snapshot,
            unhealthy: Arc::new(AtomicBool::new(false)),
            observability: None,
        }
    }

    /// Marks this exact pinned generation unhealthy exactly once.
    pub(super) fn mark_corrupt(&self) -> bool {
        let won = self
            .unhealthy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if won {
            if let Some(observability) = &self.observability {
                observability.note_corrupt(self.snapshot_id());
            }
        }
        won
    }

    /// Whether this exact pinned generation has an integrity failure.
    #[must_use]
    pub(super) fn is_unhealthy(&self) -> bool {
        self.unhealthy.load(Ordering::Acquire)
    }
}

impl Deref for PinnedSnapshot {
    type Target = BacktestHistorySnapshot;

    fn deref(&self) -> &Self::Target {
        self.snapshot.as_ref()
    }
}

/// One process-local view of the generation selected by `CURRENT`.
///
/// The history root is validated as absolute before the history runtime starts.
/// A failed reload deliberately leaves the previous immutable [`Arc`] intact.
#[derive(Debug)]
pub(super) struct SnapshotSlot {
    root: PathBuf,
    snapshot: RwLock<Option<PinnedSnapshot>>,
    observability: RwLock<Option<Arc<HistoryObservability>>>,
    reload_gate: Mutex<()>,
}

impl SnapshotSlot {
    #[must_use]
    pub(super) fn new(root: PathBuf) -> Self {
        assert!(root.is_absolute(), "history snapshot root must be absolute");
        Self {
            root,
            snapshot: RwLock::new(None),
            observability: RwLock::new(None),
            reload_gate: Mutex::new(()),
        }
    }

    /// Returns a lease-pinning clone of the currently loaded generation.
    #[must_use]
    pub(super) fn current(&self) -> Option<PinnedSnapshot> {
        self.snapshot
            .read()
            .expect("history snapshot slot lock poisoned")
            .clone()
    }

    pub(super) fn attach_observability(&self, observability: Arc<HistoryObservability>) {
        *self
            .observability
            .write()
            .expect("history snapshot observability lock poisoned") = Some(observability);
    }

    /// Opens `CURRENT` off the history runtime and atomically publishes it locally.
    ///
    /// On an open failure, no state is changed: callers continue to observe the
    /// prior generation, if one exists.
    pub(super) async fn reload(&self) -> RelayResult<PinnedSnapshot> {
        let observability = self
            .observability
            .read()
            .map_err(|_| {
                RelayError::Internal("history snapshot observability lock poisoned".to_string())
            })?
            .clone();
        if let Some(observability) = &observability {
            observability.note_reload_attempt();
        }
        let _reload_guard = self.reload_gate.lock().await;
        let root = self.root.clone();
        let opened = tokio::task::spawn_blocking(move || BacktestHistorySnapshot::open(root))
            .await
            .map_err(|error| {
                if let Some(observability) = &observability {
                    observability.note_reload_failure("snapshot_worker_failed");
                }
                RelayError::Internal(format!("history snapshot reload task failed: {error}"))
            })?
            .map(Arc::new)
            .map_err(|error| {
                if let Some(observability) = &observability {
                    observability.note_reload_failure("snapshot_open_failed");
                }
                RelayError::Transport(format!("history snapshot reload failed: {error}"))
            })?;

        let mut current = self
            .snapshot
            .write()
            .map_err(|_| RelayError::Internal("history snapshot slot lock poisoned".to_string()))?;
        if let Some(current) = current.as_ref()
            && current.snapshot_id() == opened.snapshot_id()
        {
            if let Some(observability) = &observability {
                observability.note_reload_unchanged(current.snapshot_id());
            }
            return Ok(current.clone());
        }
        let mut pinned = PinnedSnapshot::new(opened);
        pinned.observability = observability.clone();
        *current = Some(pinned.clone());
        if let Some(observability) = &observability {
            observability.note_reload_success(pinned.snapshot_id());
        }
        Ok(pinned)
    }

    /// Reloads every five seconds until the isolated history runtime shuts down.
    ///
    /// Individual failures are intentionally retained as the last-good
    /// generation continues serving requests. The next interval retries `CURRENT`.
    pub(super) async fn reload_loop(self: Arc<Self>, mut shutdown: oneshot::Receiver<()>) {
        let mut interval = tokio::time::interval_at(
            tokio::time::Instant::now() + RELOAD_INTERVAL,
            RELOAD_INTERVAL,
        );
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = &mut shutdown => return,
                _ = interval.tick() => {
                    let _ = self.reload().await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use chrono::{DateTime, Utc};
    use tqsdk_data::BacktestHistorySnapshotManifestBuilder;

    use super::SnapshotSlot;
    use crate::history::observability::{HistoryObservability, MemoryAuditSink};

    fn temp_root(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "tqsdk-relay-history-snapshot-slot-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn publish_empty_generation(root: &Path, created_at: &str) -> String {
        let staging = root.join("staging").join("pending");
        let cache = staging.join("cache");
        std::fs::create_dir_all(&cache).unwrap();
        let created_at = created_at.parse::<DateTime<Utc>>().unwrap();
        let artifact = BacktestHistorySnapshotManifestBuilder::new(created_at)
            .catalog(false, std::iter::empty::<&str>())
            .build(&cache)
            .unwrap();
        let snapshot_id = artifact.snapshot_id().to_string();
        let generation = root.join("snapshots").join(&snapshot_id);
        std::fs::create_dir_all(generation.parent().unwrap()).unwrap();
        std::fs::rename(staging, &generation).unwrap();
        std::fs::write(generation.join("lease.lock"), []).unwrap();
        std::fs::write(generation.join("manifest.json"), artifact.manifest_bytes()).unwrap();
        std::fs::write(root.join("CURRENT"), format!("{snapshot_id}\n")).unwrap();
        snapshot_id
    }

    #[tokio::test]
    async fn initial_reload_without_current_keeps_slot_empty() {
        let root = temp_root("missing-current");
        std::fs::create_dir_all(&root).unwrap();
        let slot = SnapshotSlot::new(root.clone());

        assert!(slot.reload().await.is_err());
        assert!(slot.current().is_none());

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn reload_swaps_valid_generation_and_resets_generation_health() {
        let root = temp_root("reload");
        std::fs::create_dir_all(&root).unwrap();
        let first_id = publish_empty_generation(&root, "2026-08-29T00:00:00Z");
        let slot = SnapshotSlot::new(root.clone());
        let observability = Arc::new(HistoryObservability::with_audit(
            false,
            Arc::new(MemoryAuditSink::default()),
        ));
        observability.listener_started();
        slot.attach_observability(observability.clone());
        let first = slot.reload().await.unwrap();
        assert_eq!(first.snapshot_id(), first_id);
        assert!(observability.snapshot().ready);
        assert!(first.mark_corrupt());
        assert!(first.is_unhealthy());
        assert!(!observability.snapshot().ready);

        let same_generation = slot.reload().await.unwrap();
        assert_eq!(same_generation.snapshot_id(), first_id);
        assert!(same_generation.is_unhealthy());
        assert!(!observability.snapshot().ready);

        std::fs::write(root.join("CURRENT"), "s-missing\n").unwrap();
        assert!(slot.reload().await.is_err());
        assert_eq!(slot.current().unwrap().snapshot_id(), first_id);
        assert!(first.is_unhealthy());
        assert!(observability.snapshot().degraded);

        let second_id = publish_empty_generation(&root, "2026-08-29T00:00:01Z");
        let second = slot.reload().await.unwrap();
        assert_eq!(second.snapshot_id(), second_id);
        assert_ne!(first.snapshot_id(), second.snapshot_id());
        assert!(first.is_unhealthy());
        assert!(!second.is_unhealthy());
        assert!(observability.snapshot().ready);
        assert!(second.mark_corrupt());
        assert!(!second.mark_corrupt());

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn pinned_generation_health_is_one_shot_across_concurrent_clones() {
        let root = temp_root("concurrent-health");
        std::fs::create_dir_all(&root).unwrap();
        publish_empty_generation(&root, "2026-08-29T00:00:00Z");
        let slot = SnapshotSlot::new(root.clone());
        let pinned = slot.reload().await.unwrap();
        let winners = Arc::new(AtomicUsize::new(0));

        std::thread::scope(|scope| {
            for _ in 0..16 {
                let pinned = pinned.clone();
                let winners = Arc::clone(&winners);
                scope.spawn(move || {
                    if pinned.mark_corrupt() {
                        winners.fetch_add(1, Ordering::AcqRel);
                    }
                });
            }
        });

        assert_eq!(winners.load(Ordering::Acquire), 1);
        assert!(pinned.is_unhealthy());
        std::fs::remove_dir_all(root).unwrap();
    }
}
