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
use tqsdk_data::{BacktestHistorySnapshot, BacktestHistorySnapshotError};
use tqsdk_relay::{RelayError, RelayResult};

use super::affinity::HistoryAffinity;
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
pub(super) struct SnapshotSlot {
    root: PathBuf,
    affinity: Option<HistoryAffinity>,
    snapshot: RwLock<Option<PinnedSnapshot>>,
    observability: RwLock<Option<Arc<HistoryObservability>>>,
    reload_gate: Mutex<()>,
    #[cfg(test)]
    test_worker_binder: Option<SnapshotWorkerBinder>,
}

impl SnapshotSlot {
    #[cfg(test)]
    #[must_use]
    pub(super) fn new(root: PathBuf) -> Self {
        Self::new_with_affinity(root, None)
    }

    #[must_use]
    pub(super) fn new_with_affinity(root: PathBuf, affinity: Option<HistoryAffinity>) -> Self {
        assert!(root.is_absolute(), "history snapshot root must be absolute");
        Self {
            root,
            affinity,
            snapshot: RwLock::new(None),
            observability: RwLock::new(None),
            reload_gate: Mutex::new(()),
            #[cfg(test)]
            test_worker_binder: None,
        }
    }

    #[cfg(test)]
    fn new_with_test_worker_binder(root: PathBuf, binder: SnapshotWorkerBinder) -> Self {
        let mut slot = Self::new(root);
        slot.test_worker_binder = Some(binder);
        slot
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
        let affinity = self.affinity.clone();
        #[cfg(test)]
        let test_worker_binder = self.test_worker_binder.clone();
        let opened = tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if let Some(binder) = test_worker_binder {
                binder().map_err(SnapshotWorkerError::Affinity)?;
            } else if let Some(affinity) = affinity {
                affinity
                    .bind_current()
                    .map_err(SnapshotWorkerError::Affinity)?;
            }
            #[cfg(not(test))]
            if let Some(affinity) = affinity {
                affinity
                    .bind_current()
                    .map_err(SnapshotWorkerError::Affinity)?;
            }
            BacktestHistorySnapshot::open(root).map_err(SnapshotWorkerError::Open)
        })
        .await
        .map_err(|error| {
            if let Some(observability) = &observability {
                observability.note_reload_failure("snapshot_worker_failed");
            }
            RelayError::Internal(format!("history snapshot reload task failed: {error}"))
        })?
        .map(Arc::new)
        .map_err(|error| match error {
            SnapshotWorkerError::Affinity(error) => {
                if let Some(observability) = &observability {
                    observability.note_reload_failure("snapshot_affinity_failed");
                }
                error
            }
            SnapshotWorkerError::Open(error) => {
                if let Some(observability) = &observability {
                    observability.note_reload_failure("snapshot_open_failed");
                }
                RelayError::Transport(format!("history snapshot reload failed: {error}"))
            }
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

impl std::fmt::Debug for SnapshotSlot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SnapshotSlot")
            .field("root", &self.root)
            .field("affinity", &self.affinity)
            .finish_non_exhaustive()
    }
}

enum SnapshotWorkerError {
    Affinity(RelayError),
    Open(BacktestHistorySnapshotError),
}

#[cfg(test)]
type SnapshotWorkerBinder = Arc<dyn Fn() -> RelayResult<()> + Send + Sync>;

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use chrono::{DateTime, Utc};
    use tqsdk_data::BacktestHistorySnapshotManifestBuilder;
    use tqsdk_relay::RelayError;

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
    async fn reload_worker_binding_failure_preserves_last_good_generation() {
        let root = temp_root("reload-worker-affinity");
        std::fs::create_dir_all(&root).unwrap();
        let first_id = publish_empty_generation(&root, "2026-08-29T00:00:00Z");
        let permits_binding = Arc::new(AtomicBool::new(true));
        let binding_calls = Arc::new(AtomicUsize::new(0));
        let binder_permitted = Arc::clone(&permits_binding);
        let binder_calls = Arc::clone(&binding_calls);
        let slot = SnapshotSlot::new_with_test_worker_binder(
            root.clone(),
            Arc::new(move || {
                binder_calls.fetch_add(1, Ordering::AcqRel);
                if binder_permitted.load(Ordering::Acquire) {
                    Ok(())
                } else {
                    Err(RelayError::invalid_config(
                        "test history worker affinity failure",
                    ))
                }
            }),
        );
        let observability = Arc::new(HistoryObservability::with_audit(
            false,
            Arc::new(MemoryAuditSink::default()),
        ));
        observability.listener_started();
        slot.attach_observability(observability.clone());

        assert_eq!(slot.reload().await.unwrap().snapshot_id(), first_id);
        let replacement_id = publish_empty_generation(&root, "2026-08-29T00:00:01Z");
        permits_binding.store(false, Ordering::Release);

        assert!(matches!(
            slot.reload().await,
            Err(RelayError::InvalidConfig(_))
        ));
        assert_eq!(binding_calls.load(Ordering::Acquire), 2);
        assert_eq!(slot.current().unwrap().snapshot_id(), first_id);
        assert_ne!(slot.current().unwrap().snapshot_id(), replacement_id);
        let snapshot = observability.snapshot();
        assert!(snapshot.ready);
        assert!(snapshot.degraded);
        assert_eq!(snapshot.reload_last_code, "snapshot_affinity_failed");

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
