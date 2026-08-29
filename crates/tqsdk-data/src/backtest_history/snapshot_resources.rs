use std::fmt;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::DataError;

/// Daemon-owned budget for immutable snapshot source scans.
///
/// `tqsdk-data` never creates a parallel accounting pool. The embedding service
/// supplies one implementation shared by every request it wants to constrain.
pub trait BacktestHistorySnapshotResourceBudget: Send + Sync {
    /// Immediately reserves `bytes`, returning `None` when the daemon-wide
    /// budget cannot admit another source scan.
    fn try_reserve(&self, bytes: usize) -> Option<BacktestHistorySnapshotResourceReservation>;
}

/// Opaque RAII guard returned by a daemon-owned snapshot scan budget.
///
/// Construct this from the service's own permit/lease type. Dropping the
/// reservation drops that guard and therefore returns capacity to the service.
pub struct BacktestHistorySnapshotResourceReservation {
    _guard: Box<dyn Send + Sync + 'static>,
}

impl fmt::Debug for BacktestHistorySnapshotResourceReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BacktestHistorySnapshotResourceReservation")
            .finish_non_exhaustive()
    }
}

impl BacktestHistorySnapshotResourceReservation {
    /// Wraps one caller-owned RAII permit.
    #[must_use]
    pub fn new(guard: impl Send + Sync + 'static) -> Self {
        Self {
            _guard: Box::new(guard),
        }
    }
}

/// Per-run ownership of reservations for projected event rows.
///
/// Reservations first travel with the private event envelope, then remain
/// retained by the public run after event delivery. This keeps daemon-owned
/// accounting tied to observable rows without changing public event types.
#[derive(Clone, Default)]
pub(crate) struct BacktestHistoryRunReservations {
    inner: Arc<Mutex<BacktestHistoryRunReservationState>>,
}

struct BacktestHistoryRunReservationState {
    accepting: bool,
    reservations: Vec<BacktestHistorySnapshotResourceReservation>,
}

impl Default for BacktestHistoryRunReservationState {
    fn default() -> Self {
        Self {
            accepting: true,
            reservations: Vec::new(),
        }
    }
}

impl BacktestHistoryRunReservations {
    pub(crate) fn retain(&self, reservation: BacktestHistorySnapshotResourceReservation) {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.accepting {
            state.reservations.push(reservation);
        }
    }

    pub(crate) fn close(&self) {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.accepting = false;
        state.reservations.clear();
    }
}

/// Per-query immutable snapshot resources.
///
/// The budget is daemon-global and externally owned; `active_pin` is a
/// per-request caller guard retained until the coordinator and all detached
/// blocking readers have stopped.
#[derive(Clone)]
pub struct BacktestHistorySnapshotQueryResources {
    budget: Arc<dyn BacktestHistorySnapshotResourceBudget>,
    _active_pin: Arc<dyn Send + Sync + 'static>,
    #[cfg(test)]
    daily_reader_open_probe: Option<Arc<AtomicUsize>>,
}

impl fmt::Debug for BacktestHistorySnapshotQueryResources {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BacktestHistorySnapshotQueryResources")
            .finish_non_exhaustive()
    }
}

impl BacktestHistorySnapshotQueryResources {
    /// Couples one daemon-owned global budget with one active request pin.
    #[must_use]
    pub fn new(
        budget: Arc<dyn BacktestHistorySnapshotResourceBudget>,
        active_pin: impl Send + Sync + 'static,
    ) -> Self {
        Self {
            budget,
            _active_pin: Arc::new(active_pin),
            #[cfg(test)]
            daily_reader_open_probe: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_daily_reader_open_probe(mut self, probe: Arc<AtomicUsize>) -> Self {
        self.daily_reader_open_probe = Some(probe);
        self
    }

    #[cfg(test)]
    pub(crate) fn record_daily_reader_open(&self) {
        if let Some(probe) = &self.daily_reader_open_probe {
            probe.fetch_add(1, Ordering::AcqRel);
        }
    }

    pub(crate) fn try_reserve_for_scan(
        &self,
        bytes: usize,
    ) -> Result<BacktestHistorySnapshotResourceReservation, DataError> {
        self.budget
            .try_reserve(bytes.max(1))
            .ok_or(DataError::CollectLimitExceeded {
                limit_bytes: 0,
                attempted_bytes: bytes.max(1),
            })
    }

    pub(crate) fn try_reserve_for_projected_rows<T>(
        &self,
        upper_bound_rows: usize,
    ) -> Result<BacktestHistorySnapshotResourceReservation, DataError> {
        let bytes = upper_bound_rows
            .checked_mul(std::mem::size_of::<T>())
            .ok_or(DataError::CollectLimitExceeded {
                limit_bytes: 0,
                attempted_bytes: usize::MAX,
            })?;
        self.try_reserve_for_scan(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[derive(Debug)]
    struct TestBudget {
        capacity: usize,
        used: Arc<Mutex<usize>>,
    }

    impl BacktestHistorySnapshotResourceBudget for TestBudget {
        fn try_reserve(&self, bytes: usize) -> Option<BacktestHistorySnapshotResourceReservation> {
            let mut used = self.used.lock().unwrap();
            if used.saturating_add(bytes) > self.capacity {
                return None;
            }
            *used += bytes;
            Some(BacktestHistorySnapshotResourceReservation::new(
                TestReservation {
                    used: Arc::clone(&self.used),
                    bytes,
                },
            ))
        }
    }

    #[derive(Debug)]
    struct TestReservation {
        used: Arc<Mutex<usize>>,
        bytes: usize,
    }

    impl Drop for TestReservation {
        fn drop(&mut self) {
            let mut used = self.used.lock().unwrap();
            *used = used.saturating_sub(self.bytes);
        }
    }

    #[test]
    fn separate_queries_share_the_caller_budget_and_drop_the_caller_guard() {
        let used = Arc::new(Mutex::new(0));
        let budget: Arc<dyn BacktestHistorySnapshotResourceBudget> = Arc::new(TestBudget {
            capacity: 8,
            used: Arc::clone(&used),
        });
        let left = BacktestHistorySnapshotQueryResources::new(Arc::clone(&budget), ());
        let right = BacktestHistorySnapshotQueryResources::new(budget, ());

        let reservation = left.try_reserve_for_scan(8).unwrap();
        assert!(right.try_reserve_for_scan(1).is_err());
        assert_eq!(*used.lock().unwrap(), 8);
        drop(reservation);
        assert_eq!(*used.lock().unwrap(), 0);
        assert!(right.try_reserve_for_scan(8).is_ok());
    }
}
