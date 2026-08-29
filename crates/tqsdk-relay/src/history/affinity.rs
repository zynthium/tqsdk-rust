use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tqsdk_relay::{RelayError, RelayResult};

pub(super) const ENV_MARKET_CPU_SET: &str = "TQSDK_RELAY_MARKET_CPU_SET";
pub(super) const ENV_HISTORY_CPU_SET: &str = "TQSDK_RELAY_HISTORY_CPU_SET";

/// Validated CPU ownership for the process's market and history runtimes.
///
/// This deliberately carries the concrete IDs returned by `core_affinity`:
/// validation and application therefore refer to the same OS-visible CPU set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CpuAffinityConfig {
    market: Vec<core_affinity::CoreId>,
    history: Vec<core_affinity::CoreId>,
}

impl CpuAffinityConfig {
    pub(super) fn from_env_values(
        get: impl FnMut(&str) -> Option<String>,
    ) -> RelayResult<Option<Self>> {
        let mut get = get;
        let market = get(ENV_MARKET_CPU_SET);
        let history = get(ENV_HISTORY_CPU_SET);
        match (&market, &history) {
            (None, None) => Ok(None),
            _ => Self::from_cpu_values_with_available(market, history, available_core_ids()?),
        }
    }

    #[cfg(test)]
    fn from_env_values_with_available(
        mut get: impl FnMut(&str) -> Option<String>,
        available: BTreeMap<usize, core_affinity::CoreId>,
    ) -> RelayResult<Option<Self>> {
        let market = get(ENV_MARKET_CPU_SET);
        let history = get(ENV_HISTORY_CPU_SET);
        Self::from_cpu_values_with_available(market, history, available)
    }

    fn from_cpu_values_with_available(
        market: Option<String>,
        history: Option<String>,
        available: BTreeMap<usize, core_affinity::CoreId>,
    ) -> RelayResult<Option<Self>> {
        match (market, history) {
            (None, None) => Ok(None),
            (Some(market), Some(history)) => {
                let available_ids = available.keys().copied().collect();
                let market = parse_cpu_set(&market, &available_ids)?;
                let history = parse_cpu_set(&history, &available_ids)?;
                if !cpu_sets_are_disjoint(&market, &history) {
                    return Err(RelayError::invalid_config(format!(
                        "{ENV_MARKET_CPU_SET} and {ENV_HISTORY_CPU_SET} must not overlap"
                    )));
                }
                Ok(Some(Self {
                    market: resolve_ids(market, &available)?,
                    history: resolve_ids(history, &available)?,
                }))
            }
            _ => Err(RelayError::invalid_config(format!(
                "{ENV_MARKET_CPU_SET} and {ENV_HISTORY_CPU_SET} must be configured together"
            ))),
        }
    }

    pub(super) fn bind_market_current(&self) -> RelayResult<()> {
        let core = *self.market.first().expect("validated non-empty market set");
        apply_core(core)
    }

    pub(super) fn history_binder(&self) -> HistoryAffinity {
        HistoryAffinity {
            cores: Arc::from(self.history.clone()),
            next: Arc::new(AtomicUsize::new(0)),
        }
    }
}

/// Round-robin binding for every OS thread owned by the history sibling.
#[derive(Debug, Clone)]
pub(super) struct HistoryAffinity {
    cores: Arc<[core_affinity::CoreId]>,
    next: Arc<AtomicUsize>,
}

impl HistoryAffinity {
    pub(super) fn bind_current(&self) -> RelayResult<()> {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.cores.len();
        apply_core(self.cores[index])
    }
}

fn available_core_ids() -> RelayResult<BTreeMap<usize, core_affinity::CoreId>> {
    core_affinity::get_core_ids()
        .filter(|cores| !cores.is_empty())
        .map(|cores| cores.into_iter().map(|core| (core.id, core)).collect())
        .ok_or_else(|| RelayError::invalid_config("CPU affinity is unavailable on this host"))
}

fn resolve_ids(
    ids: Vec<usize>,
    available: &BTreeMap<usize, core_affinity::CoreId>,
) -> RelayResult<Vec<core_affinity::CoreId>> {
    ids.into_iter()
        .map(|id| {
            available.get(&id).copied().ok_or_else(|| {
                RelayError::invalid_config(format!("requested CPU core {id} is unavailable"))
            })
        })
        .collect()
}

fn parse_cpu_set(value: &str, available: &BTreeSet<usize>) -> RelayResult<Vec<usize>> {
    if value.is_empty() {
        return Err(RelayError::invalid_config("CPU set must not be empty"));
    }
    let mut parsed = Vec::new();
    let mut seen = BTreeSet::new();
    for part in value.split(',') {
        if part.is_empty() || part.trim() != part {
            return Err(RelayError::invalid_config(
                "CPU set must be a comma-separated list of decimal core IDs",
            ));
        }
        let id = part.parse::<usize>().map_err(|_| {
            RelayError::invalid_config("CPU set must be a comma-separated list of decimal core IDs")
        })?;
        if !seen.insert(id) {
            return Err(RelayError::invalid_config(format!(
                "CPU set contains duplicate core {id}"
            )));
        }
        if !available.contains(&id) {
            return Err(RelayError::invalid_config(format!(
                "requested CPU core {id} is unavailable"
            )));
        }
        parsed.push(id);
    }
    Ok(parsed)
}

fn cpu_sets_are_disjoint(left: &[usize], right: &[usize]) -> bool {
    let left: BTreeSet<_> = left.iter().copied().collect();
    right.iter().all(|id| !left.contains(id))
}

fn apply_core(core: core_affinity::CoreId) -> RelayResult<()> {
    apply_core_with(core.id, |id| {
        core_affinity::set_for_current(core_affinity::CoreId { id })
    })
}

fn apply_core_with(core: usize, apply: impl FnOnce(usize) -> bool) -> RelayResult<()> {
    apply(core).then_some(()).ok_or_else(|| {
        RelayError::invalid_config(format!("failed to apply CPU affinity for core {core}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_sets_must_be_paired_nonempty_disjoint_and_available() {
        let available = BTreeSet::from([0, 1, 2, 3]);
        assert!(parse_cpu_set("0,2", &available).is_ok());
        assert!(parse_cpu_set("", &available).is_err());
        assert!(parse_cpu_set("0,0", &available).is_err());
        assert!(parse_cpu_set("4", &available).is_err());
        assert!(cpu_sets_are_disjoint(&[0], &[1]));
        assert!(!cpu_sets_are_disjoint(&[0, 1], &[1, 2]));
    }

    #[test]
    fn affinity_apply_failure_is_fatal() {
        let error = apply_core_with(0, |_| false).unwrap_err();
        assert!(matches!(error, RelayError::InvalidConfig(_)));
    }

    #[test]
    fn paired_cpu_environment_is_strict() {
        let available = BTreeMap::from([
            (0, core_affinity::CoreId { id: 0 }),
            (1, core_affinity::CoreId { id: 1 }),
        ]);
        assert!(
            CpuAffinityConfig::from_env_values_with_available(|_| None, available.clone())
                .unwrap()
                .is_none()
        );
        assert!(
            CpuAffinityConfig::from_env_values_with_available(
                |key| (key == ENV_MARKET_CPU_SET).then(|| "0".to_string()),
                available.clone(),
            )
            .is_err()
        );
        assert!(
            CpuAffinityConfig::from_env_values_with_available(
                |key| match key {
                    ENV_MARKET_CPU_SET => Some("0".to_string()),
                    ENV_HISTORY_CPU_SET => Some("0".to_string()),
                    _ => None,
                },
                available,
            )
            .is_err()
        );
    }
}
