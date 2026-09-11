#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk_data::{RollingMarketCache, RollingMarketCacheKind};

use crate::config::RollingCacheConfig;
use crate::engine::RelayEngine;
use crate::error::{RelayError, RelayResult};

/// Rows restored into Relay's serving state before listeners open.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RollingCacheRestoreReport {
    pub tick_rows: usize,
    pub kline_rows: usize,
}

/// Loads every compatible rolling snapshot into the in-memory serving state.
/// Corrupt or incompatible snapshots fail startup rather than get overwritten.
pub fn restore_rolling_cache(
    engine: &mut RelayEngine,
    config: &RollingCacheConfig,
) -> RelayResult<RollingCacheRestoreReport> {
    let cache = RollingMarketCache::open(
        config.root().clone(),
        config.capacity(),
        config.session_hash(),
        config.aggregation_algorithm_version(),
    )
    .map_err(|error| RelayError::invalid_config(error.to_string()))?;
    let mut report = RollingCacheRestoreReport::default();
    for entry in cache
        .entries()
        .map_err(|error| RelayError::Internal(format!("rolling cache inventory failed: {error}")))?
    {
        match entry.kind {
            RollingMarketCacheKind::Tick => {
                let snapshot = cache
                    .load_ticks(&entry.symbol)
                    .map_err(|error| {
                        RelayError::Internal(format!(
                            "rolling tick restore failed for {}: {error}",
                            entry.symbol
                        ))
                    })?
                    .ok_or_else(|| {
                        RelayError::Internal(format!(
                            "rolling tick disappeared during restore: {}",
                            entry.symbol
                        ))
                    })?;
                report.tick_rows = report.tick_rows.saturating_add(snapshot.rows.len());
                engine.restore_rolling_ticks(&entry.symbol, &snapshot.rows);
            }
            RollingMarketCacheKind::Kline { duration_ns } => {
                let snapshot = cache
                    .load_klines(&entry.symbol, duration_ns)
                    .map_err(|error| {
                        RelayError::Internal(format!(
                            "rolling Kline restore failed for {}:{duration_ns}: {error}",
                            entry.symbol
                        ))
                    })?
                    .ok_or_else(|| {
                        RelayError::Internal(format!(
                            "rolling Kline disappeared during restore: {}:{duration_ns}",
                            entry.symbol
                        ))
                    })?;
                report.kline_rows = report.kline_rows.saturating_add(snapshot.rows.len());
                engine.restore_official_rolling_klines(
                    &entry.symbol,
                    duration_ns,
                    &snapshot.rows,
                )?;
            }
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use tqsdk_core::{Kline, Tick};
    use tqsdk_data::RollingMarketCacheMetadata;

    use super::*;

    #[test]
    fn restores_all_discovered_streams() {
        let root =
            std::env::temp_dir().join(format!("relay-rolling-restore-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        let config = RollingCacheConfig::new(&root, "session-v1", 1).unwrap();
        let cache = RollingMarketCache::open(
            &root,
            config.capacity(),
            config.session_hash(),
            config.aggregation_algorithm_version(),
        )
        .unwrap();
        let metadata = RollingMarketCacheMetadata::new(
            NonZeroUsize::new(10_000).unwrap(),
            "session-v1",
            1,
            1,
            1,
            1,
            None,
        );
        cache
            .replace_ticks(
                "SHFE.au2602",
                metadata.clone(),
                &[Tick {
                    id: 1,
                    datetime: 60,
                    ..Tick::default()
                }],
            )
            .unwrap();
        cache
            .replace_klines(
                "SHFE.au2602",
                60,
                metadata,
                &[Kline {
                    id: 1,
                    datetime: 60,
                    ..Kline::default()
                }],
            )
            .unwrap();

        let mut engine = RelayEngine::new_memory_only(10_000, 10_000);
        assert_eq!(
            restore_rolling_cache(&mut engine, &config).unwrap(),
            RollingCacheRestoreReport {
                tick_rows: 1,
                kline_rows: 1,
            }
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
