//! Durable, bounded market-view cache for relay processes.
//!
//! This is deliberately separate from canonical minute/daily history caches.
//! A write replaces one complete rolling view in a single TQBN generation, so
//! callers never observe an official baseline and its provisional tail from
//! different commits.

use std::fs::{self, File, OpenOptions};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tqsdk_core::{Kline, Tick};

use crate::history_container::{self, Extent, Finality, Identity, Index, SeriesKind};
use crate::{DataError, Result};

pub const ROLLING_MARKET_CACHE_FORMAT_VERSION: u32 = 1;

/// Stable identity for one rolling market-view stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RollingMarketCacheKind {
    Tick,
    Kline { duration_ns: i64 },
}

/// One durable rolling market-view stream discovered under this cache root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollingMarketCacheEntry {
    pub symbol: String,
    pub kind: RollingMarketCacheKind,
}

impl RollingMarketCacheKind {
    fn series_kind(self) -> Result<SeriesKind> {
        match self {
            Self::Tick => Ok(SeriesKind::Tick),
            Self::Kline { duration_ns } if duration_ns > 0 => Ok(SeriesKind::Kline { duration_ns }),
            Self::Kline { .. } => Err(DataError::Validation(
                "rolling Kline duration must be positive".to_owned(),
            )),
        }
    }

    fn file_stem(self) -> String {
        match self {
            Self::Tick => "tick".to_owned(),
            Self::Kline { duration_ns } => format!("kline-{duration_ns}"),
        }
    }
}

/// Provenance for an atomically published rolling view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollingMarketCacheMetadata {
    pub format_version: u32,
    pub capacity: usize,
    pub session_hash: String,
    pub aggregation_algorithm_version: u32,
    pub source_epoch: u64,
    pub confirmed_revision: u64,
    pub presentation_revision: u64,
    pub official_as_of_ns: Option<i64>,
}

impl RollingMarketCacheMetadata {
    #[must_use]
    pub fn new(
        capacity: NonZeroUsize,
        session_hash: impl Into<String>,
        aggregation_algorithm_version: u32,
        source_epoch: u64,
        confirmed_revision: u64,
        presentation_revision: u64,
        official_as_of_ns: Option<i64>,
    ) -> Self {
        Self {
            format_version: ROLLING_MARKET_CACHE_FORMAT_VERSION,
            capacity: capacity.get(),
            session_hash: session_hash.into(),
            aggregation_algorithm_version,
            source_epoch,
            confirmed_revision,
            presentation_revision,
            official_as_of_ns,
        }
    }

    fn validate(
        &self,
        expected_capacity: NonZeroUsize,
        expected_session_hash: &str,
        expected_algorithm: u32,
    ) -> Result<()> {
        if self.format_version != ROLLING_MARKET_CACHE_FORMAT_VERSION
            || self.capacity != expected_capacity.get()
            || self.session_hash != expected_session_hash
            || self.aggregation_algorithm_version != expected_algorithm
            || self.confirmed_revision > self.presentation_revision
        {
            return Err(DataError::InvalidState(
                "incompatible rolling market cache metadata",
            ));
        }
        Ok(())
    }
}

impl history_container::IndexMetadata for RollingMarketCacheMetadata {}

/// One complete, self-consistent rolling snapshot.
#[derive(Debug, Clone)]
pub struct RollingMarketCacheSnapshot<R> {
    pub metadata: RollingMarketCacheMetadata,
    pub rows: Vec<R>,
}

/// Durable store for bounded tick and Kline presentation views.
#[derive(Debug, Clone)]
pub struct RollingMarketCache {
    root: PathBuf,
    capacity: NonZeroUsize,
    session_hash: String,
    aggregation_algorithm_version: u32,
}

impl RollingMarketCache {
    pub fn open(
        root: impl Into<PathBuf>,
        capacity: NonZeroUsize,
        session_hash: impl Into<String>,
        aggregation_algorithm_version: u32,
    ) -> Result<Self> {
        let root = root.into();
        let session_hash = session_hash.into();
        if session_hash.is_empty() || session_hash.len() > 4096 {
            return Err(DataError::Validation(
                "rolling market cache session hash must be nonempty and bounded".to_owned(),
            ));
        }
        if aggregation_algorithm_version == 0 {
            return Err(DataError::Validation(
                "rolling market cache aggregation algorithm version must be positive".to_owned(),
            ));
        }
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            capacity,
            session_hash,
            aggregation_algorithm_version,
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub fn capacity(&self) -> NonZeroUsize {
        self.capacity
    }

    pub fn replace_ticks(
        &self,
        symbol: &str,
        metadata: RollingMarketCacheMetadata,
        rows: &[Tick],
    ) -> Result<()> {
        self.replace(
            symbol,
            RollingMarketCacheKind::Tick,
            metadata,
            rows,
            history_container::tick::encode,
        )
    }

    pub fn replace_klines(
        &self,
        symbol: &str,
        duration_ns: i64,
        metadata: RollingMarketCacheMetadata,
        rows: &[Kline],
    ) -> Result<()> {
        self.replace(
            symbol,
            RollingMarketCacheKind::Kline { duration_ns },
            metadata,
            rows,
            history_container::encode_klines,
        )
    }

    pub fn load_ticks(&self, symbol: &str) -> Result<Option<RollingMarketCacheSnapshot<Tick>>> {
        self.load(
            symbol,
            RollingMarketCacheKind::Tick,
            history_container::tick::read,
        )
    }

    pub fn load_klines(
        &self,
        symbol: &str,
        duration_ns: i64,
    ) -> Result<Option<RollingMarketCacheSnapshot<Kline>>> {
        self.load(
            symbol,
            RollingMarketCacheKind::Kline { duration_ns },
            history_container::read_klines,
        )
    }

    /// Lists valid rolling streams already published under this cache root.
    ///
    /// Unknown files are ignored so an operator can keep diagnostics beside a
    /// cache root. Malformed cache-shaped entries fail closed rather than being
    /// silently overwritten on a later write.
    pub fn entries(&self) -> Result<Vec<RollingMarketCacheEntry>> {
        let mut entries = Vec::new();
        for directory in fs::read_dir(&self.root)? {
            let directory = directory?;
            let file_type = directory.file_type()?;
            if !file_type.is_dir() || file_type.is_symlink() {
                continue;
            }
            let directory_name = directory.file_name();
            let Some(name) = directory_name.to_str() else {
                continue;
            };
            let Some(symbol) = decode_hex_symbol(name) else {
                continue;
            };
            validate_symbol(&symbol)?;
            for file in fs::read_dir(directory.path())? {
                let file = file?;
                let file_type = file.file_type()?;
                if !file_type.is_file() || file_type.is_symlink() {
                    continue;
                }
                let file_name = file.file_name();
                let Some(name) = file_name.to_str() else {
                    continue;
                };
                let kind = if name == "tick.tqbn" {
                    Some(RollingMarketCacheKind::Tick)
                } else if let Some(duration) = name
                    .strip_prefix("kline-")
                    .and_then(|value| value.strip_suffix(".tqbn"))
                {
                    let duration_ns = duration.parse::<i64>().map_err(|_| {
                        DataError::InvalidState("invalid rolling Kline cache filename")
                    })?;
                    Some(RollingMarketCacheKind::Kline { duration_ns })
                } else {
                    None
                };
                if let Some(kind) = kind {
                    kind.series_kind()?;
                    entries.push(RollingMarketCacheEntry {
                        symbol: symbol.clone(),
                        kind,
                    });
                }
            }
        }
        entries.sort_by(|left, right| {
            left.symbol
                .cmp(&right.symbol)
                .then_with(|| left.kind.file_stem().cmp(&right.kind.file_stem()))
        });
        Ok(entries)
    }

    fn replace<R>(
        &self,
        symbol: &str,
        kind: RollingMarketCacheKind,
        metadata: RollingMarketCacheMetadata,
        rows: &[R],
        encode: impl Fn(&[R]) -> Result<history_container::EncodedBlock>,
    ) -> Result<()> {
        validate_symbol(symbol)?;
        metadata.validate(
            self.capacity,
            &self.session_hash,
            self.aggregation_algorithm_version,
        )?;
        if rows.len() > self.capacity.get() {
            return Err(DataError::Validation(
                "rolling market cache write exceeds configured capacity".to_owned(),
            ));
        }

        let path = self.path_for(symbol, kind);
        let mut index = Index::new(
            Identity {
                symbol: symbol.to_owned(),
                kind: kind.series_kind()?,
                partition_scheme: 1,
                pack_range: None,
                metadata_schema: 1,
            },
            vec![metadata],
        );
        let encoded = (!rows.is_empty()).then(|| encode(rows)).transpose()?;
        if let Some(block) = &encoded {
            let end_ns = block.slice(0).last_ns.checked_add(1).ok_or_else(|| {
                DataError::Validation("rolling market timestamp overflow".to_owned())
            })?;
            index.extents.push(Extent {
                start_ns: block.slice(0).first_ns,
                end_ns,
                logical_partition: "presentation".to_owned(),
                metadata: 0,
                finality: match kind {
                    RollingMarketCacheKind::Tick => Finality::Unverified,
                    RollingMarketCacheKind::Kline { .. } => Finality::Provisional {
                        as_of_ns: index.metadata[0].official_as_of_ns.unwrap_or_default(),
                    },
                },
                slices: vec![block.slice(0)],
            });
        }
        history_container::create(&path, index, encoded.as_slice())
    }

    fn load<R>(
        &self,
        symbol: &str,
        kind: RollingMarketCacheKind,
        read: impl Fn(
            &mut File,
            &Index<RollingMarketCacheMetadata>,
            usize,
            Option<usize>,
        ) -> Result<Vec<R>>,
    ) -> Result<Option<RollingMarketCacheSnapshot<R>>> {
        validate_symbol(symbol)?;
        let path = self.path_for(symbol, kind);
        if !path.exists() {
            return Ok(None);
        }
        let mut file = OpenOptions::new().read(true).open(path)?;
        let index: Index<RollingMarketCacheMetadata> =
            history_container::load(&mut file, kind.series_kind()?, None)?;
        let metadata = index
            .metadata
            .first()
            .cloned()
            .ok_or(DataError::InvalidState(
                "rolling market cache lacks metadata",
            ))?;
        if index.metadata.len() != 1 {
            return Err(DataError::InvalidState(
                "rolling market cache has multiple metadata records",
            ));
        }
        metadata.validate(
            self.capacity,
            &self.session_hash,
            self.aggregation_algorithm_version,
        )?;
        let mut rows = Vec::new();
        for block in 0..index.blocks.len() {
            rows.extend(read(&mut file, &index, block, None)?);
        }
        if rows.len() > self.capacity.get() {
            return Err(DataError::InvalidState(
                "rolling market cache exceeds configured capacity",
            ));
        }
        Ok(Some(RollingMarketCacheSnapshot { metadata, rows }))
    }

    fn path_for(&self, symbol: &str, kind: RollingMarketCacheKind) -> PathBuf {
        self.root
            .join(hex_symbol(symbol))
            .join(format!("{}.tqbn", kind.file_stem()))
    }
}

fn validate_symbol(symbol: &str) -> Result<()> {
    if symbol.is_empty() || symbol.len() > 4096 {
        return Err(DataError::Validation(
            "rolling market cache symbol must be nonempty and bounded".to_owned(),
        ));
    }
    Ok(())
}

fn hex_symbol(symbol: &str) -> String {
    let mut output = String::with_capacity(symbol.len() * 2);
    for byte in symbol.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn decode_hex_symbol(value: &str) -> Option<String> {
    if value.is_empty() || !value.len().is_multiple_of(2) {
        return None;
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
    debug_assert!(remainder.is_empty());
    for pair in pairs {
        let high = (pair[0] as char).to_digit(16)?;
        let low = (pair[1] as char).to_digit(16)?;
        bytes.push(u8::try_from((high << 4) | low).ok()?);
    }
    String::from_utf8(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "rolling-market-cache-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn cache() -> (Fixture, RollingMarketCache) {
        let directory = Fixture::new();
        let cache = RollingMarketCache::open(
            &directory.0,
            NonZeroUsize::new(10_000).unwrap(),
            "session-v1",
            1,
        )
        .unwrap();
        (directory, cache)
    }

    fn metadata() -> RollingMarketCacheMetadata {
        RollingMarketCacheMetadata::new(
            NonZeroUsize::new(10_000).unwrap(),
            "session-v1",
            1,
            7,
            10,
            12,
            Some(100),
        )
    }

    #[test]
    fn round_trips_tick_snapshot_with_provenance() {
        let (_directory, cache) = cache();
        let rows = vec![
            Tick {
                id: 1,
                datetime: 10,
                ..Tick::default()
            },
            Tick {
                id: 2,
                datetime: 11,
                ..Tick::default()
            },
        ];
        cache
            .replace_ticks("SHFE.au2602", metadata(), &rows)
            .unwrap();

        let loaded = cache.load_ticks("SHFE.au2602").unwrap().unwrap();
        assert_eq!(loaded.metadata.source_epoch, 7);
        assert_eq!(loaded.metadata.confirmed_revision, 10);
        assert_eq!(loaded.metadata.presentation_revision, 12);
        assert_eq!(loaded.rows.len(), 2);
        assert_eq!(loaded.rows[1].id, 2);
    }

    #[test]
    fn round_trips_kline_snapshot_and_replaces_as_one_generation() {
        let (_directory, cache) = cache();
        let first = vec![Kline {
            id: 1,
            datetime: 10,
            ..Kline::default()
        }];
        let second = vec![Kline {
            id: 2,
            datetime: 20,
            ..Kline::default()
        }];
        cache
            .replace_klines("SHFE.au2602", 60_000_000_000, metadata(), &first)
            .unwrap();
        cache
            .replace_klines("SHFE.au2602", 60_000_000_000, metadata(), &second)
            .unwrap();

        let loaded = cache
            .load_klines("SHFE.au2602", 60_000_000_000)
            .unwrap()
            .unwrap();
        assert_eq!(loaded.rows.len(), 1);
        assert_eq!(loaded.rows[0].id, 2);
    }

    #[test]
    fn rejects_rows_above_the_bounded_capacity() {
        let (_directory, cache) = cache();
        let rows = vec![Tick::default(); 10_001];
        assert!(
            cache
                .replace_ticks("SHFE.au2602", metadata(), &rows)
                .is_err()
        );
    }

    #[test]
    fn lists_tick_and_kline_entries() {
        let (_directory, cache) = cache();
        cache
            .replace_ticks("SHFE.au2602", metadata(), &[Tick::default()])
            .unwrap();
        cache
            .replace_klines(
                "SHFE.au2602",
                60_000_000_000,
                metadata(),
                &[Kline::default()],
            )
            .unwrap();

        assert_eq!(
            cache.entries().unwrap(),
            vec![
                RollingMarketCacheEntry {
                    symbol: "SHFE.au2602".to_owned(),
                    kind: RollingMarketCacheKind::Kline {
                        duration_ns: 60_000_000_000,
                    },
                },
                RollingMarketCacheEntry {
                    symbol: "SHFE.au2602".to_owned(),
                    kind: RollingMarketCacheKind::Tick,
                },
            ]
        );
    }
}
