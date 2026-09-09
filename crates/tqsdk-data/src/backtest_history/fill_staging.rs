//! Private restart journal. Never used by coverage readers or snapshot publishing.
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tqsdk_core::Kline;

use super::fill::BacktestHistoryFillRequest;
use crate::{DataError, MinuteKlineCacheSnapshot, Result};

pub(super) const CHECKPOINT_SPAN_NS: i64 = 86_400_000_000_000;
const MAX_JOURNAL_BYTES: u64 = 4 * 1024 * 1024;
type StagedKline = (i64, i64, [u64; 4], i64, i64, i64, Option<i64>);

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Identity {
    version: u32,
    symbol: String,
    range: (i64, i64),
    snapshot: MinuteKlineCacheSnapshot,
    as_of_ns: Option<i64>,
}

#[derive(Serialize, Deserialize)]
struct Journal {
    identity: Identity,
    confirmed_end_ns: i64,
    overlap_start_ns: i64,
    // Float bits preserve NaNs and signed zero; JSON floats do not.
    rows: Vec<StagedKline>,
}

#[derive(Serialize, Deserialize)]
struct Envelope {
    checksum: String,
    payload: String,
}

pub(super) struct MinuteFillJournal {
    path: PathBuf,
    identity: Identity,
    pub(super) confirmed_end_ns: i64,
    pub(super) overlap_start_ns: i64,
    pub(super) rows: BTreeMap<i64, Kline>,
}

impl MinuteFillJournal {
    pub(super) fn open(root: &Path, request: &BacktestHistoryFillRequest) -> Result<Self> {
        let identity = Identity {
            version: 1,
            symbol: request.cache_symbol.clone(),
            range: request.range,
            snapshot: request
                .minute_snapshot
                .clone()
                .ok_or(DataError::InvalidState("minute journal requires snapshot"))?,
            as_of_ns: request.provisional_as_of_ns,
        };
        let key = format!("{:x}", Sha256::digest(serde_json::to_vec(&identity)?));
        let directory = root.join(".backtest-history-staging").join("minute-v1");
        for path in [root.join(".backtest-history-staging"), directory.clone()] {
            match fs::symlink_metadata(&path) {
                Ok(meta) if !meta.is_dir() || meta.file_type().is_symlink() => {
                    return Err(invalid("unsafe journal directory"));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    if let Err(error) = fs::create_dir(&path) {
                        if error.kind() != std::io::ErrorKind::AlreadyExists {
                            return Err(error.into());
                        }
                        let meta = fs::symlink_metadata(&path)?;
                        if !meta.is_dir() || meta.file_type().is_symlink() {
                            return Err(invalid("unsafe journal directory"));
                        }
                    }
                    File::open(path.parent().expect("journal parent"))?.sync_all()?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        let path = directory.join(format!("{key}.json"));
        let mut result = Self {
            path,
            identity,
            confirmed_end_ns: request.range.0,
            overlap_start_ns: request.range.0,
            rows: BTreeMap::new(),
        };
        match fs::symlink_metadata(&result.path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(result),
            Err(error) => return Err(error.into()),
            Ok(meta)
                if !meta.is_file()
                    || meta.file_type().is_symlink()
                    || meta.len() > MAX_JOURNAL_BYTES =>
            {
                return Err(invalid("unsafe or oversized journal"));
            }
            Ok(_) => {}
        }
        let envelope: Envelope = serde_json::from_slice(&fs::read(&result.path)?)?;
        if envelope.checksum != format!("{:x}", Sha256::digest(envelope.payload.as_bytes())) {
            return Err(invalid("journal checksum mismatch"));
        }
        let journal: Journal = serde_json::from_str(&envelope.payload)?;
        if journal.identity != result.identity
            || journal.overlap_start_ns < request.range.0
            || journal.confirmed_end_ns < journal.overlap_start_ns
            || journal.confirmed_end_ns > request.range.1
            || journal.rows.len() > 10_000
        {
            return Err(invalid("journal identity or bounds mismatch"));
        }
        result.confirmed_end_ns = journal.confirmed_end_ns;
        result.overlap_start_ns = journal.overlap_start_ns;
        for (id, datetime, prices, volume, open_oi, close_oi, epoch) in journal.rows {
            if datetime < request.range.0
                || datetime >= request.range.1
                || result.rows.contains_key(&datetime)
            {
                return Err(invalid("journal row outside range or duplicate"));
            }
            result.rows.insert(
                datetime,
                Kline {
                    id,
                    datetime,
                    open: f64::from_bits(prices[0]),
                    high: f64::from_bits(prices[1]),
                    low: f64::from_bits(prices[2]),
                    close: f64::from_bits(prices[3]),
                    volume,
                    open_oi,
                    close_oi,
                    epoch,
                },
            );
        }
        Ok(result)
    }

    /// Re-fetch the last terminal-confirmed window, plus all tentative rows.
    pub(super) fn rewind(&mut self) -> i64 {
        let start = self.overlap_start_ns;
        self.rows.retain(|datetime, _| *datetime < start);
        self.confirmed_end_ns = start;
        start
    }

    pub(super) fn verify_overlap(
        &self,
        expected: &BTreeMap<i64, Kline>,
        end_ns: i64,
    ) -> Result<()> {
        let actual = self
            .rows
            .range(self.overlap_start_ns..end_ns)
            .collect::<Vec<_>>();
        if actual.len() != expected.len()
            || actual.iter().any(|(datetime, row)| {
                expected.get(datetime).is_none_or(|old| {
                    old.id != row.id
                        || old.datetime != row.datetime
                        || old.open.to_bits() != row.open.to_bits()
                        || old.high.to_bits() != row.high.to_bits()
                        || old.low.to_bits() != row.low.to_bits()
                        || old.close.to_bits() != row.close.to_bits()
                        || old.volume != row.volume
                        || old.open_oi != row.open_oi
                        || old.close_oi != row.close_oi
                        || old.epoch != row.epoch
                })
            })
        {
            // Unpublished journal only. A later retry re-fetches the whole window.
            self.remove_after_commit()?;
            return Err(invalid(
                "staged overlap changed; journal discarded, rerun to re-fetch full window",
            ));
        }
        Ok(())
    }

    pub(super) fn save(&self) -> Result<()> {
        if self.rows.len() > 10_000 {
            return Err(invalid("journal exceeds row limit"));
        }
        let journal = Journal {
            identity: Identity {
                version: self.identity.version,
                symbol: self.identity.symbol.clone(),
                range: self.identity.range,
                snapshot: self.identity.snapshot.clone(),
                as_of_ns: self.identity.as_of_ns,
            },
            confirmed_end_ns: self.confirmed_end_ns,
            overlap_start_ns: self.overlap_start_ns,
            rows: self
                .rows
                .values()
                .map(|row| {
                    (
                        row.id,
                        row.datetime,
                        [
                            row.open.to_bits(),
                            row.high.to_bits(),
                            row.low.to_bits(),
                            row.close.to_bits(),
                        ],
                        row.volume,
                        row.open_oi,
                        row.close_oi,
                        row.epoch,
                    )
                })
                .collect(),
        };
        let payload = serde_json::to_string(&journal)?;
        let bytes = serde_json::to_vec(&Envelope {
            checksum: format!("{:x}", Sha256::digest(payload.as_bytes())),
            payload,
        })?;
        if bytes.len() as u64 > MAX_JOURNAL_BYTES {
            return Err(invalid("journal exceeds byte limit"));
        }
        let temporary = self.path.with_extension(format!(
            "{}-{}.tmp",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&temporary, &self.path)?;
            File::open(self.path.parent().expect("journal parent"))?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }

    pub(super) fn remove_after_commit(&self) -> Result<()> {
        match fs::remove_file(&self.path) {
            Ok(()) => File::open(self.path.parent().expect("journal parent"))?
                .sync_all()
                .map_err(Into::into),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

fn invalid(message: &str) -> DataError {
    DataError::InvalidResponse(format!("minute fill staging: {message}"))
}
