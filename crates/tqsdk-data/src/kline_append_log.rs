//! Internal append envelope for independently encoded canonical Kline segments.
//!
//! The data and index are synced before publishing an alternating commit slot.
//! Readers pin a committed prefix; an incomplete suffix is never coverage.
#[cfg(test)]
use std::fs;
use std::fs::{File, OpenOptions};
#[cfg(test)]
use std::io::Write;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use fs2::FileExt;
use serde::{Deserialize, Serialize};

use crate::{DataError, Result};

const MAGIC: &[u8; 8] = b"TQKLOG01";
const SLOT_BYTES: usize = 48;
const HEADER_BYTES: u64 = 8 + 2 * SLOT_BYTES as u64;
const MAX_INDEX_BYTES: u64 = 16 * 1024 * 1024;

pub(crate) fn pin_for_diagnosis(path: &Path) -> Result<Option<File>> {
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .ok_or_else(|| invalid("missing partition extension"))?;
    let file = match File::open(path.with_extension(format!("{extension}.lock"))) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    FileExt::lock_shared(&file)?;
    Ok(Some(file))
}

/// A fill preflight may wait for another writer, but must remain cancellable.
pub(crate) fn lock_for_fill(path: &Path, cancelled: impl Fn() -> bool) -> Result<Option<File>> {
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .ok_or_else(|| invalid("missing partition extension"))?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path.with_extension(format!("{extension}.lock")))?;
    loop {
        if cancelled() {
            return Ok(None);
        }
        match FileExt::try_lock_exclusive(&file) {
            Ok(()) => return Ok(Some(file)),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(20))
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Segment {
    pub offset: u64,
    pub len: u64,
    pub checksum: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Index<T> {
    pub segments: Vec<Segment>,
    pub summary: T,
    #[serde(skip)]
    generation: u64,
    #[serde(skip)]
    committed_len: u64,
}

impl<T> Index<T> {
    pub(crate) fn require_clean_tail(&self, file: &File) -> Result<()> {
        if file.metadata()?.len() != self.committed_len {
            return Err(invalid(
                "uncommitted tail; rerun fill before publishing or auditing",
            ));
        }
        Ok(())
    }
}

fn invalid(message: &str) -> DataError {
    DataError::InvalidResponse(format!("Kline append log: {message}"))
}

fn checksum(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

pub(crate) fn migration_required() -> DataError {
    invalid("legacy canonical Kline file; run the explicit offline migrate_kline_cache tool")
}

pub(crate) fn require<T: serde::de::DeserializeOwned>(file: &mut File) -> Result<Index<T>> {
    load(file)?.ok_or_else(migration_required)
}

pub(crate) fn load<T: serde::de::DeserializeOwned>(file: &mut File) -> Result<Option<Index<T>>> {
    load_bounded(file, None)
}

pub(crate) fn load_bounded<T: serde::de::DeserializeOwned>(
    file: &mut File,
    max_allocation_bytes: Option<usize>,
) -> Result<Option<Index<T>>> {
    file.seek(SeekFrom::Start(0))?;
    let mut magic = [0; 8];
    if file.read(&mut magic)? != magic.len() || &magic != MAGIC {
        if magic.starts_with(b"TQKLOG") {
            return Err(invalid("unsupported append envelope version"));
        }
        file.seek(SeekFrom::Start(0))?;
        return Ok(None);
    }
    let mut selected = None;
    for _ in 0..2 {
        let mut bytes = [0; SLOT_BYTES];
        file.read_exact(&mut bytes)?;
        let value = |offset| u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        if checksum(&bytes[..40]) != value(40) || value(0) == 0 {
            continue;
        }
        let candidate = (value(0), value(8), value(16), value(24), value(32));
        if selected.is_none_or(|previous: (u64, u64, u64, u64, u64)| candidate.0 > previous.0) {
            selected = Some(candidate);
        }
    }
    let (generation, offset, len, end, expected_checksum) =
        selected.ok_or_else(|| invalid("no valid commit slot"))?;
    let physical_len = file.metadata()?.len();
    if offset < HEADER_BYTES
        || len > MAX_INDEX_BYTES
        || offset.checked_add(len) != Some(end)
        || end > physical_len
    {
        return Err(invalid("committed index bounds are invalid"));
    }
    if let Some(limit) = max_allocation_bytes
        && len.saturating_mul(4) > limit as u64
    {
        return Err(DataError::CollectLimitExceeded {
            limit_bytes: limit,
            attempted_bytes: usize::try_from(len.saturating_mul(4)).unwrap_or(usize::MAX),
        });
    }
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![0; len as usize];
    file.read_exact(&mut bytes)?;
    if checksum(&bytes) != expected_checksum {
        return Err(invalid("committed index checksum mismatch"));
    }
    let mut index: Index<T> =
        serde_json::from_slice(&bytes).map_err(|_| invalid("invalid committed index"))?;
    let mut previous_end = HEADER_BYTES;
    for segment in &index.segments {
        if segment.offset < previous_end
            || segment.len == 0
            || segment
                .offset
                .checked_add(segment.len)
                .is_none_or(|end| end > offset)
        {
            return Err(invalid("segment bounds are invalid"));
        }
        previous_end = segment.offset + segment.len;
    }
    if index.segments.is_empty() {
        return Err(invalid("empty segment index"));
    }
    index.generation = generation;
    index.committed_len = end;
    Ok(Some(index))
}

pub(crate) fn read_segment(file: &mut File, segment: &Segment) -> Result<Vec<u8>> {
    let len = usize::try_from(segment.len).map_err(|_| invalid("segment is too large"))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(len)
        .map_err(|_| invalid("segment allocation failed"))?;
    bytes.resize(len, 0);
    file.seek(SeekFrom::Start(segment.offset))?;
    file.read_exact(&mut bytes)?;
    if checksum(&bytes) != segment.checksum {
        return Err(invalid("segment checksum mismatch"));
    }
    Ok(bytes)
}

/// Bounded, checksummed input; decoding never buffers a whole minute partition.
pub(crate) struct SegmentInput {
    file: std::io::Take<File>,
    checksum: u64,
    expected: u64,
}

impl SegmentInput {
    pub(crate) fn open(mut file: File, segment: &Segment) -> Result<Self> {
        file.seek(SeekFrom::Start(segment.offset))?;
        Ok(Self {
            file: file.take(segment.len),
            checksum: 0xcbf29ce484222325,
            expected: segment.checksum,
        })
    }
}

impl Read for SegmentInput {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        let count = self.file.read(bytes)?;
        for byte in &bytes[..count] {
            self.checksum = (self.checksum ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
        }
        if !bytes.is_empty()
            && count == 0
            && (self.file.limit() != 0 || self.checksum != self.expected)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Kline append segment checksum or length mismatch",
            ));
        }
        Ok(count)
    }
}

/// Caller must hold the canonical partition's exclusive lock.
#[cfg(test)]
pub(crate) fn append<T: Serialize>(
    path: &Path,
    file: &mut File,
    mut index: Index<T>,
    summary: T,
    payload: &[u8],
) -> Result<()> {
    recover(path, file, &index)?;
    file.seek(SeekFrom::Start(index.committed_len))?;
    let segment = Segment {
        offset: index.committed_len,
        len: payload.len() as u64,
        checksum: checksum(payload),
    };
    file.write_all(payload)?;
    index.segments.push(segment);
    index.summary = summary;
    publish(file, index)
}

#[cfg(test)]
pub(crate) fn recover<T>(path: &Path, file: &mut File, index: &Index<T>) -> Result<()> {
    #[cfg(unix)]
    let shared = {
        use std::os::unix::fs::MetadataExt;
        file.metadata()?.nlink() > 1
    };
    #[cfg(not(unix))]
    let shared = true;
    if shared {
        let temporary = path.with_extension(format!(
            "cow-{}-{}.tmp",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let result = (|| -> Result<()> {
            let mut output = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)?;
            file.seek(SeekFrom::Start(0))?;
            std::io::copy(&mut (&mut *file).take(index.committed_len), &mut output)?;
            output.sync_all()?;
            fs::rename(&temporary, path)?;
            File::open(path.parent().ok_or_else(|| invalid("missing parent"))?)?.sync_all()?;
            *file = OpenOptions::new().read(true).write(true).open(path)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(temporary);
        }
        result?;
    }
    if file.metadata()?.len() != index.committed_len {
        file.set_len(index.committed_len)?;
        file.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
fn publish<T: Serialize>(file: &mut File, index: Index<T>) -> Result<()> {
    let generation = index
        .generation
        .checked_add(1)
        .ok_or_else(|| invalid("generation overflow"))?;
    let offset = file.stream_position()?;
    let bytes = serde_json::to_vec(&index).map_err(|_| invalid("index encoding failed"))?;
    if bytes.len() as u64 > MAX_INDEX_BYTES {
        return Err(invalid("index exceeds size limit; compact the partition"));
    }
    file.write_all(&bytes)?;
    file.sync_all()?;
    let mut slot = [0; SLOT_BYTES];
    for (destination, value) in slot[..40].as_chunks_mut::<8>().0.iter_mut().zip([
        generation,
        offset,
        bytes.len() as u64,
        offset + bytes.len() as u64,
        checksum(&bytes),
    ]) {
        destination.copy_from_slice(&value.to_le_bytes());
    }
    let digest = checksum(&slot[..40]);
    slot[40..].copy_from_slice(&digest.to_le_bytes());
    file.seek(SeekFrom::Start(8 + (generation % 2) * SLOT_BYTES as u64))?;
    file.write_all(&slot)?;
    file.sync_all()?;
    Ok(())
}

/// Atomically converts a validated legacy payload and its first extension.
#[cfg(test)]
pub(crate) fn create<T: Serialize>(path: &Path, summary: T, payloads: &[&[u8]]) -> Result<()> {
    let temporary = path.with_extension(format!(
        "append-{}-{}.tmp",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(MAGIC)?;
        file.write_all(&[0; SLOT_BYTES * 2])?;
        let mut segments = Vec::new();
        for payload in payloads {
            let offset = file.stream_position()?;
            file.write_all(payload)?;
            segments.push(Segment {
                offset,
                len: payload.len() as u64,
                checksum: checksum(payload),
            });
        }
        publish(
            &mut file,
            Index {
                segments,
                summary,
                generation: 0,
                committed_len: 0,
            },
        )?;
        fs::rename(&temporary, path)?;
        File::open(path.parent().ok_or_else(|| invalid("missing parent"))?)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn torn_commit_keeps_previous_prefix_but_committed_index_corruption_fails_closed() {
        let root = std::env::temp_dir().join(format!(
            "kline-log-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("log");
        create(&path, 1_u64, &[b"first"]).unwrap();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let old = load::<u64>(&mut file).unwrap().unwrap();
        append(&path, &mut file, old.clone(), 2, b"second").unwrap();
        let newest = load::<u64>(&mut file).unwrap().unwrap();
        file.seek(SeekFrom::Start(8)).unwrap();
        file.write_all(&[0; 8]).unwrap();
        let recovered = load::<u64>(&mut file).unwrap().unwrap();
        assert_eq!(recovered.summary, 1);
        assert_eq!(
            read_segment(&mut file, &recovered.segments[0]).unwrap(),
            b"first"
        );
        assert!(recovered.require_clean_tail(&file).is_err());
        recover(&path, &mut file, &recovered).unwrap();
        assert_eq!(file.metadata().unwrap().len(), old.committed_len);
        append(&path, &mut file, recovered, 3, b"third").unwrap();
        let committed = load::<u64>(&mut file).unwrap().unwrap();
        assert_eq!(committed.summary, 3);
        assert!(committed.committed_len <= newest.committed_len + 64);
        file.seek(SeekFrom::Start(committed.committed_len - 1))
            .unwrap();
        file.write_all(b"!").unwrap();
        assert!(load::<u64>(&mut file).is_err());
        drop(file);
        fs::remove_dir_all(root).unwrap();
    }
}
