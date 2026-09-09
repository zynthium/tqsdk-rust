//! Versioned common history container. Physical blocks and active logical
//! extents are separate: corrections can reference old immutable blocks without
//! confusing append order with time order. Family metadata is not coverage.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use tqsdk_core::{Kline, Tick};

use crate::{DataError, Result, cache_file, kline_codec};

mod allocation;
pub(crate) mod tick;
pub(crate) use allocation::IndexMetadata;

pub(crate) const MAGIC: &[u8; 8] = b"TQHIST01";
const SLOT_BYTES: usize = 48;
const HEADER_BYTES: u64 = 8 + 2 * SLOT_BYTES as u64;
const MAX_INDEX_BYTES: usize = 16 * 1024 * 1024;
const MAX_ENTRIES: usize = 65_536;
const MAX_BLOCK_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const KLINE_BYTES: usize = 81;
pub(crate) const TARGET_KLINE_ROWS: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum SeriesKind {
    Tick,
    Kline { duration_ns: i64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Identity {
    pub symbol: String,
    pub kind: SeriesKind,
    pub partition_scheme: u32,
    pub pack_range: Option<(i64, i64)>,
    pub metadata_schema: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Finality {
    /// Rows are durable, but no final or provisional completeness proof exists.
    /// Only Tick staging may use this state; Kline adapters require final data.
    Unverified,
    Final,
    Provisional {
        as_of_ns: i64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Codec {
    Kline81,
    TickXorV1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Compression {
    None,
    Zstd,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Block {
    offset: u64,
    len: u64,
    decoded_len: u64,
    checksum: u64,
    codec: Codec,
    compression: Compression,
    rows: u64,
    first_ns: i64,
    last_ns: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id_bounds: Option<(i64, i64)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tick_order_strict: Option<bool>,
}

impl Block {
    pub(crate) fn tick_order_follows(&self, previous: Option<&Self>) -> bool {
        self.tick_order_strict == Some(true)
            && previous.is_none_or(|previous| {
                previous.last_ns < self.first_ns
                    && previous
                        .id_bounds
                        .zip(self.id_bounds)
                        .is_some_and(|((_, last), (first, _))| last < first)
            })
    }
    pub(crate) fn id_bounds(&self) -> Option<(i64, i64)> {
        self.id_bounds
    }
    pub(crate) fn payload_sizes(&self) -> (u64, u64) {
        (self.len, self.decoded_len)
    }
    pub fn overlaps(&self, range: (i64, i64)) -> bool {
        self.first_ns < range.1 && self.last_ns >= range.0
    }

    pub fn read_allocation_bytes(&self) -> Result<usize> {
        let row_size = match self.codec {
            Codec::Kline81 => std::mem::size_of::<Kline>(),
            Codec::TickXorV1 => std::mem::size_of::<Tick>(),
        };
        let row_bytes = self.rows.checked_mul(row_size as u64);
        self.len
            .checked_add(self.decoded_len)
            .and_then(|bytes| bytes.checked_add(row_bytes?))
            .and_then(|bytes| bytes.checked_add(self.decoder_workspace_bytes() as u64))
            .and_then(|bytes| usize::try_from(bytes).ok())
            .ok_or_else(|| invalid("block allocation overflow"))
    }

    fn decoder_workspace_bytes(&self) -> usize {
        if self.compression == Compression::Zstd {
            zstd_decoder_workspace_bytes()
        } else {
            0
        }
    }
}

#[cfg(feature = "tqbn-zstd")]
#[allow(unsafe_code)]
fn zstd_decoder_workspace_bytes() -> usize {
    // SAFETY: This parameterless zstd query only reports the workspace size.
    // Our one-shot decoder uses no dictionaries or streaming buffers. zstd
    // exposes this estimate through its static API; zstd-safe lacks a wrapper.
    unsafe { zstd::zstd_safe::zstd_sys::ZSTD_estimateDCtxSize() }
}

#[cfg(not(feature = "tqbn-zstd"))]
fn zstd_decoder_workspace_bytes() -> usize {
    0
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Slice {
    pub block: usize,
    pub row_start: u64,
    pub rows: u64,
    pub first_ns: i64,
    pub last_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Extent {
    pub start_ns: i64,
    pub end_ns: i64,
    pub logical_partition: String,
    pub metadata: usize,
    pub finality: Finality,
    // Empty slices explicitly prove a successful empty interval.
    pub slices: Vec<Slice>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Index<M> {
    pub identity: Identity,
    pub metadata: Vec<M>,
    pub extents: Vec<Extent>,
    pub blocks: Vec<Block>,
    #[serde(skip)]
    pub generation: u64,
    #[serde(skip)]
    pub committed_len: u64,
    #[serde(skip)]
    pub index_bytes: usize,
    #[serde(skip)]
    pub allocation_bytes: usize,
}

type ActiveRowRange = (usize, u64, u64);

impl<M> Index<M> {
    /// Old commit indexes remain in the append log until compaction. Count
    /// their bytes separately from payload so metadata growth is measurable.
    pub fn retired_index_bytes(&self) -> u64 {
        let payload_bytes: u64 = self.blocks.iter().map(|block| block.len).sum();
        self.committed_len
            .saturating_sub(HEADER_BYTES)
            .saturating_sub(payload_bytes)
            .saturating_sub(self.index_bytes as u64)
    }

    pub fn uses_zstd(&self) -> bool {
        self.blocks
            .iter()
            .any(|block| block.compression == Compression::Zstd)
    }
    pub fn new(identity: Identity, metadata: Vec<M>) -> Self {
        Self {
            identity,
            metadata,
            extents: Vec::new(),
            blocks: Vec::new(),
            generation: 0,
            committed_len: 0,
            index_bytes: 0,
            allocation_bytes: 0,
        }
    }

    pub fn require_clean_tail(&self, file: &File) -> Result<()> {
        if file.metadata()?.len() != self.committed_len {
            return Err(invalid("uncommitted suffix; recover before publishing"));
        }
        Ok(())
    }

    pub fn active_rows(&self) -> Result<u64> {
        self.extents
            .iter()
            .flat_map(|e| &e.slices)
            .try_fold(0_u64, |rows, slice| {
                rows.checked_add(slice.rows)
                    .ok_or_else(|| invalid("row count overflow"))
            })
    }

    pub fn validate(&self, index_offset: u64) -> Result<()> {
        if self.identity.symbol.is_empty()
            || self.identity.symbol.len() > 4096
            || self.identity.partition_scheme != 1
            || self.identity.metadata_schema != 1
            || matches!(self.identity.kind, SeriesKind::Kline { duration_ns } if duration_ns <= 0)
            || self
                .identity
                .pack_range
                .is_some_and(|(start, end)| start >= end)
            || self.blocks.len() > MAX_ENTRIES
            || self.extents.len() > MAX_ENTRIES
            || self.metadata.len() > MAX_ENTRIES
        {
            return Err(invalid("invalid identity or index cardinality"));
        }
        let mut previous_end = HEADER_BYTES;
        for block in &self.blocks {
            let end = block
                .offset
                .checked_add(block.len)
                .ok_or_else(|| invalid("block bounds overflow"))?;
            if block.offset < previous_end
                || block.len == 0
                || block.rows == 0
                || end > index_offset
                || block.first_ns > block.last_ns
                || block.len > MAX_BLOCK_BYTES as u64
                || block.decoded_len == 0
                || block.decoded_len > MAX_BLOCK_BYTES as u64
                || matches!(block.compression, Compression::None) && block.len != block.decoded_len
                || !matches!(
                    (self.identity.kind, block.codec),
                    (SeriesKind::Kline { .. }, Codec::Kline81)
                        | (SeriesKind::Tick, Codec::TickXorV1)
                )
                || block.codec == Codec::Kline81
                    && block.rows.checked_mul(KLINE_BYTES as u64) != Some(block.decoded_len)
                || block.codec == Codec::TickXorV1
                    && !tick::valid_length(block.rows, block.decoded_len)
                || block.codec == Codec::TickXorV1
                    && block.id_bounds.is_none_or(|(start, end)| start > end)
                || block.codec == Codec::TickXorV1 && block.tick_order_strict.is_none()
            {
                return Err(invalid("invalid physical block table"));
            }
            previous_end = end;
        }
        let mut previous_time = None;
        let total_slices = self.extents.iter().try_fold(0_usize, |count, extent| {
            count
                .checked_add(extent.slices.len())
                .filter(|count| *count <= MAX_ENTRIES)
                .ok_or_else(|| invalid("too many block slices"))
        })?;
        let mut row_slices = Vec::<ActiveRowRange>::new();
        row_slices
            .try_reserve_exact(total_slices)
            .map_err(|_| invalid("cannot allocate row-slice validation scratch"))?;
        let mut slice_count = 0_usize;
        for extent in &self.extents {
            if extent.start_ns >= extent.end_ns
                || (extent.finality == Finality::Unverified
                    && self.identity.kind != SeriesKind::Tick)
                || extent.metadata >= self.metadata.len()
                || extent.logical_partition.is_empty()
                || extent.logical_partition.len() > 128
                || previous_time.is_some_and(|end| extent.start_ns < end)
                || self
                    .identity
                    .pack_range
                    .is_some_and(|(start, end)| extent.start_ns < start || extent.end_ns > end)
            {
                return Err(invalid("invalid or overlapping active extents"));
            }
            previous_time = Some(extent.end_ns);
            let mut last_slice_time = None;
            for slice in &extent.slices {
                slice_count = slice_count
                    .checked_add(1)
                    .ok_or_else(|| invalid("slice count overflow"))?;
                if slice_count > MAX_ENTRIES {
                    return Err(invalid("too many active slices"));
                }
                let block = self
                    .blocks
                    .get(slice.block)
                    .ok_or_else(|| invalid("unknown block reference"))?;
                let end = slice
                    .row_start
                    .checked_add(slice.rows)
                    .ok_or_else(|| invalid("slice overflow"))?;
                if slice.rows == 0
                    || end > block.rows
                    || slice.first_ns > slice.last_ns
                    || slice.first_ns < extent.start_ns
                    || slice.last_ns >= extent.end_ns
                    || slice.first_ns < block.first_ns
                    || slice.last_ns > block.last_ns
                    || (slice.row_start == 0
                        && slice.rows == block.rows
                        && (slice.first_ns != block.first_ns || slice.last_ns != block.last_ns))
                    || last_slice_time.is_some_and(|time| {
                        slice.first_ns < time
                            || (slice.first_ns == time
                                && matches!(self.identity.kind, SeriesKind::Kline { .. }))
                    })
                {
                    return Err(invalid("invalid active block slice"));
                }
                last_slice_time = Some(slice.last_ns);
                row_slices.push((slice.block, slice.row_start, end));
            }
        }
        row_slices.sort_unstable();
        if row_slices
            .windows(2)
            .any(|pair| pair[0].0 == pair[1].0 && pair[0].2 > pair[1].1)
        {
            return Err(invalid("active extents duplicate block rows"));
        }
        self.active_rows()?;
        Ok(())
    }
}

pub(crate) struct EncodedBlock {
    block: Block,
    payload: Vec<u8>,
}

impl EncodedBlock {
    pub fn slice(&self, block: usize) -> Slice {
        Slice {
            block,
            row_start: 0,
            rows: self.block.rows,
            first_ns: self.block.first_ns,
            last_ns: self.block.last_ns,
        }
    }
}

pub(crate) fn encode_klines(rows: &[Kline]) -> Result<EncodedBlock> {
    let len = rows
        .len()
        .checked_mul(KLINE_BYTES)
        .ok_or_else(|| invalid("Kline size overflow"))?;
    if rows.is_empty()
        || len > MAX_BLOCK_BYTES
        || rows
            .windows(2)
            .any(|pair| pair[0].datetime >= pair[1].datetime)
    {
        return Err(invalid("Kline block must be nonempty, bounded and ordered"));
    }
    let mut payload = Vec::with_capacity(len);
    for row in rows {
        kline_codec::encode_fields(&mut payload, row);
        payload.push(u8::from(row.epoch.is_some()));
        payload.extend_from_slice(&row.epoch.unwrap_or(0).to_le_bytes());
    }
    let compression = compress(&mut payload)?;
    Ok(EncodedBlock {
        block: Block {
            offset: 0,
            len: payload.len() as u64,
            decoded_len: len as u64,
            checksum: checksum(&payload),
            codec: Codec::Kline81,
            id_bounds: None,
            tick_order_strict: None,
            compression,
            rows: rows.len() as u64,
            first_ns: rows[0].datetime,
            last_ns: rows[rows.len() - 1].datetime,
        },
        payload,
    })
}

fn compress(payload: &mut Vec<u8>) -> Result<Compression> {
    #[cfg(feature = "tqbn-zstd")]
    {
        let compressed = zstd::bulk::compress(payload, 3).map_err(|e| invalid(&e.to_string()))?;
        if compressed.len() < payload.len() {
            *payload = compressed;
            return Ok(Compression::Zstd);
        }
    }
    #[cfg(not(feature = "tqbn-zstd"))]
    let _ = payload;
    Ok(Compression::None)
}

pub(crate) fn load<M: IndexMetadata>(
    file: &mut File,
    expected: SeriesKind,
    allocation_limit: Option<usize>,
) -> Result<Index<M>> {
    let index = load_any(file, allocation_limit)?;
    if index.identity.kind != expected {
        return Err(invalid("container series kind mismatch"));
    }
    Ok(index)
}

pub(crate) fn load_any<M: IndexMetadata>(
    file: &mut File,
    allocation_limit: Option<usize>,
) -> Result<Index<M>> {
    let [generation, offset, len, committed_len, digest] = read_commit(file)?;
    check_allocation(allocation::base::<M>(len as usize)?, allocation_limit)?;
    let mut bytes = vec![0; len as usize];
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut bytes)?;
    if checksum(&bytes) != digest {
        return Err(invalid("committed index checksum mismatch"));
    }
    let mut index = allocation::decode::<M>(&bytes, allocation_limit)?;
    index.validate(offset)?;
    index.generation = generation;
    index.committed_len = committed_len;
    index.index_bytes = bytes.len();
    Ok(index)
}

pub(crate) fn read_klines<M>(
    file: &mut File,
    index: &Index<M>,
    id: usize,
    allocation_limit: Option<usize>,
) -> Result<Vec<Kline>> {
    let block = index
        .blocks
        .get(id)
        .ok_or_else(|| invalid("unknown block"))?;
    if block.codec != Codec::Kline81 {
        return Err(invalid("not a Kline block"));
    }
    let allocation = block.read_allocation_bytes()?;
    check_allocation(allocation, allocation_limit)?;
    let payload = read_block(file, index, id)?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(block.rows as usize)
        .map_err(|_| invalid("cannot allocate decoded Kline rows"))?;
    for bytes in payload.as_chunks::<KLINE_BYTES>().0 {
        let epoch = i64::from_le_bytes(bytes[73..81].try_into().unwrap());
        let epoch = match bytes[72] {
            0 if epoch == 0 => None,
            1 => Some(epoch),
            _ => return Err(invalid("invalid epoch presence/value")),
        };
        rows.push(kline_codec::decode_fields(
            bytes[..72].try_into().unwrap(),
            epoch,
        ));
    }
    if rows.len() as u64 != block.rows
        || rows.first().map(|r| r.datetime) != Some(block.first_ns)
        || rows.last().map(|r| r.datetime) != Some(block.last_ns)
        || rows
            .windows(2)
            .any(|pair| pair[0].datetime >= pair[1].datetime)
    {
        return Err(invalid("Kline block index disagrees with decoded rows"));
    }
    Ok(rows)
}

fn read_block<M>(file: &mut File, index: &Index<M>, id: usize) -> Result<Vec<u8>> {
    let block = index
        .blocks
        .get(id)
        .ok_or_else(|| invalid("unknown block"))?;
    if block
        .offset
        .checked_add(block.len)
        .is_none_or(|end| end > index.committed_len)
        || block.len > MAX_BLOCK_BYTES as u64
        || block.decoded_len > MAX_BLOCK_BYTES as u64
    {
        return Err(invalid("block outside pinned prefix"));
    }
    file.seek(SeekFrom::Start(block.offset))?;
    let mut payload = vec![0; block.len as usize];
    file.read_exact(&mut payload)?;
    if checksum(&payload) != block.checksum {
        return Err(invalid("block checksum mismatch"));
    }
    let decoded = match block.compression {
        Compression::None => payload,
        Compression::Zstd => {
            #[cfg(feature = "tqbn-zstd")]
            {
                zstd::bulk::decompress(&payload, block.decoded_len as usize)
                    .map_err(|e| invalid(&e.to_string()))?
            }
            #[cfg(not(feature = "tqbn-zstd"))]
            {
                return Err(invalid("compressed history block requires tqbn-zstd"));
            }
        }
    };
    if decoded.len() as u64 != block.decoded_len {
        return Err(invalid("decoded block length mismatch"));
    }
    Ok(decoded)
}

pub(crate) fn recover<M>(path: &Path, file: &mut File, index: &Index<M>) -> Result<()> {
    let current = read_commit(file)?;
    if current[0] != index.generation || current[3] != index.committed_len {
        return Err(invalid(
            "stale writer generation; reopen under the partition lock",
        ));
    }
    if file.metadata()?.len() < index.committed_len {
        return Err(invalid("committed prefix truncated"));
    }
    cache_file::detach(path, file, false)?;
    if file.metadata()?.len() != index.committed_len {
        file.set_len(index.committed_len)?;
        file.sync_all()?;
    }
    file.seek(SeekFrom::Start(index.committed_len))?;
    Ok(())
}

pub(crate) fn create<M: Serialize>(
    path: &Path,
    mut index: Index<M>,
    blocks: &[EncodedBlock],
) -> Result<()> {
    if index.generation != 0 || !index.blocks.is_empty() {
        return Err(invalid("new container has old blocks"));
    }
    let mut candidate = cache_file::Candidate::create(path)?;
    write_new(&mut candidate.file, &mut index, blocks)?;
    candidate.publish(path)
}

pub(crate) fn write_new<M: Serialize>(
    file: &mut File,
    index: &mut Index<M>,
    blocks: &[EncodedBlock],
) -> Result<()> {
    if file.metadata()?.len() != 0 || index.generation != 0 || !index.blocks.is_empty() {
        return Err(invalid("new container must be empty"));
    }
    file.write_all(MAGIC)?;
    file.write_all(&[0; 2 * SLOT_BYTES])?;
    write_commit(file, index, blocks)
}

pub(crate) fn append<M: Serialize>(
    path: &Path,
    file: &mut File,
    mut index: Index<M>,
    blocks: &[EncodedBlock],
) -> Result<()> {
    if index.generation == 0 {
        return Err(invalid("append requires a committed generation"));
    }
    recover(path, file, &index)?;
    write_commit(file, &mut index, blocks)
}

fn write_commit<M: Serialize>(
    file: &mut File,
    index: &mut Index<M>,
    blocks: &[EncodedBlock],
) -> Result<()> {
    let generation = index
        .generation
        .checked_add(1)
        .ok_or_else(|| invalid("generation exhausted"))?;
    for encoded in blocks {
        let mut block = encoded.block.clone();
        block.offset = file.stream_position()?;
        file.write_all(&encoded.payload)?;
        index.blocks.push(block);
    }
    let offset = file.stream_position()?;
    index.validate(offset)?;
    let bytes = serde_json::to_vec(index).map_err(|e| invalid(&e.to_string()))?;
    if bytes.len() > MAX_INDEX_BYTES {
        return Err(invalid("index too large"));
    }
    let end = offset
        .checked_add(bytes.len() as u64)
        .ok_or_else(|| invalid("index bounds overflow"))?;
    file.write_all(&bytes)?;
    // Both payload and index become durable before either can be visible.
    file.sync_all()?;
    let mut slot = [0; SLOT_BYTES];
    for (chunk, value) in slot[..40].as_chunks_mut::<8>().0.iter_mut().zip([
        generation,
        offset,
        bytes.len() as u64,
        end,
        checksum(&bytes),
    ]) {
        chunk.copy_from_slice(&value.to_le_bytes());
    }
    let digest = checksum(&slot[..40]);
    slot[40..].copy_from_slice(&digest.to_le_bytes());
    file.seek(SeekFrom::Start(8 + generation % 2 * SLOT_BYTES as u64))?;
    file.write_all(&slot)?;
    file.sync_all()?;
    Ok(())
}

fn read_commit(file: &mut File) -> Result<[u64; 5]> {
    file.seek(SeekFrom::Start(0))?;
    let mut magic = [0; 8];
    file.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(invalid(if magic.starts_with(b"TQHIST") {
            "unsupported container version; explicit migration required"
        } else {
            "invalid container magic; explicit migration required"
        }));
    }
    let mut selected: Option<[u64; 5]> = None;
    for _ in 0..2 {
        let mut slot = [0; SLOT_BYTES];
        file.read_exact(&mut slot)?;
        let words: [u64; 6] = std::array::from_fn(|i| {
            u64::from_le_bytes(slot[i * 8..(i + 1) * 8].try_into().unwrap())
        });
        if words[0] == 0 || checksum(&slot[..40]) != words[5] {
            continue;
        }
        let candidate: [u64; 5] = words[..5].try_into().unwrap();
        if selected.is_some_and(|previous| previous[0] == candidate[0] && previous != candidate) {
            return Err(invalid("ambiguous commit generation"));
        }
        if selected.is_none_or(|previous| candidate[0] > previous[0]) {
            selected = Some(candidate);
        }
    }
    let [generation, offset, len, committed_len, digest] =
        selected.ok_or_else(|| invalid("no valid commit slot"))?;
    if offset < HEADER_BYTES
        || len > MAX_INDEX_BYTES as u64
        || offset.checked_add(len) != Some(committed_len)
        || committed_len > file.metadata()?.len()
    {
        return Err(invalid("invalid committed index bounds"));
    }
    Ok([generation, offset, len, committed_len, digest])
}

fn check_allocation(attempted_bytes: usize, limit: Option<usize>) -> Result<()> {
    if let Some(limit_bytes) = limit
        && attempted_bytes > limit_bytes
    {
        return Err(DataError::CollectLimitExceeded {
            limit_bytes,
            attempted_bytes,
        });
    }
    Ok(())
}

fn checksum(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

fn invalid(message: &str) -> DataError {
    DataError::InvalidResponse(format!("history container: {message}"))
}

#[cfg(test)]
mod tests;
