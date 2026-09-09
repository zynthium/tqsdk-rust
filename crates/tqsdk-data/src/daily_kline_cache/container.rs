//! Native daily adapter for the common indexed container. The physical index
//! owns coverage and row counts; DailySummary is a derived compatibility view.

use super::*;
use crate::history_container::{
    self as storage, EncodedBlock, Extent, Finality, Identity, SeriesKind,
};
use std::io::{Read, Seek, SeekFrom};

const KIND: SeriesKind = SeriesKind::Kline {
    duration_ns: DAILY_KLINE_DURATION_NS,
};

#[cfg(test)]
mod tests;

pub(super) struct LogIndex {
    inner: storage::Index<DailyKlineCacheSnapshot>,
    pub summary: DailySummary,
    allocation_bytes: usize,
}

impl LogIndex {
    pub fn needs_compaction(&self) -> bool {
        self.inner.blocks.len() >= 64
            || self.inner.extents.len() >= 64
            || self.inner.retired_index_bytes() >= 256 * 1024
    }

    pub fn require_clean_tail(&self, file: &File) -> Result<()> {
        self.inner.require_clean_tail(file)
    }
}

pub(super) fn load(input: &mut File) -> Result<Option<LogIndex>> {
    input.seek(SeekFrom::Start(0))?;
    let mut magic = [0; 8];
    if input.read(&mut magic)? != magic.len() || &magic != storage::MAGIC {
        input.seek(SeekFrom::Start(0))?;
        return Ok(None);
    }
    require(input, None).map(Some)
}

pub(super) fn require(input: &mut File, limit: Option<usize>) -> Result<LogIndex> {
    let inner = storage::load::<DailyKlineCacheSnapshot>(input, KIND, limit)?;
    if inner.identity.pack_range.is_some()
        || inner.metadata.len() != 1
        || inner.extents.iter().any(|extent| {
            extent.metadata != 0
                || extent.finality != Finality::Final
                || extent.logical_partition != "daily"
        })
    {
        return Err(DataError::InvalidResponse(
            "daily container metadata/finality mismatch".into(),
        ));
    }
    // Include the derived view and temporary snapshot validation copies.
    // Preflight before cloning strings or allocating the coverage array.
    let overflow = || DataError::InvalidResponse("daily summary allocation overflow".into());
    let strings = inner.metadata[0]
        .calendar_hash
        .len()
        .checked_add(inner.metadata[0].session_hash.len())
        .ok_or_else(overflow)?;
    let allocation_bytes = inner
        .extents
        .len()
        .checked_mul(std::mem::size_of::<(i64, i64)>())
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<DailySummary>()))
        .and_then(|bytes| bytes.checked_add(inner.identity.symbol.len()))
        .and_then(|bytes| bytes.checked_add(strings.checked_mul(2)?))
        .and_then(|bytes| bytes.checked_add(inner.allocation_bytes))
        .ok_or_else(overflow)?;
    if limit.is_some_and(|limit| allocation_bytes > limit) {
        return Err(DataError::CollectLimitExceeded {
            limit_bytes: limit.unwrap(),
            attempted_bytes: allocation_bytes,
        });
    }
    let mut coverage: Vec<(i64, i64)> = Vec::new();
    coverage
        .try_reserve_exact(inner.extents.len())
        .map_err(|_| overflow())?;
    for extent in &inner.extents {
        match coverage.last_mut() {
            Some(previous) if previous.1 == extent.start_ns => previous.1 = extent.end_ns,
            _ => coverage.push((extent.start_ns, extent.end_ns)),
        }
    }
    let summary = DailySummary {
        symbol: inner.identity.symbol.clone(),
        snapshot: inner.metadata[0].clone(),
        coverage,
        rows: usize::try_from(inner.active_rows()?)
            .map_err(|_| DataError::InvalidResponse("daily row count overflow".into()))?,
    };
    validate_symbol(&summary.symbol)?;
    validate_snapshot(&summary.snapshot)?;
    if summary.rows > MAX_ROWS || summary.coverage.len() > MAX_COVERAGE_RECORDS {
        return Err(DataError::InvalidResponse(
            "daily container exceeds limits".into(),
        ));
    }
    Ok(LogIndex {
        inner,
        summary,
        allocation_bytes,
    })
}

pub(super) fn recover(path: &Path, file: &mut File, index: &LogIndex) -> Result<()> {
    storage::recover(path, file, &index.inner)
}

fn encode(file: &DailyFile, first_block: usize) -> Result<(Vec<EncodedBlock>, Vec<Extent>)> {
    validate_file(file)?;
    let mut blocks = Vec::new();
    let mut extents = Vec::new();
    for &(start_ns, end_ns) in &file.coverage {
        let first = file.rows.partition_point(|row| row.datetime < start_ns);
        let last = file.rows.partition_point(|row| row.datetime < end_ns);
        let mut slices = Vec::new();
        for chunk in file.rows[first..last].chunks(storage::TARGET_KLINE_ROWS) {
            let block = storage::encode_klines(chunk)?;
            slices.push(block.slice(first_block + blocks.len()));
            blocks.push(block);
        }
        extents.push(Extent {
            start_ns,
            end_ns,
            logical_partition: "daily".into(),
            metadata: 0,
            finality: Finality::Final,
            slices,
        });
    }
    Ok((blocks, extents))
}

fn build(file: &DailyFile) -> Result<(storage::Index<DailyKlineCacheSnapshot>, Vec<EncodedBlock>)> {
    let (blocks, extents) = encode(file, 0)?;
    let mut index = storage::Index::new(
        Identity {
            symbol: file.symbol.clone(),
            kind: KIND,
            partition_scheme: 1,
            pack_range: None,
            metadata_schema: 1,
        },
        vec![file.snapshot.clone()],
    );
    index.extents = extents;
    Ok((index, blocks))
}

pub(super) fn create(path: &Path, file: &DailyFile) -> Result<()> {
    let (index, blocks) = build(file)?;
    storage::create(path, index, &blocks)
}

pub(super) fn migrate(path: &Path, file: &DailyFile) -> Result<()> {
    let (mut index, blocks) = build(file)?;
    let mut candidate = crate::cache_file::Candidate::create(path)?;
    storage::write_new(&mut candidate.file, &mut index, &blocks)?;
    let verified = read_full(&mut candidate.file, None)?;
    if verified.symbol != file.symbol
        || verified.snapshot != file.snapshot
        || verified.coverage != file.coverage
        || !crate::kline_codec::rows_equal(&verified.rows, &file.rows)
    {
        return Err(DataError::InvalidResponse(
            "daily migration changed logical data".into(),
        ));
    }
    candidate.publish(path)
}

pub(super) fn append(
    path: &Path,
    input: &mut File,
    mut index: LogIndex,
    delta: &DailyFile,
) -> Result<()> {
    if index.summary.symbol != delta.symbol || index.summary.snapshot != delta.snapshot {
        return Err(DataError::InvalidResponse(
            "daily append identity mismatch".into(),
        ));
    }
    let (blocks, extents) = encode(delta, index.inner.blocks.len())?;
    index.inner.extents.extend(extents);
    storage::append(path, input, index.inner, &blocks)
}

fn selected_slices(index: &LogIndex, range: (i64, i64)) -> impl Iterator<Item = &storage::Slice> {
    index
        .inner
        .extents
        .iter()
        .filter(move |extent| extent.start_ns < range.1 && extent.end_ns > range.0)
        .flat_map(|extent| &extent.slices)
        // Only immutable physical bounds may exclude a clipped slice.
        .filter(move |slice| index.inner.blocks[slice.block].overlaps(range))
}

struct ReadRequirements {
    rows: usize,
    slices: usize,
    bytes: usize,
}

#[cfg(test)]
thread_local! {
    static READ_PLAN_ALLOCATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn read_requirements(index: &LogIndex, range: (i64, i64), audit: bool) -> Result<ReadRequirements> {
    let overflow = || DataError::InvalidResponse("daily query allocation overflow".into());
    let mut rows = 0_usize;
    let mut slices = 0_usize;
    let mut scratch = 0_usize;
    for slice in selected_slices(index, range) {
        rows = rows
            .checked_add(usize::try_from(slice.rows).map_err(|_| overflow())?)
            .ok_or_else(overflow)?;
        slices = slices.checked_add(1).ok_or_else(overflow)?;
        scratch = scratch.max(index.inner.blocks[slice.block].read_allocation_bytes()?);
    }
    if audit {
        for block in &index.inner.blocks {
            scratch = scratch.max(block.read_allocation_bytes()?);
        }
    }
    let bytes = rows
        .checked_mul(std::mem::size_of::<Kline>())
        .and_then(|bytes| {
            bytes.checked_add(slices.checked_mul(std::mem::size_of::<&storage::Slice>())?)
        })
        .and_then(|bytes| bytes.checked_add(scratch))
        .and_then(|bytes| bytes.checked_add(index.allocation_bytes))
        .ok_or_else(overflow)?;
    Ok(ReadRequirements {
        rows,
        slices,
        bytes,
    })
}

fn read_rows(
    input: &mut File,
    index: &LogIndex,
    range: (i64, i64),
    limit: Option<usize>,
    audit: bool,
) -> Result<Vec<Kline>> {
    // Compute all sizes before allocating the selection or output vectors.
    let required = read_requirements(index, range, audit)?;
    let allocation_error = || DataError::CollectLimitExceeded {
        limit_bytes: limit.unwrap_or(usize::MAX),
        attempted_bytes: required.bytes,
    };
    if limit.is_some_and(|limit| required.bytes > limit) {
        return Err(allocation_error());
    }
    #[cfg(test)]
    READ_PLAN_ALLOCATIONS.with(|count| count.set(count.get() + 1));
    let mut selected = Vec::new();
    selected
        .try_reserve_exact(required.slices)
        .map_err(|_| allocation_error())?;
    selected.extend(selected_slices(index, range));
    selected.sort_unstable_by_key(|slice| (slice.block, slice.row_start));
    let mut output = Vec::new();
    output
        .try_reserve_exact(required.rows)
        .map_err(|_| allocation_error())?;
    let mut next_slice = 0;
    for id in 0..index.inner.blocks.len() {
        let count = selected[next_slice..].partition_point(|slice| slice.block == id);
        if count == 0 && !audit {
            continue;
        }
        let rows = storage::read_klines(input, &index.inner, id, limit)?;
        for slice in &selected[next_slice..next_slice + count] {
            let start = slice.row_start as usize;
            let end = start + slice.rows as usize;
            let clipped = rows
                .get(start..end)
                .ok_or_else(|| DataError::InvalidResponse("daily slice outside block".into()))?;
            if clipped.first().map(|row| row.datetime) != Some(slice.first_ns)
                || clipped.last().map(|row| row.datetime) != Some(slice.last_ns)
            {
                return Err(DataError::InvalidResponse(
                    "daily slice time index mismatch".into(),
                ));
            }
            output.extend(
                clipped
                    .iter()
                    .filter(|row| row.datetime >= range.0 && row.datetime < range.1)
                    .cloned(),
            );
        }
        next_slice += count;
    }
    output.sort_unstable_by_key(|row| row.datetime);
    if output
        .windows(2)
        .any(|pair| pair[0].datetime >= pair[1].datetime)
    {
        return Err(DataError::InvalidResponse(
            "duplicate/overlapping extents".into(),
        ));
    }
    Ok(output)
}

pub(super) fn load_full(path: &Path, limit: Option<usize>) -> Result<DailyFile> {
    let mut input = File::open(path)?;
    read_full(&mut input, limit)
}

pub(super) fn read_allocation_upper_bound(path: &Path) -> Result<usize> {
    let lock = File::open(path.with_extension(format!("{FILE_EXTENSION}.lock")))?;
    FileExt::lock_shared(&lock)?;
    let mut input = File::open(path)?;
    let index = require(&mut input, None)?;
    Ok(read_requirements(&index, (i64::MIN, i64::MAX), true)?.bytes)
}

fn read_full(input: &mut File, limit: Option<usize>) -> Result<DailyFile> {
    let index = require(input, limit)?;
    let rows = read_rows(input, &index, (i64::MIN, i64::MAX), limit, true)?;
    if rows.len() != index.summary.rows {
        return Err(DataError::InvalidResponse(
            "daily active index disagrees with rows".into(),
        ));
    }
    let file = DailyFile {
        symbol: index.summary.symbol,
        snapshot: index.summary.snapshot,
        coverage: index.summary.coverage,
        rows,
    };
    validate_file(&file)?;
    Ok(file)
}

pub(super) fn read_range(
    cache: &DailyKlineCache,
    symbol: &str,
    range: (i64, i64),
    snapshot: &DailyKlineCacheSnapshot,
    limit: Option<usize>,
) -> Result<Vec<Kline>> {
    validate_symbol(symbol)?;
    if range.0 >= range.1 {
        return Err(DataError::Validation(
            "daily range must have positive width".into(),
        ));
    }
    let path = cache.symbol_file_path(symbol);
    let lock = File::open(path.with_extension(format!("{FILE_EXTENSION}.lock")))?;
    FileExt::lock_shared(&lock)?;
    let mut input = File::open(&path)?;
    let index = require(&mut input, limit)?;
    // No path reopen or header reread after releasing the shared lock.
    drop(lock);
    validate_daily_summary(&cache.root_dir, symbol, snapshot, &index.summary)?;
    if !index
        .summary
        .coverage
        .iter()
        .any(|&(start, end)| start <= range.0 && end >= range.1)
    {
        return Err(DataError::InvalidState(
            "daily kline cache coverage incomplete",
        ));
    }
    read_rows(&mut input, &index, range, limit, false)
}

// Offline migration only. Runtime reads never fall back to the old envelope.
pub(super) fn load_legacy(path: &Path) -> Result<DailyFile> {
    let mut input = File::open(path)?;
    let Some(index) = kline_append_log::load::<DailySummary>(&mut input)? else {
        return decode_daily_file_bytes(&fs::read(path)?);
    };
    index.require_clean_tail(&input)?;
    let mut file = DailyFile {
        symbol: index.summary.symbol.clone(),
        snapshot: index.summary.snapshot.clone(),
        coverage: Vec::new(),
        rows: Vec::new(),
    };
    for segment in &index.segments {
        let decoded =
            decode_daily_file_bytes(&kline_append_log::read_segment(&mut input, segment)?)?;
        if decoded.symbol != file.symbol || decoded.snapshot != file.snapshot {
            return Err(DataError::InvalidResponse(
                "legacy daily segment identity mismatch".into(),
            ));
        }
        file.coverage.extend(decoded.coverage);
        file.rows.extend(decoded.rows);
    }
    file.coverage = merge_ranges(file.coverage)?;
    if file.coverage != index.summary.coverage || file.rows.len() != index.summary.rows {
        return Err(DataError::InvalidResponse(
            "legacy daily summary disagrees with rows".into(),
        ));
    }
    validate_file(&file)?;
    Ok(file)
}
