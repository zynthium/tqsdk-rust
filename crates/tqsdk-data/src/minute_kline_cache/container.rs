//! Canonical-minute adapter for the shared indexed history container.
//! Physical trading-month paths remain unchanged until the layout migration.

use super::*;
use crate::history_container::{
    self as storage, EncodedBlock, Extent, Finality, Identity, SeriesKind,
};
use std::io::{Seek, SeekFrom};

const KIND: SeriesKind = SeriesKind::Kline {
    duration_ns: MINUTE_KLINE_DURATION_NS,
};
#[cfg(test)]
mod tests;
impl storage::IndexMetadata for MonthMetadata {}

pub(super) struct LogIndex {
    inner: storage::Index<MonthMetadata>,
    pub summary: MonthSummary,
}

impl LogIndex {
    pub fn needs_compaction(&self) -> bool {
        self.inner.blocks.len() >= 64
            || self.inner.extents.len() >= 64
            || self.inner.retired_index_bytes() >= 256 * 1024
    }

    pub fn require_clean_tail(&self, input: &File) -> Result<()> {
        self.inner.require_clean_tail(input)
    }
}

pub(super) fn load(input: &mut File) -> Result<Option<LogIndex>> {
    input.seek(SeekFrom::Start(0))?;
    let mut magic = [0; 8];
    if input.read(&mut magic)? != magic.len() || &magic != storage::MAGIC {
        return Ok(None);
    }
    require(input).map(Some)
}

pub(super) fn require(input: &mut File) -> Result<LogIndex> {
    let inner = storage::load::<MonthMetadata>(input, KIND, None)?;
    if inner.identity.pack_range.is_some() || inner.metadata.len() != 1 {
        return Err(format_error(
            Path::new("<minute container>"),
            "invalid monthly identity",
        ));
    }
    let metadata = &inner.metadata[0];
    validate_symbol(&metadata.symbol)?;
    metadata.snapshot.validate()?;
    if inner.identity.symbol != metadata.symbol
        || !is_trading_month(&metadata.trading_month)
        || inner.extents.iter().any(|extent| {
            extent.metadata != 0
                || extent.finality != Finality::Final
                || extent.logical_partition != metadata.trading_month
        })
    {
        return Err(format_error(
            Path::new("<minute container>"),
            "metadata/finality mismatch",
        ));
    }
    let summary = MonthSummary {
        metadata: metadata.clone(),
        coverage: merge_ranges(
            inner
                .extents
                .iter()
                .map(|e| (e.start_ns, e.end_ns))
                .collect(),
        ),
        rows: usize::try_from(inner.active_rows()?)
            .map_err(|_| format_error(Path::new("<minute container>"), "row count overflow"))?,
    };
    summary.scan(Path::new("<minute container>"))?;
    Ok(LogIndex { inner, summary })
}

fn encode(month: &MonthFile, first_block: usize) -> Result<(Vec<EncodedBlock>, Vec<Extent>)> {
    let path = Path::new("<new minute container>");
    month.metadata.snapshot.validate()?;
    validate_symbol(&month.metadata.symbol)?;
    validate_stored_coverage(path, &month.metadata.trading_month, &month.coverage)?;
    validate_stored_rows(path, &month.metadata.trading_month, &month.rows)?;
    if month.rows.len() > MAX_ROWS_PER_MONTH
        || month.coverage.len() > MAX_COVERAGE_RECORDS
        || month.rows.iter().any(|row| {
            !month
                .coverage
                .iter()
                .any(|&(start, end)| start <= row.datetime && row.datetime < end)
        })
    {
        return Err(format_error(path, "rows exceed coverage or limits"));
    }
    let mut blocks = Vec::new();
    let mut extents = Vec::new();
    for &(start_ns, end_ns) in &month.coverage {
        let start = month.rows.partition_point(|row| row.datetime < start_ns);
        let end = month.rows.partition_point(|row| row.datetime < end_ns);
        let mut slices = Vec::new();
        for chunk in month.rows[start..end].chunks(storage::TARGET_KLINE_ROWS) {
            let block = storage::encode_klines(chunk)?;
            slices.push(block.slice(first_block + blocks.len()));
            blocks.push(block);
        }
        extents.push(Extent {
            start_ns,
            end_ns,
            logical_partition: month.metadata.trading_month.clone(),
            metadata: 0,
            finality: Finality::Final,
            slices,
        });
    }
    Ok((blocks, extents))
}

fn build(month: &MonthFile) -> Result<(storage::Index<MonthMetadata>, Vec<EncodedBlock>)> {
    let (blocks, extents) = encode(month, 0)?;
    let mut index = storage::Index::new(
        Identity {
            symbol: month.metadata.symbol.clone(),
            kind: KIND,
            partition_scheme: 1,
            pack_range: None,
            metadata_schema: 1,
        },
        vec![month.metadata.clone()],
    );
    index.extents = extents;
    Ok((index, blocks))
}

pub(super) fn create(path: &Path, month: &MonthFile) -> Result<()> {
    let (index, blocks) = build(month)?;
    storage::create(path, index, &blocks)
}

pub(super) fn append(
    path: &Path,
    input: &mut File,
    mut index: LogIndex,
    month: &MonthFile,
) -> Result<()> {
    if index.summary.metadata.symbol != month.metadata.symbol
        || index.summary.metadata.trading_month != month.metadata.trading_month
        || index.summary.metadata.snapshot != month.metadata.snapshot
    {
        return Err(format_error(path, "append metadata mismatch"));
    }
    let (blocks, extents) = encode(month, index.inner.blocks.len())?;
    index.inner.extents.extend(extents);
    storage::append(path, input, index.inner, &blocks)
}

pub(super) fn recover(path: &Path, input: &mut File, index: &LogIndex) -> Result<()> {
    storage::recover(path, input, &index.inner)
}

pub(super) fn migrate(path: &Path, month: &MonthFile) -> Result<()> {
    let (mut index, blocks) = build(month)?;
    let mut candidate = crate::cache_file::Candidate::create(path)?;
    storage::write_new(&mut candidate.file, &mut index, &blocks)?;
    #[cfg(test)]
    if MIGRATION_TRUNCATE_CANDIDATE.with(|flag| flag.replace(false)) {
        candidate.file.set_len(8)?;
    }
    let verified = read_full(candidate.file.try_clone()?, path)?;
    if verified.metadata.symbol != month.metadata.symbol
        || verified.metadata.trading_month != month.metadata.trading_month
        || verified.metadata.snapshot != month.metadata.snapshot
        || verified.coverage != month.coverage
        || !crate::kline_codec::rows_equal(&verified.rows, &month.rows)
    {
        return Err(format_error(path, "migration changed logical data"));
    }
    candidate.publish(path)
}

#[cfg(test)]
thread_local! {
    static MIGRATION_TRUNCATE_CANDIDATE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// The opened FD and index are retained together after the companion lock drops.
/// Only one decoded block is retained; unrelated time blocks are not decoded.
pub(super) struct RowReader {
    input: File,
    path: PathBuf,
    index: LogIndex,
    range: (i64, i64),
    extent: usize,
    slice: usize,
    rows: std::iter::Take<std::vec::IntoIter<Kline>>,
    last_datetime: Option<i64>,
}

impl RowReader {
    pub fn open(
        mut input: File,
        cache_dir: &Path,
        path: &Path,
        symbol: &str,
        trading_month: &str,
        snapshot: &MinuteKlineCacheSnapshot,
        range: (i64, i64),
    ) -> Result<Self> {
        let index = require(&mut input)?;
        validate_expected_metadata(
            cache_dir,
            path,
            &index.summary.metadata,
            symbol,
            trading_month,
            snapshot,
            &intersecting_ranges(&index.summary.coverage, range),
        )?;
        Ok(Self::with_index(input, path, index, range))
    }

    fn with_index(input: File, path: &Path, index: LogIndex, range: (i64, i64)) -> Self {
        Self {
            input,
            path: path.to_owned(),
            index,
            range,
            extent: 0,
            slice: 0,
            rows: Vec::new().into_iter().take(0),
            last_datetime: None,
        }
    }

    pub fn next_row(&mut self) -> Result<Option<Kline>> {
        loop {
            for row in self.rows.by_ref() {
                if row.datetime >= self.range.0 && row.datetime < self.range.1 {
                    if self
                        .last_datetime
                        .is_some_and(|previous| previous >= row.datetime)
                    {
                        return Err(format_error(
                            &self.path,
                            "rows not strictly datetime ordered",
                        ));
                    }
                    self.last_datetime = Some(row.datetime);
                    return Ok(Some(row));
                }
            }
            // Drop the exhausted allocation before decoding the next block.
            self.rows = Vec::new().into_iter().take(0);
            let Some(extent) = self.index.inner.extents.get(self.extent) else {
                return Ok(None);
            };
            if extent.end_ns <= self.range.0
                || extent.start_ns >= self.range.1
                || self.slice >= extent.slices.len()
            {
                self.extent += 1;
                self.slice = 0;
                continue;
            }
            let slice = &extent.slices[self.slice];
            self.slice += 1;
            // Physical bounds are authenticated against decoded data. Do not use
            // a partial slice's unverified boundary hints to skip its block.
            if !self.index.inner.blocks[slice.block].overlaps(self.range) {
                continue;
            }
            let rows = storage::read_klines(&mut self.input, &self.index.inner, slice.block, None)?;
            let start = usize::try_from(slice.row_start)
                .map_err(|_| format_error(&self.path, "slice overflow"))?;
            let count = usize::try_from(slice.rows)
                .map_err(|_| format_error(&self.path, "slice overflow"))?;
            let clipped = rows
                .get(start..start + count)
                .ok_or_else(|| format_error(&self.path, "slice bounds"))?;
            if clipped.first().map(|r| r.datetime) != Some(slice.first_ns)
                || clipped.last().map(|r| r.datetime) != Some(slice.last_ns)
            {
                return Err(format_error(&self.path, "slice timestamp mismatch"));
            }
            for row in clipped {
                validate_one_stored_row(
                    &self.path,
                    &self.index.summary.metadata.trading_month,
                    row,
                )?;
            }
            let mut rows = rows.into_iter();
            if start > 0 {
                let _ = rows.nth(start - 1);
            }
            self.rows = rows.take(count);
        }
    }
}

pub(super) fn read_full(mut input: File, path: &Path) -> Result<MonthFile> {
    let index = require(&mut input)?;
    read_full_index(input, path, index)
}

fn read_full_index(input: File, path: &Path, index: LogIndex) -> Result<MonthFile> {
    let expected_rows = index.summary.rows;
    let mut reader = RowReader::with_index(input, path, index, (i64::MIN, i64::MAX));
    let mut rows = Vec::new();
    rows.try_reserve_exact(expected_rows)
        .map_err(|_| format_error(path, "row allocation failed"))?;
    while let Some(row) = reader.next_row()? {
        rows.push(row);
    }
    if rows.len() != expected_rows {
        return Err(format_error(path, "active row count mismatch"));
    }
    let summary = reader.index.summary;
    validate_stored_rows(path, &summary.metadata.trading_month, &rows)?;
    Ok(MonthFile {
        metadata: summary.metadata,
        coverage: summary.coverage,
        rows,
    })
}

pub(super) fn scan(path: &Path) -> Result<MonthScan> {
    let mut input = File::open(path)?;
    let index = require(&mut input)?;
    index.require_clean_tail(&input)?;
    // Diagnosis also audits retired physical blocks, not only active extents.
    for block in 0..index.inner.blocks.len() {
        storage::read_klines(&mut input, &index.inner, block, None)?;
    }
    let month = read_full_index(input, path, index)?;
    MonthSummary::from_month(&month).scan(path)
}
