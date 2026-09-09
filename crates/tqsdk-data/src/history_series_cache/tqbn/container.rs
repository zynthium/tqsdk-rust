//! Tick adapter for common-container partitions during the explicit cutover.
//! Dispatch uses file magic, not the retained .tqbn suffix. Legacy files remain
//! on the legacy path until an offline migration has verified their candidate.

use super::*;
use crate::history_container::{
    self as storage, Extent, Finality, Identity, Index, SeriesKind, Slice,
};
use serde::{Deserialize, Serialize};

pub(super) const SCHEMA_VERSION: u32 = crate::BACKTEST_TICK_CACHE_SCHEMA_VERSION;

#[cfg(test)]
thread_local! {
    static MIGRATION_TRUNCATE_CANDIDATE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(super) fn truncate_next_migration_candidate() {
    MIGRATION_TRUNCATE_CANDIDATE.with(|flag| flag.set(true));
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Metadata {
    symbol: String,
    day: String,
    start_ns: i64,
    end_ns: i64,
    rows: usize,
    id_range: Option<(i64, i64)>,
}
impl storage::IndexMetadata for Metadata {}

pub(super) fn matches(path: &Path) -> Result<bool> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let mut magic = [0; 8];
    Ok(file.read(&mut magic)? == magic.len() && &magic == storage::MAGIC)
}

pub(super) fn snapshot_file_sha256_and_requires_zstd(path: &Path) -> Result<(String, bool, bool)> {
    let symbol = path
        .file_stem()
        .and_then(|value| value.to_str())
        .map(unescape_symbol_path_component)
        .ok_or(DataError::InvalidState(
            "Tick container path has no valid UTF-8 symbol",
        ))?;
    let mut file = File::open(path)?;
    let index = load(&mut file, path, symbol.as_str())?;
    let requires_zstd = index.uses_zstd();
    scan(path, symbol.as_str())?;

    file.seek(SeekFrom::Start(0))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok((
        format!("sha256:{:x}", hasher.finalize()),
        requires_zstd,
        true,
    ))
}

pub(super) fn write_segment(
    path: &Path,
    segment: &HistorySeriesWriteSegment<'_>,
    coverage: &[HistorySeriesCoverageCommit],
) -> Result<HistorySeriesSegmentReport> {
    let HistorySeriesWriteRows::Ticks(rows) = &segment.rows else {
        return Err(DataError::InvalidState("Tick container requires Tick rows"));
    };
    let (count, ids, range) = segment_rows_summary(segment)?;
    let mut commits = coverage.to_vec();
    if let Some((start, end)) = segment.declared_range_ns {
        commits.push(HistorySeriesCoverageCommit {
            symbol: segment.symbol.into(),
            kind: HistorySeriesKind::Tick,
            range_start_ns: start,
            range_end_ns: end,
            rows: count,
            id_range: ids,
        });
    }
    update(path, segment.symbol, rows, &commits, None, false)?;
    Ok(HistorySeriesSegmentReport {
        path: path.into(),
        symbol: segment.symbol.into(),
        kind: HistorySeriesKind::Tick,
        id_range: ids,
        range_start_ns: range.map(|r| r.0),
        range_end_ns: range.map(|r| r.1),
        rows: count,
    })
}

fn day(path: &Path) -> Result<String> {
    path.parent()
        .and_then(Path::parent)
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .filter(|day| is_partition_day(day))
        .map(str::to_owned)
        .ok_or(DataError::InvalidState("invalid Tick partition path"))
}

fn empty(path: &Path, symbol: &str) -> Result<Index<Metadata>> {
    let day = day(path)?;
    let date = NaiveDate::parse_from_str(&day, "%Y%m%d")
        .map_err(|_| DataError::InvalidState("invalid Tick partition day"))?;
    let (normalized, start, end) = trading_day_range(date)?;
    if normalized != date {
        return Err(DataError::InvalidState(
            "Tick partition is not a trading day",
        ));
    }
    Ok(Index::new(
        Identity {
            symbol: symbol.to_owned(),
            kind: SeriesKind::Tick,
            partition_scheme: 1,
            pack_range: Some((start, end)),
            metadata_schema: 1,
        },
        vec![Metadata {
            symbol: symbol.to_owned(),
            day,
            start_ns: 0,
            end_ns: 0,
            rows: 0,
            id_range: None,
        }],
    ))
}

pub(super) fn load(file: &mut File, path: &Path, symbol: &str) -> Result<Index<Metadata>> {
    let index = storage::load::<Metadata>(file, SeriesKind::Tick, None)?;
    validate_metadata(&index, path, symbol)?;
    Ok(index)
}

fn validate_metadata(index: &Index<Metadata>, path: &Path, symbol: &str) -> Result<()> {
    let expected = empty(path, symbol)?;
    if index.identity != expected.identity || index.metadata.is_empty() {
        return Err(DataError::InvalidResponse(
            "Tick container identity mismatch".into(),
        ));
    }
    for metadata in &index.metadata {
        if metadata.symbol != symbol
            || metadata.day != expected.metadata[0].day
            || metadata.id_range.is_some_and(|(start, end)| start > end)
            || (metadata.rows == 0 && metadata.id_range.is_some())
        {
            return Err(DataError::InvalidResponse(
                "Tick container metadata mismatch".into(),
            ));
        }
    }
    for extent in &index.extents {
        let metadata = index
            .metadata
            .get(extent.metadata)
            .ok_or(DataError::InvalidState("missing Tick metadata"))?;
        if extent.logical_partition != metadata.day {
            return Err(DataError::InvalidResponse(
                "Tick logical partition mismatch".into(),
            ));
        }
        if extent.finality != Finality::Unverified {
            let (start, end) = index.identity.pack_range.unwrap();
            if metadata.start_ns < start
                || metadata.end_ns > end
                || metadata.start_ns > extent.start_ns
                || metadata.end_ns < extent.end_ns
                || metadata.start_ns >= metadata.end_ns
                || matches!(extent.finality, Finality::Provisional { as_of_ns } if as_of_ns < metadata.end_ns)
            {
                return Err(DataError::InvalidResponse(
                    "Tick coverage proof mismatch".into(),
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn checkpoints(index: &Index<Metadata>) -> TqbnIndexedCoverage {
    let mut output = TqbnIndexedCoverage::default();
    for extent in &index.extents {
        match extent.finality {
            Finality::Unverified => {}
            Finality::Final => output.coverage.push((extent.start_ns, extent.end_ns)),
            Finality::Provisional { as_of_ns } => {
                let metadata = &index.metadata[extent.metadata];
                let checkpoint = TqbnProvisionalCoverage {
                    range_start_ns: metadata.start_ns,
                    complete_through_ns: metadata.end_ns,
                    as_of_ns,
                    rows: metadata.rows,
                    id_range: metadata.id_range,
                };
                if !output.provisional.contains(&checkpoint) {
                    output.provisional.push(checkpoint);
                }
            }
        }
    }
    output.coverage = super::super::merge_datetime_ranges(output.coverage);
    output
}

pub(super) struct Reader {
    file: File,
    index: Index<Metadata>,
    slices: Vec<Slice>,
    slice: usize,
    offset: usize,
    loaded: Option<usize>,
    rows: Vec<Tick>,
    range: TqbnReadRange,
    previous: Option<(i64, i64)>,
    telemetry: Arc<TqbnReadTelemetryState>,
}

impl Reader {
    pub(super) fn new(
        file: File,
        index: Index<Metadata>,
        range: TqbnReadRange,
        telemetry: Arc<TqbnReadTelemetryState>,
    ) -> Self {
        let slices = index
            .extents
            .iter()
            .filter(|extent| extent.start_ns < range.end_ns && extent.end_ns > range.start_ns)
            .flat_map(|extent| &extent.slices)
            .filter(|slice| index.blocks[slice.block].overlaps((range.start_ns, range.end_ns)))
            .cloned()
            .collect();
        Self {
            file,
            index,
            slices,
            slice: 0,
            offset: 0,
            loaded: None,
            rows: Vec::new(),
            range,
            previous: None,
            telemetry,
        }
    }

    pub(super) fn next(&mut self) -> Result<Option<HistorySeriesRow>> {
        while let Some(slice) = self.slices.get(self.slice) {
            if self.loaded != Some(slice.block) {
                self.rows = Vec::new();
                self.rows = storage::tick::read(&mut self.file, &self.index, slice.block, None)?;
                let (stored, decoded) = self.index.blocks[slice.block].payload_sizes();
                self.telemetry
                    .record_decoded_block(stored as usize, decoded as usize);
                self.loaded = Some(slice.block);
            }
            let begin = slice.row_start as usize;
            let end = begin + slice.rows as usize;
            if self.rows[begin].datetime != slice.first_ns
                || self.rows[end - 1].datetime != slice.last_ns
            {
                return Err(DataError::InvalidResponse(
                    "Tick slice disagrees with payload".into(),
                ));
            }
            if self.offset == slice.rows as usize {
                self.slice += 1;
                self.offset = 0;
                continue;
            }
            let row = &self.rows[begin + self.offset];
            self.offset += 1;
            if row.datetime < self.range.start_ns || row.datetime >= self.range.end_ns {
                continue;
            }
            let key = (row.datetime, row.id);
            if self.previous.is_some_and(|previous| previous >= key) {
                return Err(DataError::InvalidResponse(
                    "Tick active rows are not canonical".into(),
                ));
            }
            self.previous = Some(key);
            return Ok(Some(HistorySeriesRow::Tick(row.clone())));
        }
        Ok(None)
    }
}

fn read_all(file: &mut File, index: &Index<Metadata>) -> Result<Vec<Tick>> {
    let mut reader = Reader::new(
        file.try_clone()?,
        index.clone(),
        TqbnReadRange {
            start_ns: i64::MIN,
            end_ns: i64::MAX,
        },
        Arc::new(TqbnReadTelemetryState::default()),
    );
    let mut rows = Vec::new();
    while let Some(HistorySeriesRow::Tick(row)) = reader.next()? {
        rows.push(row);
    }
    Ok(rows)
}

fn canonical(rows: Vec<Tick>) -> Vec<Tick> {
    if rows
        .windows(2)
        .all(|p| p[0].datetime < p[1].datetime && p[0].id < p[1].id)
    {
        rows
    } else {
        canonicalize_tick_rows(rows)
    }
}

fn observable_checkpoints_equal(
    expected: &TqbnIndexedCoverage,
    actual: &TqbnIndexedCoverage,
    pack_range: (i64, i64),
) -> bool {
    if super::super::merge_datetime_ranges(expected.coverage.clone()) != actual.coverage {
        return false;
    }
    let mut starts = BTreeSet::from([pack_range.0]);
    for checkpoint in expected.provisional.iter().chain(&actual.provisional) {
        starts.insert(checkpoint.range_start_ns);
        if checkpoint.complete_through_ns > pack_range.0 {
            starts.insert(checkpoint.complete_through_ns - 1);
        }
    }
    starts
        .into_iter()
        .filter(|start| *start >= pack_range.0 && *start < pack_range.1)
        .all(|start| {
            select_provisional_checkpoint(
                expected.provisional.clone(),
                &expected.coverage,
                start,
                pack_range.1,
            ) == select_provisional_checkpoint(
                actual.provisional.clone(),
                &actual.coverage,
                start,
                pack_range.1,
            )
        })
}

struct Payloads<'a> {
    file: &'a mut File,
    index: &'a Index<Metadata>,
    extra: &'a [Tick],
}
impl Payloads<'_> {
    fn rows(&mut self, block: usize) -> Result<Vec<Tick>> {
        if block < self.index.blocks.len() {
            storage::tick::read(self.file, self.index, block, None)
        } else {
            let start = (block - self.index.blocks.len()) * storage::tick::MAX_ROWS;
            let end = (start + storage::tick::MAX_ROWS).min(self.extra.len());
            self.extra
                .get(start..end)
                .map(<[Tick]>::to_vec)
                .ok_or(DataError::InvalidState("missing pending Tick block"))
        }
    }
}

fn clip(extent: &Extent, start: i64, end: i64, payloads: &mut Payloads<'_>) -> Result<Extent> {
    let mut clipped = extent.clone();
    clipped.start_ns = start;
    clipped.end_ns = end;
    clipped.slices.clear();
    for slice in &extent.slices {
        if slice.last_ns < start || slice.first_ns >= end {
            continue;
        }
        if slice.first_ns >= start && slice.last_ns < end {
            clipped.slices.push(slice.clone());
            continue;
        }
        let rows = payloads.rows(slice.block)?;
        let from = slice.row_start as usize;
        let to = from + slice.rows as usize;
        let rows = rows
            .get(from..to)
            .ok_or(DataError::InvalidState("invalid Tick slice"))?;
        if rows.first().map(|row| row.datetime) != Some(slice.first_ns)
            || rows.last().map(|row| row.datetime) != Some(slice.last_ns)
        {
            return Err(DataError::InvalidResponse(
                "Tick split slice disagrees with payload".into(),
            ));
        }
        let first = rows.partition_point(|row| row.datetime < start);
        let last = rows.partition_point(|row| row.datetime < end);
        if first < last {
            clipped.slices.push(Slice {
                block: slice.block,
                row_start: (from + first) as u64,
                rows: (last - first) as u64,
                first_ns: rows[first].datetime,
                last_ns: rows[last - 1].datetime,
            });
        }
    }
    Ok(clipped)
}

fn overlay(index: &mut Index<Metadata>, proof: Extent, payloads: &mut Payloads<'_>) -> Result<()> {
    let mut output = Vec::new();
    let mut cursor = proof.start_ns;
    for extent in &index.extents {
        if extent.end_ns <= proof.start_ns || extent.start_ns >= proof.end_ns {
            output.push(extent.clone());
            continue;
        }
        let start = extent.start_ns.max(proof.start_ns);
        let end = extent.end_ns.min(proof.end_ns);
        if extent.start_ns < start {
            output.push(clip(extent, extent.start_ns, start, payloads)?);
        }
        if cursor < start {
            let mut gap = proof.clone();
            gap.start_ns = cursor;
            gap.end_ns = start;
            output.push(gap);
        }
        let mut middle = clip(extent, start, end, payloads)?;
        // A provisional checkpoint can never downgrade an existing final proof.
        if extent.finality != Finality::Final || proof.finality == Finality::Final {
            middle.finality = proof.finality;
            middle.metadata = proof.metadata;
        }
        output.push(middle);
        cursor = end;
        if end < extent.end_ns {
            output.push(clip(extent, end, extent.end_ns, payloads)?);
        }
    }
    if cursor < proof.end_ns {
        let mut gap = proof;
        gap.start_ns = cursor;
        output.push(gap);
    }
    output.sort_by_key(|extent| extent.start_ns);
    index.extents = output;
    Ok(())
}

fn add_blocks(index: &mut Index<Metadata>, rows: &[Tick]) -> Result<Vec<storage::EncodedBlock>> {
    let mut blocks = Vec::new();
    let mut slices = Vec::new();
    for chunk in rows.chunks(storage::tick::MAX_ROWS) {
        let block = storage::tick::encode(chunk)?;
        slices.push(block.slice(index.blocks.len() + blocks.len()));
        blocks.push(block);
    }
    if let (Some(first), Some(last)) = (rows.first(), rows.last()) {
        let end = last
            .datetime
            .checked_add(1)
            .ok_or(DataError::InvalidState("Tick time overflow"))?;
        index.extents.push(Extent {
            start_ns: first.datetime,
            end_ns: end,
            logical_partition: index.metadata[0].day.clone(),
            metadata: 0,
            finality: Finality::Unverified,
            slices,
        });
    }
    Ok(blocks)
}

pub(super) fn update(
    path: &Path,
    symbol: &str,
    incoming: &[Tick],
    coverage: &[HistorySeriesCoverageCommit],
    provisional: Option<&HistorySeriesProvisionalCoverage>,
    force_compact: bool,
) -> Result<()> {
    if coverage
        .iter()
        .any(|commit| commit.symbol != symbol || commit.kind != HistorySeriesKind::Tick)
        || provisional
            .is_some_and(|commit| commit.symbol != symbol || commit.kind != HistorySeriesKind::Tick)
    {
        return Err(DataError::InvalidState("Tick proof series mismatch"));
    }
    if let Some(commit) = provisional {
        validate_provisional_coverage(commit)?;
    }
    let exists = path.exists();
    let candidate = if exists {
        None
    } else {
        Some(crate::cache_file::Candidate::create(path)?)
    };
    let mut file = if exists {
        OpenOptions::new().read(true).write(true).open(path)?
    } else {
        candidate.as_ref().unwrap().file.try_clone()?
    };
    let original = if exists {
        load(&mut file, path, symbol)?
    } else {
        empty(path, symbol)?
    };
    if exists {
        storage::recover(path, &mut file, &original)?;
    }
    let incoming_monotone = incoming
        .windows(2)
        .all(|pair| pair[0].datetime < pair[1].datetime && pair[0].id < pair[1].id);
    let mut rows = incoming.to_vec();
    let (start, end) = original.identity.pack_range.unwrap();
    if rows
        .iter()
        .any(|row| row.datetime < start || row.datetime >= end)
    {
        return Err(DataError::InvalidState(
            "Tick row outside physical partition",
        ));
    }
    let overlap = rows.first().is_some_and(|first| {
        original
            .extents
            .last()
            .is_some_and(|e| first.datetime < e.end_ns)
    });
    // Match the legacy reader's streaming/spill boundary. Once IDs regress,
    // replay witnesses can propagate across the entire partition, so a fixed
    // tail window cannot preserve the historical canonicalization contract.
    let reused_ids = incoming.first().is_some_and(|first| {
        original
            .blocks
            .iter()
            .filter_map(|block| block.id_bounds())
            .any(|(_, maximum)| first.id <= maximum)
    });
    let compact = exists
        && (force_compact
            || overlap
            || reused_ids
            || !incoming_monotone
            || original.blocks.iter().enumerate().any(|(position, block)| {
                !block.tick_order_follows(position.checked_sub(1).map(|i| &original.blocks[i]))
            })
            || original.blocks.len() >= 64
            || original.extents.len() >= 64
            || original.retired_index_bytes() > 256 * 1024);
    if !compact {
        rows = canonical(rows);
    }
    let mut index = original.clone();
    let blocks;
    if compact {
        let mut all = read_all(&mut file, &original)?;
        all.extend(rows);
        rows = canonical(all);
        index = empty(path, symbol)?;
        index.metadata = original.metadata.clone();
        blocks = add_blocks(&mut index, &rows)?;
        let fresh = empty(path, symbol)?;
        let mut payloads = Payloads {
            file: &mut file,
            index: &fresh,
            extra: &rows,
        };
        for old in &original.extents {
            if old.finality != Finality::Unverified {
                let mut proof = old.clone();
                proof.slices.clear();
                overlay(&mut index, proof, &mut payloads)?;
            }
        }
    } else {
        blocks = add_blocks(&mut index, &rows)?;
    }
    let fresh = empty(path, symbol)?;
    let base = if compact { &fresh } else { &original };
    let mut payloads = Payloads {
        file: &mut file,
        index: base,
        extra: &rows,
    };
    for (from, to, count, ids, finality) in coverage
        .iter()
        .map(|commit| {
            (
                commit.range_start_ns,
                commit.range_end_ns,
                commit.rows,
                commit.id_range,
                Finality::Final,
            )
        })
        .chain(provisional.map(|commit| {
            (
                commit.range_start_ns,
                commit.complete_through_ns,
                commit.rows,
                commit.id_range,
                Finality::Provisional {
                    as_of_ns: commit.as_of_ns,
                },
            )
        }))
    {
        if from < start || to > end || from >= to {
            return Err(DataError::InvalidState(
                "Tick proof outside physical partition",
            ));
        }
        let metadata = index.metadata.len();
        index.metadata.push(Metadata {
            symbol: symbol.into(),
            day: index.metadata[0].day.clone(),
            start_ns: from,
            end_ns: to,
            rows: count,
            id_range: ids,
        });
        let proof = Extent {
            start_ns: from,
            end_ns: to,
            logical_partition: index.metadata[0].day.clone(),
            metadata,
            finality,
            slices: Vec::new(),
        };
        overlay(&mut index, proof, &mut payloads)?;
    }
    let mut remap = vec![usize::MAX; index.metadata.len()];
    remap[0] = 0;
    let mut metadata = vec![index.metadata[0].clone()];
    for extent in &mut index.extents {
        if remap[extent.metadata] == usize::MAX {
            remap[extent.metadata] = metadata.len();
            metadata.push(index.metadata[extent.metadata].clone());
        }
        extent.metadata = remap[extent.metadata];
    }
    index.metadata = metadata;
    validate_metadata(&index, path, symbol)?;
    if !exists {
        storage::write_new(&mut file, &mut index, &blocks)?;
        drop(file);
        candidate.unwrap().publish(path)
    } else if compact {
        storage::create(path, index, &blocks)
    } else {
        storage::append(path, &mut file, index, &blocks)
    }
}

pub(super) fn migrate(path: &Path, symbol: &str, state: &TqbnSeriesState) -> Result<()> {
    if matches(path)? {
        scan(path, symbol)?;
        return Ok(());
    }
    let rows = canonical(
        state
            .rows
            .iter()
            .map(|row| match row {
                HistorySeriesRow::Tick(row) => Ok(row.clone()),
                HistorySeriesRow::Kline(_) => Err(DataError::InvalidState(
                    "legacy Tick migration received Kline rows",
                )),
            })
            .collect::<Result<Vec<_>>>()?,
    );
    let mut index = empty(path, symbol)?;
    let blocks = add_blocks(&mut index, &rows)?;
    let base = empty(path, symbol)?;
    let mut candidate = crate::cache_file::Candidate::create(path)?;
    {
        let mut payloads = Payloads {
            file: &mut candidate.file,
            index: &base,
            extra: &rows,
        };
        for &(from, to) in &state.coverage {
            let first = rows.partition_point(|row| row.datetime < from);
            let last = rows.partition_point(|row| row.datetime < to);
            let metadata = index.metadata.len();
            index.metadata.push(Metadata {
                symbol: symbol.into(),
                day: index.metadata[0].day.clone(),
                start_ns: from,
                end_ns: to,
                rows: last - first,
                id_range: id_range_for_ticks(&rows[first..last])?,
            });
            overlay(
                &mut index,
                Extent {
                    start_ns: from,
                    end_ns: to,
                    logical_partition: base.metadata[0].day.clone(),
                    metadata,
                    finality: Finality::Final,
                    slices: Vec::new(),
                },
                &mut payloads,
            )?;
        }
        for checkpoint in &state.provisional {
            let metadata = index.metadata.len();
            index.metadata.push(Metadata {
                symbol: symbol.into(),
                day: index.metadata[0].day.clone(),
                start_ns: checkpoint.range_start_ns,
                end_ns: checkpoint.complete_through_ns,
                rows: checkpoint.rows,
                id_range: checkpoint.id_range,
            });
            overlay(
                &mut index,
                Extent {
                    start_ns: checkpoint.range_start_ns,
                    end_ns: checkpoint.complete_through_ns,
                    logical_partition: base.metadata[0].day.clone(),
                    metadata,
                    finality: Finality::Provisional {
                        as_of_ns: checkpoint.as_of_ns,
                    },
                    slices: Vec::new(),
                },
                &mut payloads,
            )?;
        }
    }
    let mut remap = vec![usize::MAX; index.metadata.len()];
    remap[0] = 0;
    let mut metadata = vec![index.metadata[0].clone()];
    for extent in &mut index.extents {
        if remap[extent.metadata] == usize::MAX {
            remap[extent.metadata] = metadata.len();
            metadata.push(index.metadata[extent.metadata].clone());
        }
        extent.metadata = remap[extent.metadata];
    }
    index.metadata = metadata;
    validate_metadata(&index, path, symbol)?;
    storage::write_new(&mut candidate.file, &mut index, &blocks)?;

    #[cfg(test)]
    if MIGRATION_TRUNCATE_CANDIDATE.with(|flag| flag.replace(false)) {
        candidate.file.set_len(8)?;
    }

    let verified_index = load(&mut candidate.file, path, symbol)?;
    verified_index.require_clean_tail(&candidate.file)?;
    let verified_rows = read_all(&mut candidate.file, &verified_index)?;
    let verified = checkpoints(&verified_index);
    if verified_rows.len() != rows.len()
        || verified_rows
            .iter()
            .zip(&rows)
            .any(|(left, right)| tick_to_spill_bytes(left) != tick_to_spill_bytes(right))
        || !observable_checkpoints_equal(
            &TqbnIndexedCoverage {
                coverage: state.coverage.clone(),
                provisional: state.provisional.clone(),
            },
            &verified,
            verified_index.identity.pack_range.unwrap(),
        )
    {
        return Err(DataError::InvalidResponse(
            "Tick migration candidate changed rows or coverage".into(),
        ));
    }
    candidate.publish(path)
}

pub(super) fn scan(path: &Path, symbol: &str) -> Result<TqbnSeriesState> {
    let mut file = File::open(path)?;
    let index = load(&mut file, path, symbol)?;
    index.require_clean_tail(&file)?;
    for block in 0..index.blocks.len() {
        storage::tick::read(&mut file, &index, block, None)?;
    }
    let checkpoints = checkpoints(&index);
    Ok(TqbnSeriesState {
        rows: read_all(&mut file, &index)?
            .into_iter()
            .map(HistorySeriesRow::Tick)
            .collect(),
        coverage: checkpoints.coverage,
        provisional: checkpoints.provisional,
    })
}

#[cfg(test)]
mod tests;
