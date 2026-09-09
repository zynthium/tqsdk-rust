use super::*;
use std::fs::{self, OpenOptions};
use std::path::PathBuf;

const KIND: SeriesKind = SeriesKind::Kline { duration_ns: 1 };

pub(super) struct Fixture(PathBuf);

#[test]
fn unverified_extent_is_not_a_legal_kline_container() {
    let fixture = Fixture::new("unverified-kline");
    let mut value = index();
    let mut range = extent(0, 10, Vec::new());
    range.finality = Finality::Unverified;
    value.extents.push(range);
    assert!(create(&fixture.path(), value, &[]).is_err());
    assert!(!fixture.path().exists());
}
impl Fixture {
    pub(super) fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "history-container-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    pub(super) fn path(&self) -> PathBuf {
        self.0.join("data.tqbn")
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn row(time: i64) -> Kline {
    Kline {
        id: time,
        datetime: time,
        close: 1.5,
        ..Kline::default()
    }
}
fn index() -> Index<u8> {
    Index::new(
        Identity {
            symbol: "TEST.symbol".into(),
            kind: KIND,
            partition_scheme: 1,
            pack_range: Some((0, 1000)),
            metadata_schema: 1,
        },
        vec![7],
    )
}
fn extent(start_ns: i64, end_ns: i64, slices: Vec<Slice>) -> Extent {
    Extent {
        start_ns,
        end_ns,
        slices,
        logical_partition: "day-1".into(),
        metadata: 0,
        finality: Finality::Final,
    }
}
fn seed(path: &Path) {
    let block = encode_klines(&[row(10), row(20), row(30)]).unwrap();
    let mut index = index();
    index.extents.push(extent(0, 40, vec![block.slice(0)]));
    create(path, index, &[block]).unwrap();
}
fn writer(path: &Path) -> File {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap()
}
fn next(path: &Path, input: &mut File) {
    let mut index = load::<u8>(input, KIND, None).unwrap();
    let block = encode_klines(&[row(50)]).unwrap();
    index
        .extents
        .push(extent(40, 60, vec![block.slice(index.blocks.len())]));
    append(path, input, index, &[block]).unwrap();
}

#[test]
fn empty_coverage_is_an_index_only_commit() {
    let fixture = Fixture::new("empty");
    let mut index = index();
    index.extents.push(extent(0, 40, Vec::new()));
    create(&fixture.path(), index, &[]).unwrap();
    let mut input = File::open(fixture.path()).unwrap();
    let index = load::<u8>(&mut input, KIND, None).unwrap();
    assert!(index.blocks.is_empty());
    assert_eq!(index.active_rows().unwrap(), 0);
    assert_eq!(index.extents[0].end_ns, 40);
}

thread_local! {
    static METADATA_DECODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

struct CountedMetadata(u8);

impl<'de> serde::Deserialize<'de> for CountedMetadata {
    fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> std::result::Result<Self, D::Error> {
        let value = u8::deserialize(decoder)?;
        METADATA_DECODES.with(|count| count.set(count.get() + 1));
        Ok(Self(value))
    }
}

impl IndexMetadata for CountedMetadata {}

#[test]
fn maximum_extent_index_is_rejected_before_owned_metadata_deserialization() {
    let fixture = Fixture::new("maximum-index-budget");
    let mut index = index();
    index.identity.pack_range = Some((0, 2 * MAX_ENTRIES as i64));
    index.extents = (0..MAX_ENTRIES)
        .map(|i| extent(2 * i as i64, 2 * i as i64 + 1, vec![]))
        .collect();
    create(&fixture.path(), index, &[]).unwrap();
    let mut input = File::open(fixture.path()).unwrap();
    let index = load::<u8>(&mut input, KIND, None).unwrap();
    assert!(index.allocation_bytes > index.index_bytes * 4);
    let before = METADATA_DECODES.with(|count| count.get());
    assert!(matches!(
        load::<CountedMetadata>(&mut input, KIND, Some(index.allocation_bytes - 1)),
        Err(DataError::CollectLimitExceeded { .. })
    ));
    assert_eq!(METADATA_DECODES.with(|count| count.get()), before);
    let decoded = load::<CountedMetadata>(&mut input, KIND, Some(index.allocation_bytes)).unwrap();
    assert_eq!(decoded.extents.len(), MAX_ENTRIES);
    assert_eq!(decoded.metadata[0].0, 7);
    assert_eq!(METADATA_DECODES.with(|count| count.get()), before + 1);
}

#[test]
fn preflight_rejects_cardinality_overflow_and_invalid_json() {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "identity": index().identity,
        "metadata": vec![0_u8; MAX_ENTRIES + 1],
        "extents": [], "blocks": []
    }))
    .unwrap();
    assert!(allocation::decode::<u8>(&bytes, None).is_err());
    assert!(allocation::base::<u8>(usize::MAX).is_err());
    assert!(allocation::decode::<u8>(b"{broken", None).is_err());
    assert!(allocation::decode::<u8>(b"[] trailing", None).is_err());
}

#[test]
fn kline_blocks_must_have_strict_boundaries_but_ticks_may_share_a_timestamp() {
    let first = encode_klines(&[row(10), row(20)]).unwrap();
    let second = encode_klines(&[row(20), row(30)]).unwrap();
    let mut index = index();
    index
        .extents
        .push(extent(0, 40, vec![first.slice(0), second.slice(1)]));
    index.blocks = vec![first.block, second.block];
    let mut end = HEADER_BYTES;
    for block in &mut index.blocks {
        block.offset = end;
        end += block.len;
    }
    // Decode a structurally valid wire index before the semantic check, as load does.
    let wire = serde_json::to_vec(&index).unwrap();
    let decoded = allocation::decode::<u8>(&wire, None).unwrap();
    assert!(decoded.validate(end).is_err());
    index.identity.kind = SeriesKind::Tick;
    let ticks = |times: &[i64]| {
        times
            .iter()
            .map(|&datetime| Tick {
                datetime,
                ..Tick::default()
            })
            .collect::<Vec<_>>()
    };
    let first = tick::encode(&ticks(&[10, 20])).unwrap();
    let second = tick::encode(&ticks(&[20, 30])).unwrap();
    index.extents[0].slices = vec![first.slice(0), second.slice(1)];
    index.blocks = vec![first.block, second.block];
    let mut end = HEADER_BYTES;
    for block in &mut index.blocks {
        block.offset = end;
        end += block.len;
    }
    assert!(index.validate(end).is_ok());
}

#[test]
fn kline_codec_preserves_optional_epoch_extremes_and_float_bits() {
    let fixture = Fixture::new("lossless-fields");
    let rows: Vec<_> = [None, Some(i64::MIN), Some(i64::MAX), Some(0)]
        .into_iter()
        .enumerate()
        .map(|(i, epoch)| Kline {
            id: i64::MIN + i as i64,
            datetime: i as i64 + 1,
            open: -0.0,
            high: f64::INFINITY,
            low: f64::NEG_INFINITY,
            close: f64::from_bits(0x7ff8_0000_0000_1234),
            volume: i64::MAX,
            open_oi: i64::MIN,
            close_oi: -1,
            epoch,
        })
        .collect();
    let block = encode_klines(&rows).unwrap();
    let mut index = index();
    index.extents.push(extent(0, 10, vec![block.slice(0)]));
    create(&fixture.path(), index, &[block]).unwrap();
    let mut file = File::open(fixture.path()).unwrap();
    let index = load::<u8>(&mut file, KIND, None).unwrap();
    let decoded = read_klines(&mut file, &index, 0, None).unwrap();
    for (old, new) in rows.iter().zip(&decoded) {
        assert_eq!(old.epoch, new.epoch);
        let mut old_fields = Vec::new();
        let mut new_fields = Vec::new();
        kline_codec::encode_fields(&mut old_fields, old);
        kline_codec::encode_fields(&mut new_fields, new);
        assert_eq!(old_fields, new_fields);
    }
}

#[test]
fn row_allocation_budget_includes_rust_option_layout() {
    let fixture = Fixture::new("row-budget");
    seed(&fixture.path());
    let mut file = File::open(fixture.path()).unwrap();
    let index = load::<u8>(&mut file, KIND, None).unwrap();
    let block = &index.blocks[0];
    #[cfg(feature = "tqbn-zstd")]
    assert!(matches!(block.compression, Compression::Zstd));
    let required = block.len as usize
        + block.decoded_len as usize
        + block.rows as usize * std::mem::size_of::<Kline>()
        + block.decoder_workspace_bytes();
    assert_eq!(block.read_allocation_bytes().unwrap(), required);
    assert!(matches!(
        read_klines(&mut file, &index, 0, Some(required - 1)),
        Err(DataError::CollectLimitExceeded { .. })
    ));
    assert_eq!(
        read_klines(&mut file, &index, 0, Some(required))
            .unwrap()
            .len(),
        3
    );
}

#[test]
fn pinned_prefix_survives_append_and_atomic_replacement() {
    let fixture = Fixture::new("pin");
    let path = fixture.path();
    seed(&path);
    let mut old_file = File::open(&path).unwrap();
    let old_index = load::<u8>(&mut old_file, KIND, None).unwrap();
    next(&path, &mut writer(&path));
    assert_eq!(
        read_klines(&mut old_file, &old_index, 0, None)
            .unwrap()
            .len(),
        3
    );
    let mut replacement = index();
    let block = encode_klines(&[row(99)]).unwrap();
    replacement
        .extents
        .push(extent(90, 100, vec![block.slice(0)]));
    create(&path, replacement, &[block]).unwrap();
    let fresh = load::<u8>(&mut File::open(&path).unwrap(), KIND, None).unwrap();
    assert_eq!(fresh.active_rows().unwrap(), 1);
    assert_eq!(
        read_klines(&mut old_file, &old_index, 0, None).unwrap()[0].datetime,
        10
    );
}

#[test]
fn partial_correction_splits_logical_extents_not_physical_order() {
    let fixture = Fixture::new("correction");
    let path = fixture.path();
    seed(&path);
    let mut file = writer(&path);
    let mut index = load::<u8>(&mut file, KIND, None).unwrap();
    let mut corrected = row(20);
    corrected.id = 999;
    let block = encode_klines(&[corrected]).unwrap();
    index.extents = vec![
        extent(
            0,
            15,
            vec![Slice {
                block: 0,
                row_start: 0,
                rows: 1,
                first_ns: 10,
                last_ns: 10,
            }],
        ),
        extent(15, 25, vec![block.slice(1)]),
        extent(
            25,
            40,
            vec![Slice {
                block: 0,
                row_start: 2,
                rows: 1,
                first_ns: 30,
                last_ns: 30,
            }],
        ),
    ];
    append(&path, &mut file, index, &[block]).unwrap();
    let index = load::<u8>(&mut file, KIND, None).unwrap();
    assert_eq!(index.active_rows().unwrap(), 3);
    let mut ids = Vec::new();
    for extent in &index.extents {
        for slice in &extent.slices {
            let rows = read_klines(&mut file, &index, slice.block, None).unwrap();
            ids.extend(
                rows[slice.row_start as usize..(slice.row_start + slice.rows) as usize]
                    .iter()
                    .map(|r| r.id),
            );
        }
    }
    assert_eq!(ids, [10, 999, 30]);
}

#[test]
fn stale_writer_cannot_truncate_a_newer_commit() {
    let fixture = Fixture::new("stale");
    let path = fixture.path();
    seed(&path);
    let mut input = writer(&path);
    let old = load::<u8>(&mut input, KIND, None).unwrap();
    next(&path, &mut input);
    let bytes = fs::read(&path).unwrap();
    assert!(recover(&path, &mut input, &old).is_err());
    assert_eq!(fs::read(&path).unwrap(), bytes);
}

#[test]
fn stale_open_file_cannot_append_after_path_replacement() {
    let fixture = Fixture::new("replaced-writer");
    let path = fixture.path();
    seed(&path);
    let mut old_file = writer(&path);
    let old = load::<u8>(&mut old_file, KIND, None).unwrap();
    let original = fs::read(&path).unwrap();
    let block = encode_klines(&[row(99)]).unwrap();
    let mut replacement = index();
    replacement
        .extents
        .push(extent(90, 100, vec![block.slice(0)]));
    create(&path, replacement, &[block]).unwrap();
    let current = fs::read(&path).unwrap();
    assert!(recover(&path, &mut old_file, &old).is_err());
    assert_eq!(fs::read(&path).unwrap(), current);
    old_file.seek(SeekFrom::Start(0)).unwrap();
    let mut bytes = Vec::new();
    old_file.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, original);
}

#[test]
fn torn_slot_falls_back_but_valid_slot_with_bad_index_does_not() {
    let fixture = Fixture::new("slot");
    let path = fixture.path();
    seed(&path);
    let mut file = writer(&path);
    next(&path, &mut file);
    let latest = fs::read(&path).unwrap();
    file.seek(SeekFrom::Start(8)).unwrap();
    file.write_all(&[0; SLOT_BYTES]).unwrap();
    assert_eq!(load::<u8>(&mut file, KIND, None).unwrap().generation, 1);
    fs::write(&path, &latest).unwrap();
    let commit = read_commit(&mut file).unwrap();
    file.seek(SeekFrom::Start(commit[1])).unwrap();
    file.write_all(b"!").unwrap();
    assert!(load::<u8>(&mut file, KIND, None).is_err());
}

#[test]
fn unconfirmed_suffix_is_invisible_then_recovered() {
    let fixture = Fixture::new("suffix");
    let path = fixture.path();
    seed(&path);
    let mut file = writer(&path);
    let old = load::<u8>(&mut file, KIND, None).unwrap();
    file.seek(SeekFrom::End(0)).unwrap();
    file.write_all(b"partial block and partial index").unwrap();
    let pinned = load::<u8>(&mut file, KIND, None).unwrap();
    assert_eq!(pinned.committed_len, old.committed_len);
    assert!(pinned.require_clean_tail(&file).is_err());
    recover(&path, &mut file, &pinned).unwrap();
    assert_eq!(file.metadata().unwrap().len(), old.committed_len);
}

#[test]
fn invalid_bounds_rows_metadata_and_extents_are_rejected() {
    let fixture = Fixture::new("bounds");
    let path = fixture.path();
    seed(&path);
    let mut file = File::open(&path).unwrap();
    let index = load::<u8>(&mut file, KIND, None).unwrap();
    let offset = read_commit(&mut file).unwrap()[1];
    let mut bad = index.clone();
    bad.blocks[0].offset = u64::MAX;
    assert!(bad.validate(offset).is_err());
    let mut bad = index.clone();
    bad.blocks[0].decoded_len += 1;
    assert!(bad.validate(offset).is_err());
    let mut bad = index.clone();
    bad.extents[0].metadata = 1;
    assert!(bad.validate(offset).is_err());
    let mut bad = index.clone();
    bad.extents.push(index.extents[0].clone());
    assert!(bad.validate(offset).is_err());
    let mut bad = index.clone();
    bad.extents[0].slices[0].rows = u64::MAX;
    assert!(bad.validate(offset).is_err());
    assert!(load::<u8>(&mut file, SeriesKind::Tick, None).is_err());
    assert!(matches!(
        load::<u8>(&mut file, KIND, Some(1)),
        Err(DataError::CollectLimitExceeded { .. })
    ));
}

#[cfg(feature = "tqbn-zstd")]
#[test]
fn compressed_payload_cannot_exceed_its_declared_output_bound() {
    let fixture = Fixture::new("decompression-bound");
    let rows = (1..2000).map(row).collect::<Vec<_>>();
    let mut block = encode_klines(&rows).unwrap();
    assert_eq!(block.block.compression, Compression::Zstd);
    block.block.decoded_len = KLINE_BYTES as u64;
    block.block.rows = 1;
    block.block.last_ns = 1;
    let mut index = index();
    index.extents.push(extent(0, 2, vec![block.slice(0)]));
    create(&fixture.path(), index, &[block]).unwrap();
    let mut file = File::open(fixture.path()).unwrap();
    let index = load::<u8>(&mut file, KIND, None).unwrap();
    assert!(read_klines(&mut file, &index, 0, None).is_err());
}
