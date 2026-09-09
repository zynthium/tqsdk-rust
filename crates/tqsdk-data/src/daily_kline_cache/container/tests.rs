use super::*;
use std::io::Write;

const SYMBOL: &str = "SHFE.au2402";
const START: i64 = 1_577_836_800_000_000_000;

struct Fixture(PathBuf);

impl Fixture {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "daily-container-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn cache(&self) -> DailyKlineCache {
        DailyKlineCache::open(self.0.join("cache")).unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn source(count: usize) -> DailyFile {
    DailyFile {
        symbol: SYMBOL.into(),
        snapshot: DailyKlineCacheSnapshot::cst_v1(),
        coverage: vec![(START, START + count.max(1) as i64 * DAILY_KLINE_DURATION_NS)],
        rows: (0..count)
            .map(|i| Kline {
                id: i as i64,
                datetime: START + i as i64 * DAILY_KLINE_DURATION_NS,
                close: f64::from_bits(0x7ff8_0000_0000_0007),
                open: -0.0,
                epoch: Some(i as i64),
                ..Kline::default()
            })
            .collect(),
    }
}

fn store(cache: &DailyKlineCache, source: &DailyFile) {
    cache
        .store_final_range(
            SYMBOL,
            source.coverage[0].0,
            source.coverage[0].1,
            &source.snapshot,
            &source.rows,
        )
        .unwrap();
}

#[test]
fn range_skips_unrelated_corruption_but_doctor_audits_every_block() {
    let fixture = Fixture::new("range-audit");
    let cache = fixture.cache();
    let source = source(storage::TARGET_KLINE_ROWS + 10);
    store(&cache, &source);
    let path = cache.symbol_file_path(SYMBOL);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let index = require(&mut file, None).unwrap();
    assert_eq!(index.inner.blocks.len(), 2);
    // Corrupt only the second payload, leaving its committed index intact.
    let value = serde_json::to_value(&index.inner).unwrap();
    let offset = value["blocks"][1]["offset"].as_u64().unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0xff;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
    let rows = cache
        .read_range(
            SYMBOL,
            START,
            START + DAILY_KLINE_DURATION_NS,
            &source.snapshot,
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].close.to_bits(), source.rows[0].close.to_bits());
    let last = source.rows.last().unwrap().datetime;
    assert!(
        cache
            .read_range(
                SYMBOL,
                last,
                last + DAILY_KLINE_DURATION_NS,
                &source.snapshot
            )
            .is_err()
    );
    assert_eq!(
        cache.diagnose(SYMBOL).unwrap().status,
        DailyKlineCacheDiagnosticStatus::Corrupt
    );
}

#[test]
fn pinned_daily_view_survives_a_public_correction() {
    let fixture = Fixture::new("pinned-correction");
    let cache = fixture.cache();
    let source = source(3);
    store(&cache, &source);
    let mut input = File::open(cache.symbol_file_path(SYMBOL)).unwrap();
    let index = require(&mut input, None).unwrap();
    let mut corrected = source.rows[1].clone();
    corrected.close = 99.0;
    cache
        .store_final_range(
            SYMBOL,
            corrected.datetime,
            corrected.datetime + DAILY_KLINE_DURATION_NS,
            &source.snapshot,
            &[corrected],
        )
        .unwrap();
    let old = read_rows(&mut input, &index, source.coverage[0], None, false).unwrap();
    assert_eq!(old[1].close.to_bits(), source.rows[1].close.to_bits());
    let fresh = cache
        .read_range(
            SYMBOL,
            source.coverage[0].0,
            source.coverage[0].1,
            &source.snapshot,
        )
        .unwrap();
    assert_eq!(fresh[1].close, 99.0);
}

#[test]
fn legacy_daily_migration_preserves_bits_coverage_and_backup_and_is_idempotent() {
    for append_envelope in [false, true] {
        let fixture = Fixture::new("migration");
        let cache = fixture.cache();
        let source = source(3);
        let path = cache.symbol_file_path(SYMBOL);
        let bytes = encode_complete_daily_file(&source).unwrap();
        if append_envelope {
            let summary = DailySummary {
                symbol: source.symbol.clone(),
                snapshot: source.snapshot.clone(),
                coverage: source.coverage.clone(),
                rows: source.rows.len(),
            };
            crate::kline_append_log::create(&path, summary, &[&bytes]).unwrap();
        } else {
            fs::write(&path, &bytes).unwrap();
        }
        let _lock = SymbolFileLock::acquire(&path).unwrap();
        drop(_lock);
        let original = fs::read(&path).unwrap();
        assert!(
            cache
                .read_range(
                    SYMBOL,
                    source.coverage[0].0,
                    source.coverage[0].1,
                    &source.snapshot
                )
                .is_err()
        );
        let backup = fixture.0.join("backup");
        let mut report = crate::kline_cache_migration::KlineCacheMigrationReport::default();
        cache
            .migrate_append_files(&backup, false, &mut report)
            .unwrap();
        assert_eq!(report.legacy_files, 1);
        assert_eq!(fs::read(&path).unwrap(), original);
        cache
            .migrate_append_files(&backup, true, &mut report)
            .unwrap();
        assert_eq!(report.migrated_files, 1);
        assert_eq!(
            fs::read(backup.join(path.strip_prefix(cache.root_dir()).unwrap())).unwrap(),
            original
        );
        let migrated = fs::read(&path).unwrap();
        let verified = load_full(&path, None).unwrap();
        assert_eq!(encode_complete_daily_file(&verified).unwrap(), bytes);
        cache
            .migrate_append_files(&backup, true, &mut report)
            .unwrap();
        assert_eq!(report.migrated_files, 1);
        assert_eq!(fs::read(&path).unwrap(), migrated);
    }
}

#[test]
fn daily_rejects_valid_container_with_wrong_pack_identity() {
    let fixture = Fixture::new("identity");
    let cache = fixture.cache();
    let source = source(1);
    let path = cache.symbol_file_path(SYMBOL);
    let (mut index, blocks) = build(&source).unwrap();
    index.extents[0].logical_partition = "minute-month".into();
    storage::create(&path, index, &blocks).unwrap();
    assert!(load_full(&path, None).is_err());
    let (mut index, blocks) = build(&source).unwrap();
    index.identity.pack_range = Some(source.coverage[0]);
    storage::create(&path, index, &blocks).unwrap();
    assert!(load_full(&path, None).is_err());
}

#[test]
fn wrong_clipped_slice_time_cannot_hide_matching_physical_rows() {
    let fixture = Fixture::new("slice-time");
    let cache = fixture.cache();
    let source = source(3);
    store(&cache, &source);
    let (mut index, blocks) = build(&source).unwrap();
    let slice = &mut index.extents[0].slices[0];
    slice.rows = 1;
    slice.first_ns = source.rows[1].datetime;
    slice.last_ns = source.rows[1].datetime;
    storage::create(&cache.symbol_file_path(SYMBOL), index, &blocks).unwrap();
    let error = cache
        .read_range(
            SYMBOL,
            START,
            START + DAILY_KLINE_DURATION_NS,
            &source.snapshot,
        )
        .unwrap_err();
    assert!(error.to_string().contains("slice time index mismatch"));
    assert_eq!(
        cache.diagnose(SYMBOL).unwrap().status,
        DailyKlineCacheDiagnosticStatus::Corrupt
    );
}

#[test]
fn maximum_slice_plan_obeys_the_preallocation_budget() {
    let fixture = Fixture::new("maximum-slice-plan");
    let cache = fixture.cache();
    let count = 65_536;
    let rows: Vec<_> = (0..count)
        .map(|i| Kline {
            id: i as i64,
            datetime: START + i as i64,
            epoch: None,
            ..Kline::default()
        })
        .collect();
    let block = storage::encode_klines(&rows).unwrap();
    let range = (START, START + count as i64);
    let mut index = storage::Index::new(
        Identity {
            symbol: SYMBOL.into(),
            kind: KIND,
            partition_scheme: 1,
            pack_range: None,
            metadata_schema: 1,
        },
        vec![DailyKlineCacheSnapshot::cst_v1()],
    );
    index.extents.push(Extent {
        start_ns: range.0,
        end_ns: range.1,
        logical_partition: "daily".into(),
        metadata: 0,
        finality: Finality::Final,
        slices: rows
            .iter()
            .enumerate()
            .map(|(i, row)| storage::Slice {
                block: 0,
                row_start: i as u64,
                rows: 1,
                first_ns: row.datetime,
                last_ns: row.datetime,
            })
            .collect(),
    });
    let path = cache.symbol_file_path(SYMBOL);
    let _lock = SymbolFileLock::acquire(&path).unwrap();
    storage::create(&path, index, &[block]).unwrap();
    drop(_lock);
    let mut input = File::open(&path).unwrap();
    let index = require(&mut input, None).unwrap();
    let required = read_requirements(&index, range, false).unwrap();
    assert_eq!(required.slices, count);
    // Cover validator scratch and derived summary as well as the read plan.
    let common_limit = index.inner.allocation_bytes;
    assert!(matches!(
        storage::load::<DailyKlineCacheSnapshot>(&mut input, KIND, Some(common_limit - 1)),
        Err(DataError::CollectLimitExceeded { .. })
    ));
    storage::load::<DailyKlineCacheSnapshot>(&mut input, KIND, Some(common_limit)).unwrap();
    let summary_limit = index.allocation_bytes;
    assert!(matches!(
        require(&mut input, Some(summary_limit - 1)),
        Err(DataError::CollectLimitExceeded { .. })
    ));
    require(&mut input, Some(summary_limit)).unwrap();
    let before = READ_PLAN_ALLOCATIONS.with(|count| count.get());
    assert!(matches!(
        read_rows(&mut input, &index, range, Some(required.bytes - 1), false),
        Err(DataError::CollectLimitExceeded { .. })
    ));
    assert_eq!(READ_PLAN_ALLOCATIONS.with(|count| count.get()), before);
    let upper = cache.read_range_allocation_upper_bound(SYMBOL).unwrap();
    assert_eq!(upper, required.bytes);
    let decoded = read_rows(&mut input, &index, range, Some(upper), false).unwrap();
    assert!(crate::kline_codec::rows_equal(&rows, &decoded));
    assert_eq!(READ_PLAN_ALLOCATIONS.with(|count| count.get()), before + 1);
}

#[test]
fn scan_reservation_covers_decoded_rows_not_compressed_file_size() {
    let fixture = Fixture::new("scan-reservation");
    let cache = fixture.cache();
    let source = source(storage::TARGET_KLINE_ROWS + 10);
    store(&cache, &source);
    let upper = cache.read_range_allocation_upper_bound(SYMBOL).unwrap();
    #[cfg(feature = "tqbn-zstd")]
    assert!(upper > fs::metadata(cache.symbol_file_path(SYMBOL)).unwrap().len() as usize * 4);
    let rows = cache
        .read_range_bounded(
            SYMBOL,
            source.coverage[0].0,
            source.coverage[0].1,
            &source.snapshot,
            upper,
        )
        .unwrap();
    assert!(crate::kline_codec::rows_equal(&rows, &source.rows));
}

#[cfg(unix)]
#[test]
fn symlink_append_and_recovery_do_not_modify_external_target() {
    let fixture = Fixture::new("symlink-write");
    let cache = fixture.cache();
    let source = source(3);
    store(&cache, &source);
    let path = cache.symbol_file_path(SYMBOL);
    let external = fixture.0.join("outside-cache.tqdk");
    fs::rename(&path, &external).unwrap();
    std::os::unix::fs::symlink(&external, &path).unwrap();
    OpenOptions::new()
        .append(true)
        .open(&external)
        .unwrap()
        .write_all(b"unfinished")
        .unwrap();
    let original = fs::read(&external).unwrap();
    let end = source.coverage[0].1;
    assert!(
        cache
            .store_final_range(
                SYMBOL,
                end,
                end + DAILY_KLINE_DURATION_NS,
                &source.snapshot,
                &[]
            )
            .is_err()
    );
    assert_eq!(fs::read(&external).unwrap(), original);
    assert!(cache.prepare_append_index(SYMBOL, || false).is_err());
    assert_eq!(fs::read(&external).unwrap(), original);
}
