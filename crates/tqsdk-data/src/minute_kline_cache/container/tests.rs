use super::*;
use chrono::TimeZone;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "minute-container-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        ));
        fs::create_dir_all(&root).unwrap();
        Self(root)
    }
    fn path(&self) -> PathBuf {
        self.0.join("month.tqmk")
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn month(count: usize) -> MonthFile {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 2, 2, 0, 0)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap();
    MonthFile {
        metadata: MonthMetadata {
            symbol: "SHFE.au2406".into(),
            trading_month: "202401".into(),
            snapshot: MinuteKlineCacheSnapshot::cst_v1(),
        },
        coverage: vec![(
            start,
            start + count.max(1) as i64 * MINUTE_KLINE_DURATION_NS,
        )],
        rows: (0..count)
            .map(|i| Kline {
                id: i as i64,
                datetime: start + i as i64 * MINUTE_KLINE_DURATION_NS,
                close: f64::from_bits(0x7ff8_0000_0000_0011),
                open: -0.0,
                epoch: if i % 2 == 0 { Some(i64::MIN) } else { None },
                ..Kline::default()
            })
            .collect(),
    }
}

fn reader(path: &Path, month: &MonthFile, range: (i64, i64)) -> RowReader {
    RowReader::open(
        File::open(path).unwrap(),
        Path::new(""),
        path,
        &month.metadata.symbol,
        &month.metadata.trading_month,
        &month.metadata.snapshot,
        range,
    )
    .unwrap()
}

#[test]
fn short_query_skips_unrelated_corrupt_blocks_but_doctor_detects_them() {
    let fixture = Fixture::new();
    let month = month(storage::TARGET_KLINE_ROWS + 2);
    create(&fixture.path(), &month).unwrap();
    let mut input = File::open(fixture.path()).unwrap();
    let index = require(&mut input).unwrap();
    let wire = serde_json::to_value(&index.inner).unwrap();
    let offset = wire["blocks"][1]["offset"].as_u64().unwrap();
    let mut bytes = fs::read(fixture.path()).unwrap();
    bytes[offset as usize] ^= 0x80;
    fs::write(fixture.path(), bytes).unwrap();
    let start = month.coverage[0].0;
    let mut selected = reader(
        &fixture.path(),
        &month,
        (start, start + MINUTE_KLINE_DURATION_NS),
    );
    assert!(crate::kline_codec::rows_equal(
        &[selected.next_row().unwrap().unwrap()],
        &month.rows[..1]
    ));
    assert!(selected.next_row().unwrap().is_none());
    let hit = month.rows[storage::TARGET_KLINE_ROWS].datetime;
    let mut corrupted = reader(
        &fixture.path(),
        &month,
        (hit, hit + MINUTE_KLINE_DURATION_NS),
    );
    assert!(corrupted.next_row().is_err());
    assert!(scan(&fixture.path()).is_err());
}

#[test]
fn opened_reader_survives_append_and_atomic_replacement_losslessly() {
    let fixture = Fixture::new();
    let original = month(2);
    create(&fixture.path(), &original).unwrap();
    let mut opened = reader(&fixture.path(), &original, original.coverage[0]);
    let all = month(3);
    let delta = MonthFile {
        metadata: all.metadata.clone(),
        coverage: vec![(original.coverage[0].1, all.coverage[0].1)],
        rows: all.rows[2..].to_vec(),
    };
    let mut input = OpenOptions::new()
        .read(true)
        .write(true)
        .open(fixture.path())
        .unwrap();
    let index = require(&mut input).unwrap();
    append(&fixture.path(), &mut input, index, &delta).unwrap();
    assert!(crate::kline_codec::rows_equal(
        &read_full(File::open(fixture.path()).unwrap(), &fixture.path())
            .unwrap()
            .rows,
        &all.rows
    ));
    create(&fixture.path(), &month(1)).unwrap();
    let actual = [
        opened.next_row().unwrap().unwrap(),
        opened.next_row().unwrap().unwrap(),
    ];
    assert!(crate::kline_codec::rows_equal(&actual, &original.rows));
    assert!(opened.next_row().unwrap().is_none());
}

#[test]
fn empty_coverage_has_no_payload_and_invalid_identity_is_rejected() {
    let fixture = Fixture::new();
    let empty = month(0);
    create(&fixture.path(), &empty).unwrap();
    let mut input = File::open(fixture.path()).unwrap();
    let index = require(&mut input).unwrap();
    assert!(index.inner.blocks.is_empty());
    assert_eq!(scan(&fixture.path()).unwrap().coverage, empty.coverage);
    let (mut wrong, blocks) = build(&empty).unwrap();
    wrong.metadata[0].symbol = "OTHER.symbol".into();
    storage::create(&fixture.path(), wrong, &blocks).unwrap();
    assert!(require(&mut File::open(fixture.path()).unwrap()).is_err());
}

#[test]
fn legacy_klog_migration_preserves_rows_and_requires_explicit_entry() {
    let fixture = Fixture::new();
    let mut old = month(3);
    // Old v5 uses i64::MIN as the None sentinel; compare representable values.
    old.rows[0].epoch = Some(42);
    old.rows[2].epoch = None;
    let payload = encode_month_file(&old).unwrap();
    kline_append_log::create(&fixture.path(), MonthSummary::from_month(&old), &[&payload]).unwrap();
    assert!(require(&mut File::open(fixture.path()).unwrap()).is_err());
    let decoded = load_legacy_append_file(&fixture.path()).unwrap();
    assert!(crate::kline_codec::rows_equal(&decoded.rows, &old.rows));
    migrate(&fixture.path(), &decoded).unwrap();
    let actual = read_full(File::open(fixture.path()).unwrap(), &fixture.path()).unwrap();
    assert!(crate::kline_codec::rows_equal(&actual.rows, &old.rows));
    assert_eq!(actual.coverage, old.coverage);
    assert_eq!(actual.metadata.snapshot, old.metadata.snapshot);
}

#[test]
fn public_klog_migration_preserves_backup_and_is_idempotent() {
    let fixture = Fixture::new();
    let root = fixture.0.join("cache");
    let backup = fixture.0.join("backup");
    let _gate = crate::BacktestTickCache::open(&root).unwrap();
    drop(_gate.try_acquire_consistency_read_lock().unwrap());
    let cache = MinuteKlineCache::open(&root).unwrap();
    let mut original = month(2);
    original.rows[0].epoch = Some(i64::MAX);
    let path = cache.month_file_path(&original.metadata.symbol, &original.metadata.trading_month);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let payload = encode_month_file(&original).unwrap();
    kline_append_log::create(&path, MonthSummary::from_month(&original), &[&payload]).unwrap();
    let source = fs::read(&path).unwrap();
    assert_eq!(
        crate::migrate_kline_cache(&root, &backup, false)
            .unwrap()
            .legacy_files,
        1
    );
    assert!(!backup.exists());
    assert_eq!(
        crate::migrate_kline_cache(&root, &backup, true)
            .unwrap()
            .migrated_files,
        1
    );
    let target = backup.join(path.strip_prefix(&root).unwrap());
    assert_eq!(fs::read(&target).unwrap(), source);
    let current = fs::read(&path).unwrap();
    assert_eq!(&current[..8], storage::MAGIC);
    assert!(crate::kline_codec::rows_equal(
        &cache
            .read_range(
                &original.metadata.symbol,
                original.coverage[0].0,
                original.coverage[0].1,
                &original.metadata.snapshot
            )
            .unwrap(),
        &original.rows
    ));
    assert_eq!(
        crate::migrate_kline_cache(&root, &backup, true)
            .unwrap()
            .migrated_files,
        0
    );
    assert_eq!(fs::read(&path).unwrap(), current);
    assert_eq!(fs::read(&target).unwrap(), source);
}

#[test]
fn v4_candidate_failure_preserves_original_inode_and_backup() {
    let fixture = Fixture::new();
    let root = fixture.0.join("cache");
    let cache = MinuteKlineCache::open(&root).unwrap();
    let original = month(0);
    let path = cache.month_file_path(&original.metadata.symbol, &original.metadata.trading_month);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut payload = encode_month_file(&original).unwrap();
    payload[4..6].copy_from_slice(&4_u16.to_le_bytes());
    fs::write(&path, &payload).unwrap();
    #[cfg(unix)]
    let inode = {
        use std::os::unix::fs::MetadataExt;
        fs::metadata(&path).unwrap().ino()
    };
    MIGRATION_TRUNCATE_CANDIDATE.with(|flag| flag.set(true));
    assert!(cache.migrate_legacy_v4().is_err());
    assert_eq!(fs::read(&path).unwrap(), payload);
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(fs::metadata(&path).unwrap().ino(), inode);
    }
    let backup = root
        .join(".kline-append-backups/minute-v4-to-common")
        .join(path.strip_prefix(&root).unwrap());
    assert_eq!(fs::read(&backup).unwrap(), payload);
    assert!(fs::read_dir(path.parent().unwrap()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".cow-")
    }));
    assert_eq!(cache.migrate_legacy_v4().unwrap().rewritten_files, 1);
    assert_eq!(fs::read(&backup).unwrap(), payload);
}
