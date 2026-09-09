use super::*;

const SYMBOL: &str = "SHFE.test2601";
// Keep container-only tests on an open/future partition. Closed-month
// packing is exercised separately at the store layer.
const DAY: &str = "20990105";

fn write_batches_and_read_all(f: &Fixture, batches: &[Vec<Tick>]) -> Vec<Tick> {
    for batch in batches {
        f.write(batch);
    }
    f.all()
}

#[test]
fn increasing_time_reused_id_keeps_latest_snapshot() {
    let f = Fixture::new();
    let old = Tick {
        epoch: None,
        ..f.row(10)
    };
    let replay = Tick {
        datetime: old.datetime + 10_000_000,
        ..old.clone()
    };
    let actual = write_batches_and_read_all(&f, &[vec![old], vec![replay.clone()]]);
    assert_eq!(actual.len(), 1);
    assert_eq!(actual[0].datetime, replay.datetime);
}

#[test]
fn incoming_id_reset_applies_replay_witnesses_from_existing_partition() {
    let f = Fixture::new();
    let old = Tick {
        epoch: None,
        ..f.row(10)
    };
    let replay = Tick {
        id: 12,
        datetime: old.datetime + 500,
        ..old.clone()
    };
    let reset = Tick {
        id: 10,
        datetime: old.datetime + 1_000,
        last_price: 42.0,
        ..old.clone()
    };
    let actual = write_batches_and_read_all(&f, &[vec![old], vec![replay, reset]]);
    assert_eq!(actual.len(), 2);
}

#[test]
fn replay_chain_crosses_blocks_and_more_than_twenty_minutes() {
    let f = Fixture::new();
    let old = (0..9_000)
        .map(|id| Tick {
            id,
            datetime: f.start
                + if id < 8_993 {
                    id * 1_000
                } else {
                    (id - 8_992) * 300 * NANOS_PER_SECOND
                },
            epoch: None,
            last_price: id as f64,
            ..Tick::default()
        })
        .collect::<Vec<_>>();
    let corrected = old
        .iter()
        .enumerate()
        .skip(8_993)
        .map(|(i, row)| Tick {
            id: row.id + 10_000,
            last_price: if i < old.len() - 2 {
                row.last_price + 0.5
            } else {
                row.last_price
            },
            ..row.clone()
        })
        .collect::<Vec<_>>();
    let actual = write_batches_and_read_all(&f, &[old, corrected]);
    assert_eq!(actual.len(), 9_000);
    assert_eq!(actual[0].id, 0);
    assert_eq!(actual[8_993].id, 18_993);
}

#[test]
fn deep_inventory_identifies_common_schema_and_variable_width() {
    let f = Fixture::new();
    f.write(&[f.row(10)]);
    let report = f.store.scan().unwrap();
    let file = report
        .files
        .iter()
        .find(|file| file.path == f.path)
        .unwrap();
    assert_eq!(file.schema_version, Some(SCHEMA_VERSION));
    assert_eq!(file.row_width, None);
    assert_eq!(file.status, HistorySeriesCacheFileStatus::Readable);
}

#[test]
fn existing_session_id_reset_cannot_reenter_the_streaming_append_path() {
    let f = Fixture::new();
    let old = Tick {
        epoch: None,
        ..f.row(10)
    };
    let reset = Tick {
        id: 5,
        epoch: None,
        ..f.row(20)
    };
    let next = Tick {
        epoch: None,
        ..f.row(30)
    };
    let actual = write_batches_and_read_all(&f, &[vec![old, reset], vec![next]]);
    assert_eq!(actual.len(), 3);
    let mut file = File::open(&f.path).unwrap();
    let index = load(&mut file, &f.path, SYMBOL).unwrap();
    assert_eq!(
        index.blocks.len(),
        1,
        "nonmonotone history requires compaction"
    );
}

struct Fixture {
    root: PathBuf,
    store: TqbnHistoryStore,
    path: PathBuf,
    start: i64,
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "tick-adapter-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = TqbnHistoryStore::new(root.clone()).unwrap();
        let path = store.partition_series_path(DAY, SYMBOL, HistorySeriesKind::Tick);
        with_exclusive_tqbn_lock(&path, || update(&path, SYMBOL, &[], &[], None, false)).unwrap();
        let (_, start, _) =
            trading_day_range(NaiveDate::from_ymd_opt(2099, 1, 5).unwrap()).unwrap();
        Self {
            root,
            store,
            path,
            start,
        }
    }
    fn row(&self, second: i64) -> Tick {
        Tick {
            id: second,
            datetime: self.start + second * NANOS_PER_SECOND,
            last_price: second as f64,
            epoch: Some(i64::MIN),
            ..Tick::default()
        }
    }
    fn commit(&self, from: i64, to: i64) -> HistorySeriesCoverageCommit {
        HistorySeriesCoverageCommit {
            symbol: SYMBOL.into(),
            kind: HistorySeriesKind::Tick,
            range_start_ns: self.start + from * NANOS_PER_SECOND,
            range_end_ns: self.start + to * NANOS_PER_SECOND,
            rows: 0,
            id_range: None,
        }
    }
    fn request(&self, from: i64, to: i64) -> HistorySeriesCoverageRequest {
        let c = self.commit(from, to);
        HistorySeriesCoverageRequest {
            symbol: c.symbol,
            kind: c.kind,
            range_start_ns: c.range_start_ns,
            range_end_ns: c.range_end_ns,
        }
    }
    fn write(&self, rows: &[Tick]) {
        self.store
            .write_segment(HistorySeriesWriteSegment {
                symbol: SYMBOL,
                kind: HistorySeriesKind::Tick,
                declared_range_ns: None,
                rows: HistorySeriesWriteRows::Ticks(rows),
            })
            .unwrap();
    }
    fn reader(&self, from: i64, to: i64) -> Box<dyn HistorySeriesReader> {
        let r = self.request(from, to);
        self.store
            .open_reader(HistorySeriesReadRequest {
                symbol: r.symbol,
                kind: r.kind,
                range_start_ns: r.range_start_ns,
                range_end_ns: r.range_end_ns,
            })
            .unwrap()
    }
    fn all(&self) -> Vec<Tick> {
        let mut reader = self.reader(0, 3600);
        let mut rows = Vec::new();
        while let Some(HistorySeriesRow::Tick(row)) = reader.next_row().unwrap() {
            rows.push(row);
        }
        rows
    }
}

#[test]
fn new_tick_partitions_default_to_the_common_container() {
    let root = std::env::temp_dir().join(format!(
        "tick-common-default-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let store = TqbnHistoryStore::new(root.clone()).unwrap();
    let path = store.partition_series_path(DAY, SYMBOL, HistorySeriesKind::Tick);
    let (_, start, _) = trading_day_range(NaiveDate::from_ymd_opt(2099, 1, 5).unwrap()).unwrap();
    store
        .write_segment(HistorySeriesWriteSegment {
            symbol: SYMBOL,
            kind: HistorySeriesKind::Tick,
            declared_range_ns: None,
            rows: HistorySeriesWriteRows::Ticks(&[Tick {
                id: 1,
                datetime: start + NANOS_PER_SECOND,
                ..Tick::default()
            }]),
        })
        .unwrap();
    assert!(matches(&path).unwrap());
    assert_eq!(
        store.scan().unwrap().files[0].schema_version,
        Some(SCHEMA_VERSION)
    );
    fs::remove_dir_all(root).unwrap();
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
fn durable_rows_do_not_create_coverage_and_final_can_split_a_block() {
    let f = Fixture::new();
    f.write(&[f.row(10), f.row(20), f.row(30)]);
    assert_eq!(f.all().len(), 3);
    assert!(!f.store.coverage(f.request(0, 40)).unwrap().is_complete());
    f.store.append_coverage(f.commit(15, 25)).unwrap();
    assert!(f.store.coverage(f.request(15, 25)).unwrap().is_complete());
    assert!(!f.store.coverage(f.request(0, 40)).unwrap().is_complete());
    assert_eq!(
        f.all().iter().map(|row| row.id).collect::<Vec<_>>(),
        [10, 20, 30]
    );
    assert!(f.all().iter().all(|row| row.epoch == Some(i64::MIN)));
    f.store.append_coverage(f.commit(0, 40)).unwrap();
    assert!(f.store.coverage(f.request(0, 40)).unwrap().is_complete());
    assert_eq!(scan(&f.path, SYMBOL).unwrap().rows.len(), 3);
}

#[test]
fn provisional_keeps_checkpoint_stats_but_cannot_downgrade_final() {
    let f = Fixture::new();
    f.write(&[f.row(10), f.row(20)]);
    let request = f.request(0, 40);
    let checkpoint = HistorySeriesProvisionalCoverage {
        symbol: SYMBOL.into(),
        kind: HistorySeriesKind::Tick,
        range_start_ns: request.range_start_ns,
        complete_through_ns: f.row(30).datetime,
        as_of_ns: f.row(40).datetime,
        rows: 2,
        id_range: Some((10, 21)),
    };
    f.store.append_provisional(checkpoint.clone()).unwrap();
    assert_eq!(
        f.store.provisional_coverage(request.clone()).unwrap(),
        Some(checkpoint.clone())
    );
    assert!(!f.store.coverage(request.clone()).unwrap().is_complete());
    f.store.append_coverage(f.commit(0, 15)).unwrap();
    f.store.append_provisional(checkpoint).unwrap();
    assert!(f.store.coverage(f.request(0, 15)).unwrap().is_complete());
    f.store.append_coverage(f.commit(0, 40)).unwrap();
    assert!(
        f.store
            .provisional_coverage(request.clone())
            .unwrap()
            .is_none()
    );
    assert!(f.store.coverage(request).unwrap().is_complete());
}

#[test]
fn sequential_fill_appends_and_does_not_decode_unrelated_payload() {
    let f = Fixture::new();
    f.write(&[f.row(10)]);
    let mut file = File::open(&f.path).unwrap();
    let old = load(&mut file, &f.path, SYMBOL).unwrap();
    // Corrupt the old payload after recording its trusted index. Newer rows
    // must not decode that block on the normal append path.
    let wire = serde_json::to_value(&old).unwrap();
    let offset = wire["blocks"][0]["offset"].as_u64().unwrap();
    let mut writer = OpenOptions::new().write(true).open(&f.path).unwrap();
    writer.seek(SeekFrom::Start(offset)).unwrap();
    writer.write_all(b"bad!").unwrap();
    writer.sync_all().unwrap();
    f.write(&[f.row(100)]);
    let new = load(&mut File::open(&f.path).unwrap(), &f.path, SYMBOL).unwrap();
    assert!(new.generation > old.generation);
    assert_eq!(new.blocks.len(), 2);
    let mut reader = f.reader(90, 110);
    assert!(
        matches!(reader.next_row().unwrap(), Some(HistorySeriesRow::Tick(row)) if row.id == 100)
    );
    assert!(reader.next_row().unwrap().is_none());
    assert_eq!(reader.read_telemetry().blocks_decoded, 1);
    assert!(scan(&f.path, SYMBOL).is_err());
    let before_compaction = fs::read(&f.path).unwrap();
    assert!(
        f.store
            .compact_series(SYMBOL, HistorySeriesKind::Tick)
            .is_err()
    );
    assert_eq!(fs::read(&f.path).unwrap(), before_compaction);
}

#[test]
fn overlap_replacement_preserves_coverage_and_an_open_reader() {
    let f = Fixture::new();
    f.write(&[f.row(10), f.row(20), f.row(30)]);
    f.store.append_coverage(f.commit(0, 40)).unwrap();
    let mut old = f.reader(0, 40);
    assert!(old.next_row().unwrap().is_some());
    let mut corrected = f.row(20);
    corrected.last_price = -0.0;
    f.write(&[corrected]);
    let rows = f.all();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[1].last_price.to_bits(), (-0.0_f64).to_bits());
    assert!(f.store.coverage(f.request(0, 40)).unwrap().is_complete());
    assert!(
        matches!(old.next_row().unwrap(), Some(HistorySeriesRow::Tick(row)) if row.last_price == 20.0)
    );
    f.store
        .compact_series(SYMBOL, HistorySeriesKind::Tick)
        .unwrap();
    assert_eq!(f.all().len(), 3);
}

#[test]
fn store_combined_commit_empty_proof_and_crash_tail_resume() {
    let f = Fixture::new();
    let rows = [f.row(10), f.row(20)];
    f.store
        .write_segment_with_coverage(
            HistorySeriesWriteSegment {
                symbol: SYMBOL,
                kind: HistorySeriesKind::Tick,
                declared_range_ns: None,
                rows: HistorySeriesWriteRows::Ticks(&rows),
            },
            &[f.commit(0, 30)],
        )
        .unwrap();
    assert!(f.store.coverage(f.request(0, 30)).unwrap().is_complete());
    f.store.append_coverage(f.commit(30, 100)).unwrap();
    assert!(f.store.coverage(f.request(0, 100)).unwrap().is_complete());
    let mut writer = OpenOptions::new().append(true).open(&f.path).unwrap();
    writer.write_all(b"uncommitted tail").unwrap();
    writer.sync_all().unwrap();
    assert_eq!(f.all().len(), 2);
    assert!(scan(&f.path, SYMBOL).is_err());
    f.write(&[f.row(110)]);
    assert_eq!(scan(&f.path, SYMBOL).unwrap().rows.len(), 3);
    assert!(f.store.coverage(f.request(0, 100)).unwrap().is_complete());
}

#[test]
fn shared_snapshot_inode_is_unchanged_by_store_append() {
    let f = Fixture::new();
    f.write(&[f.row(10)]);
    let snapshot = f.root.join("snapshot.tqbn");
    fs::hard_link(&f.path, &snapshot).unwrap();
    let before = fs::read(&snapshot).unwrap();
    f.write(&[f.row(20)]);
    assert_eq!(fs::read(&snapshot).unwrap(), before);
    assert_ne!(fs::read(&f.path).unwrap(), before);
    assert_eq!(f.all().len(), 2);
}

#[test]
fn inventory_accepts_common_tick_magic_and_invalid_proofs_do_not_publish() {
    let f = Fixture::new();
    f.write(&[f.row(10)]);
    let inventory = crate::BacktestTickCache::open_read_only(&f.root)
        .fast_inventory()
        .unwrap();
    assert_eq!(inventory.problem_files, 0);
    let before = fs::read(&f.path).unwrap();
    let mut invalid = f.commit(0, 40);
    invalid.id_range = Some((5, 5));
    assert!(f.store.append_coverage(invalid).is_err());
    assert_eq!(fs::read(&f.path).unwrap(), before);
    let mut invalid = f.commit(0, 40);
    invalid.symbol = "WRONG.symbol".into();
    assert!(update(&f.path, SYMBOL, &[], &[invalid], None, false).is_err());
    assert_eq!(fs::read(&f.path).unwrap(), before);
    assert!(f.all().len() == 1);
}

#[test]
fn old_reader_keeps_its_second_block_after_atomic_replacement() {
    let f = Fixture::new();
    let count = storage::tick::MAX_ROWS + 2;
    let rows: Vec<_> = (0..count).map(|n| f.row(n as i64)).collect();
    f.write(&rows);
    let mut old = f.reader(0, count as i64);
    assert!(old.next_row().unwrap().is_some());
    let mut corrected = rows[count - 1].clone();
    corrected.last_price = -0.0;
    f.write(&[corrected]);
    let mut seen = 1;
    let mut last = None;
    while let Some(HistorySeriesRow::Tick(row)) = old.next_row().unwrap() {
        seen += 1;
        last = Some(row);
    }
    assert_eq!(seen, count);
    assert_eq!(last.unwrap().last_price, (count - 1) as f64);
    assert_eq!(old.read_telemetry().blocks_decoded, 2);
    let mut current = f.reader((count - 1) as i64, count as i64);
    assert!(
        matches!(current.next_row().unwrap(), Some(HistorySeriesRow::Tick(row))
        if row.last_price.to_bits() == (-0.0_f64).to_bits())
    );
}

#[test]
fn repeated_overlays_match_a_final_coverage_oracle_without_losing_rows() {
    let f = Fixture::new();
    let rows: Vec<_> = [1, 5, 20, 50, 80, 95]
        .into_iter()
        .map(|n| f.row(n))
        .collect();
    f.write(&rows);
    let mut known = [false; 100];
    for (from, to, final_proof) in [
        (3, 97, false),
        (20, 50, true),
        (0, 6, true),
        (11, 90, false),
        (91, 99, true),
        (6, 91, true),
        (0, 100, false),
        (99, 100, true),
    ] {
        let commit = f.commit(from as i64, to as i64);
        if final_proof {
            f.store.append_coverage(commit).unwrap();
            known[from..to].fill(true);
        } else {
            f.store
                .append_provisional(HistorySeriesProvisionalCoverage {
                    symbol: SYMBOL.into(),
                    kind: HistorySeriesKind::Tick,
                    range_start_ns: commit.range_start_ns,
                    complete_through_ns: commit.range_end_ns,
                    as_of_ns: f.row(100).datetime,
                    rows: 0,
                    id_range: None,
                })
                .unwrap();
        }
        let report = f.store.coverage(f.request(0, 100)).unwrap();
        for (second, expected) in known.iter().enumerate() {
            let time = f.row(second as i64).datetime;
            assert_eq!(
                report
                    .cached_ranges
                    .iter()
                    .any(|&(a, b)| a <= time && time < b),
                *expected
            );
        }
        assert_eq!(
            f.all().iter().map(|row| row.id).collect::<Vec<_>>(),
            [1, 5, 20, 50, 80, 95]
        );
    }
    f.store
        .compact_series(SYMBOL, HistorySeriesKind::Tick)
        .unwrap();
    assert!(f.store.coverage(f.request(0, 100)).unwrap().is_complete());
    assert_eq!(f.all().len(), rows.len());
}
