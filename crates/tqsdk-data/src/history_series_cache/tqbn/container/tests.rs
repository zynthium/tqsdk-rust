use super::*;

const SYMBOL: &str = "SHFE.test2601";
const DAY: &str = "20260105";

fn assert_legacy_equivalent(f: &Fixture, batches: &[Vec<Tick>]) -> Vec<Tick> {
    let legacy = TqbnHistoryStore::new(f.root.join("legacy")).unwrap();
    let legacy_path = legacy.partition_series_path(DAY, SYMBOL, HistorySeriesKind::Tick);
    ensure_parent_dir(&legacy_path).unwrap();
    for batch in batches {
        f.write(batch);
        with_exclusive_tqbn_lock(&legacy_path, || {
            append_legacy_segment_to_file(
                &legacy_path,
                &HistorySeriesWriteSegment {
                    symbol: SYMBOL,
                    kind: HistorySeriesKind::Tick,
                    declared_range_ns: None,
                    rows: HistorySeriesWriteRows::Ticks(batch),
                },
            )
        })
        .unwrap();
    }
    let request = f.request(0, 3600);
    let mut reader = legacy
        .open_reader(HistorySeriesReadRequest {
            symbol: request.symbol,
            kind: request.kind,
            range_start_ns: request.range_start_ns,
            range_end_ns: request.range_end_ns,
        })
        .unwrap();
    let mut expected = Vec::new();
    while let Some(HistorySeriesRow::Tick(row)) = reader.next_row().unwrap() {
        expected.push(row);
    }
    let actual = f.all();
    assert_eq!(
        encode_fixed_tick_records(&actual, true).unwrap(),
        encode_fixed_tick_records(&expected, true).unwrap()
    );
    assert_eq!(
        actual.iter().map(|row| row.epoch).collect::<Vec<_>>(),
        expected.iter().map(|row| row.epoch).collect::<Vec<_>>()
    );
    actual
}

#[test]
fn increasing_time_reused_id_matches_legacy_global_replay_rules() {
    let f = Fixture::new();
    let old = Tick {
        epoch: None,
        ..f.row(10)
    };
    let replay = Tick {
        datetime: old.datetime + 10_000_000,
        ..old.clone()
    };
    let actual = assert_legacy_equivalent(&f, &[vec![old], vec![replay.clone()]]);
    assert_eq!(actual.len(), 1);
    assert_eq!(actual[0].datetime, replay.datetime);
}

// Migration decision witness, not an all-range equivalence claim: the legacy
// reader canonicalizes only rows within the requested range. A stable daily
// canonical set cannot reproduce both of these answers after dropping rows.
#[test]
fn legacy_replay_canonicalization_depends_on_the_requested_range() {
    let f = Fixture::new();
    let legacy = TqbnHistoryStore::new(f.root.join("range-witness")).unwrap();
    let legacy_path = legacy.partition_series_path(DAY, SYMBOL, HistorySeriesKind::Tick);
    ensure_parent_dir(&legacy_path).unwrap();
    let old = Tick {
        epoch: None,
        ..f.row(10)
    };
    let replay = Tick {
        datetime: old.datetime + 10_000_000,
        ..old.clone()
    };
    for row in [&old, &replay] {
        with_exclusive_tqbn_lock(&legacy_path, || {
            append_legacy_segment_to_file(
                &legacy_path,
                &HistorySeriesWriteSegment {
                    symbol: SYMBOL,
                    kind: HistorySeriesKind::Tick,
                    declared_range_ns: None,
                    rows: HistorySeriesWriteRows::Ticks(std::slice::from_ref(row)),
                },
            )
        })
        .unwrap();
    }
    for (end, expected_time) in [
        (old.datetime + 5_000_000, old.datetime),
        (replay.datetime + 1, replay.datetime),
    ] {
        let mut reader = legacy
            .open_reader(HistorySeriesReadRequest {
                symbol: SYMBOL.into(),
                kind: HistorySeriesKind::Tick,
                range_start_ns: old.datetime,
                range_end_ns: end,
            })
            .unwrap();
        let Some(HistorySeriesRow::Tick(row)) = reader.next_row().unwrap() else {
            panic!("legacy reader must retain one range-local row");
        };
        assert_eq!(row.datetime, expected_time);
        assert!(reader.next_row().unwrap().is_none());
    }
}

#[test]
fn incoming_id_reset_applies_replay_witnesses_from_old_partition() {
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
    let actual = assert_legacy_equivalent(&f, &[vec![old], vec![replay, reset]]);
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
    let actual = assert_legacy_equivalent(&f, &[old, corrected]);
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
fn legacy_partition_migration_is_verified_atomic_and_idempotent() {
    let f = Fixture::new();
    fs::remove_file(&f.path).unwrap();
    let old = Tick {
        epoch: None,
        ..f.row(10)
    };
    let replay = Tick {
        datetime: old.datetime + 10_000_000,
        ..old.clone()
    };
    with_exclusive_tqbn_lock(&f.path, || {
        append_legacy_segment_to_file(
            &f.path,
            &HistorySeriesWriteSegment {
                symbol: SYMBOL,
                kind: HistorySeriesKind::Tick,
                declared_range_ns: None,
                rows: HistorySeriesWriteRows::Ticks(&[old]),
            },
        )?;
        append_legacy_segment_to_file(
            &f.path,
            &HistorySeriesWriteSegment {
                symbol: SYMBOL,
                kind: HistorySeriesKind::Tick,
                declared_range_ns: None,
                rows: HistorySeriesWriteRows::Ticks(&[replay]),
            },
        )?;
        append_coverage_to_file(&f.path, &f.commit(0, 15))?;
        append_provisional_to_file(
            &f.path,
            &HistorySeriesProvisionalCoverage {
                symbol: SYMBOL.into(),
                kind: HistorySeriesKind::Tick,
                range_start_ns: f.start + 20 * NANOS_PER_SECOND,
                complete_through_ns: f.start + 30 * NANOS_PER_SECOND,
                as_of_ns: f.start + 40 * NANOS_PER_SECOND,
                rows: 0,
                id_range: None,
            },
        )
    })
    .unwrap();
    assert!(!matches(&f.path).unwrap());
    let before = parse_tqbn_series_file(&f.path, SYMBOL, HistorySeriesKind::Tick)
        .unwrap()
        .state;
    let expected_rows = canonical(
        before
            .rows
            .iter()
            .map(|row| match row {
                HistorySeriesRow::Tick(row) => row.clone(),
                HistorySeriesRow::Kline(_) => panic!("legacy Tick file contains Kline"),
            })
            .collect(),
    );
    let backup = f.path.with_extension("tqbn.test-backup");
    fs::hard_link(&f.path, &backup).unwrap();
    let backup_bytes = fs::read(&backup).unwrap();

    f.store.migrate_tick_series_to_current(SYMBOL).unwrap();
    assert!(matches(&f.path).unwrap());
    let after = scan(&f.path, SYMBOL).unwrap();
    assert_eq!(after.coverage, before.coverage);
    assert_eq!(after.provisional, before.provisional);
    assert_eq!(after.rows.len(), expected_rows.len());
    for (left, right) in after.rows.iter().zip(&expected_rows) {
        let HistorySeriesRow::Tick(left) = left else {
            panic!("Tick migration emitted Kline rows");
        };
        assert_eq!(tick_to_spill_bytes(left), tick_to_spill_bytes(right));
    }
    assert_eq!(fs::read(&backup).unwrap(), backup_bytes);
    f.store.migrate_tick_series_to_current(SYMBOL).unwrap();
    assert_eq!(
        scan(&f.path, SYMBOL).unwrap().rows.len(),
        expected_rows.len()
    );
}

#[test]
fn migration_compares_normalized_overlapping_proof_semantics() {
    let f = Fixture::new();
    fs::remove_file(&f.path).unwrap();
    with_exclusive_tqbn_lock(&f.path, || {
        append_legacy_segment_to_file(
            &f.path,
            &HistorySeriesWriteSegment {
                symbol: SYMBOL,
                kind: HistorySeriesKind::Tick,
                declared_range_ns: None,
                rows: HistorySeriesWriteRows::Ticks(&[Tick {
                    epoch: None,
                    ..f.row(20)
                }]),
            },
        )?;
        for (from, through, as_of, rows) in [
            (0, 10, 50, 0),
            (20, 30, 40, 1),
            (20, 35, 45, 2),
            (20, 35, 45, 2),
        ] {
            append_provisional_to_file(
                &f.path,
                &HistorySeriesProvisionalCoverage {
                    symbol: SYMBOL.into(),
                    kind: HistorySeriesKind::Tick,
                    range_start_ns: f.start + from * NANOS_PER_SECOND,
                    complete_through_ns: f.start + through * NANOS_PER_SECOND,
                    as_of_ns: f.start + as_of * NANOS_PER_SECOND,
                    rows,
                    id_range: (rows != 0).then_some((20, 21)),
                },
            )?;
        }
        append_coverage_to_file(&f.path, &f.commit(0, 15))?;
        append_coverage_to_file(&f.path, &f.commit(25, 28))
    })
    .unwrap();
    let before = parse_tqbn_checkpoint_file(&f.path, SYMBOL, HistorySeriesKind::Tick).unwrap();
    f.store.migrate_tick_series_to_current(SYMBOL).unwrap();
    let after = scan(&f.path, SYMBOL).unwrap();
    assert!(observable_checkpoints_equal(
        &before,
        &TqbnIndexedCoverage {
            coverage: after.coverage,
            provisional: after.provisional,
        },
        (f.start, f.start + 24 * 60 * 60 * NANOS_PER_SECOND),
    ));
}

#[test]
fn rejected_migration_candidate_keeps_legacy_inode_and_retries() {
    let f = Fixture::new();
    fs::remove_file(&f.path).unwrap();
    with_exclusive_tqbn_lock(&f.path, || {
        append_legacy_segment_to_file(
            &f.path,
            &HistorySeriesWriteSegment {
                symbol: SYMBOL,
                kind: HistorySeriesKind::Tick,
                declared_range_ns: None,
                rows: HistorySeriesWriteRows::Ticks(&[
                    Tick {
                        epoch: None,
                        ..f.row(10)
                    },
                    Tick {
                        epoch: None,
                        ..f.row(20)
                    },
                ]),
            },
        )
    })
    .unwrap();
    let original = fs::read(&f.path).unwrap();
    truncate_next_migration_candidate();
    assert!(f.store.migrate_tick_series_to_current(SYMBOL).is_err());
    assert_eq!(fs::read(&f.path).unwrap(), original);
    assert!(!matches(&f.path).unwrap());
    assert!(
        fs::read_dir(f.path.parent().unwrap())
            .unwrap()
            .filter_map(std::result::Result::ok)
            .all(|entry| !entry.file_name().to_string_lossy().contains(".cow-"))
    );
    f.store.migrate_tick_series_to_current(SYMBOL).unwrap();
    assert!(matches(&f.path).unwrap());
    assert_eq!(f.all().len(), 2);
}

#[test]
fn migration_ignores_complete_and_torn_uncommitted_suffixes() {
    for torn in [false, true] {
        let f = Fixture::new();
        fs::remove_file(&f.path).unwrap();
        with_exclusive_tqbn_lock(&f.path, || {
            append_legacy_segment_to_file(
                &f.path,
                &HistorySeriesWriteSegment {
                    symbol: SYMBOL,
                    kind: HistorySeriesKind::Tick,
                    declared_range_ns: None,
                    rows: HistorySeriesWriteRows::Ticks(&[Tick {
                        epoch: None,
                        ..f.row(10)
                    }]),
                },
            )?;
            let mut file = OpenOptions::new().read(true).append(true).open(&f.path)?;
            let committed_len = file.metadata()?.len();
            append_rows_block(
                &mut file,
                &HistorySeriesWriteSegment {
                    symbol: SYMBOL,
                    kind: HistorySeriesKind::Tick,
                    declared_range_ns: None,
                    rows: HistorySeriesWriteRows::Ticks(&[Tick {
                        epoch: None,
                        ..f.row(20)
                    }]),
                },
            )?;
            file.sync_data()?;
            if torn {
                file.set_len(committed_len + 7)?;
                file.sync_data()?;
            }
            Ok(())
        })
        .unwrap();
        f.store.migrate_tick_series_to_current(SYMBOL).unwrap();
        let rows = f.all();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, 10);
    }
}

#[test]
fn migration_ignores_uncommitted_coverage_suffix() {
    let f = Fixture::new();
    fs::remove_file(&f.path).unwrap();
    with_exclusive_tqbn_lock(&f.path, || {
        append_legacy_segment_to_file(
            &f.path,
            &HistorySeriesWriteSegment {
                symbol: SYMBOL,
                kind: HistorySeriesKind::Tick,
                declared_range_ns: None,
                rows: HistorySeriesWriteRows::Ticks(&[Tick {
                    epoch: None,
                    ..f.row(10)
                }]),
            },
        )?;
        append_coverage_to_file(&f.path, &f.commit(0, 15))?;
        let mut file = OpenOptions::new().read(true).append(true).open(&f.path)?;
        let file_len = file.metadata()?.len();
        let (_, first_block_offset) =
            read_and_validate_tqbn_prefix(&mut file, SYMBOL, HistorySeriesKind::Tick)?;
        let checkpoint =
            load_tqbn_tail_checkpoint(&f.path, &mut file, first_block_offset as u64, file_len)?
                .expect("committed schema 3 fixture must have a checkpoint");
        append_coverage_block(
            &mut file,
            checkpoint.latest_coverage_index_offset,
            f.start + 20 * NANOS_PER_SECOND,
            f.start + 30 * NANOS_PER_SECOND,
            0,
            None,
        )?;
        file.sync_data()?;
        Ok(())
    })
    .unwrap();

    f.store.migrate_tick_series_to_current(SYMBOL).unwrap();
    let state = scan(&f.path, SYMBOL).unwrap();
    assert_eq!(
        state.coverage,
        vec![(f.start, f.start + 15 * NANOS_PER_SECOND)]
    );
}

#[test]
fn migration_rejects_missing_schema3_checkpoint_without_touching_source() {
    let f = Fixture::new();
    fs::remove_file(&f.path).unwrap();
    with_exclusive_tqbn_lock(&f.path, || {
        append_legacy_segment_to_file(
            &f.path,
            &HistorySeriesWriteSegment {
                symbol: SYMBOL,
                kind: HistorySeriesKind::Tick,
                declared_range_ns: None,
                rows: HistorySeriesWriteRows::Ticks(&[Tick {
                    epoch: None,
                    ..f.row(10)
                }]),
            },
        )
    })
    .unwrap();
    let source = fs::read(&f.path).unwrap();
    fs::remove_file(tqbn_file_lock_path(&f.path)).unwrap();

    assert!(f.store.migrate_tick_series_to_current(SYMBOL).is_err());
    assert_eq!(fs::read(&f.path).unwrap(), source);
    assert!(!matches(&f.path).unwrap());
}

#[test]
fn migration_accepts_schema2_without_a_checkpoint() {
    let f = Fixture::new();
    fs::remove_file(&f.path).unwrap();
    with_exclusive_tqbn_lock(&f.path, || {
        append_legacy_segment_to_file(
            &f.path,
            &HistorySeriesWriteSegment {
                symbol: SYMBOL,
                kind: HistorySeriesKind::Tick,
                declared_range_ns: None,
                rows: HistorySeriesWriteRows::Ticks(&[Tick {
                    epoch: None,
                    ..f.row(10)
                }]),
            },
        )
    })
    .unwrap();
    let mut source = fs::read(&f.path).unwrap();
    source[5..9].copy_from_slice(&TQBN_LEGACY_SCHEMA_VERSION.to_le_bytes());
    fs::write(&f.path, source).unwrap();
    fs::remove_file(tqbn_file_lock_path(&f.path)).unwrap();

    f.store.migrate_tick_series_to_current(SYMBOL).unwrap();
    assert!(matches(&f.path).unwrap());
    assert_eq!(
        f.all().iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![10]
    );
}

#[test]
fn migration_rejects_nonempty_invalid_checkpoint_without_touching_source() {
    let f = Fixture::new();
    fs::remove_file(&f.path).unwrap();
    with_exclusive_tqbn_lock(&f.path, || {
        append_legacy_segment_to_file(
            &f.path,
            &HistorySeriesWriteSegment {
                symbol: SYMBOL,
                kind: HistorySeriesKind::Tick,
                declared_range_ns: None,
                rows: HistorySeriesWriteRows::Ticks(&[Tick {
                    epoch: None,
                    ..f.row(10)
                }]),
            },
        )
    })
    .unwrap();
    let source = fs::read(&f.path).unwrap();
    fs::write(tqbn_file_lock_path(&f.path), b"TQTC\x02\0\0").unwrap();
    assert!(f.store.migrate_tick_series_to_current(SYMBOL).is_err());
    assert_eq!(fs::read(&f.path).unwrap(), source);
    assert!(!matches(&f.path).unwrap());
}

#[test]
fn migration_idempotence_deeply_validates_common_commit_and_tail() {
    for truncated_commit in [true, false] {
        let f = Fixture::new();
        f.write(&[f.row(10)]);
        if truncated_commit {
            OpenOptions::new()
                .write(true)
                .open(&f.path)
                .unwrap()
                .set_len(storage::MAGIC.len() as u64)
                .unwrap();
        } else {
            let mut file = OpenOptions::new().append(true).open(&f.path).unwrap();
            file.write_all(b"uncommitted-common-tail").unwrap();
            file.sync_all().unwrap();
        }
        let source = fs::read(&f.path).unwrap();
        assert!(f.store.migrate_tick_series_to_current(SYMBOL).is_err());
        assert_eq!(fs::read(&f.path).unwrap(), source);
    }
}

#[test]
fn migration_idempotence_rejects_a_corrupt_common_index() {
    let f = Fixture::new();
    f.write(&[f.row(10)]);
    let mut bytes = fs::read(&f.path).unwrap();
    let last = bytes.last_mut().expect("common fixture must have an index");
    *last ^= 0xff;
    fs::write(&f.path, &bytes).unwrap();

    assert!(f.store.migrate_tick_series_to_current(SYMBOL).is_err());
    assert_eq!(fs::read(&f.path).unwrap(), bytes);
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
    let actual = assert_legacy_equivalent(&f, &[vec![old, reset], vec![next]]);
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
            trading_day_range(NaiveDate::from_ymd_opt(2026, 1, 5).unwrap()).unwrap();
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
    let (_, start, _) = trading_day_range(NaiveDate::from_ymd_opt(2026, 1, 5).unwrap()).unwrap();
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
    let before_migration = fs::read(&f.path).unwrap();
    assert!(f.store.migrate_tick_series_to_current(SYMBOL).is_err());
    assert_eq!(fs::read(&f.path).unwrap(), before_migration);
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
