#[cfg(unix)]
mod hardlink_tests {
    use super::super::*;
    use super::{SYMBOL, tick5, tqbn_store};
    use std::os::unix::fs::MetadataExt;

    fn retained_partition(label: &str) -> (TqbnHistoryStore, PathBuf, PathBuf, Vec<u8>) {
        let store = tqbn_store(label);
        let path = store.partition_series_path("19700101", SYMBOL, HistorySeriesKind::Tick);
        let row = tick5(1, 1_000, 618.5, 623.5);
        store
            .write_segment(HistorySeriesWriteSegment {
                symbol: SYMBOL,
                kind: HistorySeriesKind::Tick,
                declared_range_ns: Some((1_000, 2_000)),
                rows: HistorySeriesWriteRows::Ticks(std::slice::from_ref(&row)),
            })
            .unwrap();
        let retained = path.parent().unwrap().join("retained.tqbn");
        fs::hard_link(&path, &retained).unwrap();
        fs::copy(tqbn_file_lock_path(&path), tqbn_file_lock_path(&retained)).unwrap();
        let bytes = fs::read(&retained).unwrap();
        (store, path, retained, bytes)
    }

    fn append_tick(store: &TqbnHistoryStore, id: i64) {
        let row = tick5(id, id * 1_000, 618.5, 623.5);
        store
            .write_segment(HistorySeriesWriteSegment {
                symbol: SYMBOL,
                kind: HistorySeriesKind::Tick,
                declared_range_ns: Some((id * 1_000, (id + 1) * 1_000)),
                rows: HistorySeriesWriteRows::Ticks(std::slice::from_ref(&row)),
            })
            .unwrap();
    }

    fn assert_retained(path: &Path, retained: &Path, bytes: &[u8]) {
        assert!(
            fs::read(retained).unwrap() == bytes,
            "retained bytes changed"
        );
        assert_ne!(
            fs::metadata(path).unwrap().ino(),
            fs::metadata(retained).unwrap().ino(),
            "writer must publish a private inode"
        );
        let old = parse_tqbn_checkpoint_file(retained, SYMBOL, HistorySeriesKind::Tick).unwrap();
        assert_eq!(old.coverage, vec![(1_000, 2_000)]);
    }

    #[test]
    fn append_detaches_hardlinked_tick_data() {
        let (store, path, retained, bytes) = retained_partition("cow-append");
        let mut original_reader = File::open(&path).unwrap();
        let checkpoint_before = fs::read(tqbn_file_lock_path(&retained)).unwrap();
        append_tick(&store, 2);
        assert_retained(&path, &retained, &bytes);
        let mut original_bytes = Vec::new();
        original_reader.read_to_end(&mut original_bytes).unwrap();
        assert!(
            original_bytes == bytes,
            "an already-open reader changed inode"
        );
        assert_eq!(
            fs::read(tqbn_file_lock_path(&retained)).unwrap(),
            checkpoint_before
        );
        assert_eq!(
            store
                .coverage(HistorySeriesCoverageRequest {
                    symbol: SYMBOL.into(),
                    kind: HistorySeriesKind::Tick,
                    range_start_ns: 1_000,
                    range_end_ns: 3_000,
                })
                .unwrap()
                .cached_ranges,
            vec![(1_000, 3_000)]
        );
        let private_ino = fs::metadata(&path).unwrap().ino();
        append_tick(&store, 3);
        assert_eq!(
            fs::metadata(&path).unwrap().ino(),
            private_ino,
            "single-link fast path copied"
        );
    }

    #[test]
    fn empty_coverage_append_detaches_hardlinked_tick_data() {
        let (store, path, retained, bytes) = retained_partition("cow-coverage");
        store
            .append_coverage(HistorySeriesCoverageCommit {
                symbol: SYMBOL.into(),
                kind: HistorySeriesKind::Tick,
                range_start_ns: 2_000,
                range_end_ns: 3_000,
                rows: 0,
                id_range: None,
            })
            .unwrap();
        assert_retained(&path, &retained, &bytes);
    }

    #[test]
    fn provisional_append_detaches_hardlinked_tick_data() {
        let (store, path, retained, bytes) = retained_partition("cow-provisional");
        store
            .append_provisional(HistorySeriesProvisionalCoverage {
                symbol: SYMBOL.into(),
                kind: HistorySeriesKind::Tick,
                range_start_ns: 2_000,
                complete_through_ns: 3_000,
                as_of_ns: 4_000,
                rows: 0,
                id_range: None,
            })
            .unwrap();
        assert_retained(&path, &retained, &bytes);
    }

    #[test]
    fn tail_repair_does_not_truncate_retained_hardlink() {
        let (store, path, retained, _) = retained_partition("cow-tail-repair");
        let mut input = OpenOptions::new().append(true).open(&path).unwrap();
        input.write_all(b"TQBB-incomplete").unwrap();
        input.sync_all().unwrap();
        drop(input);
        let bytes = fs::read(&retained).unwrap();
        append_tick(&store, 2);
        assert_retained(&path, &retained, &bytes);
    }

    #[test]
    fn compaction_preserves_retained_hardlink() {
        let (_store, path, retained, bytes) = retained_partition("cow-compact");
        compact_tqbn_file(&path, SYMBOL, HistorySeriesKind::Tick).unwrap();
        assert_retained(&path, &retained, &bytes);
    }


    #[test]
    fn copy_failure_preserves_both_links_and_removes_own_temporary() {
        let (_store, path, retained, bytes) = retained_partition("cow-copy-failure");
        let mut write_only = OpenOptions::new().write(true).open(&path).unwrap();
        detach_hardlinked_tqbn_data(&path, &mut write_only).expect_err("source read must fail");
        assert!(fs::read(&path).unwrap() == bytes);
        assert!(fs::read(&retained).unwrap() == bytes);
        assert_eq!(
            fs::metadata(&path).unwrap().ino(),
            fs::metadata(&retained).unwrap().ino()
        );
        assert_no_copy_temporary(path.parent().unwrap());
    }

    #[test]
    fn purge_unlinks_canonical_without_mutating_shared_data_or_checkpoint() {
        let (store, path, retained, bytes) = retained_partition("cow-purge");
        let checkpoint = tqbn_file_lock_path(&path);
        let peer = store.root_dir.join("retained-checkpoint");
        fs::hard_link(&checkpoint, &peer).unwrap();
        let checkpoint_bytes = fs::read(&peer).unwrap();
        let report = store.purge_series(SYMBOL, HistorySeriesKind::Tick).unwrap();
        assert_eq!(report.removed_files, 1);
        assert!(!path.exists());
        assert_eq!(fs::read(&retained).unwrap(), bytes);
        assert_eq!(fs::read(&peer).unwrap(), checkpoint_bytes);
        assert_no_copy_temporary(path.parent().unwrap());
    }

    #[test]
    fn shared_checkpoint_rejects_compaction_before_data_replacement() {
        let (store, path, retained, bytes) = retained_partition("cow-compact-checkpoint");
        let checkpoint = tqbn_file_lock_path(&path);
        let peer = store.root_dir.join("retained-checkpoint");
        fs::hard_link(&checkpoint, &peer).unwrap();
        let checkpoint_bytes = fs::read(&peer).unwrap();
        let inode = fs::metadata(&path).unwrap().ino();
        let error = compact_tqbn_file(&path, SYMBOL, HistorySeriesKind::Tick).unwrap_err();
        assert!(error.to_string().contains("hardlinked"), "{error}");
        assert_eq!(fs::metadata(&path).unwrap().ino(), inode);
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert_eq!(fs::read(&retained).unwrap(), bytes);
        assert_eq!(fs::read(&peer).unwrap(), checkpoint_bytes);
        assert_no_copy_temporary(path.parent().unwrap());
    }

    #[test]
    fn rename_failure_preserves_source_and_removes_own_temporary() {
        let (store, path, retained, bytes) = retained_partition("cow-rename-failure");
        let occupied = store.root_dir.join("occupied");
        fs::create_dir(&occupied).unwrap();
        let mut input = File::open(&path).unwrap();
        detach_hardlinked_tqbn_data(&occupied, &mut input).expect_err("cannot replace a directory");
        assert!(fs::read(&path).unwrap() == bytes);
        assert!(fs::read(&retained).unwrap() == bytes);
        assert!(occupied.is_dir());
        assert_no_copy_temporary(&store.root_dir);
    }

    fn assert_no_copy_temporary(directory: &Path) {
        assert!(fs::read_dir(directory).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".cow-")
        }));
    }
}
