#[path = "support/backtest_history.rs"]
mod support;

use tqsdk_data::{
    BacktestHistoryMaintenanceClient, BacktestHistoryMetadataCache, BacktestHistoryPhysicalSegment,
};

use support::{metadata_symbol_dir, missing_path, segment, snapshot, temp_dir};

#[test]
fn concrete_symbol_snapshot_round_trips_through_active_sidecar() {
    let root = temp_dir("metadata-concrete");
    let cache = BacktestHistoryMetadataCache::open(&root).unwrap();
    let stored = cache
        .store_snapshot(snapshot(
            "SHFE.au2608",
            1_768_000_000_000_000_000,
            vec![segment(
                "SHFE.au2608",
                1_767_572_800_000_000_000,
                1_767_745_600_000_000_000,
            )],
        ))
        .unwrap();

    assert!(!stored.snapshot_hash.is_empty());
    assert_eq!(
        cache.load_active("SHFE.au2608").unwrap(),
        Some(stored.clone())
    );
    assert_eq!(
        BacktestHistoryMaintenanceClient::builder(&root)
            .build()
            .unwrap()
            .inspect_metadata("SHFE.au2608")
            .unwrap(),
        Some(stored)
    );
}

#[test]
fn index_symbol_snapshot_round_trips_without_ttl_expiry() {
    let root = temp_dir("metadata-index");
    let cache = BacktestHistoryMetadataCache::open(&root).unwrap();
    let stored = cache
        .store_snapshot(snapshot(
            "KQ.i@SHFE.au",
            1,
            vec![segment(
                "KQ.i@SHFE.au",
                1_767_572_800_000_000_000,
                1_767_745_600_000_000_000,
            )],
        ))
        .unwrap();

    assert_eq!(cache.load_active("KQ.i@SHFE.au").unwrap(), Some(stored));
}

#[test]
fn main_continuous_snapshot_preserves_each_physical_segment() {
    let root = temp_dir("metadata-main-continuous");
    let cache = BacktestHistoryMetadataCache::open(&root).unwrap();
    let segments = vec![
        segment(
            "SHFE.au2606",
            1_767_572_800_000_000_000,
            1_767_659_200_000_000_000,
        ),
        segment(
            "SHFE.au2608",
            1_767_659_200_000_000_000,
            1_767_745_600_000_000_000,
        ),
    ];
    let stored = cache
        .store_snapshot(snapshot("KQ.m@SHFE.au", 2, segments.clone()))
        .unwrap();

    assert_eq!(stored.physical_segments, segments);
    assert_eq!(
        cache
            .load_active("KQ.m@SHFE.au")
            .unwrap()
            .unwrap()
            .physical_segments,
        segments
    );
}

#[test]
fn read_only_missing_sidecar_is_an_offline_cache_miss_without_creating_files() {
    let root = missing_path("metadata-missing");
    let cache = BacktestHistoryMetadataCache::open_read_only(&root);

    assert_eq!(cache.load_active("KQ.m@SHFE.au").unwrap(), None);
    assert!(!root.exists());
}

#[test]
fn path_traversal_symbol_is_rejected_before_writing_a_sidecar() {
    let root = temp_dir("metadata-path-traversal");
    let cache = BacktestHistoryMetadataCache::open(&root).unwrap();

    assert!(
        cache
            .store_snapshot(snapshot(
                "..",
                1,
                vec![segment(
                    "..",
                    1_767_572_800_000_000_000,
                    1_767_745_600_000_000_000
                )],
            ))
            .is_err()
    );
    assert!(!root.join("active.json").exists());
    assert!(!root.join("snapshots").exists());
}

#[test]
fn invalid_snapshot_json_or_hash_fails_closed_without_mutating_files() {
    let root = temp_dir("metadata-fail-closed");
    let cache = BacktestHistoryMetadataCache::open(&root).unwrap();
    let stored = cache
        .store_snapshot(snapshot(
            "KQ.m@SHFE.au",
            3,
            vec![segment(
                "SHFE.au2608",
                1_767_572_800_000_000_000,
                1_767_745_600_000_000_000,
            )],
        ))
        .unwrap();
    let snapshot_path = metadata_symbol_dir(&root, "KQ.m@SHFE.au")
        .join("snapshots")
        .join(format!("{}.json", stored.snapshot_hash));
    let mut tampered = stored.clone();
    tampered.captured_at_ns = 4;
    let tampered_bytes = serde_json::to_vec(&tampered).unwrap();
    std::fs::write(&snapshot_path, &tampered_bytes).unwrap();

    assert!(cache.load_active("KQ.m@SHFE.au").is_err());
    assert!(snapshot_path.exists());
    assert_eq!(std::fs::read(&snapshot_path).unwrap(), tampered_bytes);

    let active_path = metadata_symbol_dir(&root, "KQ.m@SHFE.au").join("active.json");
    let corrupt_active = b"not json".to_vec();
    std::fs::write(&active_path, &corrupt_active).unwrap();
    assert!(cache.load_active("KQ.m@SHFE.au").is_err());
    assert!(active_path.exists());
    assert_eq!(std::fs::read(active_path).unwrap(), corrupt_active);
}

#[test]
fn storing_refresh_snapshot_keeps_prior_snapshots_and_moves_only_active_pointer() {
    let root = temp_dir("metadata-refresh");
    let cache = BacktestHistoryMetadataCache::open(&root).unwrap();
    let first = cache
        .store_snapshot(snapshot(
            "KQ.i@SHFE.au",
            10,
            vec![BacktestHistoryPhysicalSegment {
                physical_symbol: "KQ.i@SHFE.au".to_string(),
                start_ns: 1_767_572_800_000_000_000,
                end_ns: 1_767_745_600_000_000_000,
            }],
        ))
        .unwrap();
    let second = cache
        .store_snapshot(snapshot(
            "KQ.i@SHFE.au",
            11,
            vec![segment(
                "KQ.i@SHFE.au",
                1_767_572_800_000_000_000,
                1_767_745_600_000_000_000,
            )],
        ))
        .unwrap();
    let snapshots_dir = metadata_symbol_dir(&root, "KQ.i@SHFE.au").join("snapshots");

    assert_ne!(first.snapshot_hash, second.snapshot_hash);
    assert!(
        snapshots_dir
            .join(format!("{}.json", first.snapshot_hash))
            .exists()
    );
    assert!(
        snapshots_dir
            .join(format!("{}.json", second.snapshot_hash))
            .exists()
    );
    assert_eq!(cache.load_active("KQ.i@SHFE.au").unwrap(), Some(second));
}
