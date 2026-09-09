use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::Utc;
use fs2::FileExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tqsdk_core::Tick;
use tqsdk_data::{
    BacktestHistoryFailureReason, BacktestHistoryFinality, BacktestHistoryMetadataCache,
    BacktestHistoryRequest, BacktestHistoryRows, BacktestHistorySnapshot,
    BacktestHistorySnapshotFileDisposition, BacktestHistorySnapshotFileRole,
    BacktestHistorySnapshotManifestBuilder, BacktestTickCache, MinuteKlineCache,
    MinuteKlineCacheSnapshot, classify_backtest_history_snapshot_cache_path,
};

#[path = "support/backtest_history.rs"]
mod support;

fn temp_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "tqsdk-backtest-history-snapshot-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn minimal_tqbn(block_flags: u8) -> Vec<u8> {
    const FNV1A_EMPTY: u64 = 0xcbf29ce484222325;

    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"TQBN");
    bytes.push(1);
    bytes.extend_from_slice(&3_u32.to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.extend_from_slice(&FNV1A_EMPTY.to_le_bytes());
    bytes.extend_from_slice(b"TQBB");
    bytes.push(2);
    bytes.extend_from_slice(&[block_flags, 0, 0]);
    bytes.extend_from_slice(&0_u64.to_le_bytes());
    bytes.extend_from_slice(&FNV1A_EMPTY.to_le_bytes());
    bytes
}

fn tqbn_checkpoint(bytes: &[u8]) -> [u8; 32] {
    const FNV1A_EMPTY: u64 = 0xcbf29ce484222325;

    let tail_hash = bytes[bytes.len().saturating_sub(64)..]
        .iter()
        .fold(FNV1A_EMPTY, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        });
    let mut checkpoint = [0_u8; 32];
    checkpoint[..4].copy_from_slice(b"TQTC");
    checkpoint[4] = 2;
    checkpoint[8..16].copy_from_slice(&(bytes.len() as u64).to_le_bytes());
    checkpoint[16..24].copy_from_slice(&tail_hash.to_le_bytes());
    checkpoint[24..32].copy_from_slice(&u64::MAX.to_le_bytes());
    checkpoint
}

fn write_snapshot(root: &std::path::Path, catalog_complete: bool, symbols: &[&str]) {
    let metadata_hash = metadata_inventory_hash(&[]);
    let mut manifest = json!({
        "manifest_version": 1,
        "created_at": "2026-08-29T00:00:00Z",
        "minimum_reader": "0.1.0",
        "required_features": [],
        "cache_formats": [
            {"family": "daily", "format_id": "tqsdk.daily-kline.single-file.v1", "schema_version": 1},
            {"family": "minute", "format_id": "tqsdk.minute-kline.monthly.v5", "schema_version": 5},
            {"family": "tick", "format_id": "tqsdk.tqbn.daily.v3", "schema_version": 3}
        ],
        "metadata_snapshot_hash": metadata_hash,
        "catalog": {"complete": catalog_complete, "symbols": symbols},
        "coverage_summary": [],
        "files": []
    });
    let canonical = serde_json::to_vec(&manifest).unwrap();
    let identity = format!("sha256:{:x}", Sha256::digest(canonical));
    let snapshot_id = format!("s-20260829-{}", &identity[7..15]);
    let object = manifest.as_object_mut().unwrap();
    object.insert(
        "snapshot_id".to_string(),
        Value::String(snapshot_id.clone()),
    );
    object.insert("identity_sha256".to_string(), Value::String(identity));

    let generation = root.join("snapshots").join(&snapshot_id);
    std::fs::create_dir_all(generation.join("cache")).unwrap();
    std::fs::write(generation.join("lease.lock"), []).unwrap();
    std::fs::write(
        generation.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    std::fs::write(root.join("CURRENT"), format!("{snapshot_id}\n")).unwrap();
}

fn current_manifest_path(root: &std::path::Path) -> PathBuf {
    let snapshot_id = std::fs::read_to_string(root.join("CURRENT"))
        .unwrap()
        .trim()
        .to_string();
    root.join("snapshots")
        .join(snapshot_id)
        .join("manifest.json")
}

fn current_generation_path(root: &Path) -> PathBuf {
    current_manifest_path(root).parent().unwrap().to_path_buf()
}

fn publish_cache_snapshot(
    root: &Path,
    catalog_symbols: &[&str],
    prepare_cache: impl FnOnce(&Path) -> String,
) -> String {
    let pending = root.join("snapshots/pending");
    let cache_dir = pending.join("cache");
    std::fs::create_dir_all(&cache_dir).unwrap();
    std::fs::write(pending.join("lease.lock"), []).unwrap();
    let cache_metadata_snapshot_hash = prepare_cache(&cache_dir);
    let files = manifest_files(&pending, &cache_dir);
    let metadata_snapshot_hash = metadata_inventory_hash(files.as_slice());
    let mut symbols = catalog_symbols.to_vec();
    symbols.sort_unstable();
    symbols.dedup();

    let mut manifest = json!({
        "manifest_version": 1,
        "created_at": "2026-08-29T00:00:00Z",
        "minimum_reader": "0.1.0",
        "required_features": ["history-container-v1"],
        "cache_formats": [
            {"family": "daily", "format_id": "tqsdk.daily-kline.single-file.v1", "schema_version": 1},
            {"family": "minute", "format_id": "tqsdk.minute-kline.monthly.v5", "schema_version": 5},
            {"family": "tick", "format_id": "tqsdk.tqbn.daily.v3", "schema_version": 3}
        ],
        "metadata_snapshot_hash": metadata_snapshot_hash,
        "catalog": {"complete": true, "symbols": symbols},
        "coverage_summary": [],
        "files": files
    });
    let identity = format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(&manifest).unwrap())
    );
    let snapshot_id = format!("s-20260829-{}", &identity[7..15]);
    manifest["snapshot_id"] = Value::String(snapshot_id.clone());
    manifest["identity_sha256"] = Value::String(identity);

    let generation = root.join("snapshots").join(&snapshot_id);
    std::fs::rename(&pending, &generation).unwrap();
    std::fs::write(
        generation.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    std::fs::write(root.join("CURRENT"), format!("{snapshot_id}\n")).unwrap();
    cache_metadata_snapshot_hash
}

fn manifest_files(generation: &Path, cache_dir: &Path) -> Vec<Value> {
    let mut files = Vec::new();
    collect_manifest_files(generation, cache_dir, &mut files);
    files.sort_by(|left, right| {
        left["path"]
            .as_str()
            .unwrap()
            .cmp(right["path"].as_str().unwrap())
    });
    files
}

fn metadata_inventory_hash(files: &[Value]) -> String {
    let payload = files
        .iter()
        .filter(|file| {
            matches!(
                file["role"].as_str(),
                Some("metadata_content_addressed" | "pointer_copy")
            )
        })
        .map(|file| {
            json!([
                file["path"].as_str().unwrap(),
                file["role"].as_str().unwrap(),
                file["sha256"].as_str().unwrap()
            ])
        })
        .collect::<Vec<_>>();
    format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(&payload).unwrap())
    )
}

fn collect_manifest_files(generation: &Path, directory: &Path, files: &mut Vec<Value>) {
    let mut entries = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap())
        .collect::<Vec<_>>();
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        let metadata = std::fs::symlink_metadata(&path).unwrap();
        if metadata.is_dir() {
            collect_manifest_files(generation, &path, files);
            continue;
        }
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if file_name == ".tqsdk-cache-operation.lock" || file_name.ends_with(".lock") {
            continue;
        }
        assert!(metadata.is_file());
        let relative = path
            .strip_prefix(generation)
            .unwrap()
            .to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/");
        let role = if relative.ends_with(".tqbn") {
            "tqbn_mutable_layout"
        } else if relative.ends_with(".tqmk") {
            "tqmk_immutable_generation"
        } else if relative.ends_with(".tqdk") {
            "tqdk_immutable_generation"
        } else if relative.ends_with("/active.json") {
            "pointer_copy"
        } else if relative.ends_with(".json") && relative.contains("/snapshots/") {
            "metadata_content_addressed"
        } else {
            panic!("unexpected cache file in snapshot fixture: {relative}");
        };
        let bytes = std::fs::read(&path).unwrap();
        files.push(json!({
            "path": relative,
            "role": role,
            "size": metadata.len(),
            "sha256": format!("sha256:{:x}", Sha256::digest(&bytes))
        }));
    }
}

#[test]
fn public_manifest_builder_owns_roles_identity_and_staging_validation() {
    let root = temp_dir("public-manifest-builder");
    let pending = root.join("staging/pending");
    let cache = pending.join("cache");
    std::fs::create_dir_all(cache.join("series/20260829/tick")).unwrap();
    std::fs::create_dir_all(cache.join("minute-kline-v3")).unwrap();
    std::fs::create_dir_all(cache.join("daily-kline-v1")).unwrap();
    std::fs::create_dir_all(cache.join("backtest-metadata-v2/snapshots")).unwrap();
    let legacy_tick = minimal_tqbn(0);
    std::fs::write(
        cache.join("series/20260829/tick/SHFE.au2612.tqbn"),
        &legacy_tick,
    )
    .unwrap();
    BacktestTickCache::open(&cache)
        .unwrap()
        .store_ticks("SHFE.ag2612", DAY_START_NS, DAY_END_NS, std::iter::empty())
        .unwrap();
    let snapshot = MinuteKlineCacheSnapshot::cst_v1();
    MinuteKlineCache::open(&cache)
        .unwrap()
        .store_final_range(
            "SHFE.au2612",
            MINUTE_START_NS,
            MINUTE_END_NS,
            &snapshot,
            &[],
        )
        .unwrap();
    tqsdk_data::DailyKlineCache::open(&cache)
        .unwrap()
        .store_final_range("SHFE.au2612", DAY_START_NS, DAY_END_NS, &snapshot, &[])
        .unwrap();
    std::fs::write(
        cache.join("backtest-metadata-v2/snapshots/content.json"),
        b"{}",
    )
    .unwrap();
    std::fs::write(cache.join("backtest-metadata-v2/active.json"), b"{}").unwrap();
    std::fs::write(cache.join(".tqsdk-cache-operation.lock"), b"").unwrap();
    std::fs::write(
        cache.join("series/20260829/tick/SHFE.au2612.tqbn.lock"),
        tqbn_checkpoint(&legacy_tick),
    )
    .unwrap();

    let artifact =
        BacktestHistorySnapshotManifestBuilder::new("2026-08-29T00:00:00Z".parse().unwrap())
            .catalog(false, std::iter::empty::<&str>())
            .build(&cache)
            .unwrap();

    assert!(artifact.snapshot_id().starts_with("s-20260829-"));
    assert!(artifact.identity_sha256().starts_with("sha256:"));
    assert!(artifact.metadata_snapshot_hash().starts_with("sha256:"));
    let manifest: Value = serde_json::from_slice(artifact.manifest_bytes()).unwrap();
    assert_eq!(
        manifest["cache_formats"],
        json!([
            {
                "family": "daily",
                "format_id": "tqsdk.daily-kline.single-file.v1",
                "schema_version": 1
            },
            {
                "family": "minute",
                "format_id": "tqsdk.minute-kline.monthly.v6",
                "schema_version": 6
            },
            {
                "family": "tick",
                "format_id": "tqsdk.history-container.tick.v1",
                "schema_version": 4
            }
        ])
    );
    assert_eq!(
        manifest["required_features"],
        json!(["history-container-v1"])
    );
    assert_eq!(manifest["files"].as_array().unwrap().len(), 7);
    assert!(
        manifest["files"]
            .as_array()
            .unwrap()
            .windows(2)
            .all(|pair| pair[0]["path"].as_str() < pair[1]["path"].as_str())
    );

    let generation = root.join("staging").join(artifact.snapshot_id());
    std::fs::rename(&pending, &generation).unwrap();
    std::fs::write(generation.join("lease.lock"), b"").unwrap();
    std::fs::write(generation.join("manifest.json"), artifact.manifest_bytes()).unwrap();

    let opened = BacktestHistorySnapshot::open_generation(&root, &generation);
    assert_eq!(opened.unwrap().snapshot_id(), artifact.snapshot_id());
    {
        let staged_tick = BacktestTickCache::open_read_only(generation.join("cache"))
            .diagnose()
            .unwrap();
        assert_eq!(staged_tick.files.len(), 3);
        assert!(
            staged_tick
                .files
                .iter()
                .any(|file| file.schema_version == Some(4))
        );
        assert!(
            staged_tick
                .files
                .iter()
                .any(|file| { file.path.ends_with("series/20260829/tick/SHFE.au2612.tqbn") })
        );
    }
    assert!(!root.join("CURRENT").exists());

    let tqbn = classify_backtest_history_snapshot_cache_path(Path::new(
        "series/20260829/tick/SHFE.au2612.tqbn",
    ))
    .unwrap();
    assert_eq!(
        tqbn,
        BacktestHistorySnapshotFileDisposition::Include(
            BacktestHistorySnapshotFileRole::TqbnMutableLayout
        )
    );
    assert!(!BacktestHistorySnapshotFileRole::TqbnMutableLayout.allows_hardlink());
    assert_eq!(
        classify_backtest_history_snapshot_cache_path(Path::new(".metadata.lock")).unwrap(),
        BacktestHistorySnapshotFileDisposition::Rebuild
    );

    let published_cache = generation.join("cache");
    let unknown = published_cache.join("unknown.bin");
    std::fs::write(&unknown, b"unknown").unwrap();
    let error = BacktestHistorySnapshotManifestBuilder::new(Utc::now())
        .catalog(false, std::iter::empty::<&str>())
        .build(&published_cache)
        .unwrap_err();
    assert_eq!(
        error.reason(),
        &BacktestHistoryFailureReason::SnapshotIncompatible
    );
}

#[tokio::test]
async fn common_tick_only_snapshot_builds_opens_queries_and_rejects_bad_inputs() {
    let root = temp_dir("common-tick-only");
    let pending = root.join("snapshots/pending");
    let cache_dir = pending.join("cache");
    let symbol = "SHFE.ag2612";
    std::fs::create_dir_all(&cache_dir).unwrap();
    BacktestHistoryMetadataCache::open(&cache_dir)
        .unwrap()
        .store_snapshot(support::snapshot(
            symbol,
            DAY_END_NS,
            vec![support::segment(symbol, DAY_START_NS, DAY_END_NS)],
        ))
        .unwrap();
    BacktestTickCache::open(&cache_dir)
        .unwrap()
        .store_ticks(
            symbol,
            CONCRETE_SOURCE_START_NS,
            DAY_END_NS,
            [Tick {
                id: 42,
                datetime: DAY_START_NS + 1,
                last_price: 618.75,
                volume: 7,
                amount: 4_331.25,
                open_interest: 99,
                ..Tick::default()
            }],
        )
        .unwrap();

    let artifact = BacktestHistorySnapshotManifestBuilder::new(Utc::now())
        .catalog(true, [symbol])
        .build(&cache_dir)
        .unwrap();
    let manifest: Value = serde_json::from_slice(artifact.manifest_bytes()).unwrap();
    #[cfg(feature = "tqbn-zstd")]
    assert_eq!(
        manifest["required_features"],
        json!(["history-container-v1", "tqbn-zstd"])
    );
    #[cfg(not(feature = "tqbn-zstd"))]
    assert_eq!(
        manifest["required_features"],
        json!(["history-container-v1"])
    );

    let snapshot_id = artifact.snapshot_id().to_string();
    let generation = root.join("snapshots").join(&snapshot_id);
    std::fs::rename(&pending, &generation).unwrap();
    std::fs::write(generation.join("lease.lock"), []).unwrap();
    std::fs::write(generation.join("manifest.json"), artifact.manifest_bytes()).unwrap();
    std::fs::write(root.join("CURRENT"), format!("{snapshot_id}\n")).unwrap();

    let snapshot = BacktestHistorySnapshot::open(&root).unwrap();
    let result = snapshot
        .query(BacktestHistoryRequest::tick(
            91,
            symbol,
            DAY_START_NS,
            DAY_END_NS,
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let BacktestHistoryRows::Ticks(rows) = result.rows else {
        panic!("Tick snapshot query must return Tick rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 42);
    assert_eq!(rows[0].datetime, DAY_START_NS + 1);
    assert_eq!(rows[0].last_price, 618.75);
    assert_eq!(rows[0].volume, 7);
    assert_eq!(rows[0].amount, 4_331.25);
    assert_eq!(rows[0].open_interest, 99);

    let missing_feature_root = temp_dir("common-tick-missing-feature");
    let missing_cache = missing_feature_root.join("cache");
    std::fs::create_dir_all(&missing_cache).unwrap();
    BacktestTickCache::open(&missing_cache)
        .unwrap()
        .store_ticks(symbol, DAY_START_NS, DAY_END_NS, std::iter::empty())
        .unwrap();
    let missing =
        BacktestHistorySnapshotManifestBuilder::new("2026-08-29T00:00:00Z".parse().unwrap())
            .catalog(false, std::iter::empty::<&str>())
            .build(&missing_cache)
            .unwrap();
    let mut missing_manifest: Value = serde_json::from_slice(missing.manifest_bytes()).unwrap();
    missing_manifest["required_features"] = json!([]);
    let object = missing_manifest.as_object_mut().unwrap();
    object.remove("snapshot_id");
    object.remove("identity_sha256");
    let identity = format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(&missing_manifest).unwrap())
    );
    let missing_snapshot_id = format!("s-20260829-{}", &identity[7..15]);
    let object = missing_manifest.as_object_mut().unwrap();
    object.insert(
        "snapshot_id".to_string(),
        Value::String(missing_snapshot_id.clone()),
    );
    object.insert("identity_sha256".to_string(), Value::String(identity));
    let missing_manifest_bytes = serde_json::to_vec(&missing_manifest).unwrap();
    let missing_generation = missing_feature_root
        .join("staging")
        .join(&missing_snapshot_id);
    std::fs::create_dir_all(&missing_generation).unwrap();
    std::fs::rename(&missing_cache, missing_generation.join("cache")).unwrap();
    std::fs::write(missing_generation.join("lease.lock"), []).unwrap();
    std::fs::write(
        missing_generation.join("manifest.json"),
        missing_manifest_bytes,
    )
    .unwrap();
    let missing_error =
        BacktestHistorySnapshot::open_generation(&missing_feature_root, &missing_generation)
            .unwrap_err();
    assert_eq!(
        missing_error.reason(),
        &BacktestHistoryFailureReason::SnapshotIncompatible,
        "{missing_error:?}"
    );
    assert!(missing_error.to_string().contains("history-container-v1"));

    let dirty_root = temp_dir("common-tick-dirty-tail");
    let dirty_cache = dirty_root.join("cache");
    let dirty = BacktestTickCache::open(&dirty_cache).unwrap();
    dirty
        .store_ticks(symbol, DAY_START_NS, DAY_END_NS, std::iter::empty())
        .unwrap();
    let common_path = dirty
        .diagnose()
        .unwrap()
        .files
        .into_iter()
        .find(|file| file.schema_version == Some(4))
        .unwrap()
        .path;
    {
        use std::io::Write as _;
        std::fs::OpenOptions::new()
            .append(true)
            .open(common_path)
            .unwrap()
            .write_all(b"dirty-tail")
            .unwrap();
    }
    assert_eq!(
        BacktestHistorySnapshotManifestBuilder::new(Utc::now())
            .catalog(false, std::iter::empty::<&str>())
            .build(&dirty_cache)
            .unwrap_err()
            .reason(),
        &BacktestHistoryFailureReason::SnapshotCorrupt
    );
}

#[test]
fn missing_current_is_typed_as_snapshot_unavailable() {
    let root = temp_dir("missing-current");
    std::fs::create_dir_all(root.join("snapshots")).unwrap();

    let error = BacktestHistorySnapshot::open(&root).unwrap_err();

    assert_eq!(
        error.reason(),
        &BacktestHistoryFailureReason::SnapshotUnavailable
    );
    assert!(error.to_string().contains("CURRENT"));
}

#[test]
fn catalog_symbol_without_metadata_sidecar_is_incomplete() {
    let root = temp_dir("missing-metadata-sidecar");
    write_snapshot(&root, true, &["SHFE.au2602"]);

    let error = BacktestHistorySnapshot::open(&root).unwrap_err();
    assert_eq!(
        error.reason(),
        &BacktestHistoryFailureReason::MetadataIncomplete
    );
}

#[test]
fn incompatible_and_corrupt_manifests_remain_distinct_at_the_public_seam() {
    let incompatible_root = temp_dir("incompatible-manifest");
    write_snapshot(&incompatible_root, true, &[]);
    let path = current_manifest_path(&incompatible_root);
    let mut manifest: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    manifest["manifest_version"] = json!(2);
    std::fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    assert_eq!(
        BacktestHistorySnapshot::open(&incompatible_root)
            .unwrap_err()
            .reason(),
        &BacktestHistoryFailureReason::SnapshotIncompatible
    );

    let corrupt_root = temp_dir("corrupt-manifest");
    write_snapshot(&corrupt_root, true, &[]);
    let path = current_manifest_path(&corrupt_root);
    let mut manifest: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    manifest["identity_sha256"] = Value::String(format!("sha256:{}", "f".repeat(64)));
    std::fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    assert_eq!(
        BacktestHistorySnapshot::open(&corrupt_root)
            .unwrap_err()
            .reason(),
        &BacktestHistoryFailureReason::SnapshotCorrupt
    );
}

#[tokio::test]
async fn catalog_authority_and_strict_coverage_are_typed_without_error_text_parsing() {
    let root = temp_dir("catalog-coverage");
    publish_cache_snapshot(&root, &["SHFE.au2602"], |cache_dir| {
        BacktestHistoryMetadataCache::open(cache_dir)
            .unwrap()
            .store_snapshot(support::snapshot(
                "SHFE.au2602",
                DAY_END_NS,
                vec![support::segment("SHFE.au2602", DAY_START_NS, DAY_END_NS)],
            ))
            .unwrap()
            .snapshot_hash
    });
    let snapshot = BacktestHistorySnapshot::open(&root).unwrap();

    let missing = snapshot
        .inspect(BacktestHistoryRequest::tick(
            1,
            "SHFE.au2602",
            DAY_START_NS,
            DAY_END_NS,
        ))
        .await
        .unwrap_err();
    assert_eq!(
        missing.reason(),
        &BacktestHistoryFailureReason::CoverageIncomplete {
            missing_ranges: vec![(CONCRETE_SOURCE_START_NS, DAY_END_NS)],
        }
    );

    let absent = snapshot
        .inspect(BacktestHistoryRequest::tick(
            2,
            "SHFE.ag2602",
            DAY_START_NS,
            DAY_END_NS,
        ))
        .await
        .unwrap_err();
    assert_eq!(
        absent.reason(),
        &BacktestHistoryFailureReason::SymbolNotFound
    );
}

#[tokio::test]
async fn provisional_and_incomplete_catalog_fail_closed_with_distinct_reasons() {
    let root = temp_dir("provisional");
    publish_cache_snapshot(&root, &["SHFE.au2602"], |cache_dir| {
        BacktestHistoryMetadataCache::open(cache_dir)
            .unwrap()
            .store_snapshot(support::snapshot(
                "SHFE.au2602",
                DAY_END_NS,
                vec![support::segment("SHFE.au2602", DAY_START_NS, DAY_END_NS)],
            ))
            .unwrap()
            .snapshot_hash
    });
    let snapshot = BacktestHistorySnapshot::open(&root).unwrap();
    let provisional = snapshot
        .inspect(
            BacktestHistoryRequest::tick(3, "SHFE.au2602", DAY_START_NS, DAY_END_NS)
                .with_provisional_as_of_ns(DAY_END_NS - 1),
        )
        .await
        .unwrap_err();
    assert_eq!(
        provisional.reason(),
        &BacktestHistoryFailureReason::ProvisionalData {
            as_of_ns: DAY_END_NS - 1
        }
    );

    let incomplete_root = temp_dir("incomplete-catalog");
    write_snapshot(&incomplete_root, false, &[]);
    let snapshot = BacktestHistorySnapshot::open(&incomplete_root).unwrap();
    let absent = snapshot
        .inspect(BacktestHistoryRequest::tick(4, "SHFE.au2602", 1, 2))
        .await
        .unwrap_err();
    assert_eq!(
        absent.reason(),
        &BacktestHistoryFailureReason::MetadataIncomplete
    );
}

#[test]
fn metadata_schema_and_corruption_remain_distinct_at_the_strict_seam() {
    let symbol = "KQ.m@SHFE.au";
    let incompatible_root = temp_dir("metadata-incompatible");
    publish_cache_snapshot(&incompatible_root, &[symbol], |cache_dir| {
        let metadata = BacktestHistoryMetadataCache::open(cache_dir)
            .unwrap()
            .store_snapshot(support::snapshot(
                symbol,
                2,
                vec![support::segment(symbol, 1, 2)],
            ))
            .unwrap();
        let active_path = support::metadata_symbol_dir(cache_dir, symbol).join("active.json");
        let mut active: Value =
            serde_json::from_slice(&std::fs::read(&active_path).unwrap()).unwrap();
        active["schema_version"] = json!(2);
        std::fs::write(&active_path, serde_json::to_vec(&active).unwrap()).unwrap();
        metadata.snapshot_hash
    });
    let error = BacktestHistorySnapshot::open(&incompatible_root).unwrap_err();
    assert_eq!(
        error.reason(),
        &BacktestHistoryFailureReason::SnapshotIncompatible
    );

    let corrupt_root = temp_dir("metadata-corrupt");
    publish_cache_snapshot(&corrupt_root, &[symbol], |cache_dir| {
        let metadata = BacktestHistoryMetadataCache::open(cache_dir)
            .unwrap()
            .store_snapshot(support::snapshot(
                symbol,
                2,
                vec![support::segment(symbol, 1, 2)],
            ))
            .unwrap();
        let active_path = support::metadata_symbol_dir(cache_dir, symbol).join("active.json");
        let mut active: Value =
            serde_json::from_slice(&std::fs::read(&active_path).unwrap()).unwrap();
        active["snapshot_hash"] = json!("not-a-hash");
        std::fs::write(&active_path, serde_json::to_vec(&active).unwrap()).unwrap();
        metadata.snapshot_hash
    });
    let error = BacktestHistorySnapshot::open(&corrupt_root).unwrap_err();
    assert_eq!(
        error.reason(),
        &BacktestHistoryFailureReason::SnapshotCorrupt
    );
}

#[test]
fn snapshot_clones_hold_the_generation_shared_lease() {
    let root = temp_dir("lease-pinning");
    write_snapshot(&root, true, &[]);
    let snapshot = BacktestHistorySnapshot::open(&root).unwrap();
    let clone = snapshot.clone();
    let lease_path = current_generation_path(&root).join("lease.lock");
    let exclusive = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lease_path)
        .unwrap();

    assert!(FileExt::try_lock_exclusive(&exclusive).is_err());
    drop(snapshot);
    assert!(FileExt::try_lock_exclusive(&exclusive).is_err());
    drop(clone);
    FileExt::try_lock_exclusive(&exclusive).unwrap();
    FileExt::unlock(&exclusive).unwrap();
}

const DAY_START_NS: i64 = 1_767_572_800_000_000_000;
const DAY_END_NS: i64 = 1_767_659_200_000_000_000;
const CONCRETE_SOURCE_START_NS: i64 = 1_767_348_000_000_000_000;
const MINUTE_START_NS: i64 = 1_767_572_820_000_000_000;
const MINUTE_END_NS: i64 = 1_767_659_160_000_000_000;

#[tokio::test]
async fn concrete_tick_final_empty_snapshot_inspects_as_complete() {
    let root = temp_dir("concrete-final-empty");
    let symbol = "SHFE.au2608";
    let expected_metadata_hash = publish_cache_snapshot(&root, &[symbol], |cache_dir| {
        let metadata = BacktestHistoryMetadataCache::open(cache_dir)
            .unwrap()
            .store_snapshot(support::snapshot(
                symbol,
                DAY_END_NS,
                vec![support::segment(symbol, DAY_START_NS, DAY_END_NS)],
            ))
            .unwrap();
        BacktestTickCache::open(cache_dir)
            .unwrap()
            .store_ticks(
                symbol,
                CONCRETE_SOURCE_START_NS,
                DAY_END_NS,
                std::iter::empty(),
            )
            .unwrap();
        metadata.snapshot_hash
    });

    let snapshot = BacktestHistorySnapshot::open(&root).unwrap();
    let report = snapshot
        .inspect(BacktestHistoryRequest::tick(
            10,
            symbol,
            DAY_START_NS,
            DAY_END_NS,
        ))
        .await
        .unwrap()
        .into_report();

    assert_eq!(report.rows, 0);
    assert_eq!(report.coverage.finality, BacktestHistoryFinality::Final);
    assert_eq!(report.snapshot_hash, expected_metadata_hash);
    assert!(snapshot.metadata_snapshot_hash().starts_with("sha256:"));
    assert!(!report.remote_used);

    let collected = snapshot
        .query(BacktestHistoryRequest::tick(
            13,
            symbol,
            DAY_START_NS,
            DAY_END_NS,
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(collected.rows.is_empty());
}

#[tokio::test]
async fn index_minute_final_empty_snapshot_inspects_as_complete() {
    let root = temp_dir("index-final-empty");
    let symbol = "KQ.i@SHFE.au";
    let expected_metadata_hash = publish_cache_snapshot(&root, &[symbol], |cache_dir| {
        let metadata = BacktestHistoryMetadataCache::open(cache_dir)
            .unwrap()
            .store_snapshot(support::snapshot(
                symbol,
                DAY_END_NS,
                vec![support::segment(symbol, DAY_START_NS, DAY_END_NS)],
            ))
            .unwrap();
        let cache_snapshot = MinuteKlineCacheSnapshot::new(
            metadata.schema_version,
            metadata.snapshot_hash.clone(),
            metadata.session.snapshot_hash(),
        )
        .unwrap();
        MinuteKlineCache::open(cache_dir)
            .unwrap()
            .store_final_range(symbol, MINUTE_START_NS, MINUTE_END_NS, &cache_snapshot, &[])
            .unwrap();
        metadata.snapshot_hash
    });

    let snapshot = BacktestHistorySnapshot::open(&root).unwrap();
    let report = snapshot
        .inspect(BacktestHistoryRequest::kline(
            11,
            symbol,
            Duration::from_secs(60),
            MINUTE_START_NS,
            MINUTE_END_NS,
        ))
        .await
        .unwrap()
        .into_report();

    assert_eq!(report.rows, 0);
    assert_eq!(report.coverage.finality, BacktestHistoryFinality::Final);
    assert_eq!(report.snapshot_hash, expected_metadata_hash);
    assert!(snapshot.metadata_snapshot_hash().starts_with("sha256:"));
    assert!(!report.remote_used);
}

#[tokio::test]
async fn main_tick_final_empty_snapshot_resolves_the_physical_segment() {
    let root = temp_dir("main-final-empty");
    let logical_symbol = "KQ.m@SHFE.au";
    let physical_symbol = "SHFE.au2608";
    let expected_metadata_hash = publish_cache_snapshot(&root, &[logical_symbol], |cache_dir| {
        let metadata = BacktestHistoryMetadataCache::open(cache_dir)
            .unwrap()
            .store_snapshot(support::snapshot(
                logical_symbol,
                DAY_END_NS,
                vec![support::segment(physical_symbol, DAY_START_NS, DAY_END_NS)],
            ))
            .unwrap();
        BacktestTickCache::open(cache_dir)
            .unwrap()
            .store_ticks(
                physical_symbol,
                DAY_START_NS,
                DAY_END_NS,
                std::iter::empty(),
            )
            .unwrap();
        metadata.snapshot_hash
    });

    let snapshot = BacktestHistorySnapshot::open(&root).unwrap();
    let report = snapshot
        .inspect(BacktestHistoryRequest::tick(
            12,
            logical_symbol,
            DAY_START_NS,
            DAY_END_NS,
        ))
        .await
        .unwrap()
        .into_report();

    assert_eq!(report.rows, 0);
    assert_eq!(report.coverage.finality, BacktestHistoryFinality::Final);
    assert_eq!(report.snapshot_hash, expected_metadata_hash);
    assert!(snapshot.metadata_snapshot_hash().starts_with("sha256:"));
    assert_eq!(report.physical_segments.len(), 1);
    assert_eq!(report.physical_segments[0].physical_symbol, physical_symbol);
    assert!(!report.remote_used);
}
