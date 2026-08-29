use std::fs;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;
use tqsdk_data::{BacktestHistoryMetadataCache, BacktestHistorySnapshot, BacktestTickCache};

#[path = "../../tqsdk-data/tests/support/backtest_history.rs"]
mod backtest_history_support;

const DAY_START_NS: i64 = 1_767_572_800_000_000_000;
const DAY_END_NS: i64 = 1_767_659_200_000_000_000;
const CONCRETE_SOURCE_START_NS: i64 = 1_767_348_000_000_000_000;

fn temp_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "tqsdk-cache-snapshot-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn run_json(args: &[String]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_tqsdk-cache"))
        .arg("--output-format")
        .arg("json")
        .args(args)
        .env_remove("TQ_AUTH_USER")
        .env_remove("TQ_AUTH_PASS")
        .output()
        .unwrap()
}

fn run_json_failpoint(args: &[String], failpoint: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_tqsdk-cache"))
        .arg("--output-format")
        .arg("json")
        .args(args)
        .env("TQSDK_CACHE_TEST_SNAPSHOT_FAILPOINT", failpoint)
        .env_remove("TQ_AUTH_USER")
        .env_remove("TQ_AUTH_PASS")
        .output()
        .unwrap()
}

fn result(output: &Output) -> Value {
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["schema_version"], 3);
    value["result"].clone()
}

fn tqbn_fixture(payload: &[u8]) -> Vec<u8> {
    fn fnv1a(bytes: &[u8]) -> u64 {
        bytes.iter().fold(0xcbf29ce484222325, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        })
    }

    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"TQBN");
    bytes.push(1);
    bytes.extend_from_slice(&3_u32.to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.extend_from_slice(&fnv1a(&[]).to_le_bytes());
    bytes.extend_from_slice(b"TQBB");
    bytes.push(2);
    bytes.extend_from_slice(&[0, 0, 0]);
    bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&fnv1a(payload).to_le_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

fn seed_source(root: &Path, marker: &[u8]) {
    fs::create_dir_all(root.join("series/20260829/tick")).unwrap();
    fs::create_dir_all(root.join("minute-kline-v3")).unwrap();
    fs::create_dir_all(root.join("daily-kline-v1")).unwrap();
    fs::create_dir_all(root.join("backtest-history-metadata-v1/snapshots")).unwrap();
    fs::write(
        root.join("series/20260829/tick/SHFE.au2612.tqbn"),
        tqbn_fixture(marker),
    )
    .unwrap();
    fs::write(root.join("minute-kline-v3/SHFE.au2612-202608.tqmk"), marker).unwrap();
    fs::write(root.join("daily-kline-v1/SHFE.au2612.tqdk"), marker).unwrap();
    fs::write(
        root.join("backtest-history-metadata-v1/snapshots/content.json"),
        b"{}",
    )
    .unwrap();
    fs::write(root.join("backtest-history-metadata-v1/active.json"), b"{}").unwrap();
    fs::write(root.join(".tqsdk-cache-operation.lock"), b"").unwrap();
    fs::write(root.join("series/20260829/tick/SHFE.au2612.tqbn.lock"), b"").unwrap();
}

fn seed_metadata_source(root: &Path, marker: &[u8]) {
    fs::create_dir_all(root.join("backtest-history-metadata-v1/snapshots")).unwrap();
    fs::write(
        root.join("backtest-history-metadata-v1/snapshots/content.json"),
        marker,
    )
    .unwrap();
    fs::write(root.join("backtest-history-metadata-v1/active.json"), b"{}").unwrap();
    fs::write(root.join(".tqsdk-cache-operation.lock"), b"").unwrap();
}

fn seed_queryable_tick_source(root: &Path, symbol: &str) {
    BacktestHistoryMetadataCache::open(root)
        .unwrap()
        .store_snapshot(backtest_history_support::snapshot(
            symbol,
            DAY_END_NS,
            vec![backtest_history_support::segment(
                symbol,
                DAY_START_NS,
                DAY_END_NS,
            )],
        ))
        .unwrap();
    BacktestTickCache::open(root)
        .unwrap()
        .store_ticks(
            symbol,
            CONCRETE_SOURCE_START_NS,
            DAY_END_NS,
            std::iter::empty(),
        )
        .unwrap();
}

fn clone_args(source: &Path, history: &Path, created_at: &str, command: &str) -> Vec<String> {
    vec![
        "snapshot".into(),
        "--history-root".into(),
        history.display().to_string(),
        command.into(),
        "--source-cache-dir".into(),
        source.display().to_string(),
        "--created-at".into(),
        created_at.into(),
    ]
}

#[test]
fn dry_run_is_read_only_and_reports_role_copy_policy() {
    let source = temp_dir("dry-run-source");
    let history = temp_dir("dry-run-history");
    seed_source(&source, b"one");

    let output = run_json(&clone_args(
        &source,
        &history,
        "2026-08-29T00:00:00Z",
        "dry-run",
    ));
    let result = result(&output);
    assert_eq!(result["command"], "snapshot dry-run");
    assert_eq!(result["read_only"], true);
    assert_eq!(result["roles"]["tqbn_mutable_layout"]["files"], 1);
    assert!(!history.exists());
    assert!(!source.join(".tqsdk-cache-snapshot.lock").exists());
}

#[test]
fn clone_stages_role_safe_generation_and_publish_commits_current() {
    let source = temp_dir("clone-source");
    let history = temp_dir("clone-history");
    seed_source(&source, b"one");

    let cloned = result(&run_json(&clone_args(
        &source,
        &history,
        "2026-08-29T00:00:00Z",
        "clone",
    )));
    let snapshot_id = cloned["snapshot_id"].as_str().unwrap();
    let generation = history.join("staging").join(snapshot_id);
    assert!(generation.join("manifest.json").is_file());
    assert!(generation.join("lease.lock").is_file());
    assert!(
        generation
            .join("cache/.tqsdk-cache-operation.lock")
            .is_file()
    );
    assert!(
        generation
            .join("cache/series/20260829/tick/SHFE.au2612.tqbn.lock")
            .is_file()
    );
    assert!(!history.join("CURRENT").exists());

    #[cfg(unix)]
    {
        let source_tick =
            fs::metadata(source.join("series/20260829/tick/SHFE.au2612.tqbn")).unwrap();
        let staged_tick =
            fs::metadata(generation.join("cache/series/20260829/tick/SHFE.au2612.tqbn")).unwrap();
        assert_ne!(source_tick.ino(), staged_tick.ino());

        let source_minute =
            fs::metadata(source.join("minute-kline-v3/SHFE.au2612-202608.tqmk")).unwrap();
        let staged_minute =
            fs::metadata(generation.join("cache/minute-kline-v3/SHFE.au2612-202608.tqmk")).unwrap();
        assert_eq!(source_minute.ino(), staged_minute.ino());
    }

    let rejected = run_json(&[
        "snapshot".into(),
        "--history-root".into(),
        history.display().to_string(),
        "publish".into(),
        "--snapshot-id".into(),
        snapshot_id.into(),
    ]);
    assert!(!rejected.status.success());
    assert!(!history.join("CURRENT").exists());

    let publish_source = temp_dir("publish-metadata-source");
    seed_metadata_source(&publish_source, b"{}");
    let publish_candidate = result(&run_json(&clone_args(
        &publish_source,
        &history,
        "2026-08-30T00:00:00Z",
        "clone",
    )));
    let publish_id = publish_candidate["snapshot_id"].as_str().unwrap();
    let published = result(&run_json(&[
        "snapshot".into(),
        "--history-root".into(),
        history.display().to_string(),
        "publish".into(),
        "--snapshot-id".into(),
        publish_id.into(),
    ]));
    assert_eq!(published["committed"], true);
    assert_eq!(
        fs::read_to_string(history.join("CURRENT")).unwrap(),
        format!("{publish_id}\n")
    );
    assert!(history.join("snapshots").join(publish_id).is_dir());

    let clone_retry = result(&run_json(&clone_args(
        &publish_source,
        &history,
        "2026-08-30T00:00:00Z",
        "clone",
    )));
    assert_eq!(clone_retry["snapshot_id"], publish_id);
    assert_eq!(clone_retry["idempotent"], true);
    assert_eq!(clone_retry["namespace"], "snapshots");
    assert!(!history.join("staging").join(publish_id).exists());

    let inspected = result(&run_json(&[
        "snapshot".into(),
        "--history-root".into(),
        history.display().to_string(),
        "inspect".into(),
    ]));
    assert_eq!(inspected["snapshot_id"], publish_id);
}

#[test]
fn data_generation_requires_cache_only_inspect_and_real_query_smoke_before_publish() {
    let source = temp_dir("verified-source");
    let history = temp_dir("verified-history");
    let symbol = "SHFE.au2608";
    seed_queryable_tick_source(&source, symbol);

    let mut clone = clone_args(&source, &history, "2026-08-29T00:00:00Z", "clone");
    clone.extend([
        "--catalog-complete".into(),
        "--catalog-symbol".into(),
        symbol.into(),
    ]);
    let staged = result(&run_json(&clone));
    let snapshot_id = staged["snapshot_id"].as_str().unwrap();
    let publish = vec![
        "snapshot".into(),
        "--history-root".into(),
        history.display().to_string(),
        "publish".into(),
        "--snapshot-id".into(),
        snapshot_id.into(),
    ];
    assert!(!run_json(&publish).status.success());
    assert!(history.join("staging").join(snapshot_id).is_dir());

    let requests = temp_dir("verified-requests").with_extension("json");
    fs::write(
        &requests,
        serde_json::to_vec(&serde_json::json!({
            "requests": [{
                "series": "tick",
                "request_id": 1,
                "symbol": symbol,
                "start_ns": DAY_START_NS,
                "end_ns": DAY_END_NS
            }]
        }))
        .unwrap(),
    )
    .unwrap();
    let prewarmed = result(&run_json(&[
        "snapshot".into(),
        "--history-root".into(),
        history.display().to_string(),
        "prewarm".into(),
        "--snapshot-id".into(),
        snapshot_id.into(),
        "--request-file".into(),
        requests.display().to_string(),
    ]));
    assert_eq!(prewarmed["query_smoke_verified"], true);
    assert_eq!(prewarmed["snapshot_id"], snapshot_id);
    let verified = result(&run_json(&[
        "snapshot".into(),
        "--history-root".into(),
        history.display().to_string(),
        "verify".into(),
        "--snapshot-id".into(),
        snapshot_id.into(),
        "--request-file".into(),
        requests.display().to_string(),
    ]));
    assert_eq!(verified["query_smoke_verified"], true);
    assert_eq!(
        verified["families"],
        serde_json::json!(["tqbn_mutable_layout"])
    );

    let ready = history
        .join("staging")
        .join(format!(".ready-{snapshot_id}.json"));
    assert!(ready.is_file());

    let failed = run_json_failpoint(&publish, "snapshot_rename");
    assert!(!failed.status.success());
    assert!(!history.join("CURRENT").exists());
    assert!(history.join("snapshots").join(snapshot_id).is_dir());
    assert!(ready.is_file());

    let recovery = result(&run_json(&[
        "snapshot".into(),
        "--history-root".into(),
        history.display().to_string(),
        "recover".into(),
        "--snapshot-id".into(),
        snapshot_id.into(),
    ]));
    assert!(
        recovery["staging_temp_dirs"]
            .as_array()
            .unwrap()
            .iter()
            .all(|path| path.as_str() != Some(ready.to_string_lossy().as_ref()))
    );

    fs::remove_file(&ready).unwrap();
    let reverified = result(&run_json(&[
        "snapshot".into(),
        "--history-root".into(),
        history.display().to_string(),
        "verify".into(),
        "--snapshot-id".into(),
        snapshot_id.into(),
        "--request-file".into(),
        requests.display().to_string(),
    ]));
    assert_eq!(reverified["namespace"], "snapshots");
    assert!(ready.is_file());

    let published = result(&run_json(&publish));
    assert_eq!(published["committed"], true);
    assert_eq!(
        fs::read_to_string(history.join("CURRENT")).unwrap(),
        format!("{snapshot_id}\n")
    );
    assert!(!ready.exists());

    let retried = result(&run_json(&publish));
    assert_eq!(retried["committed"], true);
    assert_eq!(retried["idempotent"], true);
}

#[test]
fn import_is_retry_idempotent_and_gc_skips_a_leased_old_generation() {
    let source = temp_dir("gc-source");
    let history = temp_dir("gc-history");
    seed_metadata_source(&source, b"one");
    let mut ids = Vec::new();

    for (day, marker) in [(26, b'a'), (27, b'b'), (28, b'c'), (29, b'd')] {
        fs::write(
            source.join("backtest-history-metadata-v1/snapshots/content.json"),
            [marker],
        )
        .unwrap();
        let created_at = format!("2026-08-{day:02}T00:00:00Z");
        let imported = result(&run_json(&clone_args(
            &source,
            &history,
            created_at.as_str(),
            "import",
        )));
        let snapshot_id = imported["snapshot_id"].as_str().unwrap().to_string();
        let retried = result(&run_json(&clone_args(
            &source,
            &history,
            created_at.as_str(),
            "import",
        )));
        assert_eq!(retried["snapshot_id"], snapshot_id);
        assert_eq!(retried["idempotent"], true);
        result(&run_json(&[
            "snapshot".into(),
            "--history-root".into(),
            history.display().to_string(),
            "publish".into(),
            "--snapshot-id".into(),
            snapshot_id.clone(),
        ]));
        ids.push(snapshot_id);
    }

    let pinned =
        BacktestHistorySnapshot::open_generation(&history, history.join("snapshots").join(&ids[0]))
            .unwrap();
    let gc = result(&run_json(&[
        "snapshot".into(),
        "--history-root".into(),
        history.display().to_string(),
        "gc".into(),
        "--retain".into(),
        "3".into(),
        "--apply".into(),
    ]));
    assert_eq!(
        gc["leased"].as_array().unwrap(),
        &[Value::String(ids[0].clone())]
    );
    assert!(history.join("snapshots").join(&ids[0]).exists());
    assert!(history.join("snapshots").join(&ids[1]).exists());
    assert!(history.join("snapshots").join(&ids[2]).exists());
    assert!(history.join("snapshots").join(&ids[3]).exists());

    drop(pinned);
    let gc = result(&run_json(&[
        "snapshot".into(),
        "--history-root".into(),
        history.display().to_string(),
        "gc".into(),
        "--retain".into(),
        "3".into(),
        "--apply".into(),
    ]));
    assert_eq!(
        gc["removed"].as_array().unwrap(),
        &[Value::String(ids[0].clone())]
    );
    assert!(!history.join("snapshots").join(&ids[0]).exists());
}

#[test]
fn publish_crash_points_preserve_or_expose_current_for_idempotent_recovery() {
    let source = temp_dir("crash-source");
    let history = temp_dir("crash-history");
    seed_metadata_source(&source, b"one");

    let first = result(&run_json(&clone_args(
        &source,
        &history,
        "2026-08-28T00:00:00Z",
        "clone",
    )));
    let first_id = first["snapshot_id"].as_str().unwrap().to_string();
    result(&run_json(&[
        "snapshot".into(),
        "--history-root".into(),
        history.display().to_string(),
        "publish".into(),
        "--snapshot-id".into(),
        first_id.clone(),
    ]));

    let replacement = source.join("backtest-history-metadata-v1/snapshots/content.next");
    fs::write(&replacement, b"two").unwrap();
    fs::rename(
        replacement,
        source.join("backtest-history-metadata-v1/snapshots/content.json"),
    )
    .unwrap();
    let second = result(&run_json(&clone_args(
        &source,
        &history,
        "2026-08-29T00:00:00Z",
        "clone",
    )));
    let second_id = second["snapshot_id"].as_str().unwrap().to_string();
    let publish = vec![
        "snapshot".into(),
        "--history-root".into(),
        history.display().to_string(),
        "publish".into(),
        "--snapshot-id".into(),
        second_id.clone(),
    ];

    let failed = run_json_failpoint(&publish, "snapshot_rename");
    assert!(!failed.status.success());
    assert_eq!(
        fs::read_to_string(history.join("CURRENT")).unwrap(),
        format!("{first_id}\n")
    );
    assert!(history.join("snapshots").join(&second_id).is_dir());

    let failed = run_json_failpoint(&publish, "current_temp_sync");
    assert!(!failed.status.success());
    assert_eq!(
        fs::read_to_string(history.join("CURRENT")).unwrap(),
        format!("{first_id}\n")
    );
    let recovery = result(&run_json(&[
        "snapshot".into(),
        "--history-root".into(),
        history.display().to_string(),
        "recover".into(),
    ]));
    assert_eq!(recovery["current_snapshot_id"], first_id);
    assert_eq!(recovery["current_temp_files"].as_array().unwrap().len(), 1);
    let applied = result(&run_json(&[
        "snapshot".into(),
        "--history-root".into(),
        history.display().to_string(),
        "recover".into(),
        "--apply".into(),
    ]));
    assert_eq!(applied["applied"], true);

    let failed = run_json_failpoint(&publish, "current_rename");
    assert!(!failed.status.success());
    assert_eq!(
        fs::read_to_string(history.join("CURRENT")).unwrap(),
        format!("{second_id}\n")
    );
    let recovered = result(&run_json(&[
        "snapshot".into(),
        "--history-root".into(),
        history.display().to_string(),
        "recover".into(),
    ]));
    assert_eq!(recovered["current_snapshot_id"], second_id);

    let failed = run_json_failpoint(&publish, "history_root_sync");
    assert!(!failed.status.success());
    assert_eq!(
        fs::read_to_string(history.join("CURRENT")).unwrap(),
        format!("{second_id}\n")
    );
    let retried = result(&run_json(&publish));
    assert_eq!(retried["committed"], true);
    assert_eq!(retried["idempotent"], true);
}

#[test]
fn gc_renames_to_tombstone_before_delete_and_recover_cleans_interruption() {
    let source = temp_dir("gc-crash-source");
    let history = temp_dir("gc-crash-history");
    seed_metadata_source(&source, b"one");
    let mut ids = Vec::new();
    for (day, marker) in [(26, b'a'), (27, b'b'), (28, b'c'), (29, b'd')] {
        fs::write(
            source.join("backtest-history-metadata-v1/snapshots/content.json"),
            [marker],
        )
        .unwrap();
        let staged = result(&run_json(&clone_args(
            &source,
            &history,
            format!("2026-08-{day:02}T00:00:00Z").as_str(),
            "import",
        )));
        let snapshot_id = staged["snapshot_id"].as_str().unwrap().to_string();
        result(&run_json(&[
            "snapshot".into(),
            "--history-root".into(),
            history.display().to_string(),
            "publish".into(),
            "--snapshot-id".into(),
            snapshot_id.clone(),
        ]));
        ids.push(snapshot_id);
    }
    let gc = vec![
        "snapshot".into(),
        "--history-root".into(),
        history.display().to_string(),
        "gc".into(),
        "--retain".into(),
        "3".into(),
        "--apply".into(),
    ];
    let failed = run_json_failpoint(&gc, "gc_rename");
    assert!(!failed.status.success());
    assert!(!history.join("snapshots").join(&ids[0]).exists());
    assert!(fs::read_dir(history.join("staging")).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".gc-")
    }));
    let current = BacktestHistorySnapshot::open(&history).unwrap();
    assert_eq!(current.snapshot_id(), ids[3]);
    drop(current);

    let recovered = result(&run_json(&[
        "snapshot".into(),
        "--history-root".into(),
        history.display().to_string(),
        "recover".into(),
        "--apply".into(),
    ]));
    assert_eq!(recovered["applied"], true);
    assert!(
        !fs::read_dir(history.join("staging"))
            .unwrap()
            .any(|entry| entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".gc-"))
    );
    let retry = result(&run_json(&gc));
    assert!(retry["removed"].as_array().unwrap().is_empty());
}
