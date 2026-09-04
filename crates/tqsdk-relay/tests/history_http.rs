#![cfg(feature = "history")]

use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use serde_json::Value;
use tqsdk_core::Tick;
use tqsdk_data::{
    BacktestHistoryMetadataCache, BacktestHistorySnapshotManifestBuilder, BacktestTickCache,
};

#[path = "../../tqsdk-data/tests/support/backtest_history.rs"]
mod backtest_history_support;

const IDENTITY_HEADER: &str = "X-Tqsdk-Relay-Identity";
const LOGICAL_SYMBOL: &str = "KQ.m@SHFE.au";
const PHYSICAL_SYMBOL: &str = "SHFE.au2608";
const DAY_START_NS: i64 = 1_767_572_800_000_000_000;
const DAY_END_NS: i64 = 1_767_659_200_000_000_000;
const ROW_TIMESTAMP_NS: i64 = DAY_START_NS + 1_000_000_000;
const START: &str = "2026-01-05T00:26:40Z";
const END: &str = "2026-01-06T00:26:40Z";

#[test]
fn current_snapshot_serves_strict_coverage_query_and_etag_contracts() {
    let history_root = temp_dir("current-query");
    let fixture = publish_queryable_snapshot(&history_root);
    let downstream = free_loopback_addr();
    let metrics = free_loopback_addr();
    let history = free_loopback_addr();
    let mut child = spawn_relay(downstream, metrics, history, &history_root);

    let coverage_path =
        format!("/v1/history/coverage?symbol=KQ.m%40SHFE.au&series=tick&start={START}&end={END}");
    let coverage = request(history, &mut child, &coverage_path, None);
    assert_status(&coverage, "200 OK");
    let coverage_body = json_body(&coverage);
    assert_eq!(coverage_body["snapshot_id"], fixture.snapshot_id);
    assert_eq!(coverage_body["symbol"], LOGICAL_SYMBOL);
    assert_eq!(coverage_body["series"], "tick");
    assert_eq!(coverage_body["start"], "2026-01-05T08:26:40+08:00");
    assert_eq!(coverage_body["end"], "2026-01-06T08:26:40+08:00");
    assert_eq!(coverage_body["complete"], true);
    assert_eq!(coverage_body["final"], true);

    let query_path = format!(
        "/v1/history/query?symbol=KQ.m%40SHFE.au&series=tick&start={START}&end={END}&fields=time,id,last_price,tns"
    );
    let query = request(history, &mut child, &query_path, None);
    assert_status(&query, "200 OK");
    let etag = response_header(&query, "etag").expect("query response must be cacheable");
    let query_body = json_body(&query);
    assert_eq!(query_body["snapshot_id"], fixture.snapshot_id);
    assert_eq!(
        query_body["columns"],
        serde_json::json!(["t", "id", "lp", "tns"])
    );
    let rows = query_body["rows"].as_array().expect("positional rows");
    assert_eq!(rows.len(), 1);
    let row = rows[0].as_array().expect("one positional row");
    assert_eq!(row.len(), 4);
    assert_eq!(row[0], "2026-01-05T08:26:41.000+08:00");
    assert_eq!(row[1], "7", "integer columns use wire strings");
    assert_eq!(row[2], 123.5);
    assert_eq!(
        row[3],
        ROW_TIMESTAMP_NS.to_string(),
        "tns is an integer string"
    );

    let not_modified = request(history, &mut child, &query_path, Some(&etag));
    assert_status(&not_modified, "304 Not Modified");
    assert_eq!(response_body(&not_modified), "");
    assert_eq!(response_header(&not_modified, "etag"), Some(etag));
}

#[test]
fn live_cache_observes_committed_coverage_without_publish_or_restart() {
    let cache_dir = temp_dir("live-cache-progress");
    fs::create_dir_all(&cache_dir).unwrap();
    let downstream = free_loopback_addr();
    let metrics = free_loopback_addr();
    let history = free_loopback_addr();
    let mut child = spawn_live_relay(downstream, metrics, history, &cache_dir);
    let coverage_path =
        format!("/v1/history/coverage?symbol=KQ.m%40SHFE.au&series=tick&start={START}&end={END}");

    let before_initialization = request(history, &mut child, &coverage_path, None);
    assert_status(&before_initialization, "503 Service Unavailable");
    assert_eq!(
        json_body(&before_initialization)["error"]["code"],
        "history_unavailable"
    );

    drop(
        BacktestTickCache::open(&cache_dir)
            .unwrap()
            .try_acquire_remote_fill_shared_lock()
            .unwrap(),
    );
    BacktestHistoryMetadataCache::open(&cache_dir)
        .unwrap()
        .store_snapshot(backtest_history_support::snapshot(
            LOGICAL_SYMBOL,
            DAY_END_NS,
            vec![backtest_history_support::segment(
                PHYSICAL_SYMBOL,
                DAY_START_NS,
                DAY_END_NS,
            )],
        ))
        .unwrap();

    let before = (0..100)
        .find_map(|_| {
            let response = request(history, &mut child, &coverage_path, None);
            if response.starts_with("HTTP/1.1 503 Service Unavailable") {
                std::thread::sleep(Duration::from_millis(10));
                None
            } else {
                Some(response)
            }
        })
        .expect("live cache view must become ready");
    assert_status(&before, "409 Conflict");
    assert_eq!(json_body(&before)["error"]["code"], "coverage_incomplete");

    BacktestTickCache::open(&cache_dir)
        .unwrap()
        .store_ticks(
            PHYSICAL_SYMBOL,
            DAY_START_NS,
            DAY_END_NS,
            [Tick {
                id: 7,
                datetime: ROW_TIMESTAMP_NS,
                last_price: 123.5,
                ..Tick::default()
            }],
        )
        .unwrap();

    let after = request(history, &mut child, &coverage_path, None);
    assert_status(&after, "200 OK");
    assert_eq!(json_body(&after)["snapshot_id"], "live");
    assert_eq!(json_body(&after)["source_mode"], "live-cache");
    assert_eq!(json_body(&after)["complete"], true);

    let query_path = format!(
        "/v1/history/query?symbol=KQ.m%40SHFE.au&series=tick&start={START}&end={END}&fields=time,id,last_price,tns"
    );
    let query = request(history, &mut child, &query_path, None);
    assert_status(&query, "200 OK");
    assert_eq!(json_body(&query)["snapshot_id"], "live");
    assert_eq!(json_body(&query)["source_mode"], "live-cache");
    assert_eq!(json_body(&query)["rows"].as_array().unwrap().len(), 1);

    let maintenance = BacktestTickCache::open(&cache_dir)
        .unwrap()
        .try_acquire_consistency_read_lock()
        .unwrap();
    let during_maintenance = request(history, &mut child, &coverage_path, None);
    assert_status(&during_maintenance, "503 Service Unavailable");
    drop(maintenance);

    let recovered = request(history, &mut child, &coverage_path, None);
    assert_status(&recovered, "200 OK");
}

#[test]
fn range_starting_in_future_is_rejected_before_snapshot_lookup() {
    let history_root = temp_dir("future-start");
    fs::create_dir_all(&history_root).unwrap();
    let downstream = free_loopback_addr();
    let metrics = free_loopback_addr();
    let history = free_loopback_addr();
    let mut child = spawn_relay(downstream, metrics, history, &history_root);

    for endpoint in ["coverage", "query"] {
        let path = format!(
            "/v1/history/{endpoint}?symbol=KQ.m%40SHFE.au&series=tick&start=2099-01-01T00:00:00Z&end=2099-01-02T00:00:00Z"
        );
        let response = request(history, &mut child, &path, None);

        assert_status(&response, "409 Conflict");
        let body = json_body(&response);
        assert_eq!(body["error"]["code"], "coverage_incomplete");
        assert_eq!(body["error"]["details"]["reason"], "range_starts_in_future");
        assert_eq!(body["error"]["details"]["retryable"], true);
        assert!(body["error"]["details"]["server_time"].is_string());
    }
}

#[test]
fn absent_or_invalid_current_is_history_unavailable() {
    for (name, current) in [
        ("missing-current", None),
        ("invalid-current", Some("s-missing")),
    ] {
        let history_root = temp_dir(name);
        fs::create_dir_all(&history_root).unwrap();
        if let Some(current) = current {
            fs::write(history_root.join("CURRENT"), format!("{current}\n")).unwrap();
        }
        let downstream = free_loopback_addr();
        let metrics = free_loopback_addr();
        let history = free_loopback_addr();
        let mut child = spawn_relay(downstream, metrics, history, &history_root);

        let coverage_path = format!(
            "/v1/history/coverage?symbol=KQ.m%40SHFE.au&series=tick&start={START}&end={END}"
        );
        let response = request(history, &mut child, &coverage_path, None);
        assert_status(&response, "503 Service Unavailable");
        assert_eq!(json_body(&response)["error"]["code"], "history_unavailable");
    }
}

struct SnapshotFixture {
    snapshot_id: String,
}

fn publish_queryable_snapshot(root: &Path) -> SnapshotFixture {
    let source = root.join("source-cache");
    fs::create_dir_all(&source).unwrap();
    BacktestHistoryMetadataCache::open(&source)
        .unwrap()
        .store_snapshot(backtest_history_support::snapshot(
            LOGICAL_SYMBOL,
            DAY_END_NS,
            vec![backtest_history_support::segment(
                PHYSICAL_SYMBOL,
                DAY_START_NS,
                DAY_END_NS,
            )],
        ))
        .unwrap();
    let row = Tick {
        id: 7,
        datetime: ROW_TIMESTAMP_NS,
        last_price: 123.5,
        ..Tick::default()
    };
    BacktestTickCache::open(&source)
        .unwrap()
        .store_ticks(PHYSICAL_SYMBOL, DAY_START_NS, DAY_END_NS, [row])
        .unwrap();

    let staging = root.join("staging").join("pending");
    fs::create_dir_all(&staging).unwrap();
    fs::rename(&source, staging.join("cache")).unwrap();
    let cache = staging.join("cache");
    let artifact = BacktestHistorySnapshotManifestBuilder::new(
        "2026-08-29T00:00:00Z".parse::<DateTime<Utc>>().unwrap(),
    )
    .catalog(true, [LOGICAL_SYMBOL])
    .build(&cache)
    .unwrap();
    let snapshot_id = artifact.snapshot_id().to_string();
    let generation = root.join("snapshots").join(&snapshot_id);
    fs::create_dir_all(generation.parent().unwrap()).unwrap();
    fs::rename(staging, &generation).unwrap();
    fs::write(generation.join("lease.lock"), []).unwrap();
    fs::write(generation.join("manifest.json"), artifact.manifest_bytes()).unwrap();
    fs::write(root.join("CURRENT"), format!("{snapshot_id}\n")).unwrap();
    SnapshotFixture { snapshot_id }
}

fn spawn_relay(
    downstream: SocketAddr,
    metrics: SocketAddr,
    history: SocketAddr,
    history_root: &Path,
) -> ChildGuard {
    let mut command = Command::new(env!("CARGO_BIN_EXE_tqsdk-relay"));
    command
        .env("TQSDK_RELAY_DOWNSTREAM_LISTEN", downstream.to_string())
        .env("TQSDK_RELAY_METRICS_LISTEN", metrics.to_string())
        .env("TQSDK_RELAY_HISTORY_LISTEN", history.to_string())
        .env("TQSDK_RELAY_HISTORY_ROOT", history_root)
        .env_remove("TQSDK_RELAY_HISTORY_CACHE_DIR")
        .env("TQSDK_RELAY_HISTORY_IDENTITY_HEADER", IDENTITY_HEADER)
        .env_remove("TQSDK_RELAY_FUTURES_UNIVERSE")
        .env_remove("TQ_AUTH_USER")
        .env_remove("TQ_AUTH_PASS");
    ChildGuard {
        child: command.spawn().unwrap(),
    }
}

fn spawn_live_relay(
    downstream: SocketAddr,
    metrics: SocketAddr,
    history: SocketAddr,
    cache_dir: &Path,
) -> ChildGuard {
    let mut command = Command::new(env!("CARGO_BIN_EXE_tqsdk-relay"));
    command
        .env("TQSDK_RELAY_DOWNSTREAM_LISTEN", downstream.to_string())
        .env("TQSDK_RELAY_METRICS_LISTEN", metrics.to_string())
        .env("TQSDK_RELAY_HISTORY_LISTEN", history.to_string())
        .env("TQSDK_RELAY_HISTORY_CACHE_DIR", cache_dir)
        .env_remove("TQSDK_RELAY_HISTORY_ROOT")
        .env("TQSDK_RELAY_HISTORY_IDENTITY_HEADER", IDENTITY_HEADER)
        .env_remove("TQSDK_RELAY_FUTURES_UNIVERSE")
        .env_remove("TQ_AUTH_USER")
        .env_remove("TQ_AUTH_PASS");
    ChildGuard {
        child: command.spawn().unwrap(),
    }
}

fn request(
    addr: SocketAddr,
    child: &mut ChildGuard,
    path: &str,
    if_none_match: Option<&str>,
) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait() {
            panic!("relay binary exited before history response: {status}");
        }
        if let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(100)) {
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let if_none_match = if_none_match
                .map(|value| format!("If-None-Match: {value}\r\n"))
                .unwrap_or_default();
            let wire = format!(
                "GET {path} HTTP/1.1\r\nHost: {addr}\r\n{IDENTITY_HEADER}: test-client\r\n{if_none_match}Connection: close\r\n\r\n"
            );
            stream.write_all(wire.as_bytes()).unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            return response;
        }
        assert!(Instant::now() < deadline, "history listener did not start");
        thread::sleep(Duration::from_millis(20));
    }
}

fn assert_status(response: &str, expected: &str) {
    assert!(
        response.starts_with(&format!("HTTP/1.1 {expected}\r\n")),
        "response={response}"
    );
}

fn json_body(response: &str) -> Value {
    serde_json::from_str(response_body(response)).unwrap()
}

fn response_body(response: &str) -> &str {
    response.split_once("\r\n\r\n").unwrap().1
}

fn response_header(response: &str, name: &str) -> Option<String> {
    response
        .split_once("\r\n\r\n")
        .unwrap()
        .0
        .lines()
        .skip(1)
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name)
                .then(|| value.trim().to_string())
        })
}

fn free_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn temp_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "tqsdk-relay-history-http-{name}-{}-{nonce}",
        std::process::id()
    ))
}

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn try_wait(&mut self) -> Option<std::process::ExitStatus> {
        self.child.try_wait().unwrap()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
