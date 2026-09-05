#![cfg(feature = "history")]

use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
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

#[test]
fn context_query_is_advertised_and_uses_anchor_row() {
    let root = temp_dir("anchor");
    publish_snapshot(&root);
    let history = free_loopback_addr();
    let mut child = spawn_relay(history, &root);

    let schema = request(history, "/v1/history/schema", &mut child);
    assert_status(&schema, "200 OK");
    assert_eq!(
        json_body(&schema)["capabilities"]["context_query"]["path"],
        "/v1/history/context"
    );

    let context = request(
        history,
        "/v1/history/context?symbol=KQ.m%40SHFE.au&series=tick&anchor=2026-01-05T00%3A26%3A41Z&before=0&after=0&fields=time,id,last_price,tns",
        &mut child,
    );
    assert_status(&context, "200 OK");
    let body = json_body(&context);
    assert_eq!(body["context"]["anchor_index"], 0);
    assert_eq!(body["context"]["actual_before"], 0);
    assert_eq!(body["context"]["actual_after"], 0);
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);
    assert_eq!(body["rows"][0][1], "7");

    let over_limit = request(
        history,
        "/v1/history/context?symbol=KQ.m%40SHFE.au&series=tick&anchor=2026-01-05T00%3A26%3A41Z&before=50000&after=0",
        &mut child,
    );
    assert_status(&over_limit, "413 Payload Too Large");
    assert_eq!(
        json_body(&over_limit)["error"]["code"],
        "row_limit_exceeded"
    );
}

fn publish_snapshot(root: &Path) {
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
    BacktestTickCache::open(&source)
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
}

fn spawn_relay(history: SocketAddr, root: &Path) -> ChildGuard {
    let mut command = Command::new(env!("CARGO_BIN_EXE_tqsdk-relay"));
    command
        .env(
            "TQSDK_RELAY_DOWNSTREAM_LISTEN",
            free_loopback_addr().to_string(),
        )
        .env(
            "TQSDK_RELAY_METRICS_LISTEN",
            free_loopback_addr().to_string(),
        )
        .env("TQSDK_RELAY_HISTORY_LISTEN", history.to_string())
        .env("TQSDK_RELAY_HISTORY_ROOT", root)
        .env_remove("TQSDK_RELAY_HISTORY_CACHE_DIR")
        .env("TQSDK_RELAY_HISTORY_IDENTITY_HEADER", IDENTITY_HEADER)
        .env_remove("TQSDK_RELAY_FUTURES_UNIVERSE")
        .env_remove("TQ_AUTH_USER")
        .env_remove("TQ_AUTH_PASS");
    ChildGuard {
        child: command.spawn().unwrap(),
    }
}

fn request(addr: SocketAddr, path: &str, child: &mut ChildGuard) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.child.try_wait().unwrap() {
            panic!("relay exited before request: {status}");
        }
        if let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(100)) {
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let wire = format!(
                "GET {path} HTTP/1.1\r\nHost: {addr}\r\n{IDENTITY_HEADER}: test-client\r\nConnection: close\r\n\r\n"
            );
            stream.write_all(wire.as_bytes()).unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            return response;
        }
        assert!(Instant::now() < deadline, "relay did not start");
        thread::sleep(Duration::from_millis(20));
    }
}

fn assert_status(response: &str, expected: &str) {
    assert!(
        response.starts_with(&format!("HTTP/1.1 {expected}")),
        "{response}"
    );
}

fn json_body(response: &str) -> Value {
    serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1).unwrap()
}

fn free_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "tqsdk-relay-history-context-http-{name}-{}-{nonce}",
        std::process::id()
    ))
}

struct ChildGuard {
    child: Child,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
