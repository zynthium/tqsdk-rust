#![cfg(feature = "history")]

//! Local candidate gate for the documented history/market isolation target.
//!
//! This is deliberately ignored: it needs an operator to provide two disjoint,
//! usable CPU sets for the machine under test.  It is not evidence that the
//! production same-spec gate has passed.

use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tqsdk_core::Tick;
use tqsdk_data::{
    BacktestHistoryMetadataCache, BacktestHistorySnapshotManifestBuilder, BacktestTickCache,
};

#[path = "../../tqsdk-data/tests/support/backtest_history.rs"]
mod backtest_history_support;
#[path = "../../tqsdk-core/tests/support/websocket.rs"]
mod websocket_support;

const IDENTITY_HEADER: &str = "X-Tqsdk-Relay-Identity";
const LOGICAL_SYMBOL: &str = "KQ.m@SHFE.au";
const PHYSICAL_SYMBOL: &str = "SHFE.au2608";
const MARKET_SYMBOL: &str = "SHFE.au2608";
const DAY_START_NS: i64 = 1_767_572_800_000_000_000;
const DAY_END_NS: i64 = 1_767_659_200_000_000_000;
const MARKET_FRAMES: usize = 96;
const HISTORY_CLIENTS: usize = 8;
const HISTORY_ROWS: usize = 6_000;

#[test]
#[ignore = "local candidate gate; requires explicit disjoint CPU sets and a same-spec machine"]
fn candidate_history_load_preserves_market_sequence_and_p99() {
    let cpu_sets = RequiredCpuSets::from_env();
    let baseline = run_phase("baseline", &cpu_sets, false);
    let loaded = run_phase("history-load", &cpu_sets, true);

    let allowed_delta = Duration::from_millis(1).max(baseline.p99_forward_latency / 10);
    let observed_delta = loaded
        .p99_forward_latency
        .saturating_sub(baseline.p99_forward_latency);
    let result = json!({
        "gate": "history_market_isolation_candidate",
        "status": if observed_delta <= allowed_delta { "pass" } else { "fail" },
        "scope": "local candidate only; not production same-spec certification",
        "market_frames": MARKET_FRAMES,
        "history_clients": HISTORY_CLIENTS,
        "history_rows_per_response": HISTORY_ROWS,
        "baseline_forward_latency_p99_ms": millis(baseline.p99_forward_latency),
        "history_load_forward_latency_p99_ms": millis(loaded.p99_forward_latency),
        "forward_latency_p99_delta_ms": millis(observed_delta),
        "allowed_delta_ms": millis(allowed_delta),
        "history_load_compression_success_total": loaded.compression_success_total,
        "history_load_compression_fallback_total": loaded.compression_fallback_total,
        "market_cpu_set": cpu_sets.market,
        "history_cpu_set": cpu_sets.history,
    });
    eprintln!("{result}");

    assert!(
        observed_delta <= allowed_delta,
        "candidate isolation gate exceeded p99 limit: {result}"
    );
}

#[derive(Clone)]
struct RequiredCpuSets {
    market: String,
    history: String,
}

impl RequiredCpuSets {
    fn from_env() -> Self {
        let market =
            std::env::var("TQSDK_RELAY_ISOLATION_GATE_MARKET_CPU_SET").unwrap_or_else(|_| {
                panic!(
                    "set TQSDK_RELAY_ISOLATION_GATE_MARKET_CPU_SET and \
                 TQSDK_RELAY_ISOLATION_GATE_HISTORY_CPU_SET to disjoint CPU sets; \
                 this ignored test is only a local candidate gate"
                )
            });
        let history =
            std::env::var("TQSDK_RELAY_ISOLATION_GATE_HISTORY_CPU_SET").unwrap_or_else(|_| {
                panic!(
                    "set TQSDK_RELAY_ISOLATION_GATE_MARKET_CPU_SET and \
                 TQSDK_RELAY_ISOLATION_GATE_HISTORY_CPU_SET to disjoint CPU sets; \
                 this ignored test is only a local candidate gate"
                )
            });
        assert!(
            !market.trim().is_empty(),
            "market CPU set must not be empty"
        );
        assert!(
            !history.trim().is_empty(),
            "history CPU set must not be empty"
        );
        Self { market, history }
    }
}

struct PhaseResult {
    p99_forward_latency: Duration,
    compression_success_total: Option<u64>,
    compression_fallback_total: Option<u64>,
}

fn run_phase(name: &str, cpu_sets: &RequiredCpuSets, apply_history_load: bool) -> PhaseResult {
    let history_root = temp_dir(name);
    publish_queryable_snapshot(&history_root);
    let upstream = spawn_market_upstream(MARKET_FRAMES);
    let downstream = free_loopback_addr();
    let metrics = free_loopback_addr();
    let history = free_loopback_addr();
    let mut relay = spawn_relay(
        downstream,
        metrics,
        history,
        &history_root,
        &upstream.url,
        cpu_sets,
    );

    wait_for_history_ready(metrics, &mut relay);
    upstream.wait_ready();
    let mut market = connect_market_client(downstream, &mut relay);
    upstream.start();

    let history_load = apply_history_load.then(|| {
        let path = history_query_path();
        (0..HISTORY_CLIENTS)
            .map(|_| {
                thread::spawn({
                    let path = path.clone();
                    move || request_history(history, &path)
                })
            })
            .collect::<Vec<_>>()
    });
    let forward_latencies = receive_market_sequence(&mut market, &upstream, MARKET_FRAMES);
    let gzip_count = if let Some(history_load) = history_load {
        let mut gzip_count = 0_u64;
        for request in history_load {
            let response = request.join().expect("history load thread panicked");
            assert!(
                response.status_line.starts_with("HTTP/1.1 200 OK"),
                "history load request failed: {}",
                response.status_line
            );
            match response.headers.get("content-encoding").map(String::as_str) {
                Some("gzip") => gzip_count += 1,
                None => {}
                Some(other) => panic!("unexpected history content encoding: {other}"),
            }
        }
        assert!(
            gzip_count > 0,
            "history load must exercise at least one gzip representation"
        );
        Some(gzip_count)
    } else {
        None
    };
    let (compression_success_total, compression_fallback_total) =
        gzip_count.map_or((None, None), |gzip_count| {
            let metrics = request_metrics(metrics);
            let success = metrics["history"]["compression_success_total"]
                .as_u64()
                .expect("history compression_success_total metric");
            assert_eq!(
                success, gzip_count,
                "history compression_success_total must equal received gzip responses: {metrics}"
            );
            let fallback = metrics["history"]["compression_fallback_total"]
                .as_u64()
                .expect("history compression_fallback_total metric");
            (Some(success), Some(fallback))
        });
    assert!(
        relay.try_wait().is_none(),
        "relay exited while market stream was active"
    );
    drop(market);
    upstream.join();

    PhaseResult {
        p99_forward_latency: p99_forward_latency(&forward_latencies),
        compression_success_total,
        compression_fallback_total,
    }
}

struct HistoryResponse {
    status_line: String,
    headers: BTreeMap<String, String>,
}

fn request_history(addr: SocketAddr, path: &str) -> HistoryResponse {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))
        .expect("connect history listener");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set history read timeout");
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\n{IDENTITY_HEADER}: isolation-gate\r\nAccept-Encoding: gzip\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .expect("write history request");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .expect("read history response");
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("history response headers")
        + 4;
    let status_line = String::from_utf8_lossy(&response[..header_end])
        .lines()
        .next()
        .unwrap_or_default()
        .to_string();
    let headers = String::from_utf8_lossy(&response[..header_end])
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect();
    HistoryResponse {
        status_line,
        headers,
    }
}

fn wait_for_history_ready(addr: SocketAddr, relay: &mut ChildGuard) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = relay.try_wait() {
            panic!("relay exited before history became ready: {status}");
        }
        if let Some(health) = try_request_json(addr, "/health")
            && health["history"]["ready"] == true
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "history listener did not become ready"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn request_metrics(addr: SocketAddr) -> Value {
    try_request_json(addr, "/metrics").expect("read metrics JSON")
}

fn try_request_json(addr: SocketAddr, path: &str) -> Option<Value> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(100)).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_millis(250)))
        .ok()?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    response.starts_with("HTTP/1.1 200 OK\r\n").then_some(())?;
    serde_json::from_str(response.split_once("\r\n\r\n")?.1).ok()
}

fn history_query_path() -> String {
    "/v1/history/query?symbol=KQ.m%40SHFE.au&series=tick&start=2026-01-05T00:26:40Z&end=2026-01-06T00:26:40Z&fields=time,id,last_price,volume,open_interest,tns".to_string()
}

fn publish_queryable_snapshot(root: &Path) {
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
    let rows = (0..HISTORY_ROWS).map(|index| Tick {
        id: i64::try_from(index).unwrap(),
        datetime: DAY_START_NS + i64::try_from(index).unwrap() * 1_000_000,
        last_price: 123.5 + index as f64,
        volume: i64::try_from(index).unwrap(),
        open_interest: 1_000 + i64::try_from(index).unwrap(),
        ..Tick::default()
    });
    BacktestTickCache::open(&source)
        .unwrap()
        .store_ticks(PHYSICAL_SYMBOL, DAY_START_NS, DAY_END_NS, rows)
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
    let generation = root.join("snapshots").join(snapshot_id);
    fs::create_dir_all(generation.parent().unwrap()).unwrap();
    fs::rename(staging, &generation).unwrap();
    fs::write(generation.join("lease.lock"), []).unwrap();
    fs::write(generation.join("manifest.json"), artifact.manifest_bytes()).unwrap();
    fs::write(
        root.join("CURRENT"),
        format!("{}\n", artifact.snapshot_id()),
    )
    .unwrap();
}

struct UpstreamFixture {
    url: String,
    ready: mpsc::Receiver<()>,
    start: mpsc::Sender<()>,
    sent: mpsc::Receiver<SentMarketFrame>,
    server: websocket_support::TestWebSocketServer,
}

struct SentMarketFrame {
    sequence: usize,
    sent_at: Instant,
}

impl UpstreamFixture {
    fn wait_ready(&self) {
        self.ready
            .recv_timeout(Duration::from_secs(5))
            .expect("relay did not connect to local upstream");
    }

    fn start(&self) {
        self.start.send(()).expect("signal upstream market frames");
    }

    fn join(self) {
        self.server.join();
    }

    fn next_sent(&self) -> SentMarketFrame {
        self.sent
            .recv_timeout(Duration::from_secs(5))
            .expect("upstream did not record the market frame send")
    }
}

fn spawn_market_upstream(frames: usize) -> UpstreamFixture {
    let (ready_tx, ready) = mpsc::channel();
    let (start, start_rx) = mpsc::channel();
    let (sent_tx, sent) = mpsc::channel();
    let server = websocket_support::TestWebSocketServer::spawn(move |mut socket| {
        socket
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        wait_for_upstream_peek(&mut socket);
        ready_tx.send(()).unwrap();
        start_rx.recv().unwrap();
        for sequence in 1..=frames {
            if sequence > 1 {
                wait_for_upstream_peek(&mut socket);
            }
            sent_tx
                .send(SentMarketFrame {
                    sequence,
                    sent_at: Instant::now(),
                })
                .unwrap();
            socket
                .send_text(
                    json!({
                        "aid": "rtn_data",
                        "data": [{
                            "quotes": {
                                MARKET_SYMBOL: {
                                    "datetime": (DAY_START_NS + sequence as i64).to_string(),
                                    "instrument_id": MARKET_SYMBOL,
                                    "last_price": sequence as f64,
                                    "volume": sequence as i64,
                                    "open_interest": 1_000 + sequence as i64,
                                }
                            }
                        }]
                    })
                    .to_string(),
                )
                .unwrap();
        }
        socket.send_close().unwrap();
    })
    .unwrap();
    UpstreamFixture {
        url: server.url("/market"),
        ready,
        start,
        sent,
        server,
    }
}

fn wait_for_upstream_peek(socket: &mut websocket_support::TestWebSocketConnection) {
    loop {
        let websocket_support::ClientFrame::Text(text) = socket.recv().expect("upstream frame")
        else {
            continue;
        };
        if serde_json::from_str::<Value>(&text)
            .ok()
            .is_some_and(|frame| frame["aid"] == "peek_message")
        {
            return;
        }
    }
}

fn spawn_relay(
    downstream: SocketAddr,
    metrics: SocketAddr,
    history: SocketAddr,
    history_root: &Path,
    upstream_url: &str,
    cpu_sets: &RequiredCpuSets,
) -> ChildGuard {
    let mut command = Command::new(env!("CARGO_BIN_EXE_tqsdk-relay"));
    command
        .env("TQSDK_RELAY_DOWNSTREAM_LISTEN", downstream.to_string())
        .env("TQSDK_RELAY_METRICS_LISTEN", metrics.to_string())
        .env("TQSDK_RELAY_HISTORY_LISTEN", history.to_string())
        .env("TQSDK_RELAY_HISTORY_ROOT", history_root)
        .env("TQSDK_RELAY_HISTORY_IDENTITY_HEADER", IDENTITY_HEADER)
        .env("TQSDK_RELAY_UPSTREAM_MARKET_URL", upstream_url)
        .env("TQSDK_RELAY_FUTURES_UNIVERSE", "symbol:SHFE.au2608")
        .env("TQSDK_RELAY_MARKET_CPU_SET", &cpu_sets.market)
        .env("TQSDK_RELAY_HISTORY_CPU_SET", &cpu_sets.history)
        .env_remove("TQ_AUTH_USER")
        .env_remove("TQ_AUTH_PASS");
    ChildGuard {
        child: command.spawn().unwrap(),
    }
}

fn connect_market_client(addr: SocketAddr, relay: &mut ChildGuard) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = relay.try_wait() {
            panic!("relay exited before market listener started: {status}");
        }
        match TcpStream::connect_timeout(&addr, Duration::from_millis(100)) {
            Ok(mut stream) => {
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let handshake = format!(
                    "GET /market HTTP/1.1\r\n\
Host: {addr}\r\n\
Upgrade: websocket\r\n\
Connection: Upgrade\r\n\
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
Sec-WebSocket-Version: 13\r\n\
\r\n"
                );
                stream.write_all(handshake.as_bytes()).unwrap();
                let mut response = Vec::new();
                let mut byte = [0_u8; 1];
                while !response.windows(4).any(|window| window == b"\r\n\r\n") {
                    stream.read_exact(&mut byte).unwrap();
                    response.push(byte[0]);
                }
                assert!(
                    String::from_utf8(response)
                        .unwrap()
                        .starts_with("HTTP/1.1 101"),
                    "market websocket handshake failed"
                );
                write_client_text(
                    &mut stream,
                    json!({"aid": "subscribe_quote", "ins_list": MARKET_SYMBOL}).to_string(),
                );
                return stream;
            }
            Err(_) => assert!(Instant::now() < deadline, "market listener did not start"),
        }
    }
}

fn receive_market_sequence(
    stream: &mut TcpStream,
    upstream: &UpstreamFixture,
    expected: usize,
) -> Vec<Duration> {
    let mut forward_latencies = Vec::with_capacity(expected);
    for sequence in 1..=expected {
        write_client_text(stream, json!({"aid": "peek_message"}).to_string());
        let payload = read_server_text(stream);
        let observed = payload["data"][0]["quotes"][MARKET_SYMBOL]["last_price"]
            .as_f64()
            .expect("market frame last_price");
        assert_eq!(
            observed, sequence as f64,
            "market sequence must have no loss, duplicate, or reordering"
        );
        let received_at = Instant::now();
        let sent = upstream.next_sent();
        assert_eq!(
            sent.sequence, sequence,
            "upstream send sequence must align with decoded downstream frame"
        );
        forward_latencies.push(received_at.duration_since(sent.sent_at));
    }
    forward_latencies
}

fn write_client_text(stream: &mut TcpStream, text: String) {
    let bytes = text.as_bytes();
    assert!(
        bytes.len() <= 125,
        "test websocket client only uses short frames"
    );
    let mask = [1_u8, 2, 3, 4];
    let mut frame = Vec::with_capacity(bytes.len() + 6);
    frame.push(0x81);
    frame.push(0x80 | u8::try_from(bytes.len()).unwrap());
    frame.extend_from_slice(&mask);
    frame.extend(
        bytes
            .iter()
            .enumerate()
            .map(|(index, byte)| *byte ^ mask[index % mask.len()]),
    );
    stream.write_all(&frame).unwrap();
}

fn read_server_text(stream: &mut TcpStream) -> Value {
    let mut header = [0_u8; 2];
    stream.read_exact(&mut header).unwrap();
    assert_eq!(header[0] & 0x0f, 0x1, "server must send a text frame");
    assert_eq!(header[1] & 0x80, 0, "server frames must not be masked");
    let mut len = u64::from(header[1] & 0x7f);
    if len == 126 {
        let mut extended = [0_u8; 2];
        stream.read_exact(&mut extended).unwrap();
        len = u64::from(u16::from_be_bytes(extended));
    } else if len == 127 {
        let mut extended = [0_u8; 8];
        stream.read_exact(&mut extended).unwrap();
        len = u64::from_be_bytes(extended);
    }
    let mut payload = vec![0_u8; usize::try_from(len).unwrap()];
    stream.read_exact(&mut payload).unwrap();
    serde_json::from_slice(&payload).unwrap()
}

fn p99_forward_latency(latencies: &[Duration]) -> Duration {
    assert!(!latencies.is_empty(), "need at least one market frame");
    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();
    let index = (sorted.len() * 99).div_ceil(100).saturating_sub(1);
    sorted[index]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
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
        "tqsdk-relay-history-isolation-{name}-{}-{nonce}",
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
