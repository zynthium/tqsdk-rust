use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::json;

#[path = "../../tqsdk-core/tests/support/websocket.rs"]
mod websocket_support;

fn recv_text_json(
    socket: &mut websocket_support::TestWebSocketConnection,
    expected: &str,
) -> serde_json::Value {
    let websocket_support::ClientFrame::Text(text) = socket.recv().unwrap() else {
        panic!("expected upstream {expected} text frame");
    };
    serde_json::from_str(&text).unwrap()
}

#[test]
fn relay_binary_loads_symbols_file_and_opens_downstream_listener() {
    use websocket_support::TestWebSocketServer;

    let symbols_file = temp_symbols_file("binary-smoke-symbols");
    fs::write(&symbols_file, "SHFE.au2602\n").unwrap();
    let upstream = TestWebSocketServer::spawn(|mut socket| {
        assert_eq!(socket.request().path, "/market");

        let set_chart = recv_text_json(&mut socket, "set_chart");
        assert_eq!(set_chart["aid"], "set_chart");
        assert_eq!(
            set_chart["chart_id"],
            "relay-upstream-tick-SHFE_au2602-10000"
        );
        assert_eq!(set_chart["ins_list"], "SHFE.au2602");
        assert_eq!(set_chart["duration"], 0);

        assert_eq!(
            recv_text_json(&mut socket, "peek_message"),
            json!({"aid": "peek_message"})
        );
        socket.send_close().unwrap();
    })
    .unwrap();
    let downstream_addr = free_loopback_addr();
    let metrics_addr = free_loopback_addr();
    let mut child = ChildGuard::spawn(
        Command::new(env!("CARGO_BIN_EXE_tqsdk-relay"))
            .env_remove("TQSDK_RELAY_FUTURES_SYMBOLS")
            .env("TQSDK_RELAY_FUTURES_SYMBOLS_FILE", &symbols_file)
            .env("TQSDK_RELAY_UPSTREAM_MARKET_URL", upstream.url("/market"))
            .env("TQSDK_RELAY_DOWNSTREAM_LISTEN", downstream_addr.to_string())
            .env("TQSDK_RELAY_METRICS_LISTEN", metrics_addr.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    );

    wait_for_downstream_websocket(downstream_addr, &mut child);
    upstream.join();
    fs::remove_file(symbols_file).unwrap();
}

#[test]
fn relay_binary_dry_run_prints_diagnostic_without_connecting_upstream() {
    let symbols_file = temp_symbols_file("binary-dry-run-symbols");
    fs::write(&symbols_file, "SHFE.au2602\nDCE.m2609\n").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_tqsdk-relay"))
        .env_remove("TQSDK_RELAY_FUTURES_SYMBOLS")
        .env("TQSDK_RELAY_DRY_RUN", "1")
        .env("TQSDK_RELAY_FUTURES_SYMBOLS_FILE", &symbols_file)
        .env("TQSDK_RELAY_UPSTREAM_MARKET_URL", "ws://127.0.0.1:9/market")
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "dry-run failed: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["event"], "relay_startup");
    assert_eq!(report["dry_run"], true);
    assert_eq!(report["upstream_symbols"], 2);
    assert_eq!(report["upstream_tick_view_width"], 10_000);
    assert_eq!(
        report["futures_active_contracts_per_product"],
        serde_json::Value::Null
    );
    assert_eq!(report["upstream_source"], "static-symbols");
    fs::remove_file(symbols_file).unwrap();
}

#[test]
fn relay_binary_serves_health_and_metrics_json() {
    let downstream_addr = free_loopback_addr();
    let metrics_addr = free_loopback_addr();
    let mut child = ChildGuard::spawn(
        Command::new(env!("CARGO_BIN_EXE_tqsdk-relay"))
            .env_remove("TQSDK_RELAY_FUTURES_SYMBOLS")
            .env_remove("TQSDK_RELAY_FUTURES_SYMBOLS_FILE")
            .env_remove("TQSDK_RELAY_FUTURES_PRODUCTS")
            .env("TQSDK_RELAY_DOWNSTREAM_LISTEN", downstream_addr.to_string())
            .env("TQSDK_RELAY_METRICS_LISTEN", metrics_addr.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    );

    let health = wait_for_http_json(metrics_addr, "/health", &mut child);
    assert_eq!(health["ready"], true);
    assert_eq!(health["process_started"], true);
    assert_eq!(health["downstream_listening"], true);
    assert_eq!(health["upstream_status"], "connecting");
    assert_eq!(health["upstream_stage"], "connecting");
    assert_eq!(
        health["upstream_stage_started_unix_secs"],
        serde_json::Value::Null
    );
    assert_eq!(health["upstream_connected"], false);
    assert_eq!(health["upstream_transport_connected"], false);
    assert_eq!(health["upstream_subscription_sent"], false);
    assert_eq!(health["upstream_frames_received"], 0);
    assert_eq!(health["upstream_events_decoded"], 0);
    assert_eq!(health["upstream_frame_idle_health"], "no_sample");
    assert_eq!(health["upstream_event_idle_health"], "no_sample");
    assert_eq!(health["current_decode_health"], "healthy");
    assert_eq!(health["recent_invalid_rows_1m"], 0);
    assert_eq!(
        health["last_upstream_frame_unix_secs"],
        serde_json::Value::Null
    );
    assert_eq!(health["universe_ready"], false);
    assert_eq!(health["data_fresh"], false);
    assert_eq!(health["market_data_ready"], false);

    let metrics = wait_for_http_json(metrics_addr, "/metrics", &mut child);
    assert_eq!(metrics["downstream_clients"], 0);
    assert_eq!(metrics["ticks_ingested"], 0);
    assert_eq!(metrics["upstream_stage"], "connecting");
    assert_eq!(
        metrics["upstream_stage_started_unix_secs"],
        serde_json::Value::Null
    );
    assert_eq!(metrics["upstream_transport_connected"], false);
    assert_eq!(metrics["upstream_subscription_sent"], false);
    assert_eq!(metrics["upstream_frames_received"], 0);
    assert_eq!(metrics["upstream_events_decoded"], 0);
    assert_eq!(metrics["upstream_frame_idle_health"], "no_sample");
    assert_eq!(metrics["upstream_event_idle_health"], "no_sample");
    assert_eq!(metrics["current_decode_health"], "healthy");
    assert_eq!(metrics["recent_invalid_rows_1m"], 0);
    assert_eq!(
        metrics["last_upstream_frame_unix_secs"],
        serde_json::Value::Null
    );
    assert_eq!(metrics["upstream_symbols"], 0);

    let symbol_metrics = wait_for_http_json(metrics_addr, "/symbol-metrics", &mut child);
    assert!(symbol_metrics["now_unix_millis"].is_number());
    assert_eq!(symbol_metrics["data_stale_after_millis"], 30_000);
    assert_eq!(symbol_metrics["summary"]["total"], 0);
    assert_eq!(symbol_metrics["filtered_total"], 0);
    assert_eq!(symbol_metrics["symbols"].as_array().unwrap().len(), 0);

    let dashboard = wait_for_http_json(metrics_addr, "/dashboard-snapshot", &mut child);
    assert!(dashboard["received_at_unix_millis"].is_number());
    assert_eq!(dashboard["metrics"]["upstream_stage"], "connecting");
    assert_eq!(dashboard["global"]["total"], 0);
    assert!(dashboard["timeline"]["global"]["total"].is_number());
    assert!(dashboard.get("global_symbols").is_none());
    assert_eq!(dashboard["page"]["summary"]["total"], 0);
    assert_eq!(dashboard["page"]["symbols"].as_array().unwrap().len(), 0);
}

#[test]
fn relay_binary_rejects_invalid_symbol_metrics_query() {
    let downstream_addr = free_loopback_addr();
    let metrics_addr = free_loopback_addr();
    let mut child = ChildGuard::spawn(
        Command::new(env!("CARGO_BIN_EXE_tqsdk-relay"))
            .env_remove("TQSDK_RELAY_FUTURES_SYMBOLS")
            .env_remove("TQSDK_RELAY_FUTURES_SYMBOLS_FILE")
            .env_remove("TQSDK_RELAY_FUTURES_PRODUCTS")
            .env("TQSDK_RELAY_DOWNSTREAM_LISTEN", downstream_addr.to_string())
            .env("TQSDK_RELAY_METRICS_LISTEN", metrics_addr.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    );

    let response = wait_for_http_response(metrics_addr, "/symbol-metrics?sort=bad", &mut child);
    assert!(response.starts_with("HTTP/1.1 400"));
    let (_, body) = response.split_once("\r\n\r\n").unwrap();
    let error: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(error["error"], "invalid sort");

    let dashboard_response =
        wait_for_http_response(metrics_addr, "/dashboard-snapshot?sort=bad", &mut child);
    assert!(dashboard_response.starts_with("HTTP/1.1 400"));
}

#[test]
fn relay_binary_metrics_responses_are_not_cacheable() {
    let downstream_addr = free_loopback_addr();
    let metrics_addr = free_loopback_addr();
    let mut child = ChildGuard::spawn(
        Command::new(env!("CARGO_BIN_EXE_tqsdk-relay"))
            .env_remove("TQSDK_RELAY_FUTURES_SYMBOLS")
            .env_remove("TQSDK_RELAY_FUTURES_SYMBOLS_FILE")
            .env_remove("TQSDK_RELAY_FUTURES_PRODUCTS")
            .env("TQSDK_RELAY_DOWNSTREAM_LISTEN", downstream_addr.to_string())
            .env("TQSDK_RELAY_METRICS_LISTEN", metrics_addr.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    );

    let response = wait_for_http_response(metrics_addr, "/metrics", &mut child);
    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(response.contains("Cache-Control: no-store"));
}

#[test]
fn relay_binary_serves_embedded_dashboard_assets() {
    let downstream_addr = free_loopback_addr();
    let metrics_addr = free_loopback_addr();
    let mut child = ChildGuard::spawn(
        Command::new(env!("CARGO_BIN_EXE_tqsdk-relay"))
            .env_remove("TQSDK_RELAY_FUTURES_SYMBOLS")
            .env_remove("TQSDK_RELAY_FUTURES_SYMBOLS_FILE")
            .env_remove("TQSDK_RELAY_FUTURES_PRODUCTS")
            .env("TQSDK_RELAY_DOWNSTREAM_LISTEN", downstream_addr.to_string())
            .env("TQSDK_RELAY_METRICS_LISTEN", metrics_addr.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    );

    let html = wait_for_http_response(metrics_addr, "/dashboard/", &mut child);
    assert!(html.starts_with("HTTP/1.1 200"));
    assert!(html.contains("tqsdk-relay 行情完整性监控中心"));
    assert!(html.contains("/dashboard/assets/app.js"));
    assert!(html.contains("/dashboard/assets/app.css"));

    let js = wait_for_http_response(metrics_addr, "/dashboard/assets/app.js", &mut child);
    assert!(js.starts_with("HTTP/1.1 200"));
    assert!(js.contains("Content-Type: application/javascript; charset=utf-8"));
    assert!(js.contains("Cache-Control: public, max-age=60"));
    assert!(!js.contains("Cache-Control: no-store"));
    assert!(js.contains("/dashboard-snapshot"));
    assert!(js.contains("instrument_name"));
    assert!(js.contains("closed"));
    assert!(js.contains("upstream_stage"));

    let dashboard = wait_for_http_response(metrics_addr, "/dashboard-snapshot", &mut child);
    assert!(dashboard.starts_with("HTTP/1.1 200"));
    assert!(dashboard.contains("Cache-Control: no-store"));

    let css = wait_for_http_response(metrics_addr, "/dashboard/assets/app.css", &mut child);
    assert!(css.starts_with("HTTP/1.1 200"));
    assert!(css.contains("--relay-bg"));

    let dashboard_alias = wait_for_http_response(metrics_addr, "/dashboard", &mut child);
    assert!(dashboard_alias.starts_with("HTTP/1.1 200"));

    let missing = wait_for_http_response(metrics_addr, "/dashboard/assets/missing.js", &mut child);
    assert!(missing.starts_with("HTTP/1.1 404"));
}

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn spawn(command: &mut Command) -> Self {
        Self {
            child: command.spawn().unwrap(),
        }
    }

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

fn wait_for_downstream_websocket(addr: SocketAddr, child: &mut ChildGuard) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait() {
            panic!("relay binary exited before opening downstream listener: {status}");
        }
        if let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(100)) {
            websocket_handshake(&mut stream, addr);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for relay downstream listener at {addr}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_http_json(addr: SocketAddr, path: &str, child: &mut ChildGuard) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait() {
            panic!("relay binary exited before opening metrics listener: {status}");
        }
        if let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(100)) {
            let request = format!(
                "GET {path} HTTP/1.1\r\n\
Host: {addr}\r\n\
Connection: close\r\n\
\r\n"
            );
            stream.write_all(request.as_bytes()).unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).unwrap();
            let response = String::from_utf8(response).unwrap();
            if response.starts_with("HTTP/1.1 200") {
                let (_, body) = response.split_once("\r\n\r\n").unwrap();
                return serde_json::from_str(body).unwrap();
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for relay metrics listener at {addr}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_http_response(addr: SocketAddr, path: &str, child: &mut ChildGuard) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait() {
            panic!("relay binary exited before opening metrics listener: {status}");
        }
        if let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(100)) {
            let request = format!(
                "GET {path} HTTP/1.1\r\n\
Host: {addr}\r\n\
Connection: close\r\n\
\r\n"
            );
            stream.write_all(request.as_bytes()).unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).unwrap();
            return String::from_utf8(response).unwrap();
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for relay metrics listener at {addr}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn websocket_handshake(stream: &mut TcpStream, addr: SocketAddr) {
    let request = format!(
        "GET /market HTTP/1.1\r\n\
Host: {addr}\r\n\
Upgrade: websocket\r\n\
Connection: Upgrade\r\n\
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
Sec-WebSocket-Version: 13\r\n\
\r\n"
    );
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = Vec::new();
    let mut chunk = [0_u8; 128];
    while !response.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut chunk).unwrap();
        assert!(read > 0, "relay websocket handshake ended early");
        response.extend_from_slice(&chunk[..read]);
    }
    let response = String::from_utf8(response).unwrap();
    assert!(
        response.starts_with("HTTP/1.1 101"),
        "unexpected websocket handshake response: {response}"
    );
}

fn free_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn temp_symbols_file(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "tqsdk-relay-{name}-{}-{nanos}.txt",
        std::process::id()
    ))
}
