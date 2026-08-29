#![cfg(feature = "history")]

use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::Value;

const IDENTITY_HEADER: &str = "X-Tqsdk-Relay-Identity";

#[test]
fn configured_history_listener_serves_typed_schema_without_cors() {
    let downstream = free_loopback_addr();
    let metrics = free_loopback_addr();
    let history = free_loopback_addr();
    let history_root = temp_dir("schema-root");
    fs::create_dir_all(&history_root).unwrap();

    let mut child = spawn_relay(downstream, metrics, Some((history, &history_root)));
    let response = wait_for_history_response(history, &mut child, true);
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    assert!(!response.to_ascii_lowercase().contains("access-control-"));
    let body = response.split_once("\r\n\r\n").unwrap().1;
    let schema: Value = serde_json::from_str(body).unwrap();
    assert_eq!(schema["wire_version"], "tqsdk-history-http/1");
    assert_eq!(schema["series"][0]["name"], "tick");
    assert_eq!(schema["series"][1]["name"], "kline");
    assert!(schema["series"][0]["fields"].as_array().unwrap().len() > 8);
    assert!(schema["series"][1]["fields"].as_array().unwrap().len() > 7);

    let missing_identity = wait_for_history_response(history, &mut child, false);
    assert!(
        missing_identity.starts_with("HTTP/1.1 400 Bad Request\r\n"),
        "{missing_identity}"
    );
    let body = missing_identity.split_once("\r\n\r\n").unwrap().1;
    let error: Value = serde_json::from_str(body).unwrap();
    assert_eq!(error["error"]["code"], "missing_identity");
}

#[test]
fn missing_history_configuration_leaves_listener_disabled() {
    let downstream = free_loopback_addr();
    let metrics = free_loopback_addr();
    let history = free_loopback_addr();
    let mut child = spawn_relay(downstream, metrics, None);

    wait_for_listener(downstream, &mut child);
    assert!(TcpStream::connect_timeout(&history, Duration::from_millis(100)).is_err());
}

#[test]
fn partial_history_configuration_fails_startup() {
    let output = base_command(free_loopback_addr(), free_loopback_addr())
        .env(
            "TQSDK_RELAY_HISTORY_LISTEN",
            free_loopback_addr().to_string(),
        )
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("history"),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn spawn_relay(
    downstream: SocketAddr,
    metrics: SocketAddr,
    history: Option<(SocketAddr, &PathBuf)>,
) -> ChildGuard {
    let mut command = base_command(downstream, metrics);
    if let Some((listen, root)) = history {
        command
            .env("TQSDK_RELAY_HISTORY_LISTEN", listen.to_string())
            .env("TQSDK_RELAY_HISTORY_ROOT", root)
            .env("TQSDK_RELAY_HISTORY_IDENTITY_HEADER", IDENTITY_HEADER);
    }
    ChildGuard::spawn(&mut command)
}

fn base_command(downstream: SocketAddr, metrics: SocketAddr) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_tqsdk-relay"));
    command
        .env("TQSDK_RELAY_DOWNSTREAM_LISTEN", downstream.to_string())
        .env("TQSDK_RELAY_METRICS_LISTEN", metrics.to_string())
        .env_remove("TQSDK_RELAY_FUTURES_UNIVERSE")
        .env_remove("TQSDK_RELAY_HISTORY_LISTEN")
        .env_remove("TQSDK_RELAY_HISTORY_ROOT")
        .env_remove("TQSDK_RELAY_HISTORY_IDENTITY_HEADER")
        .env_remove("TQ_AUTH_USER")
        .env_remove("TQ_AUTH_PASS");
    command
}

fn wait_for_history_response(
    addr: SocketAddr,
    child: &mut ChildGuard,
    with_identity: bool,
) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait() {
            panic!("relay binary exited before history response: {status}");
        }
        if let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(100)) {
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let identity = if with_identity {
                format!("{IDENTITY_HEADER}: test-client\r\n")
            } else {
                String::new()
            };
            let request = format!(
                "GET /v1/history/schema HTTP/1.1\r\nHost: {addr}\r\n{identity}Connection: close\r\n\r\n"
            );
            stream.write_all(request.as_bytes()).unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            return response;
        }
        assert!(Instant::now() < deadline, "history listener did not start");
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_listener(addr: SocketAddr, child: &mut ChildGuard) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait() {
            panic!("relay binary exited before opening listener: {status}");
        }
        if TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "listener did not start at {addr}"
        );
        thread::sleep(Duration::from_millis(20));
    }
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
        "tqsdk-relay-history-{name}-{}-{nonce}",
        std::process::id()
    ))
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
