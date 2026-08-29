//! Binary-private, CacheOnly history listener.
//!
//! This module deliberately does not receive a [`tqsdk_relay::RelayEngine`] or
//! a [`tqsdk_relay::RelayServer`].  History is a separate runtime sibling of
//! the market relay, not an alternate market-data path.

mod affinity;
mod codec;
mod http;
mod observability;
mod snapshot;

use std::fs;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use tokio::sync::oneshot;
use tqsdk_relay::{RelayError, RelayResult};

use self::affinity::{CpuAffinityConfig, HistoryAffinity};

const ENV_HISTORY_LISTEN: &str = "TQSDK_RELAY_HISTORY_LISTEN";
const ENV_HISTORY_ROOT: &str = "TQSDK_RELAY_HISTORY_ROOT";
const ENV_HISTORY_IDENTITY_HEADER: &str = "TQSDK_RELAY_HISTORY_IDENTITY_HEADER";
const DEFAULT_RUNTIME_THREADS: usize = 2;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Configuration for the standalone history listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HistoryConfig {
    listen: SocketAddr,
    root: PathBuf,
    identity_header: String,
    runtime_threads: usize,
    affinity: Option<CpuAffinityConfig>,
}

impl HistoryConfig {
    /// Reads the all-or-nothing history listener configuration from the environment.
    pub(crate) fn from_env() -> RelayResult<Option<Self>> {
        Self::from_env_values(|key| std::env::var(key).ok())
    }

    fn from_env_values(mut get: impl FnMut(&str) -> Option<String>) -> RelayResult<Option<Self>> {
        let listen = get(ENV_HISTORY_LISTEN);
        let root = get(ENV_HISTORY_ROOT);
        let identity_header = get(ENV_HISTORY_IDENTITY_HEADER);
        let affinity = CpuAffinityConfig::from_env_values(&mut get)?;

        match (listen, root, identity_header, affinity) {
            (None, None, None, None) => Ok(None),
            (Some(listen), Some(root), Some(identity_header), affinity) => Self::new_with_affinity(
                listen.parse().map_err(|error| {
                    RelayError::invalid_config(format!(
                        "{ENV_HISTORY_LISTEN} must be a socket address: {error}"
                    ))
                })?,
                PathBuf::from(root),
                identity_header,
                DEFAULT_RUNTIME_THREADS,
                affinity,
            )
            .map(Some),
            _ => Err(RelayError::invalid_config(
                "history listener requires TQSDK_RELAY_HISTORY_LISTEN, TQSDK_RELAY_HISTORY_ROOT, and TQSDK_RELAY_HISTORY_IDENTITY_HEADER together",
            )),
        }
    }

    #[cfg(test)]
    pub(crate) fn new(
        listen: SocketAddr,
        root: PathBuf,
        identity_header: String,
        runtime_threads: usize,
    ) -> RelayResult<Self> {
        Self::new_with_affinity(listen, root, identity_header, runtime_threads, None)
    }

    fn new_with_affinity(
        listen: SocketAddr,
        root: PathBuf,
        identity_header: String,
        runtime_threads: usize,
        affinity: Option<CpuAffinityConfig>,
    ) -> RelayResult<Self> {
        let config = Self {
            listen,
            root,
            identity_header,
            runtime_threads,
            affinity,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> RelayResult<()> {
        if self.root.as_os_str().is_empty() || !self.root.is_absolute() {
            return Err(RelayError::invalid_config(
                "TQSDK_RELAY_HISTORY_ROOT must be a non-empty absolute path",
            ));
        }
        validate_history_root(self.root.as_path())?;
        if !is_trusted_identity_header(&self.identity_header) {
            return Err(RelayError::invalid_config(
                "TQSDK_RELAY_HISTORY_IDENTITY_HEADER must be a non-sensitive ASCII HTTP token",
            ));
        }
        if self.runtime_threads == 0 {
            return Err(RelayError::invalid_config(
                "history runtime_threads must be greater than zero",
            ));
        }
        Ok(())
    }

    pub(crate) fn bind_market_current(&self) -> RelayResult<()> {
        if let Some(affinity) = &self.affinity {
            affinity.bind_market_current()?;
        }
        Ok(())
    }

    fn history_affinity(&self) -> Option<HistoryAffinity> {
        self.affinity
            .as_ref()
            .map(CpuAffinityConfig::history_binder)
    }
}

/// Owns the isolated history runtime and its bound listener.
#[derive(Debug)]
pub(crate) struct HistoryServiceHandle {
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
    thread: Mutex<Option<JoinHandle<()>>>,
    compression: Option<Arc<http::CompressionPool>>,
    observability: Arc<observability::HistoryObservability>,
}

#[derive(Clone, Debug)]
pub(crate) struct MetricsOverlay {
    observability: Arc<observability::HistoryObservability>,
}

impl crate::metrics_http_impl::HistoryMetrics for MetricsOverlay {
    fn health(&self) -> serde_json::Value {
        self.observability.json_snapshot()
    }
    fn metrics(&self) -> serde_json::Value {
        self.observability.json_snapshot()
    }
}

impl HistoryServiceHandle {
    pub(crate) fn metrics_overlay(&self) -> MetricsOverlay {
        MetricsOverlay {
            observability: self.observability.clone(),
        }
    }
    /// Requests a graceful stop and waits until the listener thread is gone.
    pub(crate) fn shutdown(&self) -> RelayResult<()> {
        let sender = self
            .shutdown
            .lock()
            .map_err(|_| RelayError::Internal("history shutdown lock poisoned".to_string()))?
            .take();
        if let Some(sender) = sender {
            let _ = sender.send(());
        }

        if let Some(thread) = self
            .thread
            .lock()
            .map_err(|_| RelayError::Internal("history thread lock poisoned".to_string()))?
            .take()
        {
            thread.join().map_err(|_| {
                RelayError::Internal("history runtime thread panicked during shutdown".to_string())
            })?;
        }
        if let Some(compression) = &self.compression {
            compression.shutdown()?;
        }
        Ok(())
    }
}

impl Drop for HistoryServiceHandle {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

/// Binds and starts an isolated Tokio runtime before reporting success.
pub(crate) fn spawn(config: HistoryConfig) -> RelayResult<HistoryServiceHandle> {
    config.validate()?;
    let listener = TcpListener::bind(config.listen).map_err(|error| {
        RelayError::Transport(format!(
            "history bind failed for {}: {error}",
            config.listen
        ))
    })?;
    listener.set_nonblocking(true).map_err(|error| {
        RelayError::Transport(format!("history listener setup failed: {error}"))
    })?;
    let affinity = config.history_affinity();
    let compression = affinity
        .clone()
        .map(http::CompressionPool::spawn)
        .transpose()?;
    let observability = Arc::new(observability::HistoryObservability::enabled(
        compression.is_some(),
    ));
    if let Some(compression) = &compression {
        compression.attach_observability(observability.clone());
    }
    let listener_compression = compression.clone();
    let listener_observability = observability.clone();

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (startup_tx, startup_rx) = mpsc::sync_channel(1);
    let thread_name = format!("tqsdk-history-{}", config.listen.port());
    let thread = match thread::Builder::new().name(thread_name).spawn(move || {
        run_listener_thread(
            listener,
            config,
            affinity,
            listener_compression,
            listener_observability,
            shutdown_rx,
            startup_tx,
        )
    }) {
        Ok(thread) => thread,
        Err(error) => {
            if let Some(compression) = &compression {
                let _ = compression.shutdown();
            }
            return Err(RelayError::Internal(format!(
                "history thread spawn failed: {error}"
            )));
        }
    };

    match startup_rx.recv_timeout(STARTUP_TIMEOUT) {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let _ = thread.join();
            if let Some(compression) = &compression {
                let _ = compression.shutdown();
            }
            return Err(RelayError::Internal(error));
        }
        Err(error) => {
            let _ = shutdown_tx.send(());
            let _ = thread.join();
            if let Some(compression) = &compression {
                let _ = compression.shutdown();
            }
            return Err(RelayError::Internal(format!(
                "history runtime did not become ready: {error}"
            )));
        }
    }

    Ok(HistoryServiceHandle {
        shutdown: Mutex::new(Some(shutdown_tx)),
        thread: Mutex::new(Some(thread)),
        compression,
        observability,
    })
}

fn run_listener_thread(
    listener: TcpListener,
    config: HistoryConfig,
    affinity: Option<HistoryAffinity>,
    compression: Option<Arc<http::CompressionPool>>,
    observability: Arc<observability::HistoryObservability>,
    shutdown: oneshot::Receiver<()>,
    startup: mpsc::SyncSender<Result<(), String>>,
) {
    if let Some(affinity) = &affinity {
        if let Err(error) = affinity.bind_current() {
            let _ = startup.send(Err(error.to_string()));
            return;
        }
    }
    let worker_affinity = affinity.clone();
    let expected_workers = config.runtime_threads;
    let (worker_ready_tx, worker_ready_rx) = mpsc::sync_channel(expected_workers);
    let startup_workers_remaining = Arc::new(AtomicUsize::new(expected_workers));
    let startup_workers = startup_workers_remaining.clone();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.runtime_threads)
        .enable_io()
        .enable_time()
        .thread_name("tqsdk-history-worker")
        .on_thread_start(move || {
            let result = worker_affinity
                .as_ref()
                .map_or(Ok(()), HistoryAffinity::bind_current)
                .map_err(|error| error.to_string());
            if startup_workers
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                let _ = worker_ready_tx.try_send(result);
            }
        })
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = startup.send(Err(format!("history runtime setup failed: {error}")));
            return;
        }
    };
    let runtime_observability = observability.clone();
    runtime.block_on(async move {
        for _ in 0..expected_workers {
            match worker_ready_rx.recv_timeout(STARTUP_TIMEOUT) {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    let _ = startup.send(Err(format!(
                        "history runtime worker CPU affinity failed: {error}"
                    )));
                    return;
                }
                Err(error) => {
                    let _ = startup.send(Err(format!(
                        "history runtime workers did not become ready: {error}"
                    )));
                    return;
                }
            }
        }
        let listener = match tokio::net::TcpListener::from_std(listener) {
            Ok(listener) => listener,
            Err(error) => {
                let _ = startup.send(Err(format!("history listener conversion failed: {error}")));
                return;
            }
        };
        let snapshots = Arc::new(snapshot::SnapshotSlot::new(config.root));
        snapshots.attach_observability(runtime_observability.clone());
        runtime_observability.listener_started();
        if startup.send(Ok(())).is_err() {
            return;
        }
        let initial_snapshot = snapshots.clone();
        tokio::spawn(async move {
            if let Err(error) = initial_snapshot.reload().await {
                eprintln!("history snapshot unavailable at startup: {error}");
            }
        });
        if let Err(error) = http::serve_until(
            listener,
            config.identity_header,
            snapshots,
            compression,
            runtime_observability.clone(),
            shutdown,
        )
        .await
        {
            eprintln!("history listener stopped with error: {error}");
        }
    });
    observability.listener_stopped();
}

fn validate_history_root(root: &std::path::Path) -> RelayResult<()> {
    for ancestor in root.ancestors() {
        let metadata = fs::symlink_metadata(ancestor).map_err(|error| {
            RelayError::invalid_config(format!(
                "history root ancestor {} is unavailable: {error}",
                ancestor.display()
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(RelayError::invalid_config(format!(
                "history root ancestor {} must not be a symlink",
                ancestor.display()
            )));
        }
        if ancestor == root && !metadata.is_dir() {
            return Err(RelayError::invalid_config(format!(
                "history root {} must be a regular directory",
                root.display()
            )));
        }
    }
    Ok(())
}

fn is_trusted_identity_header(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(is_http_token_byte)
        && !matches!(
            value.to_ascii_lowercase().as_str(),
            "authorization" | "proxy-authorization" | "cookie" | "set-cookie"
        )
}

const fn is_http_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::{Read, Write};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::{HistoryConfig, is_trusted_identity_header, spawn};
    use tqsdk_relay::{RelayEngine, RelayError};

    fn loopback() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
    }

    #[test]
    fn history_config_is_disabled_only_when_every_variable_is_absent() {
        assert_eq!(HistoryConfig::from_env_values(|_| None).unwrap(), None);
        let error = HistoryConfig::from_env_values(|key| {
            (key == "TQSDK_RELAY_HISTORY_LISTEN").then(|| "127.0.0.1:0".to_string())
        })
        .unwrap_err();
        assert!(matches!(error, RelayError::InvalidConfig(_)));
    }

    #[test]
    fn history_config_rejects_invalid_values() {
        let error = HistoryConfig::new(
            loopback(),
            PathBuf::from("relative"),
            "X-Trusted-Identity".to_string(),
            2,
        )
        .unwrap_err();
        assert!(matches!(error, RelayError::InvalidConfig(_)));
        assert!(!is_trusted_identity_header("Authorization"));
        assert!(!is_trusted_identity_header("x bad"));
        assert!(is_trusted_identity_header("X-Trusted-Identity"));

        let regular_file =
            std::env::temp_dir().join(format!("tqsdk-history-root-file-{}", std::process::id()));
        fs::write(&regular_file, b"not a directory").unwrap();
        let error = HistoryConfig::new(
            loopback(),
            regular_file.clone(),
            "X-Trusted-Identity".to_string(),
            2,
        )
        .unwrap_err();
        assert!(matches!(error, RelayError::InvalidConfig(_)));
        fs::remove_file(regular_file).unwrap();
    }

    #[test]
    fn service_isolated_from_relay_engine_responds_and_shuts_down() {
        let reservation = TcpListener::bind(loopback()).unwrap();
        let listen = reservation.local_addr().unwrap();
        drop(reservation);
        let root = std::env::temp_dir();
        let config = HistoryConfig::new(listen, root, "X-Trusted-Identity".to_string(), 2).unwrap();
        let handle = spawn(config).unwrap();
        let market_engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
        let _market_guard = market_engine.lock().unwrap();
        let mut stream = TcpStream::connect_timeout(&listen, Duration::from_secs(1)).unwrap();
        stream
            .write_all(
                b"GET /v1/history/schema HTTP/1.1\r\nHost: relay\r\nX-Trusted-Identity: test\r\nConnection: close\r\n\r\n",
            )
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
        handle.shutdown().unwrap();
        assert!(TcpStream::connect_timeout(&listen, Duration::from_millis(50)).is_err());
    }
}
