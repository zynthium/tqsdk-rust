#![cfg_attr(not(test), forbid(unsafe_code))]

use std::ffi::OsString;
use std::future::Future;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::timeout;
use tqsdk_core::{
    ContractError, RuntimeCommand, TradeAccountType, TradeCommand, TradeLoginCommand,
};

const DEFAULT_CLIENT_APP_ID: &str = "SHINNY_TQ_1.0";
const CLIENT_INFO_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_CLIENT_INFO_STDOUT: usize = 4 * 1024;
const MAX_CLIENT_SYSTEM_INFO_BYTES: usize = 344;
const OFFICIAL_CLIENT_INFO_SCRIPT: &str = r#"
import json
import uuid
from tqsdk_ctpse import get_system_info

mac = f"{uuid.getnode():012X}"
print(json.dumps({
    "client_mac_address": "-".join(mac[i:i + 2] for i in range(0, 12, 2)),
    "client_system_info": get_system_info(),
}, separators=(",", ":")), end="")
"#;

#[derive(Debug)]
enum ClientInfoCollectionError {
    Unavailable,
    TimedOut,
    Failed,
    InvalidOutput,
}

impl std::fmt::Display for ClientInfoCollectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Unavailable => "official tqsdk-ctpse collector is unavailable",
            Self::TimedOut => "official tqsdk-ctpse collector timed out",
            Self::Failed => "official tqsdk-ctpse collector failed",
            Self::InvalidOutput => "official tqsdk-ctpse collector returned invalid output",
        })
    }
}

#[derive(Clone, Deserialize, PartialEq, Eq)]
struct CollectedClientInfo {
    #[serde(default)]
    client_mac_address: Option<String>,
    client_system_info: String,
}

#[derive(Clone)]
struct ClientInfoConfig {
    system_info: Option<String>,
    app_id: Option<String>,
    mac_address: Option<String>,
    python: OsString,
    python_explicit: bool,
    native_helper: Option<OsString>,
    native_library: Option<OsString>,
    native_explicit: bool,
    require_system_info: bool,
}

impl ClientInfoConfig {
    fn from_environment() -> Self {
        let configured_python = non_empty_env_os("TQ_TRADE_CTPSE_PYTHON");
        let native_helper = non_empty_env_os("TQ_TRADE_CTPSE_HELPER");
        let native_library = non_empty_env_os("TQ_TRADE_CTPSE_LIBRARY");
        Self {
            system_info: non_empty_env("TQ_TRADE_CLIENT_SYSTEM_INFO"),
            app_id: non_empty_env("TQ_TRADE_CLIENT_APP_ID"),
            mac_address: non_empty_env("TQ_TRADE_CLIENT_MAC_ADDRESS"),
            python: configured_python
                .clone()
                .unwrap_or_else(|| OsString::from(default_python_executable())),
            python_explicit: configured_python.is_some(),
            native_explicit: native_helper.is_some() || native_library.is_some(),
            native_helper,
            native_library,
            require_system_info: env_flag("TQ_TRADE_REQUIRE_CLIENT_SYSTEM_INFO"),
        }
    }

    fn collector_explicit(&self) -> bool {
        self.native_explicit || self.python_explicit
    }
}

pub(crate) async fn enrich_runtime_command(
    command: RuntimeCommand,
) -> Result<RuntimeCommand, ContractError> {
    let RuntimeCommand::Trade(TradeCommand::Login(login)) = command else {
        return Ok(command);
    };
    if login.account_type != TradeAccountType::Future {
        return Ok(RuntimeCommand::Trade(TradeCommand::Login(login)));
    }

    let login = enrich_trade_login_with(
        login,
        ClientInfoConfig::from_environment(),
        collect_preferred_client_info,
    )
    .await?;
    Ok(RuntimeCommand::Trade(TradeCommand::Login(login)))
}

async fn enrich_trade_login_with<F, Fut>(
    mut login: TradeLoginCommand,
    config: ClientInfoConfig,
    collect: F,
) -> Result<TradeLoginCommand, ContractError>
where
    F: FnOnce(ClientInfoConfig) -> Fut,
    Fut: Future<Output = Result<CollectedClientInfo, ClientInfoCollectionError>>,
{
    login.client_system_info = login
        .client_system_info
        .take()
        .or(config.system_info.clone())
        .map(|value| validate_system_info(&value))
        .transpose()?;
    login.client_mac_address = login
        .client_mac_address
        .take()
        .or(config.mac_address.clone())
        .map(|value| validate_mac_address(&value))
        .transpose()?;
    login.client_app_id = login
        .client_app_id
        .take()
        .map(|value| validate_app_id(&value))
        .transpose()?;

    let mut collected_mac_address = None;
    if login.client_system_info.is_none() {
        match collect(config.clone()).await {
            Ok(collected) => {
                login.client_system_info =
                    Some(validate_system_info(&collected.client_system_info)?);
                collected_mac_address = collected
                    .client_mac_address
                    .as_deref()
                    .map(validate_mac_address)
                    .transpose()?;
            }
            Err(error) if config.require_system_info || config.collector_explicit() => {
                return Err(ContractError::auth(format!(
                    "trade client system info collection failed: {error}"
                )));
            }
            Err(_) => {}
        }
    }

    if login.client_mac_address.is_none() {
        login.client_mac_address = collected_mac_address.or_else(default_client_mac_address);
    }
    if login.client_system_info.is_some() && login.client_app_id.is_none() {
        login.client_app_id = Some(validate_app_id(
            config.app_id.as_deref().unwrap_or(DEFAULT_CLIENT_APP_ID),
        )?);
    }

    Ok(login)
}

async fn collect_preferred_client_info(
    config: ClientInfoConfig,
) -> Result<CollectedClientInfo, ClientInfoCollectionError> {
    if config.native_explicit {
        return collect_native_client_info(
            config
                .native_helper
                .or_else(adjacent_helper_executable)
                .unwrap_or_else(|| OsString::from(helper_executable_name())),
            config.native_library,
        )
        .await;
    }
    if config.python_explicit {
        return collect_official_client_info(config.python).await;
    }
    if let Some(helper) = adjacent_helper_executable() {
        if let Ok(collected) = collect_native_client_info(helper, None).await {
            return Ok(collected);
        }
    }
    collect_official_client_info(config.python).await
}

async fn collect_native_client_info(
    helper: OsString,
    library: Option<OsString>,
) -> Result<CollectedClientInfo, ClientInfoCollectionError> {
    let mut command = restricted_collector_command(helper);
    if let Some(library) = library {
        command.arg("--library").arg(library);
    }
    run_collector(command).await
}

async fn collect_official_client_info(
    python: OsString,
) -> Result<CollectedClientInfo, ClientInfoCollectionError> {
    let mut command = restricted_collector_command(python);
    command
        .arg("-I")
        .arg("-c")
        .arg(OFFICIAL_CLIENT_INFO_SCRIPT)
        .env("PYTHONIOENCODING", "utf-8")
        .env("PYTHONNOUSERSITE", "1");
    run_collector(command).await
}

fn restricted_collector_command(program: OsString) -> Command {
    let mut command = Command::new(program);
    command
        .env_clear()
        .env("TZ", "Asia/Shanghai")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    for name in [
        "PATH",
        "SYSTEMROOT",
        "WINDIR",
        "CTPSE_RUN_MODE",
        "TQSDK_CTPSE_CACHE_DIR",
        "XDG_CACHE_HOME",
        "LOCALAPPDATA",
        "USERPROFILE",
        "HOME",
    ] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    command
}

async fn run_collector(
    mut command: Command,
) -> Result<CollectedClientInfo, ClientInfoCollectionError> {
    let mut child = command
        .spawn()
        .map_err(|_| ClientInfoCollectionError::Unavailable)?;
    let stdout = child
        .stdout
        .take()
        .ok_or(ClientInfoCollectionError::Unavailable)?;
    let mut stdout = stdout.take((MAX_CLIENT_INFO_STDOUT + 1) as u64);
    let mut output = Vec::with_capacity(MAX_CLIENT_INFO_STDOUT);
    let collect = async {
        stdout
            .read_to_end(&mut output)
            .await
            .map_err(|_| ClientInfoCollectionError::InvalidOutput)?;
        if output.len() > MAX_CLIENT_INFO_STDOUT {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(ClientInfoCollectionError::InvalidOutput);
        }
        child
            .wait()
            .await
            .map_err(|_| ClientInfoCollectionError::Failed)
    };
    let status = match timeout(CLIENT_INFO_TIMEOUT, collect).await {
        Ok(result) => result?,
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(ClientInfoCollectionError::TimedOut);
        }
    };
    if !status.success() {
        return Err(ClientInfoCollectionError::Failed);
    }
    if output.is_empty() {
        return Err(ClientInfoCollectionError::InvalidOutput);
    }

    serde_json::from_slice(&output).map_err(|_| ClientInfoCollectionError::InvalidOutput)
}

fn adjacent_helper_executable() -> Option<OsString> {
    let executable = std::env::current_exe().ok()?;
    let first_directory = executable.parent()?;
    let mut directories = vec![first_directory.to_path_buf()];
    if let Some(parent) = first_directory.parent() {
        directories.push(parent.to_path_buf());
    }
    directories
        .into_iter()
        .map(|directory| directory.join(helper_executable_name()))
        .find(|candidate| candidate.is_file())
        .map(PathBuf::into_os_string)
}

const fn helper_executable_name() -> &'static str {
    if cfg!(windows) {
        "tqsdk-ctpse-helper.exe"
    } else {
        "tqsdk-ctpse-helper"
    }
}

fn validate_system_info(value: &str) -> Result<String, ContractError> {
    let value = value.trim();
    let decoded = STANDARD.decode(value).map_err(|_| {
        ContractError::validation("trade client system info must be canonical Base64")
    })?;
    if decoded.is_empty() || decoded.len() > MAX_CLIENT_SYSTEM_INFO_BYTES {
        return Err(ContractError::validation(
            "trade client system info decoded length must be between 1 and 344 bytes",
        ));
    }
    if STANDARD.encode(&decoded) != value {
        return Err(ContractError::validation(
            "trade client system info must be canonical Base64",
        ));
    }
    Ok(value.to_owned())
}

fn validate_app_id(value: &str) -> Result<String, ContractError> {
    let value = value.trim();
    if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        return Err(ContractError::validation(
            "trade client app id must contain 1 to 128 non-control bytes",
        ));
    }
    Ok(value.to_owned())
}

fn validate_mac_address(value: &str) -> Result<String, ContractError> {
    normalize_mac_address(value).ok_or_else(|| {
        ContractError::validation(
            "trade client MAC address must use six non-zero hexadecimal octets",
        )
    })
}

fn normalize_mac_address(value: &str) -> Option<String> {
    let value = value.trim().to_ascii_uppercase();
    let valid = value.len() == 17
        && value.bytes().enumerate().all(|(index, byte)| {
            if (index + 1) % 3 == 0 {
                matches!(byte, b':' | b'-')
            } else {
                byte.is_ascii_hexdigit()
            }
        });
    if !valid {
        return None;
    }
    let normalized = value.replace(':', "-");
    (normalized != "00-00-00-00-00-00").then_some(normalized)
}

fn default_client_mac_address() -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        let mut interfaces = default_route_interfaces();
        let mut fallback = std::fs::read_dir("/sys/class/net")
            .ok()?
            .filter_map(Result::ok)
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| !is_obviously_virtual_interface(name))
            .collect::<Vec<_>>();
        fallback.sort();
        interfaces.extend(fallback);
        interfaces.dedup();
        interfaces.into_iter().find_map(|interface| {
            let raw =
                std::fs::read_to_string(format!("/sys/class/net/{interface}/address")).ok()?;
            normalize_mac_address(raw.trim())
        })
    }

    #[cfg(not(target_os = "linux"))]
    {
        mac_address::get_mac_address()
            .ok()
            .flatten()
            .and_then(|address| normalize_mac_address(&address.to_string()))
    }
}

#[cfg(target_os = "linux")]
fn default_route_interfaces() -> Vec<String> {
    let mut interfaces = std::fs::read_to_string("/proc/net/route")
        .ok()
        .into_iter()
        .flat_map(|routes| {
            routes
                .lines()
                .skip(1)
                .filter_map(|line| {
                    let mut fields = line.split_whitespace();
                    let interface = fields.next()?;
                    let destination = fields.next()?;
                    let _gateway = fields.next()?;
                    let flags = u16::from_str_radix(fields.next()?, 16).ok()?;
                    (destination == "00000000" && flags & 1 != 0).then(|| interface.to_owned())
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    interfaces.sort();
    interfaces.dedup();
    interfaces
}

#[cfg(target_os = "linux")]
fn is_obviously_virtual_interface(name: &str) -> bool {
    name == "lo"
        || name.starts_with("veth")
        || name.starts_with("docker")
        || name.starts_with("br-")
        || name.starts_with("virbr")
        || name.starts_with("vboxnet")
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn non_empty_env_os(name: &str) -> Option<OsString> {
    std::env::var_os(name).filter(|value| !value.is_empty())
}

fn env_flag(name: &str) -> bool {
    non_empty_env(name).is_some_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

const fn default_python_executable() -> &'static str {
    if cfg!(windows) { "python" } else { "python3" }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use tqsdk_core::{AccountId, TradeAccountType, TradeLoginCommand};

    use super::{
        ClientInfoCollectionError, ClientInfoConfig, CollectedClientInfo, DEFAULT_CLIENT_APP_ID,
        enrich_trade_login_with,
    };

    fn login() -> TradeLoginCommand {
        TradeLoginCommand {
            account_id: AccountId::new("test-account"),
            broker_id: "test-broker".to_string(),
            password: "test-password".to_string(),
            client_mac_address: None,
            account_type: TradeAccountType::Future,
            front_broker: None,
            front_url: None,
            client_app_id: None,
            client_system_info: None,
        }
    }

    fn config(require_system_info: bool) -> ClientInfoConfig {
        ClientInfoConfig {
            system_info: None,
            app_id: None,
            mac_address: None,
            python: "unused-python".into(),
            python_explicit: false,
            native_helper: None,
            native_library: None,
            native_explicit: false,
            require_system_info,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn official_collector_enriches_future_login() {
        let enriched = enrich_trade_login_with(login(), config(false), |_| async {
            Ok(CollectedClientInfo {
                client_mac_address: Some("01:23:45:67:89:AB".to_string()),
                client_system_info: "AQID".to_string(),
            })
        })
        .await
        .unwrap();

        assert_eq!(
            enriched.client_mac_address.as_deref(),
            Some("01-23-45-67-89-AB")
        );
        assert_eq!(enriched.client_system_info.as_deref(), Some("AQID"));
        assert_eq!(
            enriched.client_app_id.as_deref(),
            Some(DEFAULT_CLIENT_APP_ID)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_helper_output_without_mac_uses_configured_mac() {
        let mut configured = config(false);
        configured.mac_address = Some("01-23-45-67-89-AB".to_string());
        configured.native_helper = Some("isolated-helper".into());
        configured.native_explicit = true;

        let enriched = enrich_trade_login_with(login(), configured, |config| async move {
            assert!(config.native_explicit);
            assert_eq!(
                config.native_helper.as_deref(),
                Some(std::ffi::OsStr::new("isolated-helper"))
            );
            Ok(CollectedClientInfo {
                client_mac_address: None,
                client_system_info: "AQID".to_string(),
            })
        })
        .await
        .expect("native helper output without a MAC is valid");

        assert_eq!(enriched.client_system_info.as_deref(), Some("AQID"));
        assert_eq!(
            enriched.client_mac_address.as_deref(),
            Some("01-23-45-67-89-AB")
        );
        assert_eq!(
            enriched.client_app_id.as_deref(),
            Some(DEFAULT_CLIENT_APP_ID)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn explicit_system_info_skips_collector() {
        let called = Arc::new(AtomicBool::new(false));
        let called_in_collector = Arc::clone(&called);
        let mut configured = config(false);
        configured.system_info = Some("AQID".to_string());
        configured.mac_address = Some("01-23-45-67-89-AB".to_string());

        let enriched = enrich_trade_login_with(login(), configured, move |_| async move {
            called_in_collector.store(true, Ordering::SeqCst);
            Err(ClientInfoCollectionError::Failed)
        })
        .await
        .unwrap();

        assert!(!called.load(Ordering::SeqCst));
        assert_eq!(enriched.client_system_info.as_deref(), Some("AQID"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn best_effort_collection_failure_omits_system_info() {
        let enriched = enrich_trade_login_with(login(), config(false), |_| async {
            Err(ClientInfoCollectionError::Unavailable)
        })
        .await
        .unwrap();

        assert!(enriched.client_system_info.is_none());
        assert!(enriched.client_app_id.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn strict_collection_failure_rejects_before_submission() {
        let error = enrich_trade_login_with(login(), config(true), |_| async {
            Err(ClientInfoCollectionError::TimedOut)
        })
        .await
        .unwrap_err();

        assert!(error.to_string().contains("collector timed out"));
        assert!(!error.to_string().contains("test-password"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn explicit_native_collection_failure_is_fail_closed() {
        let mut configured = config(false);
        configured.native_helper = Some("explicit-helper".into());
        configured.native_explicit = true;
        configured.python_explicit = true;

        let error = enrich_trade_login_with(login(), configured, |_| async {
            Err(ClientInfoCollectionError::Unavailable)
        })
        .await
        .expect_err("explicit native collector failure must not silently submit login");

        assert!(error.to_string().contains("collection failed"));
        assert!(!error.to_string().contains("test-password"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn invalid_explicit_system_info_is_rejected() {
        let mut configured = config(false);
        configured.system_info = Some("not-base64".to_string());
        let error = enrich_trade_login_with(login(), configured, |_| async {
            Err(ClientInfoCollectionError::Unavailable)
        })
        .await
        .unwrap_err();

        assert!(error.to_string().contains("canonical Base64"));
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires TQ_TRADE_CTPSE_PYTHON pointing to Python with official tqsdk-ctpse"]
    async fn official_client_info_collector_smoke() {
        let python =
            std::env::var_os("TQ_TRADE_CTPSE_PYTHON").expect("TQ_TRADE_CTPSE_PYTHON is required");
        let collected = super::collect_official_client_info(python)
            .await
            .expect("official collector should succeed");

        super::validate_system_info(&collected.client_system_info)
            .expect("official system info should be valid");
        super::validate_mac_address(
            collected
                .client_mac_address
                .as_deref()
                .expect("official Python collector returns a MAC address"),
        )
        .expect("official MAC address should be valid");
    }
}
