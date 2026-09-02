#![cfg_attr(not(test), forbid(unsafe_code))]

use std::ffi::OsStr;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};

const HTTP_NO_PROXY_ENV: &str = "TQSDK_HTTP_NO_PROXY";

const DIRECT_HTTPS_HOSTS: &[(&str, &str)] = &[
    (
        "auth.shinnytech.com",
        "TQSDK_DIRECT_RESOLVE_AUTH_SHINNYTECH_COM",
    ),
    (
        "api.shinnytech.com",
        "TQSDK_DIRECT_RESOLVE_API_SHINNYTECH_COM",
    ),
    (
        "files.shinnytech.com",
        "TQSDK_DIRECT_RESOLVE_FILES_SHINNYTECH_COM",
    ),
];

pub(crate) fn direct_reqwest_client_builder() -> reqwest::ClientBuilder {
    let mut builder = reqwest::Client::builder().http1_only();
    if force_no_proxy(std::env::var_os(HTTP_NO_PROXY_ENV).as_deref()) {
        builder = builder.no_proxy();
    }
    for (host, env_name) in DIRECT_HTTPS_HOSTS {
        if let Some(addrs) = resolve_https_host(host, env_name) {
            builder = builder.resolve_to_addrs(host, &addrs);
        }
    }
    builder
}

fn force_no_proxy(value: Option<&OsStr>) -> bool {
    value == Some(OsStr::new("1"))
}

pub(crate) fn direct_reqwest_client() -> reqwest::Client {
    direct_reqwest_client_builder()
        .build()
        .expect("direct reqwest client should build")
}

fn resolve_https_host(host: &str, env_name: &str) -> Option<Vec<SocketAddr>> {
    if let Some(addrs) = resolve_https_host_from_env(env_name) {
        return Some(addrs);
    }
    let addrs = (host, 443).to_socket_addrs().ok()?.collect::<Vec<_>>();
    (!addrs.is_empty()).then_some(addrs)
}

fn resolve_https_host_from_env(env_name: &str) -> Option<Vec<SocketAddr>> {
    let addrs = std::env::var(env_name)
        .ok()?
        .split(',')
        .filter_map(|value| value.trim().parse::<IpAddr>().ok())
        .map(|ip| SocketAddr::new(ip, 443))
        .collect::<Vec<_>>();
    (!addrs.is_empty()).then_some(addrs)
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;

    #[test]
    fn no_proxy_override_accepts_only_exact_one() {
        assert!(super::force_no_proxy(Some(OsStr::new("1"))));
        assert!(!super::force_no_proxy(None));
        assert!(!super::force_no_proxy(Some(OsStr::new("true"))));
        assert!(!super::force_no_proxy(Some(OsStr::new("0"))));
    }
}
