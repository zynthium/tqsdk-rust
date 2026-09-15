//! Bounded, process-local token reuse for short-lived server-backtest sessions.
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex;
use tokio::time::Instant;
use tqsdk_core::{AuthContext, AuthProvider, Result};

use super::TqAuthProvider;

pub(super) async fn read_backtest_response(
    response: reqwest::Response,
    context: &str,
) -> Result<serde_json::Value> {
    use tqsdk_core::ContractError;
    let status = response.status();
    if !status.is_success() {
        let message = format!("{context} failed with status {status}");
        return Err(if status.is_server_error() {
            ContractError::transport(message)
        } else {
            ContractError::auth(message)
        });
    }
    let bytes = crate::response_body::read_limited_response_bytes(
        response,
        crate::response_body::AUTH_RESPONSE_BODY_LIMIT,
        context,
        ContractError::transport,
    )
    .await?;
    serde_json::from_slice(&bytes)
        .map_err(|_| ContractError::transport("invalid token response JSON"))
}

const MAX_TOKENS: usize = 16;
const EXPIRY_MARGIN: u64 = 30;

struct CachedToken {
    provider: TqAuthProvider,
    token: String,
    expires: Instant,
}

fn cache() -> &'static Mutex<Vec<CachedToken>> {
    static CACHE: OnceLock<Mutex<Vec<CachedToken>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(Vec::new()))
}

pub(super) async fn invalidate(provider: &TqAuthProvider) {
    cache()
        .lock()
        .await
        .retain(|entry| &entry.provider != provider);
}

pub(crate) struct BacktestAuthProvider(pub(crate) TqAuthProvider, pub(crate) bool);

impl AuthProvider for BacktestAuthProvider {
    async fn authenticate(&self) -> Result<AuthContext> {
        // Serialize misses, including across separate fill clients. Cancellation
        // drops the lock; no detached authentication task or secret is logged.
        let mut entries = cache().lock().await;
        entries
            .retain(|entry| entry.expires > Instant::now() && (self.1 || entry.provider != self.0));
        if let Some(entry) = entries.iter().find(|entry| entry.provider == self.0) {
            return self.0.build_auth_context(entry.token.clone());
        }
        let token = self.0.request_access_token_with_retry(false).await?;
        let auth = self.0.build_auth_context(token.clone())?;
        if let Some(lifetime) = token_lifetime(&self.0, &token) {
            if entries.len() == MAX_TOKENS {
                entries.remove(0);
            }
            entries.push(CachedToken {
                provider: self.0.clone(),
                token,
                expires: Instant::now() + lifetime,
            });
        }
        Ok(auth)
    }
}

fn token_lifetime(provider: &TqAuthProvider, token: &str) -> Option<Duration> {
    let expiry = provider
        .decode_access_token_claims(token)
        .ok()?
        .get("exp")?
        .as_u64()?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    let seconds = expiry.checked_sub(now)?.checked_sub(EXPIRY_MARGIN)?;
    // Bound retention even for unusually long-lived server tokens.
    (seconds > 0).then(|| Duration::from_secs(seconds.min(300)))
}

#[cfg(test)]
mod tests {
    use super::super::PasswordCredentials;
    use super::*;
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    fn token(expiry: u64) -> String {
        format!(
            "e30.{}.sig",
            URL_SAFE_NO_PAD.encode(format!(r#"{{"exp":{expiry},"sub":"test"}}"#))
        )
    }

    #[test]
    fn unknown_expired_and_nearly_expired_tokens_are_not_cached() {
        let provider = TqAuthProvider::new(PasswordCredentials::new("test", "test"));
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        for value in ["invalid".into(), token(now - 1), token(now + 20)] {
            assert!(token_lifetime(&provider, &value).is_none());
        }
        assert!(token_lifetime(&provider, &token(now + 3600)).unwrap() <= Duration::from_secs(300));
    }

    #[tokio::test]
    async fn concurrent_backtest_auth_reuses_token_but_credentials_and_refresh_do_not() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = Arc::clone(&calls);
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let body = serde_json::json!({"access_token": token(expiry)}).to_string();
        let server = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = BufReader::new(socket);
                let mut body_length = 0;
                loop {
                    let mut header = String::new();
                    assert!(socket.read_line(&mut header).await.unwrap() > 0);
                    if header == "\r\n" {
                        break;
                    }
                    if let Some(length) =
                        header.to_ascii_lowercase().strip_prefix("content-length:")
                    {
                        body_length = length.trim().parse::<usize>().unwrap();
                    }
                }
                socket.read_exact(&mut vec![0; body_length]).await.unwrap();
                server_calls.fetch_add(1, Ordering::SeqCst);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let provider = TqAuthProvider::new(PasswordCredentials::new("cache-test", "pass"))
            .with_auth_url(format!("http://{address}"));
        let first = BacktestAuthProvider(provider.clone(), true);
        let second = BacktestAuthProvider(provider.clone(), true);
        let (one, two) = tokio::join!(first.authenticate(), second.authenticate());
        assert_eq!(one.unwrap().access_token(), two.unwrap().access_token());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        BacktestAuthProvider(provider.clone(), false)
            .authenticate()
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        second.authenticate().await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let mut changed = provider.clone();
        changed.credentials = PasswordCredentials::new("cache-test", "changed");
        BacktestAuthProvider(changed, true)
            .authenticate()
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        provider.authenticate().await.unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            4,
            "ordinary authentication must bypass cache"
        );
        server.abort();
    }

    #[tokio::test]
    async fn backtest_http_distinguishes_refusal_from_temporary_failure() {
        for (status, body, expected) in [
            (401, "denied", tqsdk_core::ContractErrorKind::Auth),
            (403, "denied", tqsdk_core::ContractErrorKind::Auth),
            (429, "limited", tqsdk_core::ContractErrorKind::Auth),
            (503, "unavailable", tqsdk_core::ContractErrorKind::Transport),
            (
                200,
                "invalid json",
                tqsdk_core::ContractErrorKind::Transport,
            ),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = BufReader::new(socket);
                loop {
                    let mut line = String::new();
                    assert!(socket.read_line(&mut line).await.unwrap() > 0);
                    if line == "\r\n" {
                        break;
                    }
                }
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            });
            let response = reqwest::Client::builder()
                .no_proxy()
                .build()
                .unwrap()
                .get(format!("http://{address}"))
                .send()
                .await
                .unwrap();
            let error = read_backtest_response(response, "market endpoint request")
                .await
                .unwrap_err();
            assert_eq!(error.kind(), expected);
            server.await.unwrap();
        }
    }
}
