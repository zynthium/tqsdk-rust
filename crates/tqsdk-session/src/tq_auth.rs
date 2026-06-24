use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::response_body::{
    AUTH_RESPONSE_BODY_LIMIT, read_limited_response_bytes, response_body_preview,
};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderMap, HeaderValue, USER_AGENT};
use serde_json::Value;

use crate::tqkq::TqKqAccountConfig;
use tqsdk_core::{
    AuthContext, AuthDerivedTradeTarget, AuthId, AuthProvider, ContractError, ProtocolDomain,
    ReplaySessionId, Result, SessionConfig, SessionRoute, SessionRouteEndpoint, SessionTarget,
    SessionTopology, SessionTopologyResolver, WebSocketConnectOptions,
};

const DEFAULT_AUTH_URL: &str = "https://auth.shinnytech.com";
const DEFAULT_NAME_SERVICE_URL: &str = "https://api.shinnytech.com/ns";
const DEFAULT_BROKER_BASE_URL: &str = "https://files.shinnytech.com";
const DEFAULT_USER_AGENT: &str = "tqsdk-python 3.8.1";
// These are ShinnyTech's public OAuth2 client identifiers, not user
// credentials. User passwords and access tokens still come from the runtime
// authentication flow; if the platform rotates this public client, a builder
// injection point can be considered in a separate API design.
const CLIENT_ID: &str = "shinny_tq";
const CLIENT_SECRET: &str = "be30b9f4-6862-488a-99ad-21bde0400081";

fn add_route_domain(topology: &mut SessionTopology, label: &str, domain: ProtocolDomain) -> bool {
    let Some(route) = topology
        .routes
        .iter_mut()
        .find(|route| route.label == label)
    else {
        return false;
    };
    if !route.domains.contains(&domain) {
        route.domains.push(domain);
    }
    true
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct PasswordCredentials {
    username: String,
    password: String,
}

impl std::fmt::Debug for PasswordCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PasswordCredentials")
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

impl PasswordCredentials {
    pub(crate) fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TqAuthProvider {
    credentials: PasswordCredentials,
    auth_url: String,
    name_service_url: String,
    broker_base_url: String,
}

impl TqAuthProvider {
    pub(crate) fn new(credentials: PasswordCredentials) -> Self {
        Self {
            credentials,
            auth_url: DEFAULT_AUTH_URL.to_string(),
            name_service_url: DEFAULT_NAME_SERVICE_URL.to_string(),
            broker_base_url: DEFAULT_BROKER_BASE_URL.to_string(),
        }
    }

    pub(crate) fn with_auth_url(mut self, auth_url: impl Into<String>) -> Self {
        self.auth_url = auth_url.into();
        self
    }

    fn token_url(&self) -> String {
        format!(
            "{}/auth/realms/shinnytech/protocol/openid-connect/token",
            self.auth_url.trim_end_matches('/')
        )
    }

    fn build_http_client(&self, default_headers: Option<HeaderMap>) -> Result<reqwest::Client> {
        let mut builder = reqwest::Client::builder()
            .gzip(true)
            .brotli(true)
            .timeout(Duration::from_secs(30));

        if let Some(headers) = default_headers {
            builder = builder.default_headers(headers);
        }

        builder
            .build()
            .map_err(|err| ContractError::auth(format!("failed to build auth client: {err}")))
    }

    async fn run_http<F, T>(&self, future: F) -> Result<T>
    where
        F: Future<Output = Result<T>> + Send,
        T: Send,
    {
        require_tokio_runtime()?;
        future.await
    }

    async fn read_json_response(
        &self,
        response: reqwest::Response,
        context: &str,
    ) -> Result<Value> {
        let status = response.status();
        let buffer = read_limited_response_bytes(
            response,
            AUTH_RESPONSE_BODY_LIMIT,
            context,
            ContractError::auth,
        )
        .await?;

        if !status.is_success() {
            let body = response_body_preview(&buffer);
            return Err(ContractError::auth(format!(
                "{context} failed with status {status}: {body}"
            )));
        }

        serde_json::from_slice(&buffer)
            .map_err(|err| ContractError::auth(format!("{context}: invalid json response: {err}")))
    }

    async fn request_access_token(&self) -> Result<String> {
        self.run_http(async {
            let client = self.build_http_client(None)?;
            let response = client
                .post(self.token_url())
                .form(&[
                    ("client_id", CLIENT_ID),
                    ("client_secret", CLIENT_SECRET),
                    ("grant_type", "password"),
                    ("username", self.credentials.username.as_str()),
                    ("password", self.credentials.password.as_str()),
                ])
                .header(USER_AGENT, DEFAULT_USER_AGENT)
                .header(ACCEPT, "application/json")
                .send()
                .await
                .map_err(|err| ContractError::auth(format!("token request failed: {err}")))?;
            let payload = self.read_json_response(response, "token request").await?;
            let access_token = payload
                .get("access_token")
                .and_then(Value::as_str)
                .ok_or_else(|| ContractError::auth("auth response missing access_token"))?;

            Ok(access_token.to_string())
        })
        .await
    }

    pub(crate) fn auth_headers(&self, auth: &AuthContext) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        headers.insert(USER_AGENT, HeaderValue::from_static(DEFAULT_USER_AGENT));
        let authz = HeaderValue::from_str(&format!("Bearer {}", auth.access_token()))
            .map_err(|err| ContractError::auth(format!("invalid authorization header: {err}")))?;
        headers.insert(AUTHORIZATION, authz);
        Ok(headers)
    }

    fn build_auth_context(&self, access_token: String) -> Result<AuthContext> {
        let claims = self.decode_access_token_claims(&access_token)?;
        let mut auth = AuthContext::new(access_token);

        if let Some(auth_id) = claims.get("sub").and_then(Value::as_str) {
            auth = auth.with_auth_id(AuthId::new(auth_id));
        }

        if let Some(features) = claims
            .get("grants")
            .and_then(|grants| grants.get("features"))
            .and_then(Value::as_array)
        {
            for feature in features.iter().filter_map(Value::as_str) {
                auth = auth.with_feature(feature.to_string());
            }
        }

        Ok(auth)
    }

    fn decode_access_token_claims(&self, access_token: &str) -> Result<Value> {
        let payload = access_token
            .split('.')
            .nth(1)
            .ok_or_else(|| ContractError::auth("access token is not a jwt"))?;
        let decoded = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|err| ContractError::auth(format!("failed to decode token payload: {err}")))?;

        serde_json::from_slice(&decoded)
            .map_err(|err| ContractError::auth(format!("invalid token claims json: {err}")))
    }

    async fn request_market_url(
        &self,
        auth: &AuthContext,
        stock: bool,
        backtest: bool,
    ) -> Result<String> {
        self.run_http(async {
            let client = self.build_http_client(Some(self.auth_headers(auth)?))?;
            let response = client
                .get(&self.name_service_url)
                .query(&[
                    ("stock", stock.to_string()),
                    ("backtest", backtest.to_string()),
                ])
                .send()
                .await
                .map_err(|err| {
                    ContractError::auth(format!("market endpoint request failed: {err}"))
                })?;
            let payload = self
                .read_json_response(response, "market endpoint request")
                .await?;
            let md_url = payload
                .get("mdurl")
                .and_then(Value::as_str)
                .ok_or_else(|| ContractError::auth("market endpoint response missing mdurl"))?;

            if md_url.trim().is_empty() {
                return Err(ContractError::auth("market endpoint returned empty mdurl"));
            }

            Ok(md_url.to_string())
        })
        .await
    }

    async fn request_trade_broker(
        &self,
        auth: &AuthContext,
        broker_id: &str,
        account_id: &str,
    ) -> Result<String> {
        self.run_http(async {
            let client = self.build_http_client(Some(self.auth_headers(auth)?))?;
            let broker_url = format!(
                "{}/{}.json",
                self.broker_base_url.trim_end_matches('/'),
                broker_id
            );
            let response = client
                .get(&broker_url)
                .query(&[
                    ("account_id", account_id),
                    ("auth", self.credentials.username.as_str()),
                ])
                .send()
                .await
                .map_err(|err| {
                    ContractError::auth(format!("trade broker request failed: {err}"))
                })?;
            let payload = self
                .read_json_response(response, "trade broker request")
                .await?;
            let broker = payload.get(broker_id).ok_or_else(|| {
                ContractError::auth(format!("trade broker response missing {broker_id}"))
            })?;
            let category = broker
                .get("category")
                .and_then(Value::as_array)
                .ok_or_else(|| ContractError::auth("trade broker response missing category"))?
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect::<Vec<_>>();

            if !category.iter().any(|entry| entry == "TQ") {
                return Err(ContractError::auth(format!(
                    "broker {broker_id} does not support TQ login"
                )));
            }

            let url = broker
                .get("url")
                .and_then(Value::as_str)
                .ok_or_else(|| ContractError::auth("trade broker response missing url"))?;

            Ok(url.to_string())
        })
        .await
    }
}

fn require_tokio_runtime() -> Result<()> {
    tokio::runtime::Handle::try_current().map_err(|_| {
        ContractError::validation("tq auth provider requires an active Tokio runtime")
    })?;
    Ok(())
}

impl AuthProvider for TqAuthProvider {
    async fn authenticate(&self) -> Result<AuthContext> {
        let access_token = self.request_access_token().await?;
        self.build_auth_context(access_token)
    }
}

impl SessionTopologyResolver for TqAuthProvider {
    fn resolve_topology<'a>(
        &'a self,
        auth: &'a AuthContext,
        config: &'a SessionConfig,
        enabled_domains: &'a [ProtocolDomain],
    ) -> Pin<Box<dyn Future<Output = Result<SessionTopology>> + Send + 'a>> {
        Box::pin(async move {
            let mut topology = SessionTopology::default();
            let connect = WebSocketConnectOptions::default()
                .with_header("Authorization", format!("Bearer {}", auth.access_token()))
                .with_header("Accept", "application/json")
                .with_header("User-Agent", DEFAULT_USER_AGENT);

            let mut market_url = None;
            let mut market_domains = Vec::new();
            for domain in enabled_domains
                .iter()
                .copied()
                .filter(|domain| matches!(domain, ProtocolDomain::Market))
            {
                if !market_domains.contains(&domain) {
                    market_domains.push(domain);
                }
            }
            if !market_domains.is_empty() {
                let resolved_market_url = if let Some(url) = &config.endpoints.market_url {
                    url.clone()
                } else {
                    self.request_market_url(
                        auth,
                        config.market_target.stock,
                        config.market_target.backtest,
                    )
                    .await?
                };
                market_url = Some(resolved_market_url.clone());

                topology = topology.with_route(SessionRoute {
                    label: "market".to_string(),
                    target: SessionTarget::Shared,
                    domains: market_domains,
                    endpoint: SessionRouteEndpoint::WebSocket {
                        url: resolved_market_url,
                        connect: connect.clone(),
                    },
                });
            }

            if enabled_domains.contains(&ProtocolDomain::Trade) {
                if config.trade_targets.is_empty() {
                    return Err(ContractError::validation(
                        "trade domain requires at least one trade target for topology resolution",
                    ));
                }

                for target in &config.trade_targets {
                    let resolved_account_id = if let Some(auth_derived) = target.auth_derived {
                        let auth_id = auth
                            .auth_id()
                            .ok_or_else(|| {
                                ContractError::auth(
                                    "auth response missing auth_id for auth-derived trade target",
                                )
                            })?
                            .as_str();
                        match auth_derived {
                            AuthDerivedTradeTarget::TqKqFuture { number } => {
                                if let Some(number) = number {
                                    TqKqAccountConfig::future_numbered(auth_id, number)?
                                        .account_id()
                                        .clone()
                                } else {
                                    TqKqAccountConfig::future(auth_id).account_id().clone()
                                }
                            }
                            AuthDerivedTradeTarget::TqKqStock { number } => {
                                if let Some(number) = number {
                                    TqKqAccountConfig::stock_numbered(auth_id, number)?
                                        .account_id()
                                        .clone()
                                } else {
                                    TqKqAccountConfig::stock(auth_id).account_id().clone()
                                }
                            }
                        }
                    } else {
                        target.account_id.clone()
                    };
                    let trade_url = if let Some(url) = &target.trade_url {
                        url.clone()
                    } else if let Some(url) = &config.endpoints.trade_url {
                        url.clone()
                    } else {
                        self.request_trade_broker(
                            auth,
                            &target.broker_id,
                            resolved_account_id.as_str(),
                        )
                        .await?
                    };

                    topology = topology.with_route(SessionRoute {
                        label: format!("trade:{}", resolved_account_id.as_str()),
                        target: SessionTarget::Account(resolved_account_id),
                        domains: vec![ProtocolDomain::Trade],
                        endpoint: SessionRouteEndpoint::WebSocket {
                            url: trade_url,
                            connect: connect.clone(),
                        },
                    });
                }
            }

            if enabled_domains.contains(&ProtocolDomain::Query) {
                if let Some(query_url) = config.endpoints.query_url.clone() {
                    topology = topology.with_route(SessionRoute {
                        label: "query".to_string(),
                        target: SessionTarget::Shared,
                        domains: vec![ProtocolDomain::Query],
                        endpoint: SessionRouteEndpoint::Http { url: query_url },
                    });
                } else if enabled_domains.contains(&ProtocolDomain::Market)
                    && add_route_domain(&mut topology, "market", ProtocolDomain::Query)
                {
                } else {
                    let query_market_url = if let Some(url) = market_url.clone() {
                        url
                    } else if let Some(url) = &config.endpoints.market_url {
                        url.clone()
                    } else {
                        self.request_market_url(
                            auth,
                            config.market_target.stock,
                            config.market_target.backtest,
                        )
                        .await?
                    };
                    topology = topology.with_route(SessionRoute {
                        label: "query".to_string(),
                        target: SessionTarget::Shared,
                        domains: vec![ProtocolDomain::Query],
                        endpoint: SessionRouteEndpoint::WebSocket {
                            url: query_market_url,
                            connect: connect.clone(),
                        },
                    });
                }
            }

            if enabled_domains.contains(&ProtocolDomain::Schema) {
                let Some(schema_url) = config.endpoints.schema_url.clone() else {
                    return Err(ContractError::validation(
                        "schema domain requires endpoints.schema_url for topology resolution",
                    ));
                };
                topology = topology.with_route(SessionRoute {
                    label: "schema".to_string(),
                    target: SessionTarget::Shared,
                    domains: vec![ProtocolDomain::Schema],
                    endpoint: SessionRouteEndpoint::Http { url: schema_url },
                });
            }

            if enabled_domains.contains(&ProtocolDomain::Replay) {
                let Some(replay_label) = config.endpoints.replay_url.clone() else {
                    return Err(ContractError::validation(
                        "replay domain requires endpoints.replay_url for topology resolution",
                    ));
                };
                topology = topology.with_route(SessionRoute {
                    label: "replay".to_string(),
                    target: SessionTarget::Replay(ReplaySessionId::new(replay_label.clone())),
                    domains: vec![ProtocolDomain::Replay],
                    endpoint: SessionRouteEndpoint::Replay {
                        label: replay_label,
                    },
                });
            }

            if enabled_domains.contains(&ProtocolDomain::System) {
                topology = topology.with_route(SessionRoute {
                    label: "system".to_string(),
                    target: SessionTarget::Shared,
                    domains: vec![ProtocolDomain::System],
                    endpoint: SessionRouteEndpoint::Internal {
                        label: "system-driver".to_string(),
                    },
                });
            }

            Ok(topology)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use super::{PasswordCredentials, TqAuthProvider};
    use tqsdk_core::ContractError;

    #[tokio::test(flavor = "current_thread")]
    async fn read_json_response_rejects_declared_body_larger_than_auth_limit() {
        let url = spawn_declared_response("200 OK", 1024 * 1024 + 1);
        let response = reqwest::Client::new()
            .get(url)
            .send()
            .await
            .expect("test response should be returned");
        let provider = test_provider();

        let err = provider
            .read_json_response(response, "token request")
            .await
            .expect_err("oversized declared auth body should be rejected");

        assert!(
            matches!(err, ContractError::Auth(ref message) if message.contains("exceeded")),
            "unexpected error: {err}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_json_response_truncates_error_response_body() {
        let body = "x".repeat(300) + "TAIL_MARKER";
        let url = spawn_body_response("401 Unauthorized", body.as_bytes());
        let response = reqwest::Client::new()
            .get(url)
            .send()
            .await
            .expect("test response should be returned");
        let provider = test_provider();

        let err = provider
            .read_json_response(response, "token request")
            .await
            .expect_err("non-success auth response should fail");
        let message = err.to_string();

        assert!(message.contains("401 Unauthorized"));
        assert!(
            !message.contains("TAIL_MARKER"),
            "body was not truncated: {message}"
        );
    }

    fn test_provider() -> TqAuthProvider {
        TqAuthProvider::new(PasswordCredentials::new("test-user", "test-pass"))
    }

    fn spawn_declared_response(status: &'static str, content_length: usize) -> String {
        spawn_response(status, Some(content_length), Vec::new())
    }

    fn spawn_body_response(status: &'static str, body: &[u8]) -> String {
        spawn_response(status, Some(body.len()), body.to_vec())
    }

    fn spawn_response(
        status: &'static str,
        content_length: Option<usize>,
        body: Vec<u8>,
    ) -> String {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
        let addr = listener
            .local_addr()
            .expect("test listener should have an address");
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("test connection should arrive");
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request);
            write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nConnection: close\r\n"
            )
            .expect("headers should write");
            if let Some(length) = content_length {
                write!(stream, "Content-Length: {length}\r\n").expect("length should write");
            }
            stream
                .write_all(b"\r\n")
                .expect("header terminator should write");
            stream.write_all(&body).expect("body should write");
        });

        format!("http://{addr}")
    }
}
