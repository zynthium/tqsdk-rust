use std::future::Future;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures::StreamExt;
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderMap, HeaderValue, USER_AGENT};
use serde_json::Value;

use crate::auth::{AuthContext, AuthProvider, ContractFuture};
use crate::ids::ProtocolDomain;
use crate::transport::{
    SessionConfig, SessionRoute, SessionRouteEndpoint, SessionTarget, SessionTopology,
    SessionTopologyResolver, WebSocketConnectOptions,
};
use crate::{AuthId, ContractError, ReplaySessionId, Result};

const DEFAULT_AUTH_URL: &str = "https://auth.shinnytech.com";
const DEFAULT_NAME_SERVICE_URL: &str = "https://api.shinnytech.com/ns";
const DEFAULT_BROKER_BASE_URL: &str = "https://files.shinnytech.com";
const DEFAULT_USER_AGENT: &str = "tqsdk-python 3.8.1";
const CLIENT_ID: &str = "shinny_tq";
const CLIENT_SECRET: &str = "be30b9f4-6862-488a-99ad-21bde0400081";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PasswordCredentials {
    pub username: String,
    pub password: String,
}

impl PasswordCredentials {
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TqAuthProvider {
    credentials: PasswordCredentials,
    auth_url: String,
    name_service_url: String,
    broker_base_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerInfo {
    pub category: Vec<String>,
    pub url: String,
    pub broker_type: Option<String>,
    pub smtype: Option<String>,
    pub smconfig: Option<String>,
    pub condition_type: Option<String>,
    pub condition_config: Option<String>,
}

impl TqAuthProvider {
    pub fn new(credentials: PasswordCredentials) -> Self {
        Self {
            credentials,
            auth_url: DEFAULT_AUTH_URL.to_string(),
            name_service_url: DEFAULT_NAME_SERVICE_URL.to_string(),
            broker_base_url: DEFAULT_BROKER_BASE_URL.to_string(),
        }
    }

    pub fn with_auth_url(mut self, auth_url: impl Into<String>) -> Self {
        self.auth_url = auth_url.into();
        self
    }

    pub fn with_name_service_url(mut self, name_service_url: impl Into<String>) -> Self {
        self.name_service_url = name_service_url.into();
        self
    }

    pub fn with_broker_base_url(mut self, broker_base_url: impl Into<String>) -> Self {
        self.broker_base_url = broker_base_url.into();
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
        let mut stream = response.bytes_stream();
        let mut buffer = Vec::new();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|err| {
                ContractError::auth(format!("{context}: failed to read response chunk: {err}"))
            })?;
            buffer.extend_from_slice(&chunk);
        }

        if !status.is_success() {
            let body = String::from_utf8_lossy(&buffer);
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

    fn auth_headers(&self, auth: &AuthContext) -> Result<HeaderMap> {
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
    ) -> Result<BrokerInfo> {
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

            Ok(BrokerInfo {
                category,
                url: url.to_string(),
                broker_type: optional_string(broker, "broker_type"),
                smtype: optional_string(broker, "smtype"),
                smconfig: optional_string(broker, "smconfig"),
                condition_type: optional_string(broker, "condition_type"),
                condition_config: optional_string(broker, "condition_config"),
            })
        })
        .await
    }

    pub fn fetch_market_url<'a>(
        &'a self,
        auth: &'a AuthContext,
        stock: bool,
        backtest: bool,
    ) -> ContractFuture<'a, String> {
        Box::pin(async move { self.request_market_url(auth, stock, backtest).await })
    }

    pub fn fetch_trade_broker<'a>(
        &'a self,
        auth: &'a AuthContext,
        broker_id: &'a str,
        account_id: &'a str,
    ) -> ContractFuture<'a, BrokerInfo> {
        Box::pin(async move { self.request_trade_broker(auth, broker_id, account_id).await })
    }
}

fn require_tokio_runtime() -> Result<()> {
    tokio::runtime::Handle::try_current().map_err(|_| {
        ContractError::validation("tq auth provider requires an active Tokio runtime")
    })?;
    Ok(())
}

fn optional_string(payload: &Value, field: &str) -> Option<String> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

impl AuthProvider for TqAuthProvider {
    fn authenticate(&self) -> ContractFuture<'_, AuthContext> {
        Box::pin(async move {
            let access_token = self.request_access_token().await?;
            self.build_auth_context(access_token)
        })
    }
}

impl SessionTopologyResolver for TqAuthProvider {
    fn resolve_topology<'a>(
        &'a self,
        auth: &'a AuthContext,
        config: &'a SessionConfig,
        enabled_domains: &'a [ProtocolDomain],
    ) -> ContractFuture<'a, SessionTopology> {
        Box::pin(async move {
            let mut topology = SessionTopology::default();
            let connect = WebSocketConnectOptions::default()
                .with_header("Authorization", format!("Bearer {}", auth.access_token()))
                .with_header("Accept", "application/json")
                .with_header("User-Agent", DEFAULT_USER_AGENT);

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
                let market_url = if let Some(url) = &config.endpoints.market_url {
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
                    label: "market".to_string(),
                    target: SessionTarget::Shared,
                    domains: market_domains,
                    endpoint: SessionRouteEndpoint::WebSocket {
                        url: market_url,
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
                    let trade_url = if let Some(url) = &target.trade_url {
                        url.clone()
                    } else if let Some(url) = &config.endpoints.trade_url {
                        url.clone()
                    } else {
                        self.request_trade_broker(
                            auth,
                            &target.broker_id,
                            target.account_id.as_str(),
                        )
                        .await?
                        .url
                    };

                    topology = topology.with_route(SessionRoute {
                        label: format!("trade:{}", target.account_id.as_str()),
                        target: SessionTarget::Account(target.account_id.clone()),
                        domains: vec![ProtocolDomain::Trade],
                        endpoint: SessionRouteEndpoint::WebSocket {
                            url: trade_url,
                            connect: connect.clone(),
                        },
                    });
                }
            }

            if enabled_domains.contains(&ProtocolDomain::Query) {
                let Some(query_url) = config.endpoints.query_url.clone() else {
                    return Err(ContractError::validation(
                        "query domain requires endpoints.query_url for topology resolution",
                    ));
                };
                topology = topology.with_route(SessionRoute {
                    label: "query".to_string(),
                    target: SessionTarget::Shared,
                    domains: vec![ProtocolDomain::Query],
                    endpoint: SessionRouteEndpoint::Http { url: query_url },
                });
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
