use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde_json::Value;

use crate::auth::{AuthContext, AuthProvider, ContractFuture};
use crate::{AuthId, ContractError, Result};

const DEFAULT_AUTH_URL: &str = "https://auth.shinnytech.com";
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
}

impl TqAuthProvider {
    pub fn new(credentials: PasswordCredentials) -> Self {
        Self {
            credentials,
            auth_url: DEFAULT_AUTH_URL.to_string(),
        }
    }

    pub fn with_auth_url(mut self, auth_url: impl Into<String>) -> Self {
        self.auth_url = auth_url.into();
        self
    }

    fn token_url(&self) -> String {
        format!(
            "{}/auth/realms/shinnytech/protocol/openid-connect/token",
            self.auth_url.trim_end_matches('/')
        )
    }

    fn request_access_token(&self) -> Result<String> {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|err| ContractError::auth(format!("failed to build auth client: {err}")))?;
        let response = client
            .post(self.token_url())
            .form(&[
                ("client_id", CLIENT_ID),
                ("client_secret", CLIENT_SECRET),
                ("grant_type", "password"),
                ("username", self.credentials.username.as_str()),
                ("password", self.credentials.password.as_str()),
            ])
            .send()
            .map_err(|err| ContractError::auth(format!("token request failed: {err}")))?;
        let status = response.status();
        let body = response
            .text()
            .map_err(|err| ContractError::auth(format!("failed to read auth response: {err}")))?;

        if !status.is_success() {
            return Err(ContractError::auth(format!(
                "token request failed with status {status}: {body}"
            )));
        }

        let payload: Value = serde_json::from_str(&body)
            .map_err(|err| ContractError::auth(format!("invalid auth response json: {err}")))?;
        let access_token = payload
            .get("access_token")
            .and_then(Value::as_str)
            .ok_or_else(|| ContractError::auth("auth response missing access_token"))?;

        Ok(access_token.to_string())
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
}

impl AuthProvider for TqAuthProvider {
    fn authenticate(&self) -> ContractFuture<'_, AuthContext> {
        Box::pin(async move {
            let access_token = self.request_access_token()?;
            self.build_auth_context(access_token)
        })
    }
}
