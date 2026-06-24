#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fmt::{Debug, Formatter};
use std::time::Duration;

use chrono::{Datelike, NaiveDate};
use serde_json::{Value, json};
use tqsdk_core::{AuthProvider, ContractError};

use crate::error::{Result, SessionFacadeError};
use crate::response_body::{
    AUTH_RESPONSE_BODY_LIMIT, read_limited_response_bytes, response_body_preview,
};
use crate::tq_auth::{PasswordCredentials, TqAuthProvider};

const DEFAULT_REPLAY_CREATE_SESSION_URL: &str =
    "http://replay.api.shinnytech.com/t/rmd/replay/create_session";

#[derive(Clone, PartialEq, Eq)]
pub struct ServerReplayBuilder {
    auth_user: String,
    auth_pass: String,
    replay_date: NaiveDate,
    create_session_url: String,
}

impl Debug for ServerReplayBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerReplayBuilder")
            .field("auth_user", &self.auth_user)
            .field("auth_pass", &"[REDACTED]")
            .field("replay_date", &self.replay_date)
            .field("create_session_url", &self.create_session_url)
            .finish()
    }
}

impl ServerReplayBuilder {
    pub fn new(
        auth_user: impl Into<String>,
        auth_pass: impl Into<String>,
        replay_date: NaiveDate,
    ) -> Result<Self> {
        validate_replay_date(replay_date)?;
        let auth_user = non_empty("auth_user", auth_user.into())?;
        let auth_pass = non_empty("auth_pass", auth_pass.into())?;
        Ok(Self {
            auth_user,
            auth_pass,
            replay_date,
            create_session_url: DEFAULT_REPLAY_CREATE_SESSION_URL.to_string(),
        })
    }

    #[must_use]
    pub fn create_session_url(mut self, create_session_url: impl Into<String>) -> Self {
        self.create_session_url = create_session_url.into();
        self
    }

    #[must_use]
    pub fn replay_date(&self) -> NaiveDate {
        self.replay_date
    }

    #[must_use]
    pub fn create_session_body(&self) -> Value {
        json!({ "dt": self.replay_date.format("%Y%m%d").to_string() })
    }

    pub async fn create(self) -> Result<ServerReplaySession> {
        let provider =
            TqAuthProvider::new(PasswordCredentials::new(&self.auth_user, &self.auth_pass));
        let auth = provider.authenticate().await?;
        let client = reqwest::Client::builder()
            .default_headers(provider.auth_headers(&auth)?)
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|err| {
                SessionFacadeError::from(ContractError::http(format!(
                    "failed to build replay session client: {err}"
                )))
            })?;
        let response = client
            .post(&self.create_session_url)
            .json(&self.create_session_body())
            .send()
            .await
            .map_err(|err| {
                SessionFacadeError::from(ContractError::transport(format!(
                    "replay create_session request failed: {err}"
                )))
            })?;
        let payload = read_json_response(response, "replay create_session").await?;
        ServerReplaySession::from_create_session_payload(self.replay_date, &payload)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerReplaySession {
    replay_date: NaiveDate,
    session_id: String,
    session_url: String,
    instrument_url: String,
    market_url: String,
}

impl ServerReplaySession {
    pub fn from_create_session_payload(replay_date: NaiveDate, payload: &Value) -> Result<Self> {
        validate_replay_date(replay_date)?;
        let ip = required_non_empty_str(payload, "ip")?;
        let session_id = required_non_empty_str(payload, "session")?;
        let session_port = required_port(payload, "session_port")?;
        let gateway_web_port = required_port(payload, "gateway_web_port")?;
        let session_url = format!("http://{ip}:{session_port}/t/rmd/replay/session/{session_id}");
        let instrument_url = format!("{session_url}/symbol");
        let market_url = format!("ws://{ip}:{gateway_web_port}/t/rmd/front/mobile");

        Ok(Self {
            replay_date,
            session_id: session_id.to_string(),
            session_url,
            instrument_url,
            market_url,
        })
    }

    #[must_use]
    pub fn replay_date(&self) -> NaiveDate {
        self.replay_date
    }

    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    #[must_use]
    pub fn session_url(&self) -> &str {
        &self.session_url
    }

    #[must_use]
    pub fn instrument_url(&self) -> &str {
        &self.instrument_url
    }

    #[must_use]
    pub fn market_url(&self) -> &str {
        &self.market_url
    }
}

async fn read_json_response(response: reqwest::Response, context: &str) -> Result<Value> {
    let status = response.status();
    let bytes = read_limited_response_bytes(
        response,
        AUTH_RESPONSE_BODY_LIMIT,
        context,
        ContractError::http,
    )
    .await?;
    if !status.is_success() {
        let body = response_body_preview(&bytes);
        return Err(SessionFacadeError::from(ContractError::http(format!(
            "{context} failed with status {status}: {body}"
        ))));
    }
    serde_json::from_slice(&bytes).map_err(|err| {
        SessionFacadeError::from(ContractError::validation(format!(
            "{context} returned invalid json: {err}"
        )))
    })
}

fn validate_replay_date(replay_date: NaiveDate) -> Result<()> {
    if replay_date.weekday().number_from_monday() <= 5 {
        Ok(())
    } else {
        Err(SessionFacadeError::from(ContractError::validation(
            "replay_date must be a weekday trading date",
        )))
    }
}

fn non_empty(field: &str, value: String) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(SessionFacadeError::from(ContractError::validation(
            format!("{field} must not be empty"),
        )));
    }
    Ok(trimmed.to_string())
}

fn required_non_empty_str<'a>(payload: &'a Value, field: &str) -> Result<&'a str> {
    let value = payload
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| missing_field(field))?;
    if value.trim().is_empty() {
        return Err(SessionFacadeError::from(ContractError::validation(
            format!("replay create_session response field {field} must not be empty"),
        )));
    }
    Ok(value)
}

fn required_port(payload: &Value, field: &str) -> Result<u16> {
    let raw = payload.get(field).ok_or_else(|| missing_field(field))?;
    let port = if let Some(port) = raw.as_u64() {
        port
    } else if let Some(port) = raw.as_str() {
        port.parse::<u64>().map_err(|err| {
            SessionFacadeError::from(ContractError::validation(format!(
                "replay create_session response field {field} is not a valid port: {err}"
            )))
        })?
    } else {
        return Err(SessionFacadeError::from(ContractError::validation(
            format!("replay create_session response field {field} is not a valid port"),
        )));
    };

    u16::try_from(port)
        .ok()
        .filter(|port| *port > 0)
        .ok_or_else(|| {
            SessionFacadeError::from(ContractError::validation(format!(
                "replay create_session response field {field} is outside the valid port range"
            )))
        })
}

fn missing_field(field: &str) -> SessionFacadeError {
    SessionFacadeError::from(ContractError::validation(format!(
        "replay create_session response missing {field}"
    )))
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use serde_json::json;

    use super::{ServerReplayBuilder, ServerReplaySession};

    #[test]
    fn server_replay_builder_rejects_weekend_dates() {
        let weekend = NaiveDate::from_ymd_opt(2026, 6, 27).expect("valid date");

        let error = ServerReplayBuilder::new("demo-user", "demo-pass", weekend)
            .expect_err("weekend replay date should be rejected");

        assert_eq!(
            error.diagnostic().message,
            "validation error: replay_date must be a weekday trading date"
        );
    }

    #[test]
    fn server_replay_builder_formats_create_session_body() {
        let replay_date = NaiveDate::from_ymd_opt(2026, 6, 25).expect("valid date");
        let builder =
            ServerReplayBuilder::new("demo-user", "demo-pass", replay_date).expect("valid builder");

        assert_eq!(builder.create_session_body(), json!({ "dt": "20260625" }));
    }

    #[test]
    fn server_replay_session_parses_create_response_endpoints() {
        let replay_date = NaiveDate::from_ymd_opt(2026, 6, 25).expect("valid date");
        let session = ServerReplaySession::from_create_session_payload(
            replay_date,
            &json!({
                "ip": "127.0.0.1",
                "session_port": 18888,
                "gateway_web_port": 27777,
                "session": "session-1"
            }),
        )
        .expect("valid session response");

        assert_eq!(session.replay_date(), replay_date);
        assert_eq!(session.session_id(), "session-1");
        assert_eq!(
            session.session_url(),
            "http://127.0.0.1:18888/t/rmd/replay/session/session-1"
        );
        assert_eq!(
            session.instrument_url(),
            "http://127.0.0.1:18888/t/rmd/replay/session/session-1/symbol"
        );
        assert_eq!(
            session.market_url(),
            "ws://127.0.0.1:27777/t/rmd/front/mobile"
        );
    }
}
