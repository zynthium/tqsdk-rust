use std::time::Duration;

use chrono::{Days, NaiveDate};
use reqwest::StatusCode;
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderMap, HeaderValue, USER_AGENT};
use serde_json::Value;
use tqsdk_core::{AuthContext, SymbolRanking};
use url::Url;

use crate::client::SessionClient;
use crate::error::{Result, SessionFacadeError};

const DEFAULT_USER_AGENT: &str = "tqsdk-python 3.8.1";

pub(super) fn split_symbol(symbol: &str) -> (&str, &str) {
    symbol
        .split_once('.')
        .map_or(("", symbol), |(exchange, instrument)| {
            (exchange, instrument)
        })
}

pub(super) fn ranking_value(row: &SymbolRanking, field: &str) -> f64 {
    match field {
        "volume_ranking" => row.volume_ranking,
        "long_ranking" => row.long_ranking,
        "short_ranking" => row.short_ranking,
        _ => f64::NAN,
    }
}

pub(super) fn parse_service_url(url: &str, label: &str) -> Result<Url> {
    Url::parse(url).map_err(|error| {
        SessionFacadeError::from(tqsdk_core::ContractError::validation(format!(
            "invalid {label} service url: {error}"
        )))
    })
}

pub(super) fn parse_iso_date(value: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|error| {
        SessionFacadeError::from(tqsdk_core::ContractError::validation(format!(
            "invalid date string `{value}`: {error}"
        )))
    })
}

pub(super) fn next_day(date: NaiveDate) -> Result<NaiveDate> {
    date.checked_add_days(Days::new(1)).ok_or_else(|| {
        SessionFacadeError::from(tqsdk_core::ContractError::validation(
            "date overflow while advancing day",
        ))
    })
}

pub(super) fn json_value_to_f64(value: &Value) -> f64 {
    match value {
        Value::Number(number) => number.as_f64().unwrap_or(f64::NAN),
        Value::String(text) if matches!(text.as_str(), "NaN" | "-" | "") => f64::NAN,
        Value::String(text) => text.parse().unwrap_or(f64::NAN),
        Value::Null => f64::NAN,
        _ => f64::NAN,
    }
}

pub(super) async fn fetch_json_get(client: &SessionClient, url: &str) -> Result<Value> {
    fetch_json(client, "GET", url, None).await
}

pub(super) async fn fetch_json_post(
    client: &SessionClient,
    url: &str,
    body: &Value,
) -> Result<Value> {
    fetch_json(client, "POST", url, Some(body)).await
}

async fn fetch_json(
    client: &SessionClient,
    method: &'static str,
    url: &str,
    body: Option<&Value>,
) -> Result<Value> {
    require_tokio_runtime()?;

    for force_refresh in [false, true] {
        let auth = client.service_auth_context(force_refresh).await?;
        let headers = auth_headers(&auth)?;
        let request = match method {
            "GET" => client.service_http().get(url).headers(headers),
            "POST" => {
                let Some(body) = body else {
                    return Err(SessionFacadeError::from(
                        tqsdk_core::ContractError::validation("post request requires a body"),
                    ));
                };
                client.service_http().post(url).headers(headers).json(body)
            }
            _ => {
                return Err(SessionFacadeError::from(
                    tqsdk_core::ContractError::validation("unsupported service request method"),
                ));
            }
        };

        let response = request
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(|error| {
                SessionFacadeError::from(tqsdk_core::ContractError::transport(format!(
                    "{method} {url} request failed: {error}"
                )))
            })?;

        if response.status() == StatusCode::UNAUTHORIZED && !force_refresh {
            continue;
        }

        return read_json_response(method, url, response).await;
    }

    Err(SessionFacadeError::from(tqsdk_core::ContractError::auth(
        format!("{method} {url} authentication failed"),
    )))
}

fn auth_headers(auth: &AuthContext) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(USER_AGENT, HeaderValue::from_static(DEFAULT_USER_AGENT));
    let authorization =
        HeaderValue::from_str(&format!("Bearer {}", auth.access_token())).map_err(|error| {
            SessionFacadeError::from(tqsdk_core::ContractError::auth(format!(
                "invalid authorization header: {error}"
            )))
        })?;
    headers.insert(AUTHORIZATION, authorization);
    Ok(headers)
}

async fn read_json_response(method: &str, url: &str, response: reqwest::Response) -> Result<Value> {
    let status = response.status();
    let body = response.text().await.map_err(|error| {
        SessionFacadeError::from(tqsdk_core::ContractError::transport(format!(
            "{method} {url} failed while reading response body: {error}"
        )))
    })?;
    if !status.is_success() {
        return Err(SessionFacadeError::from(tqsdk_core::ContractError::http(
            format!(
                "{method} {url} failed with status {status}: {}",
                truncate_body(&body)
            ),
        )));
    }
    serde_json::from_str(&body).map_err(|error| {
        SessionFacadeError::from(tqsdk_core::ContractError::validation(format!(
            "{method} {url} returned invalid json: {error}"
        )))
    })
}

fn truncate_body(body: &str) -> String {
    const MAX_LEN: usize = 256;
    if body.chars().count() <= MAX_LEN {
        return body.to_string();
    }
    body.chars().take(MAX_LEN).collect::<String>() + "..."
}

fn require_tokio_runtime() -> Result<()> {
    tokio::runtime::Handle::try_current().map_err(|_| {
        SessionFacadeError::from(tqsdk_core::ContractError::validation(
            "session direct service helpers require an active Tokio runtime",
        ))
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use serde_json::json;

    use super::*;

    #[test]
    fn split_symbol_splits_exchange_and_instrument() {
        assert_eq!(split_symbol("SHFE.au2606"), ("SHFE", "au2606"));
        assert_eq!(split_symbol("au2606"), ("", "au2606"));
    }

    #[test]
    fn ranking_value_returns_selected_numeric_field() {
        let row = SymbolRanking {
            volume_ranking: 1.0,
            long_ranking: 2.0,
            short_ranking: 3.0,
            ..SymbolRanking::default()
        };

        assert_eq!(ranking_value(&row, "volume_ranking"), 1.0);
        assert_eq!(ranking_value(&row, "long_ranking"), 2.0);
        assert_eq!(ranking_value(&row, "short_ranking"), 3.0);
        assert!(ranking_value(&row, "unknown").is_nan());
    }

    #[test]
    fn parse_service_url_reports_label_on_invalid_url() {
        let url = parse_service_url("https://example.test/service", "ranking")
            .expect("valid URL should parse");
        assert_eq!(url.host_str(), Some("example.test"));

        let error = parse_service_url("not a url", "ranking").expect_err("invalid URL should fail");
        assert!(error.to_string().contains("invalid ranking service url"));
    }

    #[test]
    fn parse_iso_date_rejects_invalid_dates() {
        assert_eq!(
            parse_iso_date("2026-05-01").expect("valid date should parse"),
            NaiveDate::from_ymd_opt(2026, 5, 1).expect("fixture date should exist")
        );

        let error = parse_iso_date("2026-02-30").expect_err("invalid date should fail");
        assert!(
            error
                .to_string()
                .contains("invalid date string `2026-02-30`")
        );
    }

    #[test]
    fn next_day_rejects_overflow() {
        assert_eq!(
            next_day(NaiveDate::from_ymd_opt(2026, 5, 1).expect("fixture date should exist"))
                .expect("normal next day should succeed"),
            NaiveDate::from_ymd_opt(2026, 5, 2).expect("fixture date should exist")
        );
        assert!(next_day(NaiveDate::MAX).is_err());
    }

    #[test]
    fn json_value_to_f64_handles_numbers_strings_and_invalid_values() {
        assert_eq!(json_value_to_f64(&json!(12.5)), 12.5);
        assert_eq!(json_value_to_f64(&json!("13.5")), 13.5);
        assert!(json_value_to_f64(&json!("NaN")).is_nan());
        assert!(json_value_to_f64(&json!("-")).is_nan());
        assert!(json_value_to_f64(&json!("")).is_nan());
        assert!(json_value_to_f64(&json!({"value": 1})).is_nan());
        assert!(json_value_to_f64(&Value::Null).is_nan());
    }

    #[test]
    fn truncate_body_limits_error_payload_size() {
        let short = "short body";
        assert_eq!(truncate_body(short), short);

        let long = "x".repeat(300);
        let truncated = truncate_body(&long);

        assert_eq!(truncated.chars().count(), 259);
        assert!(truncated.ends_with("..."));
    }

    #[test]
    fn auth_headers_builds_bearer_authorization() {
        let headers =
            auth_headers(&AuthContext::new("token-1")).expect("auth headers should build");

        assert_eq!(
            headers
                .get(AUTHORIZATION)
                .expect("authorization header should exist")
                .to_str()
                .expect("authorization header should be valid"),
            "Bearer token-1"
        );
        assert_eq!(
            headers
                .get(USER_AGENT)
                .expect("user agent header should exist")
                .to_str()
                .expect("user agent header should be valid"),
            DEFAULT_USER_AGENT
        );
    }
}
