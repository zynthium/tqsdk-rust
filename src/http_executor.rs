use std::time::Duration;

use futures::StreamExt;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT};
use serde_json::{Value, json};
use tokio::runtime::Builder as TokioRuntimeBuilder;
use url::Url;

use crate::commands::{HttpMethod, OutboundDispatch, OutboundRequest};
use crate::events::{InputPayload, IoEvent, RuntimeInput};
use crate::session_runtime::RouteRequestExecutor;
use crate::transport::{SessionRoute, SessionRouteEndpoint};
use crate::{ContractError, ContractFuture, ProtocolDomain, Result};

const DEFAULT_USER_AGENT: &str = "tqsdk-python 3.8.1";

#[derive(Clone)]
pub struct ReqwestHttpExecutor {
    client: reqwest::Client,
}

impl ReqwestHttpExecutor {
    pub fn new() -> Result<Self> {
        Ok(Self {
            client: Self::build_client()?,
        })
    }

    fn build_client() -> Result<reqwest::Client> {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        headers.insert(USER_AGENT, HeaderValue::from_static(DEFAULT_USER_AGENT));

        reqwest::Client::builder()
            .default_headers(headers)
            .gzip(true)
            .brotli(true)
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|err| {
                ContractError::validation(format!("failed to build reqwest http executor: {err}"))
            })
    }

    fn build_runtime(&self) -> Result<tokio::runtime::Runtime> {
        TokioRuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| {
                ContractError::validation(format!(
                    "failed to build tokio runtime for http executor: {err}"
                ))
            })
    }

    async fn execute_async(
        &self,
        route: &SessionRoute,
        requests: Vec<OutboundDispatch>,
    ) -> Result<Vec<RuntimeInput>> {
        let SessionRouteEndpoint::Http { url } = &route.endpoint else {
            return Err(ContractError::validation(
                "reqwest http executor requires http route",
            ));
        };

        let mut inputs = Vec::with_capacity(requests.len());
        for request in requests {
            let OutboundRequest::Http(http) = request.request else {
                return Err(ContractError::validation(
                    "reqwest http executor received non-http request",
                ));
            };
            let request_url = resolve_request_url(url, http.path.as_deref())?;
            let response = match http.method {
                HttpMethod::Get => self.client.get(request_url).send().await,
                HttpMethod::Post => {
                    let mut builder = self.client.post(request_url);
                    if let Some(body) = http.body.as_ref() {
                        builder = builder.header(CONTENT_TYPE, "application/json").json(body);
                    }
                    builder.send().await
                }
            }
            .map_err(|err| ContractError::auth(format!("http request failed: {err}")))?;

            let status = response.status();
            let bytes = read_response_bytes(response).await?;
            if !status.is_success() {
                let body = String::from_utf8_lossy(&bytes);
                return Err(ContractError::auth(format!(
                    "http request failed with status {status}: {body}"
                )));
            }

            let payload = decode_response_payload(
                &route.domains,
                http.body.as_ref(),
                &bytes,
            )?;
            inputs.push(RuntimeInput::Io(IoEvent {
                route: route.label.clone(),
                domains: route.domains.clone(),
                payload,
            }));
        }

        Ok(inputs)
    }
}

impl Default for ReqwestHttpExecutor {
    fn default() -> Self {
        Self::new().expect("reqwest http executor should build with a valid default client")
    }
}

impl RouteRequestExecutor for ReqwestHttpExecutor {
    fn execute<'a>(
        &'a self,
        route: &'a SessionRoute,
        requests: Vec<OutboundDispatch>,
    ) -> ContractFuture<'a, Vec<RuntimeInput>> {
        Box::pin(async move {
            if tokio::runtime::Handle::try_current().is_ok() {
                self.execute_async(route, requests).await
            } else {
                self.build_runtime()?.block_on(self.execute_async(route, requests))
            }
        })
    }
}

fn resolve_request_url(base_url: &str, path: Option<&str>) -> Result<String> {
    let Some(path) = path else {
        return Ok(base_url.to_string());
    };
    if matches!(path.split_once("://"), Some((_scheme, _rest))) {
        return Ok(path.to_string());
    }

    let base = Url::parse(base_url)
        .map_err(|err| ContractError::validation(format!("invalid http route base url: {err}")))?;
    base.join(path)
        .map(|url| url.to_string())
        .map_err(|err| ContractError::validation(format!("invalid http request path: {err}")))
}

async fn read_response_bytes(response: reqwest::Response) -> Result<Vec<u8>> {
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk
            .map_err(|err| ContractError::auth(format!("failed to read http response: {err}")))?;
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn decode_response_payload(
    domains: &[ProtocolDomain],
    request_body: Option<&Value>,
    bytes: &[u8],
) -> Result<InputPayload> {
    let value = serde_json::from_slice::<Value>(bytes)
        .map_err(|err| ContractError::auth(format!("http response was not valid json: {err}")))?;

    if domains.contains(&ProtocolDomain::Query)
        && let Some(query_id) = request_body
            .and_then(|body| body.get("query_id"))
            .and_then(Value::as_str)
    {
        return Ok(InputPayload::Json(wrap_query_response(query_id, value)));
    }

    Ok(InputPayload::Json(value))
}

fn wrap_query_response(query_id: &str, value: Value) -> Value {
    match value {
        Value::Object(mut object) => {
            object.insert("query_id".to_string(), json!(query_id));
            Value::Object(object)
        }
        other => json!({
            "query_id": query_id,
            "data": other,
        }),
    }
}
