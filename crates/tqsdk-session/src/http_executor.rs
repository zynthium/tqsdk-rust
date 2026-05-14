use std::{future::Future, pin::Pin, time::Duration};

use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT};
use serde_json::{Value, json};
use url::Url;

use crate::response_body::{
    HTTP_RESPONSE_BODY_LIMIT, read_limited_response_bytes, response_body_preview,
};
use tqsdk_core::internal::RouteRequestExecutor;
use tqsdk_core::{
    ContractError, HttpMethod, InputPayload, IoEvent, OutboundDispatch, OutboundRequest,
    ProtocolDomain, Result, RuntimeInput, SessionRoute, SessionRouteEndpoint,
};

const DEFAULT_USER_AGENT: &str = "tqsdk-python 3.8.1";

/// Low-level reqwest-backed executor for pending HTTP routes such as query and
/// schema refresh requests.
///
/// This type is intentionally thin: it only turns `OutboundDispatch` batches
/// into raw HTTP I/O and returns `RuntimeInput` values for adapters/runtime
/// ingestion. It does not add any facade-level retries or response shaping
/// beyond contract-required wrapping such as `query_id`.
#[derive(Clone)]
pub struct ReqwestHttpExecutor {
    client: reqwest::Client,
}

impl ReqwestHttpExecutor {
    /// Builds an executor with the contract's default headers and timeout.
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
            let (http_method, http_path, request_body) = match request.request {
                OutboundRequest::Http(http) => (http.method, http.path, http.body),
                OutboundRequest::Query(query) => (HttpMethod::Post, None, Some(query.body())),
                other => {
                    return Err(ContractError::validation(format!(
                        "reqwest http executor received unsupported request: {other:?}"
                    )));
                }
            };
            let request_url = resolve_request_url(url, http_path.as_deref())?;
            let response = match http_method {
                HttpMethod::Get => self.client.get(request_url).send().await,
                HttpMethod::Post => {
                    let mut builder = self.client.post(request_url);
                    if let Some(body) = request_body.as_ref() {
                        builder = builder.header(CONTENT_TYPE, "application/json").json(body);
                    }
                    builder.send().await
                }
            }
            .map_err(|err| ContractError::http(format!("http request failed: {err}")))?;

            let status = response.status();
            let bytes = read_response_bytes(response).await?;
            if !status.is_success() {
                let body = response_body_preview(&bytes);
                return Err(ContractError::http(format!(
                    "http request failed with status {status}: {body}"
                )));
            }

            let payload = decode_response_payload(&route.domains, request_body.as_ref(), &bytes)?;
            inputs.push(RuntimeInput::Io(IoEvent {
                route: route.label.clone(),
                domains: route.domains.clone(),
                payload,
            }));
        }

        Ok(inputs)
    }
}

impl RouteRequestExecutor for ReqwestHttpExecutor {
    fn execute<'a>(
        &'a self,
        route: &'a SessionRoute,
        requests: Vec<OutboundDispatch>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RuntimeInput>>> + Send + 'a>> {
        Box::pin(async move {
            require_tokio_runtime()?;
            self.execute_async(route, requests).await
        })
    }
}

fn require_tokio_runtime() -> Result<()> {
    tokio::runtime::Handle::try_current().map_err(|_| {
        ContractError::validation("reqwest http executor requires an active Tokio runtime")
    })?;
    Ok(())
}

fn resolve_request_url(base_url: &str, path: Option<&str>) -> Result<String> {
    let Some(path) = path else {
        return Ok(base_url.to_string());
    };
    if Url::parse(path).is_ok() || path.starts_with("//") {
        return Err(ContractError::validation(
            "absolute http request paths are not allowed; configure the route endpoint instead",
        ));
    }

    let base = Url::parse(base_url)
        .map_err(|err| ContractError::validation(format!("invalid http route base url: {err}")))?;
    base.join(path)
        .map(|url| url.to_string())
        .map_err(|err| ContractError::validation(format!("invalid http request path: {err}")))
}

async fn read_response_bytes(response: reqwest::Response) -> Result<Vec<u8>> {
    read_limited_response_bytes(
        response,
        HTTP_RESPONSE_BODY_LIMIT,
        "http response",
        ContractError::http,
    )
    .await
}

fn decode_response_payload(
    domains: &[ProtocolDomain],
    request_body: Option<&Value>,
    bytes: &[u8],
) -> Result<InputPayload> {
    let value = serde_json::from_slice::<Value>(bytes)
        .map_err(|err| ContractError::http(format!("http response was not valid json: {err}")))?;

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

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use tqsdk_core::{
        CommandId, ContractError, HttpMethod, HttpRequest, OutboundDispatch, OutboundRequest,
        ProtocolDomain, SessionRoute, SessionRouteEndpoint, SessionTarget,
    };

    use super::{ReqwestHttpExecutor, read_response_bytes, resolve_request_url};

    #[test]
    fn resolve_request_url_rejects_absolute_request_paths() {
        let err = resolve_request_url(
            "https://schema.example/base/latest.json",
            Some("https://metadata.evil/internal.json"),
        )
        .expect_err("absolute request path should be rejected");

        assert!(matches!(err, ContractError::Validation(message) if message.contains("absolute")));
    }

    #[test]
    fn resolve_request_url_joins_relative_request_paths_to_route_base() {
        let url = resolve_request_url("https://schema.example/base/", Some("instrument.json"))
            .expect("relative path should resolve against the route base");

        assert_eq!(url, "https://schema.example/base/instrument.json");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_response_bytes_rejects_declared_body_larger_than_http_limit() {
        let url = spawn_declared_response("200 OK", 64 * 1024 * 1024 + 1);
        let response = reqwest::Client::new()
            .get(url)
            .send()
            .await
            .expect("test response should be returned");

        let err = read_response_bytes(response)
            .await
            .expect_err("oversized declared body should be rejected");

        assert!(
            matches!(err, ContractError::Http(ref message) if message.contains("exceeded")),
            "unexpected error: {err}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execute_async_truncates_error_response_body() {
        let body = "x".repeat(300) + "TAIL_MARKER";
        let url = spawn_body_response("500 Internal Server Error", body.as_bytes());
        let executor = ReqwestHttpExecutor::new().expect("executor should build");
        let route = SessionRoute {
            label: "test-http".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Query],
            endpoint: SessionRouteEndpoint::Http { url },
        };
        let request = OutboundDispatch {
            command_id: CommandId::new(1),
            domain: ProtocolDomain::Query,
            account_id: None,
            request: OutboundRequest::Http(HttpRequest {
                method: HttpMethod::Get,
                path: None,
                body: None,
            }),
        };

        let err = executor
            .execute_async(&route, vec![request])
            .await
            .expect_err("non-success response should fail");
        let message = err.to_string();

        assert!(message.contains("500 Internal Server Error"));
        assert!(
            !message.contains("TAIL_MARKER"),
            "body was not truncated: {message}"
        );
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
