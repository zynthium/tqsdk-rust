use futures::StreamExt;
use tqsdk_core::{ContractError, Result};

pub(crate) const HTTP_RESPONSE_BODY_LIMIT: usize = 64 * 1024 * 1024;
pub(crate) const AUTH_RESPONSE_BODY_LIMIT: usize = 1024 * 1024;
const ERROR_BODY_PREVIEW_CHARS: usize = 256;

pub(crate) async fn read_limited_response_bytes(
    response: reqwest::Response,
    limit_bytes: usize,
    context: &str,
    error: impl Fn(String) -> ContractError,
) -> Result<Vec<u8>> {
    if let Some(length) = response.content_length()
        && length > limit_bytes as u64
    {
        return Err(error(format!(
            "{context}: response body exceeded {limit_bytes} byte limit: content-length={length}"
        )));
    }

    let capacity = response
        .content_length()
        .and_then(|length| usize::try_from(length).ok())
        .map_or(0, |length| length.min(limit_bytes));
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::with_capacity(capacity);

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|err| {
            error(format!(
                "{context}: failed to read response body chunk: {err}"
            ))
        })?;
        if chunk.len() > limit_bytes.saturating_sub(bytes.len()) {
            return Err(error(format!(
                "{context}: response body exceeded {limit_bytes} byte limit"
            )));
        }
        bytes.extend_from_slice(&chunk);
    }

    Ok(bytes)
}

pub(crate) fn response_body_preview(bytes: &[u8]) -> String {
    let body = String::from_utf8_lossy(bytes);
    if body.chars().count() <= ERROR_BODY_PREVIEW_CHARS {
        return body.into_owned();
    }
    body.chars()
        .take(ERROR_BODY_PREVIEW_CHARS)
        .collect::<String>()
        + "..."
}
