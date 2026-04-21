#![cfg_attr(not(test), forbid(unsafe_code))]

use crate::api::TqApi;

#[derive(Debug, Clone)]
pub struct TqApiBuilder {
    inner: tqsdk_session::SessionClientBuilder,
}

impl TqApiBuilder {
    pub fn new(
        auth_user: impl Into<String>,
        auth_pass: impl Into<String>,
    ) -> Self {
        Self {
            inner: tqsdk_session::SessionClientBuilder::new(auth_user, auth_pass),
        }
    }

    pub async fn build(self) -> crate::error::Result<TqApi> {
        let session = self.inner.build()?;
        Ok(TqApi::new(session))
    }
}
