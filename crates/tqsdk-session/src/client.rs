#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk_core::{RuntimeHandle, RuntimeReader, SessionBootstrap, SessionRuntime};

use crate::config::SessionFacadeConfig;

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone)]
pub(crate) struct SessionClientContext {
    auth_user: String,
    auth_pass: String,
    endpoints: tqsdk_core::EndpointConfig,
}

impl SessionClientContext {
    pub(crate) fn new(
        auth_user: String,
        auth_pass: String,
        endpoints: tqsdk_core::EndpointConfig,
    ) -> Self {
        Self {
            auth_user,
            auth_pass,
            endpoints,
        }
    }
}

#[derive(Clone)]
pub struct SessionClient {
    handle: RuntimeHandle,
    reader: RuntimeReader,
    runtime: SessionRuntime,
    facade_config: SessionFacadeConfig,
    #[cfg_attr(not(test), allow(dead_code))]
    context: SessionClientContext,
}

impl SessionClient {
    pub(crate) fn new(
        handle: RuntimeHandle,
        facade_config: SessionFacadeConfig,
        context: SessionClientContext,
    ) -> Self {
        let reader = handle.reader();
        let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
        Self {
            handle,
            reader,
            runtime,
            facade_config,
            context,
        }
    }

    pub fn handle(&self) -> &RuntimeHandle {
        &self.handle
    }

    pub fn reader(&self) -> &RuntimeReader {
        &self.reader
    }

    pub fn runtime(&self) -> &SessionRuntime {
        &self.runtime
    }

    pub fn facade_config(&self) -> &SessionFacadeConfig {
        &self.facade_config
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn auth_user(&self) -> &str {
        &self.context.auth_user
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn auth_pass(&self) -> &str {
        &self.context.auth_pass
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn endpoints(&self) -> &tqsdk_core::EndpointConfig {
        &self.context.endpoints
    }
}

#[cfg(test)]
mod tests {
    use crate::builder::SessionClientBuilder;

    #[test]
    fn built_client_retains_builder_auth_and_endpoints() {
        let client = SessionClientBuilder::new("demo-user", "demo-pass")
            .query_url("https://query.example.com/graphql")
            .schema_url("https://schema.example.com/latest.json")
            .replay_url("wss://replay.example.com/feed")
            .build()
            .expect("builder should construct a thin session client");

        assert_eq!(client.auth_user(), "demo-user");
        assert_eq!(client.auth_pass(), "demo-pass");
        assert_eq!(
            client.endpoints().query_url.as_deref(),
            Some("https://query.example.com/graphql")
        );
        assert_eq!(
            client.endpoints().schema_url.as_deref(),
            Some("https://schema.example.com/latest.json")
        );
        assert_eq!(
            client.endpoints().replay_url.as_deref(),
            Some("wss://replay.example.com/feed")
        );
    }
}
