#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::Value;
use tqsdk_core::{
    CommandId, OutboundDispatch, QueryCommand, QueryId, Runtime, RuntimeCommand, RuntimeHandle,
    RuntimeReader, SchemaCommand, SchemaId, SessionBootstrap, SessionRuntime,
};

use crate::config::SessionFacadeConfig;
use crate::direct_query::SessionDirectQuery;

static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);

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

    pub fn reader_clone(&self) -> RuntimeReader {
        self.reader.clone()
    }

    pub fn runtime_clone(&self) -> SessionRuntime {
        self.runtime.clone()
    }

    pub async fn submit(&self, command: RuntimeCommand) -> crate::error::Result<CommandId> {
        Ok(self.handle.submit(command).await?)
    }

    pub fn drain_dispatches(&self) -> crate::error::Result<Vec<OutboundDispatch>> {
        Ok(self.handle.drain_dispatches()?)
    }

    pub async fn query_graphql(
        &self,
        query: &str,
        variables: Option<Value>,
    ) -> crate::error::Result<CommandId> {
        self.submit(RuntimeCommand::Query(QueryCommand::Fetch {
            query_id: QueryId::new(format!(
                "query-{}",
                NEXT_QUERY_ID.fetch_add(1, Ordering::Relaxed)
            )),
            query: query.to_owned(),
            variables,
        }))
        .await
    }

    pub async fn refresh_schema(
        &self,
        schema_id: &str,
        path: &str,
    ) -> crate::error::Result<CommandId> {
        self.submit(RuntimeCommand::Schema(SchemaCommand::Refresh {
            schema_id: SchemaId::new(schema_id),
            path: path.to_owned(),
        }))
        .await
    }

    pub fn facade_config(&self) -> &SessionFacadeConfig {
        &self.facade_config
    }

    #[doc(hidden)]
    pub fn new_for_test_with_handle(
        handle: RuntimeHandle,
        facade_config: SessionFacadeConfig,
    ) -> Self {
        Self::new(
            handle,
            facade_config,
            SessionClientContext::new(
                String::new(),
                String::new(),
                tqsdk_core::EndpointConfig::default(),
            ),
        )
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

impl SessionDirectQuery for SessionClient {
    async fn query_graphql(
        &self,
        query: &str,
        variables: Option<Value>,
    ) -> crate::error::Result<CommandId> {
        SessionClient::query_graphql(self, query, variables).await
    }

    async fn refresh_schema(
        &self,
        schema_id: &str,
        path: &str,
    ) -> crate::error::Result<CommandId> {
        SessionClient::refresh_schema(self, schema_id, path).await
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
