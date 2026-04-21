#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk_core::{EndpointConfig, RuntimeHandle};

use crate::{client::SessionClient, config::SessionFacadeConfig, error::Result};

#[derive(Debug, Clone)]
pub struct SessionClientBuilder {
    auth_user: String,
    auth_pass: String,
    endpoints: EndpointConfig,
    facade_config: SessionFacadeConfig,
}

impl SessionClientBuilder {
    pub fn new(
        auth_user: impl Into<String>,
        auth_pass: impl Into<String>,
    ) -> Self {
        Self {
            auth_user: auth_user.into(),
            auth_pass: auth_pass.into(),
            endpoints: EndpointConfig::from_env(),
            facade_config: SessionFacadeConfig::default(),
        }
    }

    pub fn facade_config(mut self, facade_config: SessionFacadeConfig) -> Self {
        self.facade_config = facade_config;
        self
    }

    pub fn facade_config_ref(&self) -> &SessionFacadeConfig {
        &self.facade_config
    }

    pub fn query_url(mut self, query_url: impl Into<String>) -> Self {
        self.endpoints = self.endpoints.with_query_url(query_url);
        self
    }

    pub fn schema_url(mut self, schema_url: impl Into<String>) -> Self {
        self.endpoints = self.endpoints.with_schema_url(schema_url);
        self
    }

    pub fn replay_url(mut self, replay_url: impl Into<String>) -> Self {
        self.endpoints = self.endpoints.with_replay_url(replay_url);
        self
    }

    pub fn endpoints(&self) -> &EndpointConfig {
        &self.endpoints
    }

    pub fn build(self) -> Result<SessionClient> {
        let Self {
            auth_user: _auth_user,
            auth_pass: _auth_pass,
            endpoints: _endpoints,
            facade_config,
        } = self;
        let handle = RuntimeHandle::new();
        Ok(SessionClient::new(handle, facade_config))
    }
}
