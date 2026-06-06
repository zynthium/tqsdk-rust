#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::{Arc, Mutex};

use serde_json::Value;

use crate::engine::{DownstreamFrame, RelayEngine};
use crate::error::{RelayError, RelayResult};
use crate::interest::ClientId;
use crate::protocol::DownstreamCommand;

#[derive(Clone)]
pub struct RelayServer {
    engine: Arc<Mutex<RelayEngine>>,
}

impl RelayServer {
    #[must_use]
    pub fn new(engine: Arc<Mutex<RelayEngine>>) -> Self {
        Self { engine }
    }

    #[must_use]
    pub fn engine(&self) -> Arc<Mutex<RelayEngine>> {
        self.engine.clone()
    }

    pub async fn handle_text(
        &self,
        raw_client_id: u64,
        text: String,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        let value: Value = serde_json::from_str(&text)
            .map_err(|err| RelayError::invalid_protocol(format!("invalid JSON frame: {err}")))?;
        let command = DownstreamCommand::from_value(value)?;
        let mut engine = self
            .engine
            .lock()
            .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?;
        engine.handle_command(ClientId::new(raw_client_id), command)
    }
}
