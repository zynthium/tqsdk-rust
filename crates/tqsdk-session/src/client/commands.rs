use std::sync::atomic::Ordering;

use serde_json::Value;
use tokio::time::Instant;
use tqsdk_core::{
    CommandId, CommandStatus, MarketCommand, QueryCommand, QueryId, ReplayCommand, Runtime,
    RuntimeCommand, SchemaCommand, SchemaId, Symbol, SystemCommand,
};

use super::{
    NEXT_QUERY_ID, SessionClient, WEBSOCKET_COMMAND_MAX_WAIT, WEBSOCKET_COMMAND_POLL_BUDGET,
};

impl SessionClient {
    fn validate_query_payload(value: &Value) -> crate::error::Result<()> {
        if let Some(error) = value.get("error").and_then(Value::as_str) {
            return Err(crate::error::SessionFacadeError::from(
                tqsdk_core::ContractError::validation(format!("graphql query failed: {error}")),
            ));
        }
        if let Some(errors) = value.get("errors").and_then(Value::as_array)
            && !errors.is_empty()
        {
            return Err(crate::error::SessionFacadeError::from(
                tqsdk_core::ContractError::validation(format!(
                    "graphql query failed: {}",
                    Value::Array(errors.clone())
                )),
            ));
        }
        Ok(())
    }

    fn next_query_id() -> QueryId {
        QueryId::new(format!(
            "query-{}",
            NEXT_QUERY_ID.fetch_add(1, Ordering::Relaxed)
        ))
    }

    pub async fn submit(&self, command: RuntimeCommand) -> crate::error::Result<CommandId> {
        Ok(self.handle.submit(command).await?)
    }

    pub async fn subscribe_quotes<I, S>(&self, symbols: I) -> crate::error::Result<CommandId>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let symbols = symbols
            .into_iter()
            .map(|symbol| Symbol::new(symbol.as_ref()))
            .collect::<Vec<_>>();
        if symbols.is_empty() {
            return Err(crate::error::SessionFacadeError::InvalidState(
                "subscribe_quotes requires at least one symbol",
            ));
        }
        self.submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
            symbols,
        }))
        .await
    }

    pub async fn unsubscribe_quotes<I, S>(&self, symbols: I) -> crate::error::Result<CommandId>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let symbols = symbols
            .into_iter()
            .map(|symbol| Symbol::new(symbol.as_ref()))
            .collect::<Vec<_>>();
        if symbols.is_empty() {
            return Err(crate::error::SessionFacadeError::InvalidState(
                "unsubscribe_quotes requires at least one symbol",
            ));
        }
        self.submit(RuntimeCommand::Market(MarketCommand::UnsubscribeQuotes {
            symbols,
        }))
        .await
    }

    pub fn query_result(&self, query_id: &str) -> crate::error::Result<Option<Value>> {
        let guard = self.reader.read();
        guard
            .decode_path::<Value>(&["query", query_id])
            .map_err(Into::into)
    }

    pub fn command_state(&self, command_id: CommandId) -> crate::error::Result<Option<Value>> {
        let command_segment = command_id.get().to_string();
        let guard = self.reader.read();
        guard
            .decode_path::<Value>(&["runtime", "commands", command_segment.as_str()])
            .map_err(Into::into)
    }

    pub fn command_status(&self, command_id: CommandId) -> crate::error::Result<Option<String>> {
        Ok(self.command_state(command_id)?.and_then(|command| {
            command
                .get("status")
                .and_then(Value::as_str)
                .map(str::to_owned)
        }))
    }

    pub fn command_status_typed(
        &self,
        command_id: CommandId,
    ) -> crate::error::Result<Option<CommandStatus>> {
        let Some(status) = self.command_status(command_id)? else {
            return Ok(None);
        };

        status
            .parse()
            .map(Some)
            .map_err(|()| crate::error::SessionFacadeError::InvalidState("unknown command status"))
    }

    /// Drives the substrate until the specified command reaches a completed
    /// terminal status.
    ///
    /// This helper only advances transport/runtime state for the submitted
    /// command. It does not impose `wait_update()` semantics or consume commit
    /// cursors on behalf of the caller.
    pub async fn wait_command_completed(&self, command_id: CommandId) -> crate::error::Result<()> {
        let started_at = Instant::now();
        loop {
            if self.command_completed(command_id)? {
                return Ok(());
            }

            let mut progress = false;

            progress |= self.flush_outbound().await?;
            if self.command_completed(command_id)? {
                return Ok(());
            }

            if let Some(route_label) = self.command_route_label(command_id)? {
                progress |= self
                    .drive_pending_route_label_once(route_label.as_str())
                    .await?;
                if self.command_completed(command_id)? {
                    return Ok(());
                }

                let websocket_progress = self
                    .drive_route_label_once(
                        route_label.as_str(),
                        Some(Instant::now() + WEBSOCKET_COMMAND_POLL_BUDGET),
                        vec![command_id],
                    )
                    .await?;
                progress |= websocket_progress;
                if !websocket_progress && started_at.elapsed() < WEBSOCKET_COMMAND_MAX_WAIT {
                    progress = true;
                }
            } else {
                progress |= self.drive_pending_once().await?;
                if self.command_completed(command_id)? {
                    return Ok(());
                }

                progress |= self.drive_route_once(None).await?;
            }
            if self.command_completed(command_id)? {
                return Ok(());
            }

            if !progress {
                return Err(crate::error::SessionFacadeError::InvalidState(
                    "command did not reach a terminal state",
                ));
            }
        }
    }

    fn command_route_label(&self, command_id: CommandId) -> crate::error::Result<Option<String>> {
        Ok(self.command_state(command_id)?.and_then(|command| {
            command
                .get("detail")
                .and_then(|detail| detail.get("route"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        }))
    }

    pub fn schema_value(&self, schema_id: &str) -> crate::error::Result<Option<Value>> {
        let guard = self.reader.read();
        guard
            .decode_path::<Value>(&["schema", schema_id])
            .map_err(Into::into)
    }

    pub fn auth_context(&self) -> crate::error::Result<Option<Value>> {
        let guard = self.reader.read();
        guard
            .decode_path::<Value>(&["system", "auth", "context"])
            .map_err(Into::into)
    }

    pub fn refreshed_auth(&self) -> crate::error::Result<Option<Value>> {
        let guard = self.reader.read();
        guard
            .decode_path::<Value>(&["system", "auth", "refreshed"])
            .map_err(Into::into)
    }

    pub fn replay_state(&self, replay_id: &str) -> crate::error::Result<Option<Value>> {
        let guard = self.reader.read();
        guard
            .decode_path::<Value>(&["replay", replay_id])
            .map_err(Into::into)
    }

    fn command_completed(&self, command_id: CommandId) -> crate::error::Result<bool> {
        match self.command_status_typed(command_id)? {
            Some(CommandStatus::Completed) => Ok(true),
            Some(status) if status.is_terminal() => {
                Err(crate::error::SessionFacadeError::InvalidState(
                    "command reached a non-completed terminal status",
                ))
            }
            Some(_) | None => Ok(false),
        }
    }

    async fn require_query_value_route(&self) -> crate::error::Result<()> {
        let Some(io) = self.io.as_ref() else {
            return Err(crate::error::SessionFacadeError::InvalidState(
                "query value helper requires an enabled query route",
            ));
        };
        let io = io.lock().await;
        if !io
            .config
            .enabled_domains()
            .contains(&tqsdk_core::ProtocolDomain::Query)
        {
            return Err(crate::error::SessionFacadeError::InvalidState(
                "query value helper requires an enabled query route",
            ));
        }
        if io.config.endpoints.query_url.is_none() && !io.config.market_target.stock {
            return Err(crate::error::SessionFacadeError::InvalidState(
                "websocket query helpers require stock market_target when query_url is not configured",
            ));
        }
        Ok(())
    }

    async fn require_replay_value_route(&self) -> crate::error::Result<()> {
        if let Some(io) = self.io.as_ref()
            && io
                .lock()
                .await
                .config
                .enabled_domains()
                .contains(&tqsdk_core::ProtocolDomain::Replay)
        {
            Ok(())
        } else {
            Err(crate::error::SessionFacadeError::InvalidState(
                "replay value helper requires an enabled replay route",
            ))
        }
    }

    pub async fn query_graphql(
        &self,
        query: &str,
        variables: Option<Value>,
    ) -> crate::error::Result<CommandId> {
        self.submit(RuntimeCommand::Query(QueryCommand::Fetch {
            query_id: Self::next_query_id(),
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

    pub async fn query_graphql_value(
        &self,
        query: &str,
        variables: Option<Value>,
    ) -> crate::error::Result<Value> {
        let _query_guard = self.query_lock.lock().await;
        self.require_query_value_route().await?;
        let query_id = Self::next_query_id();
        let command_id = self
            .submit(RuntimeCommand::Query(QueryCommand::Fetch {
                query_id: query_id.clone(),
                query: query.to_owned(),
                variables,
            }))
            .await?;

        self.wait_command_completed(command_id).await?;
        let value = self.query_result(query_id.as_str())?.ok_or(
            crate::error::SessionFacadeError::InvalidState(
                "query command completed without a result payload",
            ),
        )?;
        Self::validate_query_payload(&value)?;
        Ok(value)
    }

    pub async fn refresh_schema_value(
        &self,
        schema_id: &str,
        path: &str,
    ) -> crate::error::Result<Value> {
        let command_id = self
            .submit(RuntimeCommand::Schema(SchemaCommand::Refresh {
                schema_id: SchemaId::new(schema_id),
                path: path.to_owned(),
            }))
            .await?;

        self.wait_command_completed(command_id).await?;
        self.schema_value(schema_id)?
            .ok_or(crate::error::SessionFacadeError::InvalidState(
                "schema refresh completed without a schema payload",
            ))
    }

    pub async fn refresh_auth(&self) -> crate::error::Result<CommandId> {
        self.submit(RuntimeCommand::System(SystemCommand::RefreshAuth))
            .await
    }

    pub async fn refresh_auth_value(&self) -> crate::error::Result<Value> {
        let command_id = self.refresh_auth().await?;
        self.wait_command_completed(command_id).await?;
        self.refreshed_auth()?
            .ok_or(crate::error::SessionFacadeError::InvalidState(
                "auth refresh completed without a refreshed auth payload",
            ))
    }

    pub async fn replay_step(&self) -> crate::error::Result<CommandId> {
        self.submit(RuntimeCommand::Replay(ReplayCommand::Step))
            .await
    }

    pub async fn replay_step_value(&self, replay_id: &str) -> crate::error::Result<Value> {
        self.require_replay_value_route().await?;
        let command_id = self.replay_step().await?;
        self.wait_command_completed(command_id).await?;
        self.replay_state(replay_id)?
            .ok_or(crate::error::SessionFacadeError::InvalidState(
                "replay step completed without a replay state payload",
            ))
    }

    pub async fn replay_reset(&self) -> crate::error::Result<CommandId> {
        self.submit(RuntimeCommand::Replay(ReplayCommand::Reset))
            .await
    }

    pub async fn replay_reset_value(&self, replay_id: &str) -> crate::error::Result<Value> {
        self.require_replay_value_route().await?;
        let command_id = self.replay_reset().await?;
        self.wait_command_completed(command_id).await?;
        self.replay_state(replay_id)?
            .ok_or(crate::error::SessionFacadeError::InvalidState(
                "replay reset completed without a replay state payload",
            ))
    }
}
