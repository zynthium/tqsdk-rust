use tokio::time::{Instant, timeout};
use tqsdk_core::{CommandId, CommitScope, SessionRouteEndpoint, SessionRuntimeDeps};

use super::{
    SessionClient, SessionIoState, SessionProgress, prime_all_websocket_routes,
    prime_route_with_recover, recover_run,
};

impl SessionClient {
    pub async fn ensure_established(&self) -> crate::error::Result<bool> {
        let Some(io) = self.io.as_ref() else {
            return Ok(false);
        };
        let mut io = io.lock().await;
        if io.run.is_some() {
            return Ok(false);
        }

        let run = self
            .runtime
            .establish(
                io.auth_provider.as_ref(),
                io.topology_resolver.as_ref(),
                io.route_connector.as_ref(),
                &io.config,
                &io.adapters,
            )
            .await?;
        io.run = Some(run);
        prime_all_websocket_routes(&mut io).await?;
        Ok(true)
    }

    pub async fn flush_outbound(&self) -> crate::error::Result<bool> {
        self.ensure_established().await?;
        let Some(io) = self.io.as_ref() else {
            return Ok(false);
        };
        let mut io = io.lock().await;
        self.flush_outbound_locked(&mut io).await
    }

    pub async fn drive_pending_once(&self) -> crate::error::Result<bool> {
        self.ensure_established().await?;
        let Some(io) = self.io.as_ref() else {
            return Ok(false);
        };
        let mut io = io.lock().await;
        self.drive_pending_once_locked(&mut io).await
    }

    pub(super) async fn drive_pending_route_label_once(
        &self,
        route_label: &str,
    ) -> crate::error::Result<bool> {
        self.ensure_established().await?;
        let Some(io) = self.io.as_ref() else {
            return Ok(false);
        };
        let mut io = io.lock().await;
        let Some(route) = io.run.as_ref().and_then(|run| {
            run.connected
                .routes
                .iter()
                .find(|route| route.route.label == route_label)
                .map(|route| route.route.clone())
        }) else {
            return Ok(false);
        };
        let Some(executor) = (match route.endpoint {
            SessionRouteEndpoint::Http { .. } => Some(io.http_executor.clone()),
            SessionRouteEndpoint::Internal { .. } => Some(io.internal_executor.clone()),
            SessionRouteEndpoint::Replay { .. } => Some(io.replay_executor.clone()),
            SessionRouteEndpoint::WebSocket { .. } => None,
        }) else {
            return Ok(false);
        };
        let Some(run) = io.run.as_mut() else {
            return Ok(false);
        };
        let outcome = self
            .runtime
            .drive_pending_route_once(
                run,
                route_label,
                executor.as_ref(),
                Vec::new(),
                CommitScope::RealtimeUpdate,
            )
            .await?;
        Ok(!outcome.requests.is_empty() || !outcome.commits.is_empty())
    }

    pub async fn drive_route_once(&self, deadline: Option<Instant>) -> crate::error::Result<bool> {
        self.ensure_established().await?;
        let Some(io) = self.io.as_ref() else {
            return Ok(false);
        };
        let mut io = io.lock().await;
        self.drive_route_once_locked(&mut io, deadline).await
    }

    /// Performs one substrate-level progress step across outbound flush,
    /// pending-route execution, and one websocket-route drive attempt.
    ///
    /// Callers should still drain commit cursors themselves if they need
    /// commit-first semantics. This helper only advances the live session.
    pub async fn progress_once(
        &self,
        deadline: Option<Instant>,
    ) -> crate::error::Result<SessionProgress> {
        self.ensure_established().await?;
        let Some(io) = self.io.as_ref() else {
            return Ok(SessionProgress::Idle);
        };
        let mut io = io.lock().await;

        if self.flush_outbound_locked(&mut io).await? {
            return Ok(SessionProgress::FlushedOutbound);
        }
        if self.drive_pending_once_locked(&mut io).await? {
            return Ok(SessionProgress::DrovePending);
        }
        if self.drive_route_once_locked(&mut io, deadline).await? {
            return Ok(SessionProgress::DroveRoute);
        }

        Ok(SessionProgress::Idle)
    }

    pub(super) async fn drive_route_label_once(
        &self,
        route_label: &str,
        deadline: Option<Instant>,
        caused_by: Vec<CommandId>,
    ) -> crate::error::Result<bool> {
        self.ensure_established().await?;
        let Some(io) = self.io.as_ref() else {
            return Ok(false);
        };
        let mut io = io.lock().await;
        if io
            .run
            .as_ref()
            .is_none_or(|run| !run.connected.has_route(route_label))
        {
            return Ok(false);
        }
        prime_route_with_recover(&mut io, &self.runtime, route_label).await?;

        let SessionIoState {
            auth_provider,
            topology_resolver,
            route_connector,
            adapters,
            config,
            run,
            ..
        } = &mut *io;
        let Some(run) = run.as_mut() else {
            return Ok(false);
        };
        let deps = SessionRuntimeDeps::new(
            auth_provider.as_ref(),
            topology_resolver.as_ref(),
            route_connector.as_ref(),
            config,
            adapters,
        );
        let future = self.runtime.drive_route_once(
            run,
            route_label,
            caused_by,
            CommitScope::RealtimeUpdate,
            deps,
        );
        let outcome = if let Some(deadline) = deadline {
            let budget = deadline.saturating_duration_since(Instant::now());
            if budget.is_zero() {
                return Ok(false);
            }
            match timeout(budget, future).await {
                Ok(result) => result?,
                Err(_) => return Ok(false),
            }
        } else {
            future.await?
        };

        Ok(!outcome.dispatches.is_empty() || !outcome.commits.is_empty() || outcome.recovered)
    }

    async fn flush_outbound_locked(&self, io: &mut SessionIoState) -> crate::error::Result<bool> {
        if io.run.is_none() {
            return Ok(false);
        }
        let receipts = match self
            .runtime
            .flush_outbound(io.run.as_mut().expect("run checked above"))
            .await
        {
            Ok(receipts) => receipts,
            Err(tqsdk_core::ContractError::Transport(_)) => {
                recover_run(io, &self.runtime).await?;
                self.runtime
                    .flush_outbound(io.run.as_mut().expect("run recovered"))
                    .await?
            }
            Err(err) => return Err(err.into()),
        };
        Ok(!receipts.is_empty())
    }

    async fn drive_pending_once_locked(
        &self,
        io: &mut SessionIoState,
    ) -> crate::error::Result<bool> {
        let Some(route_label) = io.next_pending_route_label() else {
            return Ok(false);
        };
        let Some(route) = io.run.as_ref().and_then(|run| {
            run.connected
                .routes
                .iter()
                .find(|route| route.route.label == route_label)
                .map(|route| route.route.clone())
        }) else {
            return Ok(false);
        };
        let Some(executor) = (match route.endpoint {
            SessionRouteEndpoint::Http { .. } => Some(io.http_executor.clone()),
            SessionRouteEndpoint::Internal { .. } => Some(io.internal_executor.clone()),
            SessionRouteEndpoint::Replay { .. } => Some(io.replay_executor.clone()),
            SessionRouteEndpoint::WebSocket { .. } => None,
        }) else {
            return Ok(false);
        };
        let Some(run) = io.run.as_mut() else {
            return Ok(false);
        };
        let outcome = self
            .runtime
            .drive_pending_route_once(
                run,
                route_label.as_str(),
                executor.as_ref(),
                Vec::new(),
                CommitScope::RealtimeUpdate,
            )
            .await?;
        Ok(!outcome.requests.is_empty() || !outcome.commits.is_empty())
    }

    async fn drive_route_once_locked(
        &self,
        io: &mut SessionIoState,
        deadline: Option<Instant>,
    ) -> crate::error::Result<bool> {
        let Some(route_label) = io.next_websocket_route_label() else {
            return Ok(false);
        };
        prime_route_with_recover(io, &self.runtime, route_label.as_str()).await?;

        let SessionIoState {
            auth_provider,
            topology_resolver,
            route_connector,
            adapters,
            config,
            run,
            ..
        } = io;
        let Some(run) = run.as_mut() else {
            return Ok(false);
        };
        let deps = SessionRuntimeDeps::new(
            auth_provider.as_ref(),
            topology_resolver.as_ref(),
            route_connector.as_ref(),
            config,
            adapters,
        );
        let future = self.runtime.drive_route_once(
            run,
            route_label.as_str(),
            Vec::new(),
            CommitScope::RealtimeUpdate,
            deps,
        );
        let outcome = if let Some(deadline) = deadline {
            let budget = deadline.saturating_duration_since(Instant::now());
            if budget.is_zero() {
                return Ok(false);
            }
            match timeout(budget, future).await {
                Ok(result) => result?,
                Err(_) => return Ok(false),
            }
        } else {
            future.await?
        };

        Ok(!outcome.dispatches.is_empty() || !outcome.commits.is_empty() || outcome.recovered)
    }
}
