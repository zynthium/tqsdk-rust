use serde_json::json;

use crate::{
    adapter::AdapterRegistry,
    auth::{AuthProvider, ContractFuture},
    ids::CommandId,
    runtime::RuntimeHandle,
    state::{CommitResult, CommitScope},
    transport::{
        BootstrapResult, ConnectedTopology, DispatchReceipt, SessionBootstrap, SessionConfig, SessionPhase,
        SessionRouteConnector, SessionTopologyResolver,
    },
    Result,
};

pub struct SessionRun {
    pub bootstrap: BootstrapResult,
    pub connected: ConnectedTopology,
}

#[derive(Clone)]
pub struct SessionRuntime {
    handle: RuntimeHandle,
    bootstrap: SessionBootstrap,
}

impl SessionRuntime {
    pub fn new(handle: RuntimeHandle, bootstrap: SessionBootstrap) -> Self {
        Self { handle, bootstrap }
    }

    pub fn handle(&self) -> RuntimeHandle {
        self.handle.clone()
    }

    pub fn establish<'a>(
        &'a self,
        auth: &'a dyn AuthProvider,
        resolver: &'a dyn SessionTopologyResolver,
        connector: &'a dyn SessionRouteConnector,
        config: &'a SessionConfig,
        adapters: &'a AdapterRegistry,
    ) -> ContractFuture<'a, SessionRun> {
        Box::pin(async move {
            self.handle
                .record_session_phase(SessionPhase::Authenticating, None, vec![])?;

            let bootstrap = self
                .bootstrap
                .establish_with_resolver(auth, resolver, config, adapters)
                .await?;
            let connected = self.connect_if_needed(
                &bootstrap,
                connector,
                Some(SessionPhase::Connecting),
            )
            .await?;

            self.handle
                .record_session_phase(SessionPhase::Bootstrapping, None, vec![])?;
            self.handle.record_session_bootstrap(&bootstrap, vec![])?;

            Ok(SessionRun { bootstrap, connected })
        })
    }

    pub fn recover<'a>(
        &'a self,
        auth: &'a dyn AuthProvider,
        resolver: &'a dyn SessionTopologyResolver,
        connector: &'a dyn SessionRouteConnector,
        config: &'a SessionConfig,
        adapters: &'a AdapterRegistry,
    ) -> ContractFuture<'a, SessionRun> {
        Box::pin(async move {
            self.handle
                .record_session_phase(SessionPhase::Reconnecting, None, vec![])?;

            let bootstrap = self
                .bootstrap
                .establish_with_resolver(auth, resolver, config, adapters)
                .await?;
            let connected = self
                .connect_if_needed(&bootstrap, connector, None)
                .await?;

            self.handle
                .record_session_phase(SessionPhase::Resyncing, None, vec![])?;
            self.handle.record_session_resync(&bootstrap, vec![])?;

            Ok(SessionRun { bootstrap, connected })
        })
    }

    pub fn flush_outbound<'a>(
        &'a self,
        run: &'a mut SessionRun,
    ) -> ContractFuture<'a, Vec<DispatchReceipt>> {
        Box::pin(async move {
            let dispatches = self.handle.drain_dispatches()?;
            let mut receipts = Vec::with_capacity(dispatches.len());
            for dispatch in dispatches {
                receipts.push(run.connected.dispatch(dispatch).await?);
            }
            Ok(receipts)
        })
    }

    pub fn recv_route_and_ingest<'a>(
        &'a self,
        run: &'a mut SessionRun,
        route_label: &'a str,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> ContractFuture<'a, Option<CommitResult>> {
        Box::pin(async move {
            let Some(input) = run.connected.recv_route_input(route_label).await? else {
                return Ok(None);
            };
            self.handle.ingest(input, caused_by, scope)
        })
    }

    pub fn ingest_queued_inputs(
        &self,
        run: &mut SessionRun,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> Result<Option<CommitResult>> {
        let inputs = run.connected.drain_queued_inputs();
        if inputs.is_empty() {
            return Ok(None);
        }
        self.handle.ingest_batch(inputs, caused_by, scope)
    }

    fn connect_if_needed<'a>(
        &'a self,
        bootstrap: &'a BootstrapResult,
        connector: &'a dyn SessionRouteConnector,
        phase: Option<SessionPhase>,
    ) -> ContractFuture<'a, ConnectedTopology> {
        Box::pin(async move {
            if bootstrap.topology.routes.is_empty() {
                return Ok(ConnectedTopology::default());
            }

            if let Some(phase) = phase {
                self.handle.record_session_phase(
                    phase,
                    Some(json!({ "route_count": bootstrap.topology.routes.len() })),
                    vec![],
                )?;
            }

            self.bootstrap
                .connect_topology(&bootstrap.topology, connector)
                .await
        })
    }
}
