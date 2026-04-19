use serde_json::json;

use crate::{
    adapter::AdapterRegistry,
    auth::{AuthProvider, ContractFuture},
    runtime::RuntimeHandle,
    transport::{
        BootstrapResult, ConnectedTopology, SessionBootstrap, SessionConfig, SessionPhase, SessionRouteConnector,
        SessionTopologyResolver,
    },
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
