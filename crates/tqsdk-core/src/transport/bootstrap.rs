use std::collections::VecDeque;

use crate::Result;
use crate::adapter::AdapterRegistry;
use crate::auth::DynAuthProvider;

use super::config::SessionConfig;
use super::connected::{ConnectedSessionRoute, ConnectedTopology};
use super::connector::SessionRouteConnector;
use super::topology::{BootstrapResult, SessionTopology, SessionTopologyResolver};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionBootstrap;

impl SessionBootstrap {
    pub fn new() -> Self {
        Self
    }

    pub async fn establish(
        &self,
        auth: &dyn DynAuthProvider,
        config: &SessionConfig,
        adapters: &AdapterRegistry,
    ) -> Result<BootstrapResult> {
        let auth = auth.authenticate_boxed().await?;
        let enabled_domains = if config.enabled_domains.is_empty() {
            adapters.domains().to_vec()
        } else {
            config.enabled_domains.clone()
        };

        Ok(BootstrapResult::new(auth, enabled_domains))
    }

    pub async fn establish_with_resolver(
        &self,
        auth: &dyn DynAuthProvider,
        resolver: &dyn SessionTopologyResolver,
        config: &SessionConfig,
        adapters: &AdapterRegistry,
    ) -> Result<BootstrapResult> {
        let auth = auth.authenticate_boxed().await?;
        let enabled_domains = if config.enabled_domains.is_empty() {
            adapters.domains().to_vec()
        } else {
            config.enabled_domains.clone()
        };
        let topology = resolver
            .resolve_topology(&auth, config, &enabled_domains)
            .await?;

        Ok(BootstrapResult::new(auth, enabled_domains).with_topology(topology))
    }

    pub async fn connect_topology(
        &self,
        topology: &SessionTopology,
        connector: &dyn SessionRouteConnector,
    ) -> Result<ConnectedTopology> {
        let mut connected = ConnectedTopology::default();

        for route in &topology.routes {
            match connector.connect_route(route).await {
                Ok(transport) => connected.routes.push(ConnectedSessionRoute {
                    route: route.clone(),
                    transport,
                    pending_requests: VecDeque::new(),
                    pending_inputs: VecDeque::new(),
                }),
                Err(err) => {
                    let _ = connected.close_all().await;
                    return Err(err);
                }
            }
        }

        Ok(connected)
    }
}
