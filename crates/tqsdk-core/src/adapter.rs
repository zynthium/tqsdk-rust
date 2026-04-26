mod common;
mod market;
mod query;
mod replay;
mod schema;
mod system;
mod trade;

use crate::{
    commands::{OutboundRequest, RuntimeCommand},
    error::{ContractError, Result},
    events::{NormalizedMutation, RuntimeInput},
    ids::ProtocolDomain,
};

pub use market::MarketAdapter;
pub use query::QueryAdapter;
pub use replay::ReplayAdapter;
pub use schema::SchemaAdapter;
pub use system::SystemAdapter;
pub use trade::TradeAdapter;

pub trait ProtocolAdapter: Send + Sync {
    fn domain(&self) -> ProtocolDomain;
    fn accepts_command(&self, cmd: &RuntimeCommand) -> bool;
    fn encode(&mut self, cmd: &RuntimeCommand) -> Result<Vec<OutboundRequest>>;
    fn accepts_input(&self, input: &RuntimeInput) -> bool;
    fn decode(&mut self, input: &RuntimeInput) -> Result<Vec<NormalizedMutation>>;

    fn recovery_commands(&self) -> Vec<RuntimeCommand> {
        Vec::new()
    }
}

pub struct AdapterRegistry {
    domains: Vec<ProtocolDomain>,
    adapters: Vec<Box<dyn ProtocolAdapter>>,
}

impl AdapterRegistry {
    pub fn new() -> Self {
        Self {
            domains: Vec::new(),
            adapters: Vec::new(),
        }
    }

    pub fn register_domain(&mut self, domain: ProtocolDomain) {
        if !self.domains.contains(&domain) {
            self.domains.push(domain);
        }
    }

    pub fn register_adapter<A>(&mut self, adapter: A)
    where
        A: ProtocolAdapter + 'static,
    {
        self.register_boxed_adapter(Box::new(adapter));
    }

    pub fn register_default_adapters(&mut self) {
        self.register_adapter(SystemAdapter);
        self.register_adapter(MarketAdapter::default());
        self.register_adapter(TradeAdapter);
        self.register_adapter(ReplayAdapter);
        self.register_adapter(QueryAdapter);
        self.register_adapter(SchemaAdapter);
    }

    pub fn owning_domain(&self, cmd: &RuntimeCommand) -> Option<ProtocolDomain> {
        self.adapters
            .iter()
            .find(|adapter| adapter.accepts_command(cmd))
            .map(|adapter| adapter.domain())
    }

    pub fn encode_command(&mut self, cmd: &RuntimeCommand) -> Result<Vec<OutboundRequest>> {
        let Some(adapter) = self
            .adapters
            .iter_mut()
            .find(|adapter| adapter.accepts_command(cmd))
        else {
            return Err(ContractError::UnsupportedCommand(cmd.domain().as_str()));
        };
        adapter.encode(cmd)
    }

    pub fn decode_input(&mut self, input: &RuntimeInput) -> Result<Vec<NormalizedMutation>> {
        let mut decoded = Vec::new();
        for adapter in self
            .adapters
            .iter_mut()
            .filter(|adapter| adapter.accepts_input(input))
        {
            decoded.extend(adapter.decode(input)?);
        }
        Ok(decoded)
    }

    pub(crate) fn recovery_commands(&self) -> Vec<RuntimeCommand> {
        self.adapters
            .iter()
            .flat_map(|adapter| adapter.recovery_commands())
            .collect()
    }

    pub fn domains(&self) -> &[ProtocolDomain] {
        &self.domains
    }

    fn register_boxed_adapter(&mut self, adapter: Box<dyn ProtocolAdapter>) {
        let domain = adapter.domain();
        self.register_domain(domain);

        if let Some(index) = self
            .adapters
            .iter()
            .position(|existing| existing.domain() == domain)
        {
            self.adapters[index] = adapter;
        } else {
            self.adapters.push(adapter);
        }
    }
}

impl Default for AdapterRegistry {
    fn default() -> Self {
        Self::new()
    }
}
