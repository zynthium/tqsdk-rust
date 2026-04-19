use crate::{
    commands::{OutboundRequest, RuntimeCommand},
    error::Result,
    events::{NormalizedMutation, RuntimeInput},
    ids::ProtocolDomain,
};

pub trait ProtocolAdapter {
    fn domain(&self) -> ProtocolDomain;
    fn accepts_command(&self, cmd: &RuntimeCommand) -> bool;
    fn encode(&mut self, cmd: &RuntimeCommand) -> Result<Vec<OutboundRequest>>;
    fn accepts_input(&self, input: &RuntimeInput) -> bool;
    fn decode(&mut self, input: &RuntimeInput) -> Result<Vec<NormalizedMutation>>;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AdapterRegistry {
    domains: Vec<ProtocolDomain>,
}

impl AdapterRegistry {
    pub fn new() -> Self {
        Self { domains: Vec::new() }
    }

    pub fn register_domain(&mut self, domain: ProtocolDomain) {
        if !self.domains.contains(&domain) {
            self.domains.push(domain);
        }
    }

    pub fn domains(&self) -> &[ProtocolDomain] {
        &self.domains
    }
}
