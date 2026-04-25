use crate::{
    commands::{HttpMethod, HttpRequest, OutboundRequest, RuntimeCommand, SchemaCommand},
    error::{ContractError, Result},
    events::{IoEvent, NormalizedMutation, RuntimeInput},
    ids::ProtocolDomain,
};

use super::{ProtocolAdapter, common::decode_schema_io_payload};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaAdapter;

impl ProtocolAdapter for SchemaAdapter {
    fn domain(&self) -> ProtocolDomain {
        ProtocolDomain::Schema
    }

    fn accepts_command(&self, cmd: &RuntimeCommand) -> bool {
        matches!(cmd, RuntimeCommand::Schema(_))
    }

    fn encode(&mut self, cmd: &RuntimeCommand) -> Result<Vec<OutboundRequest>> {
        match cmd {
            RuntimeCommand::Schema(SchemaCommand::Refresh { path, .. }) => {
                if path.is_empty() {
                    return Err(ContractError::validation(
                        "schema refresh path must not be empty",
                    ));
                }
                Ok(vec![OutboundRequest::Http(HttpRequest {
                    method: HttpMethod::Get,
                    path: Some(path.clone()),
                    body: None,
                })])
            }
            _ => Err(ContractError::UnsupportedCommand("schema")),
        }
    }

    fn accepts_input(&self, input: &RuntimeInput) -> bool {
        matches!(input, RuntimeInput::Io(IoEvent { domains, .. }) if domains.contains(&ProtocolDomain::Schema))
    }

    fn decode(&mut self, input: &RuntimeInput) -> Result<Vec<NormalizedMutation>> {
        match input {
            RuntimeInput::Io(event) if event.domains.contains(&ProtocolDomain::Schema) => {
                decode_schema_io_payload(event)
            }
            _ => Ok(vec![]),
        }
    }
}
