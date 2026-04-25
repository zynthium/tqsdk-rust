use crate::{
    commands::{OutboundRequest, RuntimeCommand, SystemCommand},
    error::{ContractError, Result},
    events::{IoEvent, MutationSource, NormalizedMutation, RuntimeInput},
    ids::ProtocolDomain,
};

use super::{
    ProtocolAdapter,
    common::{decode_named_payload, decode_system_io_payload},
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SystemAdapter;

impl ProtocolAdapter for SystemAdapter {
    fn domain(&self) -> ProtocolDomain {
        ProtocolDomain::System
    }

    fn accepts_command(&self, cmd: &RuntimeCommand) -> bool {
        matches!(cmd, RuntimeCommand::System(_))
    }

    fn encode(&mut self, cmd: &RuntimeCommand) -> Result<Vec<OutboundRequest>> {
        match cmd {
            RuntimeCommand::System(SystemCommand::Shutdown) => {
                Ok(vec![OutboundRequest::internal_label("shutdown-runtime")])
            }
            RuntimeCommand::System(SystemCommand::RefreshAuth) => {
                Ok(vec![OutboundRequest::internal_label("refresh-auth")])
            }
            _ => Err(ContractError::UnsupportedCommand("system")),
        }
    }

    fn accepts_input(&self, input: &RuntimeInput) -> bool {
        match input {
            RuntimeInput::Auth(_) | RuntimeInput::Timer(_) | RuntimeInput::Internal(_) => true,
            RuntimeInput::Io(IoEvent { domains, .. }) => domains.contains(&ProtocolDomain::System),
            _ => false,
        }
    }

    fn decode(&mut self, input: &RuntimeInput) -> Result<Vec<NormalizedMutation>> {
        match input {
            RuntimeInput::Auth(event) => decode_named_payload(
                ["system".to_string(), "auth".to_string()].to_vec(),
                event,
                MutationSource::SessionControl,
            ),
            RuntimeInput::Timer(event) => decode_named_payload(
                ["system".to_string(), "timers".to_string()].to_vec(),
                event,
                MutationSource::SessionControl,
            ),
            RuntimeInput::Internal(event) => decode_named_payload(
                ["system".to_string(), "internal".to_string()].to_vec(),
                event,
                MutationSource::SessionControl,
            ),
            RuntimeInput::Io(event) if event.domains.contains(&ProtocolDomain::System) => {
                decode_system_io_payload(event)
            }
            _ => Ok(vec![]),
        }
    }
}
