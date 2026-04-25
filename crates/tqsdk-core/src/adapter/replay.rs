use crate::{
    commands::{OutboundRequest, ReplayCommand, ReplayRequest, RuntimeCommand},
    error::{ContractError, Result},
    events::{NormalizedMutation, RuntimeInput},
    ids::ProtocolDomain,
};

use super::{ProtocolAdapter, common::decode_replay_payload};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplayAdapter;

impl ProtocolAdapter for ReplayAdapter {
    fn domain(&self) -> ProtocolDomain {
        ProtocolDomain::Replay
    }

    fn accepts_command(&self, cmd: &RuntimeCommand) -> bool {
        matches!(cmd, RuntimeCommand::Replay(_))
    }

    fn encode(&mut self, cmd: &RuntimeCommand) -> Result<Vec<OutboundRequest>> {
        match cmd {
            RuntimeCommand::Replay(ReplayCommand::Step) => {
                Ok(vec![OutboundRequest::Replay(ReplayRequest {
                    action: "step",
                })])
            }
            RuntimeCommand::Replay(ReplayCommand::Reset) => {
                Ok(vec![OutboundRequest::Replay(ReplayRequest {
                    action: "reset",
                })])
            }
            _ => Err(ContractError::UnsupportedCommand("replay")),
        }
    }

    fn accepts_input(&self, input: &RuntimeInput) -> bool {
        matches!(input, RuntimeInput::Replay(_))
    }

    fn decode(&mut self, input: &RuntimeInput) -> Result<Vec<NormalizedMutation>> {
        match input {
            RuntimeInput::Replay(event) => decode_replay_payload(event),
            _ => Ok(vec![]),
        }
    }
}
