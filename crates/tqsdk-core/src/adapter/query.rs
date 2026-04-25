use crate::{
    commands::{OutboundRequest, QueryCommand, QueryRequest, RuntimeCommand},
    error::{ContractError, Result},
    events::{NormalizedMutation, RuntimeInput},
    ids::ProtocolDomain,
};

use super::{
    ProtocolAdapter,
    common::{decode_query_io_payload, is_query_io_event},
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryAdapter;

impl ProtocolAdapter for QueryAdapter {
    fn domain(&self) -> ProtocolDomain {
        ProtocolDomain::Query
    }

    fn accepts_command(&self, cmd: &RuntimeCommand) -> bool {
        matches!(cmd, RuntimeCommand::Query(_))
    }

    fn encode(&mut self, cmd: &RuntimeCommand) -> Result<Vec<OutboundRequest>> {
        match cmd {
            RuntimeCommand::Query(QueryCommand::Fetch {
                query_id,
                query,
                variables,
            }) => Ok(vec![OutboundRequest::Query(QueryRequest {
                query_id: query_id.clone(),
                query: query.clone(),
                variables: variables.clone(),
            })]),
            _ => Err(ContractError::UnsupportedCommand("query")),
        }
    }

    fn accepts_input(&self, input: &RuntimeInput) -> bool {
        matches!(input, RuntimeInput::Io(event) if event.domains.contains(&ProtocolDomain::Query) && is_query_io_event(event))
    }

    fn decode(&mut self, input: &RuntimeInput) -> Result<Vec<NormalizedMutation>> {
        match input {
            RuntimeInput::Io(event) if event.domains.contains(&ProtocolDomain::Query) => {
                decode_query_io_payload(event)
            }
            _ => Ok(vec![]),
        }
    }
}
