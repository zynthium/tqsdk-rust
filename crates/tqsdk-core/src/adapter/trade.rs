use crate::{
    commands::{OutboundRequest, RuntimeCommand, TradeCommand},
    diff_protocol::{DiffProtocolMessage, DiffTransferRequest},
    error::{ContractError, Result},
    events::{NormalizedMutation, RuntimeInput},
    ids::ProtocolDomain,
};

use super::{
    ProtocolAdapter,
    common::{
        build_insert_order_message, build_login_message, build_pre_insert_order_message,
        decode_trade_io_payload, diff_request, is_trade_io_event,
    },
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TradeAdapter;

impl ProtocolAdapter for TradeAdapter {
    fn domain(&self) -> ProtocolDomain {
        ProtocolDomain::Trade
    }

    fn accepts_command(&self, cmd: &RuntimeCommand) -> bool {
        matches!(cmd, RuntimeCommand::Trade(_))
    }

    fn encode(&mut self, cmd: &RuntimeCommand) -> Result<Vec<OutboundRequest>> {
        let message = match cmd {
            RuntimeCommand::Trade(TradeCommand::Login(login)) => build_login_message(login),
            RuntimeCommand::Trade(TradeCommand::ConfirmSettlement { .. }) => {
                DiffProtocolMessage::confirm_settlement()
            }
            RuntimeCommand::Trade(TradeCommand::QueryAccountInfo { account_id }) => {
                DiffProtocolMessage::query_account_info(account_id.as_str())
            }
            RuntimeCommand::Trade(TradeCommand::QueryAccountRegister { account_id }) => {
                DiffProtocolMessage::query_account_register(account_id.as_str())
            }
            RuntimeCommand::Trade(TradeCommand::QuerySettlementInfo {
                account_id,
                trading_day,
            }) => DiffProtocolMessage::query_settlement_info(
                account_id.as_str(),
                trading_day.to_string(),
            ),
            RuntimeCommand::Trade(TradeCommand::PreInsertOrder(order)) => {
                build_pre_insert_order_message(order)?
            }
            RuntimeCommand::Trade(TradeCommand::InsertOrder(order)) => {
                build_insert_order_message(order)?
            }
            RuntimeCommand::Trade(TradeCommand::CancelOrder {
                account_id,
                order_id,
            }) => DiffProtocolMessage::cancel_order(account_id.as_str(), order_id.as_str()),
            RuntimeCommand::Trade(TradeCommand::Transfer {
                account_id,
                bank_id,
                bank_password,
                future_account,
                future_password,
                currency,
                amount,
            }) => DiffProtocolMessage::req_transfer(DiffTransferRequest {
                user_id: account_id.as_str().to_string(),
                bank_id: bank_id.clone(),
                bank_password: bank_password.clone(),
                future_account: future_account.clone(),
                future_password: future_password.clone(),
                currency: currency.clone(),
                amount: amount.clone(),
            }),
            RuntimeCommand::Trade(TradeCommand::SetRiskManagementRule { account_id, rule }) => {
                let request = rule.as_object().cloned().ok_or_else(|| {
                    ContractError::validation(
                        "set_risk_management_rule expects an object-shaped rule payload",
                    )
                })?;
                DiffProtocolMessage::set_risk_management_rule(account_id.as_str(), request)
            }
            _ => return Err(ContractError::UnsupportedCommand("trade")),
        };

        Ok(vec![diff_request(message)?])
    }

    fn accepts_input(&self, input: &RuntimeInput) -> bool {
        matches!(input, RuntimeInput::Io(event) if event.domains.contains(&ProtocolDomain::Trade) && is_trade_io_event(event))
    }

    fn decode(&mut self, input: &RuntimeInput) -> Result<Vec<NormalizedMutation>> {
        match input {
            RuntimeInput::Io(event) if event.domains.contains(&ProtocolDomain::Trade) => {
                decode_trade_io_payload(event)
            }
            _ => Ok(vec![]),
        }
    }
}
