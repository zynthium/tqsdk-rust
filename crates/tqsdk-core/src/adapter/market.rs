use std::collections::{BTreeMap, BTreeSet};

use crate::{
    commands::{MarketChartCommand, MarketCommand, OutboundRequest, RuntimeCommand},
    diff_protocol::DiffProtocolMessage,
    error::{ContractError, Result},
    events::{MutationSource, NormalizedMutation, RuntimeInput},
    ids::{ProtocolDomain, Symbol},
};

use super::{
    ProtocolAdapter,
    common::{
        build_chart_message, decode_io_payload, decode_io_payload_owned, extend_symbols,
        is_market_io_event, join_symbols, remove_symbols, request_with_peek,
        validate_chart_request,
    },
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MarketAdapter {
    quote_subscriptions: BTreeSet<String>,
    trading_status_subscriptions: BTreeSet<String>,
    charts: BTreeMap<String, MarketChartCommand>,
}

impl ProtocolAdapter for MarketAdapter {
    fn domain(&self) -> ProtocolDomain {
        ProtocolDomain::Market
    }

    fn accepts_command(&self, cmd: &RuntimeCommand) -> bool {
        matches!(cmd, RuntimeCommand::Market(_))
    }

    fn encode(&mut self, cmd: &RuntimeCommand) -> Result<Vec<OutboundRequest>> {
        match cmd {
            RuntimeCommand::Market(MarketCommand::SubscribeQuotes { symbols }) => {
                extend_symbols(&mut self.quote_subscriptions, symbols);
                request_with_peek(DiffProtocolMessage::subscribe_quote(join_symbols(
                    &self.quote_subscriptions,
                )))
            }
            RuntimeCommand::Market(MarketCommand::UnsubscribeQuotes { symbols }) => {
                remove_symbols(&mut self.quote_subscriptions, symbols);
                request_with_peek(DiffProtocolMessage::subscribe_quote(join_symbols(
                    &self.quote_subscriptions,
                )))
            }
            RuntimeCommand::Market(MarketCommand::SetChart(chart)) => {
                validate_chart_request(chart)?;
                self.charts.insert(chart.chart_id.clone(), chart.clone());
                request_with_peek(build_chart_message(chart, false))
            }
            RuntimeCommand::Market(MarketCommand::CancelChart { chart_id }) => {
                let Some(chart) = self.charts.remove(chart_id) else {
                    return Err(ContractError::validation(format!(
                        "unknown chart_id for cancel_chart: {chart_id}"
                    )));
                };
                request_with_peek(build_chart_message(&chart, true))
            }
            RuntimeCommand::Market(MarketCommand::SubscribeTradingStatus { symbols }) => {
                extend_symbols(&mut self.trading_status_subscriptions, symbols);
                request_with_peek(DiffProtocolMessage::subscribe_trading_status(join_symbols(
                    &self.trading_status_subscriptions,
                )))
            }
            RuntimeCommand::Market(MarketCommand::UnsubscribeTradingStatus { symbols }) => {
                remove_symbols(&mut self.trading_status_subscriptions, symbols);
                request_with_peek(DiffProtocolMessage::subscribe_trading_status(join_symbols(
                    &self.trading_status_subscriptions,
                )))
            }
            _ => Err(ContractError::UnsupportedCommand("market")),
        }
    }

    fn accepts_input(&self, input: &RuntimeInput) -> bool {
        matches!(input, RuntimeInput::Io(event) if event.domains.contains(&ProtocolDomain::Market) && is_market_io_event(event))
    }

    fn decode(&mut self, input: &RuntimeInput) -> Result<Vec<NormalizedMutation>> {
        match input {
            RuntimeInput::Io(event) if event.domains.contains(&ProtocolDomain::Market) => {
                decode_io_payload(event, MutationSource::MarketDiff, vec![])
            }
            _ => Ok(vec![]),
        }
    }

    fn decode_owned(&mut self, input: RuntimeInput) -> Result<Vec<NormalizedMutation>> {
        match input {
            RuntimeInput::Io(event) if event.domains.contains(&ProtocolDomain::Market) => {
                decode_io_payload_owned(event, MutationSource::MarketDiff, vec![])
            }
            _ => Ok(vec![]),
        }
    }

    fn recovery_commands(&self) -> Vec<RuntimeCommand> {
        let mut commands = Vec::new();

        if !self.quote_subscriptions.is_empty() {
            commands.push(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
                symbols: self
                    .quote_subscriptions
                    .iter()
                    .map(|symbol| Symbol::new(symbol.clone()))
                    .collect(),
            }));
        }

        if !self.trading_status_subscriptions.is_empty() {
            commands.push(RuntimeCommand::Market(
                MarketCommand::SubscribeTradingStatus {
                    symbols: self
                        .trading_status_subscriptions
                        .iter()
                        .map(|symbol| Symbol::new(symbol.clone()))
                        .collect(),
                },
            ));
        }

        commands.extend(
            self.charts
                .values()
                .cloned()
                .map(MarketCommand::SetChart)
                .map(RuntimeCommand::Market),
        );

        commands
    }
}
