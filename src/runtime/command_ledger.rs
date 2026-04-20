use std::collections::BTreeMap;

use serde_json::{Map, Value, json};

use crate::{
    commands::{
        MarketCommand, QueryCommand, ReplayCommand, RuntimeCommand, SchemaCommand, SystemCommand,
        TradeCommand,
    },
    ids::{CommandId, ProtocolDomain, Symbol},
};

#[derive(Debug, Default)]
pub(crate) struct CommandLedger {
    next_command_id: u64,
    command_domains: BTreeMap<CommandId, ProtocolDomain>,
    command_detail_seeds: BTreeMap<CommandId, Map<String, Value>>,
}

impl CommandLedger {
    pub(crate) fn new() -> Self {
        Self {
            next_command_id: 1,
            command_domains: BTreeMap::new(),
            command_detail_seeds: BTreeMap::new(),
        }
    }

    pub(crate) fn allocate(
        &mut self,
        domain: ProtocolDomain,
        detail_seed: Map<String, Value>,
    ) -> CommandId {
        let command_id = CommandId::new(self.next_command_id);
        self.next_command_id += 1;
        self.command_domains.insert(command_id, domain);
        if !detail_seed.is_empty() {
            self.command_detail_seeds.insert(command_id, detail_seed);
        }
        command_id
    }

    pub(crate) fn domain(&self, command_id: CommandId) -> Option<ProtocolDomain> {
        self.command_domains.get(&command_id).copied()
    }

    pub(crate) fn detail_seed(&self, command_id: CommandId) -> Option<&Map<String, Value>> {
        self.command_detail_seeds.get(&command_id)
    }

    pub(crate) fn release(&mut self, command_id: CommandId) {
        self.command_domains.remove(&command_id);
        self.command_detail_seeds.remove(&command_id);
    }
}

pub(crate) fn merged_detail_from_seed(
    seed: Option<&Map<String, Value>>,
    detail: Option<Value>,
) -> Value {
    let mut merged = seed.cloned().unwrap_or_default();

    match detail {
        Some(Value::Object(fields)) => {
            merged.extend(fields);
            Value::Object(merged)
        }
        Some(value) if merged.is_empty() => value,
        Some(value) => {
            merged.insert("value".to_string(), value);
            Value::Object(merged)
        }
        None if merged.is_empty() => Value::Null,
        None => Value::Object(merged),
    }
}

pub(crate) fn command_detail_fields_from_command(cmd: &RuntimeCommand) -> Map<String, Value> {
    let mut detail = Map::new();

    match cmd {
        RuntimeCommand::System(SystemCommand::Shutdown) => {
            detail.insert("label".to_string(), json!("shutdown-runtime"));
        }
        RuntimeCommand::System(SystemCommand::RefreshAuth) => {
            detail.insert("label".to_string(), json!("refresh-auth"));
        }
        RuntimeCommand::Market(MarketCommand::SubscribeQuotes { symbols }) => {
            detail.insert("aid".to_string(), json!("subscribe_quote"));
            detail.insert("operation".to_string(), json!("subscribe"));
            detail.insert("symbols".to_string(), symbols_json(symbols));
        }
        RuntimeCommand::Market(MarketCommand::UnsubscribeQuotes { symbols }) => {
            detail.insert("aid".to_string(), json!("subscribe_quote"));
            detail.insert("operation".to_string(), json!("unsubscribe"));
            detail.insert("symbols".to_string(), symbols_json(symbols));
        }
        RuntimeCommand::Market(MarketCommand::SetChart(chart)) => {
            detail.insert("aid".to_string(), json!("set_chart"));
            detail.insert("chart_id".to_string(), json!(chart.chart_id));
            detail.insert("symbols".to_string(), symbols_json(&chart.symbols));
            detail.insert("duration_ns".to_string(), json!(chart.duration_ns));
            detail.insert("view_width".to_string(), json!(chart.view_width));
            if let Some(left_kline_id) = chart.left_kline_id {
                detail.insert("left_kline_id".to_string(), json!(left_kline_id));
            }
            if let Some(focus_datetime_ns) = chart.focus_datetime_ns {
                detail.insert("focus_datetime_ns".to_string(), json!(focus_datetime_ns));
            }
            if let Some(focus_position) = chart.focus_position {
                detail.insert("focus_position".to_string(), json!(focus_position));
            }
        }
        RuntimeCommand::Market(MarketCommand::CancelChart { chart_id }) => {
            detail.insert("aid".to_string(), json!("set_chart"));
            detail.insert("operation".to_string(), json!("cancel"));
            detail.insert("chart_id".to_string(), json!(chart_id));
        }
        RuntimeCommand::Market(MarketCommand::SubscribeTradingStatus { symbols }) => {
            detail.insert("aid".to_string(), json!("subscribe_trading_status"));
            detail.insert("operation".to_string(), json!("subscribe"));
            detail.insert("symbols".to_string(), symbols_json(symbols));
        }
        RuntimeCommand::Market(MarketCommand::UnsubscribeTradingStatus { symbols }) => {
            detail.insert("aid".to_string(), json!("subscribe_trading_status"));
            detail.insert("operation".to_string(), json!("unsubscribe"));
            detail.insert("symbols".to_string(), symbols_json(symbols));
        }
        RuntimeCommand::Trade(TradeCommand::Login(login)) => {
            detail.insert("aid".to_string(), json!("req_login"));
            detail.insert("account_id".to_string(), json!(login.account_id.as_str()));
            detail.insert("broker_id".to_string(), json!(login.broker_id));
            detail.insert(
                "account_type".to_string(),
                json!(login.account_type.as_str()),
            );
        }
        RuntimeCommand::Trade(TradeCommand::ConfirmSettlement { account_id }) => {
            detail.insert("aid".to_string(), json!("confirm_settlement"));
            detail.insert("account_id".to_string(), json!(account_id.as_str()));
        }
        RuntimeCommand::Trade(TradeCommand::QueryAccountInfo { account_id }) => {
            detail.insert("aid".to_string(), json!("qry_account_info"));
            detail.insert("account_id".to_string(), json!(account_id.as_str()));
        }
        RuntimeCommand::Trade(TradeCommand::QueryAccountRegister { account_id }) => {
            detail.insert("aid".to_string(), json!("qry_account_register"));
            detail.insert("account_id".to_string(), json!(account_id.as_str()));
        }
        RuntimeCommand::Trade(TradeCommand::QuerySettlementInfo {
            account_id,
            trading_day,
        }) => {
            detail.insert("aid".to_string(), json!("qry_settlement_info"));
            detail.insert("account_id".to_string(), json!(account_id.as_str()));
            detail.insert("trading_day".to_string(), json!(trading_day.to_string()));
        }
        RuntimeCommand::Trade(TradeCommand::PreInsertOrder(order)) => {
            detail.insert("aid".to_string(), json!("pre_insert_order"));
            detail.insert("account_id".to_string(), json!(order.account_id.as_str()));
            detail.insert("order_id".to_string(), json!(order.order_id.as_str()));
            detail.insert("symbol".to_string(), json!(order.symbol.as_str()));
            detail.insert("hedge_flag".to_string(), json!(order.hedge_flag));
            detail.insert(
                "contingent_condition".to_string(),
                json!(order.contingent_condition),
            );
        }
        RuntimeCommand::Trade(TradeCommand::InsertOrder(order)) => {
            detail.insert("aid".to_string(), json!("insert_order"));
            detail.insert("account_id".to_string(), json!(order.account_id.as_str()));
            detail.insert("order_id".to_string(), json!(order.order_id.as_str()));
            detail.insert("symbol".to_string(), json!(order.symbol.as_str()));
        }
        RuntimeCommand::Trade(TradeCommand::CancelOrder {
            account_id,
            order_id,
        }) => {
            detail.insert("aid".to_string(), json!("cancel_order"));
            detail.insert("account_id".to_string(), json!(account_id.as_str()));
            detail.insert("order_id".to_string(), json!(order_id.as_str()));
        }
        RuntimeCommand::Trade(TradeCommand::Transfer {
            account_id,
            bank_id,
            future_account,
            currency,
            amount,
            ..
        }) => {
            detail.insert("aid".to_string(), json!("req_transfer"));
            detail.insert("account_id".to_string(), json!(account_id.as_str()));
            detail.insert("bank_id".to_string(), json!(bank_id));
            detail.insert("future_account".to_string(), json!(future_account));
            detail.insert("currency".to_string(), json!(currency));
            detail.insert("amount".to_string(), amount.clone());
        }
        RuntimeCommand::Trade(TradeCommand::SetRiskManagementRule { account_id, rule }) => {
            detail.insert("aid".to_string(), json!("set_risk_management_rule"));
            detail.insert("account_id".to_string(), json!(account_id.as_str()));
            if let Some(exchange_id) = rule.get("exchange_id").and_then(Value::as_str) {
                detail.insert("exchange_id".to_string(), json!(exchange_id));
            }
        }
        RuntimeCommand::Replay(ReplayCommand::Step) => {
            detail.insert("action".to_string(), json!("step"));
        }
        RuntimeCommand::Replay(ReplayCommand::Reset) => {
            detail.insert("action".to_string(), json!("reset"));
        }
        RuntimeCommand::Query(QueryCommand::Fetch { query_id, .. }) => {
            detail.insert("aid".to_string(), json!("ins_query"));
            detail.insert("query_id".to_string(), json!(query_id.as_str()));
        }
        RuntimeCommand::Schema(SchemaCommand::Refresh { schema_id, path }) => {
            detail.insert("schema_id".to_string(), json!(schema_id.as_str()));
            detail.insert("path".to_string(), json!(path));
        }
    }

    detail
}

fn symbols_json(symbols: &[Symbol]) -> Value {
    json!(
        symbols
            .iter()
            .map(|symbol| symbol.as_str())
            .collect::<Vec<_>>()
    )
}
