use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde_json::{Map, Value, json};

use crate::{
    commands::{
        CommandStatus, MarketCommand, QueryCommand, ReplayCommand, RuntimeCommand, SchemaCommand,
        SystemCommand, TradeCommand,
    },
    ids::{CommandId, ProtocolDomain, Symbol},
};

#[derive(Debug)]
pub(crate) struct CommandLedger {
    next_command_id: u64,
    command_domains: BTreeMap<CommandId, ProtocolDomain>,
    command_statuses: BTreeMap<CommandId, CommandStatus>,
    command_detail_seeds: BTreeMap<CommandId, Map<String, Value>>,
    retained_terminal_commands: VecDeque<CommandId>,
    retained_terminal_set: BTreeSet<CommandId>,
    evicted_terminal_commands: VecDeque<CommandId>,
    evicted_terminal_set: BTreeSet<CommandId>,
    max_retained_terminal_commands: usize,
    max_evicted_terminal_commands: usize,
}

impl CommandLedger {
    pub(crate) const DEFAULT_MAX_RETAINED_TERMINAL_COMMANDS: usize = 4_096;

    pub(crate) fn with_retention(max_retained_terminal_commands: usize) -> Self {
        let max_retained_terminal_commands = max_retained_terminal_commands.max(1);
        Self {
            next_command_id: 1,
            command_domains: BTreeMap::new(),
            command_statuses: BTreeMap::new(),
            command_detail_seeds: BTreeMap::new(),
            retained_terminal_commands: VecDeque::new(),
            retained_terminal_set: BTreeSet::new(),
            evicted_terminal_commands: VecDeque::new(),
            evicted_terminal_set: BTreeSet::new(),
            max_retained_terminal_commands,
            max_evicted_terminal_commands: max_retained_terminal_commands,
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
        self.command_statuses
            .insert(command_id, CommandStatus::Queued);
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

    pub(crate) fn status(&self, command_id: CommandId) -> Option<CommandStatus> {
        self.command_statuses.get(&command_id).copied()
    }

    pub(crate) fn update_status(&mut self, command_id: CommandId, status: CommandStatus) {
        if self.command_domains.contains_key(&command_id) {
            self.command_statuses.insert(command_id, status);
        }
    }

    pub(crate) fn release(&mut self, command_id: CommandId) {
        self.command_domains.remove(&command_id);
        self.command_statuses.remove(&command_id);
        self.command_detail_seeds.remove(&command_id);
    }

    pub(crate) fn is_evicted_terminal(&self, command_id: CommandId) -> bool {
        self.evicted_terminal_set.contains(&command_id)
    }

    pub(crate) fn pending_terminal_eviction(&self, command_id: CommandId) -> Option<CommandId> {
        if self.retained_terminal_set.contains(&command_id)
            || self.evicted_terminal_set.contains(&command_id)
        {
            return None;
        }

        (self.retained_terminal_commands.len() >= self.max_retained_terminal_commands)
            .then(|| self.retained_terminal_commands.front().copied())
            .flatten()
    }

    pub(crate) fn commit_terminal(
        &mut self,
        command_id: CommandId,
        evicted_command_id: Option<CommandId>,
    ) {
        self.release(command_id);

        if self.retained_terminal_set.contains(&command_id)
            || self.evicted_terminal_set.contains(&command_id)
        {
            return;
        }

        if let Some(evicted_command_id) = evicted_command_id
            && self.retained_terminal_commands.front().copied() == Some(evicted_command_id)
        {
            self.retained_terminal_commands.pop_front();
            self.retained_terminal_set.remove(&evicted_command_id);
            self.record_evicted_terminal(evicted_command_id);
        }

        self.retained_terminal_commands.push_back(command_id);
        self.retained_terminal_set.insert(command_id);
    }

    fn record_evicted_terminal(&mut self, command_id: CommandId) {
        if !self.evicted_terminal_set.insert(command_id) {
            return;
        }

        self.evicted_terminal_commands.push_back(command_id);
        while self.evicted_terminal_commands.len() > self.max_evicted_terminal_commands {
            if let Some(expired_command_id) = self.evicted_terminal_commands.pop_front() {
                self.evicted_terminal_set.remove(&expired_command_id);
            }
        }
    }
}

impl Default for CommandLedger {
    fn default() -> Self {
        Self::with_retention(Self::DEFAULT_MAX_RETAINED_TERMINAL_COMMANDS)
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
