use crate::{
    commands::{
        MarketChartCommand, MarketCommand, OutboundFrame, OutboundRequest, QueryCommand, ReplayCommand, RuntimeCommand,
        SchemaCommand, SystemCommand, TradeCommand, TradeInsertOrderCommand,
    },
    error::{ContractError, Result},
    events::{
        AuthEvent, FieldMutation, InputPayload, InternalEvent, IoEvent, MutationSource, NormalizedMutation,
        ReplayEvent, RuntimeInput,
    },
    ids::{AccountId, OrderId, ProtocolDomain, QueryId, ReplaySessionId, SchemaId, Symbol, TradeId},
    state::{ObjectKey, SeriesKey, StatePath},
};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub trait ProtocolAdapter: Send + Sync {
    fn domain(&self) -> ProtocolDomain;
    fn accepts_command(&self, cmd: &RuntimeCommand) -> bool;
    fn encode(&mut self, cmd: &RuntimeCommand) -> Result<Vec<OutboundRequest>>;
    fn accepts_input(&self, input: &RuntimeInput) -> bool;
    fn decode(&mut self, input: &RuntimeInput) -> Result<Vec<NormalizedMutation>>;
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
        self.register_adapter(SystemAdapter::default());
        self.register_adapter(MarketAdapter::default());
        self.register_adapter(TradeAdapter::default());
        self.register_adapter(ReplayAdapter::default());
        self.register_adapter(QueryAdapter::default());
        self.register_adapter(SchemaAdapter::default());
    }

    pub fn owning_domain(&self, cmd: &RuntimeCommand) -> Option<ProtocolDomain> {
        self.adapters
            .iter()
            .find(|adapter| adapter.accepts_command(cmd))
            .map(|adapter| adapter.domain())
    }

    pub fn encode_command(&mut self, cmd: &RuntimeCommand) -> Result<Vec<OutboundRequest>> {
        let Some(adapter) = self.adapters.iter_mut().find(|adapter| adapter.accepts_command(cmd)) else {
            return Err(ContractError::UnsupportedCommand(cmd.domain().as_str()));
        };
        adapter.encode(cmd)
    }

    pub fn decode_input(&mut self, input: &RuntimeInput) -> Result<Vec<NormalizedMutation>> {
        let mut decoded = Vec::new();
        for adapter in self.adapters.iter_mut().filter(|adapter| adapter.accepts_input(input)) {
            decoded.extend(adapter.decode(input)?);
        }
        Ok(decoded)
    }

    pub fn domains(&self) -> &[ProtocolDomain] {
        &self.domains
    }

    fn register_boxed_adapter(&mut self, adapter: Box<dyn ProtocolAdapter>) {
        let domain = adapter.domain();
        self.register_domain(domain);

        if let Some(index) = self.adapters.iter().position(|existing| existing.domain() == domain) {
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
            RuntimeCommand::System(SystemCommand::Shutdown) => Ok(vec![OutboundRequest::internal_label("shutdown-runtime")]),
            RuntimeCommand::System(SystemCommand::RefreshAuth) => Ok(vec![OutboundRequest::internal_label("refresh-auth")]),
            _ => Err(ContractError::UnsupportedCommand("system")),
        }
    }

    fn accepts_input(&self, input: &RuntimeInput) -> bool {
        match input {
            RuntimeInput::Auth(_) | RuntimeInput::Internal(_) => true,
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
            RuntimeInput::Internal(event) => decode_named_payload(
                ["system".to_string(), "internal".to_string()].to_vec(),
                event,
                MutationSource::SessionControl,
            ),
            RuntimeInput::Io(event) if event.domains.contains(&ProtocolDomain::System) => {
                decode_io_payload(event, MutationSource::SessionControl, vec!["system".to_string(), event.route.clone()])
            }
            _ => Ok(vec![]),
        }
    }
}

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
                Ok(request_with_peek(json!({
                    "aid": "subscribe_quote",
                    "ins_list": join_symbols(&self.quote_subscriptions),
                })))
            }
            RuntimeCommand::Market(MarketCommand::UnsubscribeQuotes { symbols }) => {
                remove_symbols(&mut self.quote_subscriptions, symbols);
                Ok(request_with_peek(json!({
                    "aid": "subscribe_quote",
                    "ins_list": join_symbols(&self.quote_subscriptions),
                })))
            }
            RuntimeCommand::Market(MarketCommand::SetChart(chart)) => {
                validate_chart_request(chart)?;
                self.charts.insert(chart.chart_id.clone(), chart.clone());
                Ok(request_with_peek(build_chart_request(chart, false)?))
            }
            RuntimeCommand::Market(MarketCommand::CancelChart { chart_id }) => {
                let Some(chart) = self.charts.remove(chart_id) else {
                    return Err(ContractError::validation(format!(
                        "unknown chart_id for cancel_chart: {chart_id}"
                    )));
                };
                Ok(request_with_peek(build_chart_request(&chart, true)?))
            }
            RuntimeCommand::Market(MarketCommand::SubscribeTradingStatus { symbols }) => {
                extend_symbols(&mut self.trading_status_subscriptions, symbols);
                Ok(request_with_peek(json!({
                    "aid": "subscribe_trading_status",
                    "ins_list": join_symbols(&self.trading_status_subscriptions),
                })))
            }
            RuntimeCommand::Market(MarketCommand::UnsubscribeTradingStatus { symbols }) => {
                remove_symbols(&mut self.trading_status_subscriptions, symbols);
                Ok(request_with_peek(json!({
                    "aid": "subscribe_trading_status",
                    "ins_list": join_symbols(&self.trading_status_subscriptions),
                })))
            }
            _ => Err(ContractError::UnsupportedCommand("market")),
        }
    }

    fn accepts_input(&self, input: &RuntimeInput) -> bool {
        matches!(input, RuntimeInput::Io(IoEvent { domains, .. }) if domains.contains(&ProtocolDomain::Market))
    }

    fn decode(&mut self, input: &RuntimeInput) -> Result<Vec<NormalizedMutation>> {
        match input {
            RuntimeInput::Io(event) if event.domains.contains(&ProtocolDomain::Market) => {
                decode_io_payload(event, MutationSource::MarketDiff, vec![])
            }
            _ => Ok(vec![]),
        }
    }
}

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
        let request = match cmd {
            RuntimeCommand::Trade(TradeCommand::Login(login)) => {
                let mut request = Map::from_iter([
                    ("aid".to_string(), json!("req_login")),
                    ("bid".to_string(), json!(login.broker_id)),
                    ("user_name".to_string(), json!(login.account_id.as_str())),
                    ("password".to_string(), json!(login.password)),
                ]);
                insert_optional(&mut request, "client_app_id", login.client_app_id.clone());
                insert_optional(&mut request, "client_system_info", login.client_system_info.clone());
                insert_optional(&mut request, "broker_id", login.front_broker.clone());
                insert_optional(&mut request, "front", login.front_url.clone());
                Value::Object(request)
            }
            RuntimeCommand::Trade(TradeCommand::ConfirmSettlement { .. }) => json!({"aid": "confirm_settlement"}),
            RuntimeCommand::Trade(TradeCommand::QueryAccountInfo { account_id }) => json!({
                "aid": "query_account_info",
                "user_id": account_id.as_str(),
            }),
            RuntimeCommand::Trade(TradeCommand::QueryAccountRegister { account_id }) => json!({
                "aid": "query_account_register",
                "user_id": account_id.as_str(),
            }),
            RuntimeCommand::Trade(TradeCommand::QuerySettlementInfo {
                account_id,
                trading_day,
            }) => json!({
                "aid": "qry_settlement_info",
                "user_name": account_id.as_str(),
                "trading_day": trading_day.to_string(),
            }),
            RuntimeCommand::Trade(TradeCommand::InsertOrder(order)) => build_insert_order_request(order)?,
            RuntimeCommand::Trade(TradeCommand::CancelOrder {
                account_id,
                order_id,
            }) => json!({
                "aid": "cancel_order",
                "user_id": account_id.as_str(),
                "order_id": order_id.as_str(),
            }),
            RuntimeCommand::Trade(TradeCommand::Transfer {
                account_id,
                bank_id,
                bank_password,
                future_account,
                future_password,
                currency,
                amount,
            }) => json!({
                "aid": "req_transfer",
                "user_id": account_id.as_str(),
                "bank_id": bank_id,
                "bank_password": bank_password,
                "future_account": future_account,
                "future_password": future_password,
                "currency": currency,
                "amount": amount,
            }),
            RuntimeCommand::Trade(TradeCommand::SetRiskManagementRule { account_id, rule }) => json!({
                "aid": "set_risk_management_rule",
                "user_id": account_id.as_str(),
                "rule": rule,
            }),
            _ => return Err(ContractError::UnsupportedCommand("trade")),
        };

        Ok(vec![json_request(request)])
    }

    fn accepts_input(&self, input: &RuntimeInput) -> bool {
        matches!(input, RuntimeInput::Io(IoEvent { domains, .. }) if domains.contains(&ProtocolDomain::Trade))
    }

    fn decode(&mut self, input: &RuntimeInput) -> Result<Vec<NormalizedMutation>> {
        match input {
            RuntimeInput::Io(event) if event.domains.contains(&ProtocolDomain::Trade) => {
                decode_io_payload(event, MutationSource::TradeReply, vec![])
            }
            _ => Ok(vec![]),
        }
    }
}

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
            }) => {
                let mut request = Map::from_iter([
                    ("aid".to_string(), json!("ins_query")),
                    ("query_id".to_string(), json!(query_id.as_str())),
                    ("query".to_string(), json!(query)),
                ]);
                if let Some(variables) = variables.clone() {
                    let should_include = !matches!(&variables, Value::Object(map) if map.is_empty());
                    if should_include {
                        request.insert("variables".to_string(), variables);
                    }
                }
                Ok(request_with_peek(Value::Object(request)))
            }
            _ => Err(ContractError::UnsupportedCommand("query")),
        }
    }

    fn accepts_input(&self, input: &RuntimeInput) -> bool {
        matches!(input, RuntimeInput::Io(IoEvent { domains, .. }) if domains.contains(&ProtocolDomain::Query))
    }

    fn decode(&mut self, input: &RuntimeInput) -> Result<Vec<NormalizedMutation>> {
        match input {
            RuntimeInput::Io(event) if event.domains.contains(&ProtocolDomain::Query) => {
                decode_io_payload(event, MutationSource::QueryResult, vec!["query".to_string()])
            }
            _ => Ok(vec![]),
        }
    }
}

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
                    return Err(ContractError::validation("schema refresh path must not be empty"));
                }
                Ok(vec![OutboundRequest::Http(crate::commands::HttpRequest {
                    path: path.clone(),
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
            RuntimeInput::Io(event) if event.domains.contains(&ProtocolDomain::Schema) => decode_io_payload(
                event,
                MutationSource::SchemaBootstrap,
                vec!["schema".to_string(), event.route.clone()],
            ),
            _ => Ok(vec![]),
        }
    }
}

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
                Ok(vec![OutboundRequest::Replay(crate::commands::ReplayRequest { action: "step" })])
            }
            RuntimeCommand::Replay(ReplayCommand::Reset) => {
                Ok(vec![OutboundRequest::Replay(crate::commands::ReplayRequest { action: "reset" })])
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

fn json_request(value: Value) -> OutboundRequest {
    OutboundRequest::Transport(OutboundFrame::Text(value.to_string()))
}

fn request_with_peek(value: Value) -> Vec<OutboundRequest> {
    vec![json_request(value), json_request(json!({"aid": "peek_message"}))]
}

fn extend_symbols(target: &mut BTreeSet<String>, symbols: &[crate::ids::Symbol]) {
    target.extend(symbols.iter().map(|symbol| symbol.as_str().to_string()));
}

fn remove_symbols(target: &mut BTreeSet<String>, symbols: &[crate::ids::Symbol]) {
    for symbol in symbols {
        target.remove(symbol.as_str());
    }
}

fn join_symbols(symbols: &BTreeSet<String>) -> String {
    symbols.iter().cloned().collect::<Vec<_>>().join(",")
}

fn validate_chart_request(chart: &MarketChartCommand) -> Result<()> {
    if chart.chart_id.is_empty() {
        return Err(ContractError::validation("chart_id must not be empty"));
    }
    if chart.symbols.is_empty() {
        return Err(ContractError::validation("set_chart requires at least one symbol"));
    }
    if chart.focus_datetime_ns.is_some() ^ chart.focus_position.is_some() {
        return Err(ContractError::validation(
            "focus_datetime_ns and focus_position must be provided together",
        ));
    }
    Ok(())
}

fn build_chart_request(chart: &MarketChartCommand, cancel: bool) -> Result<Value> {
    let mut request = Map::from_iter([
        ("aid".to_string(), json!("set_chart")),
        ("chart_id".to_string(), json!(chart.chart_id)),
        (
            "ins_list".to_string(),
            json!(if cancel {
                String::new()
            } else {
                chart
                    .symbols
                    .iter()
                    .map(|symbol| symbol.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            }),
        ),
        ("duration".to_string(), json!(chart.duration_ns)),
        ("view_width".to_string(), json!(chart.view_width)),
    ]);

    if !cancel {
        if let Some(left_kline_id) = chart.left_kline_id {
            request.insert("left_kline_id".to_string(), json!(left_kline_id));
        } else if let (Some(focus_datetime_ns), Some(focus_position)) =
            (chart.focus_datetime_ns, chart.focus_position)
        {
            request.insert("focus_datetime".to_string(), json!(focus_datetime_ns));
            request.insert("focus_position".to_string(), json!(focus_position));
        }
    }

    Ok(Value::Object(request))
}

fn insert_optional(map: &mut Map<String, Value>, key: &str, value: Option<String>) {
    if let Some(value) = value {
        map.insert(key.to_string(), json!(value));
    }
}

fn build_insert_order_request(order: &TradeInsertOrderCommand) -> Result<Value> {
    let (exchange_id, instrument_id) = split_symbol(order.symbol.as_str())?;
    let mut request = Map::from_iter([
        ("aid".to_string(), json!("insert_order")),
        ("user_id".to_string(), json!(order.account_id.as_str())),
        ("order_id".to_string(), json!(order.order_id.as_str())),
        ("exchange_id".to_string(), json!(exchange_id)),
        ("instrument_id".to_string(), json!(instrument_id)),
        ("direction".to_string(), json!(order.direction.as_str())),
        ("volume".to_string(), json!(order.volume)),
        ("price_type".to_string(), json!(order.price_type.as_str())),
        ("time_condition".to_string(), json!(order.time_condition.as_str())),
        (
            "volume_condition".to_string(),
            json!(order.volume_condition.as_str()),
        ),
    ]);
    if let Some(offset) = order.offset {
        request.insert("offset".to_string(), json!(offset.as_str()));
    }
    if let Some(limit_price) = order.limit_price.clone() {
        request.insert("limit_price".to_string(), limit_price);
    }
    Ok(Value::Object(request))
}

fn split_symbol(symbol: &str) -> Result<(&str, &str)> {
    symbol
        .split_once('.')
        .ok_or_else(|| ContractError::validation(format!("invalid symbol format: {symbol}")))
}

fn decode_named_payload(base: Vec<String>, event: &impl NamedPayloadEvent, source: MutationSource) -> Result<Vec<NormalizedMutation>> {
    let mut path = base;
    path.push(event.label().to_string());

    match event.payload() {
        Some(Value::Object(_)) => decode_json_value(event.payload().unwrap(), source, path),
        Some(value) => Ok(vec![NormalizedMutation {
            path: StatePath::new(path),
            object: None,
            fields: vec![FieldMutation {
                field: "value".to_string(),
                value: value.clone(),
            }],
            source,
        }]),
        None => Ok(vec![NormalizedMutation {
            path: StatePath::new(path[..path.len().saturating_sub(1)].to_vec()),
            object: None,
            fields: vec![FieldMutation {
                field: "event".to_string(),
                value: json!(event.label()),
            }],
            source,
        }]),
    }
}

fn decode_replay_payload(event: &ReplayEvent) -> Result<Vec<NormalizedMutation>> {
    let mut prefix = vec!["replay".to_string()];
    if let Some(session_id) = &event.session_id {
        prefix.push(session_id.as_str().to_string());
    } else {
        prefix.push(event.label.to_string());
    }

    match &event.payload {
        Some(payload) => decode_json_value(payload, MutationSource::ReplayStep, prefix),
        None => Ok(vec![NormalizedMutation {
            path: StatePath::new(prefix),
            object: None,
            fields: vec![FieldMutation {
                field: "event".to_string(),
                value: json!(event.label),
            }],
            source: MutationSource::ReplayStep,
        }]),
    }
}

fn decode_io_payload(event: &IoEvent, source: MutationSource, prefix: Vec<String>) -> Result<Vec<NormalizedMutation>> {
    match &event.payload {
        InputPayload::Json(value) => decode_json_envelope(value, source, prefix),
        InputPayload::Text(_) | InputPayload::Binary(_) => Ok(vec![]),
    }
}

fn decode_json_envelope(value: &Value, source: MutationSource, prefix: Vec<String>) -> Result<Vec<NormalizedMutation>> {
    if value.get("aid").and_then(Value::as_str) == Some("rtn_data") {
        if let Some(data) = value.get("data").and_then(Value::as_array) {
            let mut mutations = Vec::new();
            for item in data {
                mutations.extend(decode_json_value(item, source, prefix.clone())?);
            }
            return Ok(mutations);
        }
    }

    decode_json_value(value, source, prefix)
}

fn decode_json_value(value: &Value, source: MutationSource, prefix: Vec<String>) -> Result<Vec<NormalizedMutation>> {
    let mut mutations = Vec::new();
    match value {
        Value::Object(map) => flatten_object(prefix, map, source, &mut mutations),
        _ if !prefix.is_empty() => mutations.push(NormalizedMutation {
            path: StatePath::new(prefix),
            object: None,
            fields: vec![FieldMutation {
                field: "value".to_string(),
                value: value.clone(),
            }],
            source,
        }),
        _ => {}
    }
    Ok(mutations)
}

fn flatten_object(
    path: Vec<String>,
    map: &Map<String, Value>,
    source: MutationSource,
    out: &mut Vec<NormalizedMutation>,
) {
    let mut fields = map
        .iter()
        .filter(|(_, value)| !matches!(value, Value::Object(_)))
        .map(|(field, value)| FieldMutation {
            field: field.clone(),
            value: value.clone(),
        })
        .collect::<Vec<_>>();
    fields.sort_by(|left, right| left.field.cmp(&right.field));

    if !path.is_empty() && !fields.is_empty() {
        out.push(NormalizedMutation {
            path: StatePath::new(path.clone()),
            object: infer_object_key_from_segments(&path),
            fields,
            source,
        });
    }

    for (field, value) in map {
        if let Value::Object(child) = value {
            let mut child_path = path.clone();
            child_path.push(field.clone());
            flatten_object(child_path, child, source, out);
        }
    }
}

fn infer_object_key_from_segments(path: &[String]) -> Option<ObjectKey> {
    match path {
        [root, symbol] if root == "quotes" => Some(ObjectKey::Quote {
            symbol: Symbol::new(symbol.clone()),
        }),
        [root, symbol, duration, bar_id] if root == "klines" => Some(ObjectKey::Kline {
            series: SeriesKey {
                primary: Symbol::new(symbol.clone()),
                secondary: vec![],
                duration_ns: duration.parse().ok()?,
                view_width: 0,
                right_id: None,
            },
            bar_id: bar_id.parse().ok()?,
        }),
        [root, symbol, tick_id] if root == "ticks" => Some(ObjectKey::Tick {
            symbol: Symbol::new(symbol.clone()),
            tick_id: tick_id.parse().ok()?,
        }),
        [root, account_id, branch, _currency] if root == "trade" && branch == "accounts" => Some(ObjectKey::Account {
            account_id: AccountId::new(account_id.clone()),
        }),
        [root, account_id, branch, symbol] if root == "trade" && branch == "positions" => Some(ObjectKey::Position {
            account_id: AccountId::new(account_id.clone()),
            symbol: Symbol::new(symbol.clone()),
        }),
        [root, account_id, branch, order_id] if root == "trade" && branch == "orders" => Some(ObjectKey::Order {
            account_id: AccountId::new(account_id.clone()),
            order_id: OrderId::new(order_id.clone()),
        }),
        [root, account_id, branch, trade_id] if root == "trade" && branch == "trades" => Some(ObjectKey::Trade {
            account_id: AccountId::new(account_id.clone()),
            trade_id: TradeId::new(trade_id.clone()),
        }),
        [root, query_id, ..] if root == "query" => Some(ObjectKey::QueryResult {
            query_id: QueryId::new(query_id.clone()),
        }),
        [root, schema_id, ..] if root == "schema" => Some(ObjectKey::SchemaNode {
            schema_id: SchemaId::new(schema_id.clone()),
        }),
        [root, session_id, ..] if root == "replay" => Some(ObjectKey::ReplayCursor {
            session_id: ReplaySessionId::new(session_id.clone()),
        }),
        _ => None,
    }
}

trait NamedPayloadEvent {
    fn label(&self) -> &'static str;
    fn payload(&self) -> Option<&Value>;
}

impl NamedPayloadEvent for AuthEvent {
    fn label(&self) -> &'static str {
        self.label
    }

    fn payload(&self) -> Option<&Value> {
        self.payload.as_ref()
    }
}

impl NamedPayloadEvent for InternalEvent {
    fn label(&self) -> &'static str {
        self.label
    }

    fn payload(&self) -> Option<&Value> {
        self.payload.as_ref()
    }
}
