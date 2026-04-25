use serde_json::{Map, Value, json};
use std::collections::BTreeSet;

use crate::{
    commands::{
        MarketChartCommand, OutboundFrame, OutboundRequest, TradeInsertOrderCommand,
        TradeLoginCommand, TradePreInsertOrderCommand,
    },
    diff_protocol::{
        DiffInboundAid, DiffLoginRequest, DiffOrderRequest, DiffPreInsertOrderRequest,
        DiffProtocolMessage, DiffSetChartRequest,
    },
    error::{ContractError, Result},
    events::{
        AuthEvent, FieldMutation, InputPayload, InternalEvent, IoEvent, MutationSource,
        NormalizedMutation, ReplayEvent, TimerEvent,
    },
    ids::{
        AccountId, ChartId, NotificationId, OrderId, QueryId, ReplaySessionId, SchemaId, Symbol,
        TradeId,
    },
    state::{ObjectKey, SeriesKey, StatePath},
};

pub(super) fn diff_request(message: DiffProtocolMessage) -> Result<OutboundRequest> {
    Ok(json_request(message.into_value()?))
}

pub(super) fn request_with_peek(message: DiffProtocolMessage) -> Result<Vec<OutboundRequest>> {
    Ok(vec![
        diff_request(message)?,
        diff_request(DiffProtocolMessage::peek_message())?,
    ])
}

pub(super) fn extend_symbols(target: &mut BTreeSet<String>, symbols: &[crate::ids::Symbol]) {
    target.extend(symbols.iter().map(|symbol| symbol.as_str().to_string()));
}

pub(super) fn remove_symbols(target: &mut BTreeSet<String>, symbols: &[crate::ids::Symbol]) {
    for symbol in symbols {
        target.remove(symbol.as_str());
    }
}

pub(super) fn join_symbols(symbols: &BTreeSet<String>) -> String {
    symbols.iter().cloned().collect::<Vec<_>>().join(",")
}

pub(super) fn validate_chart_request(chart: &MarketChartCommand) -> Result<()> {
    if chart.chart_id.is_empty() {
        return Err(ContractError::validation("chart_id must not be empty"));
    }
    if chart.symbols.is_empty() {
        return Err(ContractError::validation(
            "set_chart requires at least one symbol",
        ));
    }
    if chart.focus_datetime_ns.is_some() ^ chart.focus_position.is_some() {
        return Err(ContractError::validation(
            "focus_datetime_ns and focus_position must be provided together",
        ));
    }
    Ok(())
}

pub(super) fn build_chart_message(chart: &MarketChartCommand, cancel: bool) -> DiffProtocolMessage {
    let ins_list = if cancel {
        String::new()
    } else {
        chart
            .symbols
            .iter()
            .map(|symbol| symbol.as_str())
            .collect::<Vec<_>>()
            .join(",")
    };

    let mut request = DiffSetChartRequest::new(
        chart.chart_id.clone(),
        ins_list,
        chart.duration_ns,
        chart.view_width,
    );
    if !cancel {
        if let Some(left_kline_id) = chart.left_kline_id {
            request = request.with_left_kline_id(left_kline_id);
        } else if let (Some(focus_datetime_ns), Some(focus_position)) =
            (chart.focus_datetime_ns, chart.focus_position)
        {
            request = request.with_focus(focus_datetime_ns, focus_position);
        }
    }

    DiffProtocolMessage::set_chart(request)
}

pub(super) fn build_login_message(login: &TradeLoginCommand) -> DiffProtocolMessage {
    let mut request = DiffLoginRequest::new(
        login.broker_id.clone(),
        login.account_id.as_str(),
        login.password.clone(),
    );
    request.client_app_id.clone_from(&login.client_app_id);
    request
        .client_system_info
        .clone_from(&login.client_system_info);
    request.broker_id.clone_from(&login.front_broker);
    request.front.clone_from(&login.front_url);
    DiffProtocolMessage::req_login(request)
}

pub(super) fn build_insert_order_message(
    order: &TradeInsertOrderCommand,
) -> Result<DiffProtocolMessage> {
    let (exchange_id, instrument_id) = split_symbol(order.symbol.as_str())?;
    Ok(DiffProtocolMessage::insert_order(DiffOrderRequest {
        user_id: order.account_id.as_str().to_string(),
        order_id: order.order_id.as_str().to_string(),
        exchange_id: exchange_id.to_string(),
        instrument_id: instrument_id.to_string(),
        direction: order.direction.as_str().to_string(),
        offset: order.offset.map(|offset| offset.as_str().to_string()),
        volume: order.volume,
        price_type: order.price_type.as_str().to_string(),
        limit_price: order.limit_price.clone(),
        time_condition: order.time_condition.as_str().to_string(),
        volume_condition: order.volume_condition.as_str().to_string(),
    }))
}

pub(super) fn build_pre_insert_order_message(
    order: &TradePreInsertOrderCommand,
) -> Result<DiffProtocolMessage> {
    let (exchange_id, instrument_id) = split_symbol(order.symbol.as_str())?;
    Ok(DiffProtocolMessage::pre_insert_order(
        DiffPreInsertOrderRequest {
            order: DiffOrderRequest {
                user_id: order.account_id.as_str().to_string(),
                order_id: order.order_id.as_str().to_string(),
                exchange_id: exchange_id.to_string(),
                instrument_id: instrument_id.to_string(),
                direction: order.direction.as_str().to_string(),
                offset: order.offset.map(|offset| offset.as_str().to_string()),
                volume: order.volume,
                price_type: order.price_type.as_str().to_string(),
                limit_price: order.limit_price.clone(),
                time_condition: order.time_condition.as_str().to_string(),
                volume_condition: order.volume_condition.as_str().to_string(),
            },
            hedge_flag: order.hedge_flag.clone(),
            contingent_condition: order.contingent_condition.clone(),
        },
    ))
}

pub(super) fn decode_named_payload(
    base: Vec<String>,
    event: &impl NamedPayloadEvent,
    source: MutationSource,
) -> Result<Vec<NormalizedMutation>> {
    let mut path = base;
    path.push(event.label().to_string());

    match event.payload() {
        Some(payload @ Value::Object(_)) => decode_json_value(payload, source, path),
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

pub(super) fn decode_replay_payload(event: &ReplayEvent) -> Result<Vec<NormalizedMutation>> {
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

pub(super) fn decode_system_io_payload(event: &IoEvent) -> Result<Vec<NormalizedMutation>> {
    match &event.payload {
        InputPayload::Json(value) => decode_json_envelope(
            value,
            MutationSource::SessionControl,
            vec!["system".to_string()],
        ),
        InputPayload::Text(_) | InputPayload::Binary(_) => Ok(vec![]),
    }
}

pub(super) fn decode_query_io_payload(event: &IoEvent) -> Result<Vec<NormalizedMutation>> {
    match &event.payload {
        InputPayload::Json(value) => decode_query_envelope(value),
        InputPayload::Text(_) | InputPayload::Binary(_) => Ok(vec![]),
    }
}

pub(super) fn is_query_io_event(event: &IoEvent) -> bool {
    match &event.payload {
        InputPayload::Json(value) => value_contains_query_payload(value),
        InputPayload::Text(_) | InputPayload::Binary(_) => false,
    }
}

pub(super) fn is_market_io_event(event: &IoEvent) -> bool {
    match &event.payload {
        InputPayload::Json(value) => value_contains_market_payload(value),
        InputPayload::Text(_) | InputPayload::Binary(_) => false,
    }
}

pub(super) fn is_trade_io_event(event: &IoEvent) -> bool {
    match &event.payload {
        InputPayload::Json(value) => value_contains_trade_payload(value),
        InputPayload::Text(_) | InputPayload::Binary(_) => false,
    }
}

pub(super) fn decode_trade_io_payload(event: &IoEvent) -> Result<Vec<NormalizedMutation>> {
    match &event.payload {
        InputPayload::Json(value) => {
            if DiffInboundAid::from_value(value) == DiffInboundAid::QrySettlementInfo {
                return decode_trade_settlement_query_reply(value);
            }
            decode_json_envelope(value, MutationSource::TradeReply, vec![])
        }
        InputPayload::Text(_) | InputPayload::Binary(_) => Ok(vec![]),
    }
}

pub(super) fn decode_schema_io_payload(event: &IoEvent) -> Result<Vec<NormalizedMutation>> {
    match &event.payload {
        InputPayload::Json(value) => {
            let schema_id = value
                .get("schema_id")
                .and_then(Value::as_str)
                .unwrap_or(event.route.as_str());
            let owned_payload;
            let payload = if let Some(data) = value.get("data") {
                data
            } else if value.get("schema_id").and_then(Value::as_str).is_some() {
                match value {
                    Value::Object(fields) => {
                        let mut fields = fields.clone();
                        fields.remove("schema_id");
                        owned_payload = Value::Object(fields);
                        &owned_payload
                    }
                    _ => value,
                }
            } else {
                value
            };

            decode_json_envelope(
                payload,
                MutationSource::SchemaBootstrap,
                vec!["schema".to_string(), schema_id.to_string()],
            )
        }
        InputPayload::Text(_) | InputPayload::Binary(_) => Ok(vec![]),
    }
}

pub(super) fn decode_io_payload(
    event: &IoEvent,
    source: MutationSource,
    prefix: Vec<String>,
) -> Result<Vec<NormalizedMutation>> {
    match &event.payload {
        InputPayload::Json(value) => decode_json_envelope(value, source, prefix),
        InputPayload::Text(_) | InputPayload::Binary(_) => Ok(vec![]),
    }
}

fn json_request(value: Value) -> OutboundRequest {
    OutboundRequest::Transport(OutboundFrame::Text(value.to_string()))
}

fn split_symbol(symbol: &str) -> Result<(&str, &str)> {
    symbol
        .split_once('.')
        .ok_or_else(|| ContractError::validation(format!("invalid symbol format: {symbol}")))
}

fn value_contains_market_payload(value: &Value) -> bool {
    if DiffInboundAid::from_value(value) == DiffInboundAid::RtnData
        && let Some(data) = value.get("data").and_then(Value::as_array)
    {
        return data.iter().any(value_contains_market_payload);
    }

    value.as_object().is_some_and(|object| {
        ["quotes", "trading_status", "charts", "klines", "ticks"]
            .iter()
            .any(|root| object.contains_key(*root))
    })
}

fn value_contains_trade_payload(value: &Value) -> bool {
    if DiffInboundAid::from_value(value) == DiffInboundAid::QrySettlementInfo {
        return true;
    }

    if DiffInboundAid::from_value(value) == DiffInboundAid::RtnData
        && let Some(data) = value.get("data").and_then(Value::as_array)
    {
        return data.iter().any(value_contains_trade_payload);
    }

    value
        .as_object()
        .is_some_and(|object| object.contains_key("trade"))
}

fn value_contains_query_payload(value: &Value) -> bool {
    if value.get("query_id").and_then(Value::as_str).is_some() || value.get("symbols").is_some() {
        return true;
    }

    if DiffInboundAid::from_value(value) == DiffInboundAid::RtnData {
        return value
            .get("data")
            .and_then(Value::as_array)
            .is_some_and(|items| items.iter().any(value_contains_query_payload));
    }

    false
}

fn decode_json_envelope(
    value: &Value,
    source: MutationSource,
    prefix: Vec<String>,
) -> Result<Vec<NormalizedMutation>> {
    if DiffInboundAid::from_value(value) == DiffInboundAid::RtnData
        && let Some(data) = value.get("data").and_then(Value::as_array)
    {
        let mut mutations = Vec::new();
        for item in data {
            mutations.extend(decode_json_value(item, source, prefix.clone())?);
        }
        return Ok(mutations);
    }

    decode_json_value(value, source, prefix)
}

fn decode_query_envelope(value: &Value) -> Result<Vec<NormalizedMutation>> {
    if DiffInboundAid::from_value(value) == DiffInboundAid::RtnData
        && let Some(data) = value.get("data").and_then(Value::as_array)
    {
        let mut mutations = Vec::new();
        for item in data {
            mutations.extend(decode_query_value(item)?);
        }
        return Ok(mutations);
    }

    decode_query_value(value)
}

fn decode_query_value(value: &Value) -> Result<Vec<NormalizedMutation>> {
    if let Some(query_id) = value.get("query_id").and_then(Value::as_str) {
        let mut fields = Map::new();
        if let Some(data) = value.get("data") {
            match data {
                Value::Object(map) => {
                    for (field, item) in map {
                        fields.insert(field.clone(), item.clone());
                    }
                }
                other => {
                    fields.insert("data".to_string(), other.clone());
                }
            }
        }
        if let Some(errors) = value.get("errors") {
            fields.insert("errors".to_string(), errors.clone());
        }
        if let Some(extensions) = value.get("extensions") {
            fields.insert("extensions".to_string(), extensions.clone());
        }

        return decode_json_value(
            &json!({
                query_id: Value::Object(fields),
            }),
            MutationSource::QueryResult,
            vec!["query".to_string()],
        );
    }

    if let Some(symbols) = value.get("symbols") {
        return decode_json_value(
            symbols,
            MutationSource::QueryResult,
            vec!["query".to_string()],
        );
    }
    decode_json_value(
        value,
        MutationSource::QueryResult,
        vec!["query".to_string()],
    )
}

fn decode_trade_settlement_query_reply(value: &Value) -> Result<Vec<NormalizedMutation>> {
    let Some(user_name) = value.get("user_name").and_then(Value::as_str) else {
        return Ok(vec![]);
    };
    let Some(trading_day) = value.get("trading_day").and_then(Value::as_str) else {
        return Ok(vec![]);
    };
    let Some(settlement_info) = value.get("settlement_info").and_then(Value::as_str) else {
        return Ok(vec![]);
    };

    decode_json_value(
        &json!({
            "trade": {
                user_name: {
                    "his_settlements": {
                        trading_day: {
                            "content": settlement_info,
                            "parsed": false,
                        }
                    }
                }
            }
        }),
        MutationSource::TradeReply,
        vec![],
    )
}

fn decode_json_value(
    value: &Value,
    source: MutationSource,
    prefix: Vec<String>,
) -> Result<Vec<NormalizedMutation>> {
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
        .filter(|(field, value)| {
            !matches!(value, Value::Object(_)) && !emits_scalar_leaf(&path, field)
        })
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
        let mut child_path = path.clone();
        child_path.push(field.clone());

        if let Value::Object(child) = value {
            flatten_object(child_path, child, source, out);
        } else if emits_scalar_leaf(&path, field) {
            out.push(NormalizedMutation {
                path: StatePath::new(child_path),
                object: infer_object_key_from_segments(&path),
                fields: vec![FieldMutation {
                    field: "value".to_string(),
                    value: value.clone(),
                }],
                source,
            });
        }
    }
}

fn emits_scalar_leaf(path: &[String], field: &str) -> bool {
    matches!(path, [root, _account_id] if root == "trade") && field == "trade_more_data"
}

fn infer_object_key_from_segments(path: &[String]) -> Option<ObjectKey> {
    match path {
        [root, symbol] if root == "quotes" => Some(ObjectKey::Quote {
            symbol: Symbol::new(symbol.clone()),
        }),
        [root, symbol] if root == "trading_status" => Some(ObjectKey::TradingStatus {
            symbol: Symbol::new(symbol.clone()),
        }),
        [root, chart_id] if root == "charts" => Some(ObjectKey::Chart {
            chart_id: ChartId::new(chart_id.clone()),
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
        [root, symbol, duration, branch, bar_id] if root == "klines" && branch == "data" => {
            Some(ObjectKey::Kline {
                series: SeriesKey {
                    primary: Symbol::new(symbol.clone()),
                    secondary: vec![],
                    duration_ns: duration.parse().ok()?,
                    view_width: 0,
                    right_id: None,
                },
                bar_id: bar_id.parse().ok()?,
            })
        }
        [root, symbol, tick_id] if root == "ticks" => Some(ObjectKey::Tick {
            symbol: Symbol::new(symbol.clone()),
            tick_id: tick_id.parse().ok()?,
        }),
        [root, symbol, branch, tick_id] if root == "ticks" && branch == "data" => {
            Some(ObjectKey::Tick {
                symbol: Symbol::new(symbol.clone()),
                tick_id: tick_id.parse().ok()?,
            })
        }
        [root, account_id, branch, _currency] if root == "trade" && branch == "accounts" => {
            Some(ObjectKey::Account {
                account_id: AccountId::new(account_id.clone()),
            })
        }
        [root, account_id, branch] if root == "trade" && branch == "session" => {
            Some(ObjectKey::TradeSession {
                account_id: AccountId::new(account_id.clone()),
            })
        }
        [root, account_id, branch, exchange_id]
            if root == "trade" && branch == "risk_management_rule" =>
        {
            Some(ObjectKey::RiskManagementRule {
                account_id: AccountId::new(account_id.clone()),
                exchange_id: exchange_id.clone(),
            })
        }
        [root, account_id, branch, symbol]
            if root == "trade" && branch == "risk_management_data" =>
        {
            Some(ObjectKey::RiskManagementData {
                account_id: AccountId::new(account_id.clone()),
                symbol: Symbol::new(symbol.clone()),
            })
        }
        [root, account_id, branch, symbol] if root == "trade" && branch == "positions" => {
            Some(ObjectKey::Position {
                account_id: AccountId::new(account_id.clone()),
                symbol: Symbol::new(symbol.clone()),
            })
        }
        [root, account_id, branch, order_id]
            if root == "trade" && branch == "pre_insert_orders" =>
        {
            Some(ObjectKey::PreInsertOrder {
                account_id: AccountId::new(account_id.clone()),
                order_id: OrderId::new(order_id.clone()),
            })
        }
        [root, account_id, branch, order_id] if root == "trade" && branch == "orders" => {
            Some(ObjectKey::Order {
                account_id: AccountId::new(account_id.clone()),
                order_id: OrderId::new(order_id.clone()),
            })
        }
        [root, account_id, branch, trade_id] if root == "trade" && branch == "trades" => {
            Some(ObjectKey::Trade {
                account_id: AccountId::new(account_id.clone()),
                trade_id: TradeId::new(trade_id.clone()),
            })
        }
        [root, account_id, branch, trading_day]
            if root == "trade" && branch == "his_settlements" =>
        {
            Some(ObjectKey::Settlement {
                account_id: AccountId::new(account_id.clone()),
                trading_day: trading_day.clone(),
            })
        }
        [root, query_id, ..] if root == "query" => Some(ObjectKey::QueryResult {
            query_id: QueryId::new(query_id.clone()),
        }),
        [root, schema_id, ..] if root == "schema" => Some(ObjectKey::SchemaNode {
            schema_id: SchemaId::new(schema_id.clone()),
        }),
        [root, session_id, ..] if root == "replay" => Some(ObjectKey::ReplayCursor {
            session_id: ReplaySessionId::new(session_id.clone()),
        }),
        [root, notification_id] if root == "notify" => Some(ObjectKey::Notification {
            notification_id: NotificationId::new(notification_id.clone()),
        }),
        [root, branch, notification_id] if root == "system" && branch == "notify" => {
            Some(ObjectKey::Notification {
                notification_id: NotificationId::new(notification_id.clone()),
            })
        }
        [root, branch, node] if root == "system" && branch == "auth" && node == "context" => {
            Some(ObjectKey::SessionAuth)
        }
        [root, branch, node] if root == "system" && branch == "session" && node == "lifecycle" => {
            Some(ObjectKey::SessionLifecycle)
        }
        [root, branch, node] if root == "system" && branch == "session" && node == "topology" => {
            Some(ObjectKey::SessionTopology)
        }
        [root, branch, node] if root == "system" && branch == "session" && node == "reconnect" => {
            Some(ObjectKey::SessionReconnect)
        }
        _ => None,
    }
}

pub(super) trait NamedPayloadEvent {
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

impl NamedPayloadEvent for TimerEvent {
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
