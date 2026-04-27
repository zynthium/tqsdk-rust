#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashMap;

use serde_json::{Value, json};
use tqsdk_core::{
    AdapterRegistry, CommandStatus, CommitScope, InputPayload, IoEvent, Order, OutboundFrame,
    OutboundRequest, Position, ProtocolDomain, RuntimeHandle, RuntimeInput, Trade,
};
use tqsdk_session::SessionClient;
use tqsdk_wait::TqApi;

use crate::{Result, TaskError, TaskHost};

/// Builder entrypoint for deterministic strategy tests.
pub struct StrategyTestHarness {
    market: FakeMarket,
    broker: FakeBroker,
}

/// Compatibility alias for callers that prefer an explicit builder type name.
pub type StrategyTestHarnessBuilder = StrategyTestHarness;

/// Deterministic fake market seed data for strategy tests.
#[derive(Debug, Clone, Default)]
pub struct FakeMarket {
    quotes: Vec<FakeQuote>,
    accounts: Vec<FakeAccount>,
    positions: Vec<FakePosition>,
}

/// Deterministic fake broker policy for strategy tests.
#[derive(Debug, Clone)]
pub struct FakeBroker {
    policy: FakeBrokerPolicy,
}

/// Order handling policy used by [`FakeBroker`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FakeBrokerPolicy {
    FillAll,
    RejectAll { reason: String },
    PartialFill { volume: i64 },
}

/// Built fake test harness.
pub struct BuiltStrategyTestHarness {
    host: TaskHost,
}

/// Result of one fake strategy test step.
#[derive(Debug, Clone, Default)]
pub struct StrategyTestReport {
    orders: Vec<Order>,
    trades: Vec<Trade>,
    positions: HashMap<(String, String), Position>,
}

#[derive(Debug, Clone)]
struct FakeQuote {
    symbol: String,
    last_price: f64,
}

#[derive(Debug, Clone)]
struct FakeAccount {
    account_id: String,
    available: f64,
}

#[derive(Debug, Clone)]
struct FakePosition {
    account_id: String,
    symbol: String,
    net_position: i64,
}

pub(crate) struct StrategyTestRuntime {
    broker: FakeBroker,
    positions: HashMap<(String, String), Position>,
}

struct FakeOrderRequest {
    account_id: String,
    symbol: String,
    exchange_id: String,
    instrument_id: String,
    order_id: String,
    direction: String,
    offset: String,
    volume: i64,
    limit_price: f64,
}

impl StrategyTestHarness {
    #[must_use]
    pub fn new() -> Self {
        Self {
            market: FakeMarket::new(),
            broker: FakeBroker::new(),
        }
    }

    #[must_use]
    pub fn market(mut self, market: FakeMarket) -> Self {
        self.market = market;
        self
    }

    #[must_use]
    pub fn broker(mut self, broker: FakeBroker) -> Self {
        self.broker = broker;
        self
    }

    pub fn build(self) -> Result<BuiltStrategyTestHarness> {
        let mut adapters = AdapterRegistry::new();
        adapters.register_default_adapters();
        let handle = RuntimeHandle::with_adapters(adapters);
        let session = SessionClient::new_for_test_with_handle(handle);
        let mut host = TaskHost::new(TqApi::new(session));
        let positions = seed_market(&host, &self.market)?;
        host.strategy_test = Some(StrategyTestRuntime {
            broker: self.broker,
            positions,
        });

        Ok(BuiltStrategyTestHarness { host })
    }
}

impl Default for StrategyTestHarness {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeMarket {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn quote(mut self, symbol: impl AsRef<str>, last_price: f64) -> Self {
        self.quotes.push(FakeQuote {
            symbol: symbol.as_ref().to_owned(),
            last_price,
        });
        self
    }

    #[must_use]
    pub fn account(mut self, account_id: impl AsRef<str>, available: f64) -> Self {
        self.accounts.push(FakeAccount {
            account_id: account_id.as_ref().to_owned(),
            available,
        });
        self
    }

    #[must_use]
    pub fn position(
        mut self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
        net_position: i64,
    ) -> Self {
        self.positions.push(FakePosition {
            account_id: account_id.as_ref().to_owned(),
            symbol: symbol.as_ref().to_owned(),
            net_position,
        });
        self
    }
}

impl Default for FakeBroker {
    fn default() -> Self {
        Self {
            policy: FakeBrokerPolicy::FillAll,
        }
    }
}

impl FakeBroker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn fill_all(mut self) -> Self {
        self.policy = FakeBrokerPolicy::FillAll;
        self
    }

    #[must_use]
    pub fn reject_all(mut self, reason: impl Into<String>) -> Self {
        self.policy = FakeBrokerPolicy::RejectAll {
            reason: reason.into(),
        };
        self
    }

    #[must_use]
    pub fn partial_fill(mut self, volume: i64) -> Self {
        self.policy = FakeBrokerPolicy::PartialFill { volume };
        self
    }
}

impl BuiltStrategyTestHarness {
    #[must_use]
    pub fn into_task_host(self) -> TaskHost {
        self.host
    }
}

impl StrategyTestReport {
    #[must_use]
    pub fn orders(&self) -> &[Order] {
        &self.orders
    }

    #[must_use]
    pub fn trades(&self) -> &[Trade] {
        &self.trades
    }

    pub fn position(&self, account_id: impl AsRef<str>, symbol: impl AsRef<str>) -> Result<Position> {
        self.positions
            .get(&(account_id.as_ref().to_owned(), symbol.as_ref().to_owned()))
            .cloned()
            .ok_or(TaskError::InvalidState("strategy test position not ready"))
    }
}

pub(crate) async fn finish_test_step(host: &mut TaskHost) -> Result<StrategyTestReport> {
    let Some(mut runtime) = host.strategy_test.take() else {
        return Err(TaskError::InvalidState(
            "strategy test harness is not attached",
        ));
    };

    let dispatches = host.api().handle_for_test().drain_dispatches()?;
    let mut report = StrategyTestReport {
        positions: runtime.positions.clone(),
        ..StrategyTestReport::default()
    };

    for dispatch in dispatches {
        let Some(request) = fake_order_request(&dispatch.request)? else {
            continue;
        };

        let _ = host.api().handle_for_test().record_command_status(
            dispatch.command_id,
            CommandStatus::Sent,
            None,
            CommitScope::RealtimeUpdate,
        )?;
        let outcome = runtime.apply_order(request)?;
        let status = outcome.command_status;
        ingest_fake_trade_update(host, &outcome, dispatch.command_id)?;
        let _ = host.api().handle_for_test().record_command_status(
            dispatch.command_id,
            status,
            None,
            CommitScope::RealtimeUpdate,
        )?;

        report.orders.push(outcome.order);
        if let Some(trade) = outcome.trade {
            report.trades.push(trade);
        }
        report.positions = runtime.positions.clone();
    }

    host.strategy_test = Some(runtime);
    Ok(report)
}

struct FakeOrderOutcome {
    account_id: String,
    symbol: String,
    order_id: String,
    order_value: Value,
    trade_value: Option<Value>,
    position_value: Value,
    order: Order,
    trade: Option<Trade>,
    command_status: CommandStatus,
}

impl StrategyTestRuntime {
    fn apply_order(&mut self, request: FakeOrderRequest) -> Result<FakeOrderOutcome> {
        let (filled_volume, volume_left, status, lifecycle, is_dead, last_msg, command_status) =
            match self.broker.policy.clone() {
                FakeBrokerPolicy::FillAll => (
                    request.volume,
                    0,
                    "FINISHED",
                    "filled",
                    true,
                    String::new(),
                    CommandStatus::Completed,
                ),
                FakeBrokerPolicy::RejectAll { reason } => (
                    0,
                    request.volume,
                    "FINISHED",
                    "rejected",
                    true,
                    reason,
                    CommandStatus::Rejected,
                ),
                FakeBrokerPolicy::PartialFill { volume } => {
                    let filled = volume.clamp(0, request.volume);
                    (
                        filled,
                        request.volume - filled,
                        "ALIVE",
                        if filled > 0 {
                            "partially_filled"
                        } else {
                            "accepted"
                        },
                        false,
                        String::new(),
                        CommandStatus::PartiallyApplied,
                    )
                }
            };

        let position = self.apply_position(&request, filled_volume)?;
        let order_value = json!({
            "seqno": 1,
            "user_id": request.account_id,
            "order_id": request.order_id,
            "exchange_order_id": if last_msg.is_empty() {
                format!("fake-exchange-{}", request.order_id)
            } else {
                String::new()
            },
            "exchange_id": request.exchange_id,
            "instrument_id": request.instrument_id,
            "direction": request.direction,
            "offset": request.offset,
            "volume_orign": request.volume,
            "volume_left": volume_left,
            "limit_price": request.limit_price,
            "price_type": "LIMIT",
            "volume_condition": "ANY",
            "time_condition": "GFD",
            "insert_date_time": 1_777_222_800_000_000_000_i64,
            "last_msg": last_msg,
            "status": status,
            "lifecycle": lifecycle,
            "is_dead": is_dead,
            "trade_price": if filled_volume > 0 { request.limit_price } else { 0.0 },
        });
        let order = serde_json::from_value(order_value.clone())
            .map_err(|_| TaskError::InvalidState("fake order payload is invalid"))?;

        let trade_value = (filled_volume > 0).then(|| {
            json!({
                "seqno": 1,
                "user_id": request.account_id,
                "order_id": request.order_id,
                "trade_id": format!("fake-trade-{}", request.order_id),
                "exchange_trade_id": format!("fake-exchange-trade-{}", request.order_id),
                "exchange_id": request.exchange_id,
                "instrument_id": request.instrument_id,
                "direction": request.direction,
                "offset": request.offset,
                "price": request.limit_price,
                "volume": filled_volume,
                "trade_date_time": 1_777_222_800_100_000_000_i64,
                "commission": 0.0
            })
        });
        let trade = trade_value
            .clone()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|_| TaskError::InvalidState("fake trade payload is invalid"))?;
        let position_value = position_to_value(&position);

        Ok(FakeOrderOutcome {
            account_id: request.account_id,
            symbol: request.symbol,
            order_id: request.order_id,
            order_value,
            trade_value,
            position_value,
            order,
            trade,
            command_status,
        })
    }

    fn apply_position(&mut self, request: &FakeOrderRequest, filled_volume: i64) -> Result<Position> {
        let key = (request.account_id.clone(), request.symbol.clone());
        let mut position = self
            .positions
            .get(&key)
            .cloned()
            .unwrap_or_else(|| position_from_net(&request.account_id, &request.symbol, 0));
        let signed_delta = signed_position_delta(&request.direction, &request.offset, filled_volume);
        position.pos += signed_delta;
        position.pos_long = position.pos.max(0);
        position.pos_short = (-position.pos).max(0);
        position.volume_long = position.pos_long;
        position.volume_short = position.pos_short;
        self.positions.insert(key, position.clone());
        Ok(position)
    }
}

fn seed_market(host: &TaskHost, market: &FakeMarket) -> Result<HashMap<(String, String), Position>> {
    if !market.quotes.is_empty() {
        host.api().handle_for_test().ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "quotes": market.quotes.iter().map(|quote| {
                            (
                                quote.symbol.clone(),
                                json!({
                                    "instrument_id": quote.symbol,
                                    "datetime": "2026-04-27 09:30:00.000000",
                                    "last_price": quote.last_price
                                })
                            )
                        }).collect::<serde_json::Map<_, _>>()
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )?;
    }

    let mut positions = HashMap::new();
    if !market.accounts.is_empty() || !market.positions.is_empty() {
        let mut trade_accounts = serde_json::Map::new();
        for account in &market.accounts {
            trade_accounts
                .entry(account.account_id.clone())
                .or_insert_with(|| json!({ "accounts": {}, "positions": {} }));
            if let Some(account_node) = trade_accounts
                .get_mut(&account.account_id)
                .and_then(Value::as_object_mut)
            {
                account_node.insert(
                    "accounts".to_string(),
                    json!({
                        "CNY": {
                            "user_id": account.account_id,
                            "currency": "CNY",
                            "balance": account.available,
                            "available": account.available
                        }
                    }),
                );
            }
        }

        for position in &market.positions {
            let decoded = position_from_net(
                &position.account_id,
                &position.symbol,
                position.net_position,
            );
            positions.insert(
                (position.account_id.clone(), position.symbol.clone()),
                decoded.clone(),
            );
            trade_accounts
                .entry(position.account_id.clone())
                .or_insert_with(|| json!({ "accounts": {}, "positions": {} }));
            if let Some(account_node) = trade_accounts
                .get_mut(&position.account_id)
                .and_then(Value::as_object_mut)
            {
                let positions_node = account_node
                    .entry("positions".to_string())
                    .or_insert_with(|| json!({}));
                if let Some(positions_map) = positions_node.as_object_mut() {
                    positions_map.insert(position.symbol.clone(), position_to_value(&decoded));
                }
            }
        }

        host.api().handle_for_test().ingest(
            RuntimeInput::Io(IoEvent {
                route: "trade".to_string(),
                domains: vec![ProtocolDomain::Trade],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "trade": trade_accounts
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )?;
    }

    Ok(positions)
}

fn fake_order_request(request: &OutboundRequest) -> Result<Option<FakeOrderRequest>> {
    let payload: Value = match request {
        OutboundRequest::Transport(OutboundFrame::Text(text)) => serde_json::from_str(text)
            .map_err(|_| TaskError::InvalidState("fake broker received invalid text payload"))?,
        OutboundRequest::Transport(OutboundFrame::Binary(bytes)) => serde_json::from_slice(bytes)
            .map_err(|_| TaskError::InvalidState("fake broker received invalid binary payload"))?,
        _ => return Ok(None),
    };
    if payload.get("aid").and_then(Value::as_str) != Some("insert_order") {
        return Ok(None);
    }

    let exchange_id = required_string(&payload, "exchange_id")?;
    let instrument_id = required_string(&payload, "instrument_id")?;
    Ok(Some(FakeOrderRequest {
        account_id: required_string(&payload, "user_id")?,
        symbol: format!("{exchange_id}.{instrument_id}"),
        exchange_id,
        instrument_id,
        order_id: required_string(&payload, "order_id")?,
        direction: required_string(&payload, "direction")?,
        offset: required_string(&payload, "offset")?,
        volume: required_i64(&payload, "volume")?,
        limit_price: required_f64(&payload, "limit_price")?,
    }))
}

fn ingest_fake_trade_update(
    host: &TaskHost,
    outcome: &FakeOrderOutcome,
    command_id: tqsdk_core::CommandId,
) -> Result<()> {
    let mut account_node = serde_json::Map::new();
    account_node.insert(
        "orders".to_string(),
        json!({ outcome.order_id.clone(): outcome.order_value.clone() }),
    );
    account_node.insert(
        "positions".to_string(),
        json!({ outcome.symbol.clone(): outcome.position_value.clone() }),
    );
    if let Some(trade_value) = &outcome.trade_value {
        let trade_id = trade_value
            .get("trade_id")
            .and_then(Value::as_str)
            .ok_or(TaskError::InvalidState("fake trade id missing"))?;
        account_node.insert("trades".to_string(), json!({ trade_id: trade_value.clone() }));
    }

    host.api().handle_for_test().ingest(
        RuntimeInput::Io(IoEvent {
            route: "trade".to_string(),
            domains: vec![ProtocolDomain::Trade],
            payload: InputPayload::Json(json!({
                "aid": "rtn_data",
                "data": [{
                    "trade": {
                        outcome.account_id.clone(): Value::Object(account_node)
                    }
                }]
            })),
        }),
        vec![command_id],
        CommitScope::RealtimeUpdate,
    )?;

    Ok(())
}

fn position_from_net(account_id: &str, symbol: &str, net: i64) -> Position {
    let (exchange_id, instrument_id) = split_symbol(symbol);
    serde_json::from_value(position_json(
        account_id,
        symbol,
        exchange_id,
        instrument_id,
        net,
    ))
    .expect("static fake position payload should decode")
}

fn position_to_value(position: &Position) -> Value {
    let symbol = format!("{}.{}", position.exchange_id, position.instrument_id);
    position_json(
        &position.user_id,
        &symbol,
        &position.exchange_id,
        &position.instrument_id,
        position.pos,
    )
}

fn position_json(
    account_id: &str,
    _symbol: &str,
    exchange_id: &str,
    instrument_id: &str,
    net: i64,
) -> Value {
    json!({
        "user_id": account_id,
        "exchange_id": exchange_id,
        "instrument_id": instrument_id,
        "volume_long": net.max(0),
        "volume_short": (-net).max(0),
        "pos_long": net.max(0),
        "pos_short": (-net).max(0),
        "pos": net
    })
}

fn split_symbol(symbol: &str) -> (&str, &str) {
    symbol.split_once('.').unwrap_or(("", symbol))
}

fn signed_position_delta(direction: &str, offset: &str, volume: i64) -> i64 {
    match (direction, offset) {
        ("BUY", "OPEN") | ("BUY", "CLOSE") => volume,
        ("SELL", "OPEN") | ("SELL", "CLOSE") => -volume,
        _ => 0,
    }
}

fn required_string(payload: &Value, key: &'static str) -> Result<String> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or(TaskError::InvalidState("fake broker payload missing string field"))
}

fn required_i64(payload: &Value, key: &'static str) -> Result<i64> {
    payload
        .get(key)
        .and_then(Value::as_i64)
        .ok_or(TaskError::InvalidState("fake broker payload missing integer field"))
}

fn required_f64(payload: &Value, key: &'static str) -> Result<f64> {
    payload
        .get(key)
        .and_then(Value::as_f64)
        .ok_or(TaskError::InvalidState("fake broker payload missing float field"))
}
