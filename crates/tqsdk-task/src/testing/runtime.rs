use std::collections::{HashMap, VecDeque};

use serde_json::{Value, json};
use tqsdk_core::{
    CommandId, CommandStatus, CommitScope, InputPayload, IoEvent, Order, OutboundFrame,
    OutboundRequest, Position, ProtocolDomain, RuntimeInput, Trade,
};

use crate::{Result, TaskError, TaskHost};

use super::broker::{FakeBroker, FakeBrokerConnectionStatus, FakeBrokerPolicy};
use super::clock::StrategyTestClock;
use super::market::{position_from_net, position_to_value};
use super::report::StrategyTestReport;

pub(crate) struct StrategyTestRuntime {
    broker: FakeBroker,
    positions: HashMap<(String, String), Position>,
    clock: StrategyTestClock,
    pending_orders: VecDeque<PendingFakeOrder>,
    disconnect_remaining_steps: usize,
    was_disconnected: bool,
}

struct PendingFakeOrder {
    command_id: CommandId,
    request: FakeOrderRequest,
    remaining_steps: usize,
    sent: bool,
    fill_steps: VecDeque<i64>,
    filled_volume: i64,
    trade_sequence: usize,
    last_status: Option<CommandStatus>,
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

struct FakeOrderOutcome {
    account_id: String,
    symbol: String,
    order_id: String,
    order_value: Value,
    trade_value: Option<Value>,
    position_value: Value,
    order: Order,
    trade: Option<Trade>,
    command_status: Option<CommandStatus>,
}

pub(crate) async fn finish_test_step(host: &mut TaskHost) -> Result<StrategyTestReport> {
    let Some(mut runtime) = host.strategy_test.take() else {
        return Err(TaskError::InvalidState(
            "strategy test harness is not attached",
        ));
    };

    let dispatches = host.api().session().handle().drain_dispatches()?;
    let mut report = StrategyTestReport {
        positions: runtime.positions.clone(),
        ..StrategyTestReport::default()
    };
    let broker_connection_status = runtime.next_connection_status();
    let broker_connected = broker_connection_status != FakeBrokerConnectionStatus::Disconnected;
    report.broker_connection_status = broker_connection_status;

    if broker_connected {
        for (command_id, outcome) in runtime.advance_sent_pending_orders()? {
            ingest_fake_order_outcome(host, &mut report, &mut runtime, command_id, outcome)?;
        }
        for command_id in runtime.mark_unsent_pending_orders_sent() {
            let _ = host.api().session().handle().record_command_status(
                command_id,
                CommandStatus::Sent,
                None,
                CommitScope::RealtimeUpdate,
            )?;
        }
    }

    for dispatch in dispatches {
        let Some(request) = fake_order_request(&dispatch.request)? else {
            continue;
        };

        if broker_connected {
            let _ = host.api().session().handle().record_command_status(
                dispatch.command_id,
                CommandStatus::Sent,
                None,
                CommitScope::RealtimeUpdate,
            )?;
        }
        runtime.enqueue_order(dispatch.command_id, request, broker_connected);
    }

    if broker_connected {
        for (command_id, outcome) in runtime.drain_ready_orders()? {
            ingest_fake_order_outcome(host, &mut report, &mut runtime, command_id, outcome)?;
        }
    }

    report.pending_orders = runtime.pending_orders.len();
    host.strategy_test = Some(runtime);
    Ok(report)
}

impl StrategyTestRuntime {
    pub(super) fn new(
        broker: FakeBroker,
        positions: HashMap<(String, String), Position>,
        clock: StrategyTestClock,
    ) -> Self {
        let disconnect_remaining_steps = broker.disconnect_steps;
        Self {
            broker,
            positions,
            clock,
            pending_orders: VecDeque::new(),
            disconnect_remaining_steps,
            was_disconnected: false,
        }
    }

    fn next_connection_status(&mut self) -> FakeBrokerConnectionStatus {
        if self.disconnect_remaining_steps > 0 {
            self.disconnect_remaining_steps -= 1;
            self.was_disconnected = true;
            FakeBrokerConnectionStatus::Disconnected
        } else if self.was_disconnected {
            self.was_disconnected = false;
            FakeBrokerConnectionStatus::Reconnected
        } else {
            FakeBrokerConnectionStatus::Connected
        }
    }

    fn enqueue_order(&mut self, command_id: CommandId, request: FakeOrderRequest, sent: bool) {
        let fill_steps = self.fill_steps_for(&request);
        self.pending_orders.push_back(PendingFakeOrder {
            command_id,
            request,
            remaining_steps: self.broker.latency_steps,
            sent,
            fill_steps,
            filled_volume: 0,
            trade_sequence: 0,
            last_status: None,
        });
    }

    fn fill_steps_for(&self, request: &FakeOrderRequest) -> VecDeque<i64> {
        match &self.broker.policy {
            FakeBrokerPolicy::FillAll => VecDeque::from([request.volume]),
            FakeBrokerPolicy::RejectAll { .. } => VecDeque::from([0]),
            FakeBrokerPolicy::PartialFill { volume } => {
                VecDeque::from([(*volume).clamp(0, request.volume)])
            }
            FakeBrokerPolicy::PartialFills { volumes } => {
                let mut remaining = request.volume;
                let mut fills = VecDeque::new();
                for volume in volumes {
                    if remaining <= 0 {
                        break;
                    }
                    let filled = (*volume).clamp(0, remaining);
                    fills.push_back(filled);
                    remaining -= filled;
                }
                if fills.is_empty() {
                    fills.push_back(0);
                }
                fills
            }
        }
    }

    fn mark_unsent_pending_orders_sent(&mut self) -> Vec<CommandId> {
        let mut command_ids = Vec::new();
        for pending_order in &mut self.pending_orders {
            if !pending_order.sent {
                pending_order.sent = true;
                command_ids.push(pending_order.command_id);
            }
        }
        command_ids
    }

    fn advance_sent_pending_orders(&mut self) -> Result<Vec<(CommandId, FakeOrderOutcome)>> {
        for pending_order in &mut self.pending_orders {
            if pending_order.sent {
                pending_order.remaining_steps = pending_order.remaining_steps.saturating_sub(1);
            }
        }
        self.drain_ready_orders()
    }

    fn drain_ready_orders(&mut self) -> Result<Vec<(CommandId, FakeOrderOutcome)>> {
        let mut ready = Vec::new();
        let mut pending = VecDeque::new();
        while let Some(mut order) = self.pending_orders.pop_front() {
            if order.sent && order.remaining_steps == 0 {
                let outcome = self.apply_order(&mut order)?;
                let command_id = order.command_id;
                let stays_pending = order.sent
                    && order.request.volume > order.filled_volume
                    && !order.fill_steps.is_empty();
                ready.push((command_id, outcome));
                if stays_pending {
                    order.remaining_steps = 1;
                    pending.push_back(order);
                }
            } else {
                pending.push_back(order);
            }
        }
        self.pending_orders = pending;
        Ok(ready)
    }

    fn apply_order(&mut self, pending: &mut PendingFakeOrder) -> Result<FakeOrderOutcome> {
        let request = &pending.request;
        let (filled_volume, volume_left, status, lifecycle, is_dead, last_msg, next_status) =
            match self.broker.policy.clone() {
                FakeBrokerPolicy::FillAll | FakeBrokerPolicy::PartialFills { .. } => {
                    let filled = pending
                        .fill_steps
                        .pop_front()
                        .unwrap_or(request.volume - pending.filled_volume)
                        .clamp(0, request.volume - pending.filled_volume);
                    pending.filled_volume += filled;
                    let volume_left = request.volume - pending.filled_volume;
                    (
                        filled,
                        volume_left,
                        if volume_left == 0 {
                            "FINISHED"
                        } else {
                            "ALIVE"
                        },
                        if volume_left == 0 {
                            "filled"
                        } else if filled > 0 {
                            "partially_filled"
                        } else {
                            "accepted"
                        },
                        volume_left == 0,
                        String::new(),
                        if volume_left == 0 {
                            CommandStatus::Completed
                        } else {
                            CommandStatus::PartiallyApplied
                        },
                    )
                }
                FakeBrokerPolicy::RejectAll { reason } => {
                    pending.fill_steps.clear();
                    pending.filled_volume = request.volume;
                    (
                        0,
                        request.volume,
                        "FINISHED",
                        "rejected",
                        true,
                        reason,
                        CommandStatus::Rejected,
                    )
                }
                FakeBrokerPolicy::PartialFill { volume } => {
                    let filled = volume.clamp(0, request.volume);
                    pending.fill_steps.clear();
                    pending.filled_volume = request.volume;
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

        let command_status = if pending.last_status == Some(next_status) {
            None
        } else {
            pending.last_status = Some(next_status);
            Some(next_status)
        };
        let position = self.apply_position(request, filled_volume)?;
        let insert_date_time = self.clock.next_timestamp_ns();
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
            "insert_date_time": insert_date_time,
            "last_msg": last_msg,
            "status": status,
            "lifecycle": lifecycle,
            "is_dead": is_dead,
            "trade_price": if filled_volume > 0 { request.limit_price } else { 0.0 },
        });
        let order = serde_json::from_value(order_value.clone())
            .map_err(|_| TaskError::InvalidState("fake order payload is invalid"))?;

        let trade_value = (filled_volume > 0).then(|| {
            pending.trade_sequence += 1;
            let trade_sequence = pending.trade_sequence;
            let trade_date_time = self.clock.next_timestamp_ns();
            json!({
                "seqno": 1,
                "user_id": request.account_id,
                "order_id": request.order_id,
                "trade_id": format!("fake-trade-{}-{}", request.order_id, trade_sequence),
                "exchange_trade_id": format!("fake-exchange-trade-{}-{}", request.order_id, trade_sequence),
                "exchange_id": request.exchange_id,
                "instrument_id": request.instrument_id,
                "direction": request.direction,
                "offset": request.offset,
                "price": request.limit_price,
                "volume": filled_volume,
                "trade_date_time": trade_date_time,
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
            account_id: request.account_id.clone(),
            symbol: request.symbol.clone(),
            order_id: request.order_id.clone(),
            order_value,
            trade_value,
            position_value,
            order,
            trade,
            command_status,
        })
    }

    fn apply_position(
        &mut self,
        request: &FakeOrderRequest,
        filled_volume: i64,
    ) -> Result<Position> {
        let key = (request.account_id.clone(), request.symbol.clone());
        let mut position = self
            .positions
            .get(&key)
            .cloned()
            .unwrap_or_else(|| position_from_net(&request.account_id, &request.symbol, 0));
        let signed_delta =
            signed_position_delta(&request.direction, &request.offset, filled_volume);
        position.pos += signed_delta;
        position.pos_long = position.pos.max(0);
        position.pos_short = (-position.pos).max(0);
        position.volume_long = position.pos_long;
        position.volume_short = position.pos_short;
        self.positions.insert(key, position.clone());
        Ok(position)
    }
}

fn ingest_fake_order_outcome(
    host: &TaskHost,
    report: &mut StrategyTestReport,
    runtime: &mut StrategyTestRuntime,
    command_id: CommandId,
    outcome: FakeOrderOutcome,
) -> Result<()> {
    ingest_fake_trade_update(host, &outcome, command_id)?;
    if let Some(status) = outcome.command_status {
        let _ = host.api().session().handle().record_command_status(
            command_id,
            status,
            None,
            CommitScope::RealtimeUpdate,
        )?;
    }

    report.orders.push(outcome.order);
    if let Some(trade) = outcome.trade {
        report.trades.push(trade);
    }
    report.positions = runtime.positions.clone();
    Ok(())
}

fn fake_order_request(request: &OutboundRequest) -> Result<Option<FakeOrderRequest>> {
    let payload: Value = match request {
        OutboundRequest::Transport(OutboundFrame::Text(text)) => serde_json::from_str(text)
            .map_err(|_| TaskError::InvalidState("fake broker received invalid text payload"))?,
        OutboundRequest::Transport(OutboundFrame::Binary(bytes)) => serde_json::from_slice(bytes)
            .map_err(|_| {
            TaskError::InvalidState("fake broker received invalid binary payload")
        })?,
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
        account_node.insert(
            "trades".to_string(),
            json!({ trade_id: trade_value.clone() }),
        );
    }

    host.api().session().handle().ingest(
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
        .ok_or(TaskError::InvalidState(
            "fake broker payload missing string field",
        ))
}

fn required_i64(payload: &Value, key: &'static str) -> Result<i64> {
    payload
        .get(key)
        .and_then(Value::as_i64)
        .ok_or(TaskError::InvalidState(
            "fake broker payload missing integer field",
        ))
}

fn required_f64(payload: &Value, key: &'static str) -> Result<f64> {
    payload
        .get(key)
        .and_then(Value::as_f64)
        .ok_or(TaskError::InvalidState(
            "fake broker payload missing float field",
        ))
}
