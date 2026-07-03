#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashMap;

use serde_json::{Value, json};
use tqsdk_core::{
    Account, CommandId, CommandStatus, CommitScope, InputPayload, IoEvent, Order, OrderLifecycle,
    OutboundFrame, OutboundRequest, Position, ProtocolDomain, Quote, RuntimeInput, Trade,
    TradeDirection, TradeOffset, TradePriceType,
};

use crate::{Result, TaskError, TaskHost};

/// Default account id used by local Python-compatible backtest simulation.
pub const LOCAL_BACKTEST_ACCOUNT_ID: &str = "TQSIM";
const DEFAULT_INIT_BALANCE: f64 = 10_000_000.0;

/// Local Python-compatible futures simulated account.
#[derive(Debug, Clone)]
pub struct TqSim {
    account_id: String,
    init_balance: f64,
    balance: f64,
    commission: f64,
    close_profit: f64,
    margin_by_symbol: HashMap<String, f64>,
    commission_by_symbol: HashMap<String, f64>,
    contract_multiplier_by_symbol: HashMap<String, f64>,
    execution_symbol_by_strategy_symbol: HashMap<String, String>,
    quotes: HashMap<String, Quote>,
    orders: HashMap<String, SimOrder>,
    positions: HashMap<String, i64>,
    nonzero_position_count: usize,
    avg_price_by_symbol: HashMap<String, f64>,
    trades: HashMap<String, Trade>,
    next_seq: i64,
    next_trade_seq: i64,
    current_time_ns: i64,
}

/// Order request handled by [`TqSim`].
#[derive(Debug, Clone, PartialEq)]
pub struct TqSimOrderRequest {
    order_id: String,
    symbol: String,
    direction: TradeDirection,
    offset: TradeOffset,
    volume: i64,
    price_type: TradePriceType,
    limit_price: Option<f64>,
}

/// Per-step local sim changes.
#[derive(Debug, Clone, Default)]
pub struct TqSimStepReport {
    account: Option<Account>,
    orders: Vec<Order>,
    trades: Vec<Trade>,
    positions: Vec<Position>,
}

#[derive(Debug, Clone)]
struct SimOrder {
    request: TqSimOrderRequest,
    command_id: Option<CommandId>,
    alive: bool,
    inserted_at_ns: i64,
    snapshot: Option<Order>,
}

#[derive(Debug, Clone)]
struct SimOrderOutcome {
    command_id: Option<CommandId>,
    order: Order,
    trade: Option<Trade>,
    position: Option<Position>,
    command_status: Option<CommandStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MatchDecision {
    Fill,
    KeepAlive,
    CancelNoCounterparty,
}

impl Default for TqSim {
    fn default() -> Self {
        Self::with_account(LOCAL_BACKTEST_ACCOUNT_ID, DEFAULT_INIT_BALANCE)
    }
}

impl TqSim {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_account(account_id: impl Into<String>, init_balance: f64) -> Self {
        let init_balance = if init_balance.is_finite() {
            init_balance
        } else {
            DEFAULT_INIT_BALANCE
        };
        Self {
            account_id: account_id.into(),
            init_balance,
            balance: init_balance,
            commission: 0.0,
            close_profit: 0.0,
            margin_by_symbol: HashMap::new(),
            commission_by_symbol: HashMap::new(),
            contract_multiplier_by_symbol: HashMap::new(),
            execution_symbol_by_strategy_symbol: HashMap::new(),
            quotes: HashMap::new(),
            orders: HashMap::new(),
            positions: HashMap::new(),
            nonzero_position_count: 0,
            avg_price_by_symbol: HashMap::new(),
            trades: HashMap::new(),
            next_seq: 1,
            next_trade_seq: 1,
            current_time_ns: 0,
        }
    }

    #[must_use]
    pub fn with_margin(mut self, symbol: impl Into<String>, margin_per_lot: f64) -> Self {
        self.set_margin(symbol, margin_per_lot);
        self
    }

    #[must_use]
    pub fn with_commission(mut self, symbol: impl Into<String>, commission_per_lot: f64) -> Self {
        self.set_commission(symbol, commission_per_lot);
        self
    }

    #[must_use]
    pub fn with_contract_multiplier(mut self, symbol: impl Into<String>, multiplier: f64) -> Self {
        self.set_contract_multiplier(symbol, multiplier);
        self
    }

    pub fn set_margin(&mut self, symbol: impl Into<String>, margin_per_lot: f64) {
        if margin_per_lot.is_finite() && margin_per_lot >= 0.0 {
            self.margin_by_symbol.insert(symbol.into(), margin_per_lot);
        }
    }

    pub fn set_commission(&mut self, symbol: impl Into<String>, commission_per_lot: f64) {
        if commission_per_lot.is_finite() && commission_per_lot >= 0.0 {
            self.commission_by_symbol
                .insert(symbol.into(), commission_per_lot);
        }
    }

    #[must_use]
    pub fn margin(&self, symbol: impl AsRef<str>) -> f64 {
        self.margin_for(symbol.as_ref())
    }

    #[must_use]
    pub fn commission(&self, symbol: impl AsRef<str>) -> f64 {
        self.commission_for(symbol.as_ref())
    }

    pub fn set_contract_multiplier(&mut self, symbol: impl Into<String>, multiplier: f64) {
        if multiplier.is_finite() && multiplier > 0.0 {
            self.contract_multiplier_by_symbol
                .insert(symbol.into(), multiplier);
        }
    }

    /// Fill missing per-symbol simulation parameters from quote metadata.
    ///
    /// Explicit `set_*` / `with_*` configuration keeps precedence.
    pub fn apply_quote_metadata(&mut self, symbol: impl AsRef<str>, quote: &Quote) {
        let symbol = symbol.as_ref();
        if quote.margin.is_finite() && quote.margin >= 0.0 {
            self.margin_by_symbol
                .entry(symbol.to_owned())
                .or_insert(quote.margin);
        }
        if quote.commission.is_finite() && quote.commission >= 0.0 {
            self.commission_by_symbol
                .entry(symbol.to_owned())
                .or_insert(quote.commission);
        }
        if quote.volume_multiple > 0 {
            self.contract_multiplier_by_symbol
                .entry(symbol.to_owned())
                .or_insert(quote.volume_multiple as f64);
        }
    }

    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub fn account(&self) -> Account {
        self.account_snapshot()
    }

    #[must_use]
    pub fn position(&self, symbol: impl AsRef<str>) -> Position {
        self.position_snapshot(symbol.as_ref())
    }

    #[must_use]
    pub fn orders(&self) -> Vec<Order> {
        let mut orders = self
            .orders
            .values()
            .map(|order| {
                order.snapshot.clone().unwrap_or_else(|| {
                    self.order_snapshot(
                        &order.request,
                        order_volume_left(order),
                        order.inserted_at_ns,
                    )
                })
            })
            .collect::<Vec<_>>();
        orders.sort_by(|left, right| left.order_id.cmp(&right.order_id));
        orders
    }

    #[must_use]
    pub fn trades(&self) -> Vec<Trade> {
        let mut trades = self.trades.values().cloned().collect::<Vec<_>>();
        trades.sort_by(|left, right| left.trade_id.cmp(&right.trade_id));
        trades
    }

    pub fn update_quote(&mut self, symbol: impl Into<String>, quote: Quote) -> TqSimStepReport {
        let symbol = symbol.into();
        self.update_quote_ref_inner(&symbol, &quote)
    }

    pub fn update_quote_at(
        &mut self,
        symbol: impl Into<String>,
        quote: Quote,
        event_time_ns: i64,
    ) -> TqSimStepReport {
        let symbol = symbol.into();
        self.current_time_ns = event_time_ns;
        self.update_quote_ref_inner(&symbol, &quote)
    }

    pub(crate) fn update_quote_ref_at(
        &mut self,
        symbol: &str,
        quote: &Quote,
        event_time_ns: i64,
    ) -> TqSimStepReport {
        self.current_time_ns = event_time_ns;
        self.update_quote_ref_inner(symbol, quote)
    }

    fn update_quote_ref_inner(&mut self, symbol: &str, quote: &Quote) -> TqSimStepReport {
        let execution_symbol = self.update_execution_symbol_alias_from_quote(symbol, quote);
        self.apply_quote_metadata(symbol, quote);
        self.quotes.insert(symbol.to_owned(), quote.clone());
        if let Some(execution_symbol) = execution_symbol.as_deref() {
            self.apply_quote_metadata(execution_symbol, quote);
            self.quotes
                .insert(execution_symbol.to_owned(), quote.clone());
        }

        if self.orders.is_empty() && self.nonzero_position_count == 0 {
            return TqSimStepReport::default();
        }

        if self.orders.is_empty()
            && !self.has_nonzero_position(symbol)
            && execution_symbol
                .as_deref()
                .is_none_or(|execution_symbol| !self.has_nonzero_position(execution_symbol))
        {
            return TqSimStepReport::default();
        }

        let mut report = self.match_pending_for_symbol(symbol);
        if let Some(execution_symbol) = execution_symbol.as_deref() {
            report.extend(self.match_pending_for_symbol(execution_symbol));
        }

        if report.is_empty() {
            if let Some(execution_symbol) = execution_symbol.as_deref() {
                self.nonzero_symbols_snapshot_report([symbol, execution_symbol])
            } else {
                self.nonzero_symbols_snapshot_report([symbol])
            }
        } else {
            report
        }
    }

    pub fn insert_order(&mut self, request: TqSimOrderRequest) -> Result<TqSimStepReport> {
        self.insert_order_with_command(request, None)
    }

    pub(crate) fn seed_runtime(&self, host: &TaskHost) -> Result<()> {
        ingest_trade_report(
            host,
            self.account_id(),
            &self.snapshot_report(),
            &[],
            CommitScope::ReplayStep,
        )
    }

    pub(crate) fn ensure_position(&mut self, symbol: impl Into<String>) {
        self.positions.entry(symbol.into()).or_default();
    }

    pub(crate) fn process_host_orders(&mut self, host: &TaskHost) -> Result<TqSimStepReport> {
        let dispatches = host.api().session().handle().drain_dispatches()?;
        let mut report = TqSimStepReport::default();
        let mut command_ids = Vec::new();
        let mut terminal_statuses = Vec::new();

        for dispatch in dispatches {
            let Some(request) = TqSimOrderRequest::from_outbound(&dispatch.request)? else {
                continue;
            };
            let request = self.resolve_order_request(request);
            let _ = host.api().session().handle().record_command_status(
                dispatch.command_id,
                CommandStatus::Sent,
                None,
                CommitScope::ReplayStep,
            )?;
            let outcome = self.apply_order(SimOrder {
                request,
                command_id: Some(dispatch.command_id),
                alive: true,
                inserted_at_ns: self.current_time_ns,
                snapshot: None,
            })?;
            if let Some(status) = outcome.command_status {
                terminal_statuses.push((dispatch.command_id, status));
            }
            let step = self.report_from_outcomes(vec![outcome]);
            report.extend(step);
            command_ids.push(dispatch.command_id);
        }

        ingest_trade_report(
            host,
            self.account_id(),
            &report,
            &command_ids,
            CommitScope::ReplayStep,
        )?;
        for (command_id, status) in terminal_statuses {
            let _ = host.api().session().handle().record_command_status(
                command_id,
                status,
                None,
                CommitScope::ReplayStep,
            )?;
        }
        Ok(report)
    }

    pub(crate) fn ingest_step_report(
        &self,
        host: &TaskHost,
        report: &TqSimStepReport,
    ) -> Result<()> {
        ingest_trade_report(
            host,
            self.account_id(),
            report,
            &[],
            CommitScope::ReplayStep,
        )
    }

    fn insert_order_with_command(
        &mut self,
        request: TqSimOrderRequest,
        command_id: Option<CommandId>,
    ) -> Result<TqSimStepReport> {
        let request = self.resolve_order_request(request);
        validate_request(&request)?;
        let outcome = self.apply_order(SimOrder {
            request,
            command_id,
            alive: true,
            inserted_at_ns: self.current_time_ns,
            snapshot: None,
        })?;
        Ok(self.report_from_outcomes(vec![outcome]))
    }

    fn match_pending_for_symbol(&mut self, symbol: &str) -> TqSimStepReport {
        let pending = self
            .orders
            .values()
            .filter(|order| order.alive && order.request.symbol == symbol)
            .cloned()
            .collect::<Vec<_>>();
        let mut outcomes = Vec::new();
        for order in pending {
            if matches!(self.match_decision(&order.request), MatchDecision::Fill) {
                if let Ok(outcome) = self.apply_order(order) {
                    outcomes.push(outcome);
                }
            }
        }
        self.report_from_outcomes(outcomes)
    }

    fn apply_order(&mut self, order: SimOrder) -> Result<SimOrderOutcome> {
        let request = order.request;
        let command_id = order.command_id;
        let inserted_at_ns = order.inserted_at_ns;
        let decision = self.match_decision(&request);

        if let Some(reason) = self.pretrade_rejection(&request) {
            let order = self.dead_order(
                &request,
                "rejected",
                reason,
                request.volume,
                0.0,
                inserted_at_ns,
            )?;
            self.orders.insert(
                request.order_id.clone(),
                SimOrder {
                    request,
                    command_id,
                    alive: false,
                    inserted_at_ns,
                    snapshot: Some(order.clone()),
                },
            );
            return Ok(SimOrderOutcome {
                command_id,
                order,
                trade: None,
                position: None,
                command_status: Some(CommandStatus::Rejected),
            });
        }

        match decision {
            MatchDecision::Fill => {
                let trade_price = self.trade_price(&request)?;
                let commission = self.commission_for(&request.symbol) * request.volume as f64;
                self.balance -= commission;
                self.commission += commission;
                self.apply_position_delta(&request, trade_price);
                let position = self.position_snapshot(&request.symbol);
                let order_value =
                    self.dead_order(&request, "filled", "", 0, trade_price, inserted_at_ns)?;
                let trade = self.trade(&request, trade_price, commission)?;
                self.orders.insert(
                    request.order_id.clone(),
                    SimOrder {
                        request,
                        command_id,
                        alive: false,
                        inserted_at_ns,
                        snapshot: Some(order_value.clone()),
                    },
                );
                self.trades.insert(trade.trade_id.clone(), trade.clone());
                Ok(SimOrderOutcome {
                    command_id,
                    order: order_value,
                    trade: Some(trade),
                    position: Some(position),
                    command_status: Some(CommandStatus::Completed),
                })
            }
            MatchDecision::KeepAlive => {
                let order_value = self.alive_order(&request, inserted_at_ns)?;
                self.orders.insert(
                    request.order_id.clone(),
                    SimOrder {
                        request,
                        command_id,
                        alive: true,
                        inserted_at_ns,
                        snapshot: Some(order_value.clone()),
                    },
                );
                Ok(SimOrderOutcome {
                    command_id,
                    order: order_value,
                    trade: None,
                    position: None,
                    command_status: None,
                })
            }
            MatchDecision::CancelNoCounterparty => {
                let order_value = self.dead_order(
                    &request,
                    "cancelled",
                    "市价单没有对手盘",
                    request.volume,
                    0.0,
                    inserted_at_ns,
                )?;
                self.orders.insert(
                    request.order_id.clone(),
                    SimOrder {
                        request,
                        command_id,
                        alive: false,
                        inserted_at_ns,
                        snapshot: Some(order_value.clone()),
                    },
                );
                Ok(SimOrderOutcome {
                    command_id,
                    order: order_value,
                    trade: None,
                    position: None,
                    command_status: Some(CommandStatus::Cancelled),
                })
            }
        }
    }

    fn pretrade_rejection(&self, request: &TqSimOrderRequest) -> Option<&'static str> {
        if request.offset == TradeOffset::Open {
            let required = self.margin_for(&request.symbol) * request.volume as f64
                + self.commission_for(&request.symbol) * request.volume as f64;
            if self.account_snapshot().available < required {
                return Some("可用资金不足");
            }
            return None;
        }

        let current = self
            .positions
            .get(&request.symbol)
            .copied()
            .unwrap_or_default();
        match request.direction {
            TradeDirection::Sell if current < request.volume => Some("多头持仓不足"),
            TradeDirection::Buy if -current < request.volume => Some("空头持仓不足"),
            _ => None,
        }
    }

    fn match_decision(&self, request: &TqSimOrderRequest) -> MatchDecision {
        let Some(quote) = self.quotes.get(&request.symbol) else {
            return match request.price_type {
                TradePriceType::Limit => MatchDecision::KeepAlive,
                TradePriceType::Any | TradePriceType::Best | TradePriceType::FiveLevel => {
                    MatchDecision::CancelNoCounterparty
                }
            };
        };

        match request.price_type {
            TradePriceType::Limit => {
                let Some(limit_price) = request.limit_price else {
                    return MatchDecision::KeepAlive;
                };
                match request.direction {
                    TradeDirection::Buy
                        if quote.ask_price1.is_finite()
                            && quote.ask_price1 > 0.0
                            && quote.ask_volume1 >= request.volume
                            && limit_price >= quote.ask_price1 =>
                    {
                        MatchDecision::Fill
                    }
                    TradeDirection::Sell
                        if quote.bid_price1.is_finite()
                            && quote.bid_price1 > 0.0
                            && quote.bid_volume1 >= request.volume
                            && limit_price <= quote.bid_price1 =>
                    {
                        MatchDecision::Fill
                    }
                    _ => MatchDecision::KeepAlive,
                }
            }
            TradePriceType::Any | TradePriceType::Best | TradePriceType::FiveLevel => {
                if self.counterparty_price(request).is_some() {
                    MatchDecision::Fill
                } else {
                    MatchDecision::CancelNoCounterparty
                }
            }
        }
    }

    fn trade_price(&self, request: &TqSimOrderRequest) -> Result<f64> {
        match request.price_type {
            TradePriceType::Limit => request
                .limit_price
                .ok_or(TaskError::InvalidState("limit order missing limit price")),
            TradePriceType::Any | TradePriceType::Best | TradePriceType::FiveLevel => self
                .counterparty_price(request)
                .ok_or(TaskError::InvalidState(
                    "market order missing counterparty price",
                )),
        }
    }

    fn counterparty_price(&self, request: &TqSimOrderRequest) -> Option<f64> {
        let quote = self.quotes.get(&request.symbol)?;
        match request.direction {
            TradeDirection::Buy
                if quote.ask_price1.is_finite()
                    && quote.ask_price1 > 0.0
                    && quote.ask_volume1 >= request.volume =>
            {
                Some(quote.ask_price1)
            }
            TradeDirection::Sell
                if quote.bid_price1.is_finite()
                    && quote.bid_price1 > 0.0
                    && quote.bid_volume1 >= request.volume =>
            {
                Some(quote.bid_price1)
            }
            _ => None,
        }
    }

    fn apply_position_delta(&mut self, request: &TqSimOrderRequest, trade_price: f64) {
        let delta = signed_position_delta(request.direction, request.offset, request.volume);
        let previous = self
            .positions
            .get(&request.symbol)
            .copied()
            .unwrap_or_default();
        let previous_avg = self
            .avg_price_by_symbol
            .get(&request.symbol)
            .copied()
            .unwrap_or(trade_price);
        let close_profit = close_profit_for_delta(
            previous,
            delta,
            previous_avg,
            trade_price,
            self.contract_multiplier_for(&request.symbol),
        );
        self.close_profit += close_profit;
        self.balance += close_profit;
        let position = self.positions.entry(request.symbol.clone()).or_default();
        *position += delta;
        update_nonzero_position_count(&mut self.nonzero_position_count, previous, *position);
        update_average_price(
            &mut self.avg_price_by_symbol,
            &request.symbol,
            previous,
            delta,
            previous_avg,
            trade_price,
        );
    }

    fn alive_order(&mut self, request: &TqSimOrderRequest, inserted_at_ns: i64) -> Result<Order> {
        let seqno = self.next_seq();
        let account_id = self.account_id.clone();
        let exchange_order_id = format!("tqsim-exchange-{}", request.order_id);
        let (exchange_id, instrument_id) = split_symbol(&request.symbol);
        self.order_from_json(json!({
            "seqno": seqno,
            "user_id": account_id,
            "order_id": request.order_id,
            "exchange_order_id": exchange_order_id,
            "exchange_id": exchange_id,
            "instrument_id": instrument_id,
            "direction": request.direction,
            "offset": request.offset,
            "volume_orign": request.volume,
            "volume_left": request.volume,
            "limit_price": request.limit_price.unwrap_or(0.0),
            "price_type": request.price_type,
            "volume_condition": "ANY",
            "time_condition": if request.price_type == TradePriceType::Limit { "GFD" } else { "IOC" },
            "insert_date_time": inserted_at_ns,
            "last_msg": "",
            "status": "ALIVE",
            "lifecycle": OrderLifecycle::Accepted,
            "is_dead": false,
            "trade_price": 0.0,
        }))
    }

    fn dead_order(
        &mut self,
        request: &TqSimOrderRequest,
        lifecycle: &'static str,
        last_msg: &'static str,
        volume_left: i64,
        trade_price: f64,
        inserted_at_ns: i64,
    ) -> Result<Order> {
        let seqno = self.next_seq();
        let account_id = self.account_id.clone();
        let exchange_order_id = if lifecycle == "rejected" {
            String::new()
        } else {
            format!("tqsim-exchange-{}", request.order_id)
        };
        let (exchange_id, instrument_id) = split_symbol(&request.symbol);
        self.order_from_json(json!({
            "seqno": seqno,
            "user_id": account_id,
            "order_id": request.order_id,
            "exchange_order_id": exchange_order_id,
            "exchange_id": exchange_id,
            "instrument_id": instrument_id,
            "direction": request.direction,
            "offset": request.offset,
            "volume_orign": request.volume,
            "volume_left": volume_left,
            "limit_price": request.limit_price.unwrap_or(0.0),
            "price_type": request.price_type,
            "volume_condition": "ANY",
            "time_condition": if request.price_type == TradePriceType::Limit { "GFD" } else { "IOC" },
            "insert_date_time": inserted_at_ns,
            "last_msg": last_msg,
            "status": "FINISHED",
            "lifecycle": lifecycle,
            "is_dead": true,
            "trade_price": trade_price,
        }))
    }

    fn trade(&mut self, request: &TqSimOrderRequest, price: f64, commission: f64) -> Result<Trade> {
        let seqno = self.next_seq();
        let trade_id = format!("tqsim-trade-{}", self.next_trade_seq());
        let account_id = self.account_id.clone();
        let exchange_trade_id = format!("tqsim-exchange-trade-{}", request.order_id);
        let (exchange_id, instrument_id) = split_symbol(&request.symbol);
        serde_json::from_value(json!({
            "seqno": seqno,
            "user_id": account_id,
            "order_id": request.order_id,
            "trade_id": trade_id,
            "exchange_trade_id": exchange_trade_id,
            "exchange_id": exchange_id,
            "instrument_id": instrument_id,
            "direction": request.direction,
            "offset": request.offset,
            "price": price,
            "volume": request.volume,
            "trade_date_time": self.current_time_ns,
            "commission": commission,
        }))
        .map_err(|_| TaskError::InvalidState("sim trade payload is invalid"))
    }

    fn order_snapshot(
        &self,
        request: &TqSimOrderRequest,
        volume_left: i64,
        inserted_at_ns: i64,
    ) -> Order {
        let lifecycle = if volume_left == 0 {
            OrderLifecycle::Filled
        } else {
            OrderLifecycle::Accepted
        };
        serde_json::from_value(json!({
            "seqno": 0,
            "user_id": self.account_id,
            "order_id": request.order_id,
            "exchange_order_id": format!("tqsim-exchange-{}", request.order_id),
            "exchange_id": split_symbol(&request.symbol).0,
            "instrument_id": split_symbol(&request.symbol).1,
            "direction": request.direction,
            "offset": request.offset,
            "volume_orign": request.volume,
            "volume_left": volume_left,
            "limit_price": request.limit_price.unwrap_or(0.0),
            "price_type": request.price_type,
            "volume_condition": "ANY",
            "time_condition": if request.price_type == TradePriceType::Limit { "GFD" } else { "IOC" },
            "insert_date_time": inserted_at_ns,
            "last_msg": "",
            "status": if volume_left == 0 { "FINISHED" } else { "ALIVE" },
            "lifecycle": lifecycle,
            "is_dead": volume_left == 0,
            "trade_price": if volume_left == 0 { request.limit_price.unwrap_or(0.0) } else { 0.0 },
        }))
        .expect("sim order snapshot should decode")
    }

    fn order_from_json(&self, value: Value) -> Result<Order> {
        serde_json::from_value(value)
            .map_err(|_| TaskError::InvalidState("sim order payload is invalid"))
    }

    fn account_snapshot(&self) -> Account {
        let margin = self.margin_total();
        let float_profit = self.float_profit_total();
        let market_value = self.market_value_total();
        Account {
            user_id: self.account_id.clone(),
            currency: "CNY".to_string(),
            pre_balance: self.init_balance,
            static_balance: self.init_balance,
            balance: self.balance,
            available: self.balance - margin,
            ctp_balance: self.balance,
            ctp_available: self.balance - margin,
            float_profit,
            position_profit: float_profit,
            close_profit: self.close_profit,
            margin,
            commission: self.commission,
            risk_ratio: if self.balance > 0.0 {
                margin / self.balance
            } else {
                0.0
            },
            market_value,
            ..Account::default()
        }
    }

    fn position_snapshot(&self, symbol: &str) -> Position {
        let state_symbol = self.execution_symbol_for(symbol);
        self.position_snapshot_for(symbol, state_symbol)
    }

    fn has_nonzero_position(&self, symbol: &str) -> bool {
        self.positions
            .get(symbol)
            .is_some_and(|position| *position != 0)
    }

    fn position_snapshot_for(&self, report_symbol: &str, state_symbol: &str) -> Position {
        let net = self
            .positions
            .get(state_symbol)
            .copied()
            .unwrap_or_default();
        let (exchange_id, instrument_id) = split_symbol(report_symbol);
        let margin = self.margin_for(state_symbol) * net.unsigned_abs() as f64;
        let avg_price = self
            .avg_price_by_symbol
            .get(state_symbol)
            .copied()
            .unwrap_or(0.0);
        let last_price = self
            .quotes
            .get(state_symbol)
            .or_else(|| self.quotes.get(report_symbol))
            .map(|quote| quote.last_price)
            .unwrap_or_default();
        let multiplier = self.contract_multiplier_for(state_symbol);
        let float_profit = profit_for_net(net, avg_price, last_price, multiplier);
        let market_value = market_value(net, last_price, multiplier);
        let open_cost = avg_price * net.unsigned_abs() as f64 * multiplier;
        Position {
            user_id: self.account_id.clone(),
            exchange_id: exchange_id.to_string(),
            instrument_id: instrument_id.to_string(),
            volume_long: net.max(0),
            volume_short: (-net).max(0),
            pos_long: net.max(0),
            pos_short: (-net).max(0),
            pos: net,
            margin_long: if net > 0 { margin } else { 0.0 },
            margin_short: if net < 0 { margin } else { 0.0 },
            margin,
            last_price,
            open_price_long: if net > 0 { avg_price } else { f64::NAN },
            open_price_short: if net < 0 { avg_price } else { f64::NAN },
            open_cost_long: if net > 0 { open_cost } else { 0.0 },
            open_cost_short: if net < 0 { open_cost } else { 0.0 },
            position_price_long: if net > 0 { avg_price } else { f64::NAN },
            position_price_short: if net < 0 { avg_price } else { f64::NAN },
            position_cost_long: if net > 0 { open_cost } else { 0.0 },
            position_cost_short: if net < 0 { open_cost } else { 0.0 },
            float_profit_long: if net > 0 { float_profit } else { 0.0 },
            float_profit_short: if net < 0 { float_profit } else { 0.0 },
            float_profit,
            position_profit_long: if net > 0 { float_profit } else { 0.0 },
            position_profit_short: if net < 0 { float_profit } else { 0.0 },
            position_profit: float_profit,
            market_value_long: if net > 0 { market_value } else { 0.0 },
            market_value_short: if net < 0 { market_value } else { 0.0 },
            market_value,
            ..Position::default()
        }
    }

    fn snapshot_report(&self) -> TqSimStepReport {
        TqSimStepReport {
            account: Some(self.account_snapshot()),
            orders: Vec::new(),
            trades: Vec::new(),
            positions: self.snapshot_positions(),
        }
    }

    fn nonzero_symbols_snapshot_report<'a>(
        &self,
        symbols: impl IntoIterator<Item = &'a str>,
    ) -> TqSimStepReport {
        let mut unique_symbols = Vec::new();
        for symbol in symbols {
            if !unique_symbols.contains(&symbol) {
                unique_symbols.push(symbol);
            }
        }
        let positions = unique_symbols
            .into_iter()
            .map(|symbol| self.position_snapshot(symbol))
            .filter(|position| position.pos != 0)
            .collect::<Vec<_>>();
        if positions.is_empty() {
            return TqSimStepReport::default();
        }
        TqSimStepReport {
            account: Some(self.account_snapshot()),
            orders: Vec::new(),
            trades: Vec::new(),
            positions,
        }
    }

    fn report_from_outcomes(&self, outcomes: Vec<SimOrderOutcome>) -> TqSimStepReport {
        let mut report = TqSimStepReport::default();
        for outcome in outcomes {
            let _ = outcome.command_id;
            let _ = outcome.command_status;
            report.orders.push(outcome.order);
            if let Some(trade) = outcome.trade {
                report.trades.push(trade);
            }
            if let Some(position) = outcome.position {
                self.push_position_with_aliases(&mut report.positions, position);
            }
        }
        if !report.orders.is_empty() || !report.trades.is_empty() || !report.positions.is_empty() {
            report.account = Some(self.account_snapshot());
        }
        report
    }

    fn update_execution_symbol_alias_from_quote(
        &mut self,
        symbol: &str,
        quote: &Quote,
    ) -> Option<String> {
        let underlying_symbol = quote.underlying_symbol.trim();
        if underlying_symbol.is_empty() {
            return self
                .execution_symbol_by_strategy_symbol
                .get(symbol)
                .cloned();
        }
        if underlying_symbol == symbol {
            self.execution_symbol_by_strategy_symbol.remove(symbol);
            return None;
        }
        let underlying_symbol = underlying_symbol.to_owned();
        self.execution_symbol_by_strategy_symbol
            .insert(symbol.to_owned(), underlying_symbol.clone());
        Some(underlying_symbol)
    }

    fn execution_symbol_for<'a>(&'a self, symbol: &'a str) -> &'a str {
        self.execution_symbol_by_strategy_symbol
            .get(symbol)
            .map(String::as_str)
            .unwrap_or(symbol)
    }

    fn resolve_order_request(&self, mut request: TqSimOrderRequest) -> TqSimOrderRequest {
        if let Some(execution_symbol) = self
            .execution_symbol_by_strategy_symbol
            .get(&request.symbol)
        {
            request.symbol.clone_from(execution_symbol);
        }
        request
    }

    fn snapshot_positions(&self) -> Vec<Position> {
        let mut positions = Vec::new();
        let mut symbols = self.positions.keys().cloned().collect::<Vec<_>>();
        symbols.sort();
        for symbol in symbols {
            self.push_position_with_aliases(
                &mut positions,
                self.position_snapshot_for(&symbol, &symbol),
            );
        }
        positions
    }

    fn push_position_with_aliases(&self, positions: &mut Vec<Position>, position: Position) {
        let execution_symbol = position_symbol(&position);
        positions.push(position);
        let mut strategy_symbols = self
            .execution_symbol_by_strategy_symbol
            .iter()
            .filter_map(|(strategy_symbol, mapped_execution_symbol)| {
                (mapped_execution_symbol == &execution_symbol).then_some(strategy_symbol)
            })
            .collect::<Vec<_>>();
        strategy_symbols.sort();
        for strategy_symbol in strategy_symbols {
            positions.push(self.position_snapshot_for(strategy_symbol, &execution_symbol));
        }
    }

    fn margin_for(&self, symbol: &str) -> f64 {
        self.margin_by_symbol
            .get(symbol)
            .copied()
            .unwrap_or_default()
    }

    fn commission_for(&self, symbol: &str) -> f64 {
        self.commission_by_symbol
            .get(symbol)
            .copied()
            .unwrap_or_default()
    }

    fn contract_multiplier_for(&self, symbol: &str) -> f64 {
        self.contract_multiplier_by_symbol
            .get(symbol)
            .copied()
            .unwrap_or(1.0)
    }

    fn margin_total(&self) -> f64 {
        self.positions
            .iter()
            .map(|(symbol, net)| self.margin_for(symbol) * net.unsigned_abs() as f64)
            .sum()
    }

    fn float_profit_total(&self) -> f64 {
        self.positions
            .keys()
            .map(|symbol| self.float_profit_for(symbol))
            .sum()
    }

    fn float_profit_for(&self, symbol: &str) -> f64 {
        let net = self.positions.get(symbol).copied().unwrap_or_default();
        let Some(avg_price) = self.avg_price_by_symbol.get(symbol).copied() else {
            return 0.0;
        };
        let Some(last_price) = self.quotes.get(symbol).map(|quote| quote.last_price) else {
            return 0.0;
        };
        profit_for_net(
            net,
            avg_price,
            last_price,
            self.contract_multiplier_for(symbol),
        )
    }

    fn market_value_total(&self) -> f64 {
        self.positions
            .iter()
            .map(|(symbol, net)| {
                let last_price = self
                    .quotes
                    .get(symbol)
                    .map(|quote| quote.last_price)
                    .unwrap_or_default();
                market_value(*net, last_price, self.contract_multiplier_for(symbol))
            })
            .sum()
    }

    fn next_seq(&mut self) -> i64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    fn next_trade_seq(&mut self) -> i64 {
        let seq = self.next_trade_seq;
        self.next_trade_seq += 1;
        seq
    }
}

impl TqSimOrderRequest {
    #[must_use]
    pub fn limit(
        order_id: impl Into<String>,
        symbol: impl Into<String>,
        direction: TradeDirection,
        offset: TradeOffset,
        volume: i64,
        limit_price: f64,
    ) -> Self {
        Self {
            order_id: order_id.into(),
            symbol: symbol.into(),
            direction,
            offset,
            volume,
            price_type: TradePriceType::Limit,
            limit_price: Some(limit_price),
        }
    }

    #[must_use]
    pub fn any(
        order_id: impl Into<String>,
        symbol: impl Into<String>,
        direction: TradeDirection,
        offset: TradeOffset,
        volume: i64,
    ) -> Self {
        Self::market(
            order_id,
            symbol,
            direction,
            offset,
            volume,
            TradePriceType::Any,
        )
    }

    #[must_use]
    pub fn best(
        order_id: impl Into<String>,
        symbol: impl Into<String>,
        direction: TradeDirection,
        offset: TradeOffset,
        volume: i64,
    ) -> Self {
        Self::market(
            order_id,
            symbol,
            direction,
            offset,
            volume,
            TradePriceType::Best,
        )
    }

    #[must_use]
    pub fn five_level(
        order_id: impl Into<String>,
        symbol: impl Into<String>,
        direction: TradeDirection,
        offset: TradeOffset,
        volume: i64,
    ) -> Self {
        Self::market(
            order_id,
            symbol,
            direction,
            offset,
            volume,
            TradePriceType::FiveLevel,
        )
    }

    fn market(
        order_id: impl Into<String>,
        symbol: impl Into<String>,
        direction: TradeDirection,
        offset: TradeOffset,
        volume: i64,
        price_type: TradePriceType,
    ) -> Self {
        Self {
            order_id: order_id.into(),
            symbol: symbol.into(),
            direction,
            offset,
            volume,
            price_type,
            limit_price: None,
        }
    }

    fn from_outbound(request: &OutboundRequest) -> Result<Option<Self>> {
        let payload: Value = match request {
            OutboundRequest::Transport(OutboundFrame::Text(text)) => serde_json::from_str(text)
                .map_err(|_| TaskError::InvalidState("sim received invalid text payload"))?,
            OutboundRequest::Transport(OutboundFrame::Binary(bytes)) => {
                serde_json::from_slice(bytes)
                    .map_err(|_| TaskError::InvalidState("sim received invalid binary payload"))?
            }
            _ => return Ok(None),
        };
        if payload.get("aid").and_then(Value::as_str) != Some("insert_order") {
            return Ok(None);
        }

        let exchange_id = required_string(&payload, "exchange_id")?;
        let instrument_id = required_string(&payload, "instrument_id")?;
        let direction = TradeDirection::from_protocol_str(&required_string(&payload, "direction")?)
            .ok_or(TaskError::InvalidState("sim order direction is invalid"))?;
        let offset = TradeOffset::from_protocol_str(&required_string(&payload, "offset")?)
            .ok_or(TaskError::InvalidState("sim order offset is invalid"))?;
        let price_type =
            TradePriceType::from_protocol_str(&required_string(&payload, "price_type")?)
                .ok_or(TaskError::InvalidState("sim order price type is invalid"))?;
        let limit_price = payload.get("limit_price").and_then(Value::as_f64);

        Ok(Some(Self {
            order_id: required_string(&payload, "order_id")?,
            symbol: format!("{exchange_id}.{instrument_id}"),
            direction,
            offset,
            volume: required_i64(&payload, "volume")?,
            price_type,
            limit_price,
        }))
    }
}

impl TqSimStepReport {
    #[must_use]
    pub fn account(&self) -> Option<&Account> {
        self.account.as_ref()
    }

    #[must_use]
    pub fn orders(&self) -> &[Order] {
        &self.orders
    }

    #[must_use]
    pub fn trades(&self) -> &[Trade] {
        &self.trades
    }

    #[must_use]
    pub fn positions(&self) -> &[Position] {
        &self.positions
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.account.is_none()
            && self.orders.is_empty()
            && self.trades.is_empty()
            && self.positions.is_empty()
    }

    pub(crate) fn extend(&mut self, other: Self) {
        if other.account.is_some() {
            self.account = other.account;
        }
        self.orders.extend(other.orders);
        self.trades.extend(other.trades);
        self.positions.extend(other.positions);
    }
}

fn update_nonzero_position_count(count: &mut usize, previous: i64, next: i64) {
    match (previous == 0, next == 0) {
        (true, false) => *count += 1,
        (false, true) => *count = count.saturating_sub(1),
        _ => {}
    }
}

fn ingest_trade_report(
    host: &TaskHost,
    account_id: &str,
    report: &TqSimStepReport,
    command_ids: &[CommandId],
    scope: CommitScope,
) -> Result<()> {
    if report.is_empty() {
        return Ok(());
    }

    let mut account_node = serde_json::Map::new();
    if let Some(account) = &report.account {
        account_node.insert("accounts".to_string(), json!({ "CNY": account }));
    }
    if !report.orders.is_empty() {
        account_node.insert(
            "orders".to_string(),
            report
                .orders
                .iter()
                .map(|order| (order.order_id.clone(), json!(order)))
                .collect(),
        );
    }
    if !report.trades.is_empty() {
        account_node.insert(
            "trades".to_string(),
            report
                .trades
                .iter()
                .map(|trade| (trade.trade_id.clone(), json!(trade)))
                .collect(),
        );
    }
    if !report.positions.is_empty() {
        account_node.insert(
            "positions".to_string(),
            report
                .positions
                .iter()
                .map(|position| {
                    (
                        format!("{}.{}", position.exchange_id, position.instrument_id),
                        json!(position),
                    )
                })
                .collect(),
        );
    }

    host.api().session().handle().ingest(
        RuntimeInput::Io(IoEvent {
            route: "tqsim".to_string(),
            domains: vec![ProtocolDomain::Trade],
            payload: InputPayload::Json(json!({
                "aid": "rtn_data",
                "data": [{
                    "trade": {
                        account_id: Value::Object(account_node)
                    }
                }]
            })),
        }),
        command_ids.to_vec(),
        scope,
    )?;
    Ok(())
}

fn validate_request(request: &TqSimOrderRequest) -> Result<()> {
    if request.order_id.trim().is_empty() {
        return Err(TaskError::InvalidState("sim order id must not be empty"));
    }
    if request.symbol.trim().is_empty() {
        return Err(TaskError::InvalidState(
            "sim order symbol must not be empty",
        ));
    }
    if request.volume <= 0 {
        return Err(TaskError::InvalidState("sim order volume must be positive"));
    }
    if request.price_type == TradePriceType::Limit
        && !request.limit_price.is_some_and(|price| price.is_finite())
    {
        return Err(TaskError::InvalidState("sim limit price must be finite"));
    }
    Ok(())
}

fn order_volume_left(order: &SimOrder) -> i64 {
    if order.alive { order.request.volume } else { 0 }
}

fn signed_position_delta(direction: TradeDirection, offset: TradeOffset, volume: i64) -> i64 {
    match (direction, offset) {
        (TradeDirection::Buy, TradeOffset::Open | TradeOffset::Close | TradeOffset::CloseToday) => {
            volume
        }
        (
            TradeDirection::Sell,
            TradeOffset::Open | TradeOffset::Close | TradeOffset::CloseToday,
        ) => -volume,
    }
}

fn close_profit_for_delta(
    previous: i64,
    delta: i64,
    avg_price: f64,
    trade_price: f64,
    multiplier: f64,
) -> f64 {
    if previous == 0 || delta == 0 || previous.signum() == delta.signum() {
        return 0.0;
    }
    let closed_volume = previous.unsigned_abs().min(delta.unsigned_abs()) as f64;
    profit_for_net(
        previous.signum() * closed_volume as i64,
        avg_price,
        trade_price,
        multiplier,
    )
}

fn update_average_price(
    prices: &mut HashMap<String, f64>,
    symbol: &str,
    previous: i64,
    delta: i64,
    previous_avg: f64,
    trade_price: f64,
) {
    let next = previous + delta;
    if next == 0 {
        prices.remove(symbol);
        return;
    }
    if previous == 0 || previous.signum() == delta.signum() {
        let previous_volume = previous.unsigned_abs() as f64;
        let delta_volume = delta.unsigned_abs() as f64;
        let next_avg = (previous_avg * previous_volume + trade_price * delta_volume)
            / (previous_volume + delta_volume);
        prices.insert(symbol.to_string(), next_avg);
        return;
    }
    if next.signum() == previous.signum() {
        prices.insert(symbol.to_string(), previous_avg);
    } else {
        prices.insert(symbol.to_string(), trade_price);
    }
}

fn profit_for_net(net: i64, avg_price: f64, last_price: f64, multiplier: f64) -> f64 {
    if net == 0 || !avg_price.is_finite() || !last_price.is_finite() || !multiplier.is_finite() {
        return 0.0;
    }
    let volume = net.unsigned_abs() as f64;
    if net > 0 {
        (last_price - avg_price) * volume * multiplier
    } else {
        (avg_price - last_price) * volume * multiplier
    }
}

fn market_value(net: i64, last_price: f64, multiplier: f64) -> f64 {
    if net == 0 || !last_price.is_finite() || !multiplier.is_finite() {
        0.0
    } else {
        last_price * net.unsigned_abs() as f64 * multiplier
    }
}

fn split_symbol(symbol: &str) -> (&str, &str) {
    symbol.split_once('.').unwrap_or(("", symbol))
}

fn position_symbol(position: &Position) -> String {
    format!("{}.{}", position.exchange_id, position.instrument_id)
}

fn required_string(payload: &Value, key: &'static str) -> Result<String> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or(TaskError::InvalidState("sim payload missing string field"))
}

fn required_i64(payload: &Value, key: &'static str) -> Result<i64> {
    payload
        .get(key)
        .and_then(Value::as_i64)
        .ok_or(TaskError::InvalidState("sim payload missing integer field"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sim_skips_empty_quote_reports_when_no_orders_or_positions() {
        let mut sim = TqSim::new();
        sim.ensure_position("SHFE.rb2601");

        let empty = sim.update_quote_at("SHFE.rb2601", quote(100.0), 1_000);
        assert!(empty.is_empty());
        assert_eq!(sim.nonzero_position_count, 0);

        let open = sim
            .insert_order(TqSimOrderRequest::limit(
                "open-1",
                "SHFE.rb2601",
                TradeDirection::Buy,
                TradeOffset::Open,
                1,
                101.0,
            ))
            .expect("open order should fill against quote");
        assert!(!open.is_empty());
        assert_eq!(sim.nonzero_position_count, 1);

        let position_report = sim.update_quote_at("SHFE.rb2601", quote(101.0), 2_000);
        assert!(!position_report.is_empty());

        let close = sim
            .insert_order(TqSimOrderRequest::limit(
                "close-1",
                "SHFE.rb2601",
                TradeDirection::Sell,
                TradeOffset::Close,
                1,
                100.0,
            ))
            .expect("close order should fill against quote");
        assert!(!close.is_empty());
        assert_eq!(sim.nonzero_position_count, 0);

        let empty_again = sim.update_quote_at("SHFE.rb2601", quote(102.0), 3_000);
        assert!(empty_again.is_empty());
    }

    fn quote(last_price: f64) -> Quote {
        Quote {
            last_price,
            ask_price1: last_price + 0.5,
            ask_volume1: 10,
            bid_price1: last_price - 0.5,
            bid_volume1: 10,
            volume_multiple: 10,
            ..Quote::default()
        }
    }
}
