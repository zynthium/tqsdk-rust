#![cfg_attr(not(test), forbid(unsafe_code))]

use std::time::{Duration, Instant};

use serde_json::json;
use tqsdk_core::{
    AccountId, CommandId, CommandStatus, CommitResult, MarketTradeStateReadGuard, ObjectKey, Order,
    OrderId, OrderLifecycle, ProtocolDomain, Revision, RuntimeCommand, SharedCommitResult, Symbol,
    TradeCommand, TradeInsertOrderCommand, TradePriceType, TradeTimeCondition,
    TradeVolumeCondition, UpdateCursor,
};
use tqsdk_session::{OrderIntentRecord, OrderIntentRegistration, OrderIntentSpec, SessionClient};

use crate::{
    Result, RiskCheckReport, RiskDecision, RiskEngine, RiskProjectionReport, TaskError,
    TaskOrderIntent,
};

/// Thin session/reader profile for latency-sensitive trading-desk loops.
pub struct TradingDeskProfile {
    session: SessionClient,
    reader: tqsdk_core::RuntimeReader,
    cursor: UpdateCursor,
    subscribed_symbols: Vec<Symbol>,
    risk: Option<RiskEngine>,
    latency_probe: TradingLatencyProbe,
}

/// Builder for [`TradingDeskProfile`].
pub struct TradingDeskProfileBuilder {
    session: SessionClient,
    subscribed_symbols: Vec<Symbol>,
    risk: Option<RiskEngine>,
    latency_probe: TradingLatencyProbe,
}

/// Commit-backed market event observed by a trading-desk loop.
#[derive(Debug, Clone)]
pub struct TradingDeskMarketEvent {
    commit: SharedCommitResult,
    symbols: Vec<Symbol>,
    latency_cycle: Option<TradingLatencyCycle>,
}

/// Order that passed state-bound risk checks and has a registered client id.
#[derive(Debug, Clone)]
pub struct TradingDeskPrecheckedOrder {
    client_order_id: String,
    intent: TaskOrderIntent,
    risk_report: RiskCheckReport,
    projection: RiskProjectionReport,
    registration: OrderIntentRegistration,
}

/// Ticket returned by the trading-desk order submit path.
#[derive(Debug, Clone)]
pub struct TradingDeskOrderTicket {
    account_id: String,
    client_order_id: String,
    order_id: String,
    symbol: String,
    command_id: Option<CommandId>,
    submitted: bool,
}

/// Revision-bound typed status report for a trading-desk order ticket.
#[derive(Debug, Clone)]
pub struct TradingDeskOrderStatusReport {
    revision: Revision,
    command_id: Option<CommandId>,
    state: TradingDeskOrderState,
    order: Option<Order>,
}

/// Typed order state projected from runtime order lifecycle and command status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TradingDeskOrderState {
    Unknown,
    CommandPending {
        status: CommandStatus,
    },
    Live,
    Filled,
    PartiallyFilled {
        filled_volume: i64,
        volume_left: i64,
    },
    Cancelled,
    Rejected,
    Failed,
}

/// Lightweight opt-in latency probe for trading-desk cycles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradingLatencyProbe {
    enabled: bool,
}

/// Mutable marker set for one trading-desk decision cycle.
#[derive(Debug, Clone)]
pub struct TradingLatencyCycle {
    revision: Revision,
    market_event_seen_at: Instant,
    commit_seen_at: Instant,
    decision_at: Option<Instant>,
    risk_at: Option<Instant>,
    submit_at: Option<Instant>,
    ack_at: Option<Instant>,
}

/// Typed duration report for one completed trading-desk latency cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradingLatencyReport {
    revision: Revision,
    market_to_commit: Duration,
    commit_to_decision: Duration,
    decision_to_risk: Duration,
    risk_to_submit: Duration,
    submit_to_ack: Duration,
    total: Duration,
}

impl TradingDeskProfile {
    #[must_use]
    pub fn builder(session: SessionClient) -> TradingDeskProfileBuilder {
        TradingDeskProfileBuilder::new(session)
    }

    #[must_use]
    pub fn session(&self) -> &SessionClient {
        &self.session
    }

    #[must_use]
    pub fn read_market_trade_state(&self) -> MarketTradeStateReadGuard<'_> {
        self.reader.read_market_trade_state()
    }

    pub async fn next_market_event(
        &mut self,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<Option<TradingDeskMarketEvent>> {
        loop {
            while let Some(commit) = self.reader.next(&mut self.cursor) {
                if let Some(event) = self.market_event_from_commit(commit) {
                    return Ok(Some(event));
                }
            }

            let progress = self.session.progress_once(deadline).await?;
            if !progress.is_progress() {
                return Ok(None);
            }
        }
    }

    pub fn precheck_order(
        &self,
        state: &MarketTradeStateReadGuard<'_>,
        intent: TaskOrderIntent,
        client_order_id: impl Into<String>,
    ) -> Result<TradingDeskPrecheckedOrder> {
        validate_trading_desk_intent(&intent)?;
        let client_order_id = client_order_id.into();
        if client_order_id.trim().is_empty() {
            return Err(TaskError::InvalidState("client order id must not be empty"));
        }

        let risk = self.risk.clone().unwrap_or_default();
        let risk_report = risk.check_report_on_state(state, &intent)?;
        if let RiskDecision::Rejected(rejection) = risk_report.decision() {
            return Err(TaskError::RiskRejected(rejection.clone()));
        }
        let projection = risk.project_order_on_state(state, &intent)?;
        let record = OrderIntentRecord::new(OrderIntentSpec {
            account_id: intent.account_id.clone(),
            client_order_id: client_order_id.clone(),
            order_id: client_order_id.clone(),
            symbol: intent.symbol.clone(),
            direction: intent.direction,
            offset: intent.offset,
            volume: intent.volume,
            limit_price: intent.limit_price.expect("intent was validated"),
        });
        let registration = self.session.remember_order_intent(record)?;

        Ok(TradingDeskPrecheckedOrder {
            client_order_id,
            intent,
            risk_report,
            projection,
            registration,
        })
    }

    pub async fn submit_prechecked_order(
        &mut self,
        prechecked: TradingDeskPrecheckedOrder,
    ) -> Result<TradingDeskOrderTicket> {
        match prechecked.registration {
            OrderIntentRegistration::Existing(existing) => {
                Ok(TradingDeskOrderTicket::from_record(existing, false))
            }
            OrderIntentRegistration::Registered(_) => {
                let command = insert_order_command(&prechecked.intent, &prechecked.client_order_id);
                let submit = self.session.submit(command).await;
                match submit {
                    Ok(command_id) => {
                        self.session.update_order_intent_command(
                            &prechecked.intent.account_id,
                            &prechecked.client_order_id,
                            command_id,
                        )?;
                        if let Some(risk) = &mut self.risk {
                            risk.record_accepted_order(&prechecked.intent)?;
                        }
                        Ok(TradingDeskOrderTicket {
                            account_id: prechecked.intent.account_id,
                            client_order_id: prechecked.client_order_id.clone(),
                            order_id: prechecked.client_order_id,
                            symbol: prechecked.intent.symbol,
                            command_id: Some(command_id),
                            submitted: true,
                        })
                    }
                    Err(error) => {
                        self.session.forget_order_intent(
                            &prechecked.intent.account_id,
                            &prechecked.client_order_id,
                        )?;
                        Err(error.into())
                    }
                }
            }
        }
    }

    fn market_event_from_commit(
        &self,
        commit: SharedCommitResult,
    ) -> Option<TradingDeskMarketEvent> {
        if !commit.domains.contains(&ProtocolDomain::Market) {
            return None;
        }

        let mut symbols = market_symbols_from_commit(&commit);
        if symbols.is_empty() {
            symbols = self.subscribed_symbols.clone();
        }

        Some(TradingDeskMarketEvent {
            latency_cycle: self.latency_probe.start_cycle(commit.revision),
            commit,
            symbols,
        })
    }
}

impl TradingDeskProfileBuilder {
    #[must_use]
    pub fn new(session: SessionClient) -> Self {
        Self {
            session,
            subscribed_symbols: Vec::new(),
            risk: None,
            latency_probe: TradingLatencyProbe::disabled(),
        }
    }

    #[must_use]
    pub fn subscribe_quotes<I, S>(mut self, symbols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.subscribed_symbols = symbols
            .into_iter()
            .map(|symbol| Symbol::new(symbol.as_ref()))
            .collect();
        self
    }

    #[must_use]
    pub fn risk_engine(mut self, risk: RiskEngine) -> Self {
        self.risk = Some(risk);
        self
    }

    #[must_use]
    pub fn latency_probe(mut self, latency_probe: TradingLatencyProbe) -> Self {
        self.latency_probe = latency_probe;
        self
    }

    pub async fn build(self) -> Result<TradingDeskProfile> {
        if !self.subscribed_symbols.is_empty() {
            self.session
                .subscribe_quotes(self.subscribed_symbols.iter().map(|symbol| symbol.as_str()))
                .await?;
        }
        let reader = self.session.reader().clone();
        let cursor = reader.cursor();
        Ok(TradingDeskProfile {
            session: self.session,
            reader,
            cursor,
            subscribed_symbols: self.subscribed_symbols,
            risk: self.risk,
            latency_probe: self.latency_probe,
        })
    }
}

impl TradingDeskMarketEvent {
    #[must_use]
    pub fn revision(&self) -> Revision {
        self.commit.revision
    }

    #[must_use]
    pub fn commit(&self) -> &CommitResult {
        &self.commit
    }

    #[must_use]
    pub fn symbols(&self) -> &[Symbol] {
        &self.symbols
    }

    #[must_use]
    pub fn latency_cycle(&self) -> Option<&TradingLatencyCycle> {
        self.latency_cycle.as_ref()
    }

    #[must_use]
    pub fn into_latency_cycle(self) -> Option<TradingLatencyCycle> {
        self.latency_cycle
    }
}

impl TradingDeskPrecheckedOrder {
    #[must_use]
    pub fn client_order_id(&self) -> &str {
        &self.client_order_id
    }

    #[must_use]
    pub fn intent(&self) -> &TaskOrderIntent {
        &self.intent
    }

    #[must_use]
    pub fn risk_report(&self) -> &RiskCheckReport {
        &self.risk_report
    }

    #[must_use]
    pub fn projection(&self) -> &RiskProjectionReport {
        &self.projection
    }
}

impl TradingDeskOrderTicket {
    fn from_record(record: OrderIntentRecord, submitted: bool) -> Self {
        Self {
            account_id: record.account_id().to_string(),
            client_order_id: record.client_order_id().to_string(),
            order_id: record.order_id().to_string(),
            symbol: record.symbol().to_string(),
            command_id: record.command_id(),
            submitted,
        }
    }

    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub fn client_order_id(&self) -> &str {
        &self.client_order_id
    }

    #[must_use]
    pub fn order_id(&self) -> &str {
        &self.order_id
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn command_id(&self) -> Option<CommandId> {
        self.command_id
    }

    #[must_use]
    pub fn was_submitted(&self) -> bool {
        self.submitted
    }

    pub fn status(&self, desk: &TradingDeskProfile) -> Result<TradingDeskOrderStatusReport> {
        let state = desk.read_market_trade_state();
        let account_id = AccountId::new(self.account_id.clone());
        let order_id = OrderId::new(self.order_id.clone());
        let order = state.trade_state().order(&account_id, &order_id)?;
        let command_status = self
            .command_id
            .map(|command_id| desk.session.command_status_typed(command_id))
            .transpose()?
            .flatten();
        let order_state = match order.as_ref() {
            Some(order) => trading_desk_state_from_order(order),
            None => trading_desk_state_from_command(command_status),
        };

        Ok(TradingDeskOrderStatusReport {
            revision: state.revision(),
            command_id: self.command_id,
            state: order_state,
            order,
        })
    }
}

impl TradingDeskOrderStatusReport {
    #[must_use]
    pub fn revision(&self) -> Revision {
        self.revision
    }

    #[must_use]
    pub fn command_id(&self) -> Option<CommandId> {
        self.command_id
    }

    #[must_use]
    pub fn state(&self) -> &TradingDeskOrderState {
        &self.state
    }

    #[must_use]
    pub fn order(&self) -> Option<&Order> {
        self.order.as_ref()
    }
}

impl TradingLatencyProbe {
    #[must_use]
    pub fn enabled() -> Self {
        Self { enabled: true }
    }

    #[must_use]
    pub fn disabled() -> Self {
        Self { enabled: false }
    }

    #[must_use]
    pub fn is_enabled(self) -> bool {
        self.enabled
    }

    #[must_use]
    pub fn start_cycle(self, revision: Revision) -> Option<TradingLatencyCycle> {
        self.enabled.then(|| TradingLatencyCycle::new(revision))
    }
}

impl TradingLatencyCycle {
    #[must_use]
    pub fn new(revision: Revision) -> Self {
        let now = Instant::now();
        Self {
            revision,
            market_event_seen_at: now,
            commit_seen_at: now,
            decision_at: None,
            risk_at: None,
            submit_at: None,
            ack_at: None,
        }
    }

    #[must_use]
    pub fn revision(&self) -> Revision {
        self.revision
    }

    pub fn mark_decision(&mut self) {
        self.decision_at = Some(Instant::now());
    }

    pub fn mark_risk(&mut self) {
        self.risk_at = Some(Instant::now());
    }

    pub fn mark_submit(&mut self) {
        self.submit_at = Some(Instant::now());
    }

    pub fn mark_ack(&mut self) {
        self.ack_at = Some(Instant::now());
    }

    #[must_use]
    pub fn report(&self) -> Option<TradingLatencyReport> {
        let decision_at = self.decision_at?;
        let risk_at = self.risk_at?;
        let submit_at = self.submit_at?;
        let ack_at = self.ack_at?;

        Some(TradingLatencyReport {
            revision: self.revision,
            market_to_commit: self
                .commit_seen_at
                .saturating_duration_since(self.market_event_seen_at),
            commit_to_decision: decision_at.saturating_duration_since(self.commit_seen_at),
            decision_to_risk: risk_at.saturating_duration_since(decision_at),
            risk_to_submit: submit_at.saturating_duration_since(risk_at),
            submit_to_ack: ack_at.saturating_duration_since(submit_at),
            total: ack_at.saturating_duration_since(self.market_event_seen_at),
        })
    }
}

impl TradingLatencyReport {
    #[must_use]
    pub fn revision(&self) -> Revision {
        self.revision
    }

    #[must_use]
    pub fn market_to_commit(&self) -> Duration {
        self.market_to_commit
    }

    #[must_use]
    pub fn commit_to_decision(&self) -> Duration {
        self.commit_to_decision
    }

    #[must_use]
    pub fn decision_to_risk(&self) -> Duration {
        self.decision_to_risk
    }

    #[must_use]
    pub fn risk_to_submit(&self) -> Duration {
        self.risk_to_submit
    }

    #[must_use]
    pub fn submit_to_ack(&self) -> Duration {
        self.submit_to_ack
    }

    #[must_use]
    pub fn total(&self) -> Duration {
        self.total
    }
}

fn validate_trading_desk_intent(intent: &TaskOrderIntent) -> Result<()> {
    if intent.volume <= 0 {
        return Err(TaskError::InvalidState("order volume must be positive"));
    }
    if intent.offset.is_none() {
        return Err(TaskError::Unsupported(
            "trading desk orders require explicit offset",
        ));
    }
    let limit_price = intent
        .limit_price
        .ok_or(TaskError::InvalidState("limit price is required"))?;
    if !limit_price.is_finite() {
        return Err(TaskError::InvalidState("limit price must be finite"));
    }
    Ok(())
}

fn insert_order_command(intent: &TaskOrderIntent, order_id: &str) -> RuntimeCommand {
    RuntimeCommand::Trade(TradeCommand::InsertOrder(TradeInsertOrderCommand {
        account_id: AccountId::new(intent.account_id.clone()),
        order_id: OrderId::new(order_id),
        symbol: Symbol::new(intent.symbol.clone()),
        direction: intent.direction,
        offset: intent.offset,
        volume: intent.volume,
        price_type: TradePriceType::Limit,
        limit_price: Some(json!(intent.limit_price.expect("intent was validated"))),
        time_condition: TradeTimeCondition::Gfd,
        volume_condition: TradeVolumeCondition::Any,
    }))
}

fn trading_desk_state_from_order(order: &Order) -> TradingDeskOrderState {
    match order.lifecycle {
        OrderLifecycle::Filled => TradingDeskOrderState::Filled,
        OrderLifecycle::PartiallyFilled => TradingDeskOrderState::PartiallyFilled {
            filled_volume: (order.volume_origin - order.volume_left).max(0),
            volume_left: order.volume_left,
        },
        OrderLifecycle::Cancelled => TradingDeskOrderState::Cancelled,
        OrderLifecycle::Rejected => TradingDeskOrderState::Rejected,
        OrderLifecycle::Failed => TradingDeskOrderState::Failed,
        OrderLifecycle::Unknown
        | OrderLifecycle::Submitting
        | OrderLifecycle::Sent
        | OrderLifecycle::Accepted
        | OrderLifecycle::Cancelling => TradingDeskOrderState::Live,
    }
}

fn trading_desk_state_from_command(command_status: Option<CommandStatus>) -> TradingDeskOrderState {
    match command_status {
        Some(CommandStatus::Rejected) => TradingDeskOrderState::Rejected,
        Some(CommandStatus::Cancelled) => TradingDeskOrderState::Cancelled,
        Some(CommandStatus::Failed) => TradingDeskOrderState::Failed,
        Some(CommandStatus::Completed) | None => TradingDeskOrderState::Unknown,
        Some(status) => TradingDeskOrderState::CommandPending { status },
    }
}

fn market_symbols_from_commit(commit: &CommitResult) -> Vec<Symbol> {
    let mut symbols = Vec::new();
    for object in &commit.changes.object_hits {
        let symbol = match object {
            ObjectKey::Quote { symbol }
            | ObjectKey::TradingStatus { symbol }
            | ObjectKey::Tick { symbol, .. } => Some(symbol.clone()),
            ObjectKey::Kline { series, .. } => Some(series.primary.clone()),
            _ => None,
        };
        if let Some(symbol) = symbol
            && !symbols.contains(&symbol)
        {
            symbols.push(symbol);
        }
    }
    symbols
}
