#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tqsdk_core::{
    Account, CommitResult, Notification, ObjectKey, Order, Position, PreInsertOrder,
    RiskManagementData, RiskManagementRule, RuntimeReader, SecurityAccount, SecurityOrder,
    SecurityPosition, SecurityTrade, SettlementInfo, SharedCommitResult, Trade,
    TradeStateReadGuard,
};

use crate::driver::DriverEvent;
use crate::{DomainCommitStream, Result, StreamFacadeError, ValueUpdate};

type CollectFn<T, C> = for<'a> fn(
    &SharedCommitResult,
    &TradeStateReadGuard<'a>,
    &C,
    &mut VecDeque<ValueUpdate<T>>,
) -> Result<()>;

#[derive(Debug, Clone)]
struct AccountScopedSpec {
    account_id: String,
}

struct CollectedEventStream<T, C> {
    inner: DomainCommitStream,
    reader: tqsdk_core::RuntimeReader,
    context: C,
    pending: VecDeque<ValueUpdate<T>>,
    collect: CollectFn<T, C>,
    marker: PhantomData<fn() -> T>,
}

impl<T, C> CollectedEventStream<T, C> {
    fn new(
        inner: DomainCommitStream,
        reader: tqsdk_core::RuntimeReader,
        context: C,
        collect: CollectFn<T, C>,
    ) -> Self {
        Self {
            inner,
            reader,
            context,
            pending: VecDeque::new(),
            collect,
            marker: PhantomData,
        }
    }
}

impl<T, C> Stream for CollectedEventStream<T, C>
where
    T: Unpin,
    C: Unpin,
{
    type Item = Result<ValueUpdate<T>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            if let Some(update) = this.pending.pop_front() {
                return Poll::Ready(Some(Ok(update)));
            }

            match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(commit))) => {
                    let trade = this.reader.read_trade_state();
                    if let Err(error) =
                        (this.collect)(&commit, &trade, &this.context, &mut this.pending)
                    {
                        return Poll::Ready(Some(Err(error)));
                    }
                    continue;
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum TradeObjectEvent {
    Account(Account),
    SecurityAccount(SecurityAccount),
    Position(Position),
    SecurityPosition(SecurityPosition),
    PreInsertOrder(PreInsertOrder),
    Order(Order),
    SecurityOrder(SecurityOrder),
    Trade(Trade),
    SecurityTrade(SecurityTrade),
    RiskManagementRule(RiskManagementRule),
    RiskManagementData(RiskManagementData),
    SettlementInfo(SettlementInfo),
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SessionReconnectEvent {
    pub attempt: u32,
    pub scheduled_backoff_ms: u64,
    pub max_attempts: Option<u32>,
    pub exhausted: bool,
    pub detail: Value,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
#[expect(
    clippy::large_enum_variant,
    reason = "boxing trade object events would add a heap allocation on the hot path; this facade keeps items inline and only buffers a small pending queue"
)]
pub enum TradeSessionEvent {
    TradeObject(TradeObjectEvent),
    Notification(Notification),
    Reconnect(SessionReconnectEvent),
    SessionError(tqsdk_session::SessionFacadeError),
}

#[derive(Debug, Clone)]
pub struct TradeSessionEventUpdate {
    pub commit: Option<SharedCommitResult>,
    pub event: TradeSessionEvent,
}

/// Commit-backed unified trade object event stream for one account.
pub struct TradeObjectEventStream {
    inner: CollectedEventStream<TradeObjectEvent, AccountScopedSpec>,
}

impl TradeObjectEventStream {
    pub(crate) fn new(
        inner: DomainCommitStream,
        reader: tqsdk_core::RuntimeReader,
        account_id: String,
    ) -> Self {
        Self {
            inner: CollectedEventStream::new(
                inner,
                reader,
                AccountScopedSpec { account_id },
                collect_trade_object_events,
            ),
        }
    }
}

impl Stream for TradeObjectEventStream {
    type Item = Result<ValueUpdate<TradeObjectEvent>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_next(cx)
    }
}

/// Raw-driver-backed unified trade session event stream for one account.
pub struct TradeSessionEventStream {
    inner: BroadcastStream<DriverEvent>,
    reader: tqsdk_core::RuntimeReader,
    spec: AccountScopedSpec,
    pending: VecDeque<TradeSessionEventUpdate>,
}

impl TradeSessionEventStream {
    pub(crate) fn new(
        receiver: broadcast::Receiver<DriverEvent>,
        reader: tqsdk_core::RuntimeReader,
        account_id: String,
    ) -> Self {
        Self {
            inner: BroadcastStream::new(receiver),
            reader,
            spec: AccountScopedSpec { account_id },
            pending: VecDeque::new(),
        }
    }
}

impl Stream for TradeSessionEventStream {
    type Item = Result<TradeSessionEventUpdate>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            if let Some(update) = this.pending.pop_front() {
                return Poll::Ready(Some(Ok(update)));
            }

            match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(DriverEvent::Commit(commit)))) => {
                    let trade = this.reader.read_trade_state();
                    if let Err(error) = collect_trade_session_commit_events(
                        &commit,
                        &trade,
                        &this.reader,
                        &this.spec,
                        &mut this.pending,
                    ) {
                        return Poll::Ready(Some(Err(error)));
                    }
                    continue;
                }
                Poll::Ready(Some(Ok(DriverEvent::Error(error)))) => {
                    return Poll::Ready(Some(Ok(TradeSessionEventUpdate {
                        commit: None,
                        event: TradeSessionEvent::SessionError(error),
                    })));
                }
                Poll::Ready(Some(Ok(DriverEvent::Closed))) => {
                    return Poll::Ready(Some(Err(StreamFacadeError::Closed)));
                }
                Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(skipped)))) => {
                    return Poll::Ready(Some(Err(StreamFacadeError::Lagged { skipped })));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

macro_rules! define_account_event_stream {
    ($(#[$meta:meta])* $name:ident, $value:ty, $collector:expr) => {
        $(#[$meta])*
        pub struct $name {
            inner: CollectedEventStream<$value, AccountScopedSpec>,
        }

        impl $name {
            pub(crate) fn new(
                inner: DomainCommitStream,
                reader: tqsdk_core::RuntimeReader,
                account_id: String,
            ) -> Self {
                Self {
                    inner: CollectedEventStream::new(
                        inner,
                        reader,
                        AccountScopedSpec { account_id },
                        $collector,
                    ),
                }
            }
        }

        impl Stream for $name {
            type Item = Result<ValueUpdate<$value>>;

            fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
                let this = self.get_mut();
                Pin::new(&mut this.inner).poll_next(cx)
            }
        }
    };
}

define_account_event_stream!(
    /// Commit-backed position update event stream for one futures trade account.
    PositionEventStream,
    Position,
    collect_position_events::<Position>
);
define_account_event_stream!(
    /// Commit-backed pre-insert order update event stream for one futures trade account.
    PreInsertOrderEventStream,
    PreInsertOrder,
    collect_pre_insert_order_events
);
define_account_event_stream!(
    /// Commit-backed order update event stream for one futures trade account.
    OrderEventStream,
    Order,
    collect_order_events::<Order>
);
define_account_event_stream!(
    /// Commit-backed trade fill event stream for one futures trade account.
    TradeEventStream,
    Trade,
    collect_trade_events::<Trade>
);
define_account_event_stream!(
    /// Commit-backed risk rule update event stream for one trade account.
    RiskManagementRuleEventStream,
    RiskManagementRule,
    collect_risk_management_rule_events
);
define_account_event_stream!(
    /// Commit-backed risk data update event stream for one trade account.
    RiskManagementDataEventStream,
    RiskManagementData,
    collect_risk_management_data_events
);
define_account_event_stream!(
    /// Commit-backed settlement info update event stream for one trade account.
    SettlementInfoEventStream,
    SettlementInfo,
    collect_settlement_info_events
);
define_account_event_stream!(
    /// Commit-backed position update event stream for one security trade account.
    SecurityPositionEventStream,
    SecurityPosition,
    collect_position_events::<SecurityPosition>
);
define_account_event_stream!(
    /// Commit-backed order update event stream for one security trade account.
    SecurityOrderEventStream,
    SecurityOrder,
    collect_order_events::<SecurityOrder>
);
define_account_event_stream!(
    /// Commit-backed trade fill event stream for one security trade account.
    SecurityTradeEventStream,
    SecurityTrade,
    collect_trade_events::<SecurityTrade>
);

fn push_decoded_update<T>(
    commit: &SharedCommitResult,
    trade: &TradeStateReadGuard<'_>,
    pending: &mut VecDeque<ValueUpdate<T>>,
    path: &[&str],
) -> Result<()>
where
    T: DeserializeOwned,
{
    if let Some(value) = trade.decode_path::<T>(path)? {
        pending.push_back(ValueUpdate {
            commit: commit.clone(),
            value,
        });
    }

    Ok(())
}

fn push_trade_object_event(
    commit: &SharedCommitResult,
    pending: &mut VecDeque<ValueUpdate<TradeObjectEvent>>,
    value: TradeObjectEvent,
) {
    pending.push_back(ValueUpdate {
        commit: commit.clone(),
        value,
    });
}

fn push_trade_session_event(
    commit: Option<&SharedCommitResult>,
    pending: &mut VecDeque<TradeSessionEventUpdate>,
    event: TradeSessionEvent,
) {
    pending.push_back(TradeSessionEventUpdate {
        commit: commit.cloned(),
        event,
    });
}

fn path_object_has_field(
    trade: &TradeStateReadGuard<'_>,
    path: &[&str],
    field: &str,
) -> Result<bool> {
    Ok(trade
        .decode_path::<Value>(path)?
        .and_then(|value| value.as_object().map(|object| object.contains_key(field)))
        .unwrap_or(false))
}

fn decode_trade_object_events<F>(
    commit: &CommitResult,
    trade: &TradeStateReadGuard<'_>,
    spec: &AccountScopedSpec,
    mut push: F,
) -> Result<()>
where
    F: FnMut(TradeObjectEvent),
{
    for object in &commit.changes.object_hits {
        match object {
            ObjectKey::Account { account_id } if account_id.as_str() == spec.account_id => {
                let path = [account_id.as_str(), "accounts", "CNY"];
                if path_object_has_field(trade, &path, "asset")? {
                    if let Some(value) = trade.decode_path::<SecurityAccount>(&path)? {
                        push(TradeObjectEvent::SecurityAccount(value));
                    }
                } else {
                    if let Some(value) = trade.decode_path::<Account>(&path)? {
                        push(TradeObjectEvent::Account(value));
                    }
                }
            }
            ObjectKey::Position { account_id, symbol }
                if account_id.as_str() == spec.account_id =>
            {
                let path = [account_id.as_str(), "positions", symbol.as_str()];
                if path_object_has_field(trade, &path, "create_date")? {
                    if let Some(value) = trade.decode_path::<SecurityPosition>(&path)? {
                        push(TradeObjectEvent::SecurityPosition(value));
                    }
                } else {
                    if let Some(value) = trade.decode_path::<Position>(&path)? {
                        push(TradeObjectEvent::Position(value));
                    }
                }
            }
            ObjectKey::PreInsertOrder {
                account_id,
                order_id,
            } if account_id.as_str() == spec.account_id => {
                if let Some(value) = trade.decode_path::<PreInsertOrder>(&[
                    account_id.as_str(),
                    "pre_insert_orders",
                    order_id.as_str(),
                ])? {
                    push(TradeObjectEvent::PreInsertOrder(value));
                }
            }
            ObjectKey::Order {
                account_id,
                order_id,
            } if account_id.as_str() == spec.account_id => {
                let path = [account_id.as_str(), "orders", order_id.as_str()];
                if path_object_has_field(trade, &path, "frozen_fee")? {
                    if let Some(value) = trade.decode_path::<SecurityOrder>(&path)? {
                        push(TradeObjectEvent::SecurityOrder(value));
                    }
                } else {
                    if let Some(value) = trade.decode_path::<Order>(&path)? {
                        push(TradeObjectEvent::Order(value));
                    }
                }
            }
            ObjectKey::Trade {
                account_id,
                trade_id,
            } if account_id.as_str() == spec.account_id => {
                let path = [account_id.as_str(), "trades", trade_id.as_str()];
                if path_object_has_field(trade, &path, "fee")? {
                    if let Some(value) = trade.decode_path::<SecurityTrade>(&path)? {
                        push(TradeObjectEvent::SecurityTrade(value));
                    }
                } else {
                    if let Some(value) = trade.decode_path::<Trade>(&path)? {
                        push(TradeObjectEvent::Trade(value));
                    }
                }
            }
            ObjectKey::RiskManagementRule {
                account_id,
                exchange_id,
            } if account_id.as_str() == spec.account_id => {
                if let Some(value) = trade.decode_path::<RiskManagementRule>(&[
                    account_id.as_str(),
                    "risk_management_rule",
                    exchange_id.as_str(),
                ])? {
                    push(TradeObjectEvent::RiskManagementRule(value));
                }
            }
            ObjectKey::RiskManagementData { account_id, symbol }
                if account_id.as_str() == spec.account_id =>
            {
                if let Some(value) = trade.decode_path::<RiskManagementData>(&[
                    account_id.as_str(),
                    "risk_management_data",
                    symbol.as_str(),
                ])? {
                    push(TradeObjectEvent::RiskManagementData(value));
                }
            }
            ObjectKey::Settlement {
                account_id,
                trading_day,
            } if account_id.as_str() == spec.account_id => {
                if let Some(value) = trade.decode_path::<SettlementInfo>(&[
                    account_id.as_str(),
                    "his_settlements",
                    trading_day.as_str(),
                ])? {
                    push(TradeObjectEvent::SettlementInfo(value));
                }
            }
            _ => {}
        }
    }

    Ok(())
}

fn collect_trade_object_events(
    commit: &SharedCommitResult,
    trade: &TradeStateReadGuard<'_>,
    spec: &AccountScopedSpec,
    pending: &mut VecDeque<ValueUpdate<TradeObjectEvent>>,
) -> Result<()> {
    decode_trade_object_events(commit, trade, spec, |event| {
        push_trade_object_event(commit, pending, event);
    })
}

fn collect_trade_session_commit_events(
    commit: &SharedCommitResult,
    trade: &TradeStateReadGuard<'_>,
    reader: &RuntimeReader,
    spec: &AccountScopedSpec,
    pending: &mut VecDeque<TradeSessionEventUpdate>,
) -> Result<()> {
    decode_trade_object_events(commit, trade, spec, |event| {
        push_trade_session_event(
            commit.into(),
            pending,
            TradeSessionEvent::TradeObject(event),
        );
    })?;

    let read_session_snapshot = || commit_requires_session_snapshot(commit).then(|| reader.read());
    let snapshot = read_session_snapshot();

    for object in &commit.changes.object_hits {
        match object {
            ObjectKey::Notification { notification_id } => {
                let Some(snapshot) = snapshot.as_ref() else {
                    continue;
                };
                let path = ["system", "notify", notification_id.as_str()];
                if let Some(notification) = snapshot.decode_path::<Notification>(&path)?
                    && (notification.user_id.is_empty() || notification.user_id == spec.account_id)
                {
                    push_trade_session_event(
                        commit.into(),
                        pending,
                        TradeSessionEvent::Notification(notification),
                    );
                }
            }
            ObjectKey::SessionReconnect => {
                let Some(snapshot) = snapshot.as_ref() else {
                    continue;
                };
                if let Some(reconnect) = snapshot.decode_path::<SessionReconnectEvent>(&[
                    "system",
                    "session",
                    "reconnect",
                ])? {
                    push_trade_session_event(
                        commit.into(),
                        pending,
                        TradeSessionEvent::Reconnect(reconnect),
                    );
                }
            }
            _ => {}
        }
    }

    Ok(())
}

fn commit_requires_session_snapshot(commit: &SharedCommitResult) -> bool {
    commit.changes.object_hits.iter().any(|object| {
        matches!(
            object,
            ObjectKey::Notification { .. } | ObjectKey::SessionReconnect
        )
    })
}

fn collect_position_events<T>(
    commit: &SharedCommitResult,
    trade: &TradeStateReadGuard<'_>,
    spec: &AccountScopedSpec,
    pending: &mut VecDeque<ValueUpdate<T>>,
) -> Result<()>
where
    T: DeserializeOwned,
{
    for object in &commit.changes.object_hits {
        if let ObjectKey::Position { account_id, symbol } = object
            && account_id.as_str() == spec.account_id
        {
            push_decoded_update(
                commit,
                trade,
                pending,
                &[account_id.as_str(), "positions", symbol.as_str()],
            )?;
        }
    }

    Ok(())
}

fn collect_pre_insert_order_events(
    commit: &SharedCommitResult,
    trade: &TradeStateReadGuard<'_>,
    spec: &AccountScopedSpec,
    pending: &mut VecDeque<ValueUpdate<PreInsertOrder>>,
) -> Result<()> {
    for object in &commit.changes.object_hits {
        if let ObjectKey::PreInsertOrder {
            account_id,
            order_id,
        } = object
            && account_id.as_str() == spec.account_id
        {
            push_decoded_update(
                commit,
                trade,
                pending,
                &[account_id.as_str(), "pre_insert_orders", order_id.as_str()],
            )?;
        }
    }

    Ok(())
}

fn collect_order_events<T>(
    commit: &SharedCommitResult,
    trade: &TradeStateReadGuard<'_>,
    spec: &AccountScopedSpec,
    pending: &mut VecDeque<ValueUpdate<T>>,
) -> Result<()>
where
    T: DeserializeOwned,
{
    for object in &commit.changes.object_hits {
        if let ObjectKey::Order {
            account_id,
            order_id,
        } = object
            && account_id.as_str() == spec.account_id
        {
            push_decoded_update(
                commit,
                trade,
                pending,
                &[account_id.as_str(), "orders", order_id.as_str()],
            )?;
        }
    }

    Ok(())
}

fn collect_trade_events<T>(
    commit: &SharedCommitResult,
    trade: &TradeStateReadGuard<'_>,
    spec: &AccountScopedSpec,
    pending: &mut VecDeque<ValueUpdate<T>>,
) -> Result<()>
where
    T: DeserializeOwned,
{
    for object in &commit.changes.object_hits {
        if let ObjectKey::Trade {
            account_id,
            trade_id,
        } = object
            && account_id.as_str() == spec.account_id
        {
            push_decoded_update(
                commit,
                trade,
                pending,
                &[account_id.as_str(), "trades", trade_id.as_str()],
            )?;
        }
    }

    Ok(())
}

fn collect_risk_management_rule_events(
    commit: &SharedCommitResult,
    trade: &TradeStateReadGuard<'_>,
    spec: &AccountScopedSpec,
    pending: &mut VecDeque<ValueUpdate<RiskManagementRule>>,
) -> Result<()> {
    for object in &commit.changes.object_hits {
        if let ObjectKey::RiskManagementRule {
            account_id,
            exchange_id,
        } = object
            && account_id.as_str() == spec.account_id
        {
            push_decoded_update(
                commit,
                trade,
                pending,
                &[
                    account_id.as_str(),
                    "risk_management_rule",
                    exchange_id.as_str(),
                ],
            )?;
        }
    }

    Ok(())
}

fn collect_risk_management_data_events(
    commit: &SharedCommitResult,
    trade: &TradeStateReadGuard<'_>,
    spec: &AccountScopedSpec,
    pending: &mut VecDeque<ValueUpdate<RiskManagementData>>,
) -> Result<()> {
    for object in &commit.changes.object_hits {
        if let ObjectKey::RiskManagementData { account_id, symbol } = object
            && account_id.as_str() == spec.account_id
        {
            push_decoded_update(
                commit,
                trade,
                pending,
                &[account_id.as_str(), "risk_management_data", symbol.as_str()],
            )?;
        }
    }

    Ok(())
}

fn collect_settlement_info_events(
    commit: &SharedCommitResult,
    trade: &TradeStateReadGuard<'_>,
    spec: &AccountScopedSpec,
    pending: &mut VecDeque<ValueUpdate<SettlementInfo>>,
) -> Result<()> {
    for object in &commit.changes.object_hits {
        if let ObjectKey::Settlement {
            account_id,
            trading_day,
        } = object
            && account_id.as_str() == spec.account_id
        {
            push_decoded_update(
                commit,
                trade,
                pending,
                &[account_id.as_str(), "his_settlements", trading_day.as_str()],
            )?;
        }
    }

    Ok(())
}
