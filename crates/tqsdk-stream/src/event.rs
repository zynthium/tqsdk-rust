#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use serde::de::DeserializeOwned;
use tqsdk_core::{
    CommitResult, ObjectKey, Order, Position, PreInsertOrder, RiskManagementData,
    RiskManagementRule, SecurityOrder, SecurityPosition, SecurityTrade, SettlementInfo,
    SnapshotReadGuard, Trade,
};

use crate::{DomainCommitStream, Result, ValueUpdate};

type CollectFn<T, C> = for<'a> fn(
    &CommitResult,
    &SnapshotReadGuard<'a>,
    &C,
    &mut VecDeque<ValueUpdate<T>>,
) -> Result<()>;

#[derive(Debug, Clone)]
struct AccountScopedSpec {
    account_id: String,
}

struct TradeObjectEventStream<T, C> {
    inner: DomainCommitStream,
    reader: tqsdk_core::RuntimeReader,
    context: C,
    pending: VecDeque<ValueUpdate<T>>,
    collect: CollectFn<T, C>,
    marker: PhantomData<fn() -> T>,
}

impl<T, C> TradeObjectEventStream<T, C> {
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

impl<T, C> Stream for TradeObjectEventStream<T, C>
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
                    let snapshot = this.reader.read();
                    if let Err(error) =
                        (this.collect)(&commit, &snapshot, &this.context, &mut this.pending)
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

macro_rules! define_account_event_stream {
    ($(#[$meta:meta])* $name:ident, $value:ty, $collector:expr) => {
        $(#[$meta])*
        pub struct $name {
            inner: TradeObjectEventStream<$value, AccountScopedSpec>,
        }

        impl $name {
            pub(crate) fn new(
                inner: DomainCommitStream,
                reader: tqsdk_core::RuntimeReader,
                account_id: String,
            ) -> Self {
                Self {
                    inner: TradeObjectEventStream::new(
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
    commit: &CommitResult,
    snapshot: &SnapshotReadGuard<'_>,
    pending: &mut VecDeque<ValueUpdate<T>>,
    path: &[&str],
) -> Result<()>
where
    T: DeserializeOwned,
{
    if let Some(value) = snapshot.decode_path::<T>(path)? {
        pending.push_back(ValueUpdate {
            commit: commit.clone(),
            value,
        });
    }

    Ok(())
}

fn collect_position_events<T>(
    commit: &CommitResult,
    snapshot: &SnapshotReadGuard<'_>,
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
                snapshot,
                pending,
                &["trade", account_id.as_str(), "positions", symbol.as_str()],
            )?;
        }
    }

    Ok(())
}

fn collect_pre_insert_order_events(
    commit: &CommitResult,
    snapshot: &SnapshotReadGuard<'_>,
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
                snapshot,
                pending,
                &[
                    "trade",
                    account_id.as_str(),
                    "pre_insert_orders",
                    order_id.as_str(),
                ],
            )?;
        }
    }

    Ok(())
}

fn collect_order_events<T>(
    commit: &CommitResult,
    snapshot: &SnapshotReadGuard<'_>,
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
                snapshot,
                pending,
                &["trade", account_id.as_str(), "orders", order_id.as_str()],
            )?;
        }
    }

    Ok(())
}

fn collect_trade_events<T>(
    commit: &CommitResult,
    snapshot: &SnapshotReadGuard<'_>,
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
                snapshot,
                pending,
                &["trade", account_id.as_str(), "trades", trade_id.as_str()],
            )?;
        }
    }

    Ok(())
}

fn collect_risk_management_rule_events(
    commit: &CommitResult,
    snapshot: &SnapshotReadGuard<'_>,
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
                snapshot,
                pending,
                &[
                    "trade",
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
    commit: &CommitResult,
    snapshot: &SnapshotReadGuard<'_>,
    spec: &AccountScopedSpec,
    pending: &mut VecDeque<ValueUpdate<RiskManagementData>>,
) -> Result<()> {
    for object in &commit.changes.object_hits {
        if let ObjectKey::RiskManagementData { account_id, symbol } = object
            && account_id.as_str() == spec.account_id
        {
            push_decoded_update(
                commit,
                snapshot,
                pending,
                &[
                    "trade",
                    account_id.as_str(),
                    "risk_management_data",
                    symbol.as_str(),
                ],
            )?;
        }
    }

    Ok(())
}

fn collect_settlement_info_events(
    commit: &CommitResult,
    snapshot: &SnapshotReadGuard<'_>,
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
                snapshot,
                pending,
                &[
                    "trade",
                    account_id.as_str(),
                    "his_settlements",
                    trading_day.as_str(),
                ],
            )?;
        }
    }

    Ok(())
}
