#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tqsdk_core::{CommitResult, ObjectKey, Order, SnapshotReadGuard, Trade};

use crate::{DomainCommitStream, Result, ValueUpdate};

type CollectFn<T, C> = for<'a> fn(
    &CommitResult,
    &SnapshotReadGuard<'a>,
    &C,
    &mut VecDeque<ValueUpdate<T>>,
) -> Result<()>;

#[derive(Debug, Clone)]
struct OrderEventSpec {
    account_id: String,
}

#[derive(Debug, Clone)]
struct TradeEventSpec {
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

/// Commit-backed order update event stream for one trade account.
pub struct OrderEventStream {
    inner: TradeObjectEventStream<Order, OrderEventSpec>,
}

impl OrderEventStream {
    pub(crate) fn new(
        inner: DomainCommitStream,
        reader: tqsdk_core::RuntimeReader,
        account_id: String,
    ) -> Self {
        Self {
            inner: TradeObjectEventStream::new(
                inner,
                reader,
                OrderEventSpec { account_id },
                collect_order_events,
            ),
        }
    }
}

impl Stream for OrderEventStream {
    type Item = Result<ValueUpdate<Order>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_next(cx)
    }
}

/// Commit-backed trade fill event stream for one trade account.
pub struct TradeEventStream {
    inner: TradeObjectEventStream<Trade, TradeEventSpec>,
}

impl TradeEventStream {
    pub(crate) fn new(
        inner: DomainCommitStream,
        reader: tqsdk_core::RuntimeReader,
        account_id: String,
    ) -> Self {
        Self {
            inner: TradeObjectEventStream::new(
                inner,
                reader,
                TradeEventSpec { account_id },
                collect_trade_events,
            ),
        }
    }
}

impl Stream for TradeEventStream {
    type Item = Result<ValueUpdate<Trade>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_next(cx)
    }
}

fn collect_order_events(
    commit: &CommitResult,
    snapshot: &SnapshotReadGuard<'_>,
    spec: &OrderEventSpec,
    pending: &mut VecDeque<ValueUpdate<Order>>,
) -> Result<()> {
    for object in &commit.changes.object_hits {
        if let ObjectKey::Order {
            account_id,
            order_id,
        } = object
            && account_id.as_str() == spec.account_id
            && let Some(order) = snapshot.decode_path::<Order>(&[
                "trade",
                account_id.as_str(),
                "orders",
                order_id.as_str(),
            ])?
        {
            pending.push_back(ValueUpdate {
                commit: commit.clone(),
                value: order,
            });
        }
    }

    Ok(())
}

fn collect_trade_events(
    commit: &CommitResult,
    snapshot: &SnapshotReadGuard<'_>,
    spec: &TradeEventSpec,
    pending: &mut VecDeque<ValueUpdate<Trade>>,
) -> Result<()> {
    for object in &commit.changes.object_hits {
        if let ObjectKey::Trade {
            account_id,
            trade_id,
        } = object
            && account_id.as_str() == spec.account_id
            && let Some(trade) = snapshot.decode_path::<Trade>(&[
                "trade",
                account_id.as_str(),
                "trades",
                trade_id.as_str(),
            ])?
        {
            pending.push_back(ValueUpdate {
                commit: commit.clone(),
                value: trade,
            });
        }
    }

    Ok(())
}
