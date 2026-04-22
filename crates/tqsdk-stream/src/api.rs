#![cfg_attr(not(test), forbid(unsafe_code))]

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tqsdk_core::{
    Account, CommitScope, Notification, ObjectKey, Order, Position, PreInsertOrder, ProtocolDomain,
    Quote, RiskManagementData, RiskManagementRule, SecurityAccount, SecurityOrder,
    SecurityPosition, SecurityTrade, SettlementInfo, StatePath, Trade, TradingStatus,
};

use crate::driver::{DriverEvent, StreamDriver};
use crate::filter::{
    DomainCommitStream, FieldCommitStream, ObjectCommitStream, PathCommitStream, ScopeCommitStream,
};
use crate::typed::PathValueStream;

const DEFAULT_COMMIT_CHANNEL_CAPACITY: usize = 1024;

/// Shared-session stream facade over [`tqsdk_session::SessionClient`].
///
/// [`TqStream`] lazily starts a single background driver task that advances the
/// underlying session and fans out canonical [`tqsdk_core::CommitResult`]
/// values to multiple async consumers.
pub struct TqStream {
    session: Option<tqsdk_session::SessionClient>,
    reader: tqsdk_core::RuntimeReader,
    driver: StreamDriver,
}

impl TqStream {
    #[must_use]
    pub fn new(session: tqsdk_session::SessionClient) -> Self {
        Self::new_with_capacity(session, DEFAULT_COMMIT_CHANNEL_CAPACITY)
    }

    fn new_with_capacity(session: tqsdk_session::SessionClient, capacity: usize) -> Self {
        let reader = session.reader_clone();
        let driver = StreamDriver::new(session.clone(), reader.clone(), capacity);
        Self {
            session: Some(session),
            reader,
            driver,
        }
    }

    #[must_use]
    pub fn session(&self) -> &tqsdk_session::SessionClient {
        self.session
            .as_ref()
            .expect("tqsdk-stream session missing while facade is alive")
    }

    #[must_use]
    pub fn reader(&self) -> &tqsdk_core::RuntimeReader {
        &self.reader
    }

    pub fn path_stream<T, I, S>(&self, path: I) -> crate::error::Result<PathValueStream<T>>
    where
        T: serde::de::DeserializeOwned,
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let path = StatePath::new(path);
        let commits = self.commit_stream()?.filter_paths([path.clone()]);
        Ok(PathValueStream::new(commits, self.reader.clone(), path))
    }

    pub fn quote_stream(
        &self,
        symbol: impl AsRef<str>,
    ) -> crate::error::Result<PathValueStream<Quote>> {
        self.path_stream(["quotes", symbol.as_ref()])
    }

    pub fn trading_status_stream(
        &self,
        symbol: impl AsRef<str>,
    ) -> crate::error::Result<PathValueStream<TradingStatus>> {
        self.path_stream(["trading_status", symbol.as_ref()])
    }

    pub fn account_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> crate::error::Result<PathValueStream<Account>> {
        self.path_stream(["trade", account_id.as_ref(), "accounts", "CNY"])
    }

    pub fn position_stream(
        &self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> crate::error::Result<PathValueStream<Position>> {
        self.path_stream(["trade", account_id.as_ref(), "positions", symbol.as_ref()])
    }

    pub fn pre_insert_order_stream(
        &self,
        account_id: impl AsRef<str>,
        order_id: impl AsRef<str>,
    ) -> crate::error::Result<PathValueStream<PreInsertOrder>> {
        self.path_stream([
            "trade",
            account_id.as_ref(),
            "pre_insert_orders",
            order_id.as_ref(),
        ])
    }

    pub fn order_stream(
        &self,
        account_id: impl AsRef<str>,
        order_id: impl AsRef<str>,
    ) -> crate::error::Result<PathValueStream<Order>> {
        self.path_stream(["trade", account_id.as_ref(), "orders", order_id.as_ref()])
    }

    pub fn trade_stream(
        &self,
        account_id: impl AsRef<str>,
        trade_id: impl AsRef<str>,
    ) -> crate::error::Result<PathValueStream<Trade>> {
        self.path_stream(["trade", account_id.as_ref(), "trades", trade_id.as_ref()])
    }

    pub fn notification_stream(
        &self,
        notification_id: impl AsRef<str>,
    ) -> crate::error::Result<PathValueStream<Notification>> {
        self.path_stream(["system", "notify", notification_id.as_ref()])
    }

    pub fn risk_management_rule_stream(
        &self,
        account_id: impl AsRef<str>,
        exchange_id: impl AsRef<str>,
    ) -> crate::error::Result<PathValueStream<RiskManagementRule>> {
        self.path_stream([
            "trade",
            account_id.as_ref(),
            "risk_management_rule",
            exchange_id.as_ref(),
        ])
    }

    pub fn risk_management_data_stream(
        &self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> crate::error::Result<PathValueStream<RiskManagementData>> {
        self.path_stream([
            "trade",
            account_id.as_ref(),
            "risk_management_data",
            symbol.as_ref(),
        ])
    }

    pub fn settlement_info_stream(
        &self,
        account_id: impl AsRef<str>,
        trading_day: impl AsRef<str>,
    ) -> crate::error::Result<PathValueStream<SettlementInfo>> {
        self.path_stream([
            "trade",
            account_id.as_ref(),
            "his_settlements",
            trading_day.as_ref(),
        ])
    }

    pub fn security_account_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> crate::error::Result<PathValueStream<SecurityAccount>> {
        self.path_stream(["trade", account_id.as_ref(), "accounts", "CNY"])
    }

    pub fn security_position_stream(
        &self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> crate::error::Result<PathValueStream<SecurityPosition>> {
        self.path_stream(["trade", account_id.as_ref(), "positions", symbol.as_ref()])
    }

    pub fn security_order_stream(
        &self,
        account_id: impl AsRef<str>,
        order_id: impl AsRef<str>,
    ) -> crate::error::Result<PathValueStream<SecurityOrder>> {
        self.path_stream(["trade", account_id.as_ref(), "orders", order_id.as_ref()])
    }

    pub fn security_trade_stream(
        &self,
        account_id: impl AsRef<str>,
        trade_id: impl AsRef<str>,
    ) -> crate::error::Result<PathValueStream<SecurityTrade>> {
        self.path_stream(["trade", account_id.as_ref(), "trades", trade_id.as_ref()])
    }

    #[must_use]
    pub fn into_session(mut self) -> tqsdk_session::SessionClient {
        self.driver.abort();
        self.session
            .take()
            .expect("tqsdk-stream session missing during into_session")
    }

    pub fn commit_stream(&self) -> crate::error::Result<CommitStream> {
        let receiver = self.driver.subscribe();
        self.driver.ensure_started()?;
        Ok(CommitStream::new(receiver))
    }

    #[doc(hidden)]
    #[must_use]
    pub fn new_for_test_with_capacity(
        session: tqsdk_session::SessionClient,
        capacity: usize,
    ) -> Self {
        Self::new_with_capacity(session, capacity)
    }

    #[doc(hidden)]
    pub fn handle_for_test(&self) -> tqsdk_core::RuntimeHandle {
        self.session().handle().clone()
    }
}

impl Drop for TqStream {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

/// Async stream of canonical runtime commits.
pub struct CommitStream {
    inner: BroadcastStream<DriverEvent>,
}

impl CommitStream {
    fn new(receiver: broadcast::Receiver<DriverEvent>) -> Self {
        Self {
            inner: BroadcastStream::new(receiver),
        }
    }

    #[must_use]
    pub fn filter_path<I, S>(self, path: I) -> PathCommitStream
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        PathCommitStream::new(self, vec![StatePath::new(path)])
    }

    #[must_use]
    pub fn filter_paths(self, paths: impl IntoIterator<Item = StatePath>) -> PathCommitStream {
        PathCommitStream::new(self, paths.into_iter().collect())
    }

    #[must_use]
    pub fn filter_scope(self, scope: CommitScope) -> ScopeCommitStream {
        ScopeCommitStream::new(self, vec![scope])
    }

    #[must_use]
    pub fn filter_scopes(self, scopes: impl IntoIterator<Item = CommitScope>) -> ScopeCommitStream {
        ScopeCommitStream::new(self, scopes.into_iter().collect())
    }

    #[must_use]
    pub fn filter_domain(self, domain: ProtocolDomain) -> DomainCommitStream {
        DomainCommitStream::new(self, vec![domain])
    }

    #[must_use]
    pub fn filter_domains(
        self,
        domains: impl IntoIterator<Item = ProtocolDomain>,
    ) -> DomainCommitStream {
        DomainCommitStream::new(self, domains.into_iter().collect())
    }

    #[must_use]
    pub fn filter_object(self, object: ObjectKey) -> ObjectCommitStream {
        ObjectCommitStream::new(self, vec![object])
    }

    #[must_use]
    pub fn filter_objects(
        self,
        objects: impl IntoIterator<Item = ObjectKey>,
    ) -> ObjectCommitStream {
        ObjectCommitStream::new(self, objects.into_iter().collect())
    }

    #[must_use]
    pub fn filter_fields<I, S>(self, object: ObjectKey, fields: I) -> FieldCommitStream
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        FieldCommitStream::new(self, object, fields.into_iter().map(Into::into).collect())
    }
}

impl Stream for CommitStream {
    type Item = crate::error::Result<tqsdk_core::CommitResult>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(DriverEvent::Commit(commit)))) => Poll::Ready(Some(Ok(commit))),
            Poll::Ready(Some(Ok(DriverEvent::Error(error)))) => {
                Poll::Ready(Some(Err(crate::error::StreamFacadeError::Session(error))))
            }
            Poll::Ready(Some(Ok(DriverEvent::Closed))) => {
                Poll::Ready(Some(Err(crate::error::StreamFacadeError::Closed)))
            }
            Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(skipped)))) => {
                Poll::Ready(Some(Err(crate::error::StreamFacadeError::Lagged {
                    skipped,
                })))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
