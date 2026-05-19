#![cfg_attr(not(test), forbid(unsafe_code))]

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tqsdk_core::{
    Account, CommitScope, MarketChartCommand, Notification, ObjectKey, Order, Position,
    PreInsertOrder, ProtocolDomain, Quote, RiskManagementData, RiskManagementRule, SecurityAccount,
    SecurityOrder, SecurityPosition, SecurityTrade, SettlementInfo, StatePath, Symbol, Trade,
    TradingStatus,
};

use crate::driver::{DriverEvent, StreamDriver};
use crate::event::{
    OrderEventStream, PositionEventStream, PreInsertOrderEventStream,
    RiskManagementDataEventStream, RiskManagementRuleEventStream, SecurityOrderEventStream,
    SecurityPositionEventStream, SecurityTradeEventStream, SettlementInfoEventStream,
    TradeEventStream, TradeObjectEventStream, TradeSessionEventStream,
};
use crate::filter::{
    DomainCommitStream, FieldCommitStream, ObjectCommitStream, PathCommitStream, ScopeCommitStream,
};
use crate::path_dispatcher::PathDispatcher;
use crate::quote_subscription::{
    QuoteBatchSubscription, QuoteSubscription, submit_subscribe, submit_unsubscribe,
    validate_quote_symbols,
};
use crate::typed::PathValueStream;
use crate::window::{KlineRowStream, TickRowStream, kline_chart_id, tick_chart_id};

pub(crate) const DEFAULT_COMMIT_CHANNEL_CAPACITY: usize = 1024;
pub(crate) const COMMIT_CHANNEL_CAPACITY_PER_CONSUMER: usize = 8;

pub(crate) fn commit_channel_capacity_for_consumers(
    expected_consumers: usize,
) -> crate::error::Result<usize> {
    if expected_consumers == 0 {
        return Err(crate::error::StreamFacadeError::InvalidState(
            "expected commit consumers must be greater than zero",
        ));
    }

    let scaled = expected_consumers
        .checked_mul(COMMIT_CHANNEL_CAPACITY_PER_CONSUMER)
        .ok_or(crate::error::StreamFacadeError::InvalidState(
            "expected commit consumers exceeds supported commit channel capacity",
        ))?;

    Ok(scaled.max(DEFAULT_COMMIT_CHANNEL_CAPACITY))
}

pub(crate) fn duration_to_ns(duration: Duration) -> crate::error::Result<i64> {
    i64::try_from(duration.as_nanos()).map_err(|_| {
        crate::error::StreamFacadeError::InvalidState("kline duration exceeds i64 nanoseconds")
    })
}

/// Shared-session stream facade over [`tqsdk_session::SessionClient`].
///
/// [`TqStream`] lazily starts a single background driver task that advances the
/// underlying session and fans out canonical
/// [`tqsdk_core::SharedCommitResult`] values to multiple async consumers.
pub struct TqStream {
    session: Option<tqsdk_session::SessionClient>,
    reader: tqsdk_core::RuntimeReader,
    driver: StreamDriver,
    path_dispatcher: PathDispatcher,
}

impl TqStream {
    #[must_use]
    pub fn new(session: tqsdk_session::SessionClient) -> Self {
        Self::new_with_capacity(session, DEFAULT_COMMIT_CHANNEL_CAPACITY)
    }

    pub fn with_commit_channel_capacity(
        session: tqsdk_session::SessionClient,
        capacity: usize,
    ) -> crate::error::Result<Self> {
        if capacity == 0 {
            return Err(crate::error::StreamFacadeError::InvalidState(
                "commit channel capacity must be greater than zero",
            ));
        }
        Ok(Self::new_with_capacity(session, capacity))
    }

    /// Builds a stream facade with a root fan-out capacity sized from the
    /// expected number of independent commit consumers.
    ///
    /// The capacity is `max(1024, expected_consumers * 8)`. Use
    /// [`Self::with_commit_channel_capacity`] when a workload needs an explicit
    /// ring size instead of the built-in heuristic.
    pub fn with_expected_commit_consumers(
        session: tqsdk_session::SessionClient,
        expected_consumers: usize,
    ) -> crate::error::Result<Self> {
        let capacity = commit_channel_capacity_for_consumers(expected_consumers)?;
        Ok(Self::new_with_capacity(session, capacity))
    }

    pub(crate) fn new_with_capacity(
        session: tqsdk_session::SessionClient,
        capacity: usize,
    ) -> Self {
        let reader = session.reader_clone();
        let driver = StreamDriver::new(session.clone(), reader.clone(), capacity);
        Self {
            session: Some(session),
            reader,
            driver,
            path_dispatcher: PathDispatcher::new(capacity),
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

    pub fn health(&self) -> crate::error::Result<crate::health::StreamHealthSnapshot> {
        crate::health::read_health(&self.reader, self.driver.is_closed())
    }

    #[must_use]
    pub fn reconnect_monitor(&self) -> crate::reconnect::StreamReconnectMonitor<'_> {
        crate::reconnect::StreamReconnectMonitor::new(self)
    }

    #[must_use]
    pub fn graceful_shutdown(self) -> crate::shutdown::StreamGracefulShutdown {
        crate::shutdown::StreamGracefulShutdown::new(self)
    }

    pub fn path_stream<T, I, S>(&self, path: I) -> crate::error::Result<PathValueStream<T>>
    where
        T: serde::de::DeserializeOwned,
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let path = StatePath::new(path);
        let commits = self.path_commit_stream([path.clone()])?;
        Ok(PathValueStream::new(commits, self.reader.clone(), path))
    }

    pub fn quote_stream(
        &self,
        symbol: impl AsRef<str>,
    ) -> crate::error::Result<PathValueStream<Quote>> {
        self.path_stream(["quotes", symbol.as_ref()])
    }

    pub async fn quotes<I, S>(&self, symbols: I) -> crate::error::Result<QuoteSubscription>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Ok(QuoteSubscription::new(self.quote_batches(symbols).await?))
    }

    pub async fn quote_batches<I, S>(
        &self,
        symbols: I,
    ) -> crate::error::Result<QuoteBatchSubscription>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let symbols = symbols
            .into_iter()
            .map(|symbol| Symbol::new(symbol.as_ref()))
            .collect::<Vec<_>>();
        validate_quote_symbols(&symbols)?;
        let commits = self.commit_stream()?;
        let lease = self
            .session()
            .ensure_quotes(symbols.iter().map(Symbol::as_str))
            .await?;
        Ok(QuoteBatchSubscription::new(
            commits,
            self.session().clone(),
            self.reader.clone(),
            symbols,
            lease,
        ))
    }

    pub async fn subscribe_quotes<I, S>(&self, symbols: I) -> crate::error::Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let symbols = symbols
            .into_iter()
            .map(|symbol| Symbol::new(symbol.as_ref()))
            .collect::<Vec<_>>();
        submit_subscribe(self.session(), symbols).await
    }

    pub async fn unsubscribe_quotes<I, S>(&self, symbols: I) -> crate::error::Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let symbols = symbols
            .into_iter()
            .map(|symbol| Symbol::new(symbol.as_ref()))
            .collect::<Vec<_>>();
        submit_unsubscribe(self.session(), symbols).await
    }

    pub fn trading_status_stream(
        &self,
        symbol: impl AsRef<str>,
    ) -> crate::error::Result<PathValueStream<TradingStatus>> {
        self.path_stream(["trading_status", symbol.as_ref()])
    }

    #[must_use]
    pub fn market_events(&self) -> crate::market_event::MarketEventBuilder<'_> {
        crate::market_event::MarketEventBuilder::new(self)
    }

    #[must_use]
    pub fn recover_state(&self) -> crate::recovery::StreamStartupRecovery<'_> {
        crate::recovery::StreamStartupRecovery::new(self)
    }

    pub async fn kline_stream(
        &self,
        symbol: impl AsRef<str>,
        duration: Duration,
        data_length: usize,
    ) -> crate::error::Result<KlineRowStream> {
        let symbol = symbol.as_ref().to_owned();
        let duration_ns = duration_to_ns(duration)?;
        let duration_key = duration_ns.to_string();
        let chart_id = kline_chart_id(symbol.as_str(), duration_ns, data_length);
        let commits = self.path_commit_stream([
            StatePath::new(["charts", chart_id.as_str()]),
            StatePath::new(["klines", symbol.as_str(), duration_key.as_str(), "data"]),
        ])?;

        let lease = self
            .session()
            .ensure_chart(MarketChartCommand {
                chart_id: chart_id.clone(),
                symbols: vec![Symbol::new(symbol.clone())],
                duration_ns,
                view_width: data_length,
                left_kline_id: None,
                focus_datetime_ns: None,
                focus_position: None,
            })
            .await?;

        Ok(KlineRowStream::new(
            commits,
            lease,
            self.reader.clone(),
            symbol,
            duration_ns,
            data_length,
            chart_id,
        ))
    }

    pub async fn tick_stream(
        &self,
        symbol: impl AsRef<str>,
        data_length: usize,
    ) -> crate::error::Result<TickRowStream> {
        let symbol = symbol.as_ref().to_owned();
        let chart_id = tick_chart_id(symbol.as_str(), data_length);
        let commits = self.path_commit_stream([
            StatePath::new(["charts", chart_id.as_str()]),
            StatePath::new(["ticks", symbol.as_str(), "data"]),
        ])?;

        let lease = self
            .session()
            .ensure_chart(MarketChartCommand {
                chart_id: chart_id.clone(),
                symbols: vec![Symbol::new(symbol.clone())],
                duration_ns: 0,
                view_width: data_length,
                left_kline_id: None,
                focus_datetime_ns: None,
                focus_position: None,
            })
            .await?;

        Ok(TickRowStream::new(
            commits,
            lease,
            self.reader.clone(),
            symbol,
            data_length,
            chart_id,
        ))
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

    pub fn order_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> crate::error::Result<OrderEventStream> {
        Ok(OrderEventStream::new(
            self.commit_stream()?.filter_domain(ProtocolDomain::Trade),
            self.reader.clone(),
            account_id.as_ref().to_owned(),
        ))
    }

    pub fn position_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> crate::error::Result<PositionEventStream> {
        Ok(PositionEventStream::new(
            self.commit_stream()?.filter_domain(ProtocolDomain::Trade),
            self.reader.clone(),
            account_id.as_ref().to_owned(),
        ))
    }

    pub fn pre_insert_order_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> crate::error::Result<PreInsertOrderEventStream> {
        Ok(PreInsertOrderEventStream::new(
            self.commit_stream()?.filter_domain(ProtocolDomain::Trade),
            self.reader.clone(),
            account_id.as_ref().to_owned(),
        ))
    }

    pub fn trade_object_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> crate::error::Result<TradeObjectEventStream> {
        Ok(TradeObjectEventStream::new(
            self.commit_stream()?.filter_domain(ProtocolDomain::Trade),
            self.reader.clone(),
            account_id.as_ref().to_owned(),
        ))
    }

    pub fn trade_session_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> crate::error::Result<TradeSessionEventStream> {
        let receiver = self.driver.subscribe();
        self.driver.ensure_started()?;
        Ok(TradeSessionEventStream::new(
            receiver,
            self.reader.clone(),
            account_id.as_ref().to_owned(),
        ))
    }

    pub fn trade_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> crate::error::Result<TradeEventStream> {
        Ok(TradeEventStream::new(
            self.commit_stream()?.filter_domain(ProtocolDomain::Trade),
            self.reader.clone(),
            account_id.as_ref().to_owned(),
        ))
    }

    pub fn risk_management_rule_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> crate::error::Result<RiskManagementRuleEventStream> {
        Ok(RiskManagementRuleEventStream::new(
            self.commit_stream()?.filter_domain(ProtocolDomain::Trade),
            self.reader.clone(),
            account_id.as_ref().to_owned(),
        ))
    }

    pub fn risk_management_data_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> crate::error::Result<RiskManagementDataEventStream> {
        Ok(RiskManagementDataEventStream::new(
            self.commit_stream()?.filter_domain(ProtocolDomain::Trade),
            self.reader.clone(),
            account_id.as_ref().to_owned(),
        ))
    }

    pub fn settlement_info_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> crate::error::Result<SettlementInfoEventStream> {
        Ok(SettlementInfoEventStream::new(
            self.commit_stream()?.filter_domain(ProtocolDomain::Trade),
            self.reader.clone(),
            account_id.as_ref().to_owned(),
        ))
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

    pub fn security_position_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> crate::error::Result<SecurityPositionEventStream> {
        Ok(SecurityPositionEventStream::new(
            self.commit_stream()?.filter_domain(ProtocolDomain::Trade),
            self.reader.clone(),
            account_id.as_ref().to_owned(),
        ))
    }

    pub fn security_order_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> crate::error::Result<SecurityOrderEventStream> {
        Ok(SecurityOrderEventStream::new(
            self.commit_stream()?.filter_domain(ProtocolDomain::Trade),
            self.reader.clone(),
            account_id.as_ref().to_owned(),
        ))
    }

    pub fn security_trade_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> crate::error::Result<SecurityTradeEventStream> {
        Ok(SecurityTradeEventStream::new(
            self.commit_stream()?.filter_domain(ProtocolDomain::Trade),
            self.reader.clone(),
            account_id.as_ref().to_owned(),
        ))
    }

    #[must_use]
    pub fn into_session(mut self) -> tqsdk_session::SessionClient {
        self.path_dispatcher.abort();
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

    pub(crate) fn path_commit_stream(
        &self,
        paths: impl IntoIterator<Item = StatePath>,
    ) -> crate::error::Result<PathCommitStream> {
        self.path_dispatcher
            .subscribe(&self.driver, paths.into_iter().collect())
    }

    pub(crate) fn emit_driver_session_error(&self, error: tqsdk_session::SessionFacadeError) {
        let _ = self.driver.sender.send(DriverEvent::Error(error));
    }

    pub(crate) fn emit_driver_closed(&self) {
        let _ = self.driver.sender.send(DriverEvent::Closed);
    }

    pub(crate) fn close_driver_for_testing(&self) {
        self.path_dispatcher.abort();
        self.driver.abort();
    }

    pub(crate) async fn flush_outbound_for_shutdown(&self) -> crate::error::Result<bool> {
        self.session().flush_outbound().await.map_err(Into::into)
    }

    pub(crate) fn abort_driver_for_shutdown(&self) {
        self.path_dispatcher.abort();
        self.driver.abort();
    }

    pub(crate) fn driver_closed_for_shutdown(&self) -> bool {
        self.driver.is_closed()
    }
}

impl Drop for TqStream {
    fn drop(&mut self) {
        self.path_dispatcher.abort();
        self.driver.abort();
    }
}

/// Async stream of canonical runtime commits.
pub struct CommitStream {
    inner: BroadcastStream<DriverEvent>,
}

impl CommitStream {
    pub(crate) fn new(receiver: broadcast::Receiver<DriverEvent>) -> Self {
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
    type Item = crate::error::Result<tqsdk_core::SharedCommitResult>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(DriverEvent::Commit(commit)))) => Poll::Ready(Some(Ok(commit))),
            Poll::Ready(Some(Ok(DriverEvent::Error(error)))) => {
                Poll::Ready(Some(Err(crate::error::StreamFacadeError::Session(error))))
            }
            Poll::Ready(Some(Ok(DriverEvent::Lagged(skipped)))) => {
                Poll::Ready(Some(Err(crate::error::StreamFacadeError::Lagged {
                    skipped,
                })))
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

#[cfg(test)]
mod tests {
    use tqsdk_core::{AdapterRegistry, RuntimeHandle, StatePath};
    use tqsdk_session::testing::ManualSession;

    use super::TqStream;

    fn seeded_stream() -> TqStream {
        let mut adapters = AdapterRegistry::new();
        adapters.register_default_adapters();
        let handle = RuntimeHandle::with_adapters(adapters);
        let session = ManualSession::from_runtime(handle).into_client();
        TqStream::with_commit_channel_capacity(session, 16)
            .expect("test stream capacity should be valid")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn path_commit_streams_share_one_root_driver_receiver() {
        let stream = seeded_stream();

        let _first = stream
            .path_commit_stream([StatePath::new(["charts", "stream-kline-a"])])
            .expect("first path stream should open");
        let _second = stream
            .path_commit_stream([StatePath::new(["charts", "stream-kline-b"])])
            .expect("second path stream should open");

        assert_eq!(
            stream.driver.sender.receiver_count(),
            1,
            "path dispatcher should hold one root receiver for multiple path streams"
        );
    }
}
