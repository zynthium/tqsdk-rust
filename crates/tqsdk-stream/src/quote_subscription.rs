#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeSet, HashSet, VecDeque};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tqsdk_core::{CommitResult, ObjectKey, Quote, RuntimeReader, SharedCommitResult, Symbol};

use crate::api::CommitStream;
use crate::typed::ValueUpdate;
use crate::{Result, StreamFacadeError};

/// Decoded quote update in a commit-level quote batch.
#[derive(Debug, Clone)]
pub struct QuoteUpdate {
    pub symbol: Symbol,
    pub value: Quote,
}

/// Decoded quote updates that became visible in one runtime commit.
#[derive(Debug, Clone)]
pub struct QuoteBatch {
    pub commit: SharedCommitResult,
    pub quotes: Vec<QuoteUpdate>,
}

/// User-owned dynamic batch quote subscription.
pub struct QuoteBatchSubscription {
    inner: CommitStream,
    session: tqsdk_session::SessionClient,
    reader: RuntimeReader,
    symbols: BTreeSet<Symbol>,
    leases: Vec<tqsdk_session::MarketQuoteLease>,
}

impl QuoteBatchSubscription {
    pub(crate) fn new(
        inner: CommitStream,
        session: tqsdk_session::SessionClient,
        reader: RuntimeReader,
        symbols: impl IntoIterator<Item = Symbol>,
        lease: tqsdk_session::MarketQuoteLease,
    ) -> Self {
        Self {
            inner,
            session,
            reader,
            symbols: symbols.into_iter().collect(),
            leases: vec![lease],
        }
    }

    pub fn symbols(&self) -> impl Iterator<Item = &str> {
        self.symbols.iter().map(Symbol::as_str)
    }

    #[must_use]
    pub fn contains(&self, symbol: &str) -> bool {
        self.symbols.contains(&Symbol::new(symbol))
    }

    pub async fn add(&mut self, symbol: impl AsRef<str>) -> Result<()> {
        let symbol = Symbol::new(symbol.as_ref());
        if !self.symbols.insert(symbol.clone()) {
            return Ok(());
        }

        let lease = self.session.ensure_quotes([symbol.as_str()]).await?;
        self.leases.push(lease);
        Ok(())
    }

    pub async fn remove(&mut self, symbol: impl AsRef<str>) -> Result<()> {
        let symbol = Symbol::new(symbol.as_ref());
        if !self.symbols.remove(&symbol) {
            return Ok(());
        }

        for lease in &mut self.leases {
            lease.release_symbols([symbol.as_str()]).await?;
        }
        Ok(())
    }

    pub async fn close(self) -> Result<()> {
        for lease in self.leases {
            lease.close().await?;
        }
        Ok(())
    }

    fn collect_batch(&mut self, commit: SharedCommitResult) -> Result<Option<QuoteBatch>> {
        let changed_symbols = changed_quote_symbols(&commit)
            .into_iter()
            .filter(|symbol| self.symbols.contains(symbol))
            .collect::<Vec<_>>();
        if changed_symbols.is_empty() {
            return Ok(None);
        }

        let market = self.reader.read_market_state();
        let mut quotes = Vec::new();
        for symbol in changed_symbols {
            if let Some(value) = market.quote(&symbol)? {
                quotes.push(QuoteUpdate { symbol, value });
            }
        }

        if quotes.is_empty() {
            Ok(None)
        } else {
            Ok(Some(QuoteBatch { commit, quotes }))
        }
    }
}

impl Stream for QuoteBatchSubscription {
    type Item = Result<QuoteBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(commit))) => match this.collect_batch(commit) {
                    Ok(Some(batch)) => return Poll::Ready(Some(Ok(batch))),
                    Ok(None) => {}
                    Err(error) => return Poll::Ready(Some(Err(error))),
                },
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// User-owned dynamic quote subscription.
pub struct QuoteSubscription {
    inner: QuoteBatchSubscription,
    pending: VecDeque<Result<ValueUpdate<Quote>>>,
}

impl QuoteSubscription {
    pub(crate) fn new(inner: QuoteBatchSubscription) -> Self {
        Self {
            inner,
            pending: VecDeque::new(),
        }
    }

    pub fn symbols(&self) -> impl Iterator<Item = &str> {
        self.inner.symbols()
    }

    #[must_use]
    pub fn contains(&self, symbol: &str) -> bool {
        self.inner.contains(symbol)
    }

    pub async fn add(&mut self, symbol: impl AsRef<str>) -> Result<()> {
        self.inner.add(symbol).await
    }

    pub async fn remove(&mut self, symbol: impl AsRef<str>) -> Result<()> {
        self.inner.remove(symbol).await
    }

    pub async fn close(self) -> Result<()> {
        self.inner.close().await
    }
}

impl Stream for QuoteSubscription {
    type Item = Result<ValueUpdate<Quote>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if let Some(update) = this.pending.pop_front() {
            return Poll::Ready(Some(update));
        }

        loop {
            match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    for update in batch.quotes {
                        this.pending.push_back(Ok(ValueUpdate {
                            commit: batch.commit.clone(),
                            value: update.value,
                        }));
                    }
                    if let Some(update) = this.pending.pop_front() {
                        return Poll::Ready(Some(update));
                    }
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pub(crate) async fn submit_subscribe(
    session: &tqsdk_session::SessionClient,
    symbols: impl IntoIterator<Item = Symbol>,
) -> Result<()> {
    session
        .submit(tqsdk_core::RuntimeCommand::Market(
            tqsdk_core::MarketCommand::SubscribeQuotes {
                symbols: symbols.into_iter().collect(),
            },
        ))
        .await?;
    Ok(())
}

pub(crate) async fn submit_unsubscribe(
    session: &tqsdk_session::SessionClient,
    symbols: impl IntoIterator<Item = Symbol>,
) -> Result<()> {
    session
        .submit(tqsdk_core::RuntimeCommand::Market(
            tqsdk_core::MarketCommand::UnsubscribeQuotes {
                symbols: symbols.into_iter().collect(),
            },
        ))
        .await?;
    Ok(())
}

pub(crate) fn validate_quote_symbols(symbols: &[Symbol]) -> Result<()> {
    if symbols.is_empty() {
        return Err(StreamFacadeError::InvalidState(
            "quote subscription requires at least one symbol",
        ));
    }
    Ok(())
}

pub(crate) fn changed_quote_symbols(commit: &CommitResult) -> Vec<Symbol> {
    let mut seen = HashSet::new();
    let mut symbols = Vec::new();

    for object in &commit.changes.object_hits {
        if let ObjectKey::Quote { symbol } = object {
            push_unique_symbol(&mut seen, &mut symbols, symbol.clone());
        }
    }

    for path in &commit.changes.path_hits {
        let segments = path.segments();
        if segments.len() >= 2 && segments[0] == "quotes" {
            push_unique_symbol(&mut seen, &mut symbols, Symbol::new(segments[1].clone()));
        }
    }

    symbols
}

fn push_unique_symbol(seen: &mut HashSet<Symbol>, symbols: &mut Vec<Symbol>, symbol: Symbol) {
    if seen.insert(symbol.clone()) {
        symbols.push(symbol);
    }
}

#[cfg(test)]
mod tests {
    use tqsdk_core::{ChangeSet, CommitResult, CommitScope, ObjectKey, Revision, StatePath};

    use super::changed_quote_symbols;

    fn commit_with_changes(changes: ChangeSet) -> CommitResult {
        CommitResult {
            revision: Revision::new(1),
            domains: Vec::new(),
            changes,
            caused_by: Vec::new(),
            scope: CommitScope::RealtimeUpdate,
        }
    }

    #[test]
    fn changed_quote_symbols_prefers_object_hits_and_deduplicates_path_fallback() {
        let commit = commit_with_changes(ChangeSet {
            object_hits: vec![ObjectKey::Quote {
                symbol: tqsdk_core::Symbol::new("SHFE.au2602"),
            }],
            path_hits: vec![
                StatePath::new(["quotes", "SHFE.au2602"]),
                StatePath::new(["quotes", "SHFE.ag2606"]),
                StatePath::new(["quotes", "SHFE.ag2606", "last_price"]),
            ],
            field_hits: Vec::new(),
        });

        let symbols = changed_quote_symbols(&commit)
            .into_iter()
            .map(|symbol| symbol.to_string())
            .collect::<Vec<_>>();

        assert_eq!(symbols, vec!["SHFE.au2602", "SHFE.ag2606"]);
    }

    #[test]
    fn changed_quote_symbols_ignores_unrelated_paths_and_objects() {
        let commit = commit_with_changes(ChangeSet {
            object_hits: vec![ObjectKey::TradingStatus {
                symbol: tqsdk_core::Symbol::new("SHFE.au2602"),
            }],
            path_hits: vec![
                StatePath::new(["trading_status", "SHFE.au2602"]),
                StatePath::new(["klines", "SHFE.au2602", "60000000000", "data"]),
            ],
            field_hits: Vec::new(),
        });

        assert!(changed_quote_symbols(&commit).is_empty());
    }
}
