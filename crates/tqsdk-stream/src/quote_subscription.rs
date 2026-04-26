#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeSet, VecDeque};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tqsdk_core::{CommitResult, Quote, RuntimeReader, Symbol};

use crate::api::CommitStream;
use crate::typed::ValueUpdate;
use crate::{Result, StreamFacadeError};

/// User-owned dynamic quote subscription.
pub struct QuoteSubscription {
    inner: CommitStream,
    session: tqsdk_session::SessionClient,
    reader: RuntimeReader,
    symbols: BTreeSet<Symbol>,
    pending: VecDeque<Result<ValueUpdate<Quote>>>,
}

impl QuoteSubscription {
    pub(crate) fn new(
        inner: CommitStream,
        session: tqsdk_session::SessionClient,
        reader: RuntimeReader,
        symbols: impl IntoIterator<Item = Symbol>,
    ) -> Self {
        Self {
            inner,
            session,
            reader,
            symbols: symbols.into_iter().collect(),
            pending: VecDeque::new(),
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

        submit_subscribe(&self.session, [symbol]).await
    }

    pub async fn remove(&mut self, symbol: impl AsRef<str>) -> Result<()> {
        let symbol = Symbol::new(symbol.as_ref());
        if !self.symbols.remove(&symbol) {
            return Ok(());
        }

        submit_unsubscribe(&self.session, [symbol]).await
    }

    pub async fn close(self) -> Result<()> {
        if self.symbols.is_empty() {
            return Ok(());
        }

        submit_unsubscribe(&self.session, self.symbols).await
    }

    fn collect_quotes(&mut self, commit: CommitResult) -> Result<()> {
        let hits = self
            .symbols
            .iter()
            .filter(|symbol| commit_touches_quote(&commit, symbol.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if hits.is_empty() {
            return Ok(());
        }

        let market = self.reader.read_market_state();
        for symbol in hits {
            if let Some(value) = market.quote(&symbol)? {
                self.pending.push_back(Ok(ValueUpdate {
                    commit: commit.clone(),
                    value,
                }));
            }
        }

        Ok(())
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
                Poll::Ready(Some(Ok(commit))) => match this.collect_quotes(commit) {
                    Ok(()) => {
                        if let Some(update) = this.pending.pop_front() {
                            return Poll::Ready(Some(update));
                        }
                    }
                    Err(error) => return Poll::Ready(Some(Err(error))),
                },
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

fn commit_touches_quote(commit: &CommitResult, symbol: &str) -> bool {
    commit.changes.path_hits.iter().any(|path| {
        let segments = path.segments();
        segments.len() >= 2 && segments[0] == "quotes" && segments[1] == symbol
    })
}
