use std::collections::BTreeMap;

use tqsdk_core::{ObjectKey, Quote, StatePath, Symbol};

use crate::change::ChangeTrackedRef;
use crate::step::{WaitReadHandle, WaitStep};

/// Lightweight handle to `quotes/{symbol}` in the runtime state tree.
#[derive(Clone)]
pub struct QuoteRef {
    reader: WaitReadHandle,
    symbol: Symbol,
}

/// Symbol-indexed collection returned by [`crate::TqApi::quotes`].
pub struct QuoteSet {
    quotes: BTreeMap<String, QuoteRef>,
}

impl QuoteSet {
    pub(crate) fn new(quotes: BTreeMap<String, QuoteRef>) -> Self {
        Self { quotes }
    }

    pub fn get(&self, symbol: &str) -> Option<&QuoteRef> {
        self.quotes.get(symbol)
    }

    pub fn iter(&self) -> impl Iterator<Item = &QuoteRef> {
        self.quotes.values()
    }

    pub fn symbols(&self) -> impl Iterator<Item = &str> {
        self.quotes.keys().map(String::as_str)
    }

    pub fn changed<'a>(&'a self, step: &'a WaitStep) -> impl Iterator<Item = &'a QuoteRef> + 'a {
        let mut changed = step
            .changed_quote_symbols()
            .filter_map(|symbol| self.quotes.get(symbol))
            .collect::<Vec<_>>();
        changed.sort_by(|left, right| left.symbol().cmp(right.symbol()));
        changed.into_iter()
    }

    pub fn changed_snapshots(&self, step: &WaitStep) -> crate::error::Result<Vec<Quote>> {
        let mut snapshots = Vec::new();
        for quote in self.changed(step) {
            if let Some(snapshot) = quote.snapshot()? {
                snapshots.push(snapshot);
            }
        }
        Ok(snapshots)
    }
}

impl QuoteRef {
    pub(crate) fn new(reader: WaitReadHandle, symbol: impl Into<String>) -> Self {
        Self {
            reader,
            symbol: Symbol::new(symbol.into()),
        }
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        self.symbol.as_str()
    }

    pub fn is_ready(&self) -> crate::error::Result<bool> {
        Ok(self.snapshot()?.is_some())
    }

    pub fn snapshot(&self) -> crate::error::Result<Option<Quote>> {
        self.reader
            .reader()
            .read_market_state()
            .quote(&self.symbol)
            .map_err(Into::into)
    }

    pub fn load(&self) -> crate::error::Result<Quote> {
        self.snapshot()?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "quote not ready",
            ))
    }

    pub fn changed_snapshot(&self, step: &WaitStep) -> crate::error::Result<Option<Quote>> {
        if step.is_changing(self) {
            self.snapshot()
        } else {
            Ok(None)
        }
    }
}

impl ChangeTrackedRef for QuoteRef {
    fn object_key(&self) -> Option<ObjectKey> {
        Some(ObjectKey::Quote {
            symbol: self.symbol.clone(),
        })
    }

    fn state_path(&self) -> StatePath {
        StatePath::new(["quotes", self.symbol.as_str()])
    }
}
