use tqsdk_core::{ObjectKey, Quote, StatePath, Symbol};

use crate::change::ChangeTrackedRef;
use crate::step::{WaitReadHandle, WaitStep};

/// Lightweight handle to `quotes/{symbol}` in the runtime state tree.
#[derive(Clone)]
pub struct QuoteRef {
    reader: WaitReadHandle,
    symbol: Symbol,
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
