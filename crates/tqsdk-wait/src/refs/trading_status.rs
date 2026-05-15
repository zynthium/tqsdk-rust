use tqsdk_core::{ObjectKey, StatePath, Symbol, TradingStatus};

use crate::{change::ChangeTrackedRef, step::WaitReadHandle};

/// Lightweight handle to `trading_status/{symbol}`.
#[derive(Clone)]
pub struct TradingStatusRef {
    reader: WaitReadHandle,
    symbol: Symbol,
}

impl TradingStatusRef {
    pub(crate) fn new(reader: WaitReadHandle, symbol: impl Into<String>) -> Self {
        Self {
            reader,
            symbol: Symbol::new(symbol.into()),
        }
    }

    pub fn load(&self) -> crate::error::Result<TradingStatus> {
        self.snapshot()?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "trading status not ready",
            ))
    }

    pub fn snapshot(&self) -> crate::error::Result<Option<TradingStatus>> {
        self.reader
            .reader()
            .read_market_state()
            .trading_status(&self.symbol)
            .map_err(Into::into)
    }

    pub fn is_ready(&self) -> crate::error::Result<bool> {
        Ok(self.snapshot()?.is_some())
    }
}

impl ChangeTrackedRef for TradingStatusRef {
    fn object_key(&self) -> Option<ObjectKey> {
        Some(ObjectKey::TradingStatus {
            symbol: self.symbol.clone(),
        })
    }

    fn state_path(&self) -> StatePath {
        StatePath::new(["trading_status", self.symbol.as_str()])
    }
}
