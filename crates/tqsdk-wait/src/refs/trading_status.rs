use tqsdk_core::{ObjectKey, StatePath, Symbol, TradingStatus};

use crate::{api::TqApi, change::ChangeTrackedRef};

/// Lightweight handle to `trading_status/{symbol}`.
#[derive(Debug, Clone)]
pub struct TradingStatusRef {
    symbol: Symbol,
}

impl TradingStatusRef {
    #[must_use]
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: Symbol::new(symbol.into()),
        }
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<TradingStatus> {
        api.driver
            .reader
            .read()
            .decode_path::<TradingStatus>(&["trading_status", self.symbol.as_str()])?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "trading status not ready",
            ))
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
