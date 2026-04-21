use tqsdk_core::{ObjectKey, Quote, StatePath, Symbol};

use crate::api::TqApi;
use crate::change::ChangeTrackedRef;

#[derive(Debug, Clone)]
pub struct QuoteRef {
    symbol: Symbol,
}

impl QuoteRef {
    #[must_use]
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: Symbol::new(symbol.into()),
        }
    }

    pub fn is_ready(&self, api: &TqApi) -> crate::error::Result<bool> {
        Ok(self.snapshot(api)?.is_some())
    }

    pub fn snapshot(&self, api: &TqApi) -> crate::error::Result<Option<Quote>> {
        let guard = api.driver.reader.read();
        guard
            .decode_path::<Quote>(&["quotes", self.symbol.as_str()])
            .map_err(Into::into)
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<Quote> {
        self.snapshot(api)?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "quote not ready",
            ))
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
