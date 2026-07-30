#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk_core::{MarketSessionTarget, OutboundDispatch, RuntimeHandle};

use crate::{Result, SessionClient};

/// Explicit no-IO session fixture for deterministic tests and examples.
///
/// `ManualSession` owns a [`SessionClient`] whose runtime is driven manually by
/// the caller. It is intended for fixture code that needs to assert outbound
/// commands without starting live transport IO.
pub struct ManualSession {
    client: SessionClient,
}

impl ManualSession {
    /// Build a manual session around an existing runtime handle.
    #[must_use]
    pub fn from_runtime(handle: RuntimeHandle) -> Self {
        Self {
            client: SessionClient::new_manual_with_handle(handle),
        }
    }

    /// Build a manual session with an explicit market target.
    #[must_use]
    pub fn from_runtime_with_market_target(
        handle: RuntimeHandle,
        market_target: MarketSessionTarget,
    ) -> Self {
        Self {
            client: SessionClient::new_manual_with_handle_and_market_target(handle, market_target),
        }
    }

    /// Borrow the underlying session client.
    #[must_use]
    pub fn client(&self) -> &SessionClient {
        &self.client
    }

    /// Clone the underlying session client.
    #[must_use]
    pub fn client_clone(&self) -> SessionClient {
        self.client.clone()
    }

    /// Consume the fixture and return the underlying session client.
    #[must_use]
    pub fn into_client(self) -> SessionClient {
        self.client
    }

    /// Drain outbound requests produced by the manual session runtime.
    pub fn drain_dispatches(&self) -> Result<Vec<OutboundDispatch>> {
        self.client.drain_manual_dispatches()
    }
}
