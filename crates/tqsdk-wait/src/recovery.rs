#![cfg_attr(not(test), forbid(unsafe_code))]

use std::future::{Future, IntoFuture};
use std::pin::Pin;

use tqsdk_session::{StartupRecoverySpec, StartupRecoveryStatus};

use crate::TqApi;

/// Builder for wait-facade startup recovery barriers.
pub struct WaitStartupRecovery<'a> {
    api: &'a mut TqApi,
    spec: StartupRecoverySpec,
    deadline: Option<tokio::time::Instant>,
}

impl<'a> WaitStartupRecovery<'a> {
    pub(crate) fn new(api: &'a mut TqApi) -> Self {
        Self {
            api,
            spec: StartupRecoverySpec::new(),
            deadline: None,
        }
    }

    #[must_use]
    pub fn quotes<I, S>(mut self, symbols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.spec = self.spec.with_quote_symbols(symbols);
        self
    }

    #[must_use]
    pub fn trade_account(mut self, account_id: impl AsRef<str>) -> Self {
        self.spec = self.spec.with_trade_accounts([account_id.as_ref()]);
        self
    }

    #[must_use]
    pub fn deadline(mut self, deadline: tokio::time::Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    pub async fn wait(self) -> crate::error::Result<StartupRecoveryStatus> {
        wait_startup_recovery(self.api, self.spec, self.deadline).await
    }
}

impl<'a> IntoFuture for WaitStartupRecovery<'a> {
    type Output = crate::error::Result<StartupRecoveryStatus>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.wait())
    }
}

async fn wait_startup_recovery(
    api: &mut TqApi,
    spec: StartupRecoverySpec,
    deadline: Option<tokio::time::Instant>,
) -> crate::error::Result<StartupRecoveryStatus> {
    for symbol in spec.quote_symbols() {
        api.get_quote(symbol).await?;
    }

    loop {
        let status = api
            .session()
            .startup_recovery_status(&spec)
            .map_err(crate::error::WaitFacadeError::Session)?;
        if status.is_ready() {
            return Ok(status);
        }

        if !api.wait_update(deadline).await? {
            return Err(crate::error::WaitFacadeError::InvalidState(
                "startup recovery not ready",
            ));
        }
    }
}
