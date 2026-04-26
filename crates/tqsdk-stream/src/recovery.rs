#![cfg_attr(not(test), forbid(unsafe_code))]

use std::future::{Future, IntoFuture};
use std::pin::Pin;

use futures::StreamExt;
use tqsdk_core::Symbol;
use tqsdk_session::{StartupRecoverySpec, StartupRecoveryStatus};

use crate::quote_subscription::submit_subscribe;
use crate::{StreamFacadeError, TqStream};

/// Builder for stream-facade startup recovery barriers.
pub struct StreamStartupRecovery<'a> {
    stream: &'a TqStream,
    spec: StartupRecoverySpec,
    deadline: Option<tokio::time::Instant>,
}

impl<'a> StreamStartupRecovery<'a> {
    pub(crate) fn new(stream: &'a TqStream) -> Self {
        Self {
            stream,
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
        wait_startup_recovery(self.stream, self.spec, self.deadline).await
    }
}

impl<'a> IntoFuture for StreamStartupRecovery<'a> {
    type Output = crate::error::Result<StartupRecoveryStatus>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.wait())
    }
}

async fn wait_startup_recovery(
    stream: &TqStream,
    spec: StartupRecoverySpec,
    deadline: Option<tokio::time::Instant>,
) -> crate::error::Result<StartupRecoveryStatus> {
    let mut commits = stream.commit_stream()?;
    let quote_symbols = spec
        .quote_symbols()
        .map(Symbol::new)
        .collect::<Vec<Symbol>>();
    if !quote_symbols.is_empty() {
        submit_subscribe(stream.session(), quote_symbols).await?;
    }

    loop {
        let status = stream.session().startup_recovery_status(&spec)?;
        if status.is_ready() {
            return Ok(status);
        }

        match next_commit(&mut commits, deadline).await? {
            Some(()) => continue,
            None => return Err(StreamFacadeError::Closed),
        }
    }
}

async fn next_commit(
    commits: &mut crate::CommitStream,
    deadline: Option<tokio::time::Instant>,
) -> crate::error::Result<Option<()>> {
    let next = if let Some(deadline) = deadline {
        tokio::time::timeout_at(deadline, commits.next())
            .await
            .map_err(|_| StreamFacadeError::InvalidState("startup recovery not ready"))?
    } else {
        commits.next().await
    };

    match next.transpose()? {
        Some(_) => Ok(Some(())),
        None => Ok(None),
    }
}
