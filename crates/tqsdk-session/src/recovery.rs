#![cfg_attr(not(test), forbid(unsafe_code))]

use serde_json::Value;
use tqsdk_core::{AccountId, Revision, Symbol};

use crate::SessionClient;

/// User-level startup recovery readiness spec.
///
/// This type only describes which already-requested live objects must be ready.
/// It does not submit subscriptions or trade logins; consumption facades such as
/// `tqsdk-wait` and `tqsdk-stream` own those user-facing workflows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StartupRecoverySpec {
    quote_symbols: Vec<String>,
    trade_accounts: Vec<String>,
}

impl StartupRecoverySpec {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_quote_symbols<I, S>(mut self, symbols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.quote_symbols
            .extend(symbols.into_iter().map(|symbol| symbol.as_ref().to_owned()));
        self
    }

    #[must_use]
    pub fn with_trade_accounts<I, S>(mut self, account_ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.trade_accounts.extend(
            account_ids
                .into_iter()
                .map(|account_id| account_id.as_ref().to_owned()),
        );
        self
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.quote_symbols.is_empty() && self.trade_accounts.is_empty()
    }

    pub fn quote_symbols(&self) -> impl Iterator<Item = &str> {
        self.quote_symbols.iter().map(String::as_str)
    }

    pub fn trade_accounts(&self) -> impl Iterator<Item = &str> {
        self.trade_accounts.iter().map(String::as_str)
    }
}

/// Typed readiness status for startup recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StartupRecoveryStatus {
    pub revision: Revision,
    pub market_ready: bool,
    pub trade_ready: bool,
    pub missing_quotes: Vec<String>,
    pub pending_trade_accounts: Vec<String>,
}

impl StartupRecoveryStatus {
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.market_ready && self.trade_ready
    }
}

impl SessionClient {
    /// Reads startup recovery readiness from one revision-bound runtime
    /// snapshot.
    ///
    /// Trade accounts are considered ready only after the account object exists
    /// and the official `trade_more_data` marker is explicitly `false`.
    pub fn startup_recovery_status(
        &self,
        spec: &StartupRecoverySpec,
    ) -> crate::error::Result<StartupRecoveryStatus> {
        let snapshot = self.reader().read();
        let view = snapshot.view();
        let market = view.market_state();
        let trade = view.trade_state();

        let mut missing_quotes = Vec::new();
        for symbol in spec.quote_symbols() {
            if market.quote(&Symbol::new(symbol))?.is_none() {
                missing_quotes.push(symbol.to_owned());
            }
        }

        let mut pending_trade_accounts = Vec::new();
        for account_id in spec.trade_accounts() {
            let account = AccountId::new(account_id);
            let account_ready = trade.account(&account)?.is_some();
            let trade_more_data_ready = matches!(
                snapshot
                    .get_path(&["trade", account_id, "trade_more_data"])
                    .and_then(Value::as_bool),
                Some(false)
            );

            if !account_ready || !trade_more_data_ready {
                pending_trade_accounts.push(account_id.to_owned());
            }
        }

        Ok(StartupRecoveryStatus {
            revision: snapshot.revision(),
            market_ready: missing_quotes.is_empty(),
            trade_ready: pending_trade_accounts.is_empty(),
            missing_quotes,
            pending_trade_accounts,
        })
    }
}
