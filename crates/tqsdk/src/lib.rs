#![cfg_attr(not(test), forbid(unsafe_code))]
//! User-facing facade crate for `tqsdk-rust`.
//!
//! This crate gives ordinary users one dependency and one prelude while keeping
//! the underlying `core` / `session` / `wait` / `stream` / `task` / `data`
//! boundaries available under [`advanced`].

use std::env;
use std::fmt;

/// Common imports for strategy-oriented users.
pub mod prelude {
    pub use crate::{Error, Result, TargetPos, Tq, TqBuilder};
    pub use tqsdk_wait::{AccountRef, PositionRef, QuoteRef, QuoteSet, WaitStep};
}

/// Explicit access to the underlying crates for advanced users.
pub mod advanced {
    pub mod core {
        pub use tqsdk_core::{TradeAccountType, TradeDirection, TradeOffset};
    }

    pub mod data {
        pub type DataClient = tqsdk_data::DataClient;
        pub type DataError = tqsdk_data::DataError;
    }

    pub mod runtime {
        pub use tqsdk_core::{CommitResult, RuntimeHandle, RuntimeReader, UpdateCursor};
    }

    pub mod session {
        pub type SessionClient = tqsdk_session::SessionClient;
        pub type SessionClientBuilder = tqsdk_session::SessionClientBuilder;
        pub type SessionFacadeError = tqsdk_session::SessionFacadeError;
    }

    pub mod stream {
        pub type TqStream = tqsdk_stream::TqStream;
        pub type TqStreamBuilder = tqsdk_stream::TqStreamBuilder;
    }

    pub mod task {
        pub use tqsdk_task::{
            OffsetPriority, PriceMode, TargetPosConfig, TargetPosTask,
            TargetPosTaskExecutionReport, TaskError, TaskHost, VolumeSplitPolicy,
        };
    }

    pub mod wait {
        pub use tqsdk_wait::{
            AccountRef, KlineHandle, KlineWindow, OrderPrice, OrderRef, OrderTicket,
            OrderTicketState, PositionRef, QuoteRef, QuoteSet, TickHandle, TickWindow, TqApi,
            TqApiBuilder, TradingStatusRef, WaitFacadeError, WaitStep,
        };
    }
}

/// Result type for the user-facing facade.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for the user-facing facade.
#[derive(Debug)]
pub enum Error {
    MissingAuth,
    MissingAuthEnv {
        name: &'static str,
        source: env::VarError,
    },
    EmptyAuthEnv {
        name: &'static str,
    },
    Session(Box<tqsdk_session::SessionFacadeError>),
    Wait(Box<tqsdk_wait::WaitFacadeError>),
    Task(Box<tqsdk_task::TaskError>),
    Data(Box<tqsdk_data::DataError>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingAuth => write!(
                f,
                "missing auth credentials; call auth(user, pass) or auth_env() before connect()"
            ),
            Self::MissingAuthEnv { name, .. } => {
                write!(f, "missing required environment variable {name}")
            }
            Self::EmptyAuthEnv { name } => {
                write!(f, "environment variable {name} must not be empty")
            }
            Self::Session(error) => write!(f, "{error}"),
            Self::Wait(error) => write!(f, "{error}"),
            Self::Task(error) => write!(f, "{error}"),
            Self::Data(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MissingAuth => None,
            Self::MissingAuthEnv { source, .. } => Some(source),
            Self::EmptyAuthEnv { .. } => None,
            Self::Session(error) => Some(&**error),
            Self::Wait(error) => Some(&**error),
            Self::Task(error) => Some(&**error),
            Self::Data(error) => Some(&**error),
        }
    }
}

impl From<tqsdk_session::SessionFacadeError> for Error {
    fn from(error: tqsdk_session::SessionFacadeError) -> Self {
        Self::Session(Box::new(error))
    }
}

impl From<tqsdk_wait::WaitFacadeError> for Error {
    fn from(error: tqsdk_wait::WaitFacadeError) -> Self {
        Self::Wait(Box::new(error))
    }
}

impl From<tqsdk_task::TaskError> for Error {
    fn from(error: tqsdk_task::TaskError) -> Self {
        Self::Task(Box::new(error))
    }
}

impl From<tqsdk_data::DataError> for Error {
    fn from(error: tqsdk_data::DataError) -> Self {
        Self::Data(Box::new(error))
    }
}

/// Strategy-oriented entrypoint.
///
/// `Tq` owns a wait-style API plus task host so common strategy loops can use
/// one object for `next()`, live refs, history access, and target-position tasks.
pub struct Tq {
    host: tqsdk_task::TaskHost,
}

impl Tq {
    #[must_use]
    pub fn futures() -> TqBuilder {
        TqBuilder::futures()
    }

    #[must_use]
    pub fn from_api(api: tqsdk_wait::TqApi) -> Self {
        Self {
            host: tqsdk_task::TaskHost::new(api),
        }
    }

    #[must_use]
    pub fn api(&self) -> &tqsdk_wait::TqApi {
        self.host.api()
    }

    #[must_use]
    pub fn api_mut(&mut self) -> &mut tqsdk_wait::TqApi {
        self.host.api_mut()
    }

    #[must_use]
    pub fn task_host(&self) -> &tqsdk_task::TaskHost {
        &self.host
    }

    #[must_use]
    pub fn task_host_mut(&mut self) -> &mut tqsdk_task::TaskHost {
        &mut self.host
    }

    #[must_use]
    pub fn session(&self) -> &tqsdk_session::SessionClient {
        self.api().session()
    }

    #[must_use]
    pub fn history(&self) -> tqsdk_data::DataClient {
        tqsdk_data::DataClient::from_session(self.session().clone())
    }

    #[cfg(feature = "live")]
    pub async fn tqkq_account_id(&self) -> Result<String> {
        let login = self.session().tqkq_login_command().await?;
        Ok(login.account_id.as_str().to_owned())
    }

    #[cfg(feature = "live")]
    pub async fn tqkq_account_id_numbered(&self, number: u8) -> Result<String> {
        let login = self.session().tqkq_login_command_numbered(number).await?;
        Ok(login.account_id.as_str().to_owned())
    }

    #[cfg(feature = "live")]
    pub async fn target_pos_tqkq(&mut self, symbol: &str) -> Result<TargetPos> {
        let account_id = self.tqkq_account_id().await?;
        self.target_pos(&account_id, symbol)
    }

    #[cfg(feature = "live")]
    pub async fn target_pos_tqkq_numbered(
        &mut self,
        number: u8,
        symbol: &str,
    ) -> Result<TargetPos> {
        let account_id = self.tqkq_account_id_numbered(number).await?;
        self.target_pos(&account_id, symbol)
    }

    pub async fn next(&mut self) -> Result<bool> {
        self.host.wait_update(None).await.map_err(Error::from)
    }

    pub async fn wait_update(&mut self, deadline: Option<tokio::time::Instant>) -> Result<bool> {
        self.host.wait_update(deadline).await.map_err(Error::from)
    }

    pub async fn quote(&mut self, symbol: &str) -> Result<tqsdk_wait::QuoteRef> {
        self.api_mut().quote(symbol).await.map_err(Error::from)
    }

    pub async fn quotes<I, S>(&mut self, symbols: I) -> Result<tqsdk_wait::QuoteSet>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.api_mut().quotes(symbols).await.map_err(Error::from)
    }

    #[must_use]
    pub fn account(&self, account_id: &str) -> tqsdk_wait::AccountRef {
        self.api().account(account_id)
    }

    #[must_use]
    pub fn position(&self, account_id: &str, symbol: &str) -> tqsdk_wait::PositionRef {
        self.api().position(account_id, symbol)
    }

    pub fn target_pos(&mut self, account_id: &str, symbol: &str) -> Result<TargetPos> {
        let task = self.host.target_pos(account_id, symbol).build()?;
        Ok(TargetPos::new(task))
    }
}

/// Builder for [`Tq`].
#[derive(Debug, Clone)]
pub struct TqBuilder {
    auth: Option<Auth>,
    query_enabled: bool,
    trade_targets: Vec<TradeTarget>,
}

impl TqBuilder {
    #[must_use]
    pub fn futures() -> Self {
        Self {
            auth: None,
            query_enabled: false,
            trade_targets: Vec::new(),
        }
    }

    #[must_use]
    pub fn auth(mut self, user: impl Into<String>, pass: impl Into<String>) -> Self {
        self.auth = Some(Auth {
            user: user.into(),
            pass: pass.into(),
        });
        self
    }

    pub fn auth_env(self) -> Result<Self> {
        let user = read_env("TQ_AUTH_USER")?;
        let pass = read_env("TQ_AUTH_PASS")?;
        Ok(self.auth(user, pass))
    }

    #[must_use]
    pub fn enable_query(mut self) -> Self {
        self.query_enabled = true;
        self
    }

    #[must_use]
    pub fn trade_target(
        mut self,
        broker_id: impl Into<String>,
        account_id: impl Into<String>,
    ) -> Self {
        self.trade_targets.push(TradeTarget::Custom {
            broker_id: broker_id.into(),
            account_id: account_id.into(),
            trade_url: None,
        });
        self
    }

    #[must_use]
    pub fn trade_target_with_url(
        mut self,
        broker_id: impl Into<String>,
        account_id: impl Into<String>,
        trade_url: impl Into<String>,
    ) -> Self {
        self.trade_targets.push(TradeTarget::Custom {
            broker_id: broker_id.into(),
            account_id: account_id.into(),
            trade_url: Some(trade_url.into()),
        });
        self
    }

    #[must_use]
    pub fn trade_target_tqkq(mut self) -> Self {
        self.trade_targets.push(TradeTarget::TqKq);
        self
    }

    #[must_use]
    pub fn trade_target_tqkq_numbered(mut self, number: u8) -> Self {
        self.trade_targets.push(TradeTarget::TqKqNumbered(number));
        self
    }

    pub async fn connect(self) -> Result<Tq> {
        let auth = self.auth.ok_or(Error::MissingAuth)?;
        let mut builder =
            tqsdk_session::SessionClientBuilder::new(auth.user, auth.pass).futures_market();
        if self.query_enabled {
            builder = builder.enable_query();
        }
        for target in self.trade_targets {
            builder = target.apply(builder);
        }
        let api = tqsdk_wait::TqApiBuilder::from_session_builder(builder)
            .build()
            .await?;
        Ok(Tq::from_api(api))
    }
}

/// Thin ergonomic wrapper around [`tqsdk_task::TargetPosTask`].
#[derive(Clone)]
pub struct TargetPos {
    inner: tqsdk_task::TargetPosTask,
}

impl TargetPos {
    #[must_use]
    pub fn new(inner: tqsdk_task::TargetPosTask) -> Self {
        Self { inner }
    }

    #[must_use]
    pub fn inner(&self) -> &tqsdk_task::TargetPosTask {
        &self.inner
    }

    #[must_use]
    pub fn into_inner(self) -> tqsdk_task::TargetPosTask {
        self.inner
    }

    pub async fn set(&mut self, volume: i64) -> Result<()> {
        self.inner.set_target_volume(volume).map_err(Error::from)
    }

    pub async fn close(&mut self) -> Result<()> {
        self.set(0).await
    }

    pub async fn wait_target_reached(&self) -> Result<()> {
        self.inner.wait_target_reached().await.map_err(Error::from)
    }
}

#[derive(Debug, Clone)]
struct Auth {
    user: String,
    pass: String,
}

#[derive(Debug, Clone)]
enum TradeTarget {
    Custom {
        broker_id: String,
        account_id: String,
        trade_url: Option<String>,
    },
    TqKq,
    TqKqNumbered(u8),
}

impl TradeTarget {
    fn apply(
        self,
        builder: tqsdk_session::SessionClientBuilder,
    ) -> tqsdk_session::SessionClientBuilder {
        match self {
            Self::Custom {
                broker_id,
                account_id,
                trade_url: Some(trade_url),
            } => builder.trade_target_with_url(broker_id, account_id, trade_url),
            Self::Custom {
                broker_id,
                account_id,
                trade_url: None,
            } => builder.trade_target(broker_id, account_id),
            Self::TqKq => builder.trade_target_tqkq(),
            Self::TqKqNumbered(number) => builder.trade_target_tqkq_numbered(number),
        }
    }
}

fn read_env(name: &'static str) -> Result<String> {
    env::var(name).map_err(|source| Error::MissingAuthEnv { name, source })
}
