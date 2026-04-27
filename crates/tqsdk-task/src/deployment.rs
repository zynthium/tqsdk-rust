#![cfg_attr(not(test), forbid(unsafe_code))]

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

#[cfg(feature = "live")]
use tqsdk_core::TradeAccountType;

use crate::{
    Result, RiskEngine, StrategyEnvironment, StrategyEnvironmentContext, StrategyEnvironmentKind,
    StrategyEnvironmentSubscriptions,
};

#[cfg(feature = "live")]
use crate::TaskHost;
#[cfg(feature = "live")]
use tqsdk_wait::TqApiBuilder;

/// Provider family used to construct a deployed strategy environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyEnvironmentProvider {
    TaskHost,
    Replay,
    LiveTrade,
    TqKqSim,
}

/// Market route selected for provider-backed strategy environments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyMarketMode {
    FuturesLive,
    StockLive,
    FuturesBacktest,
    StockBacktest,
}

/// Lifecycle policy for a deployed strategy run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StrategyLifecycle {
    max_steps: Option<usize>,
}

/// Deployment-level config for provider-backed strategy environments.
#[derive(Debug, Clone)]
pub struct StrategyDeploymentConfig {
    provider: StrategyDeploymentProviderConfig,
    market: StrategyMarketMode,
    subscriptions: StrategyEnvironmentSubscriptions,
    lifecycle: StrategyLifecycle,
    startup_timeout: Duration,
    risk: Option<RiskEngine>,
}

/// Builder that turns a config or existing environment into a deployment.
pub struct StrategyDeploymentBuilder {
    source: StrategyDeploymentSource,
    account_id: Option<String>,
    lifecycle: StrategyLifecycle,
}

/// Owned deployment wrapper with lifecycle and shutdown helpers.
pub struct StrategyDeployment {
    environment: StrategyEnvironment,
    provider: StrategyEnvironmentProvider,
    account_id: Option<String>,
    lifecycle: StrategyLifecycle,
}

/// Summary returned after a deployment run loop stops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrategyRunReport {
    steps: usize,
    stop_reason: StrategyRunStopReason,
}

/// Why a deployment run loop stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyRunStopReason {
    EnvironmentClosed,
    MaxSteps,
}

/// Summary returned by [`StrategyDeployment::shutdown`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrategyShutdownReport {
    kind: StrategyEnvironmentKind,
    graceful: bool,
}

pub type StrategyStepFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + 'a>>;

#[derive(Debug, Clone)]
enum StrategyDeploymentProviderConfig {
    #[cfg(feature = "live")]
    LiveTrade {
        auth_user: String,
        auth_pass: String,
        broker_id: String,
        account_id: String,
        password: String,
        account_type: TradeAccountType,
    },
    #[cfg(feature = "live")]
    TqKqSim {
        auth_user: String,
        auth_pass: String,
        account_number: Option<u8>,
        stock: bool,
    },
}

enum StrategyDeploymentSource {
    Environment {
        environment: Box<StrategyEnvironment>,
        provider: StrategyEnvironmentProvider,
    },
    #[cfg(feature = "live")]
    Config(Box<StrategyDeploymentConfig>),
}

impl StrategyLifecycle {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn max_steps(mut self, max_steps: usize) -> Self {
        self.max_steps = Some(max_steps);
        self
    }

    #[must_use]
    pub fn max_steps_opt(mut self, max_steps: Option<usize>) -> Self {
        self.max_steps = max_steps;
        self
    }

    #[must_use]
    pub fn without_step_limit(mut self) -> Self {
        self.max_steps = None;
        self
    }

    #[must_use]
    pub fn max_steps_limit(&self) -> Option<usize> {
        self.max_steps
    }
}

impl StrategyDeploymentConfig {
    #[cfg(feature = "live")]
    #[must_use]
    pub fn live_trade(
        auth_user: impl Into<String>,
        auth_pass: impl Into<String>,
        broker_id: impl Into<String>,
        account_id: impl Into<String>,
        password: impl Into<String>,
        account_type: TradeAccountType,
    ) -> Self {
        Self::new(StrategyDeploymentProviderConfig::LiveTrade {
            auth_user: auth_user.into(),
            auth_pass: auth_pass.into(),
            broker_id: broker_id.into(),
            account_id: account_id.into(),
            password: password.into(),
            account_type,
        })
    }

    #[cfg(feature = "live")]
    #[must_use]
    pub fn tqkq_sim(auth_user: impl Into<String>, auth_pass: impl Into<String>) -> Self {
        Self::new(StrategyDeploymentProviderConfig::TqKqSim {
            auth_user: auth_user.into(),
            auth_pass: auth_pass.into(),
            account_number: None,
            stock: false,
        })
    }

    #[cfg(feature = "live")]
    #[must_use]
    pub fn tqkq_stock_sim(auth_user: impl Into<String>, auth_pass: impl Into<String>) -> Self {
        Self::new(StrategyDeploymentProviderConfig::TqKqSim {
            auth_user: auth_user.into(),
            auth_pass: auth_pass.into(),
            account_number: None,
            stock: true,
        })
        .stock_market()
    }

    #[cfg(feature = "live")]
    fn new(provider: StrategyDeploymentProviderConfig) -> Self {
        Self {
            provider,
            market: StrategyMarketMode::FuturesLive,
            subscriptions: StrategyEnvironmentSubscriptions::new(),
            lifecycle: StrategyLifecycle::new(),
            startup_timeout: Duration::from_secs(30),
            risk: None,
        }
    }

    #[must_use]
    pub fn provider(&self) -> StrategyEnvironmentProvider {
        match self.provider {
            #[cfg(feature = "live")]
            StrategyDeploymentProviderConfig::LiveTrade { .. } => {
                StrategyEnvironmentProvider::LiveTrade
            }
            #[cfg(feature = "live")]
            StrategyDeploymentProviderConfig::TqKqSim { .. } => {
                StrategyEnvironmentProvider::TqKqSim
            }
        }
    }

    #[cfg(feature = "live")]
    #[must_use]
    pub fn account_number(mut self, number: u8) -> Self {
        if let StrategyDeploymentProviderConfig::TqKqSim { account_number, .. } = &mut self.provider
        {
            *account_number = Some(number);
        }
        self
    }

    #[must_use]
    pub fn futures_market(mut self) -> Self {
        self.market = StrategyMarketMode::FuturesLive;
        self
    }

    #[must_use]
    pub fn stock_market(mut self) -> Self {
        self.market = StrategyMarketMode::StockLive;
        self
    }

    #[must_use]
    pub fn futures_backtest_market(mut self) -> Self {
        self.market = StrategyMarketMode::FuturesBacktest;
        self
    }

    #[must_use]
    pub fn stock_backtest_market(mut self) -> Self {
        self.market = StrategyMarketMode::StockBacktest;
        self
    }

    #[must_use]
    pub fn quote(mut self, symbol: impl AsRef<str>) -> Self {
        self.subscriptions = self.subscriptions.quote(symbol);
        self
    }

    #[must_use]
    pub fn account(mut self, account_id: impl AsRef<str>) -> Self {
        self.subscriptions = self.subscriptions.account(account_id);
        self
    }

    #[must_use]
    pub fn kline(mut self, symbol: impl AsRef<str>, duration: Duration, view_width: usize) -> Self {
        self.subscriptions = self.subscriptions.kline(symbol, duration, view_width);
        self
    }

    #[must_use]
    pub fn tick(mut self, symbol: impl AsRef<str>, view_width: usize) -> Self {
        self.subscriptions = self.subscriptions.tick(symbol, view_width);
        self
    }

    #[must_use]
    pub fn lifecycle(mut self, lifecycle: StrategyLifecycle) -> Self {
        self.lifecycle = lifecycle;
        self
    }

    #[must_use]
    pub fn startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }

    #[must_use]
    pub fn risk(mut self, risk: RiskEngine) -> Self {
        self.risk = Some(risk);
        self
    }

    #[must_use]
    pub fn subscriptions(&self) -> &StrategyEnvironmentSubscriptions {
        &self.subscriptions
    }

    #[must_use]
    pub fn lifecycle_ref(&self) -> &StrategyLifecycle {
        &self.lifecycle
    }

    #[must_use]
    pub fn lifecycle_policy(&self) -> StrategyLifecycle {
        self.lifecycle
    }

    #[must_use]
    pub fn startup_timeout_value(&self) -> Duration {
        self.startup_timeout
    }

    #[must_use]
    pub fn risk_engine(&self) -> Option<&RiskEngine> {
        self.risk.as_ref()
    }
}

impl StrategyDeployment {
    #[must_use]
    pub fn from_environment(environment: StrategyEnvironment) -> StrategyDeploymentBuilder {
        let provider = match environment.kind() {
            StrategyEnvironmentKind::TaskHost => StrategyEnvironmentProvider::TaskHost,
            StrategyEnvironmentKind::Replay => StrategyEnvironmentProvider::Replay,
        };
        StrategyDeploymentBuilder::new(StrategyDeploymentSource::Environment {
            environment: Box::new(environment),
            provider,
        })
    }

    #[must_use]
    pub fn environment(&self) -> &StrategyEnvironment {
        &self.environment
    }

    #[must_use]
    pub fn environment_mut(&mut self) -> &mut StrategyEnvironment {
        &mut self.environment
    }

    #[must_use]
    pub fn provider(&self) -> StrategyEnvironmentProvider {
        self.provider
    }

    #[must_use]
    pub fn kind(&self) -> StrategyEnvironmentKind {
        self.environment.kind()
    }

    #[must_use]
    pub fn account_id(&self) -> Option<&str> {
        self.account_id.as_deref()
    }

    #[must_use]
    pub fn lifecycle(&self) -> StrategyLifecycle {
        self.lifecycle
    }

    pub async fn run<F>(&mut self, mut step: F) -> Result<StrategyRunReport>
    where
        F: for<'a> FnMut(&'a mut StrategyEnvironmentContext<'a>) -> StrategyStepFuture<'a>,
    {
        let mut steps = 0;
        loop {
            if self
                .lifecycle
                .max_steps
                .is_some_and(|max_steps| steps >= max_steps)
            {
                return Ok(StrategyRunReport {
                    steps,
                    stop_reason: StrategyRunStopReason::MaxSteps,
                });
            }

            let Some(mut context) = self.environment.next().await? else {
                return Ok(StrategyRunReport {
                    steps,
                    stop_reason: StrategyRunStopReason::EnvironmentClosed,
                });
            };
            step(&mut context).await?;
            steps += 1;
        }
    }

    pub async fn shutdown(self) -> Result<StrategyShutdownReport> {
        Ok(StrategyShutdownReport {
            kind: self.environment.kind(),
            graceful: true,
        })
    }
}

impl StrategyDeploymentBuilder {
    fn new(source: StrategyDeploymentSource) -> Self {
        Self {
            source,
            account_id: None,
            lifecycle: StrategyLifecycle::new(),
        }
    }

    #[must_use]
    pub fn account_id(mut self, account_id: impl Into<String>) -> Self {
        self.account_id = Some(account_id.into());
        self
    }

    #[must_use]
    pub fn lifecycle(mut self, lifecycle: StrategyLifecycle) -> Self {
        self.lifecycle = lifecycle;
        self
    }

    pub async fn build(self) -> Result<StrategyDeployment> {
        match self.source {
            StrategyDeploymentSource::Environment {
                environment,
                provider,
            } => Ok(StrategyDeployment {
                environment: *environment,
                provider,
                account_id: self.account_id,
                lifecycle: self.lifecycle,
            }),
            #[cfg(feature = "live")]
            StrategyDeploymentSource::Config(config) => build_config_deployment(*config).await,
        }
    }
}

impl StrategyRunReport {
    #[must_use]
    pub fn steps(&self) -> usize {
        self.steps
    }

    #[must_use]
    pub fn stop_reason(&self) -> StrategyRunStopReason {
        self.stop_reason
    }
}

impl StrategyShutdownReport {
    #[must_use]
    pub fn kind(&self) -> StrategyEnvironmentKind {
        self.kind
    }

    #[must_use]
    pub fn graceful(&self) -> bool {
        self.graceful
    }
}

impl StrategyEnvironment {
    #[cfg(feature = "live")]
    #[must_use]
    pub fn from_config(config: StrategyDeploymentConfig) -> StrategyDeploymentBuilder {
        StrategyDeploymentBuilder::new(StrategyDeploymentSource::Config(Box::new(config)))
    }
}

#[cfg(feature = "live")]
async fn build_config_deployment(config: StrategyDeploymentConfig) -> Result<StrategyDeployment> {
    let StrategyDeploymentConfig {
        provider,
        market,
        subscriptions,
        lifecycle,
        startup_timeout,
        risk,
    } = config;

    let deadline = tokio::time::Instant::now() + startup_timeout;
    match provider {
        StrategyDeploymentProviderConfig::LiveTrade {
            auth_user,
            auth_pass,
            broker_id,
            account_id,
            password,
            account_type,
        } => {
            let mut api = apply_market_mode(TqApiBuilder::new(auth_user, auth_pass), market)
                .trade_target(broker_id.clone(), account_id.clone())
                .build()
                .await?;
            api.login_trade_account(
                &broker_id,
                &account_id,
                &password,
                account_type,
                Some(deadline),
            )
            .await?;
            let environment =
                build_task_environment(api, risk, subscriptions.account(account_id.clone()))
                    .await?;
            Ok(StrategyDeployment {
                environment,
                provider: StrategyEnvironmentProvider::LiveTrade,
                account_id: Some(account_id),
                lifecycle,
            })
        }
        StrategyDeploymentProviderConfig::TqKqSim {
            auth_user,
            auth_pass,
            account_number,
            stock,
        } => {
            let mut builder = apply_market_mode(TqApiBuilder::new(auth_user, auth_pass), market);
            builder = match (stock, account_number) {
                (false, Some(number)) => builder.trade_target_tqkq_numbered(number),
                (false, None) => builder.trade_target_tqkq(),
                (true, Some(number)) => builder.trade_target_tqkq_stock_numbered(number),
                (true, None) => builder.trade_target_tqkq_stock(),
            };
            let mut api = builder.build().await?;
            let login = match (stock, account_number) {
                (false, Some(number)) => api.session().tqkq_login_command_numbered(number).await?,
                (false, None) => api.session().tqkq_login_command().await?,
                (true, Some(number)) => {
                    api.session()
                        .tqkq_stock_login_command_numbered(number)
                        .await?
                }
                (true, None) => api.session().tqkq_stock_login_command().await?,
            };
            let broker_id = login.broker_id.clone();
            let account_id = login.account_id.as_str().to_owned();
            let password = login.password.clone();
            let account_type = login.account_type;
            api.login_trade_account(
                &broker_id,
                &account_id,
                &password,
                account_type,
                Some(deadline),
            )
            .await?;
            let environment =
                build_task_environment(api, risk, subscriptions.account(account_id.clone()))
                    .await?;
            Ok(StrategyDeployment {
                environment,
                provider: StrategyEnvironmentProvider::TqKqSim,
                account_id: Some(account_id),
                lifecycle,
            })
        }
    }
}

#[cfg(feature = "live")]
fn apply_market_mode(builder: TqApiBuilder, market: StrategyMarketMode) -> TqApiBuilder {
    match market {
        StrategyMarketMode::FuturesLive => builder.futures_market(),
        StrategyMarketMode::StockLive => builder.stock_market(),
        StrategyMarketMode::FuturesBacktest => builder.futures_backtest_market(),
        StrategyMarketMode::StockBacktest => builder.stock_backtest_market(),
    }
}

#[cfg(feature = "live")]
async fn build_task_environment(
    api: tqsdk_wait::TqApi,
    risk: Option<RiskEngine>,
    subscriptions: StrategyEnvironmentSubscriptions,
) -> Result<StrategyEnvironment> {
    let mut host = TaskHost::new(api);
    if let Some(risk) = risk {
        host.set_risk(risk);
    }
    StrategyEnvironment::from_task_host(host)
        .subscriptions(subscriptions)
        .build()
        .await
}
