use std::error::Error;

use tqsdk_core::{
    AuthProvider, ContractError, EndpointConfig, PasswordCredentials, TqAuthProvider,
    TqKqAccountConfig, TradeAccountType, TradeLoginCommand,
};

#[derive(Debug, Clone)]
pub struct LiveTradeLogin {
    broker_id: String,
    account_id: String,
    password: String,
    account_type: TradeAccountType,
}

impl LiveTradeLogin {
    pub fn broker_id(&self) -> &str {
        &self.broker_id
    }

    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    pub fn login_command(&self) -> TradeLoginCommand {
        TradeLoginCommand {
            account_id: tqsdk_core::AccountId::new(self.account_id.clone()),
            broker_id: self.broker_id.clone(),
            password: self.password.clone(),
            account_type: self.account_type,
            front_broker: None,
            front_url: None,
            client_app_id: None,
            client_system_info: None,
        }
    }

    fn from_tqkq(config: TqKqAccountConfig) -> Self {
        Self {
            broker_id: config.broker_id().to_string(),
            account_id: config.account_id().as_str().to_string(),
            password: config.password().to_string(),
            account_type: config.account_type(),
        }
    }
}

pub async fn resolve_live_trade_login(
    auth_user: &str,
    auth_pass: &str,
) -> Result<LiveTradeLogin, Box<dyn Error>> {
    match (
        read_env("TQ_TRADE_BROKER_ID"),
        read_env("TQ_TRADE_ACCOUNT_ID"),
        read_env("TQ_TRADE_PASSWORD"),
    ) {
        (Some(broker_id), Some(account_id), Some(password)) => Ok(LiveTradeLogin {
            broker_id,
            account_id,
            password,
            account_type: TradeAccountType::Future,
        }),
        (None, None, None) => resolve_tqkq_login(auth_user, auth_pass).await,
        _ => Err(ContractError::validation(
            "TQ_TRADE_BROKER_ID/TQ_TRADE_ACCOUNT_ID/TQ_TRADE_PASSWORD must be set together",
        )
        .into()),
    }
}

fn read_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

async fn resolve_tqkq_login(
    auth_user: &str,
    auth_pass: &str,
) -> Result<LiveTradeLogin, Box<dyn Error>> {
    let endpoints = EndpointConfig::from_env();
    let mut provider = TqAuthProvider::new(PasswordCredentials::new(
        auth_user.to_string(),
        auth_pass.to_string(),
    ));
    if let Some(auth_url) = endpoints.auth_url {
        provider = provider.with_auth_url(auth_url);
    }

    let auth = provider.authenticate().await?;
    let auth_id = auth
        .auth_id()
        .ok_or_else(|| ContractError::auth("auth response missing auth_id"))?;
    let account_number = read_u8_env("TQ_TRADE_ACCOUNT_NO")?;
    let config = if let Some(number) = account_number {
        TqKqAccountConfig::future_numbered(auth_id.as_str(), number)?
    } else {
        TqKqAccountConfig::future(auth_id.as_str())
    };

    Ok(LiveTradeLogin::from_tqkq(config))
}

fn read_u8_env(name: &str) -> Result<Option<u8>, Box<dyn Error>> {
    let Some(raw) = read_env(name) else {
        return Ok(None);
    };
    Ok(Some(raw.parse()?))
}
