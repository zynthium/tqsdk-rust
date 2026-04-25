#[cfg(feature = "tq-auth")]
use serde_json::Value;
use tqsdk_core::AuthContext;
#[cfg(feature = "tq-auth")]
use tqsdk_core::TradeLoginCommand;

use super::SessionClient;
#[cfg(feature = "tq-auth")]
use crate::tqkq::TqKqAccountConfig;

const LIMITED_INDEX_SYMBOLS: &[&str] = &["SSE.000016", "SSE.000300", "SSE.000905", "SSE.000852"];

impl SessionClient {
    #[cfg(feature = "tq-auth")]
    pub async fn tqkq_login_command(&self) -> crate::error::Result<TradeLoginCommand> {
        self.tqkq_login_command_with_number(None).await
    }

    #[cfg(feature = "tq-auth")]
    pub async fn tqkq_login_command_numbered(
        &self,
        number: u8,
    ) -> crate::error::Result<TradeLoginCommand> {
        self.tqkq_login_command_with_number(Some(number)).await
    }

    #[cfg(feature = "tq-auth")]
    pub async fn tqkq_stock_login_command(&self) -> crate::error::Result<TradeLoginCommand> {
        self.tqkq_stock_login_command_with_number(None).await
    }

    #[cfg(feature = "tq-auth")]
    pub async fn tqkq_stock_login_command_numbered(
        &self,
        number: u8,
    ) -> crate::error::Result<TradeLoginCommand> {
        self.tqkq_stock_login_command_with_number(Some(number))
            .await
    }

    pub async fn has_feature(&self, feature: &str) -> crate::error::Result<bool> {
        let auth = self.service_auth_context(false).await?;
        Ok(has_auth_feature(auth.features(), feature))
    }

    pub async fn check_md_grants(&self, symbols: &[&str]) -> crate::error::Result<()> {
        let auth = self.service_auth_context(false).await?;
        check_md_grants_for_features(auth.features(), symbols)
    }

    #[cfg(feature = "tq-auth")]
    async fn tqkq_login_command_with_number(
        &self,
        number: Option<u8>,
    ) -> crate::error::Result<TradeLoginCommand> {
        let auth_id = self.established_auth_id().await?;
        let config = if let Some(number) = number {
            TqKqAccountConfig::future_numbered(auth_id.as_str(), number)?
        } else {
            TqKqAccountConfig::future(auth_id.as_str())
        };
        Ok(config.login_command())
    }

    #[cfg(feature = "tq-auth")]
    async fn tqkq_stock_login_command_with_number(
        &self,
        number: Option<u8>,
    ) -> crate::error::Result<TradeLoginCommand> {
        let auth_id = self.established_auth_id().await?;
        let config = if let Some(number) = number {
            TqKqAccountConfig::stock_numbered(auth_id.as_str(), number)?
        } else {
            TqKqAccountConfig::stock(auth_id.as_str())
        };
        Ok(config.login_command())
    }

    #[cfg(feature = "tq-auth")]
    async fn established_auth_id(&self) -> crate::error::Result<String> {
        self.ensure_established().await?;
        let auth = self
            .auth_context()?
            .ok_or(crate::error::SessionFacadeError::InvalidState(
                "session established without a system auth context payload",
            ))?;
        auth.get("auth_id")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or(crate::error::SessionFacadeError::InvalidState(
                "system auth context is missing auth_id",
            ))
    }

    pub(crate) async fn service_auth_context(
        &self,
        force_refresh: bool,
    ) -> crate::error::Result<AuthContext> {
        let Some(io) = self.io.as_ref() else {
            return Err(crate::error::SessionFacadeError::InvalidState(
                "direct service helpers require a live session client",
            ));
        };

        let auth_provider = {
            let io = io.lock().await;
            if !force_refresh && let Some(auth) = io.cached_auth.as_ref() {
                return Ok(auth.clone());
            }
            io.auth_provider.clone()
        };

        let auth = auth_provider.authenticate_boxed().await?;
        io.lock().await.cached_auth = Some(auth.clone());
        Ok(auth)
    }
}

fn has_auth_feature(features: &[String], feature: &str) -> bool {
    features.iter().any(|item| item == feature)
}

fn check_md_grants_for_features(features: &[String], symbols: &[&str]) -> crate::error::Result<()> {
    for symbol in symbols {
        let prefix = symbol.split('.').next().unwrap_or_default();

        if LIMITED_INDEX_SYMBOLS.contains(symbol) {
            if has_auth_feature(features, "sec") || has_auth_feature(features, "lmt_idx") {
                continue;
            }
            return Err(crate::error::SessionFacadeError::from(
                tqsdk_core::ContractError::auth(format!(
                    "your account does not support market data for {symbol}"
                )),
            ));
        }

        if matches!(
            prefix,
            "CFFEX" | "SHFE" | "DCE" | "CZCE" | "INE" | "GFEX" | "SSWE" | "KQ" | "KQD"
        ) {
            if has_auth_feature(features, "futr") {
                continue;
            }
            return Err(crate::error::SessionFacadeError::from(
                tqsdk_core::ContractError::auth(format!(
                    "your account does not support futures market data for {symbol}"
                )),
            ));
        }

        if prefix == "CSI" || matches!(prefix, "SSE" | "SZSE") {
            if has_auth_feature(features, "sec") {
                continue;
            }
            return Err(crate::error::SessionFacadeError::from(
                tqsdk_core::ContractError::auth(format!(
                    "your account does not support stock market data for {symbol}"
                )),
            ));
        }

        return Err(crate::error::SessionFacadeError::from(
            tqsdk_core::ContractError::auth(format!(
                "unsupported market-data symbol namespace for {symbol}"
            )),
        ));
    }

    Ok(())
}
