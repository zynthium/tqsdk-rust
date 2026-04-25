use tqsdk_core::{
    AccountId, ContractError, Result, TradeAccountType, TradeLoginCommand, TradeSessionTarget,
};

const TQKQ_BROKER_ID: &str = "快期模拟";
const TQKQ_STOCK_BROKER_ID: &str = "快期股票模拟";

/// Official built-in Tianqin simulated account login material derived from an
/// authenticated `auth_id`.
///
/// This helper stays at the protocol layer. It only captures the broker/account
/// credentials required to route a trade session and emit a `req_login`
/// command. Runtime ownership and trade-session orchestration stay outside the
/// core crate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TqKqAccountConfig {
    broker_id: String,
    account_id: AccountId,
    password: String,
    account_type: TradeAccountType,
}

#[cfg_attr(not(test), allow(dead_code))]
impl TqKqAccountConfig {
    /// Builds the primary futures built-in simulated account profile.
    #[must_use]
    pub fn future(auth_id: impl AsRef<str>) -> Self {
        Self::new_future(auth_id.as_ref(), None)
    }

    /// Builds a numbered futures built-in simulated account profile.
    ///
    /// The official Python implementation accepts assistant accounts in the
    /// range `1..=99`.
    pub fn future_numbered(auth_id: impl AsRef<str>, number: u8) -> Result<Self> {
        Self::new_numbered_future(auth_id.as_ref(), number)
    }

    /// Builds the primary stock built-in simulated account profile.
    #[must_use]
    pub fn stock(auth_id: impl AsRef<str>) -> Self {
        Self::new_stock(auth_id.as_ref(), None)
    }

    /// Builds a numbered stock built-in simulated account profile.
    ///
    /// The official Python implementation accepts assistant accounts in the
    /// range `1..=99`.
    pub fn stock_numbered(auth_id: impl AsRef<str>, number: u8) -> Result<Self> {
        Self::new_numbered_stock(auth_id.as_ref(), number)
    }

    #[must_use]
    pub fn broker_id(&self) -> &str {
        &self.broker_id
    }

    #[must_use]
    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    #[must_use]
    pub fn password(&self) -> &str {
        &self.password
    }

    #[must_use]
    pub fn account_type(&self) -> TradeAccountType {
        self.account_type
    }

    #[must_use]
    pub fn trade_target(&self) -> TradeSessionTarget {
        TradeSessionTarget::new(self.broker_id.clone(), self.account_id.clone())
    }

    #[must_use]
    pub fn login_command(&self) -> TradeLoginCommand {
        TradeLoginCommand {
            account_id: self.account_id.clone(),
            broker_id: self.broker_id.clone(),
            password: self.password.clone(),
            account_type: self.account_type,
            front_broker: None,
            front_url: None,
            client_app_id: None,
            client_system_info: None,
        }
    }

    fn new_future(auth_id: &str, suffix: Option<u8>) -> Self {
        match suffix {
            Some(number) => Self {
                broker_id: TQKQ_BROKER_ID.to_string(),
                account_id: AccountId::new(format!("{auth_id}{number:03}")),
                password: format!("shinnytech{number:03}"),
                account_type: TradeAccountType::Future,
            },
            None => Self {
                broker_id: TQKQ_BROKER_ID.to_string(),
                account_id: AccountId::new(auth_id),
                password: auth_id.to_string(),
                account_type: TradeAccountType::Future,
            },
        }
    }

    fn new_numbered_future(auth_id: &str, number: u8) -> Result<Self> {
        validate_number(number)?;
        Ok(Self::new_future(auth_id, Some(number)))
    }

    fn new_stock(auth_id: &str, suffix: Option<u8>) -> Self {
        match suffix {
            Some(number) => Self {
                broker_id: TQKQ_STOCK_BROKER_ID.to_string(),
                account_id: AccountId::new(format!("{auth_id}{number:03}-sim-securities")),
                password: format!("shinnytech{number:03}"),
                account_type: TradeAccountType::Spot,
            },
            None => Self {
                broker_id: TQKQ_STOCK_BROKER_ID.to_string(),
                account_id: AccountId::new(format!("{auth_id}-sim-securities")),
                password: auth_id.to_string(),
                account_type: TradeAccountType::Spot,
            },
        }
    }

    fn new_numbered_stock(auth_id: &str, number: u8) -> Result<Self> {
        validate_number(number)?;
        Ok(Self::new_stock(auth_id, Some(number)))
    }
}

fn validate_number(number: u8) -> Result<()> {
    if (1..=99).contains(&number) {
        Ok(())
    } else {
        Err(ContractError::validation(format!(
            "TqKq assistant account number must be within 1..=99, got {number}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::TqKqAccountConfig;
    use tqsdk_core::TradeAccountType;

    #[test]
    fn future_main_matches_official_python_profile() {
        let config = TqKqAccountConfig::future("auth-1");

        assert_eq!(config.broker_id(), "快期模拟");
        assert_eq!(config.account_id().as_str(), "auth-1");
        assert_eq!(config.password(), "auth-1");
        assert_eq!(config.account_type(), TradeAccountType::Future);

        let trade_target = config.trade_target();
        assert_eq!(trade_target.broker_id, "快期模拟");
        assert_eq!(trade_target.account_id.as_str(), "auth-1");

        let login = config.login_command();
        assert_eq!(login.broker_id, "快期模拟");
        assert_eq!(login.account_id.as_str(), "auth-1");
        assert_eq!(login.password, "auth-1");
        assert_eq!(login.account_type, TradeAccountType::Future);
    }

    #[test]
    fn future_numbered_matches_official_python_profile() {
        let config = TqKqAccountConfig::future_numbered("auth-1", 7).unwrap();

        assert_eq!(config.broker_id(), "快期模拟");
        assert_eq!(config.account_id().as_str(), "auth-1007");
        assert_eq!(config.password(), "shinnytech007");
        assert_eq!(config.account_type(), TradeAccountType::Future);
    }

    #[test]
    fn stock_main_matches_official_python_profile() {
        let config = TqKqAccountConfig::stock("auth-1");

        assert_eq!(config.broker_id(), "快期股票模拟");
        assert_eq!(config.account_id().as_str(), "auth-1-sim-securities");
        assert_eq!(config.password(), "auth-1");
        assert_eq!(config.account_type(), TradeAccountType::Spot);
    }

    #[test]
    fn stock_numbered_matches_official_python_profile() {
        let config = TqKqAccountConfig::stock_numbered("auth-1", 7).unwrap();

        assert_eq!(config.broker_id(), "快期股票模拟");
        assert_eq!(config.account_id().as_str(), "auth-1007-sim-securities");
        assert_eq!(config.password(), "shinnytech007");
        assert_eq!(config.account_type(), TradeAccountType::Spot);
    }

    #[test]
    fn numbered_accounts_reject_zero_and_out_of_range_values() {
        let zero =
            TqKqAccountConfig::future_numbered("auth-1", 0).expect_err("zero should be rejected");
        assert!(
            zero.to_string().contains("1..=99"),
            "unexpected error message: {zero}"
        );

        let too_large = TqKqAccountConfig::stock_numbered("auth-1", 100)
            .expect_err("numbers above 99 should be rejected");
        assert!(
            too_large.to_string().contains("1..=99"),
            "unexpected error message: {too_large}"
        );
    }
}
