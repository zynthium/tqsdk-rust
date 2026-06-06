#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::BTreeSet;

use tqsdk_core::Quote;
#[cfg(feature = "metadata")]
use tqsdk_session::{SessionClient, SessionClientBuilder};

#[cfg(feature = "metadata")]
use crate::config::RelayConfig;
use crate::error::{RelayError, RelayResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FuturesProductFilter {
    None,
    All,
    Products(Vec<FuturesProductCode>),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FuturesProductCode {
    pub exchange_id: Option<String>,
    pub product_id: String,
}

impl FuturesProductCode {
    pub fn new(exchange_id: Option<&str>, product_id: impl Into<String>) -> RelayResult<Self> {
        let exchange_id = exchange_id.map(|value| value.trim().to_string());
        let product_id = product_id.into().trim().to_string();
        if exchange_id.as_deref().is_some_and(str::is_empty) {
            return Err(RelayError::invalid_config(
                "futures product exchange_id must not be empty",
            ));
        }
        if product_id.is_empty() {
            return Err(RelayError::invalid_config(
                "futures product_id must not be empty",
            ));
        }
        Ok(Self {
            exchange_id,
            product_id,
        })
    }

    pub fn parse(value: &str) -> RelayResult<Self> {
        let value = value.trim();
        if let Some((exchange_id, product_id)) = value.split_once('.') {
            Self::new(Some(exchange_id), product_id)
        } else {
            Self::new(None, value)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuturesContract {
    pub symbol: String,
    pub exchange_id: String,
    pub product_id: String,
    pub expired: bool,
}

impl FuturesContract {
    pub fn new(
        symbol: impl Into<String>,
        exchange_id: impl Into<String>,
        product_id: impl Into<String>,
        expired: bool,
    ) -> RelayResult<Self> {
        let symbol = symbol.into().trim().to_string();
        let exchange_id = exchange_id.into().trim().to_string();
        let product_id = product_id.into().trim().to_string();
        if symbol.is_empty() {
            return Err(RelayError::invalid_config(
                "futures contract symbol must not be empty",
            ));
        }
        if exchange_id.is_empty() {
            return Err(RelayError::invalid_config(
                "futures contract exchange_id must not be empty",
            ));
        }
        if product_id.is_empty() {
            return Err(RelayError::invalid_config(
                "futures contract product_id must not be empty",
            ));
        }
        Ok(Self {
            symbol,
            exchange_id,
            product_id,
            expired,
        })
    }

    pub fn from_symbol(symbol: impl Into<String>, expired: bool) -> RelayResult<Self> {
        let symbol = symbol.into();
        let (exchange_id, instrument_id) = symbol
            .split_once('.')
            .ok_or_else(|| RelayError::invalid_config("futures symbol must include exchange"))?;
        let product_id: String = instrument_id
            .chars()
            .take_while(|ch| !ch.is_ascii_digit())
            .collect();
        Self::new(symbol.clone(), exchange_id, product_id, expired)
    }

    pub fn from_quote(quote: &Quote) -> RelayResult<Self> {
        Self::new(
            quote.instrument_id.clone(),
            quote.exchange_id.clone(),
            quote.product_id.clone(),
            quote.expired,
        )
    }

    fn matches_product(&self, product: &FuturesProductCode) -> bool {
        product
            .exchange_id
            .as_deref()
            .is_none_or(|exchange_id| exchange_id == self.exchange_id)
            && product.product_id == self.product_id
    }
}

pub trait FuturesUniverseResolver {
    fn active_futures(
        &mut self,
    ) -> impl std::future::Future<Output = RelayResult<Vec<FuturesContract>>> + Send + '_;
}

#[derive(Debug, Clone)]
pub struct StaticFuturesUniverseResolver {
    contracts: Vec<FuturesContract>,
}

impl StaticFuturesUniverseResolver {
    pub fn new<I>(contracts: I) -> Self
    where
        I: IntoIterator<Item = FuturesContract>,
    {
        Self {
            contracts: contracts.into_iter().collect(),
        }
    }
}

impl FuturesUniverseResolver for StaticFuturesUniverseResolver {
    async fn active_futures(&mut self) -> RelayResult<Vec<FuturesContract>> {
        Ok(self.contracts.clone())
    }
}

#[cfg(feature = "metadata")]
pub struct SessionFuturesUniverseResolver {
    client: SessionClient,
}

#[cfg(feature = "metadata")]
impl SessionFuturesUniverseResolver {
    pub fn new(client: SessionClient) -> Self {
        Self { client }
    }

    pub fn from_config(config: &RelayConfig) -> RelayResult<Self> {
        let user = config
            .upstream_auth_user
            .as_deref()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                RelayError::invalid_config("TQ_AUTH_USER is required for futures product discovery")
            })?;
        let pass = config
            .upstream_auth_pass
            .as_deref()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                RelayError::invalid_config("TQ_AUTH_PASS is required for futures product discovery")
            })?;
        let client = SessionClientBuilder::new(user, pass)
            .enable_query()
            .futures_market()
            .build()
            .map_err(|err| RelayError::Internal(err.to_string()))?;
        Ok(Self::new(client))
    }
}

#[cfg(feature = "metadata")]
impl FuturesUniverseResolver for SessionFuturesUniverseResolver {
    async fn active_futures(&mut self) -> RelayResult<Vec<FuturesContract>> {
        let symbols = self
            .client
            .query_quotes(Some("FUTURE"), None, None, Some(false), None)
            .await
            .map_err(|err| RelayError::Transport(format!("futures discovery failed: {err}")))?;
        if symbols.is_empty() {
            return Ok(Vec::new());
        }
        let symbol_refs: Vec<&str> = symbols.iter().map(String::as_str).collect();
        let quotes = self
            .client
            .query_symbol_info(&symbol_refs)
            .await
            .map_err(|err| RelayError::Transport(format!("futures metadata failed: {err}")))?;
        quotes.iter().map(FuturesContract::from_quote).collect()
    }
}

pub async fn resolve_futures_symbols<R>(
    filter: &FuturesProductFilter,
    resolver: &mut R,
) -> RelayResult<Vec<String>>
where
    R: FuturesUniverseResolver + Send,
{
    match filter {
        FuturesProductFilter::None => Ok(Vec::new()),
        FuturesProductFilter::All => active_symbols(resolver.active_futures().await?, |_| true),
        FuturesProductFilter::Products(products) => {
            if products.is_empty() {
                return Err(RelayError::invalid_config(
                    "futures product filter must not be empty",
                ));
            }
            let products = products.clone();
            active_symbols(resolver.active_futures().await?, |contract| {
                products
                    .iter()
                    .any(|product| contract.matches_product(product))
            })
        }
    }
}

fn active_symbols(
    contracts: Vec<FuturesContract>,
    matches: impl Fn(&FuturesContract) -> bool,
) -> RelayResult<Vec<String>> {
    let mut symbols = BTreeSet::new();
    for contract in contracts {
        if !contract.expired && matches(&contract) {
            symbols.insert(contract.symbol);
        }
    }
    Ok(symbols.into_iter().collect())
}
