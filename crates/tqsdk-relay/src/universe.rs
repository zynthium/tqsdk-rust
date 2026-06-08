#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::BTreeSet;

#[cfg(feature = "metadata")]
use serde_json::{Value, json};
use tqsdk_core::Quote;
#[cfg(feature = "metadata")]
use tqsdk_session::{SessionClient, SessionClientBuilder};

#[cfg(feature = "metadata")]
use crate::config::{DEFAULT_FUTURES_METADATA_BATCH_SIZE, RelayConfig};
use crate::error::{RelayError, RelayResult};

#[cfg(feature = "metadata")]
const FUTURES_DISCOVERY_SYMBOL_INFO_QUERY: &str = r#"query($instrument_id:[String]){
  multi_symbol_info(instrument_id: $instrument_id){
    ... on basic {
      instrument_id
      exchange_id
      class
    }
    ... on future {
      expired
      product_id
    }
  }
}"#;

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
    metadata_batch_size: usize,
}

#[cfg(feature = "metadata")]
impl SessionFuturesUniverseResolver {
    pub fn new(client: SessionClient) -> Self {
        Self::new_with_metadata_batch_size(client, DEFAULT_FUTURES_METADATA_BATCH_SIZE)
            .expect("default futures metadata batch size is non-zero")
    }

    pub fn new_with_metadata_batch_size(
        client: SessionClient,
        metadata_batch_size: usize,
    ) -> RelayResult<Self> {
        if metadata_batch_size == 0 {
            return Err(RelayError::invalid_config(
                "futures metadata batch size must be greater than zero",
            ));
        }
        Ok(Self {
            client,
            metadata_batch_size,
        })
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
        let client = session_client_builder_for_futures_discovery(user, pass)
            .build()
            .map_err(|err| RelayError::Internal(err.to_string()))?;
        Self::new_with_metadata_batch_size(client, config.futures_metadata_batch_size)
    }
}

#[cfg(feature = "metadata")]
fn session_client_builder_for_futures_discovery(user: &str, pass: &str) -> SessionClientBuilder {
    SessionClientBuilder::new(user, pass)
        .enable_query()
        .stock_market()
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
        let mut contracts = Vec::new();
        for batch in futures_metadata_symbol_batches(&symbols, self.metadata_batch_size)? {
            let symbol_list: Vec<String> =
                batch.iter().map(|symbol| (*symbol).to_string()).collect();
            let payload = self
                .client
                .query_graphql_value(
                    FUTURES_DISCOVERY_SYMBOL_INFO_QUERY,
                    Some(json!({ "instrument_id": symbol_list })),
                )
                .await
                .map_err(|err| RelayError::Transport(format!("futures metadata failed: {err}")))?;
            contracts.extend(parse_futures_discovery_contracts(&payload)?);
        }
        Ok(contracts)
    }
}

#[cfg(feature = "metadata")]
fn parse_futures_discovery_contracts(payload: &Value) -> RelayResult<Vec<FuturesContract>> {
    let Some(symbols) = payload
        .get("result")
        .and_then(|result| result.get("multi_symbol_info"))
        .and_then(Value::as_array)
    else {
        return Ok(Vec::new());
    };

    let mut contracts = Vec::new();
    for symbol in symbols {
        let Some(node) = symbol.as_object() else {
            continue;
        };
        if node.get("class").and_then(Value::as_str) != Some("FUTURE") {
            continue;
        }
        let Some(instrument_id) = node.get("instrument_id").and_then(Value::as_str) else {
            continue;
        };
        let Some(exchange_id) = node.get("exchange_id").and_then(Value::as_str) else {
            continue;
        };
        let Some(product_id) = node.get("product_id").and_then(Value::as_str) else {
            continue;
        };
        let expired = node
            .get("expired")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        contracts.push(FuturesContract::new(
            instrument_id,
            exchange_id,
            product_id,
            expired,
        )?);
    }
    Ok(contracts)
}

pub fn futures_metadata_symbol_batches(
    symbols: &[String],
    batch_size: usize,
) -> RelayResult<Vec<Vec<&str>>> {
    if batch_size == 0 {
        return Err(RelayError::invalid_config(
            "futures metadata batch size must be greater than zero",
        ));
    }
    Ok(symbols
        .chunks(batch_size)
        .map(|chunk| chunk.iter().map(String::as_str).collect())
        .collect())
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

#[cfg(all(test, feature = "metadata"))]
mod tests {
    use serde_json::json;
    use tqsdk_core::MarketSessionTarget;

    use super::{parse_futures_discovery_contracts, session_client_builder_for_futures_discovery};

    #[test]
    fn futures_discovery_uses_stock_query_market_target() {
        let builder = session_client_builder_for_futures_discovery("user", "pass");

        assert!(builder.query_enabled());
        assert_eq!(
            builder.market_target_ref(),
            &MarketSessionTarget::stock_live()
        );
    }

    #[test]
    fn parses_minimal_futures_discovery_metadata() {
        let payload = json!({
            "result": {
                "multi_symbol_info": [
                    {
                        "instrument_id": "SHFE.au2602",
                        "exchange_id": "SHFE",
                        "class": "FUTURE",
                        "expired": false,
                        "product_id": "au"
                    },
                    {
                        "instrument_id": "SSE.600000",
                        "exchange_id": "SSE",
                        "class": "STOCK",
                        "expired": false,
                        "product_id": "600000"
                    },
                    {
                        "instrument_id": "DCE.m2609",
                        "exchange_id": "DCE",
                        "class": "FUTURE",
                        "expired": true,
                        "product_id": "m"
                    }
                ]
            }
        });

        let contracts = parse_futures_discovery_contracts(&payload).unwrap();

        assert_eq!(contracts.len(), 2);
        assert_eq!(contracts[0].symbol, "SHFE.au2602");
        assert_eq!(contracts[0].exchange_id, "SHFE");
        assert_eq!(contracts[0].product_id, "au");
        assert!(!contracts[0].expired);
        assert_eq!(contracts[1].symbol, "DCE.m2609");
        assert_eq!(contracts[1].exchange_id, "DCE");
        assert_eq!(contracts[1].product_id, "m");
        assert!(contracts[1].expired);
    }
}
