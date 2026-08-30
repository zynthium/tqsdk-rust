#![cfg_attr(not(test), forbid(unsafe_code))]

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
#[cfg(feature = "metadata")]
use std::time::Duration;

#[cfg(feature = "metadata")]
use tokio::time::Instant;
#[cfg(feature = "metadata")]
use tqsdk_core::RuntimeReader;
use tqsdk_core::{Quote, TradingTime};
#[cfg(feature = "metadata")]
use tqsdk_session::{InstrumentClass, SessionClient, SessionClientBuilder, SymbolInfo};

#[cfg(feature = "metadata")]
use crate::config::{DEFAULT_FUTURES_METADATA_BATCH_SIZE, RelayConfig};
use crate::error::{RelayError, RelayResult};
use crate::symbol_identity::{
    continuous_contract_display_name, continuous_contract_display_name_from_product_name,
    futures_product_chinese_name, product_name_from_instrument_name,
    supports_index_continuous_contract,
};
use crate::universe_expression::{
    UniverseClause, UniverseExpression, UniverseSelector, UniverseSelectorKind,
};

#[cfg(feature = "metadata")]
const FUTURES_ACTIVITY_QUOTE_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(feature = "metadata")]
const FUTURES_ACTIVITY_QUOTE_POLL: Duration = Duration::from_millis(250);
const SUPPORTED_FUTURES_UNIVERSE_EXCHANGES: &[&str] =
    &["CFFEX", "SHFE", "DCE", "CZCE", "INE", "GFEX"];

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProductScope {
    All,
    Products(Vec<FuturesProductCode>),
}

#[cfg(test)]
mod universe_v2_tests {
    use super::{FuturesContract, StaticFuturesUniverseResolver, resolve_futures_universe_v2};

    #[tokio::test]
    async fn materialized_adapter_uses_shared_v2_compiler() {
        let input = tqsdk_data::UniverseInput::from_spec(
            tqsdk_data::UniverseSpec::parse_v2(
                "snapshot(contract:all;main:DCE.m;continuous:DCE.m;index:DCE.m)",
            )
            .unwrap(),
        )
        .expand()
        .unwrap();
        let mut resolver = StaticFuturesUniverseResolver::new([
            FuturesContract::new("DCE.m2609", "DCE", "m", false).unwrap(),
            FuturesContract::new("SHFE.rb2601", "SHFE", "rb", false).unwrap(),
        ])
        .with_main_symbols(["DCE.m2609"]);

        let (compiled, contracts) = resolve_futures_universe_v2(&input, &mut resolver)
            .await
            .unwrap();
        let symbols = compiled
            .candidates()
            .iter()
            .map(|candidate| candidate.symbol())
            .collect::<Vec<_>>();
        assert_eq!(
            symbols,
            vec!["DCE.m2609", "KQ.i@DCE.m", "KQ.m@DCE.m", "SHFE.rb2601"]
        );
        assert_eq!(contracts.len(), symbols.len());
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ProductSelection {
    limit_per_product: Option<usize>,
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

#[derive(Debug, Clone, PartialEq)]
pub struct FuturesContract {
    pub symbol: String,
    pub instrument_name: Option<String>,
    pub exchange_id: String,
    pub product_id: String,
    pub expired: bool,
    pub trading_time: TradingTime,
}

impl FuturesContract {
    pub fn new(
        symbol: impl Into<String>,
        exchange_id: impl Into<String>,
        product_id: impl Into<String>,
        expired: bool,
    ) -> RelayResult<Self> {
        Self::new_with_trading_time(
            symbol,
            exchange_id,
            product_id,
            expired,
            TradingTime::default(),
        )
    }

    pub fn new_with_trading_time(
        symbol: impl Into<String>,
        exchange_id: impl Into<String>,
        product_id: impl Into<String>,
        expired: bool,
        trading_time: TradingTime,
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
            instrument_name: None,
            exchange_id,
            product_id,
            expired,
            trading_time,
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
        let mut contract = Self::new_with_trading_time(
            quote.instrument_id.clone(),
            quote.exchange_id.clone(),
            quote.product_id.clone(),
            quote.expired,
            quote.trading_time.clone(),
        )?;
        let instrument_name = quote.instrument_name.trim();
        if !instrument_name.is_empty() {
            contract.instrument_name = Some(instrument_name.to_string());
        }
        Ok(contract)
    }

    #[cfg(feature = "metadata")]
    pub fn from_symbol_info(info: SymbolInfo) -> RelayResult<Self> {
        let mut contract = Self::new_with_trading_time(
            info.instrument_id.to_string(),
            info.exchange_id,
            info.product_id,
            info.expired,
            info.trading_time,
        )?;
        let instrument_name = info.instrument_name.trim();
        if !instrument_name.is_empty() {
            contract.instrument_name = Some(instrument_name.to_string());
        }
        Ok(contract)
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

    fn main_futures(
        &mut self,
    ) -> impl std::future::Future<Output = RelayResult<Vec<String>>> + Send + '_ {
        std::future::ready(Ok(Vec::new()))
    }

    fn quote_snapshots<'a>(
        &'a mut self,
        _symbols: &'a [String],
    ) -> impl std::future::Future<Output = RelayResult<Vec<Quote>>> + Send + 'a {
        std::future::ready(Ok(Vec::new()))
    }

    fn trading_calendar(
        &mut self,
    ) -> impl std::future::Future<Output = RelayResult<Vec<tqsdk_core::TradingCalendarDay>>> + Send + '_
    {
        std::future::ready(Ok(Vec::new()))
    }
}

#[derive(Debug, Clone)]
pub struct StaticFuturesUniverseResolver {
    contracts: Vec<FuturesContract>,
    main_symbols: Vec<String>,
    quote_snapshots: Vec<Quote>,
    trading_calendar: Vec<tqsdk_core::TradingCalendarDay>,
}

impl StaticFuturesUniverseResolver {
    pub fn new<I>(contracts: I) -> Self
    where
        I: IntoIterator<Item = FuturesContract>,
    {
        Self {
            contracts: contracts.into_iter().collect(),
            main_symbols: Vec::new(),
            quote_snapshots: Vec::new(),
            trading_calendar: Vec::new(),
        }
    }

    pub fn with_main_symbols<I, S>(mut self, symbols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.main_symbols = symbols.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_quote_snapshots<I>(mut self, quotes: I) -> Self
    where
        I: IntoIterator<Item = Quote>,
    {
        self.quote_snapshots = quotes.into_iter().collect();
        self
    }
}

impl FuturesUniverseResolver for StaticFuturesUniverseResolver {
    async fn active_futures(&mut self) -> RelayResult<Vec<FuturesContract>> {
        Ok(self.contracts.clone())
    }

    async fn main_futures(&mut self) -> RelayResult<Vec<String>> {
        Ok(self.main_symbols.clone())
    }

    async fn quote_snapshots(&mut self, _symbols: &[String]) -> RelayResult<Vec<Quote>> {
        Ok(self.quote_snapshots.clone())
    }

    async fn trading_calendar(&mut self) -> RelayResult<Vec<tqsdk_core::TradingCalendarDay>> {
        Ok(self.trading_calendar.clone())
    }
}

#[cfg(feature = "metadata")]
pub struct SessionFuturesUniverseResolver {
    client: SessionClient,
    activity_client: Option<SessionClient>,
    metadata_batch_size: usize,
    activity_quote_timeout: Duration,
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
            activity_client: None,
            metadata_batch_size,
            activity_quote_timeout: FUTURES_ACTIVITY_QUOTE_TIMEOUT,
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
        let mut resolver =
            Self::new_with_metadata_batch_size(client, config.futures_metadata_batch_size)?;
        if config
            .futures_universe_expression
            .as_ref()
            .is_some_and(expression_requires_activity_quotes)
        {
            let activity_client = SessionClientBuilder::new(user, pass)
                .futures_market()
                .build()
                .map_err(|err| RelayError::Internal(err.to_string()))?;
            resolver.activity_client = Some(activity_client);
        }
        Ok(resolver)
    }
}

#[cfg(feature = "metadata")]
pub(crate) fn session_client_builder_for_futures_discovery(
    user: &str,
    pass: &str,
) -> SessionClientBuilder {
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
            let infos =
                self.client.query_symbol_info(&batch).await.map_err(|err| {
                    RelayError::Transport(format!("futures metadata failed: {err}"))
                })?;
            contracts.extend(futures_contracts_from_symbol_info(infos)?);
        }
        Ok(contracts)
    }

    async fn main_futures(&mut self) -> RelayResult<Vec<String>> {
        self.client
            .query_cont_quotes(None, None, None)
            .await
            .map_err(|err| {
                RelayError::Transport(format!("futures main contract query failed: {err}"))
            })
    }

    async fn quote_snapshots(&mut self, symbols: &[String]) -> RelayResult<Vec<Quote>> {
        if symbols.is_empty() {
            return Ok(Vec::new());
        }
        let Some(client) = self.activity_client.as_ref() else {
            return Err(RelayError::invalid_config(
                "futures activity ranking requires a futures market session",
            ));
        };
        let lease = client
            .ensure_quotes(symbols.iter().map(String::as_str))
            .await
            .map_err(|err| {
                RelayError::Transport(format!("futures quote subscription failed: {err}"))
            })?;
        let snapshots =
            wait_for_quote_snapshots(client, symbols, self.activity_quote_timeout).await;
        let _ = lease.close().await;
        snapshots
    }

    async fn trading_calendar(&mut self) -> RelayResult<Vec<tqsdk_core::TradingCalendarDay>> {
        let now = chrono::Utc::now()
            .with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap())
            .date_naive();
        let start_dt = now - chrono::Days::new(14);
        let end_dt = now + chrono::Days::new(14);
        self.client
            .get_trading_calendar(start_dt, end_dt)
            .await
            .map_err(|err| RelayError::Transport(format!("futures calendar query failed: {err}")))
    }
}

#[cfg(feature = "metadata")]
pub(crate) fn futures_contracts_from_symbol_info(
    infos: Vec<SymbolInfo>,
) -> RelayResult<Vec<FuturesContract>> {
    infos
        .into_iter()
        .filter(|info| info.class == InstrumentClass::Future)
        .map(FuturesContract::from_symbol_info)
        .collect()
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

#[cfg(feature = "metadata")]
fn expression_requires_activity_quotes(expression: &UniverseExpression) -> bool {
    expression.clauses().iter().any(
        |clause| matches!(clause.selector().kind(), UniverseSelectorKind::Top(limit) if limit > 1),
    )
}

pub async fn resolve_futures_universe_symbols<R>(
    expression: &UniverseExpression,
    resolver: &mut R,
) -> RelayResult<Vec<String>>
where
    R: FuturesUniverseResolver + Send,
{
    let contracts = resolve_futures_contracts_with_expression(expression, resolver).await?;
    Ok(contracts
        .into_iter()
        .map(|contract| contract.symbol)
        .collect())
}

pub(crate) async fn resolve_futures_universe_v2<R>(
    input: &tqsdk_data::ExpandedUniverseInput,
    resolver: &mut R,
) -> RelayResult<(tqsdk_data::CompiledUniverse, Vec<FuturesContract>)>
where
    R: FuturesUniverseResolver + Send,
{
    let contracts = resolver.active_futures().await?;
    let needs_main = input.spec().is_some_and(|spec| {
        spec.includes().iter().any(|selector| {
            matches!(
                selector.view(),
                tqsdk_data::UniverseView::Main | tqsdk_data::UniverseView::Top(_)
            )
        })
    });
    let main_symbols = if needs_main {
        resolver.main_futures().await?
    } else {
        Vec::new()
    };
    let quote_snapshots = if input.spec().is_some_and(|spec| {
        spec.includes().iter().any(
            |selector| matches!(selector.view(), tqsdk_data::UniverseView::Top(limit) if limit > 1),
        )
    }) {
        let symbols = contracts
            .iter()
            .filter(|contract| !contract.expired)
            .map(|contract| contract.symbol.clone())
            .collect::<Vec<_>>();
        resolver.quote_snapshots(&symbols).await?
    } else {
        Vec::new()
    };

    let mut data_contracts = Vec::with_capacity(contracts.len());
    for contract in &contracts {
        let mut converted = tqsdk_data::FuturesContract::new_with_trading_time(
            &contract.symbol,
            &contract.exchange_id,
            &contract.product_id,
            contract.expired,
            contract.trading_time.clone(),
        )?;
        converted.instrument_name = contract.instrument_name.clone();
        data_contracts.push(converted);
    }
    let mut data_resolver = tqsdk_data::StaticFuturesUniverseResolver::new(data_contracts)
        .with_main_symbols(main_symbols)
        .with_quote_snapshots(quote_snapshots);
    let compiled = tqsdk_data::resolve_futures_universe_v2(input, &mut data_resolver).await?;

    let contracts_by_symbol = contracts
        .into_iter()
        .map(|contract| (contract.symbol.clone(), contract))
        .collect::<BTreeMap<_, _>>();
    let resolved_contracts = compiled
        .candidates()
        .iter()
        .map(|candidate| {
            contracts_by_symbol
                .get(candidate.symbol())
                .cloned()
                .map(Ok)
                .unwrap_or_else(|| contract_from_configured_symbol(candidate.symbol()))
        })
        .collect::<RelayResult<Vec<_>>>()?;
    Ok((compiled, resolved_contracts))
}

pub fn resolve_static_symbols_with_expression(
    expression: &UniverseExpression,
) -> RelayResult<Vec<String>> {
    Ok(static_contracts_with_expression(expression)?
        .into_iter()
        .map(|contract| contract.symbol)
        .collect())
}

pub(crate) fn static_contracts_with_expression(
    expression: &UniverseExpression,
) -> RelayResult<Vec<FuturesContract>> {
    let mut included = BTreeMap::<String, FuturesContract>::new();
    let mut exclusions = Vec::<UniverseMatch>::new();
    for clause in expression.clauses() {
        if clause.exclude() {
            exclusions.extend(matches_for_clause(clause)?);
            continue;
        }
        match clause.selector().kind() {
            UniverseSelectorKind::Symbol | UniverseSelectorKind::Auto => {
                for value in clause.selector().values() {
                    let contract = contract_from_configured_symbol(value)?;
                    included.insert(contract.symbol.clone(), contract);
                }
            }
            UniverseSelectorKind::File => {
                for symbol in symbols_from_files(clause.selector().values())? {
                    let contract = contract_from_configured_symbol(&symbol)?;
                    included.insert(contract.symbol.clone(), contract);
                }
            }
            _ => {
                return Err(RelayError::invalid_config(
                    "dynamic futures universe expression requires metadata feature",
                ));
            }
        }
    }
    for exclusion in exclusions {
        included.retain(|_, contract| !exclusion.matches(contract));
    }
    retain_supported_futures_universe_contracts(&mut included);
    Ok(included.into_values().collect())
}

pub async fn resolve_futures_contracts_with_expression<R>(
    expression: &UniverseExpression,
    resolver: &mut R,
) -> RelayResult<Vec<FuturesContract>>
where
    R: FuturesUniverseResolver + Send,
{
    let mut included = BTreeMap::<String, FuturesContract>::new();
    let mut exclusions = Vec::<UniverseMatch>::new();
    for clause in expression.clauses() {
        if clause.exclude() {
            exclusions.extend(matches_for_clause(clause)?);
            continue;
        }
        for contract in contracts_for_selector(clause.selector(), resolver).await? {
            included.insert(contract.symbol.clone(), contract);
        }
    }
    for exclusion in exclusions {
        included.retain(|_, contract| !exclusion.matches(contract));
    }
    retain_supported_futures_universe_contracts(&mut included);
    Ok(included.into_values().collect())
}

fn retain_supported_futures_universe_contracts(contracts: &mut BTreeMap<String, FuturesContract>) {
    contracts.retain(|_, contract| supports_futures_universe_exchange(&contract.exchange_id));
}

fn supports_futures_universe_exchange(exchange_id: &str) -> bool {
    SUPPORTED_FUTURES_UNIVERSE_EXCHANGES
        .iter()
        .any(|supported| exchange_id.eq_ignore_ascii_case(supported))
}

async fn contracts_for_product_scope<R>(
    scope: ProductScope,
    selection: ProductSelection,
    resolver: &mut R,
) -> RelayResult<Vec<FuturesContract>>
where
    R: FuturesUniverseResolver + Send,
{
    if selection.limit_per_product == Some(0) {
        return Err(RelayError::invalid_config(
            "top selector limit must be greater than zero",
        ));
    }
    match scope {
        ProductScope::All => {
            let contracts = resolver.active_futures().await?;
            resolve_active_contracts(contracts, |_| true, selection, resolver).await
        }
        ProductScope::Products(products) => {
            if products.is_empty() {
                return Err(RelayError::invalid_config(
                    "futures universe product selector must not be empty",
                ));
            }
            let contracts = resolver.active_futures().await?;
            resolve_active_contracts(
                contracts,
                |contract| {
                    products
                        .iter()
                        .any(|product| contract.matches_product(product))
                },
                selection,
                resolver,
            )
            .await
        }
    }
}

async fn contracts_for_selector<R>(
    selector: &UniverseSelector,
    resolver: &mut R,
) -> RelayResult<Vec<FuturesContract>>
where
    R: FuturesUniverseResolver + Send,
{
    match selector.kind() {
        UniverseSelectorKind::Active => {
            let scope = product_scope_from_values(selector.values())?;
            contracts_for_product_scope(scope, ProductSelection::default(), resolver).await
        }
        UniverseSelectorKind::Main => {
            let scope = product_scope_from_values(selector.values())?;
            contracts_for_product_scope(
                scope,
                ProductSelection {
                    limit_per_product: Some(1),
                },
                resolver,
            )
            .await
        }
        UniverseSelectorKind::Top(limit) => {
            let scope = product_scope_from_values(selector.values())?;
            contracts_for_product_scope(
                scope,
                ProductSelection {
                    limit_per_product: Some(limit),
                },
                resolver,
            )
            .await
        }
        UniverseSelectorKind::Index => {
            continuous_contracts_for_values("KQ.i", selector.values(), resolver).await
        }
        UniverseSelectorKind::Cont => {
            continuous_contracts_for_values("KQ.m", selector.values(), resolver).await
        }
        UniverseSelectorKind::Symbol => selector
            .values()
            .iter()
            .map(|symbol| contract_from_configured_symbol(symbol))
            .collect(),
        UniverseSelectorKind::File => symbols_from_files(selector.values())?
            .into_iter()
            .map(|symbol| contract_from_configured_symbol(&symbol))
            .collect(),
        UniverseSelectorKind::Product => {
            let scope = product_scope_from_values(selector.values())?;
            contracts_for_product_scope(scope, ProductSelection::default(), resolver).await
        }
        UniverseSelectorKind::Exchange => {
            let contracts = resolver.active_futures().await?;
            active_contracts(contracts, |contract| {
                selector
                    .values()
                    .iter()
                    .any(|exchange| exchange == &contract.exchange_id)
            })
        }
        UniverseSelectorKind::Auto => {
            let mut selected = BTreeMap::new();
            for value in selector.values() {
                match classify_universe_token(value)? {
                    UniverseMatch::Symbol(symbol) => {
                        let contract = contract_from_configured_symbol(&symbol)?;
                        selected.insert(contract.symbol.clone(), contract);
                    }
                    UniverseMatch::Product(product) => {
                        for contract in contracts_for_product_scope(
                            ProductScope::Products(vec![product]),
                            ProductSelection::default(),
                            resolver,
                        )
                        .await?
                        {
                            selected.insert(contract.symbol.clone(), contract);
                        }
                    }
                    UniverseMatch::Exchange(exchange) => {
                        for contract in
                            active_contracts(resolver.active_futures().await?, |contract| {
                                contract.exchange_id == exchange
                            })?
                        {
                            selected.insert(contract.symbol.clone(), contract);
                        }
                    }
                }
            }
            Ok(selected.into_values().collect())
        }
    }
}

async fn continuous_contracts_for_values<R>(
    prefix: &str,
    values: &[String],
    resolver: &mut R,
) -> RelayResult<Vec<FuturesContract>>
where
    R: FuturesUniverseResolver + Send,
{
    let scope = product_scope_from_values(values)?;
    let contracts =
        contracts_for_product_scope(scope, ProductSelection::default(), resolver).await?;
    let mut products = BTreeSet::<(String, String)>::new();
    let mut product_names = BTreeMap::<(String, String), String>::new();
    let mut product_trading_times = BTreeMap::<(String, String), TradingTime>::new();
    for contract in contracts {
        if prefix == "KQ.i" && !supports_index_continuous_contract(&contract.exchange_id) {
            continue;
        }
        let key = (contract.exchange_id.clone(), contract.product_id.clone());
        if !trading_time_is_empty(&contract.trading_time) {
            product_trading_times
                .entry(key.clone())
                .or_insert_with(|| contract.trading_time.clone());
        }
        if let Some(product_name) = futures_product_chinese_name(&key.0, &key.1)
            .map(str::to_string)
            .or_else(|| {
                contract
                    .instrument_name
                    .as_deref()
                    .and_then(product_name_from_instrument_name)
            })
        {
            product_names.entry(key.clone()).or_insert(product_name);
        }
        products.insert(key);
    }
    products
        .into_iter()
        .map(|(exchange_id, product_id)| {
            let symbol = format!("{prefix}@{exchange_id}.{product_id}");
            let trading_time = product_trading_times
                .get(&(exchange_id.clone(), product_id.clone()))
                .cloned()
                .unwrap_or_default();
            let mut contract = FuturesContract::new_with_trading_time(
                symbol.clone(),
                exchange_id,
                product_id,
                false,
                trading_time,
            )?;
            contract.instrument_name = product_names
                .get(&(contract.exchange_id.clone(), contract.product_id.clone()))
                .and_then(|product_name| {
                    continuous_contract_display_name_from_product_name(prefix, product_name)
                })
                .or_else(|| continuous_contract_display_name(&symbol));
            Ok(contract)
        })
        .collect()
}

fn trading_time_is_empty(trading_time: &TradingTime) -> bool {
    trading_time.day.is_empty() && trading_time.night.is_empty()
}

fn product_scope_from_values(values: &[String]) -> RelayResult<ProductScope> {
    if values.len() == 1 && values[0].eq_ignore_ascii_case("all") {
        return Ok(ProductScope::All);
    }
    values
        .iter()
        .map(|value| FuturesProductCode::parse(value))
        .collect::<RelayResult<Vec<_>>>()
        .map(ProductScope::Products)
}

fn matches_for_clause(clause: &UniverseClause) -> RelayResult<Vec<UniverseMatch>> {
    let selector = clause.selector();
    let values = selector.values();
    match selector.kind() {
        UniverseSelectorKind::Symbol => values
            .iter()
            .map(|value| Ok(UniverseMatch::Symbol(value.clone())))
            .collect(),
        UniverseSelectorKind::File => symbols_from_files(values)?
            .into_iter()
            .map(UniverseMatch::Symbol)
            .map(Ok)
            .collect(),
        UniverseSelectorKind::Product
        | UniverseSelectorKind::Active
        | UniverseSelectorKind::Main
        | UniverseSelectorKind::Index
        | UniverseSelectorKind::Cont
        | UniverseSelectorKind::Top(_) => values
            .iter()
            .filter(|value| !value.eq_ignore_ascii_case("all"))
            .map(|value| FuturesProductCode::parse(value).map(UniverseMatch::Product))
            .collect(),
        UniverseSelectorKind::Exchange => values
            .iter()
            .map(|value| Ok(UniverseMatch::Exchange(value.clone())))
            .collect(),
        UniverseSelectorKind::Auto => values
            .iter()
            .map(|value| classify_universe_token(value))
            .collect(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum UniverseMatch {
    Symbol(String),
    Product(FuturesProductCode),
    Exchange(String),
}

impl UniverseMatch {
    fn matches(&self, contract: &FuturesContract) -> bool {
        match self {
            Self::Symbol(symbol) => symbol == &contract.symbol,
            Self::Product(product) => contract.matches_product(product),
            Self::Exchange(exchange) => exchange == &contract.exchange_id,
        }
    }
}

fn classify_universe_token(value: &str) -> RelayResult<UniverseMatch> {
    if value.starts_with("KQ.") && value.contains('@') {
        return Ok(UniverseMatch::Symbol(value.to_string()));
    }
    if let Some((exchange_id, instrument_id)) = value.split_once('.') {
        if instrument_id.chars().any(|ch| ch.is_ascii_digit()) {
            return Ok(UniverseMatch::Symbol(value.to_string()));
        }
        return FuturesProductCode::new(Some(exchange_id), instrument_id)
            .map(UniverseMatch::Product);
    }
    if is_known_futures_exchange(value) {
        return Ok(UniverseMatch::Exchange(value.to_string()));
    }
    FuturesProductCode::new(None, value).map(UniverseMatch::Product)
}

fn symbols_from_files(paths: &[String]) -> RelayResult<Vec<String>> {
    let mut symbols = Vec::new();
    for path in paths {
        let contents = std::fs::read_to_string(path).map_err(|err| {
            RelayError::invalid_config(format!(
                "failed to read futures universe file {path}: {err}"
            ))
        })?;
        symbols.extend(parse_configured_symbols(&contents)?);
    }
    Ok(symbols)
}

fn parse_configured_symbols(contents: &str) -> RelayResult<Vec<String>> {
    contents
        .lines()
        .flat_map(|line| line.split(','))
        .map(str::trim)
        .map(|symbol| {
            if symbol.is_empty() {
                return Err(RelayError::invalid_config(
                    "futures universe file must not contain empty symbols",
                ));
            }
            Ok(symbol.to_string())
        })
        .collect()
}

pub(crate) fn contract_from_configured_symbol(symbol: &str) -> RelayResult<FuturesContract> {
    if let Some((continuous_prefix, product)) = symbol.split_once('@')
        && continuous_prefix.starts_with("KQ.")
    {
        let (exchange_id, product_id) = product.split_once('.').ok_or_else(|| {
            RelayError::invalid_config("continuous futures symbol must be KQ.*@EX.product")
        })?;
        let mut contract = FuturesContract::new(symbol, exchange_id, product_id, false)?;
        contract.instrument_name = continuous_contract_display_name(symbol);
        return Ok(contract);
    }
    FuturesContract::from_symbol(symbol, false)
}

fn is_known_futures_exchange(value: &str) -> bool {
    matches!(
        value,
        "CFFEX" | "SHFE" | "DCE" | "CZCE" | "INE" | "GFEX" | "KQD"
    )
}

async fn resolve_active_contracts<R>(
    contracts: Vec<FuturesContract>,
    matches: impl Fn(&FuturesContract) -> bool,
    selection: ProductSelection,
    resolver: &mut R,
) -> RelayResult<Vec<FuturesContract>>
where
    R: FuturesUniverseResolver + Send,
{
    let Some(limit_per_product) = selection.limit_per_product else {
        return active_contracts(contracts, matches);
    };
    let main_symbols = resolver
        .main_futures()
        .await?
        .into_iter()
        .collect::<BTreeSet<_>>();
    if limit_per_product == 1 {
        return main_active_contracts(contracts, matches, &main_symbols);
    }
    let candidate_symbols = contracts
        .iter()
        .filter(|contract| !contract.expired && matches(contract))
        .map(|contract| contract.symbol.clone())
        .collect::<Vec<_>>();
    let quote_snapshots = resolver.quote_snapshots(&candidate_symbols).await?;
    active_contracts_by_activity(
        contracts,
        matches,
        limit_per_product,
        &main_symbols,
        &quote_snapshots,
    )
}

fn active_contracts(
    contracts: Vec<FuturesContract>,
    matches: impl Fn(&FuturesContract) -> bool,
) -> RelayResult<Vec<FuturesContract>> {
    let mut selected = BTreeMap::new();
    for contract in contracts {
        if !contract.expired && matches(&contract) {
            selected.insert(contract.symbol.clone(), contract);
        }
    }
    Ok(selected.into_values().collect())
}

fn main_active_contracts(
    contracts: Vec<FuturesContract>,
    matches: impl Fn(&FuturesContract) -> bool,
    main_symbols: &BTreeSet<String>,
) -> RelayResult<Vec<FuturesContract>> {
    let mut selected = BTreeMap::new();
    for contract in contracts {
        if !contract.expired && matches(&contract) && main_symbols.contains(&contract.symbol) {
            selected.insert(contract.symbol.clone(), contract);
        }
    }
    Ok(selected.into_values().collect())
}

#[derive(Debug, Clone, Copy, Default)]
struct ActivityMetrics {
    open_interest: i64,
    volume: i64,
}

fn active_contracts_by_activity(
    contracts: Vec<FuturesContract>,
    matches: impl Fn(&FuturesContract) -> bool,
    limit_per_product: usize,
    main_symbols: &BTreeSet<String>,
    quote_snapshots: &[Quote],
) -> RelayResult<Vec<FuturesContract>> {
    if limit_per_product == 0 {
        return Err(RelayError::invalid_config(
            "top selector limit must be greater than zero",
        ));
    }
    let quote_metrics = quote_snapshots
        .iter()
        .map(|quote| {
            (
                quote.instrument_id.clone(),
                ActivityMetrics {
                    open_interest: quote.open_interest,
                    volume: quote.volume,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut groups: BTreeMap<(String, String), Vec<FuturesContract>> = BTreeMap::new();
    for contract in contracts {
        if !contract.expired && matches(&contract) {
            groups
                .entry((contract.exchange_id.clone(), contract.product_id.clone()))
                .or_default()
                .push(contract);
        }
    }

    let mut selected_by_symbol = BTreeMap::new();
    for contracts in groups.values_mut() {
        contracts.sort_by(|left, right| compare_activity(left, right, &quote_metrics));
        let main_contract = contracts
            .iter()
            .find(|contract| main_symbols.contains(&contract.symbol))
            .cloned();
        let mut selected = 0usize;
        if let Some(contract) = main_contract.as_ref() {
            selected_by_symbol.insert(contract.symbol.clone(), contract.clone());
            selected += 1;
        }
        for contract in contracts {
            if selected >= limit_per_product {
                break;
            }
            if main_contract
                .as_ref()
                .is_some_and(|main| main.symbol == contract.symbol)
            {
                continue;
            }
            selected_by_symbol.insert(contract.symbol.clone(), contract.clone());
            selected += 1;
        }
    }
    Ok(selected_by_symbol.into_values().collect())
}

fn compare_activity(
    left: &FuturesContract,
    right: &FuturesContract,
    quote_metrics: &BTreeMap<String, ActivityMetrics>,
) -> Ordering {
    let left_metrics = quote_metrics.get(&left.symbol).copied().unwrap_or_default();
    let right_metrics = quote_metrics
        .get(&right.symbol)
        .copied()
        .unwrap_or_default();
    right_metrics
        .open_interest
        .cmp(&left_metrics.open_interest)
        .then_with(|| right_metrics.volume.cmp(&left_metrics.volume))
        .then_with(|| left.symbol.cmp(&right.symbol))
}

#[cfg(feature = "metadata")]
async fn wait_for_quote_snapshots(
    client: &SessionClient,
    symbols: &[String],
    timeout: Duration,
) -> RelayResult<Vec<Quote>> {
    let reader = client.reader().clone();
    let mut cursor = reader.cursor();
    let deadline = Instant::now() + timeout;
    let mut pending = symbols.iter().cloned().collect::<BTreeSet<_>>();
    let mut snapshots = BTreeMap::new();

    loop {
        collect_available_quotes(&reader, &mut pending, &mut snapshots)?;
        while reader.next(&mut cursor).is_some() {
            collect_available_quotes(&reader, &mut pending, &mut snapshots)?;
        }
        if pending.is_empty() || Instant::now() >= deadline {
            return Ok(snapshots.into_values().collect());
        }
        let now = Instant::now();
        let route_deadline = (now + FUTURES_ACTIVITY_QUOTE_POLL).min(deadline);
        let progress = client
            .progress_once(Some(route_deadline))
            .await
            .map_err(|err| {
                RelayError::Transport(format!("futures quote bootstrap failed: {err}"))
            })?;
        if !progress.is_progress() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

#[cfg(feature = "metadata")]
fn collect_available_quotes(
    reader: &RuntimeReader,
    pending: &mut BTreeSet<String>,
    snapshots: &mut BTreeMap<String, Quote>,
) -> RelayResult<()> {
    let guard = reader.read();
    let mut received = Vec::new();
    for symbol in pending.iter() {
        if let Some(quote) = guard
            .decode_path::<Quote>(&["quotes", symbol.as_str()])
            .map_err(|err| RelayError::Internal(err.to_string()))?
            && (!quote.datetime.is_empty() || quote.open_interest != 0 || quote.volume != 0)
        {
            snapshots.insert(symbol.clone(), quote);
            received.push(symbol.clone());
        }
    }
    drop(guard);
    for symbol in received {
        pending.remove(&symbol);
    }
    Ok(())
}

#[cfg(all(test, feature = "metadata"))]
mod tests {
    use tqsdk_core::MarketSessionTarget;
    use tqsdk_core::{Symbol, TradingTime};
    use tqsdk_session::{InstrumentClass, SymbolInfo};

    use super::{futures_contracts_from_symbol_info, session_client_builder_for_futures_discovery};

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
    fn maps_symbol_info_to_futures_contracts_with_trading_time() {
        let contracts = futures_contracts_from_symbol_info(vec![
            symbol_info(
                "SHFE.au2602",
                "沪金2602",
                InstrumentClass::Future,
                false,
                trading_time(&[("09:00:00", "10:15:00")], &[("21:00:00", "02:30:00")]),
            ),
            symbol_info(
                "SSE.600000",
                "浦发银行",
                InstrumentClass::Stock,
                false,
                TradingTime::default(),
            ),
            symbol_info(
                "DCE.m2609",
                "豆粕2609",
                InstrumentClass::Future,
                true,
                trading_time(&[("09:00:00", "10:15:00")], &[]),
            ),
        ])
        .unwrap();

        assert_eq!(contracts.len(), 2);
        assert_eq!(contracts[0].symbol, "SHFE.au2602");
        assert_eq!(contracts[0].exchange_id, "SHFE");
        assert_eq!(contracts[0].product_id, "au");
        assert_eq!(contracts[0].instrument_name.as_deref(), Some("沪金2602"));
        assert!(!contracts[0].expired);
        assert_eq!(contracts[0].trading_time.night[0][1], "02:30:00");
        assert_eq!(contracts[1].symbol, "DCE.m2609");
        assert_eq!(contracts[1].exchange_id, "DCE");
        assert_eq!(contracts[1].product_id, "m");
        assert_eq!(contracts[1].instrument_name.as_deref(), Some("豆粕2609"));
        assert!(contracts[1].expired);
    }

    fn symbol_info(
        symbol: &str,
        instrument_name: &str,
        class: InstrumentClass,
        expired: bool,
        trading_time: TradingTime,
    ) -> SymbolInfo {
        let (exchange_id, product_id) = symbol
            .split_once('.')
            .map(|(exchange_id, instrument_id)| {
                let product_id = instrument_id
                    .chars()
                    .take_while(|ch| !ch.is_ascii_digit())
                    .collect::<String>();
                (exchange_id.to_string(), product_id)
            })
            .unwrap();
        SymbolInfo {
            instrument_id: Symbol::new(symbol),
            instrument_name: instrument_name.to_string(),
            exchange_id,
            product_id,
            ins_class: String::new(),
            class,
            price_tick: None,
            volume_multiple: None,
            open_limit: None,
            max_limit_order_volume: None,
            max_market_order_volume: None,
            min_limit_order_volume: None,
            min_market_order_volume: None,
            open_max_market_order_volume: None,
            open_max_limit_order_volume: None,
            open_min_market_order_volume: None,
            open_min_limit_order_volume: None,
            underlying_symbol: None,
            strike_price: None,
            expired,
            expire_datetime_secs: None,
            expire_rest_days: None,
            delivery_year: None,
            delivery_month: None,
            last_exercise_datetime_secs: None,
            exercise_year: None,
            exercise_month: None,
            option_class: None,
            upper_limit: None,
            lower_limit: None,
            pre_settlement: None,
            pre_open_interest: None,
            pre_close: None,
            trading_time,
        }
    }

    fn trading_time(day: &[(&str, &str)], night: &[(&str, &str)]) -> TradingTime {
        TradingTime {
            day: day
                .iter()
                .map(|(start, end)| vec![(*start).to_string(), (*end).to_string()])
                .collect(),
            night: night
                .iter()
                .map(|(start, end)| vec![(*start).to_string(), (*end).to_string()])
                .collect(),
        }
    }
}
