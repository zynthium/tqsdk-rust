#![cfg_attr(not(test), forbid(unsafe_code))]

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use tokio::time::Instant;
use tqsdk_core::{Quote, RuntimeReader, TradingTime};
use tqsdk_session::{InstrumentClass, SessionClient, SessionClientBuilder, SymbolInfo};

use crate::error::{DataError, Result};
use crate::{UniverseClause, UniverseExpression, UniverseSelector, UniverseSelectorKind};

pub const DEFAULT_FUTURES_METADATA_BATCH_SIZE: usize = 500;

const FUTURES_ACTIVITY_QUOTE_TIMEOUT: Duration = Duration::from_secs(30);
const FUTURES_ACTIVITY_QUOTE_POLL: Duration = Duration::from_millis(250);
const SUPPORTED_FUTURES_UNIVERSE_EXCHANGES: &[&str] =
    &["CFFEX", "SHFE", "DCE", "CZCE", "INE", "GFEX"];

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProductScope {
    All,
    Products(Vec<FuturesProductCode>),
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
    pub fn new(exchange_id: Option<&str>, product_id: impl Into<String>) -> Result<Self> {
        let exchange_id = exchange_id.map(|value| value.trim().to_string());
        let product_id = product_id.into().trim().to_string();
        if exchange_id.as_deref().is_some_and(str::is_empty) {
            return Err(invalid_universe(
                "futures product exchange_id must not be empty",
            ));
        }
        if product_id.is_empty() {
            return Err(invalid_universe("futures product_id must not be empty"));
        }
        Ok(Self {
            exchange_id,
            product_id,
        })
    }

    pub fn parse(value: &str) -> Result<Self> {
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
    ) -> Result<Self> {
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
    ) -> Result<Self> {
        let symbol = symbol.into().trim().to_string();
        let exchange_id = exchange_id.into().trim().to_string();
        let product_id = product_id.into().trim().to_string();
        if symbol.is_empty() {
            return Err(invalid_universe(
                "futures contract symbol must not be empty",
            ));
        }
        if exchange_id.is_empty() {
            return Err(invalid_universe(
                "futures contract exchange_id must not be empty",
            ));
        }
        if product_id.is_empty() {
            return Err(invalid_universe(
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

    pub fn from_symbol(symbol: impl Into<String>, expired: bool) -> Result<Self> {
        let symbol = symbol.into();
        let (exchange_id, instrument_id) = symbol
            .split_once('.')
            .ok_or_else(|| invalid_universe("futures symbol must include exchange"))?;
        let product_id: String = instrument_id
            .chars()
            .take_while(|ch| !ch.is_ascii_digit())
            .collect();
        Self::new(symbol.clone(), exchange_id, product_id, expired)
    }

    pub fn from_quote(quote: &Quote) -> Result<Self> {
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

    pub fn from_symbol_info(info: SymbolInfo) -> Result<Self> {
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
    ) -> impl std::future::Future<Output = Result<Vec<FuturesContract>>> + Send + '_;

    fn main_futures(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Vec<String>>> + Send + '_ {
        std::future::ready(Ok(Vec::new()))
    }

    fn quote_snapshots<'a>(
        &'a mut self,
        _symbols: &'a [String],
    ) -> impl std::future::Future<Output = Result<Vec<Quote>>> + Send + 'a {
        std::future::ready(Ok(Vec::new()))
    }

    fn trading_calendar(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Vec<tqsdk_core::TradingCalendarDay>>> + Send + '_
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

    #[must_use]
    pub fn with_main_symbols<I, S>(mut self, symbols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.main_symbols = symbols.into_iter().map(Into::into).collect();
        self
    }

    #[must_use]
    pub fn with_quote_snapshots<I>(mut self, quotes: I) -> Self
    where
        I: IntoIterator<Item = Quote>,
    {
        self.quote_snapshots = quotes.into_iter().collect();
        self
    }
}

impl FuturesUniverseResolver for StaticFuturesUniverseResolver {
    async fn active_futures(&mut self) -> Result<Vec<FuturesContract>> {
        Ok(self.contracts.clone())
    }

    async fn main_futures(&mut self) -> Result<Vec<String>> {
        Ok(self.main_symbols.clone())
    }

    async fn quote_snapshots(&mut self, _symbols: &[String]) -> Result<Vec<Quote>> {
        Ok(self.quote_snapshots.clone())
    }

    async fn trading_calendar(&mut self) -> Result<Vec<tqsdk_core::TradingCalendarDay>> {
        Ok(self.trading_calendar.clone())
    }
}

pub struct SessionFuturesUniverseResolver {
    client: SessionClient,
    activity_client: Option<SessionClient>,
    metadata_batch_size: usize,
    activity_quote_timeout: Duration,
}

impl SessionFuturesUniverseResolver {
    pub fn new(client: SessionClient) -> Self {
        Self::new_with_metadata_batch_size(client, DEFAULT_FUTURES_METADATA_BATCH_SIZE)
            .expect("default futures metadata batch size is non-zero")
    }

    pub fn new_with_metadata_batch_size(
        client: SessionClient,
        metadata_batch_size: usize,
    ) -> Result<Self> {
        if metadata_batch_size == 0 {
            return Err(invalid_universe(
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

    #[must_use]
    pub fn with_activity_client(mut self, activity_client: SessionClient) -> Self {
        self.activity_client = Some(activity_client);
        self
    }
}

pub fn session_client_builder_for_futures_discovery(
    user: &str,
    pass: &str,
) -> SessionClientBuilder {
    SessionClientBuilder::new(user, pass)
        .enable_query()
        .stock_market()
}

impl FuturesUniverseResolver for SessionFuturesUniverseResolver {
    async fn active_futures(&mut self) -> Result<Vec<FuturesContract>> {
        let symbols = self
            .client
            .query_quotes(Some("FUTURE"), None, None, Some(false), None)
            .await?;
        if symbols.is_empty() {
            return Ok(Vec::new());
        }
        let mut contracts = Vec::new();
        for batch in futures_metadata_symbol_batches(&symbols, self.metadata_batch_size)? {
            let infos = self.client.query_symbol_info(&batch).await?;
            contracts.extend(futures_contracts_from_symbol_info(infos)?);
        }
        Ok(contracts)
    }

    async fn main_futures(&mut self) -> Result<Vec<String>> {
        self.client
            .query_cont_quotes(None, None, None)
            .await
            .map_err(Into::into)
    }

    async fn quote_snapshots(&mut self, symbols: &[String]) -> Result<Vec<Quote>> {
        if symbols.is_empty() {
            return Ok(Vec::new());
        }
        let Some(client) = self.activity_client.as_ref() else {
            return Err(invalid_universe(
                "futures activity ranking requires a futures market session",
            ));
        };
        let lease = client
            .ensure_quotes(symbols.iter().map(String::as_str))
            .await?;
        let snapshots =
            wait_for_quote_snapshots(client, symbols, self.activity_quote_timeout).await;
        let _ = lease.close().await;
        snapshots
    }

    async fn trading_calendar(&mut self) -> Result<Vec<tqsdk_core::TradingCalendarDay>> {
        #[cfg(not(feature = "services"))]
        {
            Ok(Vec::new())
        }
        #[cfg(feature = "services")]
        {
            let now = chrono::Utc::now()
                .with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap())
                .date_naive();
            let start_dt = now - chrono::Days::new(14);
            let end_dt = now + chrono::Days::new(14);
            self.client
                .get_trading_calendar(start_dt, end_dt)
                .await
                .map_err(Into::into)
        }
    }
}

pub fn futures_contracts_from_symbol_info(infos: Vec<SymbolInfo>) -> Result<Vec<FuturesContract>> {
    infos
        .into_iter()
        .filter(|info| info.class == InstrumentClass::Future)
        .map(FuturesContract::from_symbol_info)
        .collect()
}

pub fn futures_metadata_symbol_batches(
    symbols: &[String],
    batch_size: usize,
) -> Result<Vec<Vec<&str>>> {
    if batch_size == 0 {
        return Err(invalid_universe(
            "futures metadata batch size must be greater than zero",
        ));
    }
    Ok(symbols
        .chunks(batch_size)
        .map(|chunk| chunk.iter().map(String::as_str).collect())
        .collect())
}

#[must_use]
pub fn expression_requires_activity_quotes(expression: &UniverseExpression) -> bool {
    expression.clauses().iter().any(
        |clause| matches!(clause.selector().kind(), UniverseSelectorKind::Top(limit) if limit > 1),
    )
}

pub async fn resolve_futures_universe_symbols<R>(
    expression: &UniverseExpression,
    resolver: &mut R,
) -> Result<Vec<String>>
where
    R: FuturesUniverseResolver + Send,
{
    let contracts = resolve_futures_contracts_with_expression(expression, resolver).await?;
    Ok(contracts
        .into_iter()
        .map(|contract| contract.symbol)
        .collect())
}

pub fn resolve_static_symbols_with_expression(
    expression: &UniverseExpression,
) -> Result<Vec<String>> {
    Ok(static_contracts_with_expression(expression)?
        .into_iter()
        .map(|contract| contract.symbol)
        .collect())
}

pub fn static_contracts_with_expression(
    expression: &UniverseExpression,
) -> Result<Vec<FuturesContract>> {
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
                return Err(invalid_universe(
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
) -> Result<Vec<FuturesContract>>
where
    R: FuturesUniverseResolver + Send,
{
    let mut resolver = CachedActiveFuturesResolver::new(resolver);
    let mut included = BTreeMap::<String, FuturesContract>::new();
    let mut exclusions = Vec::<UniverseMatch>::new();
    for clause in expression.clauses() {
        if clause.exclude() {
            exclusions.extend(matches_for_clause(clause)?);
            continue;
        }
        for contract in contracts_for_selector(clause.selector(), &mut resolver).await? {
            included.insert(contract.symbol.clone(), contract);
        }
    }
    for exclusion in exclusions {
        included.retain(|_, contract| !exclusion.matches(contract));
    }
    retain_supported_futures_universe_contracts(&mut included);
    Ok(included.into_values().collect())
}

struct CachedActiveFuturesResolver<'a, R> {
    inner: &'a mut R,
    active_futures: Option<Vec<FuturesContract>>,
}

impl<'a, R> CachedActiveFuturesResolver<'a, R> {
    fn new(inner: &'a mut R) -> Self {
        Self {
            inner,
            active_futures: None,
        }
    }
}

impl<R> FuturesUniverseResolver for CachedActiveFuturesResolver<'_, R>
where
    R: FuturesUniverseResolver + Send,
{
    async fn active_futures(&mut self) -> Result<Vec<FuturesContract>> {
        if let Some(contracts) = self.active_futures.as_ref() {
            return Ok(contracts.clone());
        }
        let contracts = self.inner.active_futures().await?;
        self.active_futures = Some(contracts.clone());
        Ok(contracts)
    }

    async fn main_futures(&mut self) -> Result<Vec<String>> {
        self.inner.main_futures().await
    }

    async fn quote_snapshots(&mut self, symbols: &[String]) -> Result<Vec<Quote>> {
        self.inner.quote_snapshots(symbols).await
    }

    async fn trading_calendar(&mut self) -> Result<Vec<tqsdk_core::TradingCalendarDay>> {
        self.inner.trading_calendar().await
    }
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
) -> Result<Vec<FuturesContract>>
where
    R: FuturesUniverseResolver + Send,
{
    if selection.limit_per_product == Some(0) {
        return Err(invalid_universe(
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
                return Err(invalid_universe(
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
) -> Result<Vec<FuturesContract>>
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
) -> Result<Vec<FuturesContract>>
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
        if let Some(product_name) = contract
            .instrument_name
            .as_deref()
            .and_then(product_name_from_instrument_name)
            .or_else(|| futures_product_chinese_name(&key.0, &key.1).map(str::to_string))
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

fn product_scope_from_values(values: &[String]) -> Result<ProductScope> {
    if values.len() == 1 && values[0].eq_ignore_ascii_case("all") {
        return Ok(ProductScope::All);
    }
    values
        .iter()
        .map(|value| FuturesProductCode::parse(value))
        .collect::<Result<Vec<_>>>()
        .map(ProductScope::Products)
}

fn matches_for_clause(clause: &UniverseClause) -> Result<Vec<UniverseMatch>> {
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

fn classify_universe_token(value: &str) -> Result<UniverseMatch> {
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

fn symbols_from_files(paths: &[String]) -> Result<Vec<String>> {
    let mut symbols = Vec::new();
    for path in paths {
        let contents = std::fs::read_to_string(path).map_err(|err| {
            invalid_universe(format!(
                "failed to read futures universe file {path}: {err}"
            ))
        })?;
        symbols.extend(parse_configured_symbols(&contents)?);
    }
    Ok(symbols)
}

fn parse_configured_symbols(contents: &str) -> Result<Vec<String>> {
    contents
        .lines()
        .flat_map(|line| line.split(','))
        .map(str::trim)
        .map(|symbol| {
            if symbol.is_empty() {
                return Err(invalid_universe(
                    "futures universe file must not contain empty symbols",
                ));
            }
            Ok(symbol.to_string())
        })
        .collect()
}

pub fn contract_from_configured_symbol(symbol: &str) -> Result<FuturesContract> {
    if let Some((continuous_prefix, product)) = symbol.split_once('@')
        && continuous_prefix.starts_with("KQ.")
    {
        let (exchange_id, product_id) = product
            .split_once('.')
            .ok_or_else(|| invalid_universe("continuous futures symbol must be KQ.*@EX.product"))?;
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
) -> Result<Vec<FuturesContract>>
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
) -> Result<Vec<FuturesContract>> {
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
) -> Result<Vec<FuturesContract>> {
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
) -> Result<Vec<FuturesContract>> {
    if limit_per_product == 0 {
        return Err(invalid_universe(
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

async fn wait_for_quote_snapshots(
    client: &SessionClient,
    symbols: &[String],
    timeout: Duration,
) -> Result<Vec<Quote>> {
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
        let progress = client.progress_once(Some(route_deadline)).await?;
        if !progress.is_progress() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

fn collect_available_quotes(
    reader: &RuntimeReader,
    pending: &mut BTreeSet<String>,
    snapshots: &mut BTreeMap<String, Quote>,
) -> Result<()> {
    let guard = reader.read();
    let mut received = Vec::new();
    for symbol in pending.iter() {
        if let Some(quote) = guard
            .decode_path::<Quote>(&["quotes", symbol.as_str()])
            .map_err(|err| DataError::InvalidResponse(err.to_string()))?
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

fn supports_index_continuous_contract(exchange_id: &str) -> bool {
    !exchange_id.eq_ignore_ascii_case("KQD")
}

fn continuous_contract_display_name(symbol: &str) -> Option<String> {
    let (prefix, underlying) = symbol.split_once('@')?;
    let (exchange_id, product_id) = underlying.split_once('.')?;
    let product_name = futures_product_chinese_name(exchange_id, product_id)?;
    continuous_contract_display_name_from_product_name(prefix, product_name)
}

fn continuous_contract_display_name_from_product_name(
    prefix: &str,
    product_name: &str,
) -> Option<String> {
    let suffix = match prefix {
        "KQ.m" => "主连",
        "KQ.i" => "加权",
        _ => return None,
    };
    let product_name = product_name.trim();
    (!product_name.is_empty()).then(|| format!("{product_name}{suffix}"))
}

fn product_name_from_instrument_name(instrument_name: &str) -> Option<String> {
    let trimmed = instrument_name.trim();
    if trimmed.is_empty() {
        return None;
    }
    let product_name = trimmed.trim_end_matches(|ch: char| ch.is_ascii_digit());
    (!product_name.is_empty()).then(|| product_name.to_string())
}

fn futures_product_chinese_name(exchange_id: &str, product_id: &str) -> Option<&'static str> {
    match (
        exchange_id.to_ascii_uppercase().as_str(),
        product_id.to_ascii_lowercase().as_str(),
    ) {
        ("CFFEX", "if") => Some("沪深300"),
        ("CFFEX", "ih") => Some("上证50"),
        ("CFFEX", "ic") => Some("中证500"),
        ("CFFEX", "im") => Some("中证1000"),
        ("SHFE", "ao") => Some("氧化铝"),
        ("SHFE", "au") => Some("沪金"),
        ("SHFE", "rb") => Some("螺纹钢"),
        ("DCE", "i") => Some("铁矿石"),
        ("DCE", "m") => Some("豆粕"),
        ("GFEX", "lc") => Some("碳酸锂"),
        ("GFEX", "si") => Some("工业硅"),
        _ => None,
    }
}

fn invalid_universe(message: impl Into<String>) -> DataError {
    DataError::Validation(message.into())
}
