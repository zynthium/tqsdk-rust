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

#[cfg(feature = "metadata")]
const FUTURES_ACTIVITY_QUOTE_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(feature = "metadata")]
const FUTURES_ACTIVITY_QUOTE_POLL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FuturesUniverseSelection {
    pub active_contracts_per_product: Option<usize>,
}

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

#[derive(Debug, Clone, PartialEq)]
pub struct FuturesContract {
    pub symbol: String,
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
        Self::new_with_trading_time(
            quote.instrument_id.clone(),
            quote.exchange_id.clone(),
            quote.product_id.clone(),
            quote.expired,
            quote.trading_time.clone(),
        )
    }

    #[cfg(feature = "metadata")]
    pub fn from_symbol_info(info: SymbolInfo) -> RelayResult<Self> {
        Self::new_with_trading_time(
            info.instrument_id.to_string(),
            info.exchange_id,
            info.product_id,
            info.expired,
            info.trading_time,
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
}

#[derive(Debug, Clone)]
pub struct StaticFuturesUniverseResolver {
    contracts: Vec<FuturesContract>,
    main_symbols: Vec<String>,
    quote_snapshots: Vec<Quote>,
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
            .futures_active_contracts_per_product
            .is_some_and(|contracts| contracts > 1)
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
}

#[cfg(feature = "metadata")]
fn futures_contracts_from_symbol_info(infos: Vec<SymbolInfo>) -> RelayResult<Vec<FuturesContract>> {
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

pub async fn resolve_futures_symbols<R>(
    filter: &FuturesProductFilter,
    resolver: &mut R,
) -> RelayResult<Vec<String>>
where
    R: FuturesUniverseResolver + Send,
{
    resolve_futures_symbols_with_selection(filter, FuturesUniverseSelection::default(), resolver)
        .await
}

pub async fn resolve_futures_symbols_with_selection<R>(
    filter: &FuturesProductFilter,
    selection: FuturesUniverseSelection,
    resolver: &mut R,
) -> RelayResult<Vec<String>>
where
    R: FuturesUniverseResolver + Send,
{
    let contracts = resolve_futures_contracts_with_selection(filter, selection, resolver).await?;
    Ok(contracts
        .into_iter()
        .map(|contract| contract.symbol)
        .collect())
}

pub async fn resolve_futures_contracts_with_selection<R>(
    filter: &FuturesProductFilter,
    selection: FuturesUniverseSelection,
    resolver: &mut R,
) -> RelayResult<Vec<FuturesContract>>
where
    R: FuturesUniverseResolver + Send,
{
    if selection.active_contracts_per_product == Some(0) {
        return Err(RelayError::invalid_config(
            "active_contracts_per_product must be greater than zero",
        ));
    }
    match filter {
        FuturesProductFilter::None => Ok(Vec::new()),
        FuturesProductFilter::All => {
            let contracts = resolver.active_futures().await?;
            resolve_active_contracts(contracts, |_| true, selection, resolver).await
        }
        FuturesProductFilter::Products(products) => {
            if products.is_empty() {
                return Err(RelayError::invalid_config(
                    "futures product filter must not be empty",
                ));
            }
            let products = products.clone();
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

async fn resolve_active_contracts<R>(
    contracts: Vec<FuturesContract>,
    matches: impl Fn(&FuturesContract) -> bool,
    selection: FuturesUniverseSelection,
    resolver: &mut R,
) -> RelayResult<Vec<FuturesContract>>
where
    R: FuturesUniverseResolver + Send,
{
    let Some(active_contracts_per_product) = selection.active_contracts_per_product else {
        return active_contracts(contracts, matches);
    };
    let main_symbols = resolver
        .main_futures()
        .await?
        .into_iter()
        .collect::<BTreeSet<_>>();
    if active_contracts_per_product == 1 {
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
        active_contracts_per_product,
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
    active_contracts_per_product: usize,
    main_symbols: &BTreeSet<String>,
    quote_snapshots: &[Quote],
) -> RelayResult<Vec<FuturesContract>> {
    if active_contracts_per_product == 0 {
        return Err(RelayError::invalid_config(
            "active_contracts_per_product must be greater than zero",
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
            if selected >= active_contracts_per_product {
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
                InstrumentClass::Future,
                false,
                trading_time(&[("09:00:00", "10:15:00")], &[("21:00:00", "02:30:00")]),
            ),
            symbol_info(
                "SSE.600000",
                InstrumentClass::Stock,
                false,
                TradingTime::default(),
            ),
            symbol_info(
                "DCE.m2609",
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
        assert!(!contracts[0].expired);
        assert_eq!(contracts[0].trading_time.night[0][1], "02:30:00");
        assert_eq!(contracts[1].symbol, "DCE.m2609");
        assert_eq!(contracts[1].exchange_id, "DCE");
        assert_eq!(contracts[1].product_id, "m");
        assert!(contracts[1].expired);
    }

    fn symbol_info(
        symbol: &str,
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
            instrument_name: String::new(),
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
