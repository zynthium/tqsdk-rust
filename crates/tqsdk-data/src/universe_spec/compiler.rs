use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use super::{
    ExpandedUniverseInput, UniverseMode, UniverseSelectorSpec, UniverseSpec, UniverseTarget,
    UniverseView,
};

/// Product identity passed to snapshot capability adapters.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub struct UniverseProduct {
    exchange: String,
    product: String,
}

impl UniverseProduct {
    #[must_use]
    pub fn new(exchange: impl Into<String>, product: impl Into<String>) -> Self {
        Self {
            exchange: exchange.into().to_ascii_uppercase(),
            product: product.into(),
        }
    }

    #[must_use]
    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    #[must_use]
    pub fn product(&self) -> &str {
        &self.product
    }
}

/// Current physical contract metadata consumed by the pure snapshot compiler.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SnapshotContract {
    symbol: String,
    exchange: String,
    contract: String,
    product: String,
    eligible: bool,
    expired: bool,
}

impl SnapshotContract {
    #[must_use]
    pub fn new(
        exchange: impl Into<String>,
        contract: impl Into<String>,
        product: impl Into<String>,
    ) -> Self {
        let exchange = exchange.into().to_ascii_uppercase();
        let contract = contract.into();
        Self {
            symbol: format!("{exchange}.{contract}"),
            exchange,
            contract,
            product: product.into(),
            eligible: true,
            expired: false,
        }
    }

    #[must_use]
    pub const fn eligible(mut self, eligible: bool) -> Self {
        self.eligible = eligible;
        self
    }

    #[must_use]
    pub const fn expired(mut self, expired: bool) -> Self {
        self.expired = expired;
        self
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    #[must_use]
    pub fn contract(&self) -> &str {
        &self.contract
    }

    #[must_use]
    pub fn product(&self) -> &str {
        &self.product
    }

    #[must_use]
    pub const fn is_eligible(&self) -> bool {
        self.eligible
    }

    #[must_use]
    pub const fn is_expired(&self) -> bool {
        self.expired
    }

    fn product_key(&self) -> UniverseProduct {
        UniverseProduct::new(&self.exchange, &self.product)
    }
}

/// Final instrument classification used by exclusions and dependency closure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CompiledUniverseInstrumentKind {
    PhysicalContract,
    Continuous,
    Index,
    ExplicitSymbol,
}

/// Optional adapter classification for an exact provider symbol.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct UniverseSymbolClass {
    kind: CompiledUniverseInstrumentKind,
    exchange: String,
    product: String,
}

impl UniverseSymbolClass {
    #[must_use]
    pub fn physical(exchange: impl Into<String>, product: impl Into<String>) -> Self {
        Self::new(
            CompiledUniverseInstrumentKind::PhysicalContract,
            exchange,
            product,
        )
    }

    #[must_use]
    pub fn continuous(exchange: impl Into<String>, product: impl Into<String>) -> Self {
        Self::new(
            CompiledUniverseInstrumentKind::Continuous,
            exchange,
            product,
        )
    }

    #[must_use]
    pub fn index(exchange: impl Into<String>, product: impl Into<String>) -> Self {
        Self::new(CompiledUniverseInstrumentKind::Index, exchange, product)
    }

    fn new(
        kind: CompiledUniverseInstrumentKind,
        exchange: impl Into<String>,
        product: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            exchange: exchange.into().to_ascii_uppercase(),
            product: product.into(),
        }
    }
}

/// Current-data adapter required by the V2 snapshot compiler.
pub trait SnapshotCapabilities {
    type Error: Error + Send + Sync + 'static;

    fn current_contracts(&self) -> Result<Vec<SnapshotContract>, Self::Error>;

    fn main_contract(&self, product: &UniverseProduct) -> Result<Option<String>, Self::Error>;

    fn top_contracts(
        &self,
        product: &UniverseProduct,
        limit: u32,
    ) -> Result<Vec<String>, Self::Error>;

    fn continuous_symbol(&self, product: &UniverseProduct) -> Result<Option<String>, Self::Error>;

    fn index_symbol(&self, product: &UniverseProduct) -> Result<Option<String>, Self::Error>;

    fn classify_symbol(&self, symbol: &str) -> Result<Option<UniverseSymbolClass>, Self::Error>;
}

/// One final visible symbol and every selector provenance that kept it alive.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CompiledUniverseCandidate {
    symbol: String,
    kind: CompiledUniverseInstrumentKind,
    provenance: Vec<UniverseView>,
    exchange: Option<String>,
    product: Option<String>,
}

impl CompiledUniverseCandidate {
    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub const fn kind(&self) -> CompiledUniverseInstrumentKind {
        self.kind
    }

    #[must_use]
    pub fn provenance(&self) -> &[UniverseView] {
        &self.provenance
    }

    #[must_use]
    pub fn exchange(&self) -> Option<&str> {
        self.exchange.as_deref()
    }

    #[must_use]
    pub fn product(&self) -> Option<&str> {
        self.product.as_deref()
    }
}

/// Pure, deterministic result of V2 universe compilation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CompiledUniverse {
    mode: UniverseMode,
    candidates: Vec<CompiledUniverseCandidate>,
    physical_dependencies: Vec<String>,
}

impl CompiledUniverse {
    #[must_use]
    pub const fn mode(&self) -> UniverseMode {
        self.mode
    }

    #[must_use]
    pub fn candidates(&self) -> &[CompiledUniverseCandidate] {
        &self.candidates
    }

    #[must_use]
    pub fn physical_dependencies(&self) -> &[String] {
        &self.physical_dependencies
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum UniverseCompileError {
    WrongMode {
        expected: UniverseMode,
        actual: UniverseMode,
    },
    Capability {
        operation: &'static str,
        message: String,
    },
    MissingInstrument {
        view: UniverseView,
        product: UniverseProduct,
    },
    UnknownRankedContract {
        view: UniverseView,
        symbol: String,
    },
    NoCandidates,
}

impl fmt::Display for UniverseCompileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongMode { expected, actual } => write!(
                formatter,
                "Universe compiler expected {expected:?} mode, got {actual:?}"
            ),
            Self::Capability { operation, message } => {
                write!(
                    formatter,
                    "Universe capability {operation} failed: {message}"
                )
            }
            Self::MissingInstrument { view, product } => write!(
                formatter,
                "Universe capability did not provide {view} for {}.{}",
                product.exchange, product.product
            ),
            Self::UnknownRankedContract { view, symbol } => write!(
                formatter,
                "Universe {view} ranking returned unknown physical contract {symbol}"
            ),
            Self::NoCandidates => formatter.write_str("Universe V2 resolves no visible candidates"),
        }
    }
}

impl Error for UniverseCompileError {}

/// Compiles an already-normalized snapshot expression plus externally expanded symbols.
pub fn compile_snapshot_universe<C: SnapshotCapabilities>(
    spec: &UniverseSpec,
    expanded_symbols: &[String],
    capabilities: &C,
) -> Result<CompiledUniverse, UniverseCompileError> {
    compile_snapshot_parts(Some(spec), expanded_symbols, capabilities)
}

/// Compiles a fully materialized [`ExpandedUniverseInput`] without performing I/O.
pub fn compile_expanded_snapshot_universe<C: SnapshotCapabilities>(
    input: &ExpandedUniverseInput,
    capabilities: &C,
) -> Result<CompiledUniverse, UniverseCompileError> {
    compile_snapshot_parts(input.spec(), input.expanded_symbols(), capabilities)
}

fn compile_snapshot_parts<C: SnapshotCapabilities>(
    spec: Option<&UniverseSpec>,
    expanded_symbols: &[String],
    capabilities: &C,
) -> Result<CompiledUniverse, UniverseCompileError> {
    if let Some(spec) = spec
        && spec.mode() != UniverseMode::Snapshot
    {
        return Err(UniverseCompileError::WrongMode {
            expected: UniverseMode::Snapshot,
            actual: spec.mode(),
        });
    }
    let contracts = capability("current_contracts", capabilities.current_contracts())?;
    let contract_by_symbol = contracts
        .iter()
        .map(|contract| (contract.symbol().to_string(), contract))
        .collect::<BTreeMap<_, _>>();
    let active_contracts = contracts
        .iter()
        .filter(|contract| contract.is_eligible() && !contract.is_expired())
        .collect::<Vec<_>>();
    let products = active_contracts
        .iter()
        .map(|contract| contract.product_key())
        .collect::<BTreeSet<_>>();
    let mut occurrences = BTreeMap::<(String, UniverseView), CandidateOccurrence>::new();

    if let Some(spec) = spec {
        for selector in spec.includes() {
            include_selector(
                selector,
                capabilities,
                &contracts,
                &contract_by_symbol,
                &products,
                &mut occurrences,
            )?;
        }
    }
    for symbol in expanded_symbols {
        include_explicit_symbol(symbol, capabilities, &mut occurrences)?;
    }

    if let Some(spec) = spec {
        apply_exclusions(&mut occurrences, spec.excludes(), spec.global_filters());
    }
    if occurrences.is_empty() {
        return Err(UniverseCompileError::NoCandidates);
    }

    let candidates = aggregate_candidates(occurrences);
    let mut physical_dependencies = BTreeSet::new();
    for candidate in &candidates {
        match candidate.kind {
            CompiledUniverseInstrumentKind::PhysicalContract => {
                physical_dependencies.insert(candidate.symbol.clone());
            }
            CompiledUniverseInstrumentKind::Continuous | CompiledUniverseInstrumentKind::Index => {
                if let (Some(exchange), Some(product)) = (&candidate.exchange, &candidate.product) {
                    physical_dependencies.extend(
                        active_contracts
                            .iter()
                            .filter(|contract| {
                                contract.exchange() == exchange && contract.product() == product
                            })
                            .map(|contract| contract.symbol().to_string()),
                    );
                }
            }
            CompiledUniverseInstrumentKind::ExplicitSymbol => {}
        }
    }
    Ok(CompiledUniverse {
        mode: UniverseMode::Snapshot,
        candidates,
        physical_dependencies: physical_dependencies.into_iter().collect(),
    })
}

fn include_selector<C: SnapshotCapabilities>(
    selector: &UniverseSelectorSpec,
    capabilities: &C,
    contracts: &[SnapshotContract],
    contract_by_symbol: &BTreeMap<String, &SnapshotContract>,
    products: &BTreeSet<UniverseProduct>,
    occurrences: &mut BTreeMap<(String, UniverseView), CandidateOccurrence>,
) -> Result<(), UniverseCompileError> {
    match selector.view() {
        UniverseView::Contract => {
            for target in selector.targets() {
                for contract in contracts.iter().filter(|contract| {
                    if matches!(target, UniverseTarget::Contract { .. }) {
                        contract_matches_target(contract, target)
                    } else {
                        contract.is_eligible()
                            && !contract.is_expired()
                            && contract_matches_target(contract, target)
                    }
                }) {
                    insert_physical(occurrences, contract, UniverseView::Contract);
                }
            }
        }
        view @ (UniverseView::Main | UniverseView::Top(_)) => {
            for product in matching_products(products, selector.targets()) {
                let symbols = match view {
                    UniverseView::Main => {
                        capability("main_contract", capabilities.main_contract(product))?
                            .into_iter()
                            .collect()
                    }
                    UniverseView::Top(limit) => {
                        capability("top_contracts", capabilities.top_contracts(product, limit))?
                    }
                    _ => unreachable!(),
                };
                if symbols.is_empty() {
                    return Err(UniverseCompileError::MissingInstrument {
                        view,
                        product: product.clone(),
                    });
                }
                for symbol in symbols {
                    let contract = contract_by_symbol.get(&symbol).copied().ok_or_else(|| {
                        UniverseCompileError::UnknownRankedContract {
                            view,
                            symbol: symbol.clone(),
                        }
                    })?;
                    insert_physical(occurrences, contract, view);
                }
            }
        }
        view @ (UniverseView::Continuous | UniverseView::Index) => {
            for product in matching_products(products, selector.targets()) {
                let symbol = match view {
                    UniverseView::Continuous => {
                        capability("continuous_symbol", capabilities.continuous_symbol(product))?
                    }
                    UniverseView::Index => {
                        capability("index_symbol", capabilities.index_symbol(product))?
                    }
                    _ => unreachable!(),
                }
                .ok_or_else(|| UniverseCompileError::MissingInstrument {
                    view,
                    product: product.clone(),
                })?;
                let kind = match view {
                    UniverseView::Continuous => CompiledUniverseInstrumentKind::Continuous,
                    UniverseView::Index => CompiledUniverseInstrumentKind::Index,
                    _ => unreachable!(),
                };
                insert_occurrence(
                    occurrences,
                    CandidateOccurrence {
                        symbol,
                        provenance: view,
                        kind,
                        exchange: Some(product.exchange.clone()),
                        product: Some(product.product.clone()),
                    },
                );
            }
        }
        UniverseView::Symbol => {
            for target in selector.targets() {
                let UniverseTarget::Symbol { symbol } = target else {
                    unreachable!("the V2 parser restricts symbol targets")
                };
                include_explicit_symbol(symbol, capabilities, occurrences)?;
            }
        }
    }
    Ok(())
}

fn include_explicit_symbol<C: SnapshotCapabilities>(
    symbol: &str,
    capabilities: &C,
    occurrences: &mut BTreeMap<(String, UniverseView), CandidateOccurrence>,
) -> Result<(), UniverseCompileError> {
    let classification = capability("classify_symbol", capabilities.classify_symbol(symbol))?;
    let (kind, exchange, product) = classification.map_or(
        (CompiledUniverseInstrumentKind::ExplicitSymbol, None, None),
        |classification| {
            (
                classification.kind,
                Some(classification.exchange),
                Some(classification.product),
            )
        },
    );
    insert_occurrence(
        occurrences,
        CandidateOccurrence {
            symbol: symbol.to_string(),
            provenance: UniverseView::Symbol,
            kind,
            exchange,
            product,
        },
    );
    Ok(())
}

fn insert_physical(
    occurrences: &mut BTreeMap<(String, UniverseView), CandidateOccurrence>,
    contract: &SnapshotContract,
    provenance: UniverseView,
) {
    insert_occurrence(
        occurrences,
        CandidateOccurrence {
            symbol: contract.symbol().to_string(),
            provenance,
            kind: CompiledUniverseInstrumentKind::PhysicalContract,
            exchange: Some(contract.exchange().to_string()),
            product: Some(contract.product().to_string()),
        },
    );
}

fn insert_occurrence(
    occurrences: &mut BTreeMap<(String, UniverseView), CandidateOccurrence>,
    occurrence: CandidateOccurrence,
) {
    occurrences.insert(
        (occurrence.symbol.clone(), occurrence.provenance),
        occurrence,
    );
}

fn matching_products<'a>(
    products: &'a BTreeSet<UniverseProduct>,
    targets: &[UniverseTarget],
) -> Vec<&'a UniverseProduct> {
    products
        .iter()
        .filter(|product| {
            targets
                .iter()
                .any(|target| product_matches_target(product, target))
        })
        .collect()
}

fn product_matches_target(product: &UniverseProduct, target: &UniverseTarget) -> bool {
    match target {
        UniverseTarget::All => true,
        UniverseTarget::Exchange { exchange } => product.exchange == *exchange,
        UniverseTarget::Product {
            exchange,
            product: target_product,
        } => product.exchange == *exchange && product.product == *target_product,
        UniverseTarget::Contract { .. } | UniverseTarget::Symbol { .. } => false,
    }
}

fn contract_matches_target(contract: &SnapshotContract, target: &UniverseTarget) -> bool {
    match target {
        UniverseTarget::All => true,
        UniverseTarget::Exchange { exchange } => contract.exchange() == exchange,
        UniverseTarget::Product { exchange, product } => {
            contract.exchange() == exchange && contract.product() == product
        }
        UniverseTarget::Contract {
            exchange,
            contract: target_contract,
        } => contract.exchange() == exchange && contract.contract() == target_contract,
        UniverseTarget::Symbol { .. } => false,
    }
}

fn apply_exclusions(
    occurrences: &mut BTreeMap<(String, UniverseView), CandidateOccurrence>,
    excludes: &[UniverseSelectorSpec],
    global_filters: &[UniverseTarget],
) {
    occurrences.retain(|_, occurrence| {
        let excluded_by_view = excludes.iter().any(|selector| {
            if selector.view() == UniverseView::Symbol {
                return selector.targets().iter().any(|target| {
                    matches!(target, UniverseTarget::Symbol { symbol } if symbol == &occurrence.symbol)
                });
            }
            selector.view() == occurrence.provenance
                && selector
                    .targets()
                    .iter()
                    .any(|target| occurrence_matches_target(occurrence, target))
        });
        !excluded_by_view
            && !global_filters
                .iter()
                .any(|target| occurrence_matches_global_filter(occurrence, target))
    });
}

fn occurrence_matches_target(occurrence: &CandidateOccurrence, target: &UniverseTarget) -> bool {
    match target {
        UniverseTarget::All => true,
        UniverseTarget::Exchange { exchange } => occurrence.exchange.as_ref() == Some(exchange),
        UniverseTarget::Product { exchange, product } => {
            occurrence.exchange.as_ref() == Some(exchange)
                && occurrence.product.as_ref() == Some(product)
        }
        UniverseTarget::Contract { exchange, contract } => {
            occurrence.kind == CompiledUniverseInstrumentKind::PhysicalContract
                && occurrence.exchange.as_ref() == Some(exchange)
                && occurrence.symbol == format!("{exchange}.{contract}")
        }
        UniverseTarget::Symbol { symbol } => occurrence.symbol == *symbol,
    }
}

fn occurrence_matches_global_filter(
    occurrence: &CandidateOccurrence,
    target: &UniverseTarget,
) -> bool {
    occurrence_matches_target(occurrence, target)
}

fn aggregate_candidates(
    occurrences: BTreeMap<(String, UniverseView), CandidateOccurrence>,
) -> Vec<CompiledUniverseCandidate> {
    let mut candidates = BTreeMap::<String, CandidateAccumulator>::new();
    for occurrence in occurrences.into_values() {
        let candidate = candidates
            .entry(occurrence.symbol.clone())
            .or_insert_with(|| CandidateAccumulator {
                kind: occurrence.kind,
                provenance: BTreeSet::new(),
                exchange: occurrence.exchange.clone(),
                product: occurrence.product.clone(),
            });
        candidate.provenance.insert(occurrence.provenance);
        if candidate.kind == CompiledUniverseInstrumentKind::ExplicitSymbol
            && occurrence.kind != CompiledUniverseInstrumentKind::ExplicitSymbol
        {
            candidate.kind = occurrence.kind;
            candidate.exchange = occurrence.exchange;
            candidate.product = occurrence.product;
        }
    }
    candidates
        .into_iter()
        .map(|(symbol, candidate)| CompiledUniverseCandidate {
            symbol,
            kind: candidate.kind,
            provenance: candidate.provenance.into_iter().collect(),
            exchange: candidate.exchange,
            product: candidate.product,
        })
        .collect()
}

fn capability<T, E: Error>(
    operation: &'static str,
    result: Result<T, E>,
) -> Result<T, UniverseCompileError> {
    result.map_err(|error| UniverseCompileError::Capability {
        operation,
        message: error.to_string(),
    })
}

struct CandidateOccurrence {
    symbol: String,
    provenance: UniverseView,
    kind: CompiledUniverseInstrumentKind,
    exchange: Option<String>,
    product: Option<String>,
}

struct CandidateAccumulator {
    kind: CompiledUniverseInstrumentKind,
    provenance: BTreeSet<UniverseView>,
    exchange: Option<String>,
    product: Option<String>,
}
