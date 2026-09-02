use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::str::FromStr;

use sha2::{Digest, Sha256};

use crate::{
    ActiveInterval, CatalogContract, DataError, DerivedView, DynamicUniverseScope,
    ExpandedUniverseInput, HistoricalAcquisitionContract, HistoricalCatalogAcquisition,
    HistoricalCatalogProof, HistoricalDataKind, HistoricalDependencyRole,
    HistoricalSemanticCatalog, HistoricalUniverseDependency, HistoricalUniverseKindTarget,
    HistoricalUniversePlan, HistoricalUniversePlanExecution, HistoricalUniversePlanIdentity,
    HistoricalUniversePlanV3Execution, HistoricalUniversePlanV3Identity, HistoricalUniversePlanV4,
    HistoricalUniversePlanV4Execution, HistoricalUniversePlanV4Identity, HistoricalUniversePlanV5,
    HistoricalUniverseTimeline, Result, UniverseBudget, UniverseInstrumentId, UniverseMemberChange,
    UniverseMode, UniverseProduct, UniverseSelectorSpec, UniverseSpec, UniverseTarget,
    UniverseTimelineBatch, UniverseView,
};

pub const HISTORICAL_UNIVERSE_V3_PROJECTION_CANONICALIZER_ID: &str =
    "tqsdk.universe.v2-v3-projection.canonical.v1";
pub const HISTORICAL_UNIVERSE_V3_PROJECTION_COMPILER_ID: &str =
    "tqsdk.universe.v2-v3-projection.compiler.v1";
pub const HISTORICAL_UNIVERSE_CONTINUOUS_ID: &str =
    "sha256:cee33b4d6151745c7de17665632ea9c214cb4c636d4c1f20b55f2924634a279a";
const PROVIDER_TIMELINE_BOOTSTRAP_SCOPE_ID: &str =
    "tqsdk.provider-history.timeline-bootstrap-closure.v1";

/// Projects a complete provider-current discovery into the smallest physical
/// roster whose native-daily membership must be observed before compiling a
/// V2 historical timeline. Full roster discovery remains separate; this
/// acquisition is deliberately scoped to the timeline's visible physical
/// members and retained logical-view dependencies.
#[doc(hidden)]
pub fn scope_provider_current_timeline_bootstrap(
    acquisition: &HistoricalCatalogAcquisition,
    input: &ExpandedUniverseInput,
) -> Result<HistoricalCatalogAcquisition> {
    acquisition.validate()?;
    let spec = input.spec().ok_or_else(|| {
        DataError::Validation("timeline bootstrap scope requires Universe V2 input".to_string())
    })?;
    if spec.mode() != UniverseMode::Timeline {
        return Err(DataError::Validation(
            "provider bootstrap scope requires timeline(...) Universe V2 mode".to_string(),
        ));
    }
    if spec
        .includes()
        .iter()
        .any(|selector| matches!(selector.view(), UniverseView::Main | UniverseView::Top(_)))
    {
        return Err(DataError::Validation(
            "timeline main/top requires historical ranking before provider bootstrap".to_string(),
        ));
    }

    let required_symbols = provider_timeline_bootstrap_symbols(
        &acquisition.contracts,
        spec,
        input.expanded_symbols(),
    )?;
    let discovered_symbols = acquisition
        .contracts
        .iter()
        .map(|contract| contract.symbol.as_str())
        .collect::<BTreeSet<_>>();
    if required_symbols.len() == discovered_symbols.len()
        && required_symbols
            .iter()
            .all(|symbol| discovered_symbols.contains(symbol.as_str()))
    {
        return Ok(acquisition.clone());
    }
    let source_identity = format!(
        "{}+{}",
        acquisition.source_identity, PROVIDER_TIMELINE_BOOTSTRAP_SCOPE_ID
    );
    let canonical_universe = format!(
        "timeline-bootstrap-closure:v1:{}:{}",
        spec.canonical_ast_hash(),
        input.input_sources_sha256().unwrap_or("none"),
    );
    acquisition.project_provider_current_bootstrap(
        source_identity,
        canonical_universe,
        &required_symbols,
    )
}

#[derive(Debug, Clone)]
struct ProviderTimelineBootstrapOccurrence {
    symbol: String,
    physical_symbol: Option<String>,
    provenance: UniverseView,
    exchange: String,
    product: String,
}

fn provider_timeline_bootstrap_symbols(
    contracts: &[HistoricalAcquisitionContract],
    spec: &UniverseSpec,
    expanded_symbols: &[String],
) -> Result<BTreeSet<String>> {
    let products = contracts
        .iter()
        .map(|contract| UniverseProduct::new(&contract.exchange_id, &contract.product_id))
        .collect::<BTreeSet<_>>();
    let mut occurrences = Vec::new();

    for selector in spec.includes() {
        match selector.view() {
            UniverseView::Contract => {
                for target in selector.targets() {
                    for contract in contracts
                        .iter()
                        .filter(|contract| bootstrap_contract_matches_target(contract, target))
                    {
                        occurrences.push(physical_bootstrap_occurrence(
                            contract,
                            UniverseView::Contract,
                        ));
                    }
                }
            }
            view @ (UniverseView::Continuous | UniverseView::Index) => {
                for product in matching_products(&products, selector.targets()) {
                    occurrences.push(logical_bootstrap_occurrence(view, product));
                }
            }
            UniverseView::Symbol => {
                for symbol in selector.targets().iter().filter_map(|target| match target {
                    UniverseTarget::Symbol { symbol } => Some(symbol),
                    _ => None,
                }) {
                    include_bootstrap_symbol(&mut occurrences, contracts, &products, symbol)?;
                }
            }
            UniverseView::Main | UniverseView::Top(_) => {
                unreachable!("timeline ranking selectors are rejected before bootstrap scoping")
            }
        }
    }
    for symbol in expanded_symbols {
        include_bootstrap_symbol(&mut occurrences, contracts, &products, symbol)?;
    }

    occurrences.retain(|occurrence| {
        let excluded_by_view = spec.excludes().iter().any(|selector| {
            if selector.view() == UniverseView::Symbol {
                return selector.targets().iter().any(|target| {
                    matches!(target, UniverseTarget::Symbol { symbol } if symbol == &occurrence.symbol)
                });
            }
            selector.view() == occurrence.provenance
                && selector
                    .targets()
                    .iter()
                    .any(|target| bootstrap_occurrence_matches_target(occurrence, target))
        });
        !excluded_by_view
            && !spec
                .global_filters()
                .iter()
                .any(|target| bootstrap_occurrence_matches_target(occurrence, target))
    });

    let mut required_symbols = BTreeSet::new();
    for occurrence in occurrences {
        if let Some(symbol) = occurrence.physical_symbol {
            required_symbols.insert(symbol);
            continue;
        }
        required_symbols.extend(
            contracts
                .iter()
                .filter(|contract| {
                    contract.exchange_id == occurrence.exchange
                        && contract.product_id == occurrence.product
                })
                .map(|contract| contract.symbol.clone()),
        );
    }
    if required_symbols.is_empty() {
        return Err(DataError::Validation(
            "historical timeline bootstrap scope has no physical candidates".to_string(),
        ));
    }
    Ok(required_symbols)
}

fn include_bootstrap_symbol(
    occurrences: &mut Vec<ProviderTimelineBootstrapOccurrence>,
    contracts: &[HistoricalAcquisitionContract],
    products: &BTreeSet<UniverseProduct>,
    symbol: &str,
) -> Result<()> {
    if let Some(contract) = contracts.iter().find(|contract| contract.symbol == symbol) {
        occurrences.push(physical_bootstrap_occurrence(
            contract,
            UniverseView::Symbol,
        ));
        return Ok(());
    }
    let Some((view, product)) = classify_logical_symbol(symbol) else {
        return Err(DataError::Validation(format!(
            "historical Universe symbol {symbol} cannot be classified or proven"
        )));
    };
    if !products.contains(&product) {
        return Err(DataError::Validation(format!(
            "historical Universe symbol {symbol} references an unknown product"
        )));
    }
    let mut occurrence = logical_bootstrap_occurrence(view, &product);
    occurrence.symbol = symbol.to_string();
    occurrence.provenance = UniverseView::Symbol;
    occurrences.push(occurrence);
    Ok(())
}

fn physical_bootstrap_occurrence(
    contract: &HistoricalAcquisitionContract,
    provenance: UniverseView,
) -> ProviderTimelineBootstrapOccurrence {
    ProviderTimelineBootstrapOccurrence {
        symbol: contract.symbol.clone(),
        physical_symbol: Some(contract.symbol.clone()),
        provenance,
        exchange: contract.exchange_id.clone(),
        product: contract.product_id.clone(),
    }
}

fn logical_bootstrap_occurrence(
    view: UniverseView,
    product: &UniverseProduct,
) -> ProviderTimelineBootstrapOccurrence {
    let symbol = match view {
        UniverseView::Continuous => format!("KQ.m@{}.{}", product.exchange(), product.product()),
        UniverseView::Index => format!("KQ.i@{}.{}", product.exchange(), product.product()),
        _ => unreachable!("bootstrap logical occurrence requires a derived view"),
    };
    ProviderTimelineBootstrapOccurrence {
        symbol,
        physical_symbol: None,
        provenance: view,
        exchange: product.exchange().to_string(),
        product: product.product().to_string(),
    }
}

fn bootstrap_contract_matches_target(
    contract: &HistoricalAcquisitionContract,
    target: &UniverseTarget,
) -> bool {
    match target {
        UniverseTarget::All => true,
        UniverseTarget::Exchange { exchange } => contract.exchange_id == *exchange,
        UniverseTarget::Product { exchange, product } => {
            contract.exchange_id == *exchange && contract.product_id == *product
        }
        UniverseTarget::Contract {
            exchange,
            contract: suffix,
        } => contract.exchange_id == *exchange && contract.symbol == format!("{exchange}.{suffix}"),
        UniverseTarget::Symbol { symbol } => contract.symbol == *symbol,
    }
}

fn bootstrap_occurrence_matches_target(
    occurrence: &ProviderTimelineBootstrapOccurrence,
    target: &UniverseTarget,
) -> bool {
    match target {
        UniverseTarget::All => true,
        UniverseTarget::Exchange { exchange } => occurrence.exchange == *exchange,
        UniverseTarget::Product { exchange, product } => {
            occurrence.exchange == *exchange && occurrence.product == *product
        }
        UniverseTarget::Contract { exchange, contract } => {
            occurrence.physical_symbol.is_some()
                && occurrence.exchange == *exchange
                && occurrence.symbol == format!("{exchange}.{contract}")
        }
        UniverseTarget::Symbol { symbol } => occurrence.symbol == *symbol,
    }
}

/// Compatibility policy for legacy V4/V3 write-set construction.
///
/// Current CLI V2 timelines publish V5 directly; the historical policy token
/// remains only so existing callers can keep constructing a V4/V3 write set
/// for migration fixtures or controlled compatibility work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoricalPlanWritePolicy {
    LegacyOnly,
    V4WithV3Rollback,
}

impl HistoricalPlanWritePolicy {
    pub fn ensure_v2_timeline_enabled(self) -> std::result::Result<(), HistoricalUniverseV4Error> {
        match self {
            Self::LegacyOnly => Err(HistoricalUniverseV4Error::WriterDisabled),
            Self::V4WithV3Rollback => Ok(()),
        }
    }
}

impl FromStr for HistoricalPlanWritePolicy {
    type Err = HistoricalUniverseV4Error;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.trim() {
            "legacy-only" => Ok(Self::LegacyOnly),
            "v4-with-v3-rollback" => Ok(Self::V4WithV3Rollback),
            other => Err(HistoricalUniverseV4Error::Invalid(format!(
                "unknown historical plan write policy {other}"
            ))),
        }
    }
}

impl fmt::Display for HistoricalPlanWritePolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::LegacyOnly => "legacy-only",
            Self::V4WithV3Rollback => "v4-with-v3-rollback",
        })
    }
}

/// Pinned catalog and provider-membership evidence consumed by the pure compiler.
pub trait TimelineCapabilities {
    fn acquisition(&self) -> Result<&HistoricalCatalogAcquisition>;
    fn semantic_catalog(&self) -> Result<&HistoricalSemanticCatalog>;
}

impl TimelineCapabilities for (&HistoricalCatalogAcquisition, &HistoricalSemanticCatalog) {
    fn acquisition(&self) -> Result<&HistoricalCatalogAcquisition> {
        Ok(self.0)
    }

    fn semantic_catalog(&self) -> Result<&HistoricalSemanticCatalog> {
        Ok(self.1)
    }
}

/// One ranked physical membership interval supplied by a pinned ranking artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct HistoricalRankedMembership {
    symbol: String,
    intervals: Vec<ActiveInterval>,
}

impl HistoricalRankedMembership {
    pub fn new(
        symbol: impl Into<String>,
        intervals: Vec<ActiveInterval>,
    ) -> std::result::Result<Self, HistoricalUniverseV4Error> {
        let symbol = symbol.into();
        if symbol.trim().is_empty() || intervals.is_empty() {
            return Err(HistoricalUniverseV4Error::Invalid(
                "historical ranked membership requires a symbol and intervals".to_string(),
            ));
        }
        Ok(Self { symbol, intervals })
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn intervals(&self) -> &[ActiveInterval] {
        &self.intervals
    }
}

/// Optional pinned historical ranking capability for timeline `main`/`top:N`.
pub trait HistoricalRankingCapabilities {
    fn ranking_identity(&self) -> &str;

    fn ranked_membership(
        &self,
        view: UniverseView,
        product: &UniverseProduct,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<HistoricalRankedMembership>>;
}

#[derive(Debug)]
#[non_exhaustive]
pub enum HistoricalUniverseV4Error {
    WrongMode {
        actual: UniverseMode,
    },
    UnsupportedTimelineRanking {
        view: UniverseView,
    },
    WriterDisabled,
    NoCandidates,
    Capability {
        operation: &'static str,
        message: String,
    },
    Invalid(String),
}

impl fmt::Display for HistoricalUniverseV4Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongMode { actual } => write!(
                formatter,
                "historical Universe V4 compiler requires timeline mode, got {actual:?}"
            ),
            Self::UnsupportedTimelineRanking { view } => write!(
                formatter,
                "historical Universe V4 {view} requires a pinned ranking capability"
            ),
            Self::WriterDisabled => formatter.write_str(
                "Universe V2 timeline writer is disabled by historical plan policy legacy-only",
            ),
            Self::NoCandidates => {
                formatter.write_str("historical Universe V4 resolves no visible candidates")
            }
            Self::Capability { operation, message } => {
                write!(
                    formatter,
                    "historical Universe capability {operation} failed: {message}"
                )
            }
            Self::Invalid(message) => formatter.write_str(message),
        }
    }
}

impl Error for HistoricalUniverseV4Error {}

impl From<DataError> for HistoricalUniverseV4Error {
    fn from(error: DataError) -> Self {
        Self::Invalid(error.to_string())
    }
}

impl From<serde_json::Error> for HistoricalUniverseV4Error {
    fn from(error: serde_json::Error) -> Self {
        Self::Invalid(error.to_string())
    }
}

/// Reproducible V2 timeline compiler output before plan serialization.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct HistoricalUniverseResolutionV4 {
    spec: UniverseSpec,
    input_sources_sha256: Option<String>,
    timeline: HistoricalUniverseTimeline,
    budget: UniverseBudget,
    acquisition_sha256: String,
    semantic_catalog_sha256: String,
    proof: HistoricalCatalogProof,
    ranking_identity: Option<String>,
    visible_membership_sha256: String,
    dependency_set_sha256: String,
    resolved_targets_sha256: BTreeMap<HistoricalDataKind, String>,
    dependencies: Vec<HistoricalUniverseDependency>,
    targets: BTreeMap<HistoricalDataKind, Vec<HistoricalUniverseKindTarget>>,
}

impl HistoricalUniverseResolutionV4 {
    #[must_use]
    pub const fn timeline(&self) -> &HistoricalUniverseTimeline {
        &self.timeline
    }

    #[must_use]
    pub const fn budget(&self) -> UniverseBudget {
        self.budget
    }

    #[must_use]
    pub fn visible_membership_sha256(&self) -> &str {
        &self.visible_membership_sha256
    }

    #[must_use]
    pub fn dependency_set_sha256(&self) -> &str {
        &self.dependency_set_sha256
    }

    #[must_use]
    pub fn resolved_targets_sha256(&self) -> &BTreeMap<HistoricalDataKind, String> {
        &self.resolved_targets_sha256
    }

    #[must_use]
    pub fn dependencies(&self) -> &[HistoricalUniverseDependency] {
        &self.dependencies
    }

    #[must_use]
    pub fn targets(&self) -> &BTreeMap<HistoricalDataKind, Vec<HistoricalUniverseKindTarget>> {
        &self.targets
    }

    #[must_use]
    pub fn targets_for_kind(&self, kind: HistoricalDataKind) -> &[HistoricalUniverseKindTarget] {
        self.targets.get(&kind).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Produces the current V5 artifact directly. No legacy rollback artifact
    /// is created or consulted by the normal compile path.
    pub fn prepare_plan(
        &self,
    ) -> std::result::Result<HistoricalUniversePlanV5, HistoricalUniverseV4Error> {
        let execution = HistoricalUniversePlanExecution::from_domain(
            &self.timeline,
            self.dependencies.clone(),
            self.targets.clone(),
        )?;
        let timeline_requires_continuous = self
            .timeline
            .derived_views
            .contains(&DerivedView::Continuous);
        let execution_requires_continuous = self.dependencies.iter().any(|dependency| {
            dependency
                .roles
                .contains(&HistoricalDependencyRole::ContinuousUnderlying)
        });
        if timeline_requires_continuous != execution_requires_continuous {
            return Err(HistoricalUniverseV4Error::Invalid(
                "historical continuous membership/dependency closure mismatch".to_string(),
            ));
        }
        let identity = HistoricalUniversePlanIdentity::new(
            &self.spec,
            self.input_sources_sha256.clone(),
            self.acquisition_sha256.clone(),
            self.semantic_catalog_sha256.clone(),
            self.timeline.calendar_identity.clone(),
            self.proof,
            execution.execution_sha256().to_string(),
            timeline_requires_continuous.then(|| HISTORICAL_UNIVERSE_CONTINUOUS_ID.to_string()),
            self.ranking_identity.clone(),
        )?;
        HistoricalUniversePlanV5::new(self.timeline.clone(), self.budget, identity, execution)
            .map_err(Into::into)
    }

    pub fn prepare_write_set(
        &self,
        policy: HistoricalPlanWritePolicy,
    ) -> std::result::Result<HistoricalUniversePlanWriteSet, HistoricalUniverseV4Error> {
        policy.ensure_v2_timeline_enabled()?;
        let v3_execution = HistoricalUniversePlanV3Execution::new(
            self.visible_membership_sha256.clone(),
            self.dependency_set_sha256.clone(),
            self.resolved_targets_sha256.clone(),
            self.dependencies.clone(),
            self.targets.clone(),
        )?;
        let mut v3_identity = HistoricalUniversePlanV3Identity::new(
            format!("universe-v2-ast:{}", self.spec.canonical_ast_hash()),
            HISTORICAL_UNIVERSE_V3_PROJECTION_CANONICALIZER_ID,
            self.acquisition_sha256.clone(),
            self.semantic_catalog_sha256.clone(),
            HISTORICAL_UNIVERSE_V3_PROJECTION_COMPILER_ID,
            self.proof,
        )?
        .with_execution_sha256(v3_execution.execution_sha256.clone())?;
        let timeline_requires_continuous = self
            .timeline
            .derived_views
            .contains(&DerivedView::Continuous);
        let execution_requires_continuous = self.dependencies.iter().any(|dependency| {
            dependency
                .roles
                .contains(&HistoricalDependencyRole::ContinuousUnderlying)
        });
        if timeline_requires_continuous != execution_requires_continuous {
            return Err(HistoricalUniverseV4Error::Invalid(
                "historical continuous membership/dependency closure mismatch".to_string(),
            ));
        }
        if timeline_requires_continuous {
            v3_identity.continuous_identity = Some(HISTORICAL_UNIVERSE_CONTINUOUS_ID.to_string());
        }
        v3_identity.ranking_identity = self.ranking_identity.clone();
        let rollback_v3 =
            self.timeline
                .clone()
                .prepare_v3(self.budget, v3_identity, v3_execution)?;

        let v4_execution = HistoricalUniversePlanV4Execution::from_v3(
            rollback_v3
                .v3_execution
                .as_ref()
                .expect("prepare_v3 always stores the supplied execution"),
        )?;
        let mut identity_builder = HistoricalUniversePlanV4Identity::builder(&self.spec)
            .acquisition_sha256(&self.acquisition_sha256)
            .semantic_catalog_sha256(&self.semantic_catalog_sha256)
            .calendar_identity(&self.timeline.calendar_identity)
            .proof(self.proof)
            .execution_sha256(v4_execution.execution_sha256())
            .rollback_v3_plan_sha256(&rollback_v3.plan_sha256);
        if let Some(input_sources_sha256) = &self.input_sources_sha256 {
            identity_builder = identity_builder.input_sources_sha256(input_sources_sha256);
        }
        if timeline_requires_continuous {
            identity_builder =
                identity_builder.continuous_identity(HISTORICAL_UNIVERSE_CONTINUOUS_ID);
        }
        if let Some(ranking_identity) = &self.ranking_identity {
            identity_builder = identity_builder.ranking_identity(ranking_identity);
        }
        let v4 = HistoricalUniversePlanV4::new(
            self.timeline.clone(),
            self.budget,
            identity_builder.build()?,
            v4_execution,
        )?;
        Ok(HistoricalUniversePlanWriteSet { v4, rollback_v3 })
    }
}

/// V4 plan plus the V3 projection needed by legacy readers during rollout.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct HistoricalUniversePlanWriteSet {
    v4: HistoricalUniversePlanV4,
    rollback_v3: HistoricalUniversePlan,
}

impl HistoricalUniversePlanWriteSet {
    #[must_use]
    pub const fn v4(&self) -> &HistoricalUniversePlanV4 {
        &self.v4
    }

    #[must_use]
    pub const fn rollback_v3(&self) -> &HistoricalUniversePlan {
        &self.rollback_v3
    }
}

/// Compiles provider-data membership into a V4 timeline and per-kind closure.
#[allow(clippy::too_many_arguments)]
pub fn compile_historical_universe_resolution_v4<C: TimelineCapabilities>(
    capabilities: &C,
    spec: &UniverseSpec,
    expanded_symbols: &[String],
    start_ns: i64,
    end_ns: i64,
    budget: UniverseBudget,
    ranking: Option<&dyn HistoricalRankingCapabilities>,
) -> std::result::Result<HistoricalUniverseResolutionV4, HistoricalUniverseV4Error> {
    if spec.mode() != UniverseMode::Timeline {
        return Err(HistoricalUniverseV4Error::WrongMode {
            actual: spec.mode(),
        });
    }
    for selector in spec.includes() {
        if matches!(selector.view(), UniverseView::Main | UniverseView::Top(_)) && ranking.is_none()
        {
            return Err(HistoricalUniverseV4Error::UnsupportedTimelineRanking {
                view: selector.view(),
            });
        }
    }
    if start_ns >= end_ns {
        return Err(HistoricalUniverseV4Error::Invalid(
            "historical Universe V4 end_ns must be greater than start_ns".to_string(),
        ));
    }

    let acquisition = capabilities
        .acquisition()
        .map_err(|error| capability_error("acquisition", error))?;
    let semantic = capabilities
        .semantic_catalog()
        .map_err(|error| capability_error("semantic_catalog", error))?;
    acquisition.validate()?;
    semantic.validate_against_acquisition(acquisition)?;
    if semantic.acquisition_sha256 != acquisition.acquisition_sha256 {
        return Err(HistoricalUniverseV4Error::Invalid(
            "historical Universe semantic catalog does not pin the supplied acquisition"
                .to_string(),
        ));
    }
    if !matches!(
        acquisition.proof,
        HistoricalCatalogProof::AuthoritativeLifecycle
            | HistoricalCatalogProof::ProviderHistoryObserved
    ) {
        return Err(HistoricalUniverseV4Error::Invalid(
            "historical Universe V4 requires executable provider membership proof".to_string(),
        ));
    }

    let contracts = &semantic.catalog.contracts;
    let products = contracts
        .iter()
        .map(|contract| UniverseProduct::new(&contract.exchange_id, &contract.product_id))
        .collect::<BTreeSet<_>>();
    let contracts_by_symbol = contracts
        .iter()
        .map(|contract| (contract.physical_symbol.as_str(), contract))
        .collect::<BTreeMap<_, _>>();
    let mut occurrences = BTreeMap::<(String, UniverseView), TimelineOccurrence>::new();
    for selector in spec.includes() {
        include_selector(
            selector,
            contracts,
            &contracts_by_symbol,
            &products,
            start_ns,
            end_ns,
            ranking,
            &mut occurrences,
        )?;
    }
    for symbol in expanded_symbols {
        include_symbol(
            symbol,
            contracts,
            &products,
            start_ns,
            end_ns,
            &mut occurrences,
        )?;
    }
    apply_exclusions(&mut occurrences, spec.excludes(), spec.global_filters());
    if occurrences.is_empty() {
        return Err(HistoricalUniverseV4Error::NoCandidates);
    }
    let candidates = aggregate_occurrences(occurrences);
    if candidates.is_empty() {
        return Err(HistoricalUniverseV4Error::NoCandidates);
    }
    let timeline = build_timeline(
        &semantic.catalog.catalog_id,
        semantic.catalog.content_sha256(),
        &semantic.catalog.calendar_identity,
        contracts,
        &candidates,
        start_ns,
        end_ns,
    )?;
    timeline.clone().prepare(budget)?;
    let dependencies = resolve_dependencies(contracts, &candidates, start_ns, end_ns)?;
    let targets = resolve_kind_targets(acquisition, semantic, &dependencies, start_ns, end_ns)?;
    let visible_membership_sha256 = sha256(&serde_json::to_vec(&timeline.batches)?);
    let dependency_set_sha256 = sha256(&serde_json::to_vec(&dependencies)?);
    let resolved_targets_sha256 = targets
        .iter()
        .map(|(kind, targets)| Ok((*kind, sha256(&serde_json::to_vec(targets)?))))
        .collect::<Result<BTreeMap<_, _>>>()?;
    let ranking_identity = ranking
        .filter(|_| {
            spec.includes().iter().any(|selector| {
                matches!(selector.view(), UniverseView::Main | UniverseView::Top(_))
            })
        })
        .map(|ranking| ranking.ranking_identity().to_string());
    if ranking_identity
        .as_deref()
        .is_some_and(|identity| identity.trim().is_empty())
    {
        return Err(HistoricalUniverseV4Error::Invalid(
            "historical ranking identity must not be empty".to_string(),
        ));
    }

    Ok(HistoricalUniverseResolutionV4 {
        spec: spec.clone(),
        input_sources_sha256: None,
        timeline,
        budget,
        acquisition_sha256: acquisition.acquisition_sha256.clone(),
        semantic_catalog_sha256: semantic.semantic_catalog_sha256.clone(),
        proof: acquisition.proof,
        ranking_identity,
        visible_membership_sha256,
        dependency_set_sha256,
        resolved_targets_sha256,
        dependencies,
        targets,
    })
}

impl HistoricalUniverseResolutionV4 {
    /// Pins the already-expanded external source identity without changing candidates.
    #[must_use]
    pub fn with_input_sources_sha256(mut self, value: Option<String>) -> Self {
        self.input_sources_sha256 = value;
        self
    }
}

#[allow(clippy::too_many_arguments)]
fn include_selector(
    selector: &UniverseSelectorSpec,
    contracts: &[CatalogContract],
    contracts_by_symbol: &BTreeMap<&str, &CatalogContract>,
    products: &BTreeSet<UniverseProduct>,
    start_ns: i64,
    end_ns: i64,
    ranking: Option<&dyn HistoricalRankingCapabilities>,
    occurrences: &mut BTreeMap<(String, UniverseView), TimelineOccurrence>,
) -> std::result::Result<(), HistoricalUniverseV4Error> {
    match selector.view() {
        UniverseView::Contract => {
            for target in selector.targets() {
                for contract in contracts
                    .iter()
                    .filter(|contract| contract_matches_target(contract, target))
                {
                    let intervals = clipped_intervals(&contract.lifecycle, start_ns, end_ns);
                    if !intervals.is_empty() {
                        insert_occurrence(
                            occurrences,
                            physical_occurrence(contract, UniverseView::Contract, intervals),
                        );
                    }
                }
            }
        }
        view @ (UniverseView::Main | UniverseView::Top(_)) => {
            let ranking =
                ranking.ok_or(HistoricalUniverseV4Error::UnsupportedTimelineRanking { view })?;
            for product in matching_products(products, selector.targets()) {
                let memberships = ranking
                    .ranked_membership(view, product, start_ns, end_ns)
                    .map_err(|error| capability_error("historical_ranking", error))?;
                for membership in memberships {
                    let contract = contracts_by_symbol
                        .get(membership.symbol())
                        .copied()
                        .ok_or_else(|| {
                            HistoricalUniverseV4Error::Invalid(format!(
                                "historical ranking returned unknown contract {}",
                                membership.symbol()
                            ))
                        })?;
                    if contract.exchange_id != product.exchange()
                        || contract.product_id != product.product()
                    {
                        return Err(HistoricalUniverseV4Error::Invalid(format!(
                            "historical ranking contract {} does not belong to {}.{}",
                            membership.symbol(),
                            product.exchange(),
                            product.product()
                        )));
                    }
                    let intervals = intersect_intervals(
                        membership.intervals(),
                        &contract.lifecycle,
                        start_ns,
                        end_ns,
                    );
                    if !intervals.is_empty() {
                        insert_occurrence(
                            occurrences,
                            physical_occurrence(contract, view, intervals),
                        );
                    }
                }
            }
        }
        view @ (UniverseView::Continuous | UniverseView::Index) => {
            for product in matching_products(products, selector.targets()) {
                let intervals = product_intervals(contracts, product, start_ns, end_ns);
                if intervals.is_empty() {
                    continue;
                }
                let instrument = match view {
                    UniverseView::Continuous => UniverseInstrumentId::Continuous {
                        exchange_id: product.exchange().to_string(),
                        product_id: product.product().to_string(),
                    },
                    UniverseView::Index => UniverseInstrumentId::Index {
                        exchange_id: product.exchange().to_string(),
                        product_id: product.product().to_string(),
                    },
                    _ => unreachable!(),
                };
                insert_occurrence(
                    occurrences,
                    TimelineOccurrence {
                        instrument,
                        provenance: view,
                        exchange: product.exchange().to_string(),
                        product: product.product().to_string(),
                        intervals,
                    },
                );
            }
        }
        UniverseView::Symbol => {
            for target in selector.targets() {
                let UniverseTarget::Symbol { symbol } = target else {
                    unreachable!("the V2 parser restricts symbol targets")
                };
                include_symbol(symbol, contracts, products, start_ns, end_ns, occurrences)?;
            }
        }
    }
    Ok(())
}

fn include_symbol(
    symbol: &str,
    contracts: &[CatalogContract],
    products: &BTreeSet<UniverseProduct>,
    start_ns: i64,
    end_ns: i64,
    occurrences: &mut BTreeMap<(String, UniverseView), TimelineOccurrence>,
) -> std::result::Result<(), HistoricalUniverseV4Error> {
    if let Some(contract) = contracts
        .iter()
        .find(|contract| contract.physical_symbol == symbol)
    {
        let intervals = clipped_intervals(&contract.lifecycle, start_ns, end_ns);
        if !intervals.is_empty() {
            insert_occurrence(
                occurrences,
                physical_occurrence(contract, UniverseView::Symbol, intervals),
            );
        }
        return Ok(());
    }
    let (view, product) = classify_logical_symbol(symbol).ok_or_else(|| {
        HistoricalUniverseV4Error::Invalid(format!(
            "historical Universe cannot classify or prove provider symbol {symbol}"
        ))
    })?;
    if !products.contains(&product) {
        return Err(HistoricalUniverseV4Error::Invalid(format!(
            "historical Universe symbol {symbol} references an unknown product"
        )));
    }
    let intervals = product_intervals(contracts, &product, start_ns, end_ns);
    if intervals.is_empty() {
        return Ok(());
    }
    let instrument = match view {
        UniverseView::Continuous => UniverseInstrumentId::Continuous {
            exchange_id: product.exchange().to_string(),
            product_id: product.product().to_string(),
        },
        UniverseView::Index => UniverseInstrumentId::Index {
            exchange_id: product.exchange().to_string(),
            product_id: product.product().to_string(),
        },
        _ => unreachable!(),
    };
    insert_occurrence(
        occurrences,
        TimelineOccurrence {
            instrument,
            provenance: UniverseView::Symbol,
            exchange: product.exchange().to_string(),
            product: product.product().to_string(),
            intervals,
        },
    );
    Ok(())
}

fn physical_occurrence(
    contract: &CatalogContract,
    provenance: UniverseView,
    intervals: Vec<ActiveInterval>,
) -> TimelineOccurrence {
    TimelineOccurrence {
        instrument: UniverseInstrumentId::Physical {
            symbol: contract.physical_symbol.clone(),
        },
        provenance,
        exchange: contract.exchange_id.clone(),
        product: contract.product_id.clone(),
        intervals,
    }
}

fn insert_occurrence(
    occurrences: &mut BTreeMap<(String, UniverseView), TimelineOccurrence>,
    occurrence: TimelineOccurrence,
) {
    let key = (occurrence.instrument.symbol(), occurrence.provenance);
    if let Some(existing) = occurrences.get_mut(&key) {
        existing.intervals.extend(occurrence.intervals);
        existing.intervals = merge_intervals(std::mem::take(&mut existing.intervals));
    } else {
        occurrences.insert(key, occurrence);
    }
}

fn apply_exclusions(
    occurrences: &mut BTreeMap<(String, UniverseView), TimelineOccurrence>,
    excludes: &[UniverseSelectorSpec],
    global_filters: &[UniverseTarget],
) {
    occurrences.retain(|_, occurrence| {
        let excluded_by_view = excludes.iter().any(|selector| {
            if selector.view() == UniverseView::Symbol {
                return selector.targets().iter().any(|target| {
                    matches!(target, UniverseTarget::Symbol { symbol } if symbol == &occurrence.instrument.symbol())
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
                .any(|target| occurrence_matches_target(occurrence, target))
    });
}

fn occurrence_matches_target(occurrence: &TimelineOccurrence, target: &UniverseTarget) -> bool {
    match target {
        UniverseTarget::All => true,
        UniverseTarget::Exchange { exchange } => occurrence.exchange == *exchange,
        UniverseTarget::Product { exchange, product } => {
            occurrence.exchange == *exchange && occurrence.product == *product
        }
        UniverseTarget::Contract { exchange, contract } => {
            matches!(occurrence.instrument, UniverseInstrumentId::Physical { .. })
                && occurrence.exchange == *exchange
                && occurrence.instrument.symbol() == format!("{exchange}.{contract}")
        }
        UniverseTarget::Symbol { symbol } => occurrence.instrument.symbol() == *symbol,
    }
}

fn aggregate_occurrences(
    occurrences: BTreeMap<(String, UniverseView), TimelineOccurrence>,
) -> BTreeMap<UniverseInstrumentId, TimelineCandidate> {
    let mut candidates = BTreeMap::<UniverseInstrumentId, TimelineCandidate>::new();
    for occurrence in occurrences.into_values() {
        let candidate = candidates
            .entry(occurrence.instrument.clone())
            .or_insert_with(|| TimelineCandidate {
                exchange: occurrence.exchange.clone(),
                product: occurrence.product.clone(),
                provenance: BTreeSet::new(),
                intervals: Vec::new(),
            });
        candidate.provenance.insert(occurrence.provenance);
        candidate.intervals.extend(occurrence.intervals);
    }
    candidates.retain(|_, candidate| {
        candidate.intervals = merge_intervals(std::mem::take(&mut candidate.intervals));
        !candidate.intervals.is_empty()
    });
    candidates
}

#[allow(clippy::too_many_arguments)]
fn build_timeline(
    catalog_id: &str,
    catalog_sha256: &str,
    calendar_identity: &str,
    contracts: &[CatalogContract],
    candidates: &BTreeMap<UniverseInstrumentId, TimelineCandidate>,
    start_ns: i64,
    end_ns: i64,
) -> std::result::Result<HistoricalUniverseTimeline, HistoricalUniverseV4Error> {
    let mut changes = BTreeMap::<i64, Vec<UniverseMemberChange>>::new();
    let mut physical_listing_starts = BTreeMap::new();
    let mut derived_views = BTreeSet::new();
    for (instrument, candidate) in candidates {
        match instrument {
            UniverseInstrumentId::Physical { symbol } => {
                let listing_start = contracts
                    .iter()
                    .find(|contract| contract.physical_symbol == *symbol)
                    .and_then(|contract| contract.lifecycle.first())
                    .map(|interval| interval.start_ns)
                    .ok_or_else(|| {
                        HistoricalUniverseV4Error::Invalid(format!(
                            "historical physical candidate {symbol} lacks lifecycle"
                        ))
                    })?;
                physical_listing_starts.insert(symbol.clone(), listing_start);
            }
            UniverseInstrumentId::Continuous { .. } => {
                derived_views.insert(DerivedView::Continuous);
            }
            UniverseInstrumentId::Index { .. } => {
                derived_views.insert(DerivedView::Index);
            }
        }
        let provenance = format!(
            "universe-v2:{}",
            candidate
                .provenance
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("+")
        );
        for interval in &candidate.intervals {
            changes
                .entry(interval.start_ns)
                .or_default()
                .push(UniverseMemberChange::Add {
                    instrument: instrument.clone(),
                    provenance: provenance.clone(),
                });
            if interval.end_ns < end_ns {
                changes
                    .entry(interval.end_ns)
                    .or_default()
                    .push(UniverseMemberChange::Remove {
                        instrument: instrument.clone(),
                    });
            }
        }
    }
    let batches = changes
        .into_iter()
        .filter(|(effective_ns, _)| *effective_ns >= start_ns && *effective_ns < end_ns)
        .map(|(effective_ns, mut changes)| {
            changes.sort();
            changes.dedup();
            UniverseTimelineBatch {
                effective_ns,
                changes,
            }
        })
        .collect();
    Ok(HistoricalUniverseTimeline {
        catalog_id: catalog_id.to_string(),
        catalog_sha256: catalog_sha256.to_string(),
        calendar_identity: calendar_identity.to_string(),
        start_ns,
        end_ns,
        scope: DynamicUniverseScope::all(),
        derived_views,
        physical_listing_starts,
        batches,
    })
}

fn resolve_dependencies(
    contracts: &[CatalogContract],
    candidates: &BTreeMap<UniverseInstrumentId, TimelineCandidate>,
    start_ns: i64,
    end_ns: i64,
) -> std::result::Result<Vec<HistoricalUniverseDependency>, HistoricalUniverseV4Error> {
    let mut dependencies = BTreeMap::<String, HistoricalUniverseDependency>::new();
    for (instrument, candidate) in candidates {
        match instrument {
            UniverseInstrumentId::Physical { symbol } => {
                let contract = contracts
                    .iter()
                    .find(|contract| contract.physical_symbol == *symbol)
                    .ok_or_else(|| {
                        HistoricalUniverseV4Error::Invalid(format!(
                            "historical physical dependency {symbol} is absent from catalog"
                        ))
                    })?;
                add_dependency_role(
                    &mut dependencies,
                    contract,
                    HistoricalDependencyRole::VisiblePhysical,
                )?;
            }
            UniverseInstrumentId::Continuous { .. } => {
                for contract in contracts.iter().filter(|contract| {
                    contract.exchange_id == candidate.exchange
                        && contract.product_id == candidate.product
                        && contract
                            .lifecycle
                            .iter()
                            .any(|interval| interval.intersects(start_ns, end_ns))
                }) {
                    add_dependency_role(
                        &mut dependencies,
                        contract,
                        HistoricalDependencyRole::ContinuousUnderlying,
                    )?;
                }
            }
            UniverseInstrumentId::Index { .. } => {
                let symbol = format!("KQ.i@{}.{}", candidate.exchange, candidate.product);
                let listing_start_ns = contracts
                    .iter()
                    .filter(|contract| {
                        contract.exchange_id == candidate.exchange
                            && contract.product_id == candidate.product
                    })
                    .flat_map(|contract| contract.lifecycle.iter())
                    .map(|interval| interval.start_ns)
                    .min()
                    .ok_or_else(|| {
                        HistoricalUniverseV4Error::Invalid(format!(
                            "historical index dependency {symbol} has no product lifecycle"
                        ))
                    })?;
                dependencies.insert(
                    symbol.clone(),
                    HistoricalUniverseDependency {
                        source_symbol: symbol,
                        roles: BTreeSet::from([HistoricalDependencyRole::IndexSeries]),
                        listing_start_ns,
                    },
                );
            }
        }
    }
    Ok(dependencies.into_values().collect())
}

fn add_dependency_role(
    dependencies: &mut BTreeMap<String, HistoricalUniverseDependency>,
    contract: &CatalogContract,
    role: HistoricalDependencyRole,
) -> std::result::Result<(), HistoricalUniverseV4Error> {
    let listing_start_ns = contract
        .lifecycle
        .first()
        .map(|interval| interval.start_ns)
        .ok_or_else(|| {
            HistoricalUniverseV4Error::Invalid(format!(
                "historical dependency {} lacks lifecycle",
                contract.physical_symbol
            ))
        })?;
    dependencies
        .entry(contract.physical_symbol.clone())
        .and_modify(|dependency| {
            dependency.roles.insert(role);
            dependency.listing_start_ns = dependency.listing_start_ns.min(listing_start_ns);
        })
        .or_insert_with(|| HistoricalUniverseDependency {
            source_symbol: contract.physical_symbol.clone(),
            roles: BTreeSet::from([role]),
            listing_start_ns,
        });
    Ok(())
}

fn resolve_kind_targets(
    acquisition: &HistoricalCatalogAcquisition,
    semantic: &HistoricalSemanticCatalog,
    dependencies: &[HistoricalUniverseDependency],
    requested_start_ns: i64,
    end_ns: i64,
) -> std::result::Result<
    BTreeMap<HistoricalDataKind, Vec<HistoricalUniverseKindTarget>>,
    HistoricalUniverseV4Error,
> {
    let acquisitions = acquisition
        .contracts
        .iter()
        .map(|contract| (contract.symbol.as_str(), contract))
        .collect::<BTreeMap<_, _>>();
    let mut by_kind = BTreeMap::new();
    for kind in [
        HistoricalDataKind::Tick,
        HistoricalDataKind::Minute,
        HistoricalDataKind::Daily,
    ] {
        let mut targets = Vec::new();
        for dependency in dependencies {
            let kind_start = acquisitions
                .get(dependency.source_symbol.as_str())
                .and_then(|contract| contract.first_available_data_ns.get(&kind))
                .copied()
                .or_else(|| {
                    semantic
                        .derived_first_available_data_ns
                        .get(&dependency.source_symbol)
                        .and_then(|boundaries| boundaries.get(&kind))
                        .copied()
                })
                .or_else(|| {
                    (acquisition.proof == HistoricalCatalogProof::ProviderHistoryObserved)
                        .then_some(dependency.listing_start_ns)
                })
                .ok_or_else(|| {
                    HistoricalUniverseV4Error::Invalid(format!(
                        "historical {kind:?} availability boundary is unproven for {}",
                        dependency.source_symbol
                    ))
                })?
                .max(dependency.listing_start_ns)
                .max(requested_start_ns);
            if kind_start < end_ns {
                targets.push(HistoricalUniverseKindTarget {
                    source_symbol: dependency.source_symbol.clone(),
                    start_ns: kind_start,
                    end_ns,
                });
            }
        }
        targets.sort_by(|left, right| left.source_symbol.cmp(&right.source_symbol));
        by_kind.insert(kind, targets);
    }
    Ok(by_kind)
}

fn contract_matches_target(contract: &CatalogContract, target: &UniverseTarget) -> bool {
    match target {
        UniverseTarget::All => true,
        UniverseTarget::Exchange { exchange } => contract.exchange_id == *exchange,
        UniverseTarget::Product { exchange, product } => {
            contract.exchange_id == *exchange && contract.product_id == *product
        }
        UniverseTarget::Contract {
            exchange,
            contract: target_contract,
        } => {
            contract.exchange_id == *exchange
                && contract.physical_symbol == format!("{exchange}.{target_contract}")
        }
        UniverseTarget::Symbol { .. } => false,
    }
}

fn matching_products<'a>(
    products: &'a BTreeSet<UniverseProduct>,
    targets: &[UniverseTarget],
) -> Vec<&'a UniverseProduct> {
    products
        .iter()
        .filter(|product| {
            targets.iter().any(|target| match target {
                UniverseTarget::All => true,
                UniverseTarget::Exchange { exchange } => product.exchange() == exchange,
                UniverseTarget::Product {
                    exchange,
                    product: target_product,
                } => product.exchange() == exchange && product.product() == target_product,
                UniverseTarget::Contract { .. } | UniverseTarget::Symbol { .. } => false,
            })
        })
        .collect()
}

fn classify_logical_symbol(symbol: &str) -> Option<(UniverseView, UniverseProduct)> {
    let (view, rest) = if let Some(rest) = symbol.strip_prefix("KQ.m@") {
        (UniverseView::Continuous, rest)
    } else {
        let rest = symbol.strip_prefix("KQ.i@")?;
        (UniverseView::Index, rest)
    };
    let (exchange, product) = rest.split_once('.')?;
    if exchange.is_empty() || product.is_empty() || product.contains('.') {
        return None;
    }
    Some((view, UniverseProduct::new(exchange, product)))
}

fn product_intervals(
    contracts: &[CatalogContract],
    product: &UniverseProduct,
    start_ns: i64,
    end_ns: i64,
) -> Vec<ActiveInterval> {
    merge_intervals(
        contracts
            .iter()
            .filter(|contract| {
                contract.exchange_id == product.exchange()
                    && contract.product_id == product.product()
            })
            .flat_map(|contract| clipped_intervals(&contract.lifecycle, start_ns, end_ns))
            .collect(),
    )
}

fn clipped_intervals(
    intervals: &[ActiveInterval],
    start_ns: i64,
    end_ns: i64,
) -> Vec<ActiveInterval> {
    intervals
        .iter()
        .filter_map(|interval| {
            let start = interval.start_ns.max(start_ns);
            let end = interval.end_ns.min(end_ns);
            (start < end).then_some(ActiveInterval {
                start_ns: start,
                end_ns: end,
            })
        })
        .collect()
}

fn intersect_intervals(
    left: &[ActiveInterval],
    right: &[ActiveInterval],
    start_ns: i64,
    end_ns: i64,
) -> Vec<ActiveInterval> {
    let mut intersections = Vec::new();
    for left in left {
        for right in right {
            let start = left.start_ns.max(right.start_ns).max(start_ns);
            let end = left.end_ns.min(right.end_ns).min(end_ns);
            if start < end {
                intersections.push(ActiveInterval {
                    start_ns: start,
                    end_ns: end,
                });
            }
        }
    }
    merge_intervals(intersections)
}

fn merge_intervals(mut intervals: Vec<ActiveInterval>) -> Vec<ActiveInterval> {
    intervals.sort_by_key(|interval| (interval.start_ns, interval.end_ns));
    let mut merged: Vec<ActiveInterval> = Vec::new();
    for interval in intervals {
        if let Some(previous) = merged.last_mut()
            && interval.start_ns <= previous.end_ns
        {
            previous.end_ns = previous.end_ns.max(interval.end_ns);
        } else {
            merged.push(interval);
        }
    }
    merged
}

fn capability_error(
    operation: &'static str,
    error: impl fmt::Display,
) -> HistoricalUniverseV4Error {
    HistoricalUniverseV4Error::Capability {
        operation,
        message: error.to_string(),
    }
}

fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

struct TimelineOccurrence {
    instrument: UniverseInstrumentId,
    provenance: UniverseView,
    exchange: String,
    product: String,
    intervals: Vec<ActiveInterval>,
}

struct TimelineCandidate {
    exchange: String,
    product: String,
    provenance: BTreeSet<UniverseView>,
    intervals: Vec<ActiveInterval>,
}
