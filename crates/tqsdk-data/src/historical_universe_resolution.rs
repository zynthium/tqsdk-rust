use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    CatalogContract, DataError, DerivedView, DynamicUniverseScope, HistoricalCatalogAcquisition,
    HistoricalCatalogProof, HistoricalDataKind, HistoricalFillUniverseSpec,
    HistoricalSemanticCatalog, HistoricalUniversePlan, HistoricalUniversePlanV3Execution,
    HistoricalUniversePlanV3Identity, HistoricalUniverseTimeline, Result, UniverseBudget,
    UniverseExpression, UniverseInstrumentId, UniverseMemberChange, UniverseSelectorKind,
};

pub const HISTORICAL_UNIVERSE_COMPILER_IDENTITY: &str = "tqsdk.historical-universe-compiler.v1";

/// Why a source series is required even when it is not visible as a member.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalDependencyRole {
    VisiblePhysical,
    ContinuousUnderlying,
    IndexSeries,
}

/// One normalized source dependency shared by all kind-specific target sets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalUniverseDependency {
    pub source_symbol: String,
    pub roles: BTreeSet<HistoricalDependencyRole>,
    pub listing_start_ns: i64,
}

/// One exact cache fill target for a history family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalUniverseKindTarget {
    pub source_symbol: String,
    pub start_ns: i64,
    pub end_ns: i64,
}

/// Reproducible compiler output. Visible members and data dependencies are separate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalUniverseResolution {
    pub plan: HistoricalUniversePlan,
    pub visible_membership_sha256: String,
    pub dependency_set_sha256: String,
    pub resolved_targets_sha256: BTreeMap<HistoricalDataKind, String>,
    pub dependencies: Vec<HistoricalUniverseDependency>,
    pub targets: BTreeMap<HistoricalDataKind, Vec<HistoricalUniverseKindTarget>>,
}

impl HistoricalUniverseResolution {
    pub fn targets_for_kind(&self, kind: HistoricalDataKind) -> &[HistoricalUniverseKindTarget] {
        self.targets.get(&kind).map(Vec::as_slice).unwrap_or(&[])
    }
}

/// Compiles a strict authoritative catalog into membership plus dependency closure.
pub fn compile_historical_universe_resolution(
    acquisition: &HistoricalCatalogAcquisition,
    semantic: &HistoricalSemanticCatalog,
    spec: &HistoricalFillUniverseSpec,
    start_ns: i64,
    end_ns: i64,
    budget: UniverseBudget,
) -> Result<HistoricalUniverseResolution> {
    acquisition.validate()?;
    semantic.validate_against_acquisition(acquisition)?;
    if !matches!(
        acquisition.proof,
        HistoricalCatalogProof::AuthoritativeLifecycle
            | HistoricalCatalogProof::ProviderHistoryObserved
    ) {
        return Err(validation(
            "historical universe resolution requires executable membership proof",
        ));
    }
    if semantic.acquisition_sha256 != acquisition.acquisition_sha256 {
        return Err(validation(
            "historical semantic catalog does not reference the supplied acquisition",
        ));
    }
    let expression = spec.timeline_expression().ok_or_else(|| {
        validation("strict historical universe resolution requires timeline(...)")
    })?;
    if start_ns >= end_ns {
        return Err(validation(
            "historical universe resolution end_ns must be greater than start_ns",
        ));
    }
    let selection = resolve_selection(expression, &semantic.catalog.contracts)?;
    if selection.physical_symbols.is_empty()
        && selection.continuous_products.is_empty()
        && selection.index_products.is_empty()
    {
        return Err(validation(
            "historical universe selector resolves no visible members",
        ));
    }

    let mut views = BTreeSet::new();
    if !selection.continuous_products.is_empty() {
        views.insert(DerivedView::Continuous);
    }
    if !selection.index_products.is_empty() {
        views.insert(DerivedView::Index);
    }
    let mut timeline =
        semantic
            .catalog
            .compile_timeline(start_ns, end_ns, DynamicUniverseScope::all(), views)?;
    filter_visible_timeline(&mut timeline, &selection);
    timeline.validate()?;

    let dependencies =
        resolve_dependencies(&semantic.catalog.contracts, &selection, start_ns, end_ns)?;
    let targets = resolve_kind_targets(acquisition, semantic, &dependencies, start_ns, end_ns)?;
    let visible_membership_sha256 = sha256_identity(&serde_json::to_vec(&timeline.batches)?);
    let dependency_set_sha256 = sha256_identity(&serde_json::to_vec(&dependencies)?);
    let resolved_targets_sha256 = targets
        .iter()
        .map(|(kind, targets)| Ok((*kind, sha256_identity(&serde_json::to_vec(targets)?))))
        .collect::<Result<BTreeMap<_, _>>>()?;

    let execution = HistoricalUniversePlanV3Execution::new(
        visible_membership_sha256.clone(),
        dependency_set_sha256.clone(),
        resolved_targets_sha256.clone(),
        dependencies.clone(),
        targets.clone(),
    )?;
    let identity = HistoricalUniversePlanV3Identity::new(
        spec.to_string(),
        spec.canonicalization_identity(),
        acquisition.acquisition_sha256.clone(),
        semantic.semantic_catalog_sha256.clone(),
        HISTORICAL_UNIVERSE_COMPILER_IDENTITY,
        acquisition.proof,
    )?
    .with_execution_sha256(execution.execution_sha256.clone())?;
    let plan = timeline.prepare_v3(budget, identity, execution)?;
    Ok(HistoricalUniverseResolution {
        plan,
        visible_membership_sha256,
        dependency_set_sha256,
        resolved_targets_sha256,
        dependencies,
        targets,
    })
}

#[derive(Debug, Default)]
struct HistoricalSelection {
    physical_symbols: BTreeSet<String>,
    continuous_products: BTreeSet<(String, String)>,
    index_products: BTreeSet<(String, String)>,
}

fn resolve_selection(
    expression: &UniverseExpression,
    contracts: &[CatalogContract],
) -> Result<HistoricalSelection> {
    let all_products = contracts
        .iter()
        .map(|contract| (contract.exchange_id.clone(), contract.product_id.clone()))
        .collect::<BTreeSet<_>>();
    let mut selection = HistoricalSelection::default();
    for clause in expression.clauses() {
        let kind = clause.selector().kind();
        let values = clause.selector().values();
        match kind {
            UniverseSelectorKind::Active
            | UniverseSelectorKind::Symbol
            | UniverseSelectorKind::Product
            | UniverseSelectorKind::Exchange => {
                let matched = contracts
                    .iter()
                    .filter(|contract| selector_matches_contract(kind, values, contract))
                    .map(|contract| contract.physical_symbol.clone())
                    .collect::<BTreeSet<_>>();
                apply_set_clause(&mut selection.physical_symbols, matched, clause.exclude());
                if clause.exclude() {
                    let matched_products = contracts
                        .iter()
                        .filter(|contract| selector_matches_contract(kind, values, contract))
                        .map(|contract| (contract.exchange_id.clone(), contract.product_id.clone()))
                        .collect::<BTreeSet<_>>();
                    for product in matched_products {
                        selection.continuous_products.remove(&product);
                        selection.index_products.remove(&product);
                    }
                }
            }
            UniverseSelectorKind::Cont | UniverseSelectorKind::Index => {
                let matched = all_products
                    .iter()
                    .filter(|product| selector_matches_product(values, product))
                    .cloned()
                    .collect::<BTreeSet<_>>();
                let target = if matches!(kind, UniverseSelectorKind::Cont) {
                    &mut selection.continuous_products
                } else {
                    &mut selection.index_products
                };
                apply_set_clause(target, matched, clause.exclude());
            }
            UniverseSelectorKind::Main
            | UniverseSelectorKind::Top(_)
            | UniverseSelectorKind::File
            | UniverseSelectorKind::Auto => {
                return Err(validation(
                    "historical universe compiler received an unsupported selector",
                ));
            }
        }
    }
    Ok(selection)
}

fn selector_matches_contract(
    kind: UniverseSelectorKind,
    values: &[String],
    contract: &CatalogContract,
) -> bool {
    match kind {
        UniverseSelectorKind::Active => values.iter().any(|value| value == "all"),
        UniverseSelectorKind::Symbol => values
            .iter()
            .any(|value| value == &contract.physical_symbol),
        UniverseSelectorKind::Product => values.iter().any(|value| {
            value == "all"
                || value == &contract.product_id
                || value == &format!("{}.{}", contract.exchange_id, contract.product_id)
        }),
        UniverseSelectorKind::Exchange => values
            .iter()
            .any(|value| value == "all" || value == &contract.exchange_id),
        _ => false,
    }
}

fn selector_matches_product(values: &[String], product: &(String, String)) -> bool {
    values.iter().any(|value| {
        value == "all" || value == &product.1 || value == &format!("{}.{}", product.0, product.1)
    })
}

fn apply_set_clause<T: Ord>(target: &mut BTreeSet<T>, matched: BTreeSet<T>, exclude: bool) {
    if exclude {
        for value in matched {
            target.remove(&value);
        }
    } else {
        target.extend(matched);
    }
}

fn filter_visible_timeline(
    timeline: &mut HistoricalUniverseTimeline,
    selection: &HistoricalSelection,
) {
    timeline.batches.iter_mut().for_each(|batch| {
        batch.changes.retain(|change| match change {
            UniverseMemberChange::Add { instrument, .. }
            | UniverseMemberChange::Remove { instrument } => {
                instrument_is_selected(instrument, selection)
            }
        });
    });
    timeline.batches.retain(|batch| !batch.changes.is_empty());
    timeline
        .physical_listing_starts
        .retain(|symbol, _| selection.physical_symbols.contains(symbol));
}

fn instrument_is_selected(
    instrument: &UniverseInstrumentId,
    selection: &HistoricalSelection,
) -> bool {
    match instrument {
        UniverseInstrumentId::Physical { symbol } => selection.physical_symbols.contains(symbol),
        UniverseInstrumentId::Continuous {
            exchange_id,
            product_id,
        } => selection
            .continuous_products
            .contains(&(exchange_id.clone(), product_id.clone())),
        UniverseInstrumentId::Index {
            exchange_id,
            product_id,
        } => selection
            .index_products
            .contains(&(exchange_id.clone(), product_id.clone())),
    }
}

fn resolve_dependencies(
    contracts: &[CatalogContract],
    selection: &HistoricalSelection,
    start_ns: i64,
    end_ns: i64,
) -> Result<Vec<HistoricalUniverseDependency>> {
    let mut dependencies = BTreeMap::<String, HistoricalUniverseDependency>::new();
    for contract in contracts.iter().filter(|contract| {
        contract
            .lifecycle
            .iter()
            .any(|interval| interval.intersects(start_ns, end_ns))
    }) {
        let product = (contract.exchange_id.clone(), contract.product_id.clone());
        let mut roles = BTreeSet::new();
        if selection
            .physical_symbols
            .contains(&contract.physical_symbol)
        {
            roles.insert(HistoricalDependencyRole::VisiblePhysical);
        }
        if selection.continuous_products.contains(&product) {
            roles.insert(HistoricalDependencyRole::ContinuousUnderlying);
        }
        if roles.is_empty() {
            continue;
        }
        let listing_start_ns = contract
            .lifecycle
            .first()
            .ok_or_else(|| validation("historical dependency lacks lifecycle"))?
            .start_ns;
        dependencies.insert(
            contract.physical_symbol.clone(),
            HistoricalUniverseDependency {
                source_symbol: contract.physical_symbol.clone(),
                roles,
                listing_start_ns,
            },
        );
    }
    for (exchange_id, product_id) in &selection.index_products {
        let listing_start_ns = contracts
            .iter()
            .filter(|contract| {
                &contract.exchange_id == exchange_id && &contract.product_id == product_id
            })
            .flat_map(|contract| contract.lifecycle.iter())
            .map(|interval| interval.start_ns)
            .min()
            .ok_or_else(|| validation("historical index dependency has no product lifecycle"))?;
        let source_symbol = format!("KQ.i@{exchange_id}.{product_id}");
        dependencies.insert(
            source_symbol.clone(),
            HistoricalUniverseDependency {
                source_symbol,
                roles: BTreeSet::from([HistoricalDependencyRole::IndexSeries]),
                listing_start_ns,
            },
        );
    }
    Ok(dependencies.into_values().collect())
}

fn resolve_kind_targets(
    acquisition: &HistoricalCatalogAcquisition,
    semantic: &HistoricalSemanticCatalog,
    dependencies: &[HistoricalUniverseDependency],
    requested_start_ns: i64,
    end_ns: i64,
) -> Result<BTreeMap<HistoricalDataKind, Vec<HistoricalUniverseKindTarget>>> {
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
                    validation(format!(
                        "historical {kind:?} availability boundary is unproven for {}",
                        dependency.source_symbol
                    ))
                })?
                .max(dependency.listing_start_ns)
                .max(requested_start_ns);
            if kind_start >= end_ns {
                continue;
            }
            targets.push(HistoricalUniverseKindTarget {
                source_symbol: dependency.source_symbol.clone(),
                start_ns: kind_start,
                end_ns,
            });
        }
        targets.sort_by(|left, right| left.source_symbol.cmp(&right.source_symbol));
        by_kind.insert(kind, targets);
    }
    Ok(by_kind)
}

fn sha256_identity(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn validation(message: impl Into<String>) -> DataError {
    DataError::Validation(message.into())
}
