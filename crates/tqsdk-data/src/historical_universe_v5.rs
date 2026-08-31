#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::{
    DataError, DerivedView, DynamicUniverseScope, HistoricalCatalogProof, HistoricalDataKind,
    HistoricalDependencyRole, HistoricalUniverseDependency, HistoricalUniverseKindTarget,
    HistoricalUniverseTimeline, Result, UNIVERSE_CANONICALIZER_ID, UNIVERSE_COMPILER_ID,
    UNIVERSE_LANGUAGE_VERSION, UniverseBudget, UniverseInstrumentId, UniverseMemberChange,
    UniverseSpec, UniverseTimelineBatch, UniverseView,
};

pub const HISTORICAL_UNIVERSE_PLAN_VERSION: u32 = 5;
pub const HISTORICAL_UNIVERSE_CONTINUOUS_ID: &str =
    "sha256:cee33b4d6151745c7de17665632ea9c214cb4c636d4c1f20b55f2924634a279a";

const PLAN_HASH_DOMAIN: &str = "tqsdk.historical-universe-plan.v5";
const EXECUTION_HASH_DOMAIN: &str = "tqsdk.historical-universe-execution.v5";
const UNIVERSE_AST_HASH_DOMAIN: &[u8] = b"tqsdk.universe.ast.v2\0";

/// Immutable identity material pinned by a current historical-universe plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct HistoricalUniversePlanIdentity {
    language_version: u32,
    normalized_ast_json: String,
    normalized_ast_sha256: String,
    canonicalizer_identity: String,
    compiler_identity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    input_sources_sha256: Option<String>,
    acquisition_sha256: String,
    semantic_catalog_sha256: String,
    calendar_identity: String,
    proof: HistoricalCatalogProof,
    execution_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    continuous_identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ranking_identity: Option<String>,
}

impl HistoricalUniversePlanIdentity {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        spec: &UniverseSpec,
        input_sources_sha256: Option<String>,
        acquisition_sha256: impl Into<String>,
        semantic_catalog_sha256: impl Into<String>,
        calendar_identity: impl Into<String>,
        proof: HistoricalCatalogProof,
        execution_sha256: impl Into<String>,
        continuous_identity: Option<String>,
        ranking_identity: Option<String>,
    ) -> Result<Self> {
        let identity = Self {
            language_version: UNIVERSE_LANGUAGE_VERSION,
            normalized_ast_json: String::from_utf8(spec.canonical_ast_json_bytes().to_vec())
                .expect("Universe V2 canonical AST JSON is UTF-8"),
            normalized_ast_sha256: spec.canonical_ast_hash().to_string(),
            canonicalizer_identity: UNIVERSE_CANONICALIZER_ID.to_string(),
            compiler_identity: UNIVERSE_COMPILER_ID.to_string(),
            input_sources_sha256,
            acquisition_sha256: acquisition_sha256.into(),
            semantic_catalog_sha256: semantic_catalog_sha256.into(),
            calendar_identity: calendar_identity.into(),
            proof,
            execution_sha256: execution_sha256.into(),
            continuous_identity,
            ranking_identity,
        };
        identity.validate()?;
        Ok(identity)
    }

    #[must_use]
    pub fn normalized_ast_json(&self) -> &str {
        &self.normalized_ast_json
    }

    #[must_use]
    pub fn normalized_ast_sha256(&self) -> &str {
        &self.normalized_ast_sha256
    }

    #[must_use]
    pub fn input_sources_sha256(&self) -> Option<&str> {
        self.input_sources_sha256.as_deref()
    }

    #[must_use]
    pub fn acquisition_sha256(&self) -> &str {
        &self.acquisition_sha256
    }

    #[must_use]
    pub fn semantic_catalog_sha256(&self) -> &str {
        &self.semantic_catalog_sha256
    }

    #[must_use]
    pub fn calendar_identity(&self) -> &str {
        &self.calendar_identity
    }

    #[must_use]
    pub const fn proof(&self) -> HistoricalCatalogProof {
        self.proof
    }

    #[must_use]
    pub fn execution_sha256(&self) -> &str {
        &self.execution_sha256
    }

    #[must_use]
    pub fn continuous_identity(&self) -> Option<&str> {
        self.continuous_identity.as_deref()
    }

    #[must_use]
    pub fn ranking_identity(&self) -> Option<&str> {
        self.ranking_identity.as_deref()
    }

    fn validate(&self) -> Result<()> {
        if self.language_version != UNIVERSE_LANGUAGE_VERSION {
            return Err(validation(
                "historical universe plan language version mismatch",
            ));
        }
        if self.canonicalizer_identity != UNIVERSE_CANONICALIZER_ID
            || self.compiler_identity != UNIVERSE_COMPILER_ID
        {
            return Err(validation(
                "historical universe plan canonicalizer/compiler identity mismatch",
            ));
        }
        let spec = UniverseSpec::from_canonical_ast_json(self.normalized_ast_json.as_bytes())
            .map_err(|error| validation(error.to_string()))?;
        if !matches!(spec.mode(), crate::UniverseMode::Timeline) {
            return Err(validation(
                "historical universe plan requires a normalized timeline Universe V2 AST",
            ));
        }
        if self.normalized_ast_sha256
            != hash_with_domain(
                UNIVERSE_AST_HASH_DOMAIN,
                self.normalized_ast_json.as_bytes(),
            )
        {
            return Err(validation(
                "historical universe plan normalized AST hash mismatch",
            ));
        }
        for (name, value) in [
            ("normalized_ast_sha256", self.normalized_ast_sha256.as_str()),
            ("acquisition_sha256", self.acquisition_sha256.as_str()),
            (
                "semantic_catalog_sha256",
                self.semantic_catalog_sha256.as_str(),
            ),
            ("execution_sha256", self.execution_sha256.as_str()),
        ] {
            validate_sha256(name, value)?;
        }
        if let Some(value) = &self.input_sources_sha256 {
            validate_sha256("input_sources_sha256", value)?;
        }
        if self.calendar_identity.trim().is_empty() {
            return Err(validation(
                "historical universe plan calendar identity must not be empty",
            ));
        }
        if !matches!(
            self.proof,
            HistoricalCatalogProof::AuthoritativeLifecycle
                | HistoricalCatalogProof::ProviderHistoryObserved
        ) {
            return Err(validation(
                "historical universe plan proof must support executable membership",
            ));
        }
        for (name, value) in [
            ("continuous identity", self.continuous_identity.as_deref()),
            ("ranking identity", self.ranking_identity.as_deref()),
        ] {
            if value.is_some_and(|value| value.trim().is_empty()) {
                return Err(validation(format!(
                    "historical universe plan {name} must not be empty"
                )));
            }
        }
        let has_continuous = spec
            .includes()
            .iter()
            .any(|selector| matches!(selector.view(), UniverseView::Continuous));
        if has_continuous != self.continuous_identity.is_some() {
            return Err(validation(
                "historical universe plan continuous identity presence does not match AST",
            ));
        }
        if has_continuous
            && self.continuous_identity.as_deref() != Some(HISTORICAL_UNIVERSE_CONTINUOUS_ID)
        {
            return Err(validation(
                "historical universe plan continuous view requires the pinned continuous identity",
            ));
        }
        let has_ranking = spec
            .includes()
            .iter()
            .any(|selector| matches!(selector.view(), UniverseView::Main | UniverseView::Top(_)));
        if has_ranking != self.ranking_identity.is_some() {
            return Err(validation(
                "historical universe plan ranking identity presence does not match AST",
            ));
        }
        Ok(())
    }
}

/// Immutable resolved execution closure for each cache history family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct HistoricalUniversePlanExecution {
    execution_sha256: String,
    visible_membership_sha256: String,
    dependency_set_sha256: String,
    resolved_targets_sha256: BTreeMap<HistoricalDataKind, String>,
    dependencies: Vec<HistoricalUniverseDependency>,
    targets: BTreeMap<HistoricalDataKind, Vec<HistoricalUniverseKindTarget>>,
}

impl HistoricalUniversePlanExecution {
    pub fn new(
        visible_membership_sha256: impl Into<String>,
        dependency_set_sha256: impl Into<String>,
        resolved_targets_sha256: BTreeMap<HistoricalDataKind, String>,
        mut dependencies: Vec<HistoricalUniverseDependency>,
        mut targets: BTreeMap<HistoricalDataKind, Vec<HistoricalUniverseKindTarget>>,
    ) -> Result<Self> {
        normalize_execution_parts(&mut dependencies, &mut targets);
        let dependency_set_sha256 = dependency_set_sha256.into();
        let expected_dependency_set_sha256 = canonical_dependency_set_sha256(&dependencies)?;
        if dependency_set_sha256 != expected_dependency_set_sha256 {
            return Err(validation(
                "historical universe plan dependency set hash mismatch",
            ));
        }
        let expected_resolved_targets_sha256 = canonical_resolved_targets_sha256(&targets)?;
        if resolved_targets_sha256 != expected_resolved_targets_sha256 {
            return Err(validation(
                "historical universe plan resolved target hash mismatch",
            ));
        }
        let mut execution = Self {
            execution_sha256: String::new(),
            visible_membership_sha256: visible_membership_sha256.into(),
            dependency_set_sha256,
            resolved_targets_sha256,
            dependencies,
            targets,
        };
        execution.validate_body()?;
        execution.execution_sha256 = execution.computed_sha256()?;
        execution.validate()?;
        Ok(execution)
    }

    pub(crate) fn from_domain(
        timeline: &HistoricalUniverseTimeline,
        mut dependencies: Vec<HistoricalUniverseDependency>,
        mut targets: BTreeMap<HistoricalDataKind, Vec<HistoricalUniverseKindTarget>>,
    ) -> Result<Self> {
        normalize_execution_parts(&mut dependencies, &mut targets);
        Self::new(
            canonical_visible_membership_sha256(timeline)?,
            canonical_dependency_set_sha256(&dependencies)?,
            canonical_resolved_targets_sha256(&targets)?,
            dependencies,
            targets,
        )
    }

    #[must_use]
    pub fn execution_sha256(&self) -> &str {
        &self.execution_sha256
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

    fn validate(&self) -> Result<()> {
        self.validate_body()?;
        if self.execution_sha256 != self.computed_sha256()? {
            return Err(validation(
                "historical universe plan execution hash mismatch",
            ));
        }
        Ok(())
    }

    fn validate_body(&self) -> Result<()> {
        validate_sha256("visible_membership_sha256", &self.visible_membership_sha256)?;
        validate_sha256("dependency_set_sha256", &self.dependency_set_sha256)?;
        for value in self.resolved_targets_sha256.values() {
            validate_sha256("resolved_targets_sha256", value)?;
        }
        let kinds = [
            HistoricalDataKind::Tick,
            HistoricalDataKind::Minute,
            HistoricalDataKind::Daily,
        ];
        if self.targets.len() != kinds.len()
            || self.resolved_targets_sha256.len() != kinds.len()
            || kinds.iter().any(|kind| {
                !self.targets.contains_key(kind) || !self.resolved_targets_sha256.contains_key(kind)
            })
        {
            return Err(validation(
                "historical universe plan execution must contain tick/minute/daily targets",
            ));
        }
        if self.dependencies.is_empty()
            || self
                .dependencies
                .windows(2)
                .any(|pair| pair[0].source_symbol >= pair[1].source_symbol)
            || self.dependencies.iter().any(|dependency| {
                dependency.source_symbol.trim().is_empty() || dependency.roles.is_empty()
            })
        {
            return Err(validation(
                "historical universe plan dependencies must be nonempty and uniquely sorted",
            ));
        }
        let dependency_symbols = self
            .dependencies
            .iter()
            .map(|dependency| dependency.source_symbol.as_str())
            .collect::<BTreeSet<_>>();
        if dependency_symbols.len() != self.dependencies.len()
            || self.dependency_set_sha256 != canonical_dependency_set_sha256(&self.dependencies)?
        {
            return Err(validation(
                "historical universe plan dependency set hash mismatch",
            ));
        }
        for (kind, targets) in &self.targets {
            if targets.is_empty()
                || targets.windows(2).any(|pair| {
                    (&pair[0].source_symbol, pair[0].start_ns, pair[0].end_ns)
                        >= (&pair[1].source_symbol, pair[1].start_ns, pair[1].end_ns)
                })
                || targets.iter().any(|target| {
                    target.source_symbol.trim().is_empty()
                        || target.start_ns >= target.end_ns
                        || !dependency_symbols.contains(target.source_symbol.as_str())
                })
            {
                return Err(validation(
                    "historical universe plan targets must be nonempty, sorted, and dependency-backed",
                ));
            }
            let expected = self
                .resolved_targets_sha256
                .get(kind)
                .expect("known target kind is checked above");
            if expected != &canonical_target_set_sha256(targets)? {
                return Err(validation(
                    "historical universe plan resolved target hash mismatch",
                ));
            }
        }
        Ok(())
    }

    fn computed_sha256(&self) -> Result<String> {
        Ok(sha256(&serde_json::to_vec(&(
            EXECUTION_HASH_DOMAIN,
            ExecutionBodyWire::from_execution(self),
        ))?))
    }
}

/// Current immutable historical-universe artifact. V5 has no rollback plan.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct HistoricalUniversePlan {
    plan_sha256: String,
    timeline: HistoricalUniverseTimeline,
    budget: UniverseBudget,
    identity: HistoricalUniversePlanIdentity,
    execution: HistoricalUniversePlanExecution,
}

impl HistoricalUniversePlan {
    pub fn new(
        timeline: HistoricalUniverseTimeline,
        budget: UniverseBudget,
        identity: HistoricalUniversePlanIdentity,
        execution: HistoricalUniversePlanExecution,
    ) -> Result<Self> {
        let mut plan = Self {
            plan_sha256: String::new(),
            timeline,
            budget,
            identity,
            execution,
        };
        plan.validate_body()?;
        plan.plan_sha256 = plan.computed_sha256()?;
        plan.verify()?;
        Ok(plan)
    }

    #[must_use]
    pub const fn plan_version(&self) -> u32 {
        HISTORICAL_UNIVERSE_PLAN_VERSION
    }

    #[must_use]
    pub fn plan_sha256(&self) -> &str {
        &self.plan_sha256
    }

    #[must_use]
    pub const fn timeline(&self) -> &HistoricalUniverseTimeline {
        &self.timeline
    }

    #[must_use]
    pub const fn budget(&self) -> UniverseBudget {
        self.budget
    }

    #[must_use]
    pub const fn identity(&self) -> &HistoricalUniversePlanIdentity {
        &self.identity
    }

    #[must_use]
    pub const fn execution(&self) -> &HistoricalUniversePlanExecution {
        &self.execution
    }

    pub fn verify(&self) -> Result<()> {
        validate_sha256("plan_sha256", &self.plan_sha256)?;
        self.validate_body()?;
        if self.plan_sha256 != self.computed_sha256()? {
            return Err(validation("historical universe plan hash mismatch"));
        }
        Ok(())
    }

    pub fn canonical_json_bytes(&self) -> Result<Vec<u8>> {
        self.verify()?;
        serde_json::to_vec(&ArtifactWireV5::from_plan(self)).map_err(Into::into)
    }

    fn validate_body(&self) -> Result<()> {
        self.timeline.validate_with_budget(self.budget)?;
        self.identity.validate()?;
        self.execution.validate()?;
        if self.identity.execution_sha256 != self.execution.execution_sha256 {
            return Err(validation(
                "historical universe plan identity/execution hash mismatch",
            ));
        }
        if self.identity.calendar_identity != self.timeline.calendar_identity {
            return Err(validation(
                "historical universe plan identity/timeline calendar mismatch",
            ));
        }
        let visible_membership = canonical_visible_membership_sha256(&self.timeline)?;
        if self.execution.visible_membership_sha256 != visible_membership {
            return Err(validation(
                "historical universe plan visible membership hash mismatch",
            ));
        }
        let timeline_requires_continuous = self
            .timeline
            .derived_views
            .contains(&DerivedView::Continuous);
        let execution_requires_continuous = self.execution.dependencies.iter().any(|dependency| {
            dependency
                .roles
                .contains(&HistoricalDependencyRole::ContinuousUnderlying)
        });
        if timeline_requires_continuous != execution_requires_continuous
            || timeline_requires_continuous != self.identity.continuous_identity.is_some()
        {
            return Err(validation(
                "historical universe plan continuous membership/dependency closure mismatch",
            ));
        }
        Ok(())
    }

    fn computed_sha256(&self) -> Result<String> {
        Ok(sha256(&serde_json::to_vec(&(
            PLAN_HASH_DOMAIN,
            HISTORICAL_UNIVERSE_PLAN_VERSION,
            TimelineWire::from_timeline(&self.timeline),
            BudgetWire::from_budget(self.budget),
            IdentityWire::from_identity(&self.identity),
            ExecutionWire::from_execution(&self.execution),
        ))?))
    }
}

impl Serialize for HistoricalUniversePlan {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        ArtifactWireV5::from_plan(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for HistoricalUniversePlan {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        ArtifactWireV5Owned::deserialize(deserializer)?
            .into_plan()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Serialize)]
struct ExecutionBodyWire {
    visible_membership_sha256: String,
    dependency_set_sha256: String,
    kind_targets: Vec<KindTargetsWire>,
    dependencies: Vec<DependencyWire>,
}

impl ExecutionBodyWire {
    fn from_execution(execution: &HistoricalUniversePlanExecution) -> Self {
        Self {
            visible_membership_sha256: execution.visible_membership_sha256.clone(),
            dependency_set_sha256: execution.dependency_set_sha256.clone(),
            kind_targets: kind_targets_from_execution(execution),
            dependencies: execution
                .dependencies
                .iter()
                .map(DependencyWire::from_dependency)
                .collect(),
        }
    }
}

#[derive(Serialize)]
struct TimelineWire {
    catalog_id: String,
    catalog_sha256: String,
    calendar_identity: String,
    start_ns: i64,
    end_ns: i64,
    scope: ScopeWire,
    derived_views: Vec<DerivedViewWire>,
    physical_listing_starts: Vec<PhysicalListingStartWire>,
    batches: Vec<TimelineBatchWire>,
}

impl TimelineWire {
    fn from_timeline(timeline: &HistoricalUniverseTimeline) -> Self {
        Self {
            catalog_id: timeline.catalog_id.clone(),
            catalog_sha256: timeline.catalog_sha256.clone(),
            calendar_identity: timeline.calendar_identity.clone(),
            start_ns: timeline.start_ns,
            end_ns: timeline.end_ns,
            scope: ScopeWire::from_scope(&timeline.scope),
            derived_views: timeline
                .derived_views
                .iter()
                .map(DerivedViewWire::from_derived_view)
                .collect(),
            physical_listing_starts: timeline
                .physical_listing_starts
                .iter()
                .map(|(symbol, start_ns)| PhysicalListingStartWire {
                    symbol: symbol.clone(),
                    start_ns: *start_ns,
                })
                .collect(),
            batches: timeline
                .batches
                .iter()
                .map(TimelineBatchWire::from_batch)
                .collect(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TimelineWireOwned {
    catalog_id: String,
    catalog_sha256: String,
    calendar_identity: String,
    start_ns: i64,
    end_ns: i64,
    scope: ScopeWireOwned,
    derived_views: Vec<DerivedViewWire>,
    physical_listing_starts: Vec<PhysicalListingStartWire>,
    batches: Vec<TimelineBatchWireOwned>,
}

impl TimelineWireOwned {
    fn into_timeline(self) -> Result<HistoricalUniverseTimeline> {
        let physical_listing_starts = ordered_string_i64_map(
            "timeline physical listing starts",
            self.physical_listing_starts
                .into_iter()
                .map(|entry| (entry.symbol, entry.start_ns))
                .collect(),
        )?;
        Ok(HistoricalUniverseTimeline {
            catalog_id: self.catalog_id,
            catalog_sha256: self.catalog_sha256,
            calendar_identity: self.calendar_identity,
            start_ns: self.start_ns,
            end_ns: self.end_ns,
            scope: self.scope.into_scope()?,
            derived_views: ordered_set("timeline derived views", self.derived_views)?
                .into_iter()
                .map(Into::into)
                .collect(),
            physical_listing_starts,
            batches: self
                .batches
                .into_iter()
                .map(TimelineBatchWireOwned::into_batch)
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

#[derive(Serialize)]
struct ScopeWire {
    exchanges: Vec<String>,
    products: Vec<String>,
    excluded_exchanges: Vec<String>,
}

impl ScopeWire {
    fn from_scope(scope: &DynamicUniverseScope) -> Self {
        Self {
            exchanges: scope.exchanges.iter().cloned().collect(),
            products: scope.products.iter().cloned().collect(),
            excluded_exchanges: scope.excluded_exchanges.iter().cloned().collect(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeWireOwned {
    exchanges: Vec<String>,
    products: Vec<String>,
    excluded_exchanges: Vec<String>,
}

impl ScopeWireOwned {
    fn into_scope(self) -> Result<DynamicUniverseScope> {
        Ok(DynamicUniverseScope {
            exchanges: ordered_set("timeline scope exchanges", self.exchanges)?,
            products: ordered_set("timeline scope products", self.products)?,
            excluded_exchanges: ordered_set(
                "timeline scope excluded exchanges",
                self.excluded_exchanges,
            )?,
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DerivedViewWire {
    Continuous,
    Index,
}

impl DerivedViewWire {
    fn from_derived_view(value: &DerivedView) -> Self {
        match value {
            DerivedView::Continuous => Self::Continuous,
            DerivedView::Index => Self::Index,
        }
    }

    fn into_derived_view(self) -> DerivedView {
        match self {
            Self::Continuous => DerivedView::Continuous,
            Self::Index => DerivedView::Index,
        }
    }
}

impl From<DerivedViewWire> for DerivedView {
    fn from(value: DerivedViewWire) -> Self {
        value.into_derived_view()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PhysicalListingStartWire {
    symbol: String,
    start_ns: i64,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum InstrumentWire {
    Physical {
        symbol: String,
    },
    Continuous {
        exchange_id: String,
        product_id: String,
    },
    Index {
        exchange_id: String,
        product_id: String,
    },
}

impl InstrumentWire {
    fn from_instrument(instrument: &UniverseInstrumentId) -> Self {
        match instrument {
            UniverseInstrumentId::Physical { symbol } => Self::Physical {
                symbol: symbol.clone(),
            },
            UniverseInstrumentId::Continuous {
                exchange_id,
                product_id,
            } => Self::Continuous {
                exchange_id: exchange_id.clone(),
                product_id: product_id.clone(),
            },
            UniverseInstrumentId::Index {
                exchange_id,
                product_id,
            } => Self::Index {
                exchange_id: exchange_id.clone(),
                product_id: product_id.clone(),
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum InstrumentWireOwned {
    Physical {
        symbol: String,
    },
    Continuous {
        exchange_id: String,
        product_id: String,
    },
    Index {
        exchange_id: String,
        product_id: String,
    },
}

impl InstrumentWireOwned {
    fn into_instrument(self) -> UniverseInstrumentId {
        match self {
            Self::Physical { symbol } => UniverseInstrumentId::Physical { symbol },
            Self::Continuous {
                exchange_id,
                product_id,
            } => UniverseInstrumentId::Continuous {
                exchange_id,
                product_id,
            },
            Self::Index {
                exchange_id,
                product_id,
            } => UniverseInstrumentId::Index {
                exchange_id,
                product_id,
            },
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum TimelineChangeWire {
    Add {
        instrument: InstrumentWire,
        provenance: String,
    },
    Remove {
        instrument: InstrumentWire,
    },
}

impl TimelineChangeWire {
    fn from_change(change: &UniverseMemberChange) -> Self {
        match change {
            UniverseMemberChange::Add {
                instrument,
                provenance,
            } => Self::Add {
                instrument: InstrumentWire::from_instrument(instrument),
                provenance: provenance.clone(),
            },
            UniverseMemberChange::Remove { instrument } => Self::Remove {
                instrument: InstrumentWire::from_instrument(instrument),
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum TimelineChangeWireOwned {
    Add {
        instrument: InstrumentWireOwned,
        provenance: String,
    },
    Remove {
        instrument: InstrumentWireOwned,
    },
}

impl TimelineChangeWireOwned {
    fn into_change(self) -> UniverseMemberChange {
        match self {
            Self::Add {
                instrument,
                provenance,
            } => UniverseMemberChange::Add {
                instrument: instrument.into_instrument(),
                provenance,
            },
            Self::Remove { instrument } => UniverseMemberChange::Remove {
                instrument: instrument.into_instrument(),
            },
        }
    }
}

#[derive(Serialize)]
struct TimelineBatchWire {
    effective_ns: i64,
    changes: Vec<TimelineChangeWire>,
}

impl TimelineBatchWire {
    fn from_batch(batch: &UniverseTimelineBatch) -> Self {
        Self {
            effective_ns: batch.effective_ns,
            changes: batch
                .changes
                .iter()
                .map(TimelineChangeWire::from_change)
                .collect(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TimelineBatchWireOwned {
    effective_ns: i64,
    changes: Vec<TimelineChangeWireOwned>,
}

impl TimelineBatchWireOwned {
    fn into_batch(self) -> Result<UniverseTimelineBatch> {
        if self.changes.is_empty() {
            return Err(validation(
                "historical universe plan timeline batch must contain changes",
            ));
        }
        Ok(UniverseTimelineBatch {
            effective_ns: self.effective_ns,
            changes: self
                .changes
                .into_iter()
                .map(TimelineChangeWireOwned::into_change)
                .collect(),
        })
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BudgetWire {
    max_batches: usize,
    max_changes: usize,
}

impl BudgetWire {
    const fn from_budget(budget: UniverseBudget) -> Self {
        Self {
            max_batches: budget.max_batches,
            max_changes: budget.max_changes,
        }
    }

    const fn into_budget(self) -> UniverseBudget {
        UniverseBudget {
            max_batches: self.max_batches,
            max_changes: self.max_changes,
        }
    }
}

#[derive(Serialize)]
struct IdentityWire<'a> {
    language_version: u32,
    normalized_ast_json: &'a str,
    normalized_ast_sha256: &'a str,
    canonicalizer_identity: &'a str,
    compiler_identity: &'a str,
    input_sources_sha256: Option<&'a str>,
    acquisition_sha256: &'a str,
    semantic_catalog_sha256: &'a str,
    calendar_identity: &'a str,
    proof: ProofWire,
    execution_sha256: &'a str,
    continuous_identity: Option<&'a str>,
    ranking_identity: Option<&'a str>,
}

impl<'a> IdentityWire<'a> {
    fn from_identity(identity: &'a HistoricalUniversePlanIdentity) -> Self {
        Self {
            language_version: identity.language_version,
            normalized_ast_json: &identity.normalized_ast_json,
            normalized_ast_sha256: &identity.normalized_ast_sha256,
            canonicalizer_identity: &identity.canonicalizer_identity,
            compiler_identity: &identity.compiler_identity,
            input_sources_sha256: identity.input_sources_sha256.as_deref(),
            acquisition_sha256: &identity.acquisition_sha256,
            semantic_catalog_sha256: &identity.semantic_catalog_sha256,
            calendar_identity: &identity.calendar_identity,
            proof: ProofWire::from_proof(identity.proof),
            execution_sha256: &identity.execution_sha256,
            continuous_identity: identity.continuous_identity.as_deref(),
            ranking_identity: identity.ranking_identity.as_deref(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityWireOwned {
    language_version: u32,
    normalized_ast_json: String,
    normalized_ast_sha256: String,
    canonicalizer_identity: String,
    compiler_identity: String,
    input_sources_sha256: Option<String>,
    acquisition_sha256: String,
    semantic_catalog_sha256: String,
    calendar_identity: String,
    proof: ProofWire,
    execution_sha256: String,
    continuous_identity: Option<String>,
    ranking_identity: Option<String>,
}

impl IdentityWireOwned {
    fn into_identity(self) -> Result<HistoricalUniversePlanIdentity> {
        let identity = HistoricalUniversePlanIdentity {
            language_version: self.language_version,
            normalized_ast_json: self.normalized_ast_json,
            normalized_ast_sha256: self.normalized_ast_sha256,
            canonicalizer_identity: self.canonicalizer_identity,
            compiler_identity: self.compiler_identity,
            input_sources_sha256: self.input_sources_sha256,
            acquisition_sha256: self.acquisition_sha256,
            semantic_catalog_sha256: self.semantic_catalog_sha256,
            calendar_identity: self.calendar_identity,
            proof: self.proof.into_proof(),
            execution_sha256: self.execution_sha256,
            continuous_identity: self.continuous_identity,
            ranking_identity: self.ranking_identity,
        };
        identity.validate()?;
        Ok(identity)
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProofWire {
    ProviderCurrentObserved,
    ProviderHistoryObserved,
    AuthoritativeLifecycle,
}

impl ProofWire {
    fn from_proof(proof: HistoricalCatalogProof) -> Self {
        match proof {
            HistoricalCatalogProof::ProviderCurrentObserved => Self::ProviderCurrentObserved,
            HistoricalCatalogProof::ProviderHistoryObserved => Self::ProviderHistoryObserved,
            HistoricalCatalogProof::AuthoritativeLifecycle => Self::AuthoritativeLifecycle,
        }
    }

    fn into_proof(self) -> HistoricalCatalogProof {
        match self {
            Self::ProviderCurrentObserved => HistoricalCatalogProof::ProviderCurrentObserved,
            Self::ProviderHistoryObserved => HistoricalCatalogProof::ProviderHistoryObserved,
            Self::AuthoritativeLifecycle => HistoricalCatalogProof::AuthoritativeLifecycle,
        }
    }
}

#[derive(Serialize)]
struct ExecutionWire {
    execution_sha256: String,
    visible_membership_sha256: String,
    dependency_set_sha256: String,
    kind_targets: Vec<KindTargetsWire>,
    dependencies: Vec<DependencyWire>,
}

impl ExecutionWire {
    fn from_execution(execution: &HistoricalUniversePlanExecution) -> Self {
        Self {
            execution_sha256: execution.execution_sha256.clone(),
            visible_membership_sha256: execution.visible_membership_sha256.clone(),
            dependency_set_sha256: execution.dependency_set_sha256.clone(),
            kind_targets: kind_targets_from_execution(execution),
            dependencies: execution
                .dependencies
                .iter()
                .map(DependencyWire::from_dependency)
                .collect(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionWireOwned {
    execution_sha256: String,
    visible_membership_sha256: String,
    dependency_set_sha256: String,
    kind_targets: Vec<KindTargetsWireOwned>,
    dependencies: Vec<DependencyWireOwned>,
}

impl ExecutionWireOwned {
    fn into_execution(self) -> Result<HistoricalUniversePlanExecution> {
        let expected_kinds = [
            DataKindWire::Tick,
            DataKindWire::Minute,
            DataKindWire::Daily,
        ];
        if self.kind_targets.len() != expected_kinds.len()
            || self
                .kind_targets
                .iter()
                .zip(expected_kinds)
                .any(|(entry, expected)| entry.kind != expected)
        {
            return Err(validation(
                "historical universe plan execution kind targets must contain tick/minute/daily in order",
            ));
        }
        if self
            .dependencies
            .windows(2)
            .any(|pair| pair[0].source_symbol >= pair[1].source_symbol)
        {
            return Err(validation(
                "historical universe plan dependencies must be strictly sorted",
            ));
        }
        let dependencies = self
            .dependencies
            .into_iter()
            .map(DependencyWireOwned::into_dependency)
            .collect::<Result<Vec<_>>>()?;
        let mut resolved_targets_sha256 = BTreeMap::new();
        let mut targets = BTreeMap::new();
        for entry in self.kind_targets {
            let (kind, hash, values) = entry.into_targets()?;
            resolved_targets_sha256.insert(kind, hash);
            targets.insert(kind, values);
        }
        let execution = HistoricalUniversePlanExecution::new(
            self.visible_membership_sha256,
            self.dependency_set_sha256,
            resolved_targets_sha256,
            dependencies,
            targets,
        )?;
        if execution.execution_sha256 != self.execution_sha256 {
            return Err(validation(
                "historical universe plan execution wire hash mismatch",
            ));
        }
        Ok(execution)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DataKindWire {
    Tick,
    Minute,
    Daily,
}

impl From<HistoricalDataKind> for DataKindWire {
    fn from(value: HistoricalDataKind) -> Self {
        match value {
            HistoricalDataKind::Tick => Self::Tick,
            HistoricalDataKind::Minute => Self::Minute,
            HistoricalDataKind::Daily => Self::Daily,
        }
    }
}

impl From<DataKindWire> for HistoricalDataKind {
    fn from(value: DataKindWire) -> Self {
        match value {
            DataKindWire::Tick => Self::Tick,
            DataKindWire::Minute => Self::Minute,
            DataKindWire::Daily => Self::Daily,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DependencyRoleWire {
    VisiblePhysical,
    ContinuousUnderlying,
    IndexSeries,
}

impl DependencyRoleWire {
    fn from_role(role: &HistoricalDependencyRole) -> Self {
        match role {
            HistoricalDependencyRole::VisiblePhysical => Self::VisiblePhysical,
            HistoricalDependencyRole::ContinuousUnderlying => Self::ContinuousUnderlying,
            HistoricalDependencyRole::IndexSeries => Self::IndexSeries,
        }
    }
}

impl From<DependencyRoleWire> for HistoricalDependencyRole {
    fn from(value: DependencyRoleWire) -> Self {
        match value {
            DependencyRoleWire::VisiblePhysical => Self::VisiblePhysical,
            DependencyRoleWire::ContinuousUnderlying => Self::ContinuousUnderlying,
            DependencyRoleWire::IndexSeries => Self::IndexSeries,
        }
    }
}

#[derive(Serialize)]
struct DependencyWire {
    source_symbol: String,
    roles: Vec<DependencyRoleWire>,
    listing_start_ns: i64,
}

impl DependencyWire {
    fn from_dependency(dependency: &HistoricalUniverseDependency) -> Self {
        Self {
            source_symbol: dependency.source_symbol.clone(),
            roles: dependency
                .roles
                .iter()
                .map(DependencyRoleWire::from_role)
                .collect(),
            listing_start_ns: dependency.listing_start_ns,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DependencyWireOwned {
    source_symbol: String,
    roles: Vec<DependencyRoleWire>,
    listing_start_ns: i64,
}

impl DependencyWireOwned {
    fn into_dependency(self) -> Result<HistoricalUniverseDependency> {
        Ok(HistoricalUniverseDependency {
            source_symbol: self.source_symbol,
            roles: ordered_set("dependency roles", self.roles)?
                .into_iter()
                .map(Into::into)
                .collect(),
            listing_start_ns: self.listing_start_ns,
        })
    }
}

#[derive(Serialize)]
struct KindTargetWire {
    source_symbol: String,
    start_ns: i64,
    end_ns: i64,
}

impl KindTargetWire {
    fn from_target(target: &HistoricalUniverseKindTarget) -> Self {
        Self {
            source_symbol: target.source_symbol.clone(),
            start_ns: target.start_ns,
            end_ns: target.end_ns,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KindTargetWireOwned {
    source_symbol: String,
    start_ns: i64,
    end_ns: i64,
}

impl KindTargetWireOwned {
    fn into_target(self) -> HistoricalUniverseKindTarget {
        HistoricalUniverseKindTarget {
            source_symbol: self.source_symbol,
            start_ns: self.start_ns,
            end_ns: self.end_ns,
        }
    }
}

#[derive(Serialize)]
struct KindTargetsWire {
    kind: DataKindWire,
    resolved_targets_sha256: String,
    targets: Vec<KindTargetWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KindTargetsWireOwned {
    kind: DataKindWire,
    resolved_targets_sha256: String,
    targets: Vec<KindTargetWireOwned>,
}

impl KindTargetsWireOwned {
    fn into_targets(
        self,
    ) -> Result<(
        HistoricalDataKind,
        String,
        Vec<HistoricalUniverseKindTarget>,
    )> {
        if self.targets.is_empty()
            || self.targets.windows(2).any(|pair| {
                (&pair[0].source_symbol, pair[0].start_ns, pair[0].end_ns)
                    >= (&pair[1].source_symbol, pair[1].start_ns, pair[1].end_ns)
            })
        {
            return Err(validation(
                "historical universe plan kind targets must be nonempty and strictly sorted",
            ));
        }
        Ok((
            self.kind.into(),
            self.resolved_targets_sha256,
            self.targets
                .into_iter()
                .map(KindTargetWireOwned::into_target)
                .collect(),
        ))
    }
}

fn kind_targets_from_execution(
    execution: &HistoricalUniversePlanExecution,
) -> Vec<KindTargetsWire> {
    [
        HistoricalDataKind::Tick,
        HistoricalDataKind::Minute,
        HistoricalDataKind::Daily,
    ]
    .into_iter()
    .map(|kind| KindTargetsWire {
        kind: kind.into(),
        resolved_targets_sha256: execution
            .resolved_targets_sha256
            .get(&kind)
            .expect("validated execution contains every history kind")
            .clone(),
        targets: execution
            .targets
            .get(&kind)
            .expect("validated execution contains every history kind")
            .iter()
            .map(KindTargetWire::from_target)
            .collect(),
    })
    .collect()
}

fn normalize_execution_parts(
    dependencies: &mut [HistoricalUniverseDependency],
    targets: &mut BTreeMap<HistoricalDataKind, Vec<HistoricalUniverseKindTarget>>,
) {
    dependencies.sort_by(|left, right| left.source_symbol.cmp(&right.source_symbol));
    for values in targets.values_mut() {
        values.sort_by(|left, right| {
            (&left.source_symbol, left.start_ns, left.end_ns).cmp(&(
                &right.source_symbol,
                right.start_ns,
                right.end_ns,
            ))
        });
    }
}

fn canonical_visible_membership_sha256(timeline: &HistoricalUniverseTimeline) -> Result<String> {
    let batches = timeline
        .batches
        .iter()
        .map(TimelineBatchWire::from_batch)
        .collect::<Vec<_>>();
    Ok(sha256(&serde_json::to_vec(&batches)?))
}

fn canonical_dependency_set_sha256(
    dependencies: &[HistoricalUniverseDependency],
) -> Result<String> {
    let dependencies = dependencies
        .iter()
        .map(DependencyWire::from_dependency)
        .collect::<Vec<_>>();
    Ok(sha256(&serde_json::to_vec(&dependencies)?))
}

fn canonical_target_set_sha256(targets: &[HistoricalUniverseKindTarget]) -> Result<String> {
    let targets = targets
        .iter()
        .map(KindTargetWire::from_target)
        .collect::<Vec<_>>();
    Ok(sha256(&serde_json::to_vec(&targets)?))
}

fn canonical_resolved_targets_sha256(
    targets: &BTreeMap<HistoricalDataKind, Vec<HistoricalUniverseKindTarget>>,
) -> Result<BTreeMap<HistoricalDataKind, String>> {
    targets
        .iter()
        .map(|(kind, targets)| Ok((*kind, canonical_target_set_sha256(targets)?)))
        .collect()
}

fn ordered_set<T>(field: &str, values: Vec<T>) -> Result<BTreeSet<T>>
where
    T: Ord,
{
    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(validation(format!(
            "historical universe plan {field} must be strictly sorted without duplicates",
        )));
    }
    Ok(values.into_iter().collect())
}

fn ordered_string_i64_map(
    field: &str,
    values: Vec<(String, i64)>,
) -> Result<BTreeMap<String, i64>> {
    let mut map = BTreeMap::new();
    let mut previous = None;
    for (key, value) in values {
        if previous
            .as_ref()
            .is_some_and(|previous: &String| previous >= &key)
        {
            return Err(validation(format!(
                "historical universe plan {field} must be strictly sorted without duplicates",
            )));
        }
        previous = Some(key.clone());
        map.insert(key, value);
    }
    Ok(map)
}

#[derive(Serialize)]
struct ArtifactWireV5<'a> {
    plan_version: u32,
    plan_sha256: &'a str,
    timeline: TimelineWire,
    budget: BudgetWire,
    identity: IdentityWire<'a>,
    execution: ExecutionWire,
}

impl<'a> ArtifactWireV5<'a> {
    fn from_plan(plan: &'a HistoricalUniversePlan) -> Self {
        Self {
            plan_version: plan.plan_version(),
            plan_sha256: &plan.plan_sha256,
            timeline: TimelineWire::from_timeline(&plan.timeline),
            budget: BudgetWire::from_budget(plan.budget),
            identity: IdentityWire::from_identity(&plan.identity),
            execution: ExecutionWire::from_execution(&plan.execution),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactWireV5Owned {
    plan_version: u32,
    plan_sha256: String,
    timeline: TimelineWireOwned,
    budget: BudgetWire,
    identity: IdentityWireOwned,
    execution: ExecutionWireOwned,
}

impl ArtifactWireV5Owned {
    fn into_plan(self) -> Result<HistoricalUniversePlan> {
        if self.plan_version != HISTORICAL_UNIVERSE_PLAN_VERSION {
            return Err(validation(format!(
                "historical universe plan version must be {HISTORICAL_UNIVERSE_PLAN_VERSION}"
            )));
        }
        let plan = HistoricalUniversePlan {
            plan_sha256: self.plan_sha256,
            timeline: self.timeline.into_timeline()?,
            budget: self.budget.into_budget(),
            identity: self.identity.into_identity()?,
            execution: self.execution.into_execution()?,
        };
        plan.verify()?;
        Ok(plan)
    }
}

fn hash_with_domain(domain: &[u8], bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}

fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn validate_sha256(name: &str, value: &str) -> Result<()> {
    let hex = value.strip_prefix("sha256:").ok_or_else(|| {
        validation(format!(
            "historical universe plan {name} must have sha256: prefix"
        ))
    })?;
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(validation(format!(
            "historical universe plan {name} must contain a SHA-256 digest"
        )));
    }
    Ok(())
}

fn validation(message: impl Into<String>) -> DataError {
    DataError::Validation(message.into())
}
