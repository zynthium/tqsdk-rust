use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::historical_universe_v4_resolution::HISTORICAL_UNIVERSE_CONTINUOUS_ID;
use crate::{
    DataError, DerivedView, DynamicUniverseScope, HistoricalCatalogProof, HistoricalDataKind,
    HistoricalUniverseDependency, HistoricalUniverseKindTarget, HistoricalUniversePlan,
    HistoricalUniversePlanV3Execution, HistoricalUniverseTimeline, Result,
    UNIVERSE_CANONICALIZER_ID, UNIVERSE_COMPILER_ID, UNIVERSE_LANGUAGE_VERSION, UniverseBudget,
    UniverseSpec, UniverseTimelineBatch, UniverseView,
};

const PLAN_VERSION_V4: u32 = 4;
const PLAN_HASH_DOMAIN_V4: &str = "tqsdk.historical-universe-plan.v4";
const EXECUTION_HASH_DOMAIN_V4: &str = "tqsdk.historical-universe-execution.v4";
const UNIVERSE_AST_HASH_DOMAIN: &[u8] = b"tqsdk.universe.ast.v2\0";

/// V4 plan identity. Fields stay private so persisted additions remain source-compatible.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct HistoricalUniversePlanV4Identity {
    language_version: u32,
    normalized_ast_json: String,
    normalized_ast_sha256: String,
    canonicalizer_identity: String,
    compiler_identity: String,
    input_sources_sha256: Option<String>,
    acquisition_sha256: String,
    semantic_catalog_sha256: String,
    calendar_identity: String,
    proof: HistoricalCatalogProof,
    execution_sha256: String,
    rollback_v3_plan_sha256: String,
    continuous_identity: Option<String>,
    ranking_identity: Option<String>,
}

impl HistoricalUniversePlanV4Identity {
    #[must_use]
    pub fn builder(spec: &UniverseSpec) -> HistoricalUniversePlanV4IdentityBuilder {
        HistoricalUniversePlanV4IdentityBuilder::new(spec)
    }

    #[must_use]
    pub const fn language_version(&self) -> u32 {
        self.language_version
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
    pub fn canonicalizer_identity(&self) -> &str {
        &self.canonicalizer_identity
    }

    #[must_use]
    pub fn compiler_identity(&self) -> &str {
        &self.compiler_identity
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
    pub fn rollback_v3_plan_sha256(&self) -> &str {
        &self.rollback_v3_plan_sha256
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
                "historical universe V4 language version mismatch",
            ));
        }
        if self.canonicalizer_identity != UNIVERSE_CANONICALIZER_ID
            || self.compiler_identity != UNIVERSE_COMPILER_ID
        {
            return Err(validation(
                "historical universe V4 canonicalizer/compiler identity mismatch",
            ));
        }
        let spec = UniverseSpec::from_canonical_ast_json(self.normalized_ast_json.as_bytes())
            .map_err(|error| validation(error.to_string()))?;
        if spec.mode() != crate::UniverseMode::Timeline {
            return Err(validation(
                "historical universe V4 requires a normalized timeline V2 AST",
            ));
        }
        if self.normalized_ast_sha256
            != hash_with_domain(
                UNIVERSE_AST_HASH_DOMAIN,
                self.normalized_ast_json.as_bytes(),
            )
        {
            return Err(validation(
                "historical universe V4 normalized AST hash mismatch",
            ));
        }
        for (field, value) in [
            ("normalized_ast_sha256", self.normalized_ast_sha256.as_str()),
            ("acquisition_sha256", self.acquisition_sha256.as_str()),
            (
                "semantic_catalog_sha256",
                self.semantic_catalog_sha256.as_str(),
            ),
            ("execution_sha256", self.execution_sha256.as_str()),
            (
                "rollback_v3_plan_sha256",
                self.rollback_v3_plan_sha256.as_str(),
            ),
        ] {
            validate_sha256(field, value)?;
        }
        if let Some(input_sources_sha256) = &self.input_sources_sha256 {
            validate_sha256("input_sources_sha256", input_sources_sha256)?;
        }
        if self.calendar_identity.trim().is_empty() {
            return Err(validation(
                "historical universe V4 calendar identity must not be empty",
            ));
        }
        if !matches!(
            self.proof,
            HistoricalCatalogProof::AuthoritativeLifecycle
                | HistoricalCatalogProof::ProviderHistoryObserved
        ) {
            return Err(validation(
                "historical universe V4 requires executable historical membership proof",
            ));
        }
        for (field, identity) in [
            ("continuous_identity", self.continuous_identity.as_deref()),
            ("ranking_identity", self.ranking_identity.as_deref()),
        ] {
            if identity.is_some_and(|value| value.trim().is_empty()) {
                return Err(validation(format!(
                    "historical universe V4 {field} must not be empty"
                )));
            }
        }
        let has_continuous = spec
            .includes()
            .iter()
            .any(|selector| selector.view() == UniverseView::Continuous);
        if self
            .continuous_identity
            .as_deref()
            .is_some_and(|identity| identity != HISTORICAL_UNIVERSE_CONTINUOUS_ID)
        {
            return Err(validation(
                "historical universe V4 continuous identity does not match the pinned mapping",
            ));
        }
        if has_continuous && self.continuous_identity.is_none() {
            return Err(validation(
                "historical universe V4 continuous view requires the pinned continuous identity",
            ));
        }
        let has_ranking = spec
            .includes()
            .iter()
            .any(|selector| matches!(selector.view(), UniverseView::Main | UniverseView::Top(_)));
        if has_ranking != self.ranking_identity.is_some() {
            return Err(validation(
                "historical universe V4 ranking identity presence does not match main/top views",
            ));
        }
        Ok(())
    }
}

/// Validated builder for [`HistoricalUniversePlanV4Identity`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct HistoricalUniversePlanV4IdentityBuilder {
    normalized_ast_json: String,
    normalized_ast_sha256: String,
    input_sources_sha256: Option<String>,
    acquisition_sha256: Option<String>,
    semantic_catalog_sha256: Option<String>,
    calendar_identity: Option<String>,
    proof: Option<HistoricalCatalogProof>,
    execution_sha256: Option<String>,
    rollback_v3_plan_sha256: Option<String>,
    continuous_identity: Option<String>,
    ranking_identity: Option<String>,
}

impl HistoricalUniversePlanV4IdentityBuilder {
    fn new(spec: &UniverseSpec) -> Self {
        Self {
            normalized_ast_json: String::from_utf8(spec.canonical_ast_json_bytes().to_vec())
                .expect("Universe V2 canonical AST JSON is UTF-8"),
            normalized_ast_sha256: spec.canonical_ast_hash().to_string(),
            input_sources_sha256: None,
            acquisition_sha256: None,
            semantic_catalog_sha256: None,
            calendar_identity: None,
            proof: None,
            execution_sha256: None,
            rollback_v3_plan_sha256: None,
            continuous_identity: None,
            ranking_identity: None,
        }
    }

    #[must_use]
    pub fn input_sources_sha256(mut self, value: impl Into<String>) -> Self {
        self.input_sources_sha256 = Some(value.into());
        self
    }

    #[must_use]
    pub fn acquisition_sha256(mut self, value: impl Into<String>) -> Self {
        self.acquisition_sha256 = Some(value.into());
        self
    }

    #[must_use]
    pub fn semantic_catalog_sha256(mut self, value: impl Into<String>) -> Self {
        self.semantic_catalog_sha256 = Some(value.into());
        self
    }

    #[must_use]
    pub fn calendar_identity(mut self, value: impl Into<String>) -> Self {
        self.calendar_identity = Some(value.into());
        self
    }

    #[must_use]
    pub const fn proof(mut self, value: HistoricalCatalogProof) -> Self {
        self.proof = Some(value);
        self
    }

    #[must_use]
    pub fn execution_sha256(mut self, value: impl Into<String>) -> Self {
        self.execution_sha256 = Some(value.into());
        self
    }

    #[must_use]
    pub fn rollback_v3_plan_sha256(mut self, value: impl Into<String>) -> Self {
        self.rollback_v3_plan_sha256 = Some(value.into());
        self
    }

    #[must_use]
    pub fn continuous_identity(mut self, value: impl Into<String>) -> Self {
        self.continuous_identity = Some(value.into());
        self
    }

    #[must_use]
    pub fn ranking_identity(mut self, value: impl Into<String>) -> Self {
        self.ranking_identity = Some(value.into());
        self
    }

    pub fn build(self) -> Result<HistoricalUniversePlanV4Identity> {
        let identity = HistoricalUniversePlanV4Identity {
            language_version: UNIVERSE_LANGUAGE_VERSION,
            normalized_ast_json: self.normalized_ast_json,
            normalized_ast_sha256: self.normalized_ast_sha256,
            canonicalizer_identity: UNIVERSE_CANONICALIZER_ID.to_string(),
            compiler_identity: UNIVERSE_COMPILER_ID.to_string(),
            input_sources_sha256: self.input_sources_sha256,
            acquisition_sha256: required("acquisition_sha256", self.acquisition_sha256)?,
            semantic_catalog_sha256: required(
                "semantic_catalog_sha256",
                self.semantic_catalog_sha256,
            )?,
            calendar_identity: required("calendar_identity", self.calendar_identity)?,
            proof: self
                .proof
                .ok_or_else(|| validation("historical universe V4 proof is required"))?,
            execution_sha256: required("execution_sha256", self.execution_sha256)?,
            rollback_v3_plan_sha256: required(
                "rollback_v3_plan_sha256",
                self.rollback_v3_plan_sha256,
            )?,
            continuous_identity: self.continuous_identity,
            ranking_identity: self.ranking_identity,
        };
        identity.validate()?;
        Ok(identity)
    }
}

/// Immutable execution closure for V4 history plans.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct HistoricalUniversePlanV4Execution {
    execution_sha256: String,
    visible_membership_sha256: String,
    dependency_set_sha256: String,
    resolved_targets_sha256: BTreeMap<HistoricalDataKind, String>,
    dependencies: Vec<HistoricalUniverseDependency>,
    targets: BTreeMap<HistoricalDataKind, Vec<HistoricalUniverseKindTarget>>,
}

impl HistoricalUniversePlanV4Execution {
    pub fn new(
        visible_membership_sha256: impl Into<String>,
        dependency_set_sha256: impl Into<String>,
        resolved_targets_sha256: BTreeMap<HistoricalDataKind, String>,
        dependencies: Vec<HistoricalUniverseDependency>,
        targets: BTreeMap<HistoricalDataKind, Vec<HistoricalUniverseKindTarget>>,
    ) -> Result<Self> {
        let v3 = HistoricalUniversePlanV3Execution::new(
            visible_membership_sha256,
            dependency_set_sha256,
            resolved_targets_sha256,
            dependencies,
            targets,
        )?;
        Self::from_v3(&v3)
    }

    pub fn from_v3(execution: &HistoricalUniversePlanV3Execution) -> Result<Self> {
        execution.validate()?;
        let mut value = Self {
            execution_sha256: String::new(),
            visible_membership_sha256: execution.visible_membership_sha256.clone(),
            dependency_set_sha256: execution.dependency_set_sha256.clone(),
            resolved_targets_sha256: execution.resolved_targets_sha256.clone(),
            dependencies: execution.dependencies.clone(),
            targets: execution.targets.clone(),
        };
        value.execution_sha256 = value.computed_sha256()?;
        value.validate()?;
        Ok(value)
    }

    pub fn to_v3(&self) -> Result<HistoricalUniversePlanV3Execution> {
        self.validate()?;
        HistoricalUniversePlanV3Execution::new(
            self.visible_membership_sha256.clone(),
            self.dependency_set_sha256.clone(),
            self.resolved_targets_sha256.clone(),
            self.dependencies.clone(),
            self.targets.clone(),
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
        self.to_v3_body()?.validate()?;
        if self.execution_sha256 != self.computed_sha256()? {
            return Err(validation("historical universe V4 execution hash mismatch"));
        }
        Ok(())
    }

    fn to_v3_body(&self) -> Result<HistoricalUniversePlanV3Execution> {
        HistoricalUniversePlanV3Execution::new(
            self.visible_membership_sha256.clone(),
            self.dependency_set_sha256.clone(),
            self.resolved_targets_sha256.clone(),
            self.dependencies.clone(),
            self.targets.clone(),
        )
    }

    fn computed_sha256(&self) -> Result<String> {
        let bytes = serde_json::to_vec(&(
            EXECUTION_HASH_DOMAIN_V4,
            ExecutionBodyWire::from_execution(self),
        ))?;
        Ok(sha256(&bytes))
    }
}

/// Source-compatible V4 history plan with a fixed private wire contract.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct HistoricalUniversePlanV4 {
    plan_sha256: String,
    timeline: HistoricalUniverseTimeline,
    budget: UniverseBudget,
    identity: HistoricalUniversePlanV4Identity,
    execution: HistoricalUniversePlanV4Execution,
}

impl HistoricalUniversePlanV4 {
    pub fn new(
        timeline: HistoricalUniverseTimeline,
        budget: UniverseBudget,
        identity: HistoricalUniversePlanV4Identity,
        execution: HistoricalUniversePlanV4Execution,
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
        PLAN_VERSION_V4
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
    pub const fn identity(&self) -> &HistoricalUniversePlanV4Identity {
        &self.identity
    }

    #[must_use]
    pub const fn execution(&self) -> &HistoricalUniversePlanV4Execution {
        &self.execution
    }

    pub fn verify(&self) -> Result<()> {
        validate_sha256("plan_sha256", &self.plan_sha256)?;
        self.validate_body()?;
        if self.plan_sha256 != self.computed_sha256()? {
            return Err(validation("historical universe V4 plan hash mismatch"));
        }
        Ok(())
    }

    fn validate_body(&self) -> Result<()> {
        self.timeline.clone().prepare(self.budget)?;
        self.identity.validate()?;
        self.execution.validate()?;
        if self.identity.execution_sha256 != self.execution.execution_sha256 {
            return Err(validation(
                "historical universe V4 identity does not pin execution",
            ));
        }
        if self.identity.calendar_identity != self.timeline.calendar_identity {
            return Err(validation(
                "historical universe V4 calendar identity mismatch",
            ));
        }
        let membership_sha256 = sha256(&serde_json::to_vec(&self.timeline.batches)?);
        if membership_sha256 != self.execution.visible_membership_sha256 {
            return Err(validation(
                "historical universe V4 visible membership hash mismatch",
            ));
        }
        let timeline_requires_continuous = self
            .timeline
            .derived_views
            .contains(&DerivedView::Continuous);
        let execution_requires_continuous = self.execution.dependencies.iter().any(|dependency| {
            dependency
                .roles
                .contains(&crate::HistoricalDependencyRole::ContinuousUnderlying)
        });
        if timeline_requires_continuous != execution_requires_continuous {
            return Err(validation(
                "historical universe V4 continuous membership/dependency closure mismatch",
            ));
        }
        if timeline_requires_continuous != self.identity.continuous_identity.is_some() {
            return Err(validation(
                "historical universe V4 continuous identity does not match materialized execution",
            ));
        }
        Ok(())
    }

    fn computed_sha256(&self) -> Result<String> {
        let bytes = serde_json::to_vec(&(
            PLAN_HASH_DOMAIN_V4,
            PLAN_VERSION_V4,
            TimelineWire::from_timeline(&self.timeline),
            BudgetWire::from_budget(self.budget),
            IdentityWire::from_identity(&self.identity),
            ExecutionWire::from_execution(&self.execution),
        ))?;
        Ok(sha256(&bytes))
    }

    pub(crate) fn canonical_json_bytes(&self) -> Result<Vec<u8>> {
        self.verify()?;
        Ok(serde_json::to_vec(&ArtifactWireV4::from_plan(self))?)
    }
}

impl Serialize for HistoricalUniversePlanV4 {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        ArtifactWireV4::from_plan(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for HistoricalUniversePlanV4 {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ArtifactWireV4Owned::deserialize(deserializer)?;
        wire.into_plan().map_err(serde::de::Error::custom)
    }
}

/// Version-dispatched history plan artifact. Serialization stays flat on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum HistoricalUniversePlanArtifact {
    Legacy(HistoricalUniversePlan),
    V4(HistoricalUniversePlanV4),
}

impl HistoricalUniversePlanArtifact {
    #[must_use]
    pub const fn plan_version(&self) -> u32 {
        match self {
            Self::Legacy(plan) => plan.plan_version,
            Self::V4(plan) => plan.plan_version(),
        }
    }

    #[must_use]
    pub fn plan_sha256(&self) -> &str {
        match self {
            Self::Legacy(plan) => &plan.plan_sha256,
            Self::V4(plan) => plan.plan_sha256(),
        }
    }

    pub fn verify(&self) -> Result<()> {
        match self {
            Self::Legacy(plan) => {
                if !(1..=3).contains(&plan.plan_version) {
                    return Err(validation(
                        "legacy historical universe artifact version must be 1..=3",
                    ));
                }
                plan.verify()
            }
            Self::V4(plan) => plan.verify(),
        }
    }

    pub(crate) fn canonical_json_bytes(&self) -> Result<Vec<u8>> {
        self.verify()?;
        match self {
            Self::Legacy(plan) => Ok(serde_json::to_vec(plan)?),
            Self::V4(plan) => plan.canonical_json_bytes(),
        }
    }
}

impl Serialize for HistoricalUniversePlanArtifact {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Legacy(plan) => plan.serialize(serializer),
            Self::V4(plan) => plan.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for HistoricalUniversePlanArtifact {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let version = value
            .get("plan_version")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| serde::de::Error::custom("historical plan lacks plan_version"))?;
        match version {
            1..=3 => serde_json::from_value::<HistoricalUniversePlan>(value)
                .map(Self::Legacy)
                .map_err(serde::de::Error::custom),
            4 => serde_json::from_value::<HistoricalUniversePlanV4>(value)
                .map(Self::V4)
                .map_err(serde::de::Error::custom),
            _ => Err(serde::de::Error::custom(format!(
                "unsupported historical universe plan version {version}"
            ))),
        }
    }
}

#[derive(Serialize)]
struct TimelineWire<'a> {
    catalog_id: &'a str,
    catalog_sha256: &'a str,
    calendar_identity: &'a str,
    start_ns: i64,
    end_ns: i64,
    scope: &'a DynamicUniverseScope,
    derived_views: &'a BTreeSet<DerivedView>,
    physical_listing_starts: &'a BTreeMap<String, i64>,
    batches: &'a [UniverseTimelineBatch],
}

impl<'a> TimelineWire<'a> {
    fn from_timeline(timeline: &'a HistoricalUniverseTimeline) -> Self {
        Self {
            catalog_id: &timeline.catalog_id,
            catalog_sha256: &timeline.catalog_sha256,
            calendar_identity: &timeline.calendar_identity,
            start_ns: timeline.start_ns,
            end_ns: timeline.end_ns,
            scope: &timeline.scope,
            derived_views: &timeline.derived_views,
            physical_listing_starts: &timeline.physical_listing_starts,
            batches: &timeline.batches,
        }
    }
}

#[derive(Serialize)]
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
    proof: HistoricalCatalogProof,
    execution_sha256: &'a str,
    rollback_v3_plan_sha256: &'a str,
    continuous_identity: Option<&'a str>,
    ranking_identity: Option<&'a str>,
}

impl<'a> IdentityWire<'a> {
    fn from_identity(identity: &'a HistoricalUniversePlanV4Identity) -> Self {
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
            proof: identity.proof,
            execution_sha256: &identity.execution_sha256,
            rollback_v3_plan_sha256: &identity.rollback_v3_plan_sha256,
            continuous_identity: identity.continuous_identity.as_deref(),
            ranking_identity: identity.ranking_identity.as_deref(),
        }
    }
}

#[derive(Serialize)]
struct ExecutionBodyWire<'a> {
    visible_membership_sha256: &'a str,
    dependency_set_sha256: &'a str,
    resolved_targets_sha256: &'a BTreeMap<HistoricalDataKind, String>,
    dependencies: &'a [HistoricalUniverseDependency],
    targets: &'a BTreeMap<HistoricalDataKind, Vec<HistoricalUniverseKindTarget>>,
}

impl<'a> ExecutionBodyWire<'a> {
    fn from_execution(execution: &'a HistoricalUniversePlanV4Execution) -> Self {
        Self {
            visible_membership_sha256: &execution.visible_membership_sha256,
            dependency_set_sha256: &execution.dependency_set_sha256,
            resolved_targets_sha256: &execution.resolved_targets_sha256,
            dependencies: &execution.dependencies,
            targets: &execution.targets,
        }
    }
}

#[derive(Serialize)]
struct ExecutionWire<'a> {
    execution_sha256: &'a str,
    visible_membership_sha256: &'a str,
    dependency_set_sha256: &'a str,
    resolved_targets_sha256: &'a BTreeMap<HistoricalDataKind, String>,
    dependencies: &'a [HistoricalUniverseDependency],
    targets: &'a BTreeMap<HistoricalDataKind, Vec<HistoricalUniverseKindTarget>>,
}

impl<'a> ExecutionWire<'a> {
    fn from_execution(execution: &'a HistoricalUniversePlanV4Execution) -> Self {
        Self {
            execution_sha256: &execution.execution_sha256,
            visible_membership_sha256: &execution.visible_membership_sha256,
            dependency_set_sha256: &execution.dependency_set_sha256,
            resolved_targets_sha256: &execution.resolved_targets_sha256,
            dependencies: &execution.dependencies,
            targets: &execution.targets,
        }
    }
}

#[derive(Serialize)]
struct ArtifactWireV4<'a> {
    plan_version: u32,
    plan_sha256: &'a str,
    timeline: TimelineWire<'a>,
    budget: BudgetWire,
    v4_identity: IdentityWire<'a>,
    v4_execution: ExecutionWire<'a>,
}

impl<'a> ArtifactWireV4<'a> {
    fn from_plan(plan: &'a HistoricalUniversePlanV4) -> Self {
        Self {
            plan_version: PLAN_VERSION_V4,
            plan_sha256: &plan.plan_sha256,
            timeline: TimelineWire::from_timeline(&plan.timeline),
            budget: BudgetWire::from_budget(plan.budget),
            v4_identity: IdentityWire::from_identity(&plan.identity),
            v4_execution: ExecutionWire::from_execution(&plan.execution),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactWireV4Owned {
    plan_version: u32,
    plan_sha256: String,
    timeline: HistoricalUniverseTimeline,
    budget: UniverseBudget,
    v4_identity: IdentityWireOwned,
    v4_execution: ExecutionWireOwned,
}

impl ArtifactWireV4Owned {
    fn into_plan(self) -> Result<HistoricalUniversePlanV4> {
        if self.plan_version != PLAN_VERSION_V4 {
            return Err(validation("historical universe V4 plan_version mismatch"));
        }
        let identity = self.v4_identity.into_identity();
        let execution = self.v4_execution.into_execution();
        let plan = HistoricalUniversePlanV4::new(self.timeline, self.budget, identity, execution)?;
        if self.plan_sha256 != plan.plan_sha256 {
            return Err(validation("historical universe V4 plan hash mismatch"));
        }
        Ok(plan)
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
    proof: HistoricalCatalogProof,
    execution_sha256: String,
    rollback_v3_plan_sha256: String,
    continuous_identity: Option<String>,
    ranking_identity: Option<String>,
}

impl IdentityWireOwned {
    fn into_identity(self) -> HistoricalUniversePlanV4Identity {
        HistoricalUniversePlanV4Identity {
            language_version: self.language_version,
            normalized_ast_json: self.normalized_ast_json,
            normalized_ast_sha256: self.normalized_ast_sha256,
            canonicalizer_identity: self.canonicalizer_identity,
            compiler_identity: self.compiler_identity,
            input_sources_sha256: self.input_sources_sha256,
            acquisition_sha256: self.acquisition_sha256,
            semantic_catalog_sha256: self.semantic_catalog_sha256,
            calendar_identity: self.calendar_identity,
            proof: self.proof,
            execution_sha256: self.execution_sha256,
            rollback_v3_plan_sha256: self.rollback_v3_plan_sha256,
            continuous_identity: self.continuous_identity,
            ranking_identity: self.ranking_identity,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionWireOwned {
    execution_sha256: String,
    visible_membership_sha256: String,
    dependency_set_sha256: String,
    resolved_targets_sha256: BTreeMap<HistoricalDataKind, String>,
    dependencies: Vec<HistoricalUniverseDependency>,
    targets: BTreeMap<HistoricalDataKind, Vec<HistoricalUniverseKindTarget>>,
}

impl ExecutionWireOwned {
    fn into_execution(self) -> HistoricalUniversePlanV4Execution {
        HistoricalUniversePlanV4Execution {
            execution_sha256: self.execution_sha256,
            visible_membership_sha256: self.visible_membership_sha256,
            dependency_set_sha256: self.dependency_set_sha256,
            resolved_targets_sha256: self.resolved_targets_sha256,
            dependencies: self.dependencies,
            targets: self.targets,
        }
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

fn validate_sha256(field: &str, value: &str) -> Result<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(validation(format!("{field} must use sha256: prefix")));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(validation(format!(
            "{field} must contain 64 lowercase hexadecimal digits"
        )));
    }
    Ok(())
}

fn required(field: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| validation(format!("historical universe V4 {field} is required")))
}

fn validation(message: impl Into<String>) -> DataError {
    DataError::Validation(message.into())
}

impl fmt::Display for HistoricalUniversePlanArtifact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "historical-universe-plan-v{}:{}",
            self.plan_version(),
            self.plan_sha256()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SHA256: &str =
        "sha256:1111111111111111111111111111111111111111111111111111111111111111";

    fn identity_builder(spec: &UniverseSpec) -> HistoricalUniversePlanV4IdentityBuilder {
        HistoricalUniversePlanV4Identity::builder(spec)
            .acquisition_sha256(TEST_SHA256)
            .semantic_catalog_sha256(TEST_SHA256)
            .calendar_identity("calendar:test")
            .proof(HistoricalCatalogProof::AuthoritativeLifecycle)
            .execution_sha256(TEST_SHA256)
            .rollback_v3_plan_sha256(TEST_SHA256)
    }

    fn plain_identity() -> HistoricalUniversePlanV4Identity {
        let spec = UniverseSpec::parse_v2("timeline(contract:all)").unwrap();
        identity_builder(&spec).build().unwrap()
    }

    fn replace_ast(identity: &mut HistoricalUniversePlanV4Identity, normalized_ast_json: String) {
        identity.normalized_ast_sha256 =
            hash_with_domain(UNIVERSE_AST_HASH_DOMAIN, normalized_ast_json.as_bytes());
        identity.normalized_ast_json = normalized_ast_json;
    }

    #[test]
    fn identity_rejects_unknown_ast_fields_even_with_matching_hash() {
        let mut identity = plain_identity();
        let mut ast: serde_json::Value =
            serde_json::from_str(&identity.normalized_ast_json).unwrap();
        ast.as_object_mut()
            .unwrap()
            .insert("unknown".to_string(), serde_json::Value::Bool(true));
        replace_ast(&mut identity, serde_json::to_string(&ast).unwrap());

        assert!(identity.validate().is_err());
    }

    #[test]
    fn identity_rejects_noncanonical_ast_bytes_even_with_matching_hash() {
        let mut identity = plain_identity();
        let noncanonical = format!(" {}", identity.normalized_ast_json);
        replace_ast(&mut identity, noncanonical);

        assert!(identity.validate().is_err());
    }

    #[test]
    fn identity_requires_exact_continuous_identity_and_ast_view_requires_presence() {
        let continuous = UniverseSpec::parse_v2("timeline(continuous:SHFE.au)").unwrap();
        assert!(identity_builder(&continuous).build().is_err());
        assert!(
            identity_builder(&continuous)
                .continuous_identity("continuous:wrong")
                .build()
                .is_err()
        );
        identity_builder(&continuous)
            .continuous_identity(HISTORICAL_UNIVERSE_CONTINUOUS_ID)
            .build()
            .unwrap();

        let physical = UniverseSpec::parse_v2("timeline(contract:all)").unwrap();
        identity_builder(&physical)
            .continuous_identity(HISTORICAL_UNIVERSE_CONTINUOUS_ID)
            .build()
            .unwrap();
    }

    #[test]
    fn identity_requires_ranking_identity_exactly_when_main_or_top_is_present() {
        let main = UniverseSpec::parse_v2("timeline(main:all)").unwrap();
        assert!(identity_builder(&main).build().is_err());
        identity_builder(&main)
            .ranking_identity("ranking:test")
            .build()
            .unwrap();

        let physical = UniverseSpec::parse_v2("timeline(contract:all)").unwrap();
        assert!(
            identity_builder(&physical)
                .ranking_identity("ranking:test")
                .build()
                .is_err()
        );
    }
}
