use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    ActiveInterval, CatalogContract, CatalogSnapshot, DataError, DynamicUniverseScope,
    HistoricalCatalogProof, HistoricalUniversePlan, Result,
};

pub const HISTORICAL_UNIVERSE_ARTIFACT_FORMAT_VERSION: u32 = 1;
pub const HISTORICAL_UNIVERSE_ARTIFACT_NAMESPACE: &str = "historical-universe-v1";

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// History family whose first-available boundary was independently proven.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalDataKind {
    Tick,
    Minute,
    Daily,
}

/// Outcome of one provider native-daily observation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum HistoricalDailyObservationStatus {
    /// The requested range reached a terminal response. `first_row_ns=None`
    /// therefore proves a provider-empty range.
    #[default]
    Complete,
    /// The provider did not expose a usable history chart for this isolated
    /// contract before the bounded probe ended. This is not an assertion that
    /// the exchange never listed the contract.
    ProviderUnavailable,
}

impl HistoricalDailyObservationStatus {
    fn is_complete(status: &Self) -> bool {
        *status == Self::Complete
    }
}

/// Native-daily observation for one provider roster member.
///
/// A complete observation with `first_row_ns=None` is an explicitly observed
/// empty range. A `provider_unavailable` observation records that the provider
/// could not serve the chart; both outcomes are distinct from missing facts
/// and participate in artifact identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct HistoricalDailyObservation {
    pub range_start_ns: i64,
    pub range_end_ns: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_row_ns: Option<i64>,
    #[serde(
        default,
        skip_serializing_if = "HistoricalDailyObservationStatus::is_complete"
    )]
    pub status: HistoricalDailyObservationStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_unavailable_after_ns: Option<u64>,
}

impl HistoricalDailyObservation {
    pub fn new(range_start_ns: i64, range_end_ns: i64, first_row_ns: Option<i64>) -> Result<Self> {
        let observation = Self {
            range_start_ns,
            range_end_ns,
            first_row_ns,
            status: HistoricalDailyObservationStatus::Complete,
            provider_unavailable_after_ns: None,
        };
        observation.validate()?;
        Ok(observation)
    }

    pub fn provider_unavailable(
        range_start_ns: i64,
        range_end_ns: i64,
        unavailable_after_ns: u64,
    ) -> Result<Self> {
        let observation = Self {
            range_start_ns,
            range_end_ns,
            first_row_ns: None,
            status: HistoricalDailyObservationStatus::ProviderUnavailable,
            provider_unavailable_after_ns: Some(unavailable_after_ns),
        };
        observation.validate()?;
        Ok(observation)
    }

    fn validate(&self) -> Result<()> {
        if self.range_start_ns <= 0 || self.range_end_ns <= self.range_start_ns {
            return Err(validation(
                "historical daily observation requires a positive non-empty range",
            ));
        }
        if self
            .first_row_ns
            .is_some_and(|first| first < self.range_start_ns || first >= self.range_end_ns)
        {
            return Err(validation(
                "historical daily first row must stay inside observed range",
            ));
        }
        if self.status == HistoricalDailyObservationStatus::ProviderUnavailable
            && self.first_row_ns.is_some()
        {
            return Err(validation(
                "provider-unavailable daily observation cannot contain a first row",
            ));
        }
        match (self.status, self.provider_unavailable_after_ns) {
            (HistoricalDailyObservationStatus::Complete, None) => {}
            (HistoricalDailyObservationStatus::ProviderUnavailable, Some(value)) if value > 0 => {}
            (HistoricalDailyObservationStatus::Complete, Some(_)) => {
                return Err(validation(
                    "complete daily observation cannot contain an unavailable timeout",
                ));
            }
            (HistoricalDailyObservationStatus::ProviderUnavailable, _) => {
                return Err(validation(
                    "provider-unavailable daily observation requires a positive timeout",
                ));
            }
        }
        Ok(())
    }
}

/// Provider facts for one physical futures contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalAcquisitionContract {
    pub symbol: String,
    pub exchange_id: String,
    pub product_id: String,
    pub expired: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expire_datetime_ns: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub authoritative_lifecycle: Vec<ActiveInterval>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub first_available_data_ns: BTreeMap<HistoricalDataKind, i64>,
}

impl HistoricalAcquisitionContract {
    fn normalize(&mut self) -> Result<()> {
        self.symbol = normalized("symbol", std::mem::take(&mut self.symbol))?;
        self.exchange_id = normalized("exchange_id", std::mem::take(&mut self.exchange_id))?;
        self.product_id = normalized("product_id", std::mem::take(&mut self.product_id))?;
        if self.authoritative_lifecycle.is_empty() {
            return Ok(());
        }
        self.authoritative_lifecycle
            .sort_by_key(|interval| interval.start_ns);
        for (index, interval) in self.authoritative_lifecycle.iter().enumerate() {
            if interval.start_ns >= interval.end_ns {
                return Err(validation(format!(
                    "historical lifecycle for {} has an empty interval",
                    self.symbol
                )));
            }
            if index > 0 && self.authoritative_lifecycle[index - 1].end_ns > interval.start_ns {
                return Err(validation(format!(
                    "historical lifecycle for {} overlaps",
                    self.symbol
                )));
            }
        }
        Ok(())
    }
}

/// Immutable record of provider observations used to build a semantic catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalCatalogAcquisition {
    pub format_version: u32,
    pub acquisition_sha256: String,
    pub proof: HistoricalCatalogProof,
    pub source_identity: String,
    pub canonical_universe: String,
    pub requested_as_of_ns: i64,
    pub observed_at_ns: i64,
    pub complete: bool,
    pub roster_before: Vec<String>,
    pub roster_after: Vec<String>,
    pub contracts: Vec<HistoricalAcquisitionContract>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub provider_daily_observations: BTreeMap<String, HistoricalDailyObservation>,
}

#[derive(Serialize)]
struct HistoricalCatalogAcquisitionBody<'a> {
    format_version: u32,
    proof: HistoricalCatalogProof,
    source_identity: &'a str,
    canonical_universe: &'a str,
    requested_as_of_ns: i64,
    observed_at_ns: i64,
    complete: bool,
    roster_before: &'a [String],
    roster_after: &'a [String],
    contracts: &'a [HistoricalAcquisitionContract],
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    provider_daily_observations: &'a BTreeMap<String, HistoricalDailyObservation>,
}

impl HistoricalCatalogAcquisition {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        proof: HistoricalCatalogProof,
        source_identity: impl Into<String>,
        canonical_universe: impl Into<String>,
        requested_as_of_ns: i64,
        observed_at_ns: i64,
        complete: bool,
        roster_before: Vec<String>,
        roster_after: Vec<String>,
        contracts: Vec<HistoricalAcquisitionContract>,
    ) -> Result<Self> {
        let mut acquisition = Self {
            format_version: HISTORICAL_UNIVERSE_ARTIFACT_FORMAT_VERSION,
            acquisition_sha256: String::new(),
            proof,
            source_identity: source_identity.into(),
            canonical_universe: canonical_universe.into(),
            requested_as_of_ns,
            observed_at_ns,
            complete,
            roster_before,
            roster_after,
            contracts,
            provider_daily_observations: BTreeMap::new(),
        };
        acquisition.normalize()?;
        acquisition.acquisition_sha256 = sha256_identity(&acquisition.body_bytes()?);
        Ok(acquisition)
    }

    pub(crate) fn promote_provider_daily_history(
        mut self,
        source_identity: impl Into<String>,
        observations: BTreeMap<String, HistoricalDailyObservation>,
    ) -> Result<Self> {
        self.validate()?;
        if self.proof != HistoricalCatalogProof::ProviderCurrentObserved || !self.complete {
            return Err(validation(
                "provider daily promotion requires complete provider-current acquisition",
            ));
        }
        for contract in &mut self.contracts {
            let observation = observations.get(&contract.symbol).ok_or_else(|| {
                validation("provider daily observations must cover every acquired contract")
            })?;
            match observation.first_row_ns {
                Some(first_row_ns) => {
                    contract
                        .first_available_data_ns
                        .insert(HistoricalDataKind::Daily, first_row_ns);
                }
                None => {
                    contract
                        .first_available_data_ns
                        .remove(&HistoricalDataKind::Daily);
                }
            }
        }
        self.proof = HistoricalCatalogProof::ProviderHistoryObserved;
        self.source_identity = source_identity.into();
        self.provider_daily_observations = observations;
        self.acquisition_sha256.clear();
        self.normalize()?;
        self.acquisition_sha256 = sha256_identity(&self.body_bytes()?);
        Ok(self)
    }

    pub fn validate(&self) -> Result<()> {
        let mut normalized = self.clone();
        let claimed = normalized.acquisition_sha256.clone();
        normalized.acquisition_sha256.clear();
        normalized.normalize()?;
        if self
            != &(Self {
                acquisition_sha256: claimed.clone(),
                ..normalized.clone()
            })
        {
            return Err(validation(
                "historical catalog acquisition is not canonically ordered",
            ));
        }
        let expected = sha256_identity(&normalized.body_bytes()?);
        if claimed != expected {
            return Err(validation("historical catalog acquisition hash mismatch"));
        }
        Ok(())
    }

    fn normalize(&mut self) -> Result<()> {
        if self.format_version != HISTORICAL_UNIVERSE_ARTIFACT_FORMAT_VERSION {
            return Err(validation(format!(
                "unsupported historical catalog acquisition version {}",
                self.format_version
            )));
        }
        self.source_identity =
            normalized("source_identity", std::mem::take(&mut self.source_identity))?;
        self.canonical_universe = normalized(
            "canonical_universe",
            std::mem::take(&mut self.canonical_universe),
        )?;
        if self.requested_as_of_ns <= 0 || self.observed_at_ns <= 0 {
            return Err(validation(
                "historical acquisition timestamps must be positive",
            ));
        }
        normalize_roster(&mut self.roster_before)?;
        normalize_roster(&mut self.roster_after)?;
        for contract in &mut self.contracts {
            contract.normalize()?;
        }
        self.contracts
            .sort_by(|left, right| left.symbol.cmp(&right.symbol));
        if self
            .contracts
            .windows(2)
            .any(|pair| pair[0].symbol == pair[1].symbol)
        {
            return Err(validation(
                "historical catalog acquisition contains duplicate contracts",
            ));
        }
        let contract_symbols = self
            .contracts
            .iter()
            .map(|contract| contract.symbol.as_str())
            .collect::<BTreeSet<_>>();
        for (symbol, observation) in &self.provider_daily_observations {
            if symbol.is_empty() || symbol.trim() != symbol {
                return Err(validation(
                    "provider daily observation symbol must be non-empty and trimmed",
                ));
            }
            observation.validate()?;
        }
        let roster_symbols = self
            .roster_before
            .iter()
            .chain(&self.roster_after)
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if contract_symbols
            .iter()
            .any(|symbol| !roster_symbols.contains(symbol))
        {
            return Err(validation(
                "historical catalog metadata contains a contract outside the observed rosters",
            ));
        }
        if self.complete
            && self
                .roster_before
                .iter()
                .chain(&self.roster_after)
                .any(|symbol| !contract_symbols.contains(symbol.as_str()))
        {
            return Err(validation(
                "historical catalog roster contains a contract without metadata",
            ));
        }
        if self.complete && self.roster_before != self.roster_after {
            return Err(validation(
                "complete historical acquisition requires stable before/after rosters",
            ));
        }
        if self.complete && self.roster_before.len() != self.contracts.len() {
            return Err(validation(
                "complete historical acquisition requires metadata for the full roster",
            ));
        }
        if self.proof == HistoricalCatalogProof::AuthoritativeLifecycle && !self.complete {
            return Err(validation(
                "authoritative historical acquisition must be complete",
            ));
        }
        if self.proof == HistoricalCatalogProof::ProviderHistoryObserved && !self.complete {
            return Err(validation("provider-history acquisition must be complete"));
        }
        if self.proof == HistoricalCatalogProof::ProviderHistoryObserved {
            let observed_symbols = self
                .provider_daily_observations
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>();
            if observed_symbols != contract_symbols {
                return Err(validation(
                    "provider-history daily observations must exactly cover acquired contracts",
                ));
            }
            for contract in &self.contracts {
                let observation = &self.provider_daily_observations[&contract.symbol];
                if observation.range_start_ns != crate::PROVIDER_DAILY_HISTORY_BOOTSTRAP_START_NS
                    || observation.range_end_ns != self.requested_as_of_ns
                {
                    return Err(validation(
                        "provider-history daily observation range does not match bootstrap contract",
                    ));
                }
                let first_available = contract
                    .first_available_data_ns
                    .get(&HistoricalDataKind::Daily)
                    .copied();
                if first_available != observation.first_row_ns {
                    return Err(validation(format!(
                        "provider-history daily origin differs from persisted observation for {}",
                        contract.symbol
                    )));
                }
            }
        } else if !self.provider_daily_observations.is_empty() {
            return Err(validation(
                "provider daily observations require provider-history proof",
            ));
        }
        if self.proof == HistoricalCatalogProof::AuthoritativeLifecycle
            && self
                .contracts
                .iter()
                .any(|contract| contract.authoritative_lifecycle.is_empty())
        {
            return Err(validation(
                "authoritative historical acquisition requires every contract lifecycle",
            ));
        }
        if self.proof == HistoricalCatalogProof::ProviderHistoryObserved
            && self.contracts.iter().any(|contract| {
                contract
                    .first_available_data_ns
                    .get(&HistoricalDataKind::Daily)
                    .is_some_and(|origin| *origin >= self.requested_as_of_ns)
            })
        {
            return Err(validation(
                "provider-history daily origin must precede requested_as_of_ns",
            ));
        }
        Ok(())
    }

    fn body_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&HistoricalCatalogAcquisitionBody {
            format_version: self.format_version,
            proof: self.proof,
            source_identity: &self.source_identity,
            canonical_universe: &self.canonical_universe,
            requested_as_of_ns: self.requested_as_of_ns,
            observed_at_ns: self.observed_at_ns,
            complete: self.complete,
            roster_before: &self.roster_before,
            roster_after: &self.roster_after,
            contracts: &self.contracts,
            provider_daily_observations: &self.provider_daily_observations,
        })?)
    }
}

/// Content-addressed semantic catalog used by a strict timeline compiler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalSemanticCatalog {
    pub format_version: u32,
    pub semantic_catalog_sha256: String,
    pub acquisition_sha256: String,
    pub canonical_universe: String,
    pub catalog: CatalogSnapshot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derived_availability_identity: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub derived_first_available_data_ns: BTreeMap<String, BTreeMap<HistoricalDataKind, i64>>,
}

#[derive(Serialize)]
struct HistoricalSemanticCatalogBody<'a> {
    format_version: u32,
    acquisition_sha256: &'a str,
    canonical_universe: &'a str,
    catalog: &'a CatalogSnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    derived_availability_identity: Option<&'a str>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    derived_first_available_data_ns: &'a BTreeMap<String, BTreeMap<HistoricalDataKind, i64>>,
}

impl HistoricalSemanticCatalog {
    pub fn new(
        acquisition: &HistoricalCatalogAcquisition,
        canonical_universe: impl Into<String>,
        catalog: CatalogSnapshot,
    ) -> Result<Self> {
        acquisition.validate()?;
        if acquisition.proof != HistoricalCatalogProof::AuthoritativeLifecycle {
            return Err(validation(
                "semantic historical catalog requires authoritative lifecycle proof",
            ));
        }
        let mut artifact = Self {
            format_version: HISTORICAL_UNIVERSE_ARTIFACT_FORMAT_VERSION,
            semantic_catalog_sha256: String::new(),
            acquisition_sha256: acquisition.acquisition_sha256.clone(),
            canonical_universe: canonical_universe.into(),
            catalog,
            derived_availability_identity: None,
            derived_first_available_data_ns: BTreeMap::new(),
        };
        artifact.normalize()?;
        artifact.semantic_catalog_sha256 = sha256_identity(&artifact.body_bytes()?);
        artifact.validate_against_acquisition(acquisition)?;
        Ok(artifact)
    }

    /// Builds an effective data-membership catalog from native-daily provider
    /// observations. Membership begins at the first observed daily row;
    /// terminal-empty and provider-unavailable candidates are retained in the
    /// acquisition audit but are not universe members.
    pub fn from_provider_history_observed(
        acquisition: &HistoricalCatalogAcquisition,
        calendar_identity: impl Into<String>,
    ) -> Result<Self> {
        acquisition.validate()?;
        if acquisition.proof != HistoricalCatalogProof::ProviderHistoryObserved
            || !acquisition.complete
        {
            return Err(validation(
                "provider-history semantic catalog requires complete provider-history proof",
            ));
        }
        let contracts = provider_history_catalog_contracts(acquisition)?;
        let catalog = CatalogSnapshot::new(
            format!("provider-history:{}", acquisition.acquisition_sha256),
            calendar_identity,
            true,
            DynamicUniverseScope::all(),
            contracts,
        )?;
        let mut artifact = Self {
            format_version: HISTORICAL_UNIVERSE_ARTIFACT_FORMAT_VERSION,
            semantic_catalog_sha256: String::new(),
            acquisition_sha256: acquisition.acquisition_sha256.clone(),
            canonical_universe: acquisition.canonical_universe.clone(),
            catalog,
            derived_availability_identity: None,
            derived_first_available_data_ns: BTreeMap::new(),
        };

        artifact.normalize()?;
        artifact.semantic_catalog_sha256 = sha256_identity(&artifact.body_bytes()?);
        artifact.validate_against_acquisition(acquisition)?;
        Ok(artifact)
    }

    /// Pins independently observed availability boundaries for logical
    /// provider series such as `KQ.i@...` that are not physical contracts in
    /// the acquisition roster.
    pub fn with_derived_availability(
        mut self,
        identity: impl Into<String>,
        first_available_data_ns: BTreeMap<String, BTreeMap<HistoricalDataKind, i64>>,
    ) -> Result<Self> {
        self.derived_availability_identity = Some(identity.into());
        self.derived_first_available_data_ns = first_available_data_ns;
        self.semantic_catalog_sha256.clear();
        self.normalize()?;
        self.semantic_catalog_sha256 = sha256_identity(&self.body_bytes()?);
        Ok(self)
    }

    pub fn validate(&self) -> Result<()> {
        let mut normalized = self.clone();
        let claimed = normalized.semantic_catalog_sha256.clone();
        normalized.semantic_catalog_sha256.clear();
        normalized.normalize()?;
        if self
            != &(Self {
                semantic_catalog_sha256: claimed.clone(),
                ..normalized.clone()
            })
        {
            return Err(validation(
                "historical semantic catalog is not canonically encoded",
            ));
        }
        let expected = sha256_identity(&normalized.body_bytes()?);
        if claimed != expected {
            return Err(validation("historical semantic catalog hash mismatch"));
        }
        Ok(())
    }

    /// Proves that this semantic catalog is a lossless interpretation of its
    /// complete authoritative acquisition, rather than an unrelated catalog
    /// carrying only the acquisition hash.
    pub fn validate_against_acquisition(
        &self,
        acquisition: &HistoricalCatalogAcquisition,
    ) -> Result<()> {
        self.validate()?;
        acquisition.validate()?;
        if !matches!(
            acquisition.proof,
            HistoricalCatalogProof::AuthoritativeLifecycle
                | HistoricalCatalogProof::ProviderHistoryObserved
        ) || !acquisition.complete
        {
            return Err(validation(
                "semantic historical catalog requires a complete executable-membership acquisition",
            ));
        }
        if self.acquisition_sha256 != acquisition.acquisition_sha256 {
            return Err(validation(
                "historical semantic catalog acquisition link is broken",
            ));
        }

        let acquired = acquisition
            .contracts
            .iter()
            .map(|contract| (contract.symbol.as_str(), contract))
            .collect::<BTreeMap<_, _>>();
        if acquisition.proof == HistoricalCatalogProof::AuthoritativeLifecycle
            && acquired.len() != self.catalog.contracts.len()
        {
            return Err(validation(
                "historical acquisition/catalog contract counts differ",
            ));
        }
        if acquisition.proof == HistoricalCatalogProof::ProviderHistoryObserved
            && provider_history_catalog_contracts(acquisition)? != self.catalog.contracts
        {
            return Err(validation(
                "provider-history acquisition/catalog lifecycle interpretation differs",
            ));
        }
        for contract in &self.catalog.contracts {
            let observed = acquired
                .get(contract.physical_symbol.as_str())
                .ok_or_else(|| validation("historical acquisition/catalog symbols differ"))?;
            let lifecycle_differs = acquisition.proof
                == HistoricalCatalogProof::AuthoritativeLifecycle
                && observed.authoritative_lifecycle != contract.lifecycle;
            if observed.exchange_id != contract.exchange_id
                || observed.product_id != contract.product_id
                || lifecycle_differs
            {
                return Err(validation(format!(
                    "historical acquisition/catalog facts differ for {}",
                    contract.physical_symbol
                )));
            }
        }
        Ok(())
    }

    fn normalize(&mut self) -> Result<()> {
        if self.format_version != HISTORICAL_UNIVERSE_ARTIFACT_FORMAT_VERSION {
            return Err(validation(format!(
                "unsupported historical semantic catalog version {}",
                self.format_version
            )));
        }
        validate_sha256("acquisition_sha256", &self.acquisition_sha256)?;
        self.canonical_universe = normalized(
            "canonical_universe",
            std::mem::take(&mut self.canonical_universe),
        )?;
        self.catalog.validate()?;
        match (
            self.derived_availability_identity.as_mut(),
            self.derived_first_available_data_ns.is_empty(),
        ) {
            (Some(identity), false) => {
                *identity = normalized("derived_availability_identity", std::mem::take(identity))?;
            }
            (None, true) => {}
            _ => {
                return Err(validation(
                    "derived availability identity and boundaries must be supplied together",
                ));
            }
        }
        for (symbol, boundaries) in &self.derived_first_available_data_ns {
            normalized("derived availability symbol", symbol.clone())?;
            if boundaries.is_empty() || boundaries.values().any(|value| *value <= 0) {
                return Err(validation(format!(
                    "derived availability boundaries for {symbol} must be positive and non-empty"
                )));
            }
        }
        Ok(())
    }

    fn body_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&HistoricalSemanticCatalogBody {
            format_version: self.format_version,
            acquisition_sha256: &self.acquisition_sha256,
            canonical_universe: &self.canonical_universe,
            catalog: &self.catalog,
            derived_availability_identity: self.derived_availability_identity.as_deref(),
            derived_first_available_data_ns: &self.derived_first_available_data_ns,
        })?)
    }
}

fn provider_history_catalog_contracts(
    acquisition: &HistoricalCatalogAcquisition,
) -> Result<Vec<CatalogContract>> {
    let mut contracts = Vec::new();
    for observed in &acquisition.contracts {
        let observation = acquisition
            .provider_daily_observations
            .get(&observed.symbol)
            .ok_or_else(|| validation("provider-history contract lacks daily observation"))?;
        let Some(start_ns) = observation.first_row_ns else {
            continue;
        };
        if observed.expired && observed.expire_datetime_ns.is_none() {
            return Err(validation(format!(
                "expired provider-history contract lacks expiry metadata: {}",
                observed.symbol
            )));
        }
        let end_ns = observed
            .expire_datetime_ns
            .unwrap_or(acquisition.requested_as_of_ns)
            .min(acquisition.requested_as_of_ns);
        if end_ns <= start_ns {
            return Err(validation(format!(
                "provider-history membership interval is empty for {}",
                observed.symbol
            )));
        }
        contracts.push(CatalogContract::new(
            observed.symbol.clone(),
            observed.exchange_id.clone(),
            observed.product_id.clone(),
            vec![ActiveInterval::new(start_ns, end_ns)?],
        )?);
    }
    Ok(contracts)
}

/// Data-owned immutable artifact reader/publisher.
#[derive(Debug, Clone)]
pub struct HistoricalUniverseArtifactStore {
    cache_dir: PathBuf,
}

impl HistoricalUniverseArtifactStore {
    /// Creates a handle without touching the filesystem. This makes dry-run callers safe.
    pub fn new(cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            cache_dir: cache_dir.into(),
        }
    }

    pub fn namespace_dir(&self) -> PathBuf {
        self.cache_dir.join(HISTORICAL_UNIVERSE_ARTIFACT_NAMESPACE)
    }

    pub fn acquisition_path(&self, sha256: &str) -> Result<PathBuf> {
        self.artifact_path("acquisitions", sha256)
    }

    pub fn semantic_catalog_path(&self, sha256: &str) -> Result<PathBuf> {
        self.artifact_path("catalogs", sha256)
    }

    pub fn plan_path(&self, sha256: &str) -> Result<PathBuf> {
        self.artifact_path("plans", sha256)
    }

    pub fn publish_acquisition(
        &self,
        acquisition: &HistoricalCatalogAcquisition,
    ) -> Result<PathBuf> {
        acquisition.validate()?;
        self.publish(
            "acquisitions",
            &acquisition.acquisition_sha256,
            &serde_json::to_vec(acquisition)?,
        )
    }

    pub fn load_acquisition(&self, sha256: &str) -> Result<HistoricalCatalogAcquisition> {
        let value: HistoricalCatalogAcquisition = self.load("acquisitions", sha256)?;
        value.validate()?;
        if value.acquisition_sha256 != sha256 {
            return Err(validation("historical acquisition path/hash mismatch"));
        }
        Ok(value)
    }

    pub fn publish_semantic_catalog(&self, catalog: &HistoricalSemanticCatalog) -> Result<PathBuf> {
        catalog.validate()?;
        self.publish(
            "catalogs",
            &catalog.semantic_catalog_sha256,
            &serde_json::to_vec(catalog)?,
        )
    }

    pub fn load_semantic_catalog(&self, sha256: &str) -> Result<HistoricalSemanticCatalog> {
        let value: HistoricalSemanticCatalog = self.load("catalogs", sha256)?;
        value.validate()?;
        if value.semantic_catalog_sha256 != sha256 {
            return Err(validation("historical catalog path/hash mismatch"));
        }
        Ok(value)
    }

    pub fn publish_plan(&self, plan: &HistoricalUniversePlan) -> Result<PathBuf> {
        plan.verify()?;
        self.publish("plans", &plan.plan_sha256, &serde_json::to_vec(plan)?)
    }

    pub fn load_plan(&self, sha256: &str) -> Result<HistoricalUniversePlan> {
        let value: HistoricalUniversePlan = self.load("plans", sha256)?;
        value.verify()?;
        if value.plan_sha256 != sha256 {
            return Err(validation("historical universe plan path/hash mismatch"));
        }
        Ok(value)
    }

    /// Verifies the complete content-addressed identity chain for an executable plan.
    /// Legacy v1/v2 plans have no external chain and retain their original verification.
    pub fn verify_plan_artifact_chain(&self, plan: &HistoricalUniversePlan) -> Result<()> {
        plan.verify()?;
        if plan.plan_version < 3 {
            return Ok(());
        }
        let identity = plan.v3_identity.as_ref().ok_or_else(|| {
            validation("historical universe plan v3 lacks artifact identity chain")
        })?;
        let acquisition = self.load_acquisition(&identity.acquisition_sha256)?;
        let semantic = self.load_semantic_catalog(&identity.semantic_catalog_sha256)?;
        semantic.validate_against_acquisition(&acquisition)?;
        if !matches!(
            acquisition.proof,
            HistoricalCatalogProof::AuthoritativeLifecycle
                | HistoricalCatalogProof::ProviderHistoryObserved
        ) || acquisition.proof != identity.proof
        {
            return Err(validation(
                "historical universe plan proof does not match executable-membership acquisition",
            ));
        }
        if semantic.acquisition_sha256 != acquisition.acquisition_sha256 {
            return Err(validation(
                "historical universe semantic catalog acquisition link is broken",
            ));
        }
        if plan.timeline.catalog_id != semantic.catalog.catalog_id
            || plan.timeline.catalog_sha256 != semantic.catalog.content_sha256()
            || plan.timeline.calendar_identity != semantic.catalog.calendar_identity
        {
            return Err(validation(
                "historical universe plan timeline does not match its semantic catalog",
            ));
        }
        Ok(())
    }

    fn artifact_path(&self, family: &str, sha256: &str) -> Result<PathBuf> {
        validate_sha256("artifact sha256", sha256)?;
        Ok(self
            .namespace_dir()
            .join(family)
            .join(format!("{}.json", &sha256["sha256:".len()..])))
    }

    fn publish(&self, family: &str, sha256: &str, bytes: &[u8]) -> Result<PathBuf> {
        let final_path = self.artifact_path(family, sha256)?;
        let family_dir = final_path
            .parent()
            .ok_or_else(|| validation("historical artifact path has no parent"))?;
        ensure_directory_without_symlink(&self.cache_dir)?;
        ensure_directory_without_symlink(&self.namespace_dir())?;
        ensure_directory_without_symlink(family_dir)?;

        let lock_path = self.namespace_dir().join(".publish.lock");
        reject_symlink_if_exists(&lock_path)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        lock.lock_exclusive()?;
        let result = publish_locked(family_dir, &final_path, bytes);
        let unlock_result = FileExt::unlock(&lock);
        if let Err(error) = unlock_result {
            return Err(DataError::Io(error));
        }
        result?;
        Ok(final_path)
    }

    fn load<T: for<'de> Deserialize<'de>>(&self, family: &str, sha256: &str) -> Result<T> {
        let path = self.artifact_path(family, sha256)?;
        reject_symlink_ancestors(&path)?;
        let mut bytes = Vec::new();
        File::open(path)?.read_to_end(&mut bytes)?;
        Ok(serde_json::from_slice(&bytes)?)
    }
}

fn publish_locked(directory: &Path, final_path: &Path, bytes: &[u8]) -> Result<()> {
    reject_symlink_if_exists(final_path)?;
    if final_path.exists() {
        if fs::read(final_path)? != bytes {
            return Err(validation(
                "historical artifact hash collision or existing corruption",
            ));
        }
        File::open(directory)?.sync_all()?;
        return Ok(());
    }

    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temp_path = directory.join(format!(".publish-{}-{sequence}.tmp", std::process::id()));
    reject_symlink_if_exists(&temp_path)?;
    let publish_result = (|| -> Result<()> {
        let mut temp = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)?;
        temp.write_all(bytes)?;
        temp.sync_all()?;
        fs::rename(&temp_path, final_path)?;
        File::open(directory)?.sync_all()?;
        Ok(())
    })();
    if publish_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    publish_result
}

fn ensure_directory_without_symlink(path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(validation(format!(
                    "historical artifact path must not contain a symlink: {}",
                    current.display()
                )));
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(validation(format!(
                    "historical artifact path is not a directory: {}",
                    current.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)?;
                File::open(&current)?.sync_all()?;
                if let Some(parent) = current.parent()
                    && !parent.as_os_str().is_empty()
                {
                    File::open(parent)?.sync_all()?;
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn reject_symlink_ancestors(path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(validation(format!(
                    "historical artifact path must not contain a symlink: {}",
                    current.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn reject_symlink_if_exists(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(validation(format!(
            "historical artifact path must not be a symlink: {}",
            path.display()
        ))),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn normalize_roster(roster: &mut [String]) -> Result<()> {
    for symbol in roster.iter_mut() {
        *symbol = normalized("roster symbol", std::mem::take(symbol))?;
    }
    roster.sort();
    if roster.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(validation(
            "historical catalog roster contains duplicate symbols",
        ));
    }
    Ok(())
}

fn normalized(field: &str, value: String) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(validation(format!("historical artifact {field} is empty")));
    }
    if trimmed != value {
        return Err(validation(format!(
            "historical artifact {field} must already be normalized"
        )));
    }
    Ok(value)
}

fn validate_sha256(field: &str, value: &str) -> Result<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(validation(format!("{field} must use sha256 identity")));
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(validation(format!("{field} must contain 64 hex digits")));
    }
    Ok(())
}

fn sha256_identity(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn validation(message: impl Into<String>) -> DataError {
    DataError::Validation(message.into())
}
