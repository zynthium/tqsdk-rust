use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::historical_universe_v4_resolution::{
    HISTORICAL_UNIVERSE_V3_PROJECTION_CANONICALIZER_ID,
    HISTORICAL_UNIVERSE_V3_PROJECTION_COMPILER_ID,
};
use crate::{
    ActiveInterval, CatalogContract, CatalogSnapshot, DataError, DynamicUniverseScope,
    HistoricalCatalogProof, HistoricalUniversePlan, HistoricalUniversePlanArtifact,
    HistoricalUniversePlanV5, HistoricalUniversePlanWriteSet,
    PROVIDER_DAILY_HISTORY_BOOTSTRAP_START_NS, PROVIDER_DAILY_HISTORY_SOURCE_IDENTITY, Result,
};

pub const HISTORICAL_UNIVERSE_ARTIFACT_FORMAT_VERSION: u32 = 1;
pub const HISTORICAL_UNIVERSE_ARTIFACT_NAMESPACE: &str = "historical-universe-v1";
pub const PROVIDER_DAILY_UNAVAILABLE_RETRY_STATE_FORMAT_VERSION: u32 = 1;

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

/// Retry receipt for one bounded provider-unavailable native-daily probe.
///
/// This is operator maintenance state, deliberately separate from the
/// provider-history observation proof. It gives retry scheduling a durable,
/// immutable receipt without changing acquisition identity semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ProviderDailyUnavailableRetry {
    pub attempts: u32,
    pub next_retry_at_ns: i64,
}

impl ProviderDailyUnavailableRetry {
    fn initial(expired: bool, observed_at_ns: i64) -> Result<Self> {
        Self::scheduled(expired, 1, observed_at_ns)
    }

    fn next(self, expired: bool, observed_at_ns: i64) -> Result<Self> {
        let attempts = self
            .attempts
            .checked_add(1)
            .ok_or_else(|| validation("provider-unavailable retry attempt overflow"))?;
        Self::scheduled(expired, attempts, observed_at_ns)
    }

    fn scheduled(expired: bool, attempts: u32, observed_at_ns: i64) -> Result<Self> {
        if observed_at_ns <= 0 {
            return Err(validation(
                "provider-unavailable retry requires positive observation timestamp",
            ));
        }
        let next_retry_at_ns = observed_at_ns
            .checked_add(provider_daily_unavailable_retry_delay_ns(expired, attempts))
            .ok_or_else(|| validation("provider-unavailable retry timestamp overflow"))?;
        let retry = Self {
            attempts,
            next_retry_at_ns,
        };
        retry.validate()?;
        Ok(retry)
    }

    fn validate(&self) -> Result<()> {
        if self.attempts == 0 || self.next_retry_at_ns <= 0 {
            return Err(validation(
                "provider-unavailable retry requires positive attempt and due timestamp",
            ));
        }
        Ok(())
    }
}

/// One provider-unavailable contract eligible for maintenance selection.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ProviderDailyUnavailableRetryCandidate {
    pub symbol: String,
    pub expired: bool,
    pub unavailable_after_ns: u64,
    pub retry: ProviderDailyUnavailableRetry,
}

/// Immutable retry receipt bound to one provider-history acquisition.
///
/// The proof artifact records provider observations only. This side receipt
/// records retry scheduling and is keyed by the immutable acquisition hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ProviderDailyUnavailableRetryState {
    pub format_version: u32,
    pub retry_state_sha256: String,
    pub acquisition_sha256: String,
    pub observed_at_ns: i64,
    pub retries: BTreeMap<String, ProviderDailyUnavailableRetry>,
}

#[derive(Serialize)]
struct ProviderDailyUnavailableRetryStateBody<'a> {
    format_version: u32,
    acquisition_sha256: &'a str,
    observed_at_ns: i64,
    retries: &'a BTreeMap<String, ProviderDailyUnavailableRetry>,
}

impl ProviderDailyUnavailableRetryState {
    /// Derive the first bounded-retry schedule from immutable observations.
    pub fn from_acquisition(acquisition: &HistoricalCatalogAcquisition) -> Result<Self> {
        acquisition.validate()?;
        ensure_provider_history_observed(acquisition)?;

        let contracts = acquisition
            .contracts
            .iter()
            .map(|contract| (contract.symbol.as_str(), contract))
            .collect::<BTreeMap<_, _>>();
        let mut retries = BTreeMap::new();
        for (symbol, observation) in &acquisition.provider_daily_observations {
            if observation.status != HistoricalDailyObservationStatus::ProviderUnavailable {
                continue;
            }
            let contract = contracts.get(symbol.as_str()).ok_or_else(|| {
                validation(format!(
                    "provider-unavailable observation references missing contract {symbol}"
                ))
            })?;
            retries.insert(
                symbol.clone(),
                ProviderDailyUnavailableRetry::initial(
                    contract.expired,
                    acquisition.observed_at_ns,
                )?,
            );
        }
        Self::new(
            acquisition.acquisition_sha256.clone(),
            acquisition.observed_at_ns,
            retries,
        )
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.retries.is_empty()
    }

    /// Return all retry candidates. Caller applies operator budget and force policy.
    pub fn candidates(
        &self,
        acquisition: &HistoricalCatalogAcquisition,
    ) -> Result<Vec<ProviderDailyUnavailableRetryCandidate>> {
        self.validate_against(acquisition)?;
        let contracts = acquisition
            .contracts
            .iter()
            .map(|contract| (contract.symbol.as_str(), contract))
            .collect::<BTreeMap<_, _>>();
        let mut candidates = self
            .retries
            .iter()
            .map(|(symbol, retry)| {
                let contract = contracts.get(symbol.as_str()).ok_or_else(|| {
                    validation(format!(
                        "provider-unavailable retry references missing contract {symbol}"
                    ))
                })?;
                let observation = acquisition
                    .provider_daily_observations
                    .get(symbol)
                    .ok_or_else(|| {
                        validation(format!(
                            "provider-unavailable retry omits observation for {symbol}"
                        ))
                    })?;
                let unavailable_after_ns =
                    observation.provider_unavailable_after_ns.ok_or_else(|| {
                        validation(format!(
                            "provider-unavailable observation omits timeout for {symbol}"
                        ))
                    })?;
                Ok(ProviderDailyUnavailableRetryCandidate {
                    symbol: symbol.clone(),
                    expired: contract.expired,
                    unavailable_after_ns,
                    retry: *retry,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        candidates.sort_by(|left, right| {
            left.expired
                .cmp(&right.expired)
                .then(
                    left.retry
                        .next_retry_at_ns
                        .cmp(&right.retry.next_retry_at_ns),
                )
                .then(left.symbol.cmp(&right.symbol))
        });
        Ok(candidates)
    }

    /// Advance only attempted provider-unavailable candidates onto a refreshed
    /// immutable acquisition. Completed retries disappear from the receipt.
    pub fn refreshed(
        &self,
        previous: &HistoricalCatalogAcquisition,
        refreshed: &HistoricalCatalogAcquisition,
        attempted_symbols: &BTreeSet<String>,
        observed_at_ns: i64,
    ) -> Result<Self> {
        self.validate_against(previous)?;
        refreshed.validate()?;
        ensure_provider_history_observed(refreshed)?;
        if observed_at_ns < self.observed_at_ns {
            return Err(validation(
                "provider-unavailable retry refresh timestamp moved backwards",
            ));
        }
        if !previous.matches_provider_history_refresh(refreshed) {
            return Err(validation(
                "provider-unavailable retry refresh must preserve stable provider-history roster",
            ));
        }

        let previous_unavailable = provider_unavailable_symbols(previous)?;
        if !attempted_symbols.is_subset(&previous_unavailable) {
            return Err(validation(
                "provider-unavailable retry refresh attempted a non-unavailable contract",
            ));
        }

        let refreshed_unavailable = provider_unavailable_symbols(refreshed)?;
        if !refreshed_unavailable.is_subset(&previous_unavailable) {
            return Err(validation(
                "provider-unavailable retry refresh introduced an unobserved unavailable contract",
            ));
        }
        if previous_unavailable
            .difference(attempted_symbols)
            .any(|symbol| !refreshed_unavailable.contains(symbol))
        {
            return Err(validation(
                "provider-unavailable retry refresh changed an unattempted contract",
            ));
        }

        let contracts = refreshed
            .contracts
            .iter()
            .map(|contract| (contract.symbol.as_str(), contract))
            .collect::<BTreeMap<_, _>>();
        let mut retries = BTreeMap::new();
        for symbol in refreshed_unavailable {
            let prior = self.retries.get(&symbol).ok_or_else(|| {
                validation(format!("provider-unavailable retry receipt omits {symbol}"))
            })?;
            let contract = contracts.get(symbol.as_str()).ok_or_else(|| {
                validation(format!(
                    "provider-unavailable refreshed observation references missing contract {symbol}"
                ))
            })?;
            let retry = if attempted_symbols.contains(&symbol) {
                prior.next(contract.expired, observed_at_ns)?
            } else {
                *prior
            };
            retries.insert(symbol, retry);
        }
        Self::new(
            refreshed.acquisition_sha256.clone(),
            observed_at_ns,
            retries,
        )
    }

    fn new(
        acquisition_sha256: String,
        observed_at_ns: i64,
        retries: BTreeMap<String, ProviderDailyUnavailableRetry>,
    ) -> Result<Self> {
        let mut state = Self {
            format_version: PROVIDER_DAILY_UNAVAILABLE_RETRY_STATE_FORMAT_VERSION,
            retry_state_sha256: String::new(),
            acquisition_sha256,
            observed_at_ns,
            retries,
        };
        state.normalize()?;
        state.retry_state_sha256 = sha256_identity(&state.body_bytes()?);
        Ok(state)
    }

    fn validate_against(&self, acquisition: &HistoricalCatalogAcquisition) -> Result<()> {
        self.validate()?;
        acquisition.validate()?;
        ensure_provider_history_observed(acquisition)?;
        if self.acquisition_sha256 != acquisition.acquisition_sha256 {
            return Err(validation(
                "provider-unavailable retry receipt acquisition hash mismatch",
            ));
        }
        let expected = provider_unavailable_symbols(acquisition)?;
        let actual = self.retries.keys().cloned().collect::<BTreeSet<_>>();
        if actual != expected {
            return Err(validation(
                "provider-unavailable retry receipt must cover exactly unavailable observations",
            ));
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        let mut normalized = self.clone();
        let claimed = normalized.retry_state_sha256.clone();
        normalized.retry_state_sha256.clear();
        normalized.normalize()?;
        if self
            != &(Self {
                retry_state_sha256: claimed.clone(),
                ..normalized.clone()
            })
        {
            return Err(validation(
                "provider-unavailable retry receipt not canonically encoded",
            ));
        }
        let expected = sha256_identity(&normalized.body_bytes()?);
        if claimed != expected {
            return Err(validation(
                "provider-unavailable retry receipt hash mismatch",
            ));
        }
        Ok(())
    }

    fn normalize(&mut self) -> Result<()> {
        if self.format_version != PROVIDER_DAILY_UNAVAILABLE_RETRY_STATE_FORMAT_VERSION {
            return Err(validation(format!(
                "unsupported provider-unavailable retry receipt version {}",
                self.format_version
            )));
        }
        validate_sha256(
            "provider-unavailable retry acquisition sha256",
            &self.acquisition_sha256,
        )?;
        if self.observed_at_ns <= 0 {
            return Err(validation(
                "provider-unavailable retry receipt requires positive observation timestamp",
            ));
        }
        for (symbol, retry) in &self.retries {
            if symbol.trim().is_empty() {
                return Err(validation(
                    "provider-unavailable retry receipt symbol must be non-empty",
                ));
            }
            retry.validate()?;
        }
        Ok(())
    }

    fn body_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(
            &ProviderDailyUnavailableRetryStateBody {
                format_version: self.format_version,
                acquisition_sha256: &self.acquisition_sha256,
                observed_at_ns: self.observed_at_ns,
                retries: &self.retries,
            },
        )?)
    }
}

fn provider_daily_unavailable_retry_delay_ns(expired: bool, attempts: u32) -> i64 {
    const HOUR_NS: i64 = 60 * 60 * 1_000_000_000;
    const DAY_NS: i64 = 24 * HOUR_NS;
    const ACTIVE_DELAYS_NS: [i64; 4] = [HOUR_NS, DAY_NS, 7 * DAY_NS, 30 * DAY_NS];
    const EXPIRED_DELAYS_NS: [i64; 3] = [7 * DAY_NS, 30 * DAY_NS, 90 * DAY_NS];
    let delays = if expired {
        &EXPIRED_DELAYS_NS[..]
    } else {
        &ACTIVE_DELAYS_NS[..]
    };
    let index = usize::try_from(attempts.saturating_sub(1)).unwrap_or(usize::MAX);
    delays[index.min(delays.len() - 1)]
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

    fn matches_provider_current_acquisition(&self, current: &Self) -> bool {
        if self.proof != HistoricalCatalogProof::ProviderHistoryObserved
            || current.proof != HistoricalCatalogProof::ProviderCurrentObserved
            || !self.complete
            || !current.complete
            || self.format_version != current.format_version
            || self.source_identity
                != format!(
                    "{}+{}",
                    current.source_identity, PROVIDER_DAILY_HISTORY_SOURCE_IDENTITY
                )
            || self.canonical_universe != current.canonical_universe
            || self.requested_as_of_ns != current.requested_as_of_ns
            || self.roster_before != current.roster_before
            || self.roster_after != current.roster_after
            || self.contracts.len() != current.contracts.len()
            || self.provider_daily_observations.len() != self.contracts.len()
        {
            return false;
        }

        self.contracts
            .iter()
            .zip(&current.contracts)
            .all(|(observed, current)| {
                let mut observed = observed.clone();
                observed
                    .first_available_data_ns
                    .remove(&HistoricalDataKind::Daily);
                observed == *current
            })
            && self
                .provider_daily_observations
                .values()
                .all(|observation| {
                    observation.range_start_ns == PROVIDER_DAILY_HISTORY_BOOTSTRAP_START_NS
                        && observation.range_end_ns == current.requested_as_of_ns
                })
    }

    /// Promote a bounded subset of prior provider-unavailable observations
    /// against a newly stable provider-current acquisition. All other
    /// observations remain byte-for-byte facts from the prior acquisition.
    pub fn refresh_provider_daily_observations(
        &self,
        current: Self,
        updates: BTreeMap<String, HistoricalDailyObservation>,
    ) -> Result<Self> {
        self.validate()?;
        current.validate()?;
        ensure_provider_history_observed(self)?;
        self.validate_provider_daily_refresh_current(&current)?;
        if updates.is_empty() {
            return Err(validation(
                "provider-unavailable refresh requires at least one attempted observation",
            ));
        }

        let mut observations = self.provider_daily_observations.clone();
        for (symbol, observation) in updates {
            observation.validate()?;
            let prior = observations.get(&symbol).ok_or_else(|| {
                validation(format!(
                    "provider-unavailable refresh references unknown contract {symbol}"
                ))
            })?;
            if prior.status != HistoricalDailyObservationStatus::ProviderUnavailable {
                return Err(validation(format!(
                    "provider-unavailable refresh may update only unavailable contract {symbol}"
                )));
            }
            if observation.range_start_ns != prior.range_start_ns
                || observation.range_end_ns != prior.range_end_ns
            {
                return Err(validation(format!(
                    "provider-unavailable refresh changed observation range for {symbol}"
                )));
            }
            observations.insert(symbol, observation);
        }

        let source_identity = format!(
            "{}+{}",
            current.source_identity, PROVIDER_DAILY_HISTORY_SOURCE_IDENTITY
        );
        current.promote_provider_daily_history(source_identity, observations)
    }

    /// Verify a current provider roster can safely refresh this pinned
    /// provider-history acquisition without widening its proof range.
    pub fn validate_provider_daily_refresh_current(&self, current: &Self) -> Result<()> {
        self.validate()?;
        current.validate()?;
        ensure_provider_history_observed(self)?;
        if !self.matches_provider_current_acquisition(current) {
            return Err(validation(
                "provider-unavailable refresh requires an exact stable provider-current acquisition",
            ));
        }
        Ok(())
    }

    fn matches_provider_history_refresh(&self, refreshed: &Self) -> bool {
        if self.proof != HistoricalCatalogProof::ProviderHistoryObserved
            || refreshed.proof != HistoricalCatalogProof::ProviderHistoryObserved
            || !self.complete
            || !refreshed.complete
            || self.format_version != refreshed.format_version
            || self.source_identity != refreshed.source_identity
            || self.canonical_universe != refreshed.canonical_universe
            || self.requested_as_of_ns != refreshed.requested_as_of_ns
            || self.roster_before != refreshed.roster_before
            || self.roster_after != refreshed.roster_after
            || self.contracts.len() != refreshed.contracts.len()
        {
            return false;
        }

        self.contracts
            .iter()
            .zip(&refreshed.contracts)
            .all(|(previous, refreshed)| {
                let mut previous = previous.clone();
                let mut refreshed = refreshed.clone();
                previous
                    .first_available_data_ns
                    .remove(&HistoricalDataKind::Daily);
                refreshed
                    .first_available_data_ns
                    .remove(&HistoricalDataKind::Daily);
                previous == refreshed
            })
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

fn ensure_provider_history_observed(acquisition: &HistoricalCatalogAcquisition) -> Result<()> {
    if acquisition.proof != HistoricalCatalogProof::ProviderHistoryObserved || !acquisition.complete
    {
        return Err(validation(
            "provider-unavailable retry requires complete provider-history observation",
        ));
    }
    Ok(())
}

fn provider_unavailable_symbols(
    acquisition: &HistoricalCatalogAcquisition,
) -> Result<BTreeSet<String>> {
    ensure_provider_history_observed(acquisition)?;
    let symbols = acquisition
        .provider_daily_observations
        .iter()
        .filter(|(_, observation)| {
            observation.status == HistoricalDailyObservationStatus::ProviderUnavailable
        })
        .map(|(symbol, _)| symbol.clone())
        .collect::<BTreeSet<_>>();
    Ok(symbols)
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

/// Root-scoped exclusive guard for a provider-membership retry operation.
///
/// A retry receipt selection, remote probe, and any resulting immutable
/// publication form one operation. Holding this guard prevents two operators
/// from advancing the same retry schedule concurrently.
#[derive(Debug)]
pub struct ProviderDailyUnavailableRetryOperationLock {
    _file: fs::File,
}

/// Paths published by one successful legacy V4 + V3 rollback dual write.
///
/// The normal V5 writer does not use this compatibility-only write set.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct HistoricalUniversePublishedPlanSet {
    v4_path: PathBuf,
    rollback_v3_path: PathBuf,
    v4_plan_sha256: String,
    rollback_v3_plan_sha256: String,
}

impl HistoricalUniversePublishedPlanSet {
    #[must_use]
    pub fn v4_path(&self) -> &Path {
        &self.v4_path
    }

    #[must_use]
    pub fn rollback_v3_path(&self) -> &Path {
        &self.rollback_v3_path
    }

    #[must_use]
    pub fn v4_plan_sha256(&self) -> &str {
        &self.v4_plan_sha256
    }

    #[must_use]
    pub fn rollback_v3_plan_sha256(&self) -> &str {
        &self.rollback_v3_plan_sha256
    }
}

/// Immutable source-to-current mapping produced by a verified V4 migration.
///
/// The source artifact is intentionally retained. Consumers can use this
/// receipt to replace an explicit V4 hash in their own configuration without
/// relying on a mutable "current" pointer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct HistoricalUniversePlanMigration {
    source_plan_version: u32,
    source_plan_sha256: String,
    source_path: PathBuf,
    current_plan_version: u32,
    current_plan_sha256: String,
    current_path: PathBuf,
}

impl HistoricalUniversePlanMigration {
    #[must_use]
    pub const fn source_plan_version(&self) -> u32 {
        self.source_plan_version
    }

    #[must_use]
    pub fn source_plan_sha256(&self) -> &str {
        &self.source_plan_sha256
    }

    #[must_use]
    pub fn source_path(&self) -> &Path {
        &self.source_path
    }

    #[must_use]
    pub const fn current_plan_version(&self) -> u32 {
        self.current_plan_version
    }

    #[must_use]
    pub fn current_plan_sha256(&self) -> &str {
        &self.current_plan_sha256
    }

    #[must_use]
    pub fn current_path(&self) -> &Path {
        &self.current_path
    }
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

    pub fn try_acquire_provider_daily_retry_operation_lock(
        &self,
    ) -> Result<ProviderDailyUnavailableRetryOperationLock> {
        ensure_directory_without_symlink(&self.cache_dir)?;
        let namespace_dir = self.namespace_dir();
        ensure_directory_without_symlink(&namespace_dir)?;
        let path = namespace_dir.join(".provider-daily-retry.lock");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        file.try_lock_exclusive()
            .map_err(|_| DataError::CacheBusy {
                cache_dir: self.cache_dir.clone(),
                operation: "provider-unavailable retry refresh",
            })?;
        Ok(ProviderDailyUnavailableRetryOperationLock { _file: file })
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

    pub fn provider_daily_retry_state_path(&self, retry_state_sha256: &str) -> Result<PathBuf> {
        self.artifact_path("provider-daily-retries", retry_state_sha256)
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

    /// Persist one immutable maintenance receipt bound to an acquisition.
    pub fn publish_provider_daily_retry_state(
        &self,
        state: &ProviderDailyUnavailableRetryState,
    ) -> Result<PathBuf> {
        state.validate()?;
        self.publish(
            "provider-daily-retries",
            &state.retry_state_sha256,
            &serde_json::to_vec(state)?,
        )
    }

    pub fn load_provider_daily_retry_state(
        &self,
        retry_state_sha256: &str,
        acquisition: &HistoricalCatalogAcquisition,
    ) -> Result<ProviderDailyUnavailableRetryState> {
        acquisition.validate()?;
        let value: ProviderDailyUnavailableRetryState =
            self.load("provider-daily-retries", retry_state_sha256)?;
        value.validate_against(acquisition)?;
        if value.retry_state_sha256 != retry_state_sha256 {
            return Err(validation(
                "provider-unavailable retry receipt path/hash mismatch",
            ));
        }
        Ok(value)
    }

    /// Find the latest immutable retry receipt for one provider-history
    /// acquisition. Missing receipts are expected for artifacts published
    /// before retry maintenance existed.
    pub fn find_provider_daily_retry_state(
        &self,
        acquisition: &HistoricalCatalogAcquisition,
    ) -> Result<Option<ProviderDailyUnavailableRetryState>> {
        acquisition.validate()?;
        let directory = self.namespace_dir().join("provider-daily-retries");
        let metadata = match fs::symlink_metadata(&directory) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Ok(None);
        }

        let mut matched = None;
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if !file_type.is_file() || file_type.is_symlink() {
                continue;
            }
            let file_name = entry.file_name();
            let Some(stem) = file_name
                .to_str()
                .and_then(|name| name.strip_suffix(".json"))
            else {
                continue;
            };
            let retry_state_sha256 = format!("sha256:{stem}");
            if validate_sha256(
                "provider-unavailable retry receipt sha256",
                &retry_state_sha256,
            )
            .is_err()
            {
                continue;
            }
            let candidate: ProviderDailyUnavailableRetryState =
                self.load("provider-daily-retries", &retry_state_sha256)?;
            candidate.validate()?;
            if candidate.acquisition_sha256 != acquisition.acquisition_sha256 {
                continue;
            }
            candidate.validate_against(acquisition)?;
            match &matched {
                None => matched = Some(candidate),
                Some(existing)
                    if (candidate.observed_at_ns, &candidate.retry_state_sha256)
                        > (existing.observed_at_ns, &existing.retry_state_sha256) =>
                {
                    matched = Some(candidate);
                }
                Some(_) => {}
            }
        }
        Ok(matched)
    }

    /// Finds the newest immutable provider-history observation for this exact
    /// stable provider roster. Missing, malformed, or unrelated artifacts are
    /// ignored so callers can safely fall back to a fresh bootstrap.
    pub fn find_matching_provider_history_observed_acquisition(
        &self,
        current: &HistoricalCatalogAcquisition,
    ) -> Result<Option<HistoricalCatalogAcquisition>> {
        current.validate()?;
        if current.proof != HistoricalCatalogProof::ProviderCurrentObserved || !current.complete {
            return Ok(None);
        }

        let directory = self.namespace_dir().join("acquisitions");
        let metadata = match fs::symlink_metadata(&directory) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Ok(None);
        }

        let mut matched = None;
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if !file_type.is_file() || file_type.is_symlink() {
                continue;
            }
            let file_name = entry.file_name();
            let Some(stem) = file_name
                .to_str()
                .and_then(|name| name.strip_suffix(".json"))
            else {
                continue;
            };
            let sha256 = format!("sha256:{stem}");
            if validate_sha256("artifact sha256", &sha256).is_err() {
                continue;
            }
            let Ok(candidate) = self.load_acquisition(&sha256) else {
                continue;
            };
            if !candidate.matches_provider_current_acquisition(current) {
                continue;
            }
            match &matched {
                None => matched = Some(candidate),
                Some(existing)
                    if (candidate.observed_at_ns, &candidate.acquisition_sha256)
                        > (existing.observed_at_ns, &existing.acquisition_sha256) =>
                {
                    matched = Some(candidate);
                }
                Some(_) => {}
            }
        }
        Ok(matched)
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

    /// Publishes a current V5 plan into the immutable content-addressed plan
    /// namespace. V5 never writes a rollback companion plan.
    pub fn publish_current_plan(&self, plan: &HistoricalUniversePlanV5) -> Result<PathBuf> {
        plan.verify()?;
        self.publish("plans", plan.plan_sha256(), &plan.canonical_json_bytes()?)
    }

    /// Loads only the current V5 artifact. Older plan versions must be
    /// migrated by the dedicated migration entry point before normal use.
    pub fn load_current_plan(&self, sha256: &str) -> Result<HistoricalUniversePlanV5> {
        let bytes = self.load_bytes("plans", sha256)?;
        let value: HistoricalUniversePlanV5 = serde_json::from_slice(&bytes)?;
        if value.canonical_json_bytes()? != bytes {
            return Err(validation(
                "historical universe plan does not use canonical V5 JSON",
            ));
        }
        if value.plan_sha256() != sha256 {
            return Err(validation("historical universe plan path/hash mismatch"));
        }
        Ok(value)
    }

    /// Verifies and plans a V4-to-V5 conversion without writing the V5 file.
    pub fn preview_v4_migration(
        &self,
        source_plan_sha256: &str,
    ) -> Result<HistoricalUniversePlanMigration> {
        Ok(self.verified_v4_migration(source_plan_sha256)?.0)
    }

    /// Migrates one V4 plan only after validating its complete V4/V3 chain.
    ///
    /// The source and its rollback companion are never overwritten or
    /// deleted. V1-V3 plans must be recompiled because they do not carry the
    /// V4 execution closure required for a lossless V5 conversion.
    pub fn migrate_v4_plan(
        &self,
        source_plan_sha256: &str,
    ) -> Result<HistoricalUniversePlanMigration> {
        let (migration, current) = self.verified_v4_migration(source_plan_sha256)?;
        let current_path = self.publish_current_plan(&current)?;
        if current_path != migration.current_path {
            return Err(validation(
                "historical universe V5 migration path changed during publish",
            ));
        }
        let loaded = self.load_current_plan(current.plan_sha256())?;
        if loaded != current {
            return Err(validation(
                "historical universe V5 migration changed plan bytes during publish",
            ));
        }
        self.verify_current_plan_artifact_chain(&loaded)?;
        Ok(migration)
    }

    fn verified_v4_migration(
        &self,
        source_plan_sha256: &str,
    ) -> Result<(HistoricalUniversePlanMigration, HistoricalUniversePlanV5)> {
        let source_path = self.plan_path(source_plan_sha256)?;
        let artifact = self.load_plan_artifact(source_plan_sha256)?;
        let plan = match &artifact {
            HistoricalUniversePlanArtifact::V4(plan) => plan,
            HistoricalUniversePlanArtifact::Legacy(plan) => {
                return Err(validation(format!(
                    "historical universe plan V{} must be recompiled before V5 migration",
                    plan.plan_version
                )));
            }
            HistoricalUniversePlanArtifact::V5(_) => {
                return Err(validation("historical universe plan is already V5"));
            }
        };
        self.verify_plan_artifact_chain_artifact(&artifact)?;
        let current = plan.migrate_to_v5()?;
        let migration = HistoricalUniversePlanMigration {
            source_plan_version: plan.plan_version(),
            source_plan_sha256: plan.plan_sha256().to_string(),
            source_path,
            current_plan_version: current.plan_version(),
            current_plan_sha256: current.plan_sha256().to_string(),
            current_path: self.plan_path(current.plan_sha256())?,
        };
        Ok((migration, current))
    }

    /// Publishes either a frozen V1-V3 plan or a canonical V4 artifact.
    pub fn publish_plan_artifact(
        &self,
        artifact: &HistoricalUniversePlanArtifact,
    ) -> Result<PathBuf> {
        artifact.verify()?;
        match artifact {
            HistoricalUniversePlanArtifact::Legacy(plan) => self.publish_plan(plan),
            HistoricalUniversePlanArtifact::V4(_) | HistoricalUniversePlanArtifact::V5(_) => self
                .publish(
                    "plans",
                    artifact.plan_sha256(),
                    &artifact.canonical_json_bytes()?,
                ),
        }
    }

    /// Publishes the V3 rollback first and V4 second. A failure may leave only
    /// an immutable content-addressed orphan; no mutable active pointer exists.
    pub fn publish_plan_write_set(
        &self,
        write_set: &HistoricalUniversePlanWriteSet,
    ) -> Result<HistoricalUniversePublishedPlanSet> {
        let rollback_v3_path = self.publish_plan(write_set.rollback_v3())?;
        let v4_path = self
            .publish_plan_artifact(&HistoricalUniversePlanArtifact::V4(write_set.v4().clone()))?;
        Ok(HistoricalUniversePublishedPlanSet {
            v4_path,
            rollback_v3_path,
            v4_plan_sha256: write_set.v4().plan_sha256().to_string(),
            rollback_v3_plan_sha256: write_set.rollback_v3().plan_sha256.clone(),
        })
    }

    /// Loads V1-V5 by the flat top-level `plan_version` discriminator.
    ///
    /// V4 bytes must exactly match the fixed-order canonical writer. The legacy
    /// `load_plan` path intentionally keeps its existing V1-V3-only behavior.
    pub fn load_plan_artifact(&self, sha256: &str) -> Result<HistoricalUniversePlanArtifact> {
        let bytes = self.load_bytes("plans", sha256)?;
        let value: serde_json::Value = serde_json::from_slice(&bytes)?;
        let version = value
            .get("plan_version")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| validation("historical universe plan lacks plan_version"))?;
        let artifact = match version {
            1..=3 => HistoricalUniversePlanArtifact::Legacy(serde_json::from_slice(&bytes)?),
            4 | 5 => {
                let artifact: HistoricalUniversePlanArtifact = serde_json::from_slice(&bytes)?;
                if artifact.canonical_json_bytes()? != bytes {
                    return Err(validation(
                        "historical universe artifact bytes are not canonical",
                    ));
                }
                artifact
            }
            _ => {
                return Err(validation(format!(
                    "unsupported historical universe plan version {version}"
                )));
            }
        };
        artifact.verify()?;
        if artifact.plan_sha256() != sha256 {
            return Err(validation("historical universe plan path/hash mismatch"));
        }
        Ok(artifact)
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

    /// Verifies the complete acquisition/catalog chain for a current V5
    /// executable plan. There is deliberately no V3 rollback dependency.
    pub fn verify_current_plan_artifact_chain(
        &self,
        plan: &HistoricalUniversePlanV5,
    ) -> Result<()> {
        plan.verify()?;
        let identity = plan.identity();
        let acquisition = self.load_acquisition(identity.acquisition_sha256())?;
        let semantic = self.load_semantic_catalog(identity.semantic_catalog_sha256())?;
        semantic.validate_against_acquisition(&acquisition)?;
        if acquisition.proof != identity.proof()
            || !matches!(
                acquisition.proof,
                HistoricalCatalogProof::AuthoritativeLifecycle
                    | HistoricalCatalogProof::ProviderHistoryObserved
            )
        {
            return Err(validation(
                "historical universe plan proof does not match executable acquisition",
            ));
        }
        if semantic.acquisition_sha256 != acquisition.acquisition_sha256 {
            return Err(validation(
                "historical universe semantic catalog acquisition link broken",
            ));
        }
        let timeline = plan.timeline();
        if timeline.catalog_id != semantic.catalog.catalog_id
            || timeline.catalog_sha256 != semantic.catalog.content_sha256()
            || timeline.calendar_identity != semantic.catalog.calendar_identity
            || identity.calendar_identity() != semantic.catalog.calendar_identity
        {
            return Err(validation(
                "historical universe plan timeline does not match semantic catalog",
            ));
        }
        Ok(())
    }

    /// Verifies the version-specific artifact chain without applying numeric
    /// `plan_version >= N` assumptions.
    pub fn verify_plan_artifact_chain_artifact(
        &self,
        artifact: &HistoricalUniversePlanArtifact,
    ) -> Result<()> {
        let plan = match artifact {
            HistoricalUniversePlanArtifact::Legacy(plan) => {
                return self.verify_plan_artifact_chain(plan);
            }
            HistoricalUniversePlanArtifact::V4(plan) => plan,
            HistoricalUniversePlanArtifact::V5(plan) => {
                return self.verify_current_plan_artifact_chain(plan);
            }
        };

        plan.verify()?;
        let identity = plan.identity();
        let acquisition = self.load_acquisition(identity.acquisition_sha256())?;
        let semantic = self.load_semantic_catalog(identity.semantic_catalog_sha256())?;
        semantic.validate_against_acquisition(&acquisition)?;
        if acquisition.proof != identity.proof()
            || !matches!(
                acquisition.proof,
                HistoricalCatalogProof::AuthoritativeLifecycle
                    | HistoricalCatalogProof::ProviderHistoryObserved
            )
        {
            return Err(validation(
                "historical universe V4 proof does not match executable acquisition",
            ));
        }
        if semantic.acquisition_sha256 != acquisition.acquisition_sha256 {
            return Err(validation(
                "historical universe V4 semantic/acquisition link is broken",
            ));
        }
        let timeline = plan.timeline();
        if timeline.catalog_id != semantic.catalog.catalog_id
            || timeline.catalog_sha256 != semantic.catalog.content_sha256()
            || timeline.calendar_identity != semantic.catalog.calendar_identity
            || identity.calendar_identity() != semantic.catalog.calendar_identity
        {
            return Err(validation(
                "historical universe V4 timeline does not match semantic catalog",
            ));
        }

        let rollback = self.load_plan(identity.rollback_v3_plan_sha256())?;
        if rollback.plan_version != 3
            || rollback.timeline != *timeline
            || rollback.budget != plan.budget()
        {
            return Err(validation(
                "historical universe V4 rollback projection does not match V4 plan",
            ));
        }
        let rollback_identity = rollback
            .v3_identity
            .as_ref()
            .ok_or_else(|| validation("historical universe V4 rollback plan lacks V3 identity"))?;
        let rollback_execution = rollback
            .v3_execution
            .as_ref()
            .ok_or_else(|| validation("historical universe V4 rollback plan lacks V3 execution"))?;
        if rollback_identity.acquisition_sha256 != identity.acquisition_sha256()
            || rollback_identity.semantic_catalog_sha256 != identity.semantic_catalog_sha256()
            || rollback_identity.proof != identity.proof()
            || rollback_identity.canonical_universe
                != format!("universe-v2-ast:{}", identity.normalized_ast_sha256())
            || rollback_identity.canonicalization_identity
                != HISTORICAL_UNIVERSE_V3_PROJECTION_CANONICALIZER_ID
            || rollback_identity.compiler_identity != HISTORICAL_UNIVERSE_V3_PROJECTION_COMPILER_ID
            || rollback_identity.continuous_identity.as_deref() != identity.continuous_identity()
            || rollback_identity.ranking_identity.as_deref() != identity.ranking_identity()
            || plan.execution().to_v3()? != *rollback_execution
        {
            return Err(validation(
                "historical universe V4 rollback identity/execution chain mismatch",
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
        Ok(serde_json::from_slice(&self.load_bytes(family, sha256)?)?)
    }

    fn load_bytes(&self, family: &str, sha256: &str) -> Result<Vec<u8>> {
        let path = self.artifact_path(family, sha256)?;
        reject_symlink_ancestors(&path)?;
        let mut bytes = Vec::new();
        File::open(path)?.read_to_end(&mut bytes)?;
        Ok(bytes)
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
