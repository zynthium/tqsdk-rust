use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{DataError, Result};

/// Stable identity for an instrument visible in a historical universe.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UniverseInstrumentId {
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

impl UniverseInstrumentId {
    #[must_use]
    pub fn symbol(&self) -> String {
        match self {
            Self::Physical { symbol } => symbol.clone(),
            Self::Continuous {
                exchange_id,
                product_id,
            } => format!("KQ.m@{exchange_id}.{product_id}"),
            Self::Index {
                exchange_id,
                product_id,
            } => format!("KQ.i@{exchange_id}.{product_id}"),
        }
    }
}

/// An interval during which a physical contract is historically active.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveInterval {
    pub start_ns: i64,
    pub end_ns: i64,
}

impl ActiveInterval {
    pub fn new(start_ns: i64, end_ns: i64) -> Result<Self> {
        if end_ns <= start_ns {
            return Err(validation(
                "active interval end_ns must be greater than start_ns",
            ));
        }
        Ok(Self { start_ns, end_ns })
    }

    #[must_use]
    pub fn intersects(self, start_ns: i64, end_ns: i64) -> bool {
        self.start_ns < end_ns && start_ns < self.end_ns
    }
}

/// Historical lifecycle metadata supplied by a catalogue snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogContract {
    pub physical_symbol: String,
    pub exchange_id: String,
    pub product_id: String,
    pub lifecycle: Vec<ActiveInterval>,
}

impl CatalogContract {
    pub fn new(
        physical_symbol: impl Into<String>,
        exchange_id: impl Into<String>,
        product_id: impl Into<String>,
        lifecycle: Vec<ActiveInterval>,
    ) -> Result<Self> {
        let contract = Self {
            physical_symbol: normalized("physical_symbol", physical_symbol.into())?,
            exchange_id: normalized("exchange_id", exchange_id.into())?,
            product_id: normalized("product_id", product_id.into())?,
            lifecycle,
        };
        contract.validate()?;
        Ok(contract)
    }

    fn validate(&self) -> Result<()> {
        if self.lifecycle.is_empty() {
            return Err(validation("catalog contract lifecycle must not be empty"));
        }
        let mut previous_end = None;
        for interval in &self.lifecycle {
            if interval.end_ns <= interval.start_ns {
                return Err(validation(
                    "catalog lifecycle interval must have positive width",
                ));
            }
            if previous_end.is_some_and(|end| interval.start_ns < end) {
                return Err(validation(
                    "catalog lifecycle intervals must be sorted and non-overlapping",
                ));
            }
            previous_end = Some(interval.end_ns);
        }
        Ok(())
    }
}

/// Physical filters for a dynamic historical universe.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DynamicUniverseScope {
    pub exchanges: BTreeSet<String>,
    pub products: BTreeSet<String>,
    pub excluded_exchanges: BTreeSet<String>,
}

impl DynamicUniverseScope {
    #[must_use]
    pub fn all() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn includes(&self, contract: &CatalogContract) -> bool {
        (self.exchanges.is_empty() || self.exchanges.contains(&contract.exchange_id))
            && (self.products.is_empty() || self.products.contains(&contract.product_id))
            && !self.excluded_exchanges.contains(&contract.exchange_id)
    }
}

/// Derived logical views selected in addition to active physical contracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DerivedView {
    Continuous,
    Index,
}

/// Versioned, complete historical contract catalogue supplied by the caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogSnapshot {
    pub format_version: u32,
    pub catalog_id: String,
    pub calendar_identity: String,
    pub complete: bool,
    pub scope: DynamicUniverseScope,
    pub contracts: Vec<CatalogContract>,
    content_sha256: String,
}

impl CatalogSnapshot {
    pub fn new(
        catalog_id: impl Into<String>,
        calendar_identity: impl Into<String>,
        complete: bool,
        scope: DynamicUniverseScope,
        mut contracts: Vec<CatalogContract>,
    ) -> Result<Self> {
        contracts.sort_by(|left, right| {
            (&left.exchange_id, &left.product_id, &left.physical_symbol).cmp(&(
                &right.exchange_id,
                &right.product_id,
                &right.physical_symbol,
            ))
        });
        let mut snapshot = Self {
            format_version: 1,
            catalog_id: normalized("catalog_id", catalog_id.into())?,
            calendar_identity: normalized("calendar_identity", calendar_identity.into())?,
            complete,
            scope,
            contracts,
            content_sha256: String::new(),
        };
        snapshot.validate()?;
        snapshot.content_sha256 = snapshot.compute_content_sha256()?;
        Ok(snapshot)
    }

    #[must_use]
    pub fn content_sha256(&self) -> &str {
        &self.content_sha256
    }

    pub fn validate(&self) -> Result<()> {
        if self.format_version != 1 {
            return Err(validation("unsupported historical catalog format version"));
        }
        if !self.complete {
            return Err(validation(
                "historical catalog must declare complete=true for dynamic universe planning",
            ));
        }
        let mut previous = None;
        for contract in &self.contracts {
            contract.validate()?;
            let key = (
                &contract.exchange_id,
                &contract.product_id,
                &contract.physical_symbol,
            );
            if previous.is_some_and(|previous| previous >= key) {
                return Err(validation(
                    "historical catalog contracts must have unique canonical ordering",
                ));
            }
            previous = Some(key);
        }
        Ok(())
    }

    fn compute_content_sha256(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Identity<'a> {
            format_version: u32,
            catalog_id: &'a str,
            calendar_identity: &'a str,
            complete: bool,
            scope: &'a DynamicUniverseScope,
            contracts: &'a [CatalogContract],
        }

        let encoded = serde_json::to_vec(&Identity {
            format_version: self.format_version,
            catalog_id: &self.catalog_id,
            calendar_identity: &self.calendar_identity,
            complete: self.complete,
            scope: &self.scope,
            contracts: &self.contracts,
        })?;
        Ok(format!("sha256:{:x}", Sha256::digest(encoded)))
    }
}

/// A membership transition emitted by a historical universe timeline.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum UniverseMemberChange {
    Add {
        instrument: UniverseInstrumentId,
        provenance: String,
    },
    Remove {
        instrument: UniverseInstrumentId,
    },
}

/// All membership changes that become visible at one UTC epoch-nanosecond instant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UniverseTimelineBatch {
    pub effective_ns: i64,
    pub changes: Vec<UniverseMemberChange>,
}

/// Compiled historical membership timeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalUniverseTimeline {
    pub catalog_id: String,
    pub catalog_sha256: String,
    pub calendar_identity: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub scope: DynamicUniverseScope,
    pub derived_views: BTreeSet<DerivedView>,
    /// Earliest known timestamp for each physical contract represented here.
    /// Cache warmup may begin there while replay remains membership-clipped.
    #[serde(default)]
    pub physical_listing_starts: BTreeMap<String, i64>,
    pub batches: Vec<UniverseTimelineBatch>,
}

/// Explicit resource limit required to prepare a dynamic universe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct UniverseBudget {
    pub max_batches: usize,
    pub max_changes: usize,
}

impl UniverseBudget {
    pub fn new(max_batches: usize, max_changes: usize) -> Result<Self> {
        if max_batches == 0 || max_changes == 0 {
            return Err(validation("universe budget limits must be positive"));
        }
        Ok(Self {
            max_batches,
            max_changes,
        })
    }
}

/// Reusable, identity-pinned offline preparation result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalUniversePlan {
    pub plan_version: u32,
    pub plan_sha256: String,
    pub timeline: HistoricalUniverseTimeline,
    pub budget: UniverseBudget,
}

impl HistoricalUniverseTimeline {
    pub fn prepare(self, budget: UniverseBudget) -> Result<HistoricalUniversePlan> {
        self.validate()?;
        let changes: usize = self.batches.iter().map(|batch| batch.changes.len()).sum();
        if self.batches.len() > budget.max_batches {
            return Err(validation(format!(
                "historical universe requires {} batches, exceeding budget {}",
                self.batches.len(),
                budget.max_batches
            )));
        }
        if changes > budget.max_changes {
            return Err(validation(format!(
                "historical universe requires {changes} changes, exceeding budget {}",
                budget.max_changes
            )));
        }
        let bytes = serde_json::to_vec(&(2_u32, &self, budget))?;
        Ok(HistoricalUniversePlan {
            plan_version: 2,
            plan_sha256: format!("sha256:{:x}", Sha256::digest(bytes)),
            timeline: self,
            budget,
        })
    }

    pub fn validate(&self) -> Result<()> {
        if self.start_ns >= self.end_ns {
            return Err(validation(
                "historical universe timeline end_ns must be greater than start_ns",
            ));
        }
        let mut previous = None;
        let mut active = BTreeSet::new();
        for batch in &self.batches {
            if !(self.start_ns..self.end_ns).contains(&batch.effective_ns) {
                return Err(validation(
                    "historical universe batch falls outside its timeline",
                ));
            }
            if previous.is_some_and(|time| time >= batch.effective_ns) {
                return Err(validation(
                    "historical universe batches must be strictly time ordered",
                ));
            }
            if batch.changes.is_empty() {
                return Err(validation("historical universe batch must not be empty"));
            }
            for change in &batch.changes {
                match change {
                    UniverseMemberChange::Add { instrument, .. } => {
                        if !active.insert(instrument.clone()) {
                            return Err(validation(
                                "historical universe adds an already-active instrument",
                            ));
                        }
                    }
                    UniverseMemberChange::Remove { instrument } => {
                        if !active.remove(instrument) {
                            return Err(validation(
                                "historical universe removes an inactive instrument",
                            ));
                        }
                    }
                }
            }
            previous = Some(batch.effective_ns);
        }
        Ok(())
    }
}

impl HistoricalUniversePlan {
    pub fn verify(&self) -> Result<()> {
        self.timeline.validate()?;
        UniverseBudget::new(self.budget.max_batches, self.budget.max_changes)?;
        let bytes = match self.plan_version {
            1 => {
                if !self.timeline.physical_listing_starts.is_empty() {
                    return Err(validation(
                        "historical universe plan v1 must not contain listing starts",
                    ));
                }
                serde_json::to_vec(&(
                    1_u32,
                    HistoricalUniverseTimelineV1::from(&self.timeline),
                    self.budget,
                ))?
            }
            2 => {
                let mut physical_adds = BTreeMap::new();
                for batch in &self.timeline.batches {
                    for change in &batch.changes {
                        if let UniverseMemberChange::Add {
                            instrument: UniverseInstrumentId::Physical { symbol },
                            ..
                        } = change
                        {
                            physical_adds
                                .entry(symbol.as_str())
                                .or_insert(batch.effective_ns);
                        }
                    }
                }
                if physical_adds.len() != self.timeline.physical_listing_starts.len() {
                    return Err(validation(
                        "historical universe plan v2 requires listing starts for every physical member",
                    ));
                }
                for (symbol, first_add_ns) in physical_adds {
                    let Some(listing_start_ns) = self.timeline.physical_listing_starts.get(symbol)
                    else {
                        return Err(validation(format!(
                            "historical universe plan v2 lacks listing start for {symbol}"
                        )));
                    };
                    if *listing_start_ns > first_add_ns {
                        return Err(validation(format!(
                            "historical universe listing start follows first membership for {symbol}"
                        )));
                    }
                }
                serde_json::to_vec(&(2_u32, &self.timeline, self.budget))?
            }
            version => {
                return Err(validation(format!(
                    "unsupported historical universe plan version {version}"
                )));
            }
        };
        let expected = format!("sha256:{:x}", Sha256::digest(bytes));
        if self.plan_sha256 != expected {
            return Err(validation("historical universe plan hash mismatch"));
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct HistoricalUniverseTimelineV1<'a> {
    catalog_id: &'a str,
    catalog_sha256: &'a str,
    calendar_identity: &'a str,
    start_ns: i64,
    end_ns: i64,
    scope: &'a DynamicUniverseScope,
    derived_views: &'a BTreeSet<DerivedView>,
    batches: &'a [UniverseTimelineBatch],
}

impl<'a> From<&'a HistoricalUniverseTimeline> for HistoricalUniverseTimelineV1<'a> {
    fn from(timeline: &'a HistoricalUniverseTimeline) -> Self {
        Self {
            catalog_id: &timeline.catalog_id,
            catalog_sha256: &timeline.catalog_sha256,
            calendar_identity: &timeline.calendar_identity,
            start_ns: timeline.start_ns,
            end_ns: timeline.end_ns,
            scope: &timeline.scope,
            derived_views: &timeline.derived_views,
            batches: &timeline.batches,
        }
    }
}

impl CatalogSnapshot {
    pub fn compile_timeline(
        &self,
        start_ns: i64,
        end_ns: i64,
        scope: DynamicUniverseScope,
        derived_views: impl IntoIterator<Item = DerivedView>,
    ) -> Result<HistoricalUniverseTimeline> {
        if end_ns <= start_ns {
            return Err(validation(
                "historical universe end_ns must be greater than start_ns",
            ));
        }
        self.validate()?;
        if self.scope != scope {
            return Err(validation(
                "historical universe scope must exactly match the pinned catalog scope",
            ));
        }

        let derived_views = derived_views.into_iter().collect::<BTreeSet<_>>();
        let mut physical_events: BTreeMap<i64, Vec<UniverseMemberChange>> = BTreeMap::new();
        let mut product_deltas: BTreeMap<i64, BTreeMap<(String, String), i32>> = BTreeMap::new();
        let mut physical_listing_starts = BTreeMap::new();

        for contract in self
            .contracts
            .iter()
            .filter(|contract| scope.includes(contract))
        {
            for interval in &contract.lifecycle {
                if !interval.intersects(start_ns, end_ns) {
                    continue;
                }
                physical_listing_starts
                    .entry(contract.physical_symbol.clone())
                    .or_insert(contract.lifecycle[0].start_ns);
                let active_start = interval.start_ns.max(start_ns);
                let active_end = interval.end_ns.min(end_ns);
                let physical = UniverseInstrumentId::Physical {
                    symbol: contract.physical_symbol.clone(),
                };
                physical_events
                    .entry(active_start)
                    .or_default()
                    .push(UniverseMemberChange::Add {
                        instrument: physical.clone(),
                        provenance: format!("catalog:{}", self.catalog_id),
                    });
                physical_events
                    .entry(active_end)
                    .or_default()
                    .push(UniverseMemberChange::Remove {
                        instrument: physical,
                    });
                let product = (contract.exchange_id.clone(), contract.product_id.clone());
                *product_deltas
                    .entry(active_start)
                    .or_default()
                    .entry(product.clone())
                    .or_default() += 1;
                *product_deltas
                    .entry(active_end)
                    .or_default()
                    .entry(product)
                    .or_default() -= 1;
            }
        }

        let mut all_times = physical_events
            .keys()
            .chain(product_deltas.keys())
            .copied()
            .collect::<BTreeSet<_>>();
        all_times.remove(&end_ns);

        let mut product_members = BTreeMap::<(String, String), i32>::new();
        let mut batches = Vec::new();
        for effective_ns in all_times {
            let mut changes = physical_events.remove(&effective_ns).unwrap_or_default();
            if let Some(deltas) = product_deltas.remove(&effective_ns) {
                for (product, delta) in deltas {
                    let previous = product_members.get(&product).copied().unwrap_or_default();
                    let current = previous + delta;
                    if current < 0 {
                        return Err(validation("historical product membership underflow"));
                    }
                    if previous == 0 && current > 0 {
                        append_derived_additions(
                            &mut changes,
                            &derived_views,
                            &product,
                            &self.catalog_id,
                        );
                    }
                    if previous > 0 && current == 0 {
                        append_derived_removals(&mut changes, &derived_views, &product);
                    }
                    product_members.insert(product, current);
                }
            }
            changes.sort();
            if !changes.is_empty() {
                batches.push(UniverseTimelineBatch {
                    effective_ns,
                    changes,
                });
            }
        }

        Ok(HistoricalUniverseTimeline {
            catalog_id: self.catalog_id.clone(),
            catalog_sha256: self.content_sha256.clone(),
            calendar_identity: self.calendar_identity.clone(),
            start_ns,
            end_ns,
            scope,
            derived_views,
            physical_listing_starts,
            batches,
        })
    }
}

fn append_derived_additions(
    changes: &mut Vec<UniverseMemberChange>,
    views: &BTreeSet<DerivedView>,
    product: &(String, String),
    catalog_id: &str,
) {
    for view in views {
        let instrument = derived_instrument(*view, product);
        changes.push(UniverseMemberChange::Add {
            instrument,
            provenance: format!("catalog:{catalog_id}:derived"),
        });
    }
}

fn append_derived_removals(
    changes: &mut Vec<UniverseMemberChange>,
    views: &BTreeSet<DerivedView>,
    product: &(String, String),
) {
    for view in views {
        changes.push(UniverseMemberChange::Remove {
            instrument: derived_instrument(*view, product),
        });
    }
}

fn derived_instrument(view: DerivedView, product: &(String, String)) -> UniverseInstrumentId {
    match view {
        DerivedView::Continuous => UniverseInstrumentId::Continuous {
            exchange_id: product.0.clone(),
            product_id: product.1.clone(),
        },
        DerivedView::Index => UniverseInstrumentId::Index {
            exchange_id: product.0.clone(),
            product_id: product.1.clone(),
        },
    }
}

fn normalized(name: &str, value: String) -> Result<String> {
    let value = value.trim().to_string();
    if value.is_empty() {
        return Err(validation(format!("{name} must not be empty")));
    }
    Ok(value)
}

fn validation(message: impl Into<String>) -> DataError {
    DataError::Validation(message.into())
}
