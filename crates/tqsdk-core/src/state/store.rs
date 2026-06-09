use std::{
    collections::BTreeSet,
    sync::{
        LockResult, RwLock, RwLockReadGuard, RwLockWriteGuard,
        atomic::{AtomicU64, Ordering},
    },
};

use serde_json::{Map, Value};

use crate::{Result, events::NormalizedMutation, ids::Revision};

use super::{
    AppliedChange, MarketStateReadGuard, MarketStateView, MarketTradeStateReadGuard, ObjectKey,
    PathSegment, StateReadView, TradeStateReadGuard, TradeStateView, read::get_at_path,
};

/// Owned snapshot clone of the runtime state tree.
///
/// Prefer `StateReadView` and `SnapshotReadGuard` on hot paths. Keep
/// `StateSnapshot` when detached ownership is required.
#[derive(Debug, Clone, PartialEq)]
pub struct StateSnapshot {
    revision: Revision,
    data: Value,
}

#[derive(Debug)]
pub(crate) struct StateStore {
    revision: AtomicU64,
    quotes: RwLock<Value>,
    trading_status: RwLock<Value>,
    charts: RwLock<Value>,
    klines: RwLock<Value>,
    ticks: RwLock<Value>,
    trade: RwLock<Value>,
    query: RwLock<Value>,
    schema: RwLock<Value>,
    replay: RwLock<Value>,
    system: RwLock<Value>,
    runtime: RwLock<Value>,
    other: RwLock<Value>,
}

impl StateSnapshot {
    /// Creates an owned empty snapshot at the provided revision.
    pub fn new(revision: Revision) -> Self {
        Self {
            revision,
            data: Value::Object(Map::new()),
        }
    }

    /// Returns the revision carried by this owned snapshot.
    pub fn revision(&self) -> Revision {
        self.revision
    }

    /// Looks up a value at the provided path.
    pub fn get<I, S>(&self, path: I) -> Option<&Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.read().get(path)
    }

    /// Looks up a value using a borrowed path slice.
    pub fn get_path(&self, path: &[&str]) -> Option<&Value> {
        self.read().get_path(path)
    }

    /// Decodes a value at the provided path.
    pub fn decode<T, I, S>(&self, path: I) -> Result<Option<T>>
    where
        T: serde::de::DeserializeOwned,
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.read().decode(path)
    }

    /// Decodes a value using a borrowed path slice.
    pub fn decode_path<T>(&self, path: &[&str]) -> Result<Option<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        self.read().decode_path(path)
    }

    /// Returns a borrowed read view over this owned snapshot.
    pub fn read(&self) -> StateReadView<'_> {
        StateReadView::new(self.revision, &self.data)
    }

    /// Returns a typed market-domain view over this owned snapshot.
    pub fn market_state(&self) -> MarketStateView<'_> {
        self.read().market_state()
    }

    /// Returns a typed trade-domain view over this owned snapshot.
    pub fn trade_state(&self) -> TradeStateView<'_> {
        self.read().trade_state()
    }

    fn from_data(revision: Revision, data: Value) -> Self {
        Self { revision, data }
    }
}

pub(crate) struct StatePartitionReadGuard<'a> {
    partition: RwLockReadGuard<'a, Value>,
}

impl<'a> StatePartitionReadGuard<'a> {
    fn new(partition: RwLockReadGuard<'a, Value>) -> Self {
        Self { partition }
    }

    pub(crate) fn get_path(&self, path: &[&str]) -> Option<&Value> {
        get_at_path(&self.partition, path.iter().copied())
    }
}

impl StateStore {
    pub(crate) fn new(revision: Revision) -> Self {
        Self {
            revision: AtomicU64::new(revision.get()),
            quotes: empty_partition(),
            trading_status: empty_partition(),
            charts: empty_partition(),
            klines: empty_partition(),
            ticks: empty_partition(),
            trade: empty_partition(),
            query: empty_partition(),
            schema: empty_partition(),
            replay: empty_partition(),
            system: empty_partition(),
            runtime: empty_partition(),
            other: empty_partition(),
        }
    }

    pub(crate) fn revision(&self) -> Revision {
        Revision::new(self.revision.load(Ordering::SeqCst))
    }

    pub(crate) fn snapshot(&self) -> StateSnapshot {
        let guards = StateRoot::ALL
            .iter()
            .copied()
            .map(|root| (root, rwlock_read(root.partition(self))))
            .collect::<Vec<_>>();
        let revision = self.revision();
        let mut data = Map::new();

        for (root, guard) in guards {
            if root == StateRoot::Other {
                merge_fallback_roots(&mut data, &guard);
                continue;
            }

            if !is_empty_partition(&guard) {
                data.insert(root.as_str().to_string(), guard.clone());
            }
        }

        StateSnapshot::from_data(revision, Value::Object(data))
    }

    pub(crate) fn read_market_state(&self) -> MarketStateReadGuard<'_> {
        let quotes = rwlock_read(&self.quotes);
        let trading_status = rwlock_read(&self.trading_status);
        let charts = rwlock_read(&self.charts);
        let klines = rwlock_read(&self.klines);
        let ticks = rwlock_read(&self.ticks);
        MarketStateReadGuard::new(
            self.revision(),
            quotes,
            trading_status,
            charts,
            klines,
            ticks,
        )
    }

    pub(crate) fn read_trade_state(&self) -> TradeStateReadGuard<'_> {
        let trade = rwlock_read(&self.trade);
        TradeStateReadGuard::new(self.revision(), trade)
    }

    pub(crate) fn read_market_trade_state(&self) -> MarketTradeStateReadGuard<'_> {
        let quotes = rwlock_read(&self.quotes);
        let trading_status = rwlock_read(&self.trading_status);
        let charts = rwlock_read(&self.charts);
        let klines = rwlock_read(&self.klines);
        let ticks = rwlock_read(&self.ticks);
        let trade = rwlock_read(&self.trade);
        let revision = self.revision();
        let market =
            MarketStateReadGuard::new(revision, quotes, trading_status, charts, klines, ticks);
        let trade = TradeStateReadGuard::new(revision, trade);
        MarketTradeStateReadGuard::new(revision, market, trade)
    }

    pub(crate) fn read_partition(&self, root: &str) -> Option<StatePartitionReadGuard<'_>> {
        let root = StateRoot::from_segment(root)?;
        let partition = rwlock_read(root.partition(self));
        Some(StatePartitionReadGuard::new(partition))
    }

    pub(crate) fn read_runtime_state(&self) -> StatePartitionReadGuard<'_> {
        self.read_partition("runtime")
            .expect("runtime partition root should be known")
    }

    #[cfg(test)]
    pub(crate) fn apply(
        &self,
        revision: Revision,
        mutations: &[NormalizedMutation],
    ) -> Vec<AppliedChange> {
        self.apply_with(revision, mutations, |applied| applied)
            .unwrap_or_default()
    }

    pub(crate) fn apply_with<T, F>(
        &self,
        revision: Revision,
        mutations: &[NormalizedMutation],
        on_applied: F,
    ) -> Option<T>
    where
        F: FnOnce(Vec<AppliedChange>) -> T,
    {
        let mut roots = BTreeSet::new();
        for mutation in mutations {
            roots.insert(partition_path(mutation).0);
        }

        let mut guards = roots
            .into_iter()
            .map(|root| (root, rwlock_write(root.partition(self))))
            .collect::<Vec<_>>();

        let mut applied = Vec::new();
        for mutation in mutations {
            let (root, path) = partition_path(mutation);
            let Some((_, partition)) = guards
                .iter_mut()
                .find(|(partition_root, _)| *partition_root == root)
            else {
                continue;
            };
            if let Some(changed) = apply_mutation_at_partition(&mut *partition, path, mutation) {
                applied.push(changed);
            }
        }

        if !applied.is_empty() {
            self.revision.store(revision.get(), Ordering::SeqCst);
            Some(on_applied(applied))
        } else {
            None
        }
    }

    #[cfg(test)]
    pub(crate) fn partition_roots_for_test(&self) -> Vec<&'static str> {
        StateRoot::ALL
            .iter()
            .copied()
            .filter_map(StateRoot::visible_root)
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn poison_partition_for_test(&self, root: &str) {
        let root = StateRoot::from_segment(root).unwrap_or(StateRoot::Other);
        let _guard = root.partition(self).write().unwrap();
        panic!("poison state partition");
    }
}

fn apply_mutation_at_partition(
    root: &mut Value,
    path: &[PathSegment],
    mutation: &NormalizedMutation,
) -> Option<AppliedChange> {
    let mut changed_fields = Vec::new();
    apply_mutation_at_path(
        root,
        path,
        mutation.object.as_ref(),
        &mutation.fields,
        &mut changed_fields,
    );

    if changed_fields.is_empty() {
        None
    } else {
        let (root, _) = partition_path(mutation);
        Some(AppliedChange::new(
            root.as_str(),
            mutation.path.clone(),
            mutation.object.clone(),
            changed_fields,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum StateRoot {
    Quotes,
    TradingStatus,
    Charts,
    Klines,
    Ticks,
    Trade,
    Query,
    Schema,
    Replay,
    System,
    Runtime,
    Other,
}

impl StateRoot {
    const ALL: &'static [Self] = &[
        Self::Quotes,
        Self::TradingStatus,
        Self::Charts,
        Self::Klines,
        Self::Ticks,
        Self::Trade,
        Self::Query,
        Self::Schema,
        Self::Replay,
        Self::System,
        Self::Runtime,
        Self::Other,
    ];

    fn from_segment(segment: &str) -> Option<Self> {
        match segment {
            "quotes" => Some(Self::Quotes),
            "trading_status" => Some(Self::TradingStatus),
            "charts" => Some(Self::Charts),
            "klines" => Some(Self::Klines),
            "ticks" => Some(Self::Ticks),
            "trade" => Some(Self::Trade),
            "query" => Some(Self::Query),
            "schema" => Some(Self::Schema),
            "replay" => Some(Self::Replay),
            "system" => Some(Self::System),
            "runtime" => Some(Self::Runtime),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Quotes => "quotes",
            Self::TradingStatus => "trading_status",
            Self::Charts => "charts",
            Self::Klines => "klines",
            Self::Ticks => "ticks",
            Self::Trade => "trade",
            Self::Query => "query",
            Self::Schema => "schema",
            Self::Replay => "replay",
            Self::System => "system",
            Self::Runtime => "runtime",
            Self::Other => "other",
        }
    }

    #[cfg(test)]
    fn visible_root(self) -> Option<&'static str> {
        match self {
            Self::Other => None,
            root => Some(root.as_str()),
        }
    }

    fn partition(self, store: &StateStore) -> &RwLock<Value> {
        match self {
            Self::Quotes => &store.quotes,
            Self::TradingStatus => &store.trading_status,
            Self::Charts => &store.charts,
            Self::Klines => &store.klines,
            Self::Ticks => &store.ticks,
            Self::Trade => &store.trade,
            Self::Query => &store.query,
            Self::Schema => &store.schema,
            Self::Replay => &store.replay,
            Self::System => &store.system,
            Self::Runtime => &store.runtime,
            Self::Other => &store.other,
        }
    }
}

fn partition_path(mutation: &NormalizedMutation) -> (StateRoot, &[PathSegment]) {
    let segments = mutation.path.segments();
    let Some(root) = segments.first() else {
        return (StateRoot::Other, segments);
    };

    match StateRoot::from_segment(root) {
        Some(root) => (root, &segments[1..]),
        None => (StateRoot::Other, segments),
    }
}

fn empty_partition() -> RwLock<Value> {
    RwLock::new(Value::Object(Map::new()))
}

fn recover_poisoned_lock<G>(result: LockResult<G>) -> G {
    match result {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn rwlock_read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    recover_poisoned_lock(lock.read())
}

fn rwlock_write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    recover_poisoned_lock(lock.write())
}

fn is_empty_partition(value: &Value) -> bool {
    value.as_object().is_some_and(Map::is_empty)
}

fn merge_fallback_roots(root: &mut Map<String, Value>, fallback: &Value) {
    let Some(entries) = fallback.as_object() else {
        return;
    };
    for (key, value) in entries {
        if !is_empty_partition(value) {
            root.insert(key.clone(), value.clone());
        }
    }
}

fn apply_mutation_at_path(
    cursor: &mut Value,
    path: &[PathSegment],
    object: Option<&ObjectKey>,
    fields: &[crate::events::FieldMutation],
    changed_fields: &mut Vec<String>,
) {
    if path.is_empty() {
        apply_fields(cursor, object, fields, changed_fields);
        return;
    }

    let segment = &path[0];
    let child = ensure_child_object(cursor, segment);
    apply_mutation_at_path(child, &path[1..], object, fields, changed_fields);
    prune_empty_child(cursor, segment);
}

fn apply_fields(
    cursor: &mut Value,
    object: Option<&ObjectKey>,
    fields: &[crate::events::FieldMutation],
    changed_fields: &mut Vec<String>,
) {
    if !cursor.is_object() {
        *cursor = Value::Object(Map::new());
    }

    let Value::Object(map) = cursor else {
        unreachable!("state snapshot path targets must always resolve to objects");
    };

    for field in fields {
        let preserve_null = preserves_null_field(object, &field.field);
        let has_changed = if field.value.is_null() && !preserve_null {
            map.contains_key(&field.field)
        } else {
            map.get(&field.field) != Some(&field.value)
        };

        if !has_changed {
            continue;
        }

        if field.value.is_null() && !preserve_null {
            map.remove(&field.field);
        } else {
            map.insert(field.field.clone(), field.value.clone());
        }

        changed_fields.push(field.field.clone());
    }
}

fn preserves_null_field(object: Option<&ObjectKey>, field: &str) -> bool {
    matches!(object, Some(ObjectKey::SessionReconnect)) && field == "max_attempts"
}

fn ensure_child_object<'a>(root: &'a mut Value, segment: &PathSegment) -> &'a mut Value {
    if !root.is_object() {
        *root = Value::Object(Map::new());
    }

    let Value::Object(map) = root else {
        unreachable!("state snapshot intermediate nodes must always be objects");
    };
    let child = map
        .entry(segment.clone())
        .or_insert_with(|| Value::Object(Map::new()));
    if !child.is_object() {
        *child = Value::Object(Map::new());
    }

    child
}

fn prune_empty_child(root: &mut Value, segment: &PathSegment) {
    let Some(map) = root.as_object_mut() else {
        return;
    };

    let should_remove = map
        .get(segment)
        .and_then(Value::as_object)
        .is_some_and(Map::is_empty);
    if should_remove {
        map.remove(segment);
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use serde_json::json;

    use super::*;
    use crate::{
        events::{FieldMutation, MutationSource, NormalizedMutation},
        state::StatePath,
    };

    #[test]
    fn state_store_materializes_domain_partitions_as_compatible_snapshot() {
        let store = StateStore::new(Revision::new(0));
        assert!(store.partition_roots_for_test().contains(&"quotes"));
        assert!(store.partition_roots_for_test().contains(&"trade"));

        let market = NormalizedMutation {
            path: StatePath::new(["quotes", "SHFE.au2602"]),
            object: None,
            fields: vec![FieldMutation {
                field: "last_price".to_string(),
                value: json!(620.5),
            }],
            source: MutationSource::MarketDiff,
        };
        let trade = NormalizedMutation {
            path: StatePath::new(["trade", "simnow", "accounts", "CNY"]),
            object: None,
            fields: vec![FieldMutation {
                field: "balance".to_string(),
                value: json!(1000.0),
            }],
            source: MutationSource::TradeReply,
        };

        assert_eq!(
            store.apply(Revision::new(1), &[market]).len(),
            1,
            "market mutation should apply to its partition"
        );
        assert_eq!(
            store.apply(Revision::new(2), &[trade]).len(),
            1,
            "trade mutation should apply to its partition"
        );

        let snapshot = store.snapshot();
        assert_eq!(snapshot.revision(), Revision::new(2));
        assert_eq!(
            snapshot.get(["quotes", "SHFE.au2602", "last_price"]),
            Some(&json!(620.5))
        );
        assert_eq!(
            snapshot.get(["trade", "simnow", "accounts", "CNY", "balance"]),
            Some(&json!(1000.0))
        );
    }

    #[test]
    fn state_store_recovers_from_poisoned_partition_lock() {
        let store = StateStore::new(Revision::new(0));

        let panic = catch_unwind(AssertUnwindSafe(|| {
            store.poison_partition_for_test("runtime");
        }));
        assert!(panic.is_err());

        assert_eq!(store.snapshot().revision(), Revision::new(0));
    }

    #[test]
    fn state_store_preserves_unbounded_session_reconnect_attempts_as_null() {
        let store = StateStore::new(Revision::new(0));
        let mutation = NormalizedMutation {
            path: StatePath::new(["system", "session", "reconnect"]),
            object: Some(ObjectKey::SessionReconnect),
            fields: vec![FieldMutation {
                field: "max_attempts".to_string(),
                value: Value::Null,
            }],
            source: MutationSource::SessionControl,
        };

        assert_eq!(
            store.apply(Revision::new(1), &[mutation]).len(),
            1,
            "session reconnect max_attempts=null is a visible unbounded policy"
        );
        assert_eq!(
            store
                .snapshot()
                .get(["system", "session", "reconnect", "max_attempts"]),
            Some(&Value::Null)
        );
    }

    #[test]
    fn state_store_still_treats_other_null_fields_as_deletes() {
        let store = StateStore::new(Revision::new(0));
        let insert = NormalizedMutation {
            path: StatePath::new(["system", "session", "reconnect"]),
            object: Some(ObjectKey::SessionReconnect),
            fields: vec![FieldMutation {
                field: "detail".to_string(),
                value: json!({ "reason": "test" }),
            }],
            source: MutationSource::SessionControl,
        };
        let delete = NormalizedMutation {
            path: StatePath::new(["system", "session", "reconnect"]),
            object: Some(ObjectKey::SessionReconnect),
            fields: vec![FieldMutation {
                field: "detail".to_string(),
                value: Value::Null,
            }],
            source: MutationSource::SessionControl,
        };

        assert_eq!(store.apply(Revision::new(1), &[insert]).len(), 1);
        assert_eq!(store.apply(Revision::new(2), &[delete]).len(), 1);
        assert_eq!(
            store
                .snapshot()
                .get(["system", "session", "reconnect", "detail"]),
            None
        );
    }
}
