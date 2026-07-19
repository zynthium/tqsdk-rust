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
        let other = rwlock_read(&self.other);
        MarketStateReadGuard::new(
            self.revision(),
            quotes,
            trading_status,
            charts,
            klines,
            ticks,
            other,
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
        let other = rwlock_read(&self.other);
        let trade = rwlock_read(&self.trade);
        let revision = self.revision();
        let market = MarketStateReadGuard::new(
            revision,
            quotes,
            trading_status,
            charts,
            klines,
            ticks,
            other,
        );
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
        let mut mutations = mutations.to_vec();
        self.apply_with(revision, &mut mutations, |applied, _| applied)
            .unwrap_or_default()
    }

    pub(crate) fn apply_with<T, F>(
        &self,
        revision: Revision,
        mutations: &mut [NormalizedMutation],
        on_applied: F,
    ) -> Option<T>
    where
        F: FnOnce(Vec<AppliedChange>, &[NormalizedMutation]) -> T,
    {
        let first = mutations.first()?;
        let first_root = partition_path(first).0;
        if mutations
            .iter()
            .all(|mutation| partition_path(mutation).0 == first_root)
        {
            return self.apply_single_root(revision, first_root, mutations, on_applied);
        }

        let mut roots = BTreeSet::new();
        for mutation in mutations.iter() {
            roots.insert(partition_path(mutation).0);
        }

        let mut guards = roots
            .into_iter()
            .map(|root| (root, rwlock_write(root.partition(self))))
            .collect::<Vec<_>>();

        let mut applied = Vec::with_capacity(mutations.len());
        for (mutation_index, mutation) in mutations.iter_mut().enumerate() {
            let NormalizedMutation {
                path,
                object,
                fields,
                ..
            } = mutation;
            let (root, relative_path) = partition_path_segments(path.segments());
            let Some((_, partition)) = guards
                .iter_mut()
                .find(|(partition_root, _)| *partition_root == root)
            else {
                continue;
            };
            if let Some(changed) = apply_mutation_at_partition(
                root,
                &mut *partition,
                relative_path,
                mutation_index,
                object,
                fields,
            ) {
                applied.push(changed);
            }
        }

        if !applied.is_empty() {
            self.revision.store(revision.get(), Ordering::SeqCst);
            Some(on_applied(applied, mutations))
        } else {
            None
        }
    }

    /// Applies a market-only batch while preserving the generic multi-root commit contract.
    ///
    /// Market updates commonly span quotes, charts, and serial rows. Locking their known
    /// partitions directly avoids allocating a root set and searching the guard list for each
    /// mutation, while retaining the `StateRoot` lock order used by the generic path.
    pub(crate) fn apply_market_with<T, F>(
        &self,
        revision: Revision,
        mutations: &mut [NormalizedMutation],
        on_applied: F,
    ) -> Option<T>
    where
        F: FnOnce(Vec<AppliedChange>, &[NormalizedMutation]) -> T,
    {
        let first = mutations.first()?;
        let first_root = partition_path(first).0;
        if mutations
            .iter()
            .all(|mutation| partition_path(mutation).0 == first_root)
        {
            return self.apply_single_root(revision, first_root, mutations, on_applied);
        }

        let mut has_quotes = false;
        let mut has_trading_status = false;
        let mut has_charts = false;
        let mut has_klines = false;
        let mut has_ticks = false;
        let mut has_other = false;
        for mutation in mutations.iter() {
            match partition_path(mutation).0 {
                StateRoot::Quotes => has_quotes = true,
                StateRoot::TradingStatus => has_trading_status = true,
                StateRoot::Charts => has_charts = true,
                StateRoot::Klines => has_klines = true,
                StateRoot::Ticks => has_ticks = true,
                StateRoot::Other => has_other = true,
                _ => return self.apply_with(revision, mutations, on_applied),
            }
        }

        // Keep the same total lock order as StateRoot's Ord implementation and generic path.
        let mut quotes = has_quotes.then(|| rwlock_write(&self.quotes));
        let mut trading_status = has_trading_status.then(|| rwlock_write(&self.trading_status));
        let mut charts = has_charts.then(|| rwlock_write(&self.charts));
        let mut klines = has_klines.then(|| rwlock_write(&self.klines));
        let mut ticks = has_ticks.then(|| rwlock_write(&self.ticks));
        let mut other = has_other.then(|| rwlock_write(&self.other));

        let mut applied = Vec::with_capacity(mutations.len());
        for (mutation_index, mutation) in mutations.iter_mut().enumerate() {
            let NormalizedMutation {
                path,
                object,
                fields,
                ..
            } = mutation;
            let (root, relative_path) = partition_path_segments(path.segments());
            let partition: &mut Value = match root {
                StateRoot::Quotes => &mut *quotes
                    .as_mut()
                    .expect("market quote root must have a write guard"),
                StateRoot::TradingStatus => &mut *trading_status
                    .as_mut()
                    .expect("market trading_status root must have a write guard"),
                StateRoot::Charts => &mut *charts
                    .as_mut()
                    .expect("market charts root must have a write guard"),
                StateRoot::Klines => &mut *klines
                    .as_mut()
                    .expect("market klines root must have a write guard"),
                StateRoot::Ticks => &mut *ticks
                    .as_mut()
                    .expect("market ticks root must have a write guard"),
                StateRoot::Other => &mut *other
                    .as_mut()
                    .expect("market fallback root must have a write guard"),
                _ => unreachable!("non-market root must use the generic state apply path"),
            };
            if let Some(changed) = apply_mutation_at_partition(
                root,
                partition,
                relative_path,
                mutation_index,
                object,
                fields,
            ) {
                applied.push(changed);
            }
        }

        if applied.is_empty() {
            None
        } else {
            self.revision.store(revision.get(), Ordering::SeqCst);
            Some(on_applied(applied, mutations))
        }
    }

    fn apply_single_root<T, F>(
        &self,
        revision: Revision,
        root: StateRoot,
        mutations: &mut [NormalizedMutation],
        on_applied: F,
    ) -> Option<T>
    where
        F: FnOnce(Vec<AppliedChange>, &[NormalizedMutation]) -> T,
    {
        let mut partition = rwlock_write(root.partition(self));
        let mut applied = Vec::with_capacity(mutations.len());
        for (mutation_index, mutation) in mutations.iter_mut().enumerate() {
            let NormalizedMutation {
                path,
                object,
                fields,
                ..
            } = mutation;
            let (_, relative_path) = partition_path_segments(path.segments());
            if let Some(changed) = apply_mutation_at_partition(
                root,
                &mut partition,
                relative_path,
                mutation_index,
                object,
                fields,
            ) {
                applied.push(changed);
            }
        }

        if applied.is_empty() {
            None
        } else {
            self.revision.store(revision.get(), Ordering::SeqCst);
            Some(on_applied(applied, mutations))
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
    partition_root: StateRoot,
    root: &mut Value,
    path: &[PathSegment],
    mutation_index: usize,
    object: &Option<ObjectKey>,
    fields: &mut [crate::events::FieldMutation],
) -> Option<AppliedChange> {
    let mut field_indexes = Vec::with_capacity(fields.len());
    let structural_changed = if is_partition_root_delete(partition_root, path, fields) {
        apply_partition_root_delete(root, &mut field_indexes)
    } else if is_direct_scalar_path(partition_root, path) {
        apply_direct_scalar(root, path, fields, &mut field_indexes)
    } else {
        apply_mutation_at_path(
            root,
            path,
            object.as_ref(),
            fields,
            &mut field_indexes,
            partition_root == StateRoot::Runtime,
        )
    };

    if field_indexes.is_empty() && !structural_changed {
        None
    } else {
        Some(AppliedChange::new(
            partition_root.as_str(),
            mutation_index,
            field_indexes,
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
    partition_path_segments(mutation.path.segments())
}

fn partition_path_segments(segments: &[PathSegment]) -> (StateRoot, &[PathSegment]) {
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
    fields: &mut [crate::events::FieldMutation],
    field_indexes: &mut Vec<usize>,
    prune_empty_parents: bool,
) -> bool {
    if path.is_empty() {
        return apply_fields(cursor, object, fields, field_indexes);
    }

    let segment = &path[0];
    let (mut changed, child_empty_after) = {
        let (child, structural_changed) = ensure_child_object(cursor, segment);
        let child_changed = apply_mutation_at_path(
            child,
            &path[1..],
            object,
            fields,
            field_indexes,
            prune_empty_parents,
        );
        (
            structural_changed || child_changed,
            prune_empty_parents && is_empty_partition(child),
        )
    };

    if child_empty_after
        && let Value::Object(map) = cursor
        && map.remove(segment).is_some()
    {
        changed = true;
    }

    changed
}

fn apply_fields(
    cursor: &mut Value,
    object: Option<&ObjectKey>,
    fields: &mut [crate::events::FieldMutation],
    field_indexes: &mut Vec<usize>,
) -> bool {
    let mut changed = false;
    if !cursor.is_object() {
        *cursor = Value::Object(Map::new());
        changed = true;
    }

    let Value::Object(map) = cursor else {
        unreachable!("state snapshot path targets must always resolve to objects");
    };

    for (field_index, field) in fields.iter_mut().enumerate() {
        let preserve_null = preserves_null_field(object, &field.field);
        if field.value.is_null() && !preserve_null {
            if map.remove(&field.field).is_none() {
                continue;
            }
        } else if let Some(existing) = map.get_mut(&field.field) {
            if *existing == field.value {
                continue;
            }
            *existing = std::mem::replace(&mut field.value, Value::Null);
        } else {
            let value = std::mem::replace(&mut field.value, Value::Null);
            map.insert(field.field.clone(), value);
        }

        changed = true;
        field_indexes.push(field_index);
    }

    changed
}

fn preserves_null_field(object: Option<&ObjectKey>, field: &str) -> bool {
    matches!(object, Some(ObjectKey::SessionReconnect)) && field == "max_attempts"
}

fn is_partition_root_delete(
    partition_root: StateRoot,
    path: &[PathSegment],
    fields: &[crate::events::FieldMutation],
) -> bool {
    !matches!(partition_root, StateRoot::Other)
        && path.is_empty()
        && matches!(fields, [field] if field.field == "value" && field.value.is_null())
}

fn apply_partition_root_delete(root: &mut Value, field_indexes: &mut Vec<usize>) -> bool {
    if is_empty_partition(root) {
        return false;
    }

    *root = Value::Object(Map::new());
    field_indexes.push(0);
    true
}

fn is_direct_scalar_path(partition_root: StateRoot, path: &[PathSegment]) -> bool {
    match partition_root {
        StateRoot::Other => {
            matches!(path, [field] if matches!(field.as_str(), "ins_list" | "mdhis_more_data"))
        }
        StateRoot::Trade => {
            matches!(path, [_account_id, field] if field == "trade_more_data")
        }
        _ => false,
    }
}

fn apply_direct_scalar(
    root: &mut Value,
    path: &[PathSegment],
    fields: &mut [crate::events::FieldMutation],
    field_indexes: &mut Vec<usize>,
) -> bool {
    let [parent_path @ .., segment] = path else {
        return false;
    };
    let [field] = fields else {
        return false;
    };
    if field.field != "value" {
        return false;
    }

    if field.value.is_null() {
        let mut cursor = root;
        for parent in parent_path {
            let Some(map) = cursor.as_object_mut() else {
                return false;
            };
            let Some(child) = map.get_mut(parent) else {
                return false;
            };
            cursor = child;
        }
        let Some(map) = cursor.as_object_mut() else {
            return false;
        };
        if !map.contains_key(segment) {
            return false;
        }
        map.remove(segment);
        field_indexes.push(0);
        return true;
    }

    let mut cursor = root;
    for parent in parent_path {
        let (child, _) = ensure_child_object(cursor, parent);
        cursor = child;
    }
    if !cursor.is_object() {
        *cursor = Value::Object(Map::new());
    }
    let Value::Object(map) = cursor else {
        unreachable!("direct scalar parent must be an object");
    };

    if map.get(segment) == Some(&field.value) {
        return false;
    }

    let value = std::mem::replace(&mut field.value, Value::Null);
    map.insert(segment.clone(), value);
    field_indexes.push(0);
    true
}

fn ensure_child_object<'a>(root: &'a mut Value, segment: &PathSegment) -> (&'a mut Value, bool) {
    let mut changed = false;
    if !root.is_object() {
        *root = Value::Object(Map::new());
        changed = true;
    }

    let Value::Object(map) = root else {
        unreachable!("state snapshot intermediate nodes must always be objects");
    };

    if !map.contains_key(segment) {
        map.insert(segment.clone(), Value::Object(Map::new()));
        changed = true;
    }
    let child = map
        .get_mut(segment)
        .expect("child was inserted or already present");
    if !child.is_object() {
        *child = Value::Object(Map::new());
        changed = true;
    }

    (child, changed)
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use serde_json::json;

    use super::*;
    use crate::{
        events::{FieldMutation, MutationSource, NormalizedMutation},
        ids::{ChartId, Symbol},
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

    #[test]
    fn state_store_updates_and_evicts_rolling_tick_rows() {
        let store = StateStore::new(Revision::new(0));
        let tick_path = StatePath::new(["ticks", "SHFE.au2606", "data", "7"]);
        let tick_data_path = StatePath::new(["ticks", "SHFE.au2606", "data"]);

        let insert = NormalizedMutation {
            path: tick_path.clone(),
            object: Some(ObjectKey::Tick {
                symbol: Symbol::new("SHFE.au2606"),
                tick_id: 7,
            }),
            fields: vec![FieldMutation {
                field: "last_price".to_string(),
                value: json!(610.0),
            }],
            source: MutationSource::MarketDiff,
        };
        let update = NormalizedMutation {
            path: tick_path,
            object: Some(ObjectKey::Tick {
                symbol: Symbol::new("SHFE.au2606"),
                tick_id: 7,
            }),
            fields: vec![FieldMutation {
                field: "last_price".to_string(),
                value: json!(611.0),
            }],
            source: MutationSource::MarketDiff,
        };
        let evict = NormalizedMutation {
            path: tick_data_path,
            object: None,
            fields: vec![FieldMutation {
                field: "7".to_string(),
                value: Value::Null,
            }],
            source: MutationSource::MarketDiff,
        };

        assert_eq!(store.apply(Revision::new(1), &[insert]).len(), 1);
        assert_eq!(store.apply(Revision::new(2), &[update]).len(), 1);
        assert_eq!(
            store
                .snapshot()
                .get(["ticks", "SHFE.au2606", "data", "7", "last_price"]),
            Some(&json!(611.0))
        );
        assert_eq!(store.apply(Revision::new(3), &[evict]).len(), 1);
        assert_eq!(
            store.snapshot().get(["ticks", "SHFE.au2606", "data", "7"]),
            None
        );
    }

    #[test]
    fn market_multi_root_fast_path_matches_generic_apply() {
        let generic = StateStore::new(Revision::new(0));
        let optimized = StateStore::new(Revision::new(0));
        let mut generic_mutations = multi_root_market_mutations();
        let mut optimized_mutations = generic_mutations.clone();

        let generic_applied = generic
            .apply_with(Revision::new(1), &mut generic_mutations, |applied, _| {
                applied
            })
            .expect("generic market batch should apply");
        let optimized_applied = optimized
            .apply_market_with(Revision::new(1), &mut optimized_mutations, |applied, _| {
                applied
            })
            .expect("optimized market batch should apply");

        assert_eq!(optimized_applied, generic_applied);
        assert_eq!(optimized.revision(), generic.revision());
        assert_eq!(optimized.snapshot(), generic.snapshot());
    }

    #[test]
    fn market_multi_root_fast_path_falls_back_for_non_market_partition() {
        let generic = StateStore::new(Revision::new(0));
        let optimized = StateStore::new(Revision::new(0));
        let mut generic_mutations = vec![
            NormalizedMutation {
                path: StatePath::new(["quotes", "SHFE.au2606"]),
                object: None,
                fields: vec![FieldMutation {
                    field: "last_price".to_string(),
                    value: json!(610.0),
                }],
                source: MutationSource::MarketDiff,
            },
            NormalizedMutation {
                path: StatePath::new(["trade", "TQSIM", "accounts", "CNY"]),
                object: None,
                fields: vec![FieldMutation {
                    field: "balance".to_string(),
                    value: json!(1_000_000.0),
                }],
                source: MutationSource::MarketDiff,
            },
        ];
        let mut optimized_mutations = generic_mutations.clone();

        let generic_applied = generic
            .apply_with(Revision::new(1), &mut generic_mutations, |applied, _| {
                applied
            })
            .expect("generic mixed batch should apply");
        let optimized_applied = optimized
            .apply_market_with(Revision::new(1), &mut optimized_mutations, |applied, _| {
                applied
            })
            .expect("market fast path should fall back to generic apply");

        assert_eq!(optimized_applied, generic_applied);
        assert_eq!(optimized.snapshot(), generic.snapshot());
    }

    fn multi_root_market_mutations() -> Vec<NormalizedMutation> {
        vec![
            NormalizedMutation {
                path: StatePath::new(["charts", "tick-chart"]),
                object: Some(ObjectKey::Chart {
                    chart_id: ChartId::new("tick-chart"),
                }),
                fields: vec![FieldMutation {
                    field: "right_id".to_string(),
                    value: json!(7),
                }],
                source: MutationSource::MarketDiff,
            },
            NormalizedMutation {
                path: StatePath::new(["quotes", "SHFE.au2606"]),
                object: Some(ObjectKey::Quote {
                    symbol: Symbol::new("SHFE.au2606"),
                }),
                fields: vec![FieldMutation {
                    field: "last_price".to_string(),
                    value: json!(610.0),
                }],
                source: MutationSource::MarketDiff,
            },
            NormalizedMutation {
                path: StatePath::new(["ticks", "SHFE.au2606", "data", "7"]),
                object: Some(ObjectKey::Tick {
                    symbol: Symbol::new("SHFE.au2606"),
                    tick_id: 7,
                }),
                fields: vec![FieldMutation {
                    field: "last_price".to_string(),
                    value: json!(610.0),
                }],
                source: MutationSource::MarketDiff,
            },
            NormalizedMutation {
                path: StatePath::new(["trading_status", "SHFE.au2606"]),
                object: Some(ObjectKey::TradingStatus {
                    symbol: Symbol::new("SHFE.au2606"),
                }),
                fields: vec![FieldMutation {
                    field: "tradeable".to_string(),
                    value: json!(true),
                }],
                source: MutationSource::MarketDiff,
            },
            NormalizedMutation {
                path: StatePath::new(["symbols", "SHFE.au2606"]),
                object: None,
                fields: vec![FieldMutation {
                    field: "instrument_name".to_string(),
                    value: json!("gold"),
                }],
                source: MutationSource::MarketDiff,
            },
        ]
    }
}
