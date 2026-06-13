# Relay Symbol Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an embedded `tqsdk-relay` dashboard that shows per-symbol data receipt status, receive lag, market-time lag, and downstream subscription markers without changing SDK default behavior.

**Architecture:** Add a relay-local `symbol_metrics` module that records O(1) per-symbol telemetry during `RelayEngine::ingest_tick`, stores the upstream universe when it is known, and builds filtered snapshots only on HTTP requests. Extend the existing metrics HTTP server with `/symbol-metrics`, `/dashboard`, and `/dashboard/app.js`; keep `/health` and `/metrics` unchanged.

**Tech Stack:** Rust 2024, Tokio, serde/serde_json, existing `tqsdk-relay` HTTP/WebSocket test helpers, no new frontend build step, no new runtime dependency.

---

## File Structure

- Create `crates/tqsdk-relay/src/symbol_metrics.rs`
  - Owns `SymbolTelemetryStore`, `SymbolTelemetry`, `SymbolStatus`, snapshot/query types, p95 calculation, filtering, sorting, and limit behavior.
  - Does not depend on HTTP or WebSocket transport.
- Modify `crates/tqsdk-relay/src/lib.rs`
  - Exports the new symbol metrics types needed by tests and future library users.
- Modify `crates/tqsdk-relay/src/interest.rs`
  - Adds a focused helper for per-symbol quote/chart subscription counts.
- Modify `crates/tqsdk-relay/src/engine.rs`
  - Adds `SymbolTelemetryStore`, records ticks in O(1), records upstream universe, and exposes `symbol_metrics_snapshot_at(...)`.
- Modify `crates/tqsdk-relay/src/runtime.rs`
  - Passes actual upstream chart symbols into engine universe recording.
- Modify `crates/tqsdk-relay/src/metrics_http.rs`
  - Adds `/symbol-metrics`, `/dashboard`, `/dashboard/app.js`, query parsing, 400 handling, and text responses.
- Create `crates/tqsdk-relay/src/dashboard.rs`
  - Contains static HTML and JS constants for the embedded dashboard.
- Modify `crates/tqsdk-relay/tests/observability.rs`
  - Covers engine-level symbol status and subscription aggregation.
- Create `crates/tqsdk-relay/tests/symbol_metrics.rs`
  - Covers pure telemetry snapshot logic, filtering, sorting, limits, and p95.
- Modify `crates/tqsdk-relay/tests/binary_smoke.rs`
  - Covers `/dashboard`, `/dashboard/app.js`, and `/symbol-metrics`.
- Modify `crates/tqsdk-relay/README.md`
  - Documents the dashboard endpoints and performance boundary.
- Modify `docs/architecture/validation.md`
  - Adds the dashboard verification commands if the file already lists relay checks.

---

### Task 1: Add Pure Symbol Telemetry Model

**Files:**
- Create: `crates/tqsdk-relay/src/symbol_metrics.rs`
- Modify: `crates/tqsdk-relay/src/lib.rs`
- Test: `crates/tqsdk-relay/tests/symbol_metrics.rs`

- [ ] **Step 1: Write failing pure telemetry tests**

Create `crates/tqsdk-relay/tests/symbol_metrics.rs`:

```rust
use tqsdk_relay::{
    RelayTickRow, SymbolMetricsQuery, SymbolSort, SymbolStatus, SymbolTelemetryStore,
};

fn tick(id: i64, datetime_ns: i64, price: f64) -> RelayTickRow {
    RelayTickRow {
        id,
        datetime: datetime_ns,
        last_price: price,
        volume: id * 10,
        open_interest: 100 + id,
    }
}

#[test]
fn universe_symbol_without_tick_is_missing() {
    let mut store = SymbolTelemetryStore::default();
    store.record_universe(["SHFE.au2602"], 1_700_000_000_000);

    let snapshot = store.snapshot_at(
        1_700_000_010_000,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );

    assert_eq!(snapshot.summary.total, 1);
    assert_eq!(snapshot.summary.missing, 1);
    assert_eq!(snapshot.symbols[0].symbol, "SHFE.au2602");
    assert_eq!(snapshot.symbols[0].status, SymbolStatus::Missing);
    assert!(snapshot.symbols[0].receive_gap_ms.is_none());
}

#[test]
fn ticked_symbol_transitions_from_live_to_stale() {
    let mut store = SymbolTelemetryStore::default();
    store.record_universe(["SHFE.au2602"], 1_700_000_000_000);
    store.record_tick_at(
        "SHFE.au2602",
        &tick(1, 1_700_000_001_000_000_000, 610.0),
        1_700_000_001_200,
    );

    let live = store.snapshot_at(
        1_700_000_002_000,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    assert_eq!(live.symbols[0].status, SymbolStatus::Live);
    assert_eq!(live.symbols[0].receive_gap_ms, Some(800));
    assert_eq!(live.symbols[0].market_time_lag_ms, Some(1_000));

    let stale = store.snapshot_at(
        1_700_000_032_201,
        30_000,
        &Default::default(),
        &SymbolMetricsQuery::default(),
    );
    assert_eq!(stale.symbols[0].status, SymbolStatus::Stale);
    assert_eq!(stale.summary.stale, 1);
}

#[test]
fn subscribed_symbol_outside_universe_is_inactive() {
    let store = SymbolTelemetryStore::default();
    let mut subscriptions = Default::default();
    subscriptions.insert(
        "DCE.m2609".to_string(),
        tqsdk_relay::SymbolSubscriptionCounts {
            quote_subscriber_count: 1,
            chart_subscriber_count: 0,
        },
    );

    let snapshot = store.snapshot_at(
        1_700_000_010_000,
        30_000,
        &subscriptions,
        &SymbolMetricsQuery::default(),
    );

    assert_eq!(snapshot.summary.total, 1);
    assert_eq!(snapshot.summary.inactive, 1);
    assert_eq!(snapshot.summary.subscribed, 1);
    assert_eq!(snapshot.symbols[0].status, SymbolStatus::Inactive);
    assert!(snapshot.symbols[0].subscribed);
}

#[test]
fn snapshot_filters_sorts_limits_and_computes_p95_receive_gap() {
    let mut store = SymbolTelemetryStore::default();
    store.record_universe(["SHFE.au2602", "DCE.m2609", "CZCE.AP610"], 1_700_000_000_000);
    store.record_tick_at(
        "SHFE.au2602",
        &tick(1, 1_700_000_001_000_000_000, 610.0),
        1_700_000_001_000,
    );
    store.record_tick_at(
        "DCE.m2609",
        &tick(2, 1_700_000_000_500_000_000, 3100.0),
        1_700_000_000_500,
    );

    let query = SymbolMetricsQuery {
        statuses: vec![SymbolStatus::Live, SymbolStatus::Stale],
        subscribed_only: false,
        q: Some("260".to_string()),
        sort: SymbolSort::ReceiveGapDesc,
        limit: Some(1),
    };
    let snapshot = store.snapshot_at(
        1_700_000_002_000,
        30_000,
        &Default::default(),
        &query,
    );

    assert_eq!(snapshot.summary.total, 3);
    assert_eq!(snapshot.summary.live, 2);
    assert_eq!(snapshot.summary.missing, 1);
    assert_eq!(snapshot.summary.p95_receive_gap_ms, Some(2_000));
    assert_eq!(snapshot.symbols.len(), 1);
    assert_eq!(snapshot.symbols[0].symbol, "DCE.m2609");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p tqsdk-relay --test symbol_metrics
```

Expected: compile failure because `symbol_metrics` types are not defined or exported.

- [ ] **Step 3: Add symbol metrics module**

Create `crates/tqsdk-relay/src/symbol_metrics.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::protocol::RelayTickRow;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolStatus {
    Live,
    Stale,
    Missing,
    Inactive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SymbolSort {
    SymbolAsc,
    StatusAsc,
    ReceiveGapDesc,
    MarketTimeLagDesc,
    TicksIngestedDesc,
}

impl Default for SymbolSort {
    fn default() -> Self {
        Self::SymbolAsc
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolMetricsQuery {
    pub statuses: Vec<SymbolStatus>,
    pub subscribed_only: bool,
    pub q: Option<String>,
    pub sort: SymbolSort,
    pub limit: Option<usize>,
}

impl Default for SymbolMetricsQuery {
    fn default() -> Self {
        Self {
            statuses: Vec::new(),
            subscribed_only: false,
            q: None,
            sort: SymbolSort::default(),
            limit: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SymbolSubscriptionCounts {
    pub quote_subscriber_count: usize,
    pub chart_subscriber_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SymbolTelemetry {
    ticks_ingested: u64,
    last_receive_unix_millis: Option<u64>,
    last_tick_datetime_ns: Option<i64>,
    last_price: Option<f64>,
    last_volume: Option<i64>,
    last_open_interest: Option<i64>,
    invalid_rows: u64,
    last_invalid_row_error: Option<String>,
}

impl Default for SymbolTelemetry {
    fn default() -> Self {
        Self {
            ticks_ingested: 0,
            last_receive_unix_millis: None,
            last_tick_datetime_ns: None,
            last_price: None,
            last_volume: None,
            last_open_interest: None,
            invalid_rows: 0,
            last_invalid_row_error: None,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct SymbolTelemetryStore {
    universe: BTreeSet<String>,
    telemetry: BTreeMap<String, SymbolTelemetry>,
    last_universe_refresh_unix_millis: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SymbolMetricsSnapshot {
    pub now_unix_millis: u64,
    pub data_stale_after_millis: u64,
    pub summary: SymbolMetricsSummary,
    pub symbols: Vec<SymbolTelemetrySnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SymbolMetricsSummary {
    pub total: usize,
    pub live: usize,
    pub stale: usize,
    pub missing: usize,
    pub inactive: usize,
    pub subscribed: usize,
    pub p95_receive_gap_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SymbolTelemetrySnapshot {
    pub symbol: String,
    pub status: SymbolStatus,
    pub in_universe: bool,
    pub subscribed: bool,
    pub quote_subscriber_count: usize,
    pub chart_subscriber_count: usize,
    pub ticks_ingested: u64,
    pub receive_gap_ms: Option<u64>,
    pub market_time_lag_ms: Option<u64>,
    pub last_receive_unix_millis: Option<u64>,
    pub last_tick_datetime_ns: Option<i64>,
    pub last_price: Option<f64>,
    pub last_volume: Option<i64>,
    pub last_open_interest: Option<i64>,
    pub invalid_rows: u64,
    pub last_invalid_row_error: Option<String>,
}

impl SymbolTelemetryStore {
    pub fn record_universe<I, S>(&mut self, symbols: I, unix_millis: u64)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.universe = symbols
            .into_iter()
            .map(|symbol| symbol.as_ref().trim().to_string())
            .filter(|symbol| !symbol.is_empty())
            .collect();
        self.last_universe_refresh_unix_millis = Some(unix_millis);
    }

    pub fn record_tick_at(&mut self, symbol: &str, row: &RelayTickRow, receive_unix_millis: u64) {
        let telemetry = self.telemetry.entry(symbol.to_string()).or_default();
        telemetry.ticks_ingested = telemetry.ticks_ingested.saturating_add(1);
        telemetry.last_receive_unix_millis = Some(receive_unix_millis);
        telemetry.last_tick_datetime_ns = Some(row.datetime);
        telemetry.last_price = Some(row.last_price);
        telemetry.last_volume = Some(row.volume);
        telemetry.last_open_interest = Some(row.open_interest);
    }

    pub fn record_invalid_row(&mut self, symbol: &str, message: impl Into<String>) {
        let telemetry = self.telemetry.entry(symbol.to_string()).or_default();
        telemetry.invalid_rows = telemetry.invalid_rows.saturating_add(1);
        telemetry.last_invalid_row_error = Some(message.into());
    }

    pub fn snapshot_at(
        &self,
        now_unix_millis: u64,
        stale_after_millis: u64,
        subscriptions: &BTreeMap<String, SymbolSubscriptionCounts>,
        query: &SymbolMetricsQuery,
    ) -> SymbolMetricsSnapshot {
        let mut symbols = BTreeSet::new();
        symbols.extend(self.universe.iter().cloned());
        symbols.extend(self.telemetry.keys().cloned());
        symbols.extend(subscriptions.keys().cloned());

        let mut unfiltered = Vec::new();
        for symbol in symbols {
            let in_universe = self.universe.contains(&symbol);
            let telemetry = self.telemetry.get(&symbol);
            let subscriptions = subscriptions.get(&symbol).copied().unwrap_or_default();
            let subscribed =
                subscriptions.quote_subscriber_count > 0 || subscriptions.chart_subscriber_count > 0;
            let receive_gap_ms = telemetry
                .and_then(|telemetry| telemetry.last_receive_unix_millis)
                .map(|last_receive| now_unix_millis.saturating_sub(last_receive));
            let market_time_lag_ms = telemetry
                .and_then(|telemetry| telemetry.last_tick_datetime_ns)
                .and_then(tick_datetime_ns_to_unix_millis)
                .map(|tick_millis| now_unix_millis.saturating_sub(tick_millis));
            let status = classify_symbol(in_universe, receive_gap_ms, stale_after_millis);
            let telemetry = telemetry.cloned().unwrap_or_default();
            unfiltered.push(SymbolTelemetrySnapshot {
                symbol,
                status,
                in_universe,
                subscribed,
                quote_subscriber_count: subscriptions.quote_subscriber_count,
                chart_subscriber_count: subscriptions.chart_subscriber_count,
                ticks_ingested: telemetry.ticks_ingested,
                receive_gap_ms,
                market_time_lag_ms,
                last_receive_unix_millis: telemetry.last_receive_unix_millis,
                last_tick_datetime_ns: telemetry.last_tick_datetime_ns,
                last_price: telemetry.last_price,
                last_volume: telemetry.last_volume,
                last_open_interest: telemetry.last_open_interest,
                invalid_rows: telemetry.invalid_rows,
                last_invalid_row_error: telemetry.last_invalid_row_error,
            });
        }

        let summary = summarize(&unfiltered);
        let mut symbols = unfiltered
            .into_iter()
            .filter(|symbol| query.statuses.is_empty() || query.statuses.contains(&symbol.status))
            .filter(|symbol| !query.subscribed_only || symbol.subscribed)
            .filter(|symbol| {
                query
                    .q
                    .as_ref()
                    .is_none_or(|needle| symbol.symbol.contains(needle))
            })
            .collect::<Vec<_>>();
        sort_symbols(&mut symbols, query.sort);
        if let Some(limit) = query.limit {
            symbols.truncate(limit);
        }

        SymbolMetricsSnapshot {
            now_unix_millis,
            data_stale_after_millis: stale_after_millis,
            summary,
            symbols,
        }
    }
}

fn classify_symbol(
    in_universe: bool,
    receive_gap_ms: Option<u64>,
    stale_after_millis: u64,
) -> SymbolStatus {
    match receive_gap_ms {
        Some(gap) if gap <= stale_after_millis => SymbolStatus::Live,
        Some(_) => SymbolStatus::Stale,
        None if in_universe => SymbolStatus::Missing,
        None => SymbolStatus::Inactive,
    }
}

fn summarize(symbols: &[SymbolTelemetrySnapshot]) -> SymbolMetricsSummary {
    let mut receive_gaps = Vec::new();
    let mut summary = SymbolMetricsSummary {
        total: symbols.len(),
        live: 0,
        stale: 0,
        missing: 0,
        inactive: 0,
        subscribed: 0,
        p95_receive_gap_ms: None,
    };
    for symbol in symbols {
        match symbol.status {
            SymbolStatus::Live => summary.live += 1,
            SymbolStatus::Stale => summary.stale += 1,
            SymbolStatus::Missing => summary.missing += 1,
            SymbolStatus::Inactive => summary.inactive += 1,
        }
        if symbol.subscribed {
            summary.subscribed += 1;
        }
        if let Some(gap) = symbol.receive_gap_ms {
            receive_gaps.push(gap);
        }
    }
    summary.p95_receive_gap_ms = percentile_95(receive_gaps);
    summary
}

fn sort_symbols(symbols: &mut [SymbolTelemetrySnapshot], sort: SymbolSort) {
    match sort {
        SymbolSort::SymbolAsc => symbols.sort_by(|left, right| left.symbol.cmp(&right.symbol)),
        SymbolSort::StatusAsc => {
            symbols.sort_by(|left, right| (left.status, &left.symbol).cmp(&(right.status, &right.symbol)));
        }
        SymbolSort::ReceiveGapDesc => symbols.sort_by(|left, right| {
            right
                .receive_gap_ms
                .cmp(&left.receive_gap_ms)
                .then_with(|| left.symbol.cmp(&right.symbol))
        }),
        SymbolSort::MarketTimeLagDesc => symbols.sort_by(|left, right| {
            right
                .market_time_lag_ms
                .cmp(&left.market_time_lag_ms)
                .then_with(|| left.symbol.cmp(&right.symbol))
        }),
        SymbolSort::TicksIngestedDesc => symbols.sort_by(|left, right| {
            right
                .ticks_ingested
                .cmp(&left.ticks_ingested)
                .then_with(|| left.symbol.cmp(&right.symbol))
        }),
    }
}

fn percentile_95(mut values: Vec<u64>) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let index = ((values.len() - 1) * 95).div_ceil(100);
    values.get(index).copied()
}

fn tick_datetime_ns_to_unix_millis(datetime_ns: i64) -> Option<u64> {
    u64::try_from(datetime_ns).ok().map(|value| value / 1_000_000)
}
```

- [ ] **Step 4: Export the module and public types**

Modify `crates/tqsdk-relay/src/lib.rs`:

```rust
pub mod symbol_metrics;

pub use symbol_metrics::{
    SymbolMetricsQuery, SymbolMetricsSnapshot, SymbolMetricsSummary, SymbolSort, SymbolStatus,
    SymbolSubscriptionCounts, SymbolTelemetrySnapshot, SymbolTelemetryStore,
};
```

- [ ] **Step 5: Run pure telemetry tests**

Run:

```bash
cargo test -p tqsdk-relay --test symbol_metrics
```

Expected: all `symbol_metrics` tests pass.

- [ ] **Step 6: Commit Task 1**

```bash
git add crates/tqsdk-relay/src/lib.rs crates/tqsdk-relay/src/symbol_metrics.rs crates/tqsdk-relay/tests/symbol_metrics.rs
git commit -m "feat(relay): add symbol telemetry snapshots"
```

---

### Task 2: Wire Symbol Telemetry Into RelayEngine

**Files:**
- Modify: `crates/tqsdk-relay/src/interest.rs`
- Modify: `crates/tqsdk-relay/src/engine.rs`
- Modify: `crates/tqsdk-relay/src/runtime.rs`
- Test: `crates/tqsdk-relay/tests/observability.rs`

- [ ] **Step 1: Write failing engine tests**

Append these tests to `crates/tqsdk-relay/tests/observability.rs`:

```rust
#[test]
fn engine_symbol_metrics_include_universe_missing_live_and_stale_states() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    engine.record_universe_refresh_success_for_symbols(
        ["SHFE.au2602", "DCE.m2609"],
        21,
        Some(32_000),
        None,
        1_700_000_000,
    );
    engine.ingest_tick_at_for_test("SHFE.au2602", tick(1), 1_700_000_001_000);

    let live = engine.symbol_metrics_snapshot_at(
        1_700_000_002_000,
        &tqsdk_relay::SymbolMetricsQuery::default(),
    );
    assert_eq!(live.summary.total, 2);
    assert_eq!(live.summary.live, 1);
    assert_eq!(live.summary.missing, 1);

    let stale = engine.symbol_metrics_snapshot_at(
        1_700_000_032_001,
        &tqsdk_relay::SymbolMetricsQuery::default(),
    );
    let au = stale
        .symbols
        .iter()
        .find(|symbol| symbol.symbol == "SHFE.au2602")
        .unwrap();
    assert_eq!(au.status, tqsdk_relay::SymbolStatus::Stale);
}

#[test]
fn engine_symbol_metrics_include_quote_and_chart_subscriptions() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    engine
        .handle_command(
            ClientId::new(1),
            DownstreamCommand::SubscribeQuote {
                symbols: vec!["SHFE.au2602".to_string()],
            },
        )
        .unwrap();
    engine
        .handle_command(ClientId::new(2), chart_command("chart-2"))
        .unwrap();

    let snapshot = engine.symbol_metrics_snapshot_at(
        1_700_000_002_000,
        &tqsdk_relay::SymbolMetricsQuery::default(),
    );
    let symbol = snapshot
        .symbols
        .iter()
        .find(|symbol| symbol.symbol == "SHFE.au2602")
        .unwrap();

    assert_eq!(symbol.status, tqsdk_relay::SymbolStatus::Inactive);
    assert!(symbol.subscribed);
    assert_eq!(symbol.quote_subscriber_count, 1);
    assert_eq!(symbol.chart_subscriber_count, 1);

    engine.remove_client(ClientId::new(1));
    let after_remove = engine.symbol_metrics_snapshot_at(
        1_700_000_002_000,
        &tqsdk_relay::SymbolMetricsQuery::default(),
    );
    let symbol = after_remove
        .symbols
        .iter()
        .find(|symbol| symbol.symbol == "SHFE.au2602")
        .unwrap();
    assert_eq!(symbol.quote_subscriber_count, 0);
    assert_eq!(symbol.chart_subscriber_count, 1);
}
```

- [ ] **Step 2: Run observability tests to verify failure**

Run:

```bash
cargo test -p tqsdk-relay --test observability engine_symbol_metrics
```

Expected: compile failure because engine methods are not implemented.

- [ ] **Step 3: Add subscription count helper**

Modify `crates/tqsdk-relay/src/interest.rs`:

```rust
use crate::symbol_metrics::SymbolSubscriptionCounts;

impl InterestRegistry {
    #[must_use]
    pub fn symbol_subscription_counts(&self) -> BTreeMap<String, SymbolSubscriptionCounts> {
        let mut counts = BTreeMap::new();
        for symbols in self.client_quotes.values() {
            for symbol in symbols {
                counts
                    .entry(symbol.clone())
                    .or_insert_with(SymbolSubscriptionCounts::default)
                    .quote_subscriber_count += 1;
            }
        }
        for source in self.chart_mappings.values() {
            for symbol in &source.symbols {
                counts
                    .entry(symbol.clone())
                    .or_insert_with(SymbolSubscriptionCounts::default)
                    .chart_subscriber_count += 1;
            }
        }
        counts
    }
}
```

- [ ] **Step 4: Wire telemetry into `RelayEngine`**

Modify `crates/tqsdk-relay/src/engine.rs` imports and struct:

```rust
use crate::symbol_metrics::{
    SymbolMetricsQuery, SymbolMetricsSnapshot, SymbolTelemetryStore,
};

pub struct RelayEngine {
    cache: MarketCache,
    interests: InterestRegistry,
    bootstrap: BootstrapQueue,
    klines: HashMap<SourceKey, KlineSynthesis>,
    symbol_metrics: SymbolTelemetryStore,
    upstream_status: RelaySourceStatus,
    // existing fields remain unchanged
}
```

Initialize it in `new_memory_only`:

```rust
symbol_metrics: SymbolTelemetryStore::default(),
```

Add these methods:

```rust
pub fn ingest_tick_at_for_test(
    &mut self,
    symbol: impl AsRef<str>,
    row: RelayTickRow,
    receive_unix_millis: u64,
) -> RelayResult<Vec<DownstreamFrame>> {
    self.ingest_tick_at(symbol, row, receive_unix_millis)
}

fn ingest_tick_at(
    &mut self,
    symbol: impl AsRef<str>,
    row: RelayTickRow,
    receive_unix_millis: u64,
) -> RelayResult<Vec<DownstreamFrame>> {
    let symbol = symbol.as_ref();
    self.ticks_ingested = self.ticks_ingested.saturating_add(1);
    self.upstream_status = RelaySourceStatus::Up;
    self.record_data_activity_at(receive_unix_millis / 1_000);
    self.symbol_metrics.record_tick_at(symbol, &row, receive_unix_millis);
    self.cache.push_tick(symbol, row.clone());
    let mut frames = self.quote_frames(symbol);
    frames.extend(self.kline_frames(symbol, row)?);
    Ok(frames)
}

pub fn record_universe_refresh_success_for_symbols<I, S>(
    &mut self,
    symbols: I,
    upstream_ins_list_chars: usize,
    warn_chars: Option<usize>,
    max_chars: Option<usize>,
    unix_secs: u64,
)
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let symbols: Vec<String> = symbols
        .into_iter()
        .map(|symbol| symbol.as_ref().to_string())
        .collect();
    self.record_universe_refresh_success(
        symbols.len(),
        upstream_ins_list_chars,
        warn_chars,
        max_chars,
        unix_secs,
    );
    self.symbol_metrics
        .record_universe(symbols, unix_secs.saturating_mul(1_000));
}

#[must_use]
pub fn symbol_metrics_snapshot_at(
    &self,
    now_unix_millis: u64,
    query: &SymbolMetricsQuery,
) -> SymbolMetricsSnapshot {
    self.symbol_metrics.snapshot_at(
        now_unix_millis,
        DEFAULT_DATA_STALE_AFTER_SECS.saturating_mul(1_000),
        &self.interests.symbol_subscription_counts(),
        query,
    )
}

#[must_use]
pub fn symbol_metrics_snapshot(&self, query: &SymbolMetricsQuery) -> SymbolMetricsSnapshot {
    self.symbol_metrics_snapshot_at(current_unix_millis(), query)
}
```

Change current `ingest_tick` to delegate:

```rust
pub fn ingest_tick(
    &mut self,
    symbol: impl AsRef<str>,
    row: RelayTickRow,
) -> RelayResult<Vec<DownstreamFrame>> {
    self.ingest_tick_at(symbol, row, current_unix_millis())
}
```

Add `current_unix_millis` near `current_unix_secs`:

```rust
fn current_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}
```

- [ ] **Step 5: Record chart symbols during runtime universe refresh**

Modify `crates/tqsdk-relay/src/runtime.rs` helper `record_universe_refresh_success` to take chart symbols:

```rust
fn record_universe_refresh_success(
    server: &RelayServer,
    config: &RelayConfig,
    symbols: &[String],
    upstream_ins_list_chars: usize,
) {
    let engine = server.engine();
    match engine.lock() {
        Ok(mut engine) => engine.record_universe_refresh_success_for_symbols(
            symbols.iter().map(String::as_str),
            upstream_ins_list_chars,
            config.upstream_ins_list_limits.warn_chars,
            config.upstream_ins_list_limits.max_chars,
            current_unix_secs(),
        ),
        Err(_) => eprintln!("relay internal error: relay engine lock poisoned"),
    }
}
```

Update the call site inside `connect_configured_upstream_for_pump`:

```rust
record_universe_refresh_success(
    server,
    config,
    chart.symbols(),
    chart.ins_list_chars(),
);
```

- [ ] **Step 6: Run engine tests**

Run:

```bash
cargo test -p tqsdk-relay --test observability engine_symbol_metrics
```

Expected: the new observability tests pass.

- [ ] **Step 7: Commit Task 2**

```bash
git add crates/tqsdk-relay/src/engine.rs crates/tqsdk-relay/src/interest.rs crates/tqsdk-relay/src/runtime.rs crates/tqsdk-relay/tests/observability.rs
git commit -m "feat(relay): wire symbol metrics into engine"
```

---

### Task 3: Add Symbol Metrics HTTP API

**Files:**
- Modify: `crates/tqsdk-relay/src/symbol_metrics.rs`
- Modify: `crates/tqsdk-relay/src/metrics_http.rs`
- Test: `crates/tqsdk-relay/tests/binary_smoke.rs`

- [ ] **Step 1: Write failing HTTP smoke test**

Append to `relay_binary_serves_health_and_metrics_json` in `crates/tqsdk-relay/tests/binary_smoke.rs`:

```rust
let symbol_metrics = wait_for_http_json(metrics_addr, "/symbol-metrics", &mut child);
assert_eq!(symbol_metrics["now_unix_millis"].is_number(), true);
assert_eq!(symbol_metrics["data_stale_after_millis"], 30_000);
assert_eq!(symbol_metrics["summary"]["total"], 0);
assert_eq!(symbol_metrics["symbols"].as_array().unwrap().len(), 0);
```

Add a new test below it:

```rust
#[test]
fn relay_binary_rejects_invalid_symbol_metrics_query() {
    let downstream_addr = free_loopback_addr();
    let metrics_addr = free_loopback_addr();
    let mut child = ChildGuard::spawn(
        Command::new(env!("CARGO_BIN_EXE_tqsdk-relay"))
            .env("TQSDK_RELAY_DOWNSTREAM_LISTEN", downstream_addr.to_string())
            .env("TQSDK_RELAY_METRICS_LISTEN", metrics_addr.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    );

    let response = wait_for_http_response(metrics_addr, "/symbol-metrics?sort=bad", &mut child);
    assert!(response.starts_with("HTTP/1.1 400"));
    let (_, body) = response.split_once("\r\n\r\n").unwrap();
    let error: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(error["error"], "invalid sort");
}
```

Add helper:

```rust
fn wait_for_http_response(addr: SocketAddr, path: &str, child: &mut ChildGuard) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait() {
            panic!("relay binary exited before opening metrics listener: {status}");
        }
        if let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(100)) {
            let request = format!(
                "GET {path} HTTP/1.1\r\n\
Host: {addr}\r\n\
Connection: close\r\n\
\r\n"
            );
            stream.write_all(request.as_bytes()).unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).unwrap();
            return String::from_utf8(response).unwrap();
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for relay metrics listener at {addr}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}
```

- [ ] **Step 2: Run binary smoke tests to verify failure**

Run:

```bash
cargo test -p tqsdk-relay --test binary_smoke relay_binary_serves_health_and_metrics_json relay_binary_rejects_invalid_symbol_metrics_query
```

Expected: `/symbol-metrics` returns 404 or invalid query test fails.

- [ ] **Step 3: Add query parser**

Add to `crates/tqsdk-relay/src/symbol_metrics.rs`:

```rust
impl SymbolMetricsQuery {
    pub fn from_query_string(query: &str) -> Result<Self, &'static str> {
        let mut parsed = Self::default();
        if query.is_empty() {
            return Ok(parsed);
        }
        for pair in query.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            match key {
                "status" => parsed.statuses = parse_statuses(value)?,
                "subscribed" => parsed.subscribed_only = parse_bool(value)?,
                "q" => {
                    if !value.is_empty() {
                        parsed.q = Some(value.to_string());
                    }
                }
                "sort" => parsed.sort = parse_sort(value)?,
                "limit" => parsed.limit = Some(parse_limit(value)?),
                _ => return Err("unknown query parameter"),
            }
        }
        Ok(parsed)
    }
}

fn parse_statuses(value: &str) -> Result<Vec<SymbolStatus>, &'static str> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value.split(',').map(parse_status).collect()
}

fn parse_status(value: &str) -> Result<SymbolStatus, &'static str> {
    match value {
        "live" => Ok(SymbolStatus::Live),
        "stale" => Ok(SymbolStatus::Stale),
        "missing" => Ok(SymbolStatus::Missing),
        "inactive" => Ok(SymbolStatus::Inactive),
        _ => Err("invalid status"),
    }
}

fn parse_bool(value: &str) -> Result<bool, &'static str> {
    match value {
        "1" | "true" => Ok(true),
        "0" | "false" => Ok(false),
        _ => Err("invalid subscribed"),
    }
}

fn parse_sort(value: &str) -> Result<SymbolSort, &'static str> {
    match value {
        "" | "symbol_asc" => Ok(SymbolSort::SymbolAsc),
        "status_asc" => Ok(SymbolSort::StatusAsc),
        "receive_gap_ms_desc" => Ok(SymbolSort::ReceiveGapDesc),
        "market_time_lag_ms_desc" => Ok(SymbolSort::MarketTimeLagDesc),
        "ticks_ingested_desc" => Ok(SymbolSort::TicksIngestedDesc),
        _ => Err("invalid sort"),
    }
}

fn parse_limit(value: &str) -> Result<usize, &'static str> {
    value
        .parse::<usize>()
        .ok()
        .filter(|limit| *limit > 0)
        .ok_or("invalid limit")
}
```

- [ ] **Step 4: Add `/symbol-metrics` route**

Modify `crates/tqsdk-relay/src/metrics_http.rs` imports:

```rust
use crate::symbol_metrics::SymbolMetricsQuery;
```

Replace `request_path` with a split result:

```rust
struct RequestTarget<'a> {
    path: &'a str,
    query: &'a str,
}

fn request_target(request: &str) -> RelayResult<RequestTarget<'_>> {
    let first = request
        .lines()
        .next()
        .ok_or_else(|| RelayError::invalid_protocol("metrics HTTP request missing request line"))?;
    let mut parts = first.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| RelayError::invalid_protocol("metrics HTTP request missing method"))?;
    let target = parts
        .next()
        .ok_or_else(|| RelayError::invalid_protocol("metrics HTTP request missing path"))?;
    if method != "GET" {
        return Err(RelayError::invalid_protocol(
            "metrics HTTP server only accepts GET",
        ));
    }
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    Ok(RequestTarget { path, query })
}
```

Change route handling:

```rust
let target = request_target(&request)?;
let response = match target.path {
    "/health" => { /* existing health block */ }
    "/metrics" => { /* existing metrics block */ }
    "/symbol-metrics" => {
        let query = match SymbolMetricsQuery::from_query_string(target.query) {
            Ok(query) => query,
            Err(error) => {
                write_response(stream, 400, json!({ "error": error })).await?;
                return Ok(());
            }
        };
        let snapshot = engine
            .lock()
            .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?
            .symbol_metrics_snapshot(&query);
        serde_json::to_value(snapshot).map_err(|err| {
            RelayError::Internal(format!("symbol metrics JSON encode failed: {err}"))
        })?
    }
    _ => {
        write_response(stream, 404, json!({"error": "not found"})).await?;
        return Ok(());
    }
};
```

Update response reason helper:

```rust
fn status_reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "OK",
    }
}
```

Use it inside `write_response`:

```rust
let reason = status_reason(status);
```

- [ ] **Step 5: Run HTTP tests**

Run:

```bash
cargo test -p tqsdk-relay --test binary_smoke relay_binary_serves_health_and_metrics_json relay_binary_rejects_invalid_symbol_metrics_query
```

Expected: both tests pass.

- [ ] **Step 6: Commit Task 3**

```bash
git add crates/tqsdk-relay/src/symbol_metrics.rs crates/tqsdk-relay/src/metrics_http.rs crates/tqsdk-relay/tests/binary_smoke.rs
git commit -m "feat(relay): expose symbol metrics endpoint"
```

---

### Task 4: Add Embedded Dashboard Static Page

**Files:**
- Create: `crates/tqsdk-relay/src/dashboard.rs`
- Modify: `crates/tqsdk-relay/src/lib.rs`
- Modify: `crates/tqsdk-relay/src/metrics_http.rs`
- Test: `crates/tqsdk-relay/tests/binary_smoke.rs`

- [ ] **Step 1: Write failing dashboard smoke test**

Append to `crates/tqsdk-relay/tests/binary_smoke.rs`:

```rust
#[test]
fn relay_binary_serves_embedded_dashboard_assets() {
    let downstream_addr = free_loopback_addr();
    let metrics_addr = free_loopback_addr();
    let mut child = ChildGuard::spawn(
        Command::new(env!("CARGO_BIN_EXE_tqsdk-relay"))
            .env("TQSDK_RELAY_DOWNSTREAM_LISTEN", downstream_addr.to_string())
            .env("TQSDK_RELAY_METRICS_LISTEN", metrics_addr.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    );

    let html = wait_for_http_response(metrics_addr, "/dashboard", &mut child);
    assert!(html.starts_with("HTTP/1.1 200"));
    assert!(html.contains("Relay Symbol Dashboard"));
    assert!(html.contains("/dashboard/app.js"));

    let js = wait_for_http_response(metrics_addr, "/dashboard/app.js", &mut child);
    assert!(js.starts_with("HTTP/1.1 200"));
    assert!(js.contains("/symbol-metrics"));
}
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
cargo test -p tqsdk-relay --test binary_smoke relay_binary_serves_embedded_dashboard_assets
```

Expected: `/dashboard` returns 404.

- [ ] **Step 3: Add static dashboard module**

Create `crates/tqsdk-relay/src/dashboard.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

pub const DASHBOARD_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Relay Symbol Dashboard</title>
  <style>
    body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #172033; background: #f6f7f9; }
    header { padding: 16px 20px; background: #111827; color: white; }
    main { padding: 16px 20px; }
    .summary { display: grid; grid-template-columns: repeat(6, minmax(120px, 1fr)); gap: 10px; margin-bottom: 14px; }
    .tile { background: white; border: 1px solid #d9dde5; border-radius: 6px; padding: 10px; }
    .label { color: #667085; font-size: 12px; text-transform: uppercase; }
    .value { font-size: 22px; font-weight: 700; margin-top: 4px; }
    .toolbar { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 12px; }
    input, select, button { border: 1px solid #cbd2df; border-radius: 6px; padding: 8px 10px; background: white; }
    table { width: 100%; border-collapse: collapse; background: white; border: 1px solid #d9dde5; }
    th, td { padding: 8px 10px; border-bottom: 1px solid #edf0f5; text-align: left; font-size: 13px; }
    th { background: #f1f3f7; color: #475467; position: sticky; top: 0; }
    .status { font-weight: 700; }
    .live { color: #067647; }
    .stale { color: #b54708; }
    .missing { color: #b42318; }
    .inactive { color: #667085; }
  </style>
</head>
<body>
  <header>
    <h1>Relay Symbol Dashboard</h1>
  </header>
  <main>
    <section class="summary" id="summary"></section>
    <section class="toolbar">
      <input id="query" aria-label="Search symbol">
      <select id="status">
        <option value="">All statuses</option>
        <option value="live">Live</option>
        <option value="stale">Stale</option>
        <option value="missing">Missing</option>
        <option value="inactive">Inactive</option>
      </select>
      <select id="subscribed">
        <option value="">All symbols</option>
        <option value="1">Subscribed only</option>
      </select>
      <select id="sort">
        <option value="receive_gap_ms_desc">Receive gap desc</option>
        <option value="market_time_lag_ms_desc">Market time lag desc</option>
        <option value="symbol_asc">Symbol asc</option>
        <option value="status_asc">Status asc</option>
        <option value="ticks_ingested_desc">Tick count desc</option>
      </select>
      <select id="limit">
        <option value="200">200 rows</option>
        <option value="500">500 rows</option>
        <option value="1000">1000 rows</option>
      </select>
      <button id="refresh">Refresh</button>
    </section>
    <table>
      <thead>
        <tr>
          <th>Status</th><th>Symbol</th><th>Subscribed</th><th>Receive gap</th>
          <th>Market lag</th><th>Last receive</th><th>Last tick</th><th>Ticks</th>
          <th>Last price</th><th>Quote subs</th><th>Chart subs</th><th>Invalid rows</th>
        </tr>
      </thead>
      <tbody id="symbols"></tbody>
    </table>
  </main>
  <script src="/dashboard/app.js"></script>
</body>
</html>
"#;

pub const DASHBOARD_JS: &str = r#"
const summary = document.getElementById('summary');
const symbols = document.getElementById('symbols');
const controls = ['query', 'status', 'subscribed', 'sort', 'limit']
  .map((id) => document.getElementById(id));

function params() {
  const query = new URLSearchParams();
  const q = document.getElementById('query').value.trim();
  const status = document.getElementById('status').value;
  const subscribed = document.getElementById('subscribed').value;
  const sort = document.getElementById('sort').value;
  const limit = document.getElementById('limit').value;
  if (q) query.set('q', q);
  if (status) query.set('status', status);
  if (subscribed) query.set('subscribed', subscribed);
  if (sort) query.set('sort', sort);
  if (limit) query.set('limit', limit);
  return query.toString();
}

function fmtMs(value) {
  if (value === null || value === undefined) return '--';
  if (value < 1000) return `${value}ms`;
  return `${(value / 1000).toFixed(1)}s`;
}

function fmtTime(value) {
  if (!value) return '--';
  return new Date(value).toLocaleTimeString();
}

function render(data) {
  const s = data.summary;
  summary.innerHTML = [
    ['live', s.live], ['stale', s.stale], ['missing', s.missing],
    ['inactive', s.inactive], ['subscribed', s.subscribed],
    ['p95 gap', fmtMs(s.p95_receive_gap_ms)]
  ].map(([label, value]) => `<div class="tile"><div class="label">${label}</div><div class="value">${value}</div></div>`).join('');
  symbols.innerHTML = data.symbols.map((row) => `
    <tr>
      <td class="status ${row.status}">${row.status}</td>
      <td>${row.symbol}</td>
      <td>${row.subscribed ? 'yes' : 'no'}</td>
      <td>${fmtMs(row.receive_gap_ms)}</td>
      <td>${fmtMs(row.market_time_lag_ms)}</td>
      <td>${fmtTime(row.last_receive_unix_millis)}</td>
      <td>${row.last_tick_datetime_ns ? fmtTime(Math.floor(row.last_tick_datetime_ns / 1000000)) : '--'}</td>
      <td>${row.ticks_ingested}</td>
      <td>${row.last_price === null || row.last_price === undefined ? '--' : row.last_price}</td>
      <td>${row.quote_subscriber_count}</td>
      <td>${row.chart_subscriber_count}</td>
      <td title="${row.last_invalid_row_error || ''}">${row.invalid_rows}</td>
    </tr>
  `).join('');
}

async function load() {
  const suffix = params();
  const response = await fetch(`/symbol-metrics${suffix ? `?${suffix}` : ''}`);
  render(await response.json());
}

document.getElementById('refresh').addEventListener('click', load);
for (const control of controls) {
  control.addEventListener('change', load);
}
document.getElementById('query').addEventListener('input', () => {
  clearTimeout(window.relayDashboardSearchTimer);
  window.relayDashboardSearchTimer = setTimeout(load, 250);
});
load();
setInterval(load, 2000);
"#;
```

Modify `crates/tqsdk-relay/src/lib.rs`:

```rust
pub mod dashboard;
```

- [ ] **Step 4: Serve dashboard assets**

Modify `crates/tqsdk-relay/src/metrics_http.rs` imports:

```rust
use crate::dashboard::{DASHBOARD_HTML, DASHBOARD_JS};
```

Add route branches before the fallback:

```rust
"/dashboard" => {
    write_text_response(stream, 200, "text/html; charset=utf-8", DASHBOARD_HTML).await?;
    return Ok(());
}
"/dashboard/app.js" => {
    write_text_response(
        stream,
        200,
        "application/javascript; charset=utf-8",
        DASHBOARD_JS,
    )
    .await?;
    return Ok(());
}
```

Add text response helper:

```rust
async fn write_text_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &str,
) -> RelayResult<()> {
    let response = format!(
        "HTTP/1.1 {status} {}\r\n\
Content-Type: {content_type}\r\n\
Content-Length: {}\r\n\
Connection: close\r\n\
\r\n\
{body}",
        status_reason(status),
        body.len(),
    );
    stream
        .write_all(response.as_bytes())
        .await
        .map_err(|err| RelayError::Transport(format!("metrics write failed: {err}")))
}
```

- [ ] **Step 5: Run dashboard smoke test**

Run:

```bash
cargo test -p tqsdk-relay --test binary_smoke relay_binary_serves_embedded_dashboard_assets
```

Expected: dashboard asset test passes.

- [ ] **Step 6: Commit Task 4**

```bash
git add crates/tqsdk-relay/src/dashboard.rs crates/tqsdk-relay/src/lib.rs crates/tqsdk-relay/src/metrics_http.rs crates/tqsdk-relay/tests/binary_smoke.rs
git commit -m "feat(relay): add embedded symbol dashboard"
```

---

### Task 5: Document Dashboard Behavior

**Files:**
- Modify: `crates/tqsdk-relay/README.md`
- Modify: `docs/architecture/validation.md`
- Test: none beyond markdown diff check

- [ ] **Step 1: Update relay README HTTP observability section**

In `crates/tqsdk-relay/README.md`, extend the HTTP commands block:

```bash
curl http://127.0.0.1:7789/health
curl http://127.0.0.1:7789/metrics
curl http://127.0.0.1:7789/symbol-metrics
open http://127.0.0.1:7789/dashboard
```

Add this paragraph after the existing `/metrics` description:

```markdown
`/symbol-metrics` 返回合约级 telemetry 快照，覆盖当前上游 universe 和下游实际订阅合约。状态主口径是 relay 接收间隔延迟：`live` 表示最近 `30s` 内收到 tick，`stale` 表示收过 tick 但超过 freshness 窗口，`missing` 表示在上游 universe 中但从未收到 tick，`inactive` 表示下游订阅了不在上游 universe 中的合约。响应同时包含 `market_time_lag_ms`，用于辅助判断行情时间与本地时间的差距。

`/dashboard` 是内置只读运维页面，每 `2s` 轮询 `/symbol-metrics`。它不连接 relay market websocket，不创建下游订阅，也不会触发额外行情命令。tick ingest 热路径只更新当前合约的轻量 telemetry，排序、过滤和 JSON 生成只发生在 HTTP snapshot 请求侧。
```

- [ ] **Step 2: Update validation doc if relay checks are listed**

Search:

```bash
rg -n "tqsdk-relay|symbol-metrics|cargo test -p tqsdk-relay" docs/architecture/validation.md
```

If `docs/architecture/validation.md` already lists relay validation commands, add:

```markdown
- 修改 relay dashboard 或 symbol telemetry 时，补充运行：
  `cargo test -p tqsdk-relay --test symbol_metrics`
  `cargo test -p tqsdk-relay --test binary_smoke relay_binary_serves_embedded_dashboard_assets`
```

If the file does not list relay-specific checks, leave it unchanged.

- [ ] **Step 3: Run markdown diff check**

Run:

```bash
git diff --check -- crates/tqsdk-relay/README.md docs/architecture/validation.md
```

Expected: no output.

- [ ] **Step 4: Commit Task 5**

```bash
git add crates/tqsdk-relay/README.md docs/architecture/validation.md
git commit -m "docs: describe relay symbol dashboard"
```

If `docs/architecture/validation.md` was not changed, use:

```bash
git add crates/tqsdk-relay/README.md
git commit -m "docs: describe relay symbol dashboard"
```

---

### Task 6: Final Verification and Scope Check

**Files:**
- No planned source edits
- Use GitNexus before final commit or final report

- [ ] **Step 1: Run formatting check**

```bash
cargo fmt --all --check
```

Expected: command exits 0.

- [ ] **Step 2: Run relay test suite**

```bash
cargo test -p tqsdk-relay --tests
```

Expected: all relay tests pass.

- [ ] **Step 3: Run no-default-features check**

```bash
cargo check -p tqsdk-relay --no-default-features
```

Expected: command exits 0. If `dashboard.rs` is always compiled, it must not depend on `server` or `metadata` features.

- [ ] **Step 4: Run relay clippy**

```bash
cargo clippy -p tqsdk-relay --all-targets -- -D warnings
```

Expected: command exits 0.

- [ ] **Step 5: Run workspace examples check**

```bash
cargo check --workspace --examples
```

Expected: command exits 0. This guards against accidental API breakage in sibling crates.

- [ ] **Step 6: Run whitespace check**

```bash
git diff --check
```

Expected: no output.

- [ ] **Step 7: Run GitNexus change detection**

```bash
npx gitnexus detect-changes --repo tqsdk-rust
```

Expected: risk is low or explainable, and affected files are limited to `tqsdk-relay` plus docs.

- [ ] **Step 8: Final scope audit**

Confirm:

```bash
git status --short
```

Expected: only intended relay/dashboard files and docs are modified. Do not stage `.superpowers/`, unrelated `AGENTS.md` / `CLAUDE.md` GitNexus statistic churn, or other scratch files.

- [ ] **Step 9: Final commit if any changes remain uncommitted**

If Tasks 1-5 were already committed and Step 8 shows no intended unstaged changes, do not create an empty commit.

If final verification required a formatting or documentation fix, commit only those intended files:

```bash
git add <intended-files>
git commit -m "chore: finalize relay symbol dashboard"
```

---

## Self-Review

### Spec coverage

- Table-first dashboard: Task 4.
- Upstream universe plus downstream subscription markers: Tasks 1 and 2.
- Receive gap and market-time lag: Task 1.
- Embedded `/dashboard`, `/dashboard/app.js`, `/symbol-metrics`: Tasks 3 and 4.
- O(1) tick ingest and snapshot-side scanning: Tasks 1, 2, and 6.
- No SSE, no Prometheus high-cardinality export, no dashboard market websocket subscription: Task 4 and Task 5.
- Tests and validation commands: Tasks 1 through 6.

### Type consistency

- `SymbolStatus` values serialize as `live`, `stale`, `missing`, and `inactive`.
- `SymbolMetricsQuery::from_query_string` maps HTTP query parameters to `SymbolSort` and status filters used by `SymbolTelemetryStore::snapshot_at`.
- `RelayEngine::symbol_metrics_snapshot_at` passes subscription counts from `InterestRegistry::symbol_subscription_counts`.
- `/symbol-metrics` uses the same snapshot type that tests assert.

### Performance guard

- Tick ingest updates only one telemetry entry with `record_tick_at`.
- Universe scan, summary, sorting, filtering, and limit happen only in `SymbolTelemetryStore::snapshot_at`, which is called by HTTP snapshot access.
- The dashboard fetches `/symbol-metrics`; it never connects to the relay market websocket and never sends `subscribe_quote` or `set_chart`.
